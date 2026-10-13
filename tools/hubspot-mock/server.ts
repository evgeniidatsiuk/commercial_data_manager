/**
 * Minimal in-memory HubSpot CRM API mock (subset used by CDM) + webhook simulator.
 * Runs directly on Node >= 24 (native TypeScript type stripping, no dependencies).
 */
import { createHmac } from 'node:crypto';
import { createServer, type IncomingMessage, type ServerResponse } from 'node:http';

const PORT = Number(process.env.PORT ?? 4010);
const WEBHOOK_TARGET_URL = process.env.WEBHOOK_TARGET_URL ?? 'http://cdm:3000/webhooks/hubspot';
const CLIENT_SECRET = process.env.HUBSPOT_CLIENT_SECRET ?? 'mock-client-secret';
const RATE_LIMIT_PERCENT = Number(process.env.MOCK_RATE_LIMIT_PERCENT ?? 0);

type CrmObject = { id: string; properties: Record<string, string>; createdAt: string; updatedAt: string; archived: false };
type Association = { from: string; fromId: string; to: string; toId: string };

const OBJECT_TYPES = ['contacts', 'companies', 'deals'];
const store = new Map<string, Map<string, CrmObject>>(OBJECT_TYPES.map((t) => [t, new Map()]));
const properties = new Map<string, Record<string, unknown>>();
const associations: Association[] = [];
// Like real HubSpot, IDs are never reused (also across mock restarts) so CDM's inbox/mappings stay valid.
let nextObjectId = Date.now();
let nextEventId = Date.now();

const send = (res: ServerResponse, status: number, body?: unknown): void => {
  res.writeHead(status, body === undefined ? {} : { 'content-type': 'application/json' });
  res.end(body === undefined ? undefined : JSON.stringify(body));
};

const error = (res: ServerResponse, status: number, message: string, category = 'VALIDATION_ERROR'): void =>
  send(res, status, { status: 'error', message, category, correlationId: crypto.randomUUID() });

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function readJson(req: IncomingMessage): Promise<any> {
  const chunks: Buffer[] = [];
  for await (const chunk of req) chunks.push(chunk as Buffer);
  const raw = Buffer.concat(chunks).toString('utf8');
  return raw ? JSON.parse(raw) : {};
}

/** Properties HubSpot enforces uniqueness on (email natively, others via hasUniqueValue). */
const uniqueProperties = (type: string): string[] => [
  ...(type === 'contacts' ? ['email'] : []),
  ...[...properties.entries()]
    .filter(([key, def]) => key.startsWith(`${type}:`) && def.hasUniqueValue)
    .map(([, def]) => String(def.name)),
];

const findConflict = (type: string, props: Record<string, string>, selfId?: string): CrmObject | undefined =>
  [...store.get(type)!.values()].find(
    (o) => o.id !== selfId && uniqueProperties(type).some((p) => props[p] && o.properties[p] === props[p]),
  );

const BATCH_LIMIT = 100;
const requestCounts = new Map<string, number>();

function createObject(type: string, props: Record<string, string>): CrmObject {
  const now = new Date().toISOString();
  const obj: CrmObject = { id: String(nextObjectId++), properties: { ...props }, createdAt: now, updatedAt: now, archived: false };
  store.get(type)!.set(obj.id, obj);
  return obj;
}

function associate(from: string, fromId: string, to: string, toId: string): boolean {
  if (!store.get(from)?.has(fromId) || !store.get(to)?.has(toId)) return false;
  if (!associations.some((a) => a.from === from && a.fromId === fromId && a.to === to && a.toId === toId)) {
    associations.push({ from, fromId, to, toId });
  }
  return true;
}

const batchError = (category: string, message: string, id: string) => ({
  status: 'error',
  category,
  message,
  context: { ids: [id] },
});

async function sendDealStageWebhook(dealId: string, stage: string): Promise<number> {
  const body = JSON.stringify([
    {
      eventId: nextEventId++,
      subscriptionId: 1,
      portalId: 1,
      appId: 1,
      occurredAt: Date.now(),
      subscriptionType: 'deal.propertyChange',
      attemptNumber: 0,
      objectId: Number(dealId),
      propertyName: 'dealstage',
      propertyValue: stage,
      changeSource: 'CRM_UI',
    },
  ]);
  const timestamp = String(Date.now());
  const signature = createHmac('sha256', CLIENT_SECRET)
    .update(`POST${WEBHOOK_TARGET_URL}${body}${timestamp}`)
    .digest('base64');
  const res = await fetch(WEBHOOK_TARGET_URL, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'x-hubspot-signature-v3': signature,
      'x-hubspot-request-timestamp': timestamp,
    },
    body,
  });
  return res.status;
}

async function route(req: IncomingMessage, res: ServerResponse): Promise<void> {
  const url = new URL(req.url ?? '/', 'http://localhost');
  const path = url.pathname;
  const method = req.method ?? 'GET';
  let m: RegExpMatchArray | null;

  // --- Mock control endpoints -------------------------------------------------------------
  if (path === '/__mock/health') return send(res, 200, { status: 'ok' });
  if (path === '/__mock/stats') return send(res, 200, { requests: Object.fromEntries(requestCounts) });
  if (path === '/__mock/state') {
    return send(res, 200, {
      objects: Object.fromEntries([...store].map(([t, objs]) => [t, [...objs.values()]])),
      associations,
      properties: [...properties.values()],
    });
  }
  if (method === 'POST' && (m = path.match(/^\/__mock\/deals\/(\d+)\/stage$/))) {
    const deal = store.get('deals')!.get(m[1]);
    if (!deal) return error(res, 404, 'Deal not found', 'OBJECT_NOT_FOUND');
    const { stage } = await readJson(req);
    deal.properties.dealstage = stage;
    deal.updatedAt = new Date().toISOString();
    const webhookStatus = await sendDealStageWebhook(deal.id, stage);
    return send(res, 200, { deal, webhookStatus });
  }

  // --- HubSpot API ------------------------------------------------------------------------
  const routeKey = `${method} ${path.replace(/\/\d+/g, '/{id}')}`;
  requestCounts.set(routeKey, (requestCounts.get(routeKey) ?? 0) + 1);
  if (!req.headers.authorization?.startsWith('Bearer ')) {
    return error(res, 401, 'Authentication credentials not found.', 'INVALID_AUTHENTICATION');
  }
  // Chaos only on writes/search (what CDM calls), so GET inspection from tests stays deterministic.
  if (method !== 'GET' && Math.random() * 100 < RATE_LIMIT_PERCENT) {
    return error(res, 429, 'You have reached your secondly limit.', 'RATE_LIMITS');
  }

  if (method === 'POST' && (m = path.match(/^\/crm\/v3\/properties\/(\w+)$/))) {
    const def = await readJson(req);
    const key = `${m[1]}:${def.name}`;
    if (properties.has(key)) return error(res, 409, `Property ${def.name} already exists`, 'OBJECT_ALREADY_EXISTS');
    properties.set(key, def);
    return send(res, 201, def);
  }

  if ((m = path.match(/^\/crm\/v4\/objects\/(\w+)\/(\d+)\/associations\/default\/(\w+)\/(\d+)$/)) && method === 'PUT') {
    const [, from, fromId, to, toId] = m;
    if (!associate(from, fromId, to, toId)) return error(res, 404, 'Object not found');
    return send(res, 200, { status: 'COMPLETE', results: [{ from: { id: fromId }, to: { id: toId } }] });
  }

  if (method === 'POST' && (m = path.match(/^\/crm\/v4\/associations\/(\w+)\/(\w+)\/batch\/associate\/default$/))) {
    const [, from, to] = m;
    const { inputs = [] } = await readJson(req);
    if (inputs.length > BATCH_LIMIT) return error(res, 400, `Batch size ${inputs.length} exceeds ${BATCH_LIMIT}`);
    const results: unknown[] = [];
    const errors: unknown[] = [];
    for (const input of inputs) {
      if (associate(from, input.from.id, to, input.to.id)) results.push(input);
      else errors.push(batchError('OBJECT_NOT_FOUND', 'Object not found', input.from.id));
    }
    return send(res, errors.length ? 207 : 200, { status: 'COMPLETE', results, errors });
  }

  if (method === 'POST' && (m = path.match(/^\/crm\/v3\/objects\/(\w+)\/batch\/(upsert|read|create|update)$/)) && store.has(m[1])) {
    const [, type, op] = m;
    const objects = store.get(type)!;
    const body = await readJson(req);
    // eslint-disable-next-line @typescript-eslint/no-explicit-any
    const inputs: Array<Record<string, any>> = body.inputs ?? [];
    if (inputs.length > BATCH_LIMIT) return error(res, 400, `Batch size ${inputs.length} exceeds ${BATCH_LIMIT}`);
    const findBy = (prop: string, value: string) => [...objects.values()].find((o) => o.properties[prop] === value);
    const results: unknown[] = [];
    const errors: unknown[] = [];

    if (op === 'create') {
      // Like HubSpot, a conflict fails the whole batch.
      const conflict = inputs.map((i) => findConflict(type, i.properties ?? {})).find(Boolean);
      if (conflict) return error(res, 409, `Object already exists. Existing ID: ${conflict.id}`, 'CONFLICT');
      for (const i of inputs) results.push(createObject(type, i.properties ?? {}));
      return send(res, 201, { status: 'COMPLETE', results });
    }

    for (const input of inputs) {
      if (op === 'read') {
        const obj = body.idProperty ? findBy(body.idProperty, input.id) : objects.get(input.id);
        if (obj) results.push(obj);
        else errors.push(batchError('OBJECT_NOT_FOUND', `Object ${input.id} not found`, input.id));
      } else if (op === 'update') {
        const obj = objects.get(input.id);
        if (!obj) {
          errors.push(batchError('OBJECT_NOT_FOUND', `Object ${input.id} not found`, input.id));
          continue;
        }
        Object.assign(obj.properties, input.properties);
        obj.updatedAt = new Date().toISOString();
        results.push(obj);
      } else {
        const value = input.idProperty === 'email' ? String(input.id).toLowerCase() : input.id;
        const existing = findBy(input.idProperty, value);
        if (existing) {
          Object.assign(existing.properties, input.properties, { [input.idProperty]: value });
          existing.updatedAt = new Date().toISOString();
          results.push({ ...existing, new: false });
        } else {
          results.push({ ...createObject(type, { ...input.properties, [input.idProperty]: value }), new: true });
        }
      }
    }
    return send(res, errors.length ? 207 : 200, { status: 'COMPLETE', results, errors });
  }

  if (!(m = path.match(/^\/crm\/v3\/objects\/(\w+)(?:\/(\w+))?$/)) || !store.has(m[1])) {
    return error(res, 404, `No route for ${method} ${path}`, 'NOT_FOUND');
  }
  const type = m[1];
  const sub = m[2];
  const objects = store.get(type)!;

  if (method === 'POST' && sub === 'search') {
    const { filterGroups = [], limit = 10 } = await readJson(req);
    const filters: Array<{ propertyName: string; operator: string; value: string }> = filterGroups[0]?.filters ?? [];
    const results = [...objects.values()]
      .filter((o) => filters.every((f) => f.operator === 'EQ' && o.properties[f.propertyName] === f.value))
      .slice(0, limit);
    return send(res, 200, { total: results.length, results });
  }

  if (method === 'POST' && !sub) {
    const { properties: props = {} } = await readJson(req);
    const conflict = findConflict(type, props);
    if (conflict) return error(res, 409, `Object already exists. Existing ID: ${conflict.id}`, 'CONFLICT');
    return send(res, 201, createObject(type, props));
  }

  if (method === 'GET' && !sub) {
    return send(res, 200, { results: [...objects.values()] });
  }

  const obj = sub ? objects.get(sub) : undefined;
  if (!obj) return error(res, 404, 'Object not found', 'OBJECT_NOT_FOUND');

  if (method === 'GET') return send(res, 200, obj);
  if (method === 'PATCH') {
    const { properties: props = {} } = await readJson(req);
    const conflict = findConflict(type, props, obj.id);
    if (conflict) return error(res, 409, `Object already exists. Existing ID: ${conflict.id}`, 'CONFLICT');
    Object.assign(obj.properties, props);
    obj.updatedAt = new Date().toISOString();
    return send(res, 200, obj);
  }

  return error(res, 405, `Method ${method} not allowed`);
}

createServer((req, res) => {
  route(req, res).catch((err: unknown) => {
    console.error(err);
    if (!res.headersSent) error(res, 500, 'Internal mock error', 'INTERNAL');
  });
}).listen(PORT, () => console.log(`HubSpot mock listening on :${PORT} (webhooks -> ${WEBHOOK_TARGET_URL})`));
