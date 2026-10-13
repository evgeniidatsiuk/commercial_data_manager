import { randomUUID } from 'node:crypto';
import { setTimeout as sleep } from 'node:timers/promises';
import { Kafka, logLevel } from 'kafkajs';

/**
 * Black-box test against the running docker compose stack:
 *   docker compose up -d --build --wait && npm run test:e2e
 */
const CDM = process.env.CDM_URL ?? 'http://localhost:3000';
const MOCK = process.env.HUBSPOT_MOCK_URL ?? 'http://localhost:4010';
const BROKERS = (process.env.KAFKA_BROKERS ?? 'localhost:29092').split(',');

// eslint-disable-next-line @typescript-eslint/no-explicit-any
type Json = any;

async function http(method: string, url: string, body?: unknown): Promise<{ status: number; body: Json }> {
  const res = await fetch(url, {
    method,
    headers: { 'content-type': 'application/json', authorization: 'Bearer e2e' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : undefined };
}

async function eventually<T>(probe: () => Promise<T | undefined>, what: string, timeoutMs = 45_000): Promise<T> {
  const deadline = Date.now() + timeoutMs;
  while (Date.now() < deadline) {
    const value = await probe();
    if (value !== undefined) return value;
    await sleep(500);
  }
  throw new Error(`Timed out waiting for: ${what}`);
}

jest.setTimeout(120_000);

describe('CDM round trip: Kafka -> Postgres -> HubSpot -> webhook -> Kafka', () => {
  const runId = randomUUID().slice(0, 8);
  const affiliateId = `aff-${runId}`;
  const dealId = `deal-${runId}`;
  let hubspotCompanyId: string;
  let hubspotDealId: string;

  it('syncs an affiliate to a HubSpot company + contact, ignoring duplicate deliveries', async () => {
    const first = {
      eventId: randomUUID(),
      affiliateId,
      companyName: 'Lucky Reviews Ltd',
      domain: `${affiliateId}.example.com`,
      contact: { email: `${affiliateId}@example.com`, firstName: 'Ada', lastName: 'Lovelace' },
      country: 'GB',
      tier: 'gold',
    };
    expect((await http('POST', `${CDM}/dev/events/affiliate`, first)).status).toBe(202);
    expect((await http('POST', `${CDM}/dev/events/affiliate`, first)).status).toBe(202);
    const second = { ...first, eventId: randomUUID(), companyName: 'Lucky Reviews Group' };
    expect((await http('POST', `${CDM}/dev/events/affiliate`, second)).status).toBe(202);

    const affiliate = await eventually(async () => {
      const res = await http('GET', `${CDM}/affiliates/${affiliateId}`);
      return res.status === 200 && res.body.companyName === 'Lucky Reviews Group' && res.body.sync?.status === 'synced'
        ? res.body
        : undefined;
    }, 'affiliate synced');

    // Two distinct events applied, the duplicate was dropped by the inbox.
    expect(affiliate.sync.desiredVersion).toBe(2);
    expect(affiliate.hubspotContactId).toBeTruthy();
    hubspotCompanyId = affiliate.hubspotCompanyId;

    const company = await http('GET', `${MOCK}/crm/v3/objects/companies/${hubspotCompanyId}`);
    expect(company.body.properties).toMatchObject({
      name: 'Lucky Reviews Group',
      legend_affiliate_id: affiliateId,
      legend_affiliate_tier: 'gold',
    });
  });

  it('syncs a deal and associates it with the affiliate company', async () => {
    const res = await http('POST', `${CDM}/dev/events/deal`, {
      dealId,
      affiliateId,
      name: 'Q4 CPA - UK casino',
      commissionModel: 'CPA',
      amount: 150,
    });
    expect(res.status).toBe(202);

    const deal = await eventually(async () => {
      const r = await http('GET', `${CDM}/deals/${dealId}`);
      return r.status === 200 && r.body.sync?.status === 'synced' ? r.body : undefined;
    }, 'deal synced');
    hubspotDealId = deal.hubspotDealId;

    const hsDeal = await http('GET', `${MOCK}/crm/v3/objects/deals/${hubspotDealId}`);
    expect(hsDeal.body.properties).toMatchObject({
      dealname: 'Q4 CPA - UK casino',
      dealstage: 'appointmentscheduled',
      legend_commission_model: 'CPA',
    });

    const state = await http('GET', `${MOCK}/__mock/state`);
    expect(state.body.associations).toContainEqual({
      from: 'deals',
      fromId: hubspotDealId,
      to: 'companies',
      toId: hubspotCompanyId,
    });
  });

  it('propagates a HubSpot deal-stage change to CDM and back onto Kafka', async () => {
    const kafka = new Kafka({ clientId: 'cdm-e2e', brokers: BROKERS, logLevel: logLevel.NOTHING });
    const consumer = kafka.consumer({ groupId: `cdm-e2e-${runId}` });
    const received: Json[] = [];
    await consumer.connect();
    await consumer.subscribe({ topic: 'cdm.hubspot-changes.v1', fromBeginning: true });
    await consumer.run({
      eachMessage: async ({ message }) => {
        received.push(JSON.parse(message.value?.toString() ?? '{}'));
      },
    });

    try {
      const sim = await http('POST', `${MOCK}/__mock/deals/${hubspotDealId}/stage`, { stage: 'closedwon' });
      expect(sim.body.webhookStatus).toBe(204);

      await eventually(async () => {
        const r = await http('GET', `${CDM}/deals/${dealId}`);
        return r.body.hubspotStage === 'closedwon' ? true : undefined;
      }, 'stage stored in CDM');

      const event = await eventually(
        async () => received.find((e) => e.data?.dealId === dealId),
        'stage-changed event on Kafka',
      );
      expect(event).toMatchObject({
        type: 'hubspot.deal.stage-changed',
        data: { dealId, affiliateId, hubspotDealId, stage: 'closedwon' },
      });
    } finally {
      await consumer.disconnect();
    }
  });

  it('rejects webhooks with an invalid signature', async () => {
    const res = await fetch(`${CDM}/webhooks/hubspot`, {
      method: 'POST',
      headers: {
        'content-type': 'application/json',
        'x-hubspot-signature-v3': 'forged',
        'x-hubspot-request-timestamp': String(Date.now()),
      },
      body: '[]',
    });
    expect(res.status).toBe(401);
  });
});

describe('Backfill through HubSpot batch APIs', () => {
  const AFFILIATES = 150;

  const syncStatus = async (path: string) => (await http('GET', `${CDM}${path}`)).body?.sync;

  it('drains a burst of seeded affiliates + deals', async () => {
    const seeded = await http('POST', `${CDM}/dev/seed`, { affiliates: AFFILIATES, dealsPerAffiliate: 1 });
    expect(seeded.status).toBe(202);
    const { prefix } = seeded.body;

    for (const path of [`/affiliates/${prefix}-aff-0`, `/deals/${prefix}-deal-${AFFILIATES - 1}-0`]) {
      await eventually(async () => ((await syncStatus(path))?.status === 'synced' ? true : undefined), path, 90_000);
    }
    // Deals that raced ahead of their affiliate are retried with backoff; wait for a quiet system.
    await eventually(
      async () => ((await http('GET', `${CDM}/sync/stats`)).body.totals.pending === 0 ? true : undefined),
      'backlog drained',
      90_000,
    );
  });

  it('re-pushes every affiliate with a fraction of the API calls of per-record sync', async () => {
    const before = (await http('GET', `${MOCK}/__mock/stats`)).body.requests;
    const resync = await http('POST', `${CDM}/sync/resync`, { entityType: 'affiliate' });
    expect(resync.status).toBe(202);
    const requested: number = resync.body.requested;
    expect(requested).toBeGreaterThanOrEqual(AFFILIATES);

    await eventually(async () => {
      const stats = (await http('GET', `${CDM}/sync/stats`)).body;
      return stats.byEntityType.affiliate.pending === 0 ? stats : undefined;
    }, 'affiliate resync drained', 90_000);

    const after = (await http('GET', `${MOCK}/__mock/stats`)).body.requests;
    const delta = (key: string) => (after[key] ?? 0) - (before[key] ?? 0);
    const totalCalls = Object.keys(after)
      .filter((key) => !key.includes('deals'))
      .reduce((sum, key) => sum + delta(key), 0);

    expect(delta('POST /crm/v3/objects/companies/batch/upsert')).toBeGreaterThanOrEqual(Math.ceil(requested / 100));
    expect(delta('POST /crm/v3/objects/contacts/batch/upsert')).toBeGreaterThanOrEqual(1);
    // Per-record sync would need ~4 calls per affiliate (search/update company + contact, associate).
    expect(totalCalls).toBeLessThan(requested / 2);
  });
});
