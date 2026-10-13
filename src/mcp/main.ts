import '../observability/otel';
import { McpServer } from '@modelcontextprotocol/sdk/server/mcp.js';
import { StdioServerTransport } from '@modelcontextprotocol/sdk/server/stdio.js';
import { SpanKind, SpanStatusCode, trace } from '@opentelemetry/api';
import { z } from 'zod';
import { affiliateDataSchema, dealDataSchema } from '../contracts/events';
import { otelSdk } from '../observability/otel';
import { ENTITY_TYPES, SYNC_STATUSES } from '../persistence/sync-state.repository';

/**
 * MCP server exposing CDM operations as tools for AI agents (Copilot, Claude, ...).
 * It is a thin adapter over the CDM HTTP API, so agents get exactly the same guardrails as humans.
 * stdout is reserved for the MCP protocol: log to stderr only.
 */
const baseUrl = process.env.CDM_API_URL ?? 'http://localhost:3000';
const tracer = trace.getTracer('cdm-mcp');

/** Each tool call is a span; fetch propagates traceparent so the CDM side joins the same trace. */
function callCdm(tool: string, method: 'GET' | 'POST', path: string, body?: unknown) {
  const attributes = { 'mcp.tool.name': tool, 'http.request.method': method, 'url.path': path };
  return tracer.startActiveSpan(`mcp.tool ${tool}`, { kind: SpanKind.SERVER, attributes }, async (span) => {
    try {
      const res = await fetch(`${baseUrl}${path}`, {
        method,
        headers: body === undefined ? undefined : { 'content-type': 'application/json' },
        body: body === undefined ? undefined : JSON.stringify(body),
      });
      const text = await res.text();
      span.setAttribute('http.response.status_code', res.status);
      if (!res.ok) span.setStatus({ code: SpanStatusCode.ERROR, message: `HTTP ${res.status}` });
      return {
        content: [{ type: 'text' as const, text: `HTTP ${res.status}\n${text}` }],
        isError: !res.ok,
      };
    } finally {
      span.end();
    }
  });
}

const server = new McpServer({ name: 'commercial-data-manager', version: '0.1.0' });

server.registerTool(
  'get_affiliate',
  {
    description: 'Get an affiliate as known by CDM: HubSpot company/contact IDs, deals and HubSpot sync state.',
    inputSchema: { affiliateId: z.string() },
    annotations: { readOnlyHint: true },
  },
  ({ affiliateId }) => callCdm('get_affiliate', 'GET', `/affiliates/${encodeURIComponent(affiliateId)}`),
);

server.registerTool(
  'get_deal',
  {
    description: 'Get a deal: commercial terms, HubSpot deal ID, current HubSpot pipeline stage and sync state.',
    inputSchema: { dealId: z.string() },
    annotations: { readOnlyHint: true },
  },
  ({ dealId }) => callCdm('get_deal', 'GET', `/deals/${encodeURIComponent(dealId)}`),
);

server.registerTool(
  'get_sync_stats',
  {
    description:
      'Health overview of the HubSpot sync: entity counts per status and lagSeconds (age of the oldest unsynced change).',
    annotations: { readOnlyHint: true },
  },
  () => callCdm('get_sync_stats', 'GET', '/sync/stats'),
);

server.registerTool(
  'list_sync_states',
  {
    description: 'List HubSpot sync states, e.g. status=failed to investigate integration issues (includes lastError).',
    inputSchema: { status: z.enum(SYNC_STATUSES).optional(), limit: z.number().int().min(1).max(500).optional() },
    annotations: { readOnlyHint: true },
  },
  ({ status, limit }) => {
    const qs = new URLSearchParams();
    if (status) qs.set('status', status);
    if (limit) qs.set('limit', String(limit));
    return callCdm('list_sync_states', 'GET', `/sync?${qs}`);
  },
);

server.registerTool(
  'retry_sync',
  {
    description: 'Re-queue a failed HubSpot sync after the root cause has been fixed.',
    inputSchema: { entityType: z.enum(ENTITY_TYPES), entityId: z.string() },
    annotations: { idempotentHint: true },
  },
  ({ entityType, entityId }) =>
    callCdm('retry_sync', 'POST', `/sync/${entityType}/${encodeURIComponent(entityId)}/retry`),
);

server.registerTool(
  'publish_affiliate_event',
  {
    description: '[dev only] Publish an affiliate.upserted event to Kafka, as the affiliate platform would.',
    inputSchema: affiliateDataSchema.shape,
  },
  (data) => callCdm('publish_affiliate_event', 'POST', '/dev/events/affiliate', data),
);

server.registerTool(
  'publish_deal_event',
  {
    description: '[dev only] Publish a deal.upserted event to Kafka, as the deals service would.',
    inputSchema: dealDataSchema.shape,
  },
  (data) => callCdm('publish_deal_event', 'POST', '/dev/events/deal', data),
);

server
  .connect(new StdioServerTransport())
  .then(() => console.error(`CDM MCP server ready (CDM_API_URL=${baseUrl})`))
  .catch((err: unknown) => {
    console.error(err);
    process.exit(1);
  });

// Agent sessions can be short: flush tool-call spans when the client disconnects.
process.stdin.on('end', () => {
  void (otelSdk?.shutdown() ?? Promise.resolve()).finally(() => process.exit(0));
});
