import { metrics, trace } from '@opentelemetry/api';

export const meter = metrics.getMeter('commercial-data-manager');
export const tracer = trace.getTracer('commercial-data-manager');

export const cdmMetrics = {
  eventsIngested: meter.createCounter('cdm.events.ingested', {
    description: 'Kafka events consumed, by topic and result',
  }),
  webhookEvents: meter.createCounter('cdm.webhook.events', {
    description: 'HubSpot webhook events, by result',
  }),
  syncResults: meter.createCounter('cdm.sync.results', {
    description: 'HubSpot sync attempts, by entity type, mode (single/bulk) and result',
  }),
  syncBatchSize: meter.createHistogram('cdm.sync.batch.size', {
    description: 'Entities per bulk sync',
    advice: { explicitBucketBoundaries: [1, 5, 10, 25, 50, 100, 250] },
  }),
  hubspotRequestDuration: meter.createHistogram('cdm.hubspot.request.duration', {
    unit: 's',
    description: 'HubSpot API latency, by operation and HTTP status',
    advice: { explicitBucketBoundaries: [0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10] },
  }),
  outboxPublished: meter.createCounter('cdm.outbox.published', {
    description: 'Outbox messages published to Kafka',
  }),
};
