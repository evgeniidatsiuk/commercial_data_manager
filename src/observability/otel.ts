import { OTLPLogExporter } from '@opentelemetry/exporter-logs-otlp-http';
import { OTLPMetricExporter } from '@opentelemetry/exporter-metrics-otlp-http';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { ExpressInstrumentation } from '@opentelemetry/instrumentation-express';
import { HttpInstrumentation } from '@opentelemetry/instrumentation-http';
import { KafkaJsInstrumentation } from '@opentelemetry/instrumentation-kafkajs';
import { NestInstrumentation } from '@opentelemetry/instrumentation-nestjs-core';
import { PgInstrumentation } from '@opentelemetry/instrumentation-pg';
import { UndiciInstrumentation } from '@opentelemetry/instrumentation-undici';
import { BatchLogRecordProcessor } from '@opentelemetry/sdk-logs';
import { PeriodicExportingMetricReader } from '@opentelemetry/sdk-metrics';
import { NodeSDK } from '@opentelemetry/sdk-node';

/**
 * OpenTelemetry bootstrap. Must be the first import of an entrypoint so instrumentations
 * can patch modules as they are required. No-op unless OTEL_EXPORTER_OTLP_ENDPOINT is set.
 */
export const otelSdk = process.env.OTEL_EXPORTER_OTLP_ENDPOINT
  ? new NodeSDK({
      serviceName: process.env.OTEL_SERVICE_NAME ?? 'commercial-data-manager',
      traceExporter: new OTLPTraceExporter(),
      metricReaders: [
        new PeriodicExportingMetricReader({ exporter: new OTLPMetricExporter(), exportIntervalMillis: 5_000 }),
      ],
      logRecordProcessors: [new BatchLogRecordProcessor({ exporter: new OTLPLogExporter() })],
      instrumentations: [
        new HttpInstrumentation({ ignoreIncomingRequestHook: (req) => req.url === '/health' }),
        new ExpressInstrumentation(),
        new NestInstrumentation(),
        // Pollers query every 500 ms: only trace DB/HTTP calls that belong to real work.
        new PgInstrumentation({ requireParentSpan: true }),
        new UndiciInstrumentation({ requireParentforSpans: true }),
        new KafkaJsInstrumentation(),
      ],
    })
  : undefined;

otelSdk?.start();
