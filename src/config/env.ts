import { z } from 'zod';

const bool = z.enum(['true', 'false']).transform((v) => v === 'true');

const envSchema = z.object({
  PORT: z.coerce.number().int().positive().default(3000),
  DATABASE_URL: z.string().url(),
  KAFKA_BROKERS: z
    .string()
    .min(1)
    .transform((v) => v.split(',').map((b) => b.trim())),
  KAFKA_CLIENT_ID: z.string().default('commercial-data-manager'),
  KAFKA_GROUP_ID: z.string().default('cdm-ingestion'),
  HUBSPOT_BASE_URL: z.string().url().default('https://api.hubapi.com'),
  HUBSPOT_ACCESS_TOKEN: z.string().min(1),
  HUBSPOT_CLIENT_SECRET: z.string().min(1),
  HUBSPOT_ENSURE_PROPERTIES: bool.default('true'),
  // Set when running behind a proxy so the webhook signature URI matches what HubSpot called.
  WEBHOOK_PUBLIC_BASE_URL: z.string().url().optional(),
  SYNC_POLL_INTERVAL_MS: z.coerce.number().int().positive().default(500),
  SYNC_BATCH_SIZE: z.coerce.number().int().positive().default(100),
  // Claims with at least this many entities of one type use HubSpot batch APIs.
  SYNC_BULK_THRESHOLD: z.coerce.number().int().positive().default(10),
  SYNC_MAX_ATTEMPTS: z.coerce.number().int().positive().default(8),
  DEV_ENDPOINTS_ENABLED: bool.default('false'),
});

export type Env = z.infer<typeof envSchema>;

export const ENV = Symbol('ENV');

export const loadEnv = (): Env => envSchema.parse(process.env);
