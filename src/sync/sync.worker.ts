import { BeforeApplicationShutdown, Inject, Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { type Span, SpanStatusCode } from '@opentelemetry/api';
import { PollingLoop } from '../common/polling-loop';
import { ENV, type Env } from '../config/env';
import { HubSpotBulkSyncService } from '../hubspot/hubspot-bulk-sync.service';
import { HubSpotApiError } from '../hubspot/hubspot.client';
import { HubSpotSyncService, NonRetryableSyncError } from '../hubspot/hubspot-sync.service';
import { cdmMetrics, tracer } from '../observability/metrics';
import {
  type ClaimedJob,
  ENTITY_TYPES,
  type EntityType,
  SyncStateRepository,
} from '../persistence/sync-state.repository';

const LEASE_SECONDS = 60;
const MAX_BACKOFF_SECONDS = 300;

type SyncMode = 'single' | 'bulk';
type Failure = { error: unknown } | null;

export const backoffSeconds = (attempt: number): number => Math.min(MAX_BACKOFF_SECONDS, 2 ** attempt);

const isRetryable = (err: unknown): boolean =>
  err instanceof NonRetryableSyncError ? false : err instanceof HubSpotApiError ? err.retryable : true;

const markSpanFailed = (span: Span, err: unknown): void => {
  span.recordException(err instanceof Error ? err : String(err));
  span.setStatus({ code: SpanStatusCode.ERROR, message: err instanceof Error ? err.message : String(err) });
};

/**
 * Pushes pending entities to HubSpot, decoupled from Kafka consumption so HubSpot rate limits never stall ingestion.
 * Small claims use per-record calls (lowest latency); large claims switch to batch APIs (fewest API calls).
 */
@Injectable()
export class SyncWorker implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly logger = new Logger(SyncWorker.name);
  private readonly loop: PollingLoop;

  constructor(
    private readonly syncState: SyncStateRepository,
    private readonly single: HubSpotSyncService,
    private readonly bulk: HubSpotBulkSyncService,
    @Inject(ENV) private readonly env: Env,
  ) {
    this.loop = new PollingLoop(() => this.tick(), env.SYNC_POLL_INTERVAL_MS, this.logger);
  }

  onApplicationBootstrap(): void {
    this.loop.start();
  }

  async beforeApplicationShutdown(): Promise<void> {
    await this.loop.stop();
  }

  private async tick(): Promise<number> {
    const jobs = await this.syncState.claimDue(this.env.SYNC_BATCH_SIZE, LEASE_SECONDS);
    // Affiliates first: deals need the affiliate's HubSpot company.
    for (const entityType of ENTITY_TYPES) {
      const group = jobs.filter((j) => j.entityType === entityType);
      if (group.length >= this.env.SYNC_BULK_THRESHOLD) {
        await this.processBulk(entityType, group);
      } else {
        // Sequential on purpose: keeps us well within HubSpot's per-second limits.
        for (const job of group) await this.processSingle(job);
      }
    }
    return jobs.length;
  }

  private processSingle(job: ClaimedJob): Promise<void> {
    const attributes = {
      'cdm.entity_type': job.entityType,
      'cdm.entity_id': job.entityId,
      'cdm.version': job.version,
      'cdm.attempt': job.attempts,
    };
    return tracer.startActiveSpan(`sync ${job.entityType}`, { attributes }, async (span) => {
      let failure: Failure = null;
      try {
        await this.single.sync(job.entityType, job.entityId);
      } catch (error) {
        markSpanFailed(span, error);
        failure = { error };
      }
      try {
        await this.complete(job, 'single', failure);
      } finally {
        span.end();
      }
    });
  }

  private processBulk(entityType: EntityType, jobs: ClaimedJob[]): Promise<void> {
    const attributes = { 'cdm.entity_type': entityType, 'cdm.batch_size': jobs.length };
    return tracer.startActiveSpan(`bulk sync ${entityType}`, { attributes }, async (span) => {
      cdmMetrics.syncBatchSize.record(jobs.length, { entity_type: entityType });
      let failures: Map<string, unknown>;
      try {
        failures = await this.bulk.sync(
          entityType,
          jobs.map((j) => j.entityId),
        );
      } catch (error) {
        markSpanFailed(span, error);
        failures = new Map(jobs.map((j) => [j.entityId, error]));
      }
      try {
        for (const job of jobs) {
          await this.complete(job, 'bulk', failures.has(job.entityId) ? { error: failures.get(job.entityId) } : null);
        }
        span.setAttribute('cdm.failed', failures.size);
        this.logger.log(`Bulk synced ${jobs.length - failures.size}/${jobs.length} ${entityType}(s)`);
      } finally {
        span.end();
      }
    });
  }

  private async complete(job: ClaimedJob, mode: SyncMode, failure: Failure): Promise<void> {
    const ref = `${job.entityType}:${job.entityId}@v${job.version}`;
    const labels = { entity_type: job.entityType, mode };

    if (!failure) {
      await this.syncState.markSynced(job);
      cdmMetrics.syncResults.add(1, { ...labels, result: 'synced' });
      if (mode === 'single') this.logger.log(`Synced ${ref}`);
      return;
    }

    const { error } = failure;
    const message = error instanceof Error ? error.message : String(error);
    const retryIn = isRetryable(error) && job.attempts < this.env.SYNC_MAX_ATTEMPTS ? backoffSeconds(job.attempts) : null;
    await this.syncState.markFailed(job, message, retryIn);
    cdmMetrics.syncResults.add(1, { ...labels, result: retryIn === null ? 'failed' : 'retry' });
    this.logger.warn(
      `Sync ${ref} (${mode}) attempt ${job.attempts} failed (${retryIn === null ? 'giving up' : `retry in ${retryIn}s`}): ${message}`,
    );
  }
}
