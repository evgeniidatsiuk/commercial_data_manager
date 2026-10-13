import { BeforeApplicationShutdown, Inject, Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { PollingLoop } from '../common/polling-loop';
import { ENV, type Env } from '../config/env';
import { HubSpotApiError } from '../hubspot/hubspot.client';
import { HubSpotSyncService, NonRetryableSyncError } from '../hubspot/hubspot-sync.service';
import { type ClaimedJob, SyncStateRepository } from '../persistence/sync-state.repository';

const LEASE_SECONDS = 60;
const MAX_BACKOFF_SECONDS = 300;

export const backoffSeconds = (attempt: number): number => Math.min(MAX_BACKOFF_SECONDS, 2 ** attempt);

const isRetryable = (err: unknown): boolean =>
  err instanceof NonRetryableSyncError ? false : err instanceof HubSpotApiError ? err.retryable : true;

/** Pushes pending entities to HubSpot, decoupled from Kafka consumption so HubSpot rate limits never stall ingestion. */
@Injectable()
export class SyncWorker implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly logger = new Logger(SyncWorker.name);
  private readonly loop: PollingLoop;

  constructor(
    private readonly syncState: SyncStateRepository,
    private readonly hubspot: HubSpotSyncService,
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
    // Sequential on purpose: keeps us well within HubSpot's per-second limits.
    for (const job of jobs) await this.process(job);
    return jobs.length;
  }

  private async process(job: ClaimedJob): Promise<void> {
    const ref = `${job.entityType}:${job.entityId}@v${job.version}`;
    try {
      await this.hubspot.sync(job.entityType, job.entityId);
      await this.syncState.markSynced(job);
      this.logger.log(`Synced ${ref}`);
    } catch (err) {
      const message = err instanceof Error ? err.message : String(err);
      const retryIn = isRetryable(err) && job.attempts < this.env.SYNC_MAX_ATTEMPTS ? backoffSeconds(job.attempts) : null;
      await this.syncState.markFailed(job, message, retryIn);
      this.logger.warn(
        `Sync ${ref} attempt ${job.attempts} failed (${retryIn === null ? 'giving up' : `retry in ${retryIn}s`}): ${message}`,
      );
    }
  }
}
