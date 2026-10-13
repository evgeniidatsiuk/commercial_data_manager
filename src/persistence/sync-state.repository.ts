import { Injectable } from '@nestjs/common';
import { DatabaseService, type Queryable } from './database.service';

export const ENTITY_TYPES = ['affiliate', 'deal'] as const;
export type EntityType = (typeof ENTITY_TYPES)[number];

export const SYNC_STATUSES = ['pending', 'synced', 'failed'] as const;
export type SyncStatus = (typeof SYNC_STATUSES)[number];

export interface SyncState {
  entityType: EntityType;
  entityId: string;
  status: SyncStatus;
  desiredVersion: number;
  syncedVersion: number;
  attempts: number;
  nextAttemptAt: Date;
  lastError: string | null;
  lastSyncedAt: Date | null;
}

export interface ClaimedJob {
  entityType: EntityType;
  entityId: string;
  version: number;
  attempts: number;
}

export interface SyncStats {
  totals: Record<SyncStatus, number>;
  byEntityType: Record<EntityType, Record<SyncStatus, number>>;
  lagSeconds: number;
}

const emptyCounts = (): Record<SyncStatus, number> => ({ pending: 0, synced: 0, failed: 0 });

const COLUMNS = `
  entity_type AS "entityType", entity_id AS "entityId", status,
  desired_version::int AS "desiredVersion", synced_version::int AS "syncedVersion", attempts,
  next_attempt_at AS "nextAttemptAt", last_error AS "lastError", last_synced_at AS "lastSyncedAt"`;

@Injectable()
export class SyncStateRepository {
  constructor(private readonly db: DatabaseService) {}

  /** Marks the entity as needing a push to HubSpot. Multiple changes coalesce into one sync. */
  async requestSync(tx: Queryable, entityType: EntityType, entityId: string): Promise<void> {
    await tx.query(
      `INSERT INTO sync_state (entity_type, entity_id) VALUES ($1, $2)
       ON CONFLICT (entity_type, entity_id) DO UPDATE SET
         desired_version = sync_state.desired_version + 1,
         requested_at = CASE WHEN sync_state.status = 'pending' THEN sync_state.requested_at ELSE now() END,
         status = 'pending', attempts = 0, next_attempt_at = now(), last_error = NULL, updated_at = now()`,
      [entityType, entityId],
    );
  }

  /** Backfill: re-push every entity (optionally of one type) to HubSpot. */
  async resync(entityType: EntityType | undefined): Promise<number> {
    const rows = await this.db.query(
      `UPDATE sync_state SET
         desired_version = desired_version + 1,
         requested_at = CASE WHEN status = 'pending' THEN requested_at ELSE now() END,
         status = 'pending', attempts = 0, next_attempt_at = now(), last_error = NULL, updated_at = now()
       WHERE $1::text IS NULL OR entity_type = $1
       RETURNING entity_id`,
      [entityType ?? null],
    );
    return rows.length;
  }

  async stats(): Promise<SyncStats> {
    const rows = await this.db.query<{ entityType: EntityType; status: SyncStatus; count: number; oldest: number }>(
      `SELECT entity_type AS "entityType", status, count(*)::int AS count,
              EXTRACT(EPOCH FROM now() - min(requested_at))::float8 AS oldest
       FROM sync_state GROUP BY entity_type, status`,
    );
    const stats: SyncStats = {
      totals: emptyCounts(),
      byEntityType: { affiliate: emptyCounts(), deal: emptyCounts() },
      lagSeconds: 0,
    };
    for (const r of rows) {
      stats.totals[r.status] += r.count;
      stats.byEntityType[r.entityType][r.status] = r.count;
      if (r.status === 'pending') stats.lagSeconds = Math.max(stats.lagSeconds, r.oldest);
    }
    return stats;
  }

  /** Leases due jobs; an expired lease (crashed worker) makes the job claimable again. */
  claimDue(limit: number, leaseSeconds: number): Promise<ClaimedJob[]> {
    return this.db.query<ClaimedJob>(
      `WITH due AS (
         SELECT entity_type, entity_id FROM sync_state
         WHERE status = 'pending' AND next_attempt_at <= now()
           AND (locked_until IS NULL OR locked_until < now())
         ORDER BY next_attempt_at
         LIMIT $1
         FOR UPDATE SKIP LOCKED
       )
       UPDATE sync_state s
       SET locked_until = now() + make_interval(secs => $2), attempts = s.attempts + 1, updated_at = now()
       FROM due
       WHERE s.entity_type = due.entity_type AND s.entity_id = due.entity_id
       RETURNING s.entity_type AS "entityType", s.entity_id AS "entityId",
                 s.desired_version::int AS "version", s.attempts`,
      [limit, leaseSeconds],
    );
  }

  async markSynced(job: ClaimedJob): Promise<void> {
    // If a newer change arrived while syncing, desired_version > job.version and the row stays pending.
    await this.db.query(
      `UPDATE sync_state SET
         synced_version = GREATEST(synced_version, $3),
         status = CASE WHEN desired_version > $3 THEN 'pending' ELSE 'synced' END,
         attempts = 0, locked_until = NULL, last_error = NULL, last_synced_at = now(), updated_at = now()
       WHERE entity_type = $1 AND entity_id = $2`,
      [job.entityType, job.entityId, job.version],
    );
  }

  /** retryInSeconds = null means give up (status = failed) until manually retried. */
  async markFailed(job: ClaimedJob, error: string, retryInSeconds: number | null): Promise<void> {
    await this.db.query(
      `UPDATE sync_state SET
         status = CASE WHEN $4::int IS NULL THEN 'failed' ELSE 'pending' END,
         next_attempt_at = now() + make_interval(secs => COALESCE($4::int, 0)),
         locked_until = NULL, last_error = $3, updated_at = now()
       WHERE entity_type = $1 AND entity_id = $2`,
      [job.entityType, job.entityId, error.slice(0, 2000), retryInSeconds],
    );
  }

  async retry(entityType: EntityType, entityId: string): Promise<boolean> {
    const rows = await this.db.query(
      `UPDATE sync_state SET status = 'pending', attempts = 0, next_attempt_at = now(), requested_at = now(), updated_at = now()
       WHERE entity_type = $1 AND entity_id = $2 AND status = 'failed'
       RETURNING entity_id`,
      [entityType, entityId],
    );
    return rows.length > 0;
  }

  async get(entityType: EntityType, entityId: string): Promise<SyncState | null> {
    const [row] = await this.db.query<SyncState>(
      `SELECT ${COLUMNS} FROM sync_state WHERE entity_type = $1 AND entity_id = $2`,
      [entityType, entityId],
    );
    return row ?? null;
  }

  list(status: SyncStatus | undefined, limit: number): Promise<SyncState[]> {
    return this.db.query<SyncState>(
      `SELECT ${COLUMNS} FROM sync_state
       WHERE $1::text IS NULL OR status = $1
       ORDER BY updated_at DESC
       LIMIT $2`,
      [status ?? null, limit],
    );
  }
}
