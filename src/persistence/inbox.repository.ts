import { Injectable } from '@nestjs/common';
import type { Queryable } from './database.service';

@Injectable()
export class InboxRepository {
  /** Returns false if the event was already processed (duplicate delivery). */
  async tryRecord(tx: Queryable, eventId: string, source: string): Promise<boolean> {
    const rows = await tx.query(
      `INSERT INTO processed_events (event_id, source) VALUES ($1, $2)
       ON CONFLICT (event_id) DO NOTHING
       RETURNING event_id`,
      [eventId, source],
    );
    return rows.length > 0;
  }
}
