import { Injectable } from '@nestjs/common';
import type { Queryable } from './database.service';

export interface OutboxMessage {
  id: string;
  topic: string;
  key: string | null;
  payload: unknown;
}

@Injectable()
export class OutboxRepository {
  async enqueue(tx: Queryable, topic: string, key: string | null, payload: unknown): Promise<void> {
    await tx.query('INSERT INTO outbox (topic, message_key, payload) VALUES ($1, $2, $3)', [
      topic,
      key,
      JSON.stringify(payload),
    ]);
  }

  lockUnpublished(tx: Queryable, limit: number): Promise<OutboxMessage[]> {
    return tx.query<OutboxMessage>(
      `SELECT id, topic, message_key AS key, payload FROM outbox
       WHERE published_at IS NULL
       ORDER BY id
       LIMIT $1
       FOR UPDATE SKIP LOCKED`,
      [limit],
    );
  }

  async markPublished(tx: Queryable, ids: string[]): Promise<void> {
    await tx.query('UPDATE outbox SET published_at = now() WHERE id = ANY($1::bigint[])', [ids]);
  }
}
