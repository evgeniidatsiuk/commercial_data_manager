import { BeforeApplicationShutdown, Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { PollingLoop } from '../common/polling-loop';
import { KafkaService } from '../kafka/kafka.service';
import { cdmMetrics } from '../observability/metrics';
import { DatabaseService } from '../persistence/database.service';
import { OutboxRepository } from '../persistence/outbox.repository';

const BATCH_SIZE = 100;
const IDLE_MS = 500;

/** Publishes outbox rows to Kafka (at-least-once; consumers dedupe by eventId). */
@Injectable()
export class OutboxRelay implements OnApplicationBootstrap, BeforeApplicationShutdown {
  private readonly logger = new Logger(OutboxRelay.name);
  private readonly loop = new PollingLoop(() => this.tick(), IDLE_MS, this.logger);

  constructor(
    private readonly db: DatabaseService,
    private readonly outbox: OutboxRepository,
    private readonly kafka: KafkaService,
  ) {}

  onApplicationBootstrap(): void {
    this.loop.start();
  }

  async beforeApplicationShutdown(): Promise<void> {
    await this.loop.stop();
  }

  private tick(): Promise<number> {
    return this.db.transaction(async (tx) => {
      const rows = await this.outbox.lockUnpublished(tx, BATCH_SIZE);
      if (rows.length === 0) return 0;

      await this.kafka.send(
        rows.map((r) => ({ topic: r.topic, key: r.key, value: JSON.stringify(r.payload) })),
      );
      await this.outbox.markPublished(
        tx,
        rows.map((r) => r.id),
      );
      cdmMetrics.outboxPublished.add(rows.length);
      this.logger.log(`Published ${rows.length} outbox message(s)`);
      return rows.length;
    });
  }
}
