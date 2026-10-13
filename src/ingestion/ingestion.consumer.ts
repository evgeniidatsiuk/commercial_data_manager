import { Inject, Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { ENV, type Env } from '../config/env';
import { affiliateUpsertedSchema, dealUpsertedSchema, type InboundEvent } from '../contracts/events';
import { Topics } from '../contracts/topics';
import { type InboundMessage, KafkaService } from '../kafka/kafka.service';
import { IngestionService } from './ingestion.service';

const schemaByTopic = {
  [Topics.AffiliateEvents]: affiliateUpsertedSchema,
  [Topics.DealEvents]: dealUpsertedSchema,
} as const;

type ParseResult = { ok: true; event: InboundEvent } | { ok: false; reason: string };

@Injectable()
export class IngestionConsumer implements OnApplicationBootstrap {
  private readonly logger = new Logger(IngestionConsumer.name);

  constructor(
    private readonly kafka: KafkaService,
    private readonly ingestion: IngestionService,
    @Inject(ENV) private readonly env: Env,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    await this.kafka.consume(this.env.KAFKA_GROUP_ID, Object.keys(schemaByTopic), (msg) => this.handle(msg));
  }

  async handle(msg: InboundMessage): Promise<void> {
    const parsed = this.parse(msg);
    if (!parsed.ok) {
      // Poison messages go to the DLQ instead of blocking the partition.
      this.logger.warn(`Dead-lettering ${msg.topic}[${msg.partition}]@${msg.offset}: ${parsed.reason}`);
      await this.kafka.send([
        {
          topic: Topics.DeadLetter,
          key: msg.key,
          value: msg.value ?? '',
          headers: {
            ...msg.headers,
            'x-dlq-reason': parsed.reason.slice(0, 1000),
            'x-original-topic': msg.topic,
            'x-original-partition': String(msg.partition),
            'x-original-offset': msg.offset,
          },
        },
      ]);
      return;
    }

    // Infrastructure errors (DB down) propagate so kafkajs retries without committing the offset.
    const result = await this.ingestion.apply(parsed.event);
    this.logger.log(`${parsed.event.type} ${parsed.event.eventId}: ${result}`);
  }

  private parse(msg: InboundMessage): ParseResult {
    const schema = schemaByTopic[msg.topic as keyof typeof schemaByTopic];
    if (!schema) return { ok: false, reason: `Unexpected topic ${msg.topic}` };

    let json: unknown;
    try {
      json = JSON.parse(msg.value ?? '');
    } catch {
      return { ok: false, reason: 'Payload is not valid JSON' };
    }
    const result = schema.safeParse(json);
    return result.success ? { ok: true, event: result.data } : { ok: false, reason: result.error.message };
  }
}
