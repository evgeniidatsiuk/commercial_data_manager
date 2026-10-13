import {
  BeforeApplicationShutdown,
  Inject,
  Injectable,
  Logger,
  OnApplicationShutdown,
  OnModuleInit,
} from '@nestjs/common';
import { type Consumer, type IHeaders, Kafka, logLevel, Partitioners, type Producer } from 'kafkajs';
import { ENV, type Env } from '../config/env';
import { ALL_TOPICS } from '../contracts/topics';

export interface InboundMessage {
  topic: string;
  partition: number;
  offset: string;
  key: string | null;
  value: string | null;
  headers: Record<string, string>;
}

export interface OutboundMessage {
  topic: string;
  key: string | null;
  value: string;
  headers?: Record<string, string>;
}

const decodeHeaders = (headers: IHeaders | undefined): Record<string, string> =>
  Object.fromEntries(Object.entries(headers ?? {}).map(([k, v]) => [k, String(v ?? '')]));

@Injectable()
export class KafkaService implements OnModuleInit, BeforeApplicationShutdown, OnApplicationShutdown {
  private readonly logger = new Logger(KafkaService.name);
  private readonly kafka: Kafka;
  private readonly producer: Producer;
  private readonly consumers: Consumer[] = [];

  constructor(@Inject(ENV) env: Env) {
    this.kafka = new Kafka({ clientId: env.KAFKA_CLIENT_ID, brokers: env.KAFKA_BROKERS, logLevel: logLevel.WARN });
    this.producer = this.kafka.producer({
      allowAutoTopicCreation: false,
      createPartitioner: Partitioners.DefaultPartitioner,
    });
  }

  async onModuleInit(): Promise<void> {
    await this.ensureTopics();
    await this.producer.connect();
  }

  async send(messages: OutboundMessage[]): Promise<void> {
    await this.producer.sendBatch({
      topicMessages: messages.map(({ topic, ...message }) => ({ topic, messages: [message] })),
    });
  }

  /** Throwing from the handler makes kafkajs retry the message (offset is not committed). */
  async consume(groupId: string, topics: string[], handler: (msg: InboundMessage) => Promise<void>): Promise<void> {
    const consumer = this.kafka.consumer({ groupId });
    this.consumers.push(consumer);
    await consumer.connect();
    await consumer.subscribe({ topics, fromBeginning: true });
    await consumer.run({
      eachMessage: ({ topic, partition, message }) =>
        handler({
          topic,
          partition,
          offset: message.offset,
          key: message.key?.toString() ?? null,
          value: message.value?.toString() ?? null,
          headers: decodeHeaders(message.headers),
        }),
    });
  }

  async beforeApplicationShutdown(): Promise<void> {
    await Promise.all(this.consumers.map((c) => c.disconnect()));
  }

  async onApplicationShutdown(): Promise<void> {
    await this.producer.disconnect();
  }

  private async ensureTopics(): Promise<void> {
    const admin = this.kafka.admin();
    await admin.connect();
    try {
      // Partition count / replication factor fall back to broker defaults.
      const created = await admin.createTopics({ waitForLeaders: true, topics: ALL_TOPICS.map((topic) => ({ topic })) });
      if (created) this.logger.log(`Created topics: ${ALL_TOPICS.join(', ')}`);
    } finally {
      await admin.disconnect();
    }
  }
}
