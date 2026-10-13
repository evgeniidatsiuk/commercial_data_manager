import {
  Body,
  CanActivate,
  Controller,
  HttpCode,
  Inject,
  Injectable,
  NotFoundException,
  Post,
  UseGuards,
} from '@nestjs/common';
import { randomUUID } from 'node:crypto';
import { z } from 'zod';
import { ZodValidationPipe } from '../common/zod-validation.pipe';
import { ENV, type Env } from '../config/env';
import { affiliateDataSchema, dealDataSchema } from '../contracts/events';
import { Topics } from '../contracts/topics';
import { KafkaService, type OutboundMessage } from '../kafka/kafka.service';

@Injectable()
class DevEndpointsGuard implements CanActivate {
  constructor(@Inject(ENV) private readonly env: Env) {}

  canActivate(): boolean {
    if (!this.env.DEV_ENDPOINTS_ENABLED) throw new NotFoundException();
    return true;
  }
}

const meta = z.object({
  eventId: z.string().min(1).optional(),
  occurredAt: z.string().datetime({ offset: true }).optional(),
});
const affiliateBody = affiliateDataSchema.merge(meta);
const dealBody = dealDataSchema.merge(meta);
const seedBody = z.object({
  affiliates: z.number().int().min(1).max(5000),
  dealsPerAffiliate: z.number().int().min(0).max(5).default(1),
});

const TIERS = ['bronze', 'silver', 'gold', 'platinum'] as const;
const COMMISSION_MODELS = ['CPA', 'REVSHARE', 'HYBRID'] as const;
const KAFKA_CHUNK = 500;

/** Simulates upstream Legend services by producing events to Kafka. Disabled unless DEV_ENDPOINTS_ENABLED. */
@Controller('dev')
@UseGuards(DevEndpointsGuard)
export class DevController {
  constructor(private readonly kafka: KafkaService) {}

  @Post('events/affiliate')
  @HttpCode(202)
  async publishAffiliate(@Body(new ZodValidationPipe(affiliateBody)) body: z.infer<typeof affiliateBody>) {
    const { eventId, occurredAt, ...data } = body;
    const message = this.event(Topics.AffiliateEvents, 'affiliate.upserted', data.affiliateId, data, eventId, occurredAt);
    await this.kafka.send([message]);
    return { topic: message.topic, event: JSON.parse(message.value) };
  }

  @Post('events/deal')
  @HttpCode(202)
  async publishDeal(@Body(new ZodValidationPipe(dealBody)) body: z.infer<typeof dealBody>) {
    const { eventId, occurredAt, ...data } = body;
    const message = this.event(Topics.DealEvents, 'deal.upserted', data.dealId, data, eventId, occurredAt);
    await this.kafka.send([message]);
    return { topic: message.topic, event: JSON.parse(message.value) };
  }

  /** Generates a burst of affiliates + deals to exercise backlog handling and the bulk sync path. */
  @Post('seed')
  @HttpCode(202)
  async seed(@Body(new ZodValidationPipe(seedBody)) body: z.infer<typeof seedBody>) {
    const prefix = `seed-${randomUUID().slice(0, 8)}`;
    const messages: OutboundMessage[] = [];
    for (let i = 0; i < body.affiliates; i++) {
      const affiliateId = `${prefix}-aff-${i}`;
      messages.push(
        this.event(Topics.AffiliateEvents, 'affiliate.upserted', affiliateId, {
          affiliateId,
          companyName: `Seed Affiliate ${i}`,
          domain: `${affiliateId}.example.com`,
          contact: { email: `${affiliateId}@example.com`, firstName: 'Seed', lastName: String(i) },
          tier: TIERS[i % TIERS.length],
        }),
      );
      for (let d = 0; d < body.dealsPerAffiliate; d++) {
        const dealId = `${prefix}-deal-${i}-${d}`;
        messages.push(
          this.event(Topics.DealEvents, 'deal.upserted', dealId, {
            dealId,
            affiliateId,
            name: `Seed deal ${i}-${d}`,
            commissionModel: COMMISSION_MODELS[(i + d) % COMMISSION_MODELS.length],
            amount: 100 + i,
          }),
        );
      }
    }
    for (let i = 0; i < messages.length; i += KAFKA_CHUNK) {
      await this.kafka.send(messages.slice(i, i + KAFKA_CHUNK));
    }
    return { prefix, affiliates: body.affiliates, deals: body.affiliates * body.dealsPerAffiliate };
  }

  private event(
    topic: string,
    type: string,
    key: string,
    data: unknown,
    eventId: string = randomUUID(),
    occurredAt: string = new Date().toISOString(),
  ): OutboundMessage {
    const event = { eventId, type, occurredAt, source: 'cdm-dev-endpoint', data };
    return { topic, key, value: JSON.stringify(event) };
  }
}
