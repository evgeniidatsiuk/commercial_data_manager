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
import { KafkaService } from '../kafka/kafka.service';

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

/** Simulates upstream Legend services by producing events to Kafka. Disabled unless DEV_ENDPOINTS_ENABLED. */
@Controller('dev/events')
@UseGuards(DevEndpointsGuard)
export class DevController {
  constructor(private readonly kafka: KafkaService) {}

  @Post('affiliate')
  @HttpCode(202)
  publishAffiliate(@Body(new ZodValidationPipe(affiliateBody)) body: z.infer<typeof affiliateBody>) {
    const { eventId, occurredAt, ...data } = body;
    return this.publish(Topics.AffiliateEvents, 'affiliate.upserted', data.affiliateId, data, eventId, occurredAt);
  }

  @Post('deal')
  @HttpCode(202)
  publishDeal(@Body(new ZodValidationPipe(dealBody)) body: z.infer<typeof dealBody>) {
    const { eventId, occurredAt, ...data } = body;
    return this.publish(Topics.DealEvents, 'deal.upserted', data.dealId, data, eventId, occurredAt);
  }

  private async publish(
    topic: string,
    type: string,
    key: string,
    data: unknown,
    eventId: string = randomUUID(),
    occurredAt: string = new Date().toISOString(),
  ) {
    const event = { eventId, type, occurredAt, source: 'cdm-dev-endpoint', data };
    await this.kafka.send([{ topic, key, value: JSON.stringify(event) }]);
    return { topic, event };
  }
}
