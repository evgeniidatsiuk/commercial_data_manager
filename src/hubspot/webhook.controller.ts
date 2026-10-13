import {
  BadRequestException,
  Controller,
  Headers,
  HttpCode,
  Inject,
  Post,
  type RawBodyRequest,
  Req,
  UnauthorizedException,
} from '@nestjs/common';
import type { Request } from 'express';
import { ENV, type Env } from '../config/env';
import { hubspotWebhookPayloadSchema } from '../contracts/events';
import { cdmMetrics } from '../observability/metrics';
import { verifyHubSpotV3 } from './webhook-signature';
import { WebhookService } from './webhook.service';

@Controller('webhooks/hubspot')
export class WebhookController {
  constructor(
    private readonly webhooks: WebhookService,
    @Inject(ENV) private readonly env: Env,
  ) {}

  @Post()
  @HttpCode(204)
  async receive(
    @Req() req: RawBodyRequest<Request>,
    @Headers('x-hubspot-signature-v3') signature?: string,
    @Headers('x-hubspot-request-timestamp') timestamp?: string,
  ): Promise<void> {
    const baseUrl = this.env.WEBHOOK_PUBLIC_BASE_URL ?? `${req.protocol}://${req.get('host')}`;
    const valid = verifyHubSpotV3(this.env.HUBSPOT_CLIENT_SECRET, {
      method: req.method,
      uri: `${baseUrl}${req.originalUrl}`,
      body: req.rawBody?.toString('utf8') ?? '',
      timestamp,
      signature,
    });
    if (!valid) {
      cdmMetrics.webhookEvents.add(1, { subscription_type: 'unknown', result: 'invalid_signature' });
      throw new UnauthorizedException('Invalid HubSpot signature');
    }

    const payload = hubspotWebhookPayloadSchema.safeParse(req.body);
    if (!payload.success) throw new BadRequestException(payload.error.flatten());

    await this.webhooks.handle(payload.data);
  }
}
