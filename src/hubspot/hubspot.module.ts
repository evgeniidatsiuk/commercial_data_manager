import { Module } from '@nestjs/common';
import { HubSpotSyncService } from './hubspot-sync.service';
import { HubSpotClient } from './hubspot.client';
import { WebhookController } from './webhook.controller';
import { WebhookService } from './webhook.service';

@Module({
  controllers: [WebhookController],
  providers: [HubSpotClient, HubSpotSyncService, WebhookService],
  exports: [HubSpotSyncService],
})
export class HubSpotModule {}
