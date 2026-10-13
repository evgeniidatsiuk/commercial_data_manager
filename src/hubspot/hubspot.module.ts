import { Module } from '@nestjs/common';
import { HubSpotBulkSyncService } from './hubspot-bulk-sync.service';
import { HubSpotSyncService } from './hubspot-sync.service';
import { HubSpotClient } from './hubspot.client';
import { WebhookController } from './webhook.controller';
import { WebhookService } from './webhook.service';

@Module({
  controllers: [WebhookController],
  providers: [HubSpotClient, HubSpotSyncService, HubSpotBulkSyncService, WebhookService],
  exports: [HubSpotSyncService, HubSpotBulkSyncService],
})
export class HubSpotModule {}
