import { Injectable, Logger } from '@nestjs/common';
import type { DealStageChanged, HubSpotWebhookEvent } from '../contracts/events';
import { Topics } from '../contracts/topics';
import { cdmMetrics } from '../observability/metrics';
import { CommercialRepository } from '../persistence/commercial.repository';
import { DatabaseService } from '../persistence/database.service';
import { InboxRepository } from '../persistence/inbox.repository';
import { OutboxRepository } from '../persistence/outbox.repository';

@Injectable()
export class WebhookService {
  private readonly logger = new Logger(WebhookService.name);

  constructor(
    private readonly db: DatabaseService,
    private readonly inbox: InboxRepository,
    private readonly outbox: OutboxRepository,
    private readonly commercial: CommercialRepository,
  ) {}

  /** HubSpot retries on non-2xx, so every event is deduplicated via the inbox. */
  async handle(events: HubSpotWebhookEvent[]): Promise<void> {
    for (const event of events) {
      const result = await this.db.transaction(async (tx) => {
        if (!(await this.inbox.tryRecord(tx, `hubspot:${event.eventId}`, 'hubspot-webhook'))) return 'duplicate';
        if (event.subscriptionType !== 'deal.propertyChange' || event.propertyName !== 'dealstage') return 'ignored';
        if (!event.propertyValue) return 'ignored';

        const deal = await this.commercial.setStageByHubSpotId(tx, String(event.objectId), event.propertyValue);
        if (!deal) {
          this.logger.warn(`Ignoring stage change for unknown HubSpot deal ${event.objectId}`);
          return 'unknown_deal';
        }

        const message: DealStageChanged = {
          eventId: `hubspot-${event.eventId}`,
          type: 'hubspot.deal.stage-changed',
          occurredAt: new Date(event.occurredAt).toISOString(),
          source: 'commercial-data-manager',
          data: {
            dealId: deal.dealId,
            affiliateId: deal.affiliateId,
            hubspotDealId: String(event.objectId),
            stage: event.propertyValue,
          },
        };
        // Same transaction as the state change: published by OutboxRelay, never lost.
        await this.outbox.enqueue(tx, Topics.HubSpotChanges, deal.dealId, message);
        this.logger.log(`Deal ${deal.dealId} moved to stage ${event.propertyValue} in HubSpot`);
        return 'applied';
      });
      cdmMetrics.webhookEvents.add(1, { subscription_type: event.subscriptionType, result });
    }
  }
}
