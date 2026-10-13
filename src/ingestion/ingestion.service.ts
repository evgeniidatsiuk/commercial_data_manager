import { Injectable } from '@nestjs/common';
import type { InboundEvent } from '../contracts/events';
import { CommercialRepository } from '../persistence/commercial.repository';
import { DatabaseService } from '../persistence/database.service';
import { InboxRepository } from '../persistence/inbox.repository';
import { SyncStateRepository } from '../persistence/sync-state.repository';

export type IngestionResult = 'applied' | 'duplicate' | 'stale';

@Injectable()
export class IngestionService {
  constructor(
    private readonly db: DatabaseService,
    private readonly inbox: InboxRepository,
    private readonly commercial: CommercialRepository,
    private readonly syncState: SyncStateRepository,
  ) {}

  /** Inbox + projection + sync request in one transaction: exactly-once effect on top of at-least-once Kafka. */
  apply(event: InboundEvent): Promise<IngestionResult> {
    return this.db.transaction(async (tx) => {
      if (!(await this.inbox.tryRecord(tx, event.eventId, event.source))) return 'duplicate';

      if (event.type === 'affiliate.upserted') {
        if (!(await this.commercial.upsertAffiliate(tx, event.data, event.occurredAt))) return 'stale';
        await this.syncState.requestSync(tx, 'affiliate', event.data.affiliateId);
      } else {
        if (!(await this.commercial.upsertDeal(tx, event.data, event.occurredAt))) return 'stale';
        await this.syncState.requestSync(tx, 'deal', event.data.dealId);
      }
      return 'applied';
    });
  }
}
