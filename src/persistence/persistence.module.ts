import { Global, Module } from '@nestjs/common';
import { CommercialRepository } from './commercial.repository';
import { DatabaseService } from './database.service';
import { InboxRepository } from './inbox.repository';
import { OutboxRepository } from './outbox.repository';
import { SyncStateRepository } from './sync-state.repository';

const providers = [DatabaseService, CommercialRepository, SyncStateRepository, InboxRepository, OutboxRepository];

@Global()
@Module({ providers, exports: providers })
export class PersistenceModule {}
