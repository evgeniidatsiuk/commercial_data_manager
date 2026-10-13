import { Module } from '@nestjs/common';
import { ApiModule } from './api/api.module';
import { ConfigModule } from './config/config.module';
import { HubSpotModule } from './hubspot/hubspot.module';
import { IngestionModule } from './ingestion/ingestion.module';
import { KafkaModule } from './kafka/kafka.module';
import { ObservabilityModule } from './observability/observability.module';
import { OutboxModule } from './outbox/outbox.module';
import { PersistenceModule } from './persistence/persistence.module';
import { SyncModule } from './sync/sync.module';

@Module({
  imports: [
    ConfigModule,
    PersistenceModule,
    KafkaModule,
    HubSpotModule,
    IngestionModule,
    SyncModule,
    OutboxModule,
    ApiModule,
    ObservabilityModule,
  ],
})
export class AppModule {}
