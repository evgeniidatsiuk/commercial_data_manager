import { Module } from '@nestjs/common';
import { HubSpotModule } from '../hubspot/hubspot.module';
import { SyncWorker } from './sync.worker';

@Module({ imports: [HubSpotModule], providers: [SyncWorker] })
export class SyncModule {}
