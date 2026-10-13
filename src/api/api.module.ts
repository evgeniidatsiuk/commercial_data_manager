import { Module } from '@nestjs/common';
import { CommercialController } from './commercial.controller';
import { DevController } from './dev.controller';
import { HealthController } from './health.controller';
import { SyncController } from './sync.controller';

@Module({ controllers: [HealthController, CommercialController, SyncController, DevController] })
export class ApiModule {}
