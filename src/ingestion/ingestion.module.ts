import { Module } from '@nestjs/common';
import { IngestionConsumer } from './ingestion.consumer';
import { IngestionService } from './ingestion.service';

@Module({ providers: [IngestionService, IngestionConsumer] })
export class IngestionModule {}
