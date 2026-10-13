import { Module } from '@nestjs/common';
import { OutboxRelay } from './outbox.relay';

@Module({ providers: [OutboxRelay] })
export class OutboxModule {}
