import { Body, Controller, Get, HttpCode, NotFoundException, Param, Post, Query } from '@nestjs/common';
import { z } from 'zod';
import { ZodValidationPipe } from '../common/zod-validation.pipe';
import { ENTITY_TYPES, type EntityType, SYNC_STATUSES, SyncStateRepository } from '../persistence/sync-state.repository';

const listQuerySchema = z.object({
  status: z.enum(SYNC_STATUSES).optional(),
  limit: z.coerce.number().int().min(1).max(500).default(50),
});

const resyncBodySchema = z.object({ entityType: z.enum(ENTITY_TYPES).optional() });

@Controller('sync')
export class SyncController {
  constructor(private readonly syncState: SyncStateRepository) {}

  @Get()
  list(@Query(new ZodValidationPipe(listQuerySchema)) query: z.infer<typeof listQuerySchema>) {
    return this.syncState.list(query.status, query.limit);
  }

  @Get('stats')
  stats() {
    return this.syncState.stats();
  }

  /** Backfill: re-push everything; the worker drains the backlog through HubSpot batch APIs. */
  @Post('resync')
  @HttpCode(202)
  async resync(@Body(new ZodValidationPipe(resyncBodySchema)) body: z.infer<typeof resyncBodySchema>) {
    return { entityType: body.entityType ?? 'all', requested: await this.syncState.resync(body.entityType) };
  }

  @Post(':entityType/:entityId/retry')
  @HttpCode(202)
  async retry(
    @Param('entityType', new ZodValidationPipe(z.enum(ENTITY_TYPES))) entityType: EntityType,
    @Param('entityId') entityId: string,
  ) {
    if (!(await this.syncState.retry(entityType, entityId))) {
      throw new NotFoundException(`No failed sync for ${entityType}:${entityId}`);
    }
    return { entityType, entityId, status: 'pending' };
  }
}
