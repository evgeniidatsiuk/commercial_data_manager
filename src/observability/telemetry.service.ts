import { Injectable, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { ENTITY_TYPES, SYNC_STATUSES, SyncStateRepository } from '../persistence/sync-state.repository';
import { meter } from './metrics';
import { otelSdk } from './otel';

/** Exposes DB-derived gauges (backlog, lag) and flushes telemetry on shutdown. */
@Injectable()
export class TelemetryService implements OnModuleInit, OnApplicationShutdown {
  constructor(private readonly syncState: SyncStateRepository) {}

  onModuleInit(): void {
    const entities = meter.createObservableGauge('cdm.sync.entities', {
      description: 'Entities by sync status',
    });
    const lag = meter.createObservableGauge('cdm.sync.lag', {
      unit: 's',
      description: 'Age of the oldest change not yet pushed to HubSpot (main SLI)',
    });
    meter.addBatchObservableCallback(
      async (result) => {
        const stats = await this.syncState.stats();
        for (const entityType of ENTITY_TYPES) {
          for (const status of SYNC_STATUSES) {
            result.observe(entities, stats.byEntityType[entityType][status], { entity_type: entityType, status });
          }
        }
        result.observe(lag, stats.lagSeconds);
      },
      [entities, lag],
    );
  }

  async onApplicationShutdown(): Promise<void> {
    await otelSdk?.shutdown();
  }
}
