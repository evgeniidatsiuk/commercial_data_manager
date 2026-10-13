import type { Logger } from '@nestjs/common';
import { setTimeout as sleep } from 'node:timers/promises';

/** Runs `tick` continuously; sleeps only when a tick processed nothing. */
export class PollingLoop {
  private running = false;
  private loop: Promise<void> | null = null;

  constructor(
    private readonly tick: () => Promise<number>,
    private readonly idleMs: number,
    private readonly logger: Logger,
  ) {}

  start(): void {
    this.running = true;
    this.loop = this.run();
  }

  async stop(): Promise<void> {
    this.running = false;
    await this.loop;
  }

  private async run(): Promise<void> {
    while (this.running) {
      let processed = 0;
      try {
        processed = await this.tick();
      } catch (err) {
        this.logger.error(err instanceof Error ? err.stack : String(err));
      }
      if (processed === 0) await sleep(this.idleMs);
    }
  }
}
