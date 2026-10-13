import { Inject, Injectable, Logger, OnApplicationShutdown, OnModuleInit } from '@nestjs/common';
import { readdir, readFile } from 'node:fs/promises';
import { join } from 'node:path';
import { Pool, type PoolClient, type QueryResultRow } from 'pg';
import { ENV, type Env } from '../config/env';

const MIGRATIONS_LOCK_ID = 7_340_001;

export interface Queryable {
  query<T extends QueryResultRow = QueryResultRow>(sql: string, params?: unknown[]): Promise<T[]>;
}

const wrap = (client: Pool | PoolClient): Queryable => ({
  query: async <T extends QueryResultRow>(sql: string, params: unknown[] = []) =>
    (await client.query<T>(sql, params)).rows,
});

@Injectable()
export class DatabaseService implements Queryable, OnModuleInit, OnApplicationShutdown {
  private readonly logger = new Logger(DatabaseService.name);
  private readonly pool: Pool;
  private readonly queryable: Queryable;

  constructor(@Inject(ENV) env: Env) {
    this.pool = new Pool({ connectionString: env.DATABASE_URL, max: 10 });
    this.queryable = wrap(this.pool);
  }

  async onModuleInit(): Promise<void> {
    await this.migrate();
  }

  async onApplicationShutdown(): Promise<void> {
    await this.pool.end();
  }

  query<T extends QueryResultRow = QueryResultRow>(sql: string, params?: unknown[]): Promise<T[]> {
    return this.queryable.query<T>(sql, params);
  }

  async transaction<T>(fn: (tx: Queryable) => Promise<T>): Promise<T> {
    const client = await this.pool.connect();
    try {
      await client.query('BEGIN');
      const result = await fn(wrap(client));
      await client.query('COMMIT');
      return result;
    } catch (err) {
      await client.query('ROLLBACK').catch(() => undefined);
      throw err;
    } finally {
      client.release();
    }
  }

  private async migrate(): Promise<void> {
    const dir = join(process.cwd(), 'migrations');
    const files = (await readdir(dir)).filter((f) => f.endsWith('.sql')).sort();
    const client = await this.pool.connect();
    try {
      // Serialises migrations when several replicas start at once.
      await client.query('SELECT pg_advisory_lock($1)', [MIGRATIONS_LOCK_ID]);
      await client.query(
        'CREATE TABLE IF NOT EXISTS schema_migrations (name text PRIMARY KEY, applied_at timestamptz NOT NULL DEFAULT now())',
      );
      const { rows } = await client.query<{ name: string }>('SELECT name FROM schema_migrations');
      const applied = new Set(rows.map((r) => r.name));

      for (const file of files.filter((f) => !applied.has(f))) {
        const sql = await readFile(join(dir, file), 'utf8');
        await client.query('BEGIN');
        try {
          await client.query(sql);
          await client.query('INSERT INTO schema_migrations (name) VALUES ($1)', [file]);
          await client.query('COMMIT');
        } catch (err) {
          await client.query('ROLLBACK');
          throw err;
        }
        this.logger.log(`Applied migration ${file}`);
      }
    } finally {
      await client.query('SELECT pg_advisory_unlock($1)', [MIGRATIONS_LOCK_ID]).catch(() => undefined);
      client.release();
    }
  }
}
