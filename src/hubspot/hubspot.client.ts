import { Inject, Injectable, Logger } from '@nestjs/common';
import { setTimeout as sleep } from 'node:timers/promises';
import { ENV, type Env } from '../config/env';

export type ObjectType = 'contacts' | 'companies' | 'deals';
export type Properties = Record<string, string>;

export interface HubSpotObject {
  id: string;
  properties: Record<string, string | null>;
}

export interface PropertyDefinition {
  name: string;
  label: string;
  groupName: string;
  hasUniqueValue?: boolean;
}

export class HubSpotApiError extends Error {
  constructor(
    readonly status: number,
    readonly body: string,
  ) {
    super(`HubSpot API responded ${status}: ${body.slice(0, 500)}`);
  }

  /** 409 = object already exists (e.g. search index lag); the next attempt will find it. */
  get retryable(): boolean {
    return this.status === 409 || this.status === 429 || this.status >= 500;
  }
}

const MAX_IN_PROCESS_ATTEMPTS = 3;
const REQUEST_TIMEOUT_MS = 10_000;

/** Thin client over HubSpot CRM v3/v4 REST API. */
@Injectable()
export class HubSpotClient {
  private readonly logger = new Logger(HubSpotClient.name);

  constructor(@Inject(ENV) private readonly env: Env) {}

  create(type: ObjectType, properties: Properties): Promise<HubSpotObject> {
    return this.request('POST', `/crm/v3/objects/${type}`, { properties });
  }

  update(type: ObjectType, id: string, properties: Properties): Promise<HubSpotObject> {
    return this.request('PATCH', `/crm/v3/objects/${type}/${id}`, { properties });
  }

  async findOneBy(type: ObjectType, propertyName: string, value: string): Promise<HubSpotObject | null> {
    const res = await this.request<{ results: HubSpotObject[] }>('POST', `/crm/v3/objects/${type}/search`, {
      filterGroups: [{ filters: [{ propertyName, operator: 'EQ', value }] }],
      limit: 1,
    });
    return res.results[0] ?? null;
  }

  /** Idempotent: re-associating existing records is a no-op. */
  async associate(fromType: ObjectType, fromId: string, toType: ObjectType, toId: string): Promise<void> {
    await this.request('PUT', `/crm/v4/objects/${fromType}/${fromId}/associations/default/${toType}/${toId}`);
  }

  async ensureProperty(type: ObjectType, def: PropertyDefinition): Promise<boolean> {
    try {
      await this.request('POST', `/crm/v3/properties/${type}`, { ...def, type: 'string', fieldType: 'text' });
      return true;
    } catch (err) {
      if (err instanceof HubSpotApiError && err.status === 409) return false;
      throw err;
    }
  }

  private async request<T>(method: string, path: string, body?: unknown): Promise<T> {
    for (let attempt = 1; ; attempt++) {
      const res = await fetch(`${this.env.HUBSPOT_BASE_URL}${path}`, {
        method,
        headers: {
          authorization: `Bearer ${this.env.HUBSPOT_ACCESS_TOKEN}`,
          'content-type': 'application/json',
        },
        body: body === undefined ? undefined : JSON.stringify(body),
        signal: AbortSignal.timeout(REQUEST_TIMEOUT_MS),
      });

      if (res.ok) return (res.status === 204 ? undefined : await res.json()) as T;

      const text = await res.text();
      // Short in-process retry for burst rate limits; longer outages are handled by the sync worker backoff.
      if ((res.status === 429 || res.status >= 500) && attempt < MAX_IN_PROCESS_ATTEMPTS) {
        const retryAfter = Number(res.headers.get('retry-after'));
        const delayMs = Number.isFinite(retryAfter) && retryAfter > 0 ? retryAfter * 1000 : 250 * 2 ** attempt;
        this.logger.warn(`${method} ${path} -> ${res.status}, retrying in ${delayMs}ms`);
        await sleep(delayMs);
        continue;
      }
      throw new HubSpotApiError(res.status, text);
    }
  }
}
