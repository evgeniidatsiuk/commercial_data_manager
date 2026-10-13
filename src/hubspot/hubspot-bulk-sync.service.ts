import { Injectable } from '@nestjs/common';
import { CommercialRepository } from '../persistence/commercial.repository';
import type { EntityType } from '../persistence/sync-state.repository';
import type { BatchResponse, HubSpotObject } from './hubspot.client';
import { HubSpotClient } from './hubspot.client';
import { NonRetryableSyncError } from './hubspot-sync.service';
import { toCompanyProperties, toContactProperties, toDealProperties } from './hubspot.mapper';

/** entityId -> error for every entity that did NOT sync; absent = synced. */
export type BulkFailures = Map<string, Error>;

const indexBy = (results: HubSpotObject[], property: string, normalize = (v: string) => v): Map<string, string> =>
  new Map(
    results
      .filter((r) => r.properties[property] != null)
      .map((r) => [normalize(r.properties[property] as string), r.id]),
  );

const lower = (v: string) => v.toLowerCase();

const describe = (res: BatchResponse<unknown>[]): string =>
  res
    .flatMap((r) => r.errors.map((e) => e.message))
    .join('; ')
    .slice(0, 500) || 'no result returned for this record';

/**
 * High-volume path using HubSpot batch APIs (100 records per call), used by SyncWorker
 * when the backlog is large (backfills, replays, recovery after a HubSpot outage).
 */
@Injectable()
export class HubSpotBulkSyncService {
  constructor(
    private readonly hubspot: HubSpotClient,
    private readonly commercial: CommercialRepository,
  ) {}

  sync(entityType: EntityType, ids: string[]): Promise<BulkFailures> {
    return entityType === 'affiliate' ? this.syncAffiliates(ids) : this.syncDeals(ids);
  }

  private async syncAffiliates(ids: string[]): Promise<BulkFailures> {
    const failures: BulkFailures = new Map();
    const affiliates = await this.commercial.getAffiliates(ids);
    this.markMissing(ids, affiliates.map((a) => a.affiliateId), 'Affiliate', failures);
    if (affiliates.length === 0) return failures;

    // Companies and contacts have no create-only fields, so a single upsert per 100 records is enough.
    const companies = await this.hubspot.batchUpsert(
      'companies',
      'legend_affiliate_id',
      affiliates.map((a) => ({ id: a.affiliateId, properties: toCompanyProperties(a) })),
    );
    // HubSpot rejects duplicate IDs within one batch and lower-cases emails.
    const byEmail = new Map(affiliates.map((a) => [lower(a.contactEmail), a]));
    const contacts = await this.hubspot.batchUpsert(
      'contacts',
      'email',
      [...byEmail].map(([email, a]) => ({ id: email, properties: toContactProperties(a) })),
    );

    const companyIds = indexBy(companies.results, 'legend_affiliate_id');
    const contactIds = indexBy(contacts.results, 'email', lower);
    const mappings = affiliates.map((a) => ({
      affiliateId: a.affiliateId,
      companyId: companyIds.get(a.affiliateId) ?? null,
      contactId: contactIds.get(lower(a.contactEmail)) ?? null,
    }));
    await this.commercial.setAffiliateHubSpotIdsBulk(mappings);

    const pairs: Array<{ fromId: string; toId: string }> = [];
    for (const m of mappings) {
      if (m.companyId && m.contactId) pairs.push({ fromId: m.contactId, toId: m.companyId });
      else failures.set(m.affiliateId, new Error(`Batch upsert failed: ${describe([companies, contacts])}`));
    }
    await this.hubspot.batchAssociate('contacts', 'companies', pairs);
    return failures;
  }

  private async syncDeals(ids: string[]): Promise<BulkFailures> {
    const failures: BulkFailures = new Map();
    const deals = await this.commercial.getDeals(ids);
    this.markMissing(ids, deals.map((d) => d.dealId), 'Deal', failures);

    const affiliates = await this.commercial.getAffiliates([...new Set(deals.map((d) => d.affiliateId))]);
    const companyByAffiliate = new Map(
      affiliates.filter((a) => a.hubspotCompanyId).map((a) => [a.affiliateId, a.hubspotCompanyId as string]),
    );
    const ready = deals.filter((d) => {
      if (companyByAffiliate.has(d.affiliateId)) return true;
      failures.set(d.dealId, new Error(`Affiliate ${d.affiliateId} is not synced to HubSpot yet`));
      return false;
    });
    if (ready.length === 0) return failures;

    // No upsert for deals: dealstage is HubSpot-owned and must only be set on creation.
    // Batch read by unique property is strongly consistent (unlike search) and heals stale local mappings.
    const existing = indexBy(
      (await this.hubspot.batchRead('deals', 'legend_deal_id', ready.map((d) => d.dealId))).results,
      'legend_deal_id',
    );
    const toUpdate = ready.filter((d) => existing.has(d.dealId));
    const toCreate = ready.filter((d) => !existing.has(d.dealId));

    const updated = await this.hubspot.batchUpdate(
      'deals',
      toUpdate.map((d) => ({ id: existing.get(d.dealId) as string, properties: toDealProperties(d, { isNew: false }) })),
    );
    const created = await this.hubspot.batchCreate(
      'deals',
      toCreate.map((d) => ({ properties: toDealProperties(d, { isNew: true }) })),
    );

    const updatedIds = new Set(updated.results.map((r) => r.id));
    const createdIds = indexBy(created.results, 'legend_deal_id');
    const mappings: Array<{ dealId: string; hubspotDealId: string; companyId: string }> = [];
    for (const d of ready) {
      const hubspotDealId = existing.get(d.dealId) ?? createdIds.get(d.dealId);
      if (hubspotDealId && (createdIds.has(d.dealId) || updatedIds.has(hubspotDealId))) {
        mappings.push({ dealId: d.dealId, hubspotDealId, companyId: companyByAffiliate.get(d.affiliateId) as string });
      } else {
        const op = existing.has(d.dealId) ? 'update' : 'create';
        failures.set(d.dealId, new Error(`Batch ${op} failed: ${describe([updated, created])}`));
      }
    }
    await this.commercial.setDealHubSpotIdsBulk(mappings);
    await this.hubspot.batchAssociate(
      'deals',
      'companies',
      mappings.map((m) => ({ fromId: m.hubspotDealId, toId: m.companyId })),
    );
    return failures;
  }

  private markMissing(requested: string[], found: string[], label: string, failures: BulkFailures): void {
    const foundSet = new Set(found);
    for (const id of requested) {
      if (!foundSet.has(id)) failures.set(id, new NonRetryableSyncError(`${label} ${id} not found`));
    }
  }
}
