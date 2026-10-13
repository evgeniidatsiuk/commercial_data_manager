import { Inject, Injectable, Logger, OnApplicationBootstrap } from '@nestjs/common';
import { ENV, type Env } from '../config/env';
import { CommercialRepository } from '../persistence/commercial.repository';
import type { EntityType } from '../persistence/sync-state.repository';
import { HubSpotClient, type ObjectType, type Properties } from './hubspot.client';
import { CUSTOM_PROPERTIES, toCompanyProperties, toContactProperties, toDealProperties } from './hubspot.mapper';

export class NonRetryableSyncError extends Error {}

@Injectable()
export class HubSpotSyncService implements OnApplicationBootstrap {
  private readonly logger = new Logger(HubSpotSyncService.name);

  constructor(
    private readonly hubspot: HubSpotClient,
    private readonly commercial: CommercialRepository,
    @Inject(ENV) private readonly env: Env,
  ) {}

  async onApplicationBootstrap(): Promise<void> {
    if (!this.env.HUBSPOT_ENSURE_PROPERTIES) return;
    try {
      for (const { objectType, ...def } of CUSTOM_PROPERTIES) {
        if (await this.hubspot.ensureProperty(objectType, def)) {
          this.logger.log(`Created HubSpot property ${objectType}.${def.name}`);
        }
      }
    } catch (err) {
      this.logger.warn(`Could not ensure HubSpot custom properties: ${(err as Error).message}`);
    }
  }

  sync(entityType: EntityType, entityId: string): Promise<void> {
    return entityType === 'affiliate' ? this.syncAffiliate(entityId) : this.syncDeal(entityId);
  }

  private async syncAffiliate(affiliateId: string): Promise<void> {
    const affiliate = await this.commercial.getAffiliate(affiliateId);
    if (!affiliate) throw new NonRetryableSyncError(`Affiliate ${affiliateId} not found`);

    // Each HubSpot ID is persisted immediately so a partial failure never creates duplicates on retry.
    const companyId = await this.upsert(
      'companies',
      affiliate.hubspotCompanyId,
      ['legend_affiliate_id', affiliate.affiliateId],
      toCompanyProperties(affiliate),
    );
    if (companyId !== affiliate.hubspotCompanyId) {
      await this.commercial.setAffiliateHubSpotIds(affiliateId, { companyId });
    }

    const contactId = await this.upsert(
      'contacts',
      affiliate.hubspotContactId,
      ['email', affiliate.contactEmail],
      toContactProperties(affiliate),
    );
    if (contactId !== affiliate.hubspotContactId) {
      await this.commercial.setAffiliateHubSpotIds(affiliateId, { contactId });
    }

    await this.hubspot.associate('contacts', contactId, 'companies', companyId);
  }

  private async syncDeal(dealId: string): Promise<void> {
    const deal = await this.commercial.getDeal(dealId);
    if (!deal) throw new NonRetryableSyncError(`Deal ${dealId} not found`);

    const affiliate = await this.commercial.getAffiliate(deal.affiliateId);
    if (!affiliate?.hubspotCompanyId) {
      // Retryable: the affiliate event may simply not have been synced yet.
      throw new Error(`Affiliate ${deal.affiliateId} is not synced to HubSpot yet`);
    }

    const hubspotDealId = await this.upsert(
      'deals',
      deal.hubspotDealId,
      ['legend_deal_id', deal.dealId],
      toDealProperties(deal, { isNew: false }),
      toDealProperties(deal, { isNew: true }),
    );
    if (hubspotDealId !== deal.hubspotDealId) await this.commercial.setDealHubSpotId(dealId, hubspotDealId);

    await this.hubspot.associate('deals', hubspotDealId, 'companies', affiliate.hubspotCompanyId);
  }

  /**
   * Find-or-create by external ID. Our own mapping is checked first because
   * HubSpot search is eventually consistent for freshly created records.
   */
  private async upsert(
    type: ObjectType,
    knownId: string | null,
    [keyProperty, keyValue]: [string, string],
    properties: Properties,
    createProperties: Properties = properties,
  ): Promise<string> {
    if (knownId) {
      await this.hubspot.update(type, knownId, properties);
      return knownId;
    }
    const existing = await this.hubspot.findOneBy(type, keyProperty, keyValue);
    if (existing) {
      await this.hubspot.update(type, existing.id, properties);
      return existing.id;
    }
    return (await this.hubspot.create(type, createProperties)).id;
  }
}
