import { Injectable } from '@nestjs/common';
import type { AffiliateData, DealData } from '../contracts/events';
import { DatabaseService, type Queryable } from './database.service';

export interface Affiliate {
  affiliateId: string;
  companyName: string;
  domain: string | null;
  contactEmail: string;
  contactFirstName: string | null;
  contactLastName: string | null;
  country: string | null;
  tier: string;
  hubspotCompanyId: string | null;
  hubspotContactId: string | null;
  updatedAt: Date;
}

export interface Deal {
  dealId: string;
  affiliateId: string;
  name: string;
  commissionModel: string;
  amount: string | null;
  hubspotDealId: string | null;
  hubspotStage: string | null;
  updatedAt: Date;
}

const AFFILIATE_COLUMNS = `
  affiliate_id AS "affiliateId", company_name AS "companyName", domain, contact_email AS "contactEmail",
  contact_first_name AS "contactFirstName", contact_last_name AS "contactLastName", country, tier,
  hubspot_company_id AS "hubspotCompanyId", hubspot_contact_id AS "hubspotContactId", updated_at AS "updatedAt"`;

const DEAL_COLUMNS = `
  deal_id AS "dealId", affiliate_id AS "affiliateId", name, commission_model AS "commissionModel", amount,
  hubspot_deal_id AS "hubspotDealId", hubspot_stage AS "hubspotStage", updated_at AS "updatedAt"`;

@Injectable()
export class CommercialRepository {
  constructor(private readonly db: DatabaseService) {}

  /** Returns false when a newer version is already stored (out-of-order delivery). */
  async upsertAffiliate(tx: Queryable, a: AffiliateData, occurredAt: string): Promise<boolean> {
    const rows = await tx.query(
      `INSERT INTO affiliates (affiliate_id, company_name, domain, contact_email, contact_first_name,
                               contact_last_name, country, tier, source_occurred_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
       ON CONFLICT (affiliate_id) DO UPDATE SET
         company_name = EXCLUDED.company_name, domain = EXCLUDED.domain, contact_email = EXCLUDED.contact_email,
         contact_first_name = EXCLUDED.contact_first_name, contact_last_name = EXCLUDED.contact_last_name,
         country = EXCLUDED.country, tier = EXCLUDED.tier,
         source_occurred_at = EXCLUDED.source_occurred_at, updated_at = now()
       WHERE affiliates.source_occurred_at <= EXCLUDED.source_occurred_at
       RETURNING affiliate_id`,
      [
        a.affiliateId,
        a.companyName,
        a.domain ?? null,
        a.contact.email,
        a.contact.firstName ?? null,
        a.contact.lastName ?? null,
        a.country ?? null,
        a.tier,
        occurredAt,
      ],
    );
    return rows.length > 0;
  }

  /** Returns false when a newer version is already stored (out-of-order delivery). */
  async upsertDeal(tx: Queryable, d: DealData, occurredAt: string): Promise<boolean> {
    const rows = await tx.query(
      `INSERT INTO deals (deal_id, affiliate_id, name, commission_model, amount, source_occurred_at)
       VALUES ($1, $2, $3, $4, $5, $6)
       ON CONFLICT (deal_id) DO UPDATE SET
         affiliate_id = EXCLUDED.affiliate_id, name = EXCLUDED.name, commission_model = EXCLUDED.commission_model,
         amount = EXCLUDED.amount, source_occurred_at = EXCLUDED.source_occurred_at, updated_at = now()
       WHERE deals.source_occurred_at <= EXCLUDED.source_occurred_at
       RETURNING deal_id`,
      [d.dealId, d.affiliateId, d.name, d.commissionModel, d.amount ?? null, occurredAt],
    );
    return rows.length > 0;
  }

  async getAffiliate(affiliateId: string): Promise<Affiliate | null> {
    const [row] = await this.db.query<Affiliate>(
      `SELECT ${AFFILIATE_COLUMNS} FROM affiliates WHERE affiliate_id = $1`,
      [affiliateId],
    );
    return row ?? null;
  }

  async getDeal(dealId: string): Promise<Deal | null> {
    const [row] = await this.db.query<Deal>(`SELECT ${DEAL_COLUMNS} FROM deals WHERE deal_id = $1`, [dealId]);
    return row ?? null;
  }

  listDealsByAffiliate(affiliateId: string): Promise<Deal[]> {
    return this.db.query<Deal>(
      `SELECT ${DEAL_COLUMNS} FROM deals WHERE affiliate_id = $1 ORDER BY deal_id`,
      [affiliateId],
    );
  }

  async setAffiliateHubSpotIds(affiliateId: string, ids: { companyId?: string; contactId?: string }): Promise<void> {
    await this.db.query(
      `UPDATE affiliates
       SET hubspot_company_id = COALESCE($2, hubspot_company_id),
           hubspot_contact_id = COALESCE($3, hubspot_contact_id)
       WHERE affiliate_id = $1`,
      [affiliateId, ids.companyId ?? null, ids.contactId ?? null],
    );
  }

  async setDealHubSpotId(dealId: string, hubspotDealId: string): Promise<void> {
    await this.db.query('UPDATE deals SET hubspot_deal_id = $2 WHERE deal_id = $1', [dealId, hubspotDealId]);
  }

  async setStageByHubSpotId(tx: Queryable, hubspotDealId: string, stage: string): Promise<Deal | null> {
    const [row] = await tx.query<Deal>(
      `UPDATE deals SET hubspot_stage = $2, updated_at = now()
       WHERE hubspot_deal_id = $1
       RETURNING ${DEAL_COLUMNS}`,
      [hubspotDealId, stage],
    );
    return row ?? null;
  }
}
