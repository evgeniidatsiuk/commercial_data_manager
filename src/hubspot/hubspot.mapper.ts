import type { Affiliate, Deal } from '../persistence/commercial.repository';
import type { ObjectType, Properties, PropertyDefinition } from './hubspot.client';

export const DEFAULT_PIPELINE = 'default';
export const INITIAL_DEAL_STAGE = 'appointmentscheduled';

/** Custom properties CDM owns in HubSpot. Unique ones act as external IDs for find-or-create. */
export const CUSTOM_PROPERTIES: Array<{ objectType: ObjectType } & PropertyDefinition> = [
  {
    objectType: 'companies',
    name: 'legend_affiliate_id',
    label: 'Legend affiliate ID',
    groupName: 'companyinformation',
    hasUniqueValue: true,
  },
  { objectType: 'companies', name: 'legend_affiliate_tier', label: 'Legend affiliate tier', groupName: 'companyinformation' },
  { objectType: 'contacts', name: 'legend_affiliate_id', label: 'Legend affiliate ID', groupName: 'contactinformation' },
  { objectType: 'deals', name: 'legend_deal_id', label: 'Legend deal ID', groupName: 'dealinformation', hasUniqueValue: true },
  { objectType: 'deals', name: 'legend_commission_model', label: 'Commission model', groupName: 'dealinformation' },
];

const compact = (props: Record<string, string | null | undefined>): Properties =>
  Object.fromEntries(Object.entries(props).filter((e): e is [string, string] => e[1] != null));

export const toCompanyProperties = (a: Affiliate): Properties =>
  compact({
    name: a.companyName,
    domain: a.domain,
    country: a.country,
    legend_affiliate_id: a.affiliateId,
    legend_affiliate_tier: a.tier,
  });

export const toContactProperties = (a: Affiliate): Properties =>
  compact({
    email: a.contactEmail,
    firstname: a.contactFirstName,
    lastname: a.contactLastName,
    legend_affiliate_id: a.affiliateId,
  });

/**
 * Field ownership: Legend owns commercial terms, the sales team owns the pipeline stage in HubSpot.
 * Stage is therefore only set on creation and never overwritten by CDM.
 */
export const toDealProperties = (d: Deal, { isNew }: { isNew: boolean }): Properties =>
  compact({
    dealname: d.name,
    amount: d.amount,
    legend_deal_id: d.dealId,
    legend_commission_model: d.commissionModel,
    ...(isNew ? { pipeline: DEFAULT_PIPELINE, dealstage: INITIAL_DEAL_STAGE } : {}),
  });
