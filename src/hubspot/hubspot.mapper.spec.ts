import type { Affiliate, Deal } from '../persistence/commercial.repository';
import { toCompanyProperties, toContactProperties, toDealProperties } from './hubspot.mapper';

const affiliate: Affiliate = {
  affiliateId: 'aff-1',
  companyName: 'Lucky Reviews Ltd',
  domain: 'luckyreviews.example',
  contactEmail: 'ada@luckyreviews.example',
  contactFirstName: 'Ada',
  contactLastName: null,
  country: 'GB',
  tier: 'gold',
  hubspotCompanyId: null,
  hubspotContactId: null,
  updatedAt: new Date(),
};

const deal: Deal = {
  dealId: 'deal-1',
  affiliateId: 'aff-1',
  name: 'Q4 CPA',
  commissionModel: 'CPA',
  amount: '150.00',
  hubspotDealId: null,
  hubspotStage: null,
  updatedAt: new Date(),
};

describe('HubSpot mapper', () => {
  it('maps an affiliate to a company with the external ID', () => {
    expect(toCompanyProperties(affiliate)).toEqual({
      name: 'Lucky Reviews Ltd',
      domain: 'luckyreviews.example',
      country: 'GB',
      legend_affiliate_id: 'aff-1',
      legend_affiliate_tier: 'gold',
    });
  });

  it('omits null fields so HubSpot values are not wiped', () => {
    expect(toContactProperties(affiliate)).not.toHaveProperty('lastname');
  });

  it('sets pipeline and stage only when creating a deal', () => {
    expect(toDealProperties(deal, { isNew: true })).toMatchObject({ pipeline: 'default', dealstage: 'appointmentscheduled' });
    expect(toDealProperties(deal, { isNew: false })).not.toHaveProperty('dealstage');
  });
});
