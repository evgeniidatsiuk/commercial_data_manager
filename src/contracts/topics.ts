export const Topics = {
  /** Produced by the affiliate platform. */
  AffiliateEvents: 'legend.affiliates.v1',
  /** Produced by the deals/commissions service. */
  DealEvents: 'legend.deals.v1',
  /** Produced by CDM: changes made by the commercial team in HubSpot. */
  HubSpotChanges: 'cdm.hubspot-changes.v1',
  /** Poison messages CDM could not parse/validate. */
  DeadLetter: 'cdm.dead-letter.v1',
} as const;

export const ALL_TOPICS = Object.values(Topics);
