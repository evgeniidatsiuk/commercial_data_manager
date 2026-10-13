import { z } from 'zod';

const envelope = <TType extends string, TData extends z.ZodTypeAny>(type: TType, data: TData) =>
  z.object({
    eventId: z.string().min(1),
    type: z.literal(type),
    occurredAt: z.string().datetime({ offset: true }),
    source: z.string().min(1),
    data,
  });

export const affiliateDataSchema = z.object({
  affiliateId: z.string().min(1),
  companyName: z.string().min(1),
  domain: z.string().min(1).optional(),
  contact: z.object({
    email: z.string().email(),
    firstName: z.string().optional(),
    lastName: z.string().optional(),
  }),
  country: z.string().length(2).optional(),
  tier: z.enum(['bronze', 'silver', 'gold', 'platinum']),
});

export const dealDataSchema = z.object({
  dealId: z.string().min(1),
  affiliateId: z.string().min(1),
  name: z.string().min(1),
  commissionModel: z.enum(['CPA', 'REVSHARE', 'HYBRID']),
  amount: z.number().nonnegative().optional(),
});

export const affiliateUpsertedSchema = envelope('affiliate.upserted', affiliateDataSchema);
export const dealUpsertedSchema = envelope('deal.upserted', dealDataSchema);

export type AffiliateData = z.infer<typeof affiliateDataSchema>;
export type DealData = z.infer<typeof dealDataSchema>;
export type AffiliateUpserted = z.infer<typeof affiliateUpsertedSchema>;
export type DealUpserted = z.infer<typeof dealUpsertedSchema>;
export type InboundEvent = AffiliateUpserted | DealUpserted;

export interface DealStageChanged {
  eventId: string;
  type: 'hubspot.deal.stage-changed';
  occurredAt: string;
  source: 'commercial-data-manager';
  data: { dealId: string; affiliateId: string; hubspotDealId: string; stage: string };
}

export const hubspotWebhookPayloadSchema = z.array(
  z
    .object({
      eventId: z.number(),
      subscriptionType: z.string(),
      objectId: z.number(),
      occurredAt: z.number(),
      propertyName: z.string().optional(),
      propertyValue: z.string().optional(),
    })
    .passthrough(),
);

export type HubSpotWebhookEvent = z.infer<typeof hubspotWebhookPayloadSchema>[number];
