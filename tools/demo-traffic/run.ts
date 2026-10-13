/**
 * Demo traffic: plays the affiliate platform, the deals service and HubSpot sales reps,
 * so dashboards, traces, Kafka UI and the HubSpot mock always have something to show.
 * Runs directly on Node >= 24 (type stripping, no dependencies).
 */
const CDM = process.env.CDM_URL ?? 'http://cdm:3000';
const MOCK = process.env.HUBSPOT_MOCK_URL ?? 'http://hubspot-mock:4010';
const TICK_MS = Number(process.env.TICK_MS ?? 3000);
const INITIAL_SEED = Number(process.env.INITIAL_SEED ?? 200);
const BURST_EVERY_TICKS = Number(process.env.BURST_EVERY_TICKS ?? 30);

const BRANDS = ['Spin Media', 'Lucky Reviews', 'Casino Guru Partners', 'Bet Insights', 'Slots Hub', 'Jackpot Journal',
  'Odds Wizard', 'Reel Rewards', 'Poker Pulse', 'Bonus Finder', 'Free Spins Club', 'Sportsbook Scout'];
const FIRST_NAMES = ['Ada', 'Linus', 'Grace', 'Alan', 'Margaret', 'Ken', 'Barbara', 'Dennis'];
const LAST_NAMES = ['Lovelace', 'Torvalds', 'Hopper', 'Turing', 'Hamilton', 'Thompson', 'Liskov', 'Ritchie'];
const COUNTRIES = ['GB', 'SE', 'DE', 'CA', 'NZ', 'IE', 'FI', 'NO'];
const TIERS = ['bronze', 'silver', 'gold', 'platinum'];
const MODELS = ['CPA', 'REVSHARE', 'HYBRID'];
const STAGES = ['qualifiedtobuy', 'presentationscheduled', 'decisionmakerboughtin', 'contractsent', 'closedwon', 'closedlost'];

type Affiliate = {
  affiliateId: string;
  companyName: string;
  domain: string;
  contact: { email: string; firstName: string; lastName: string };
  country: string;
  tier: string;
};

const affiliates: Affiliate[] = [];
const dealIds: string[] = [];
let affiliateSeq = 0;
let dealSeq = 0;

const pick = <T>(items: T[]): T => items[Math.floor(Math.random() * items.length)];
const chance = (p: number): boolean => Math.random() < p;
const sleep = (ms: number) => new Promise((resolve) => setTimeout(resolve, ms));

// eslint-disable-next-line @typescript-eslint/no-explicit-any
async function call(method: string, url: string, body?: unknown): Promise<{ status: number; body: any }> {
  const res = await fetch(url, {
    method,
    headers: { 'content-type': 'application/json' },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await res.text();
  return { status: res.status, body: text ? JSON.parse(text) : undefined };
}

async function newAffiliate(): Promise<string> {
  const i = ++affiliateSeq;
  const brand = pick(BRANDS);
  const slug = `${brand.toLowerCase().replace(/\s+/g, '-')}-${i}`;
  const affiliate: Affiliate = {
    affiliateId: `demo-aff-${i}`,
    companyName: `${brand} #${i}`,
    domain: `${slug}.example`,
    contact: { email: `partners@${slug}.example`, firstName: pick(FIRST_NAMES), lastName: pick(LAST_NAMES) },
    country: pick(COUNTRIES),
    tier: pick(TIERS),
  };
  await call('POST', `${CDM}/dev/events/affiliate`, affiliate);
  affiliates.push(affiliate);
  return affiliate.affiliateId;
}

async function upgradeAffiliate(): Promise<string | null> {
  const affiliate = affiliates.length ? pick(affiliates) : null;
  if (!affiliate) return null;
  affiliate.tier = TIERS[Math.min(TIERS.indexOf(affiliate.tier) + 1, TIERS.length - 1)];
  await call('POST', `${CDM}/dev/events/affiliate`, affiliate);
  return `${affiliate.affiliateId} -> ${affiliate.tier}`;
}

async function newDeal(): Promise<string | null> {
  const affiliate = affiliates.length ? pick(affiliates) : null;
  if (!affiliate) return null;
  const i = ++dealSeq;
  const model = pick(MODELS);
  const dealId = `demo-deal-${i}`;
  await call('POST', `${CDM}/dev/events/deal`, {
    dealId,
    affiliateId: affiliate.affiliateId,
    name: `${affiliate.companyName} - ${model} ${affiliate.country}`,
    commissionModel: model,
    amount: 50 + Math.round(Math.random() * 450),
  });
  dealIds.push(dealId);
  return dealId;
}

/** A sales rep moves a deal in HubSpot -> signed webhook -> CDM -> Kafka. */
async function moveDealStage(): Promise<string | null> {
  if (!dealIds.length) return null;
  const dealId = pick(dealIds);
  const deal = await call('GET', `${CDM}/deals/${dealId}`);
  if (!deal.body?.hubspotDealId) return null;
  const stage = pick(STAGES);
  const res = await call('POST', `${MOCK}/__mock/deals/${deal.body.hubspotDealId}/stage`, { stage });
  return res.status === 200 ? `${dealId} -> ${stage}` : null;
}

async function waitForCdm(): Promise<void> {
  for (;;) {
    try {
      if ((await fetch(`${CDM}/health`)).ok) return;
    } catch {
      // not up yet
    }
    await sleep(1000);
  }
}

async function main(): Promise<void> {
  await waitForCdm();

  const stats = (await call('GET', `${CDM}/sync/stats`)).body;
  const existing = stats.totals.pending + stats.totals.synced + stats.totals.failed;
  if (existing < INITIAL_SEED) {
    const seed = await call('POST', `${CDM}/dev/seed`, { affiliates: INITIAL_SEED, dealsPerAffiliate: 2 });
    console.log(`Initial seed: ${seed.body.affiliates} affiliates + ${seed.body.deals} deals (${seed.body.prefix})`);
  }
  for (let i = 0; i < 10; i++) await newAffiliate();

  for (let tick = 1; ; tick++) {
    try {
      const done: string[] = [];
      done.push(`new affiliate ${await newAffiliate()}`);
      for (let i = 0; i < 2; i++) {
        const dealId = await newDeal();
        if (dealId) done.push(`new deal ${dealId}`);
      }
      if (chance(0.4)) done.push(`tier upgrade ${await upgradeAffiliate()}`);
      if (chance(0.6)) {
        const moved = await moveDealStage();
        if (moved) done.push(`stage ${moved}`);
      }
      if (tick % BURST_EVERY_TICKS === 0) {
        const burst = await call('POST', `${CDM}/dev/seed`, { affiliates: 120, dealsPerAffiliate: 1 });
        done.push(`burst ${burst.body.prefix} (bulk sync path)`);
      }
      console.log(`[tick ${tick}] ${done.join(' | ')}`);
    } catch (err) {
      console.error(`[tick ${tick}] ${(err as Error).message}`);
    }
    await sleep(TICK_MS);
  }
}

void main();
