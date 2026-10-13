-- Local projection of commercial entities owned by other Legend services.
CREATE TABLE affiliates (
  affiliate_id        text PRIMARY KEY,
  company_name        text NOT NULL,
  domain              text,
  contact_email       text NOT NULL,
  contact_first_name  text,
  contact_last_name   text,
  country             text,
  tier                text NOT NULL,
  hubspot_company_id  text,
  hubspot_contact_id  text,
  source_occurred_at  timestamptz NOT NULL,
  updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE TABLE deals (
  deal_id             text PRIMARY KEY,
  -- No FK: deal events may arrive before the affiliate event (different topics).
  affiliate_id        text NOT NULL,
  name                text NOT NULL,
  commission_model    text NOT NULL,
  amount              numeric(14, 2),
  hubspot_deal_id     text,
  -- Owned by HubSpot (sales pipeline), updated via webhooks only.
  hubspot_stage       text,
  source_occurred_at  timestamptz NOT NULL,
  updated_at          timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX deals_affiliate_id_idx ON deals (affiliate_id);
CREATE UNIQUE INDEX deals_hubspot_deal_id_idx ON deals (hubspot_deal_id) WHERE hubspot_deal_id IS NOT NULL;

-- Inbox for idempotent consumption (Kafka + HubSpot webhooks).
CREATE TABLE processed_events (
  event_id      text PRIMARY KEY,
  source        text NOT NULL,
  processed_at  timestamptz NOT NULL DEFAULT now()
);

-- Desired-state reconciliation: every local change bumps desired_version,
-- the worker pushes the latest state to HubSpot and records synced_version.
CREATE TABLE sync_state (
  entity_type      text NOT NULL,
  entity_id        text NOT NULL,
  desired_version  bigint NOT NULL DEFAULT 1,
  synced_version   bigint NOT NULL DEFAULT 0,
  status           text NOT NULL DEFAULT 'pending' CHECK (status IN ('pending', 'synced', 'failed')),
  attempts         int NOT NULL DEFAULT 0,
  next_attempt_at  timestamptz NOT NULL DEFAULT now(),
  locked_until     timestamptz,
  last_error       text,
  last_synced_at   timestamptz,
  updated_at       timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (entity_type, entity_id)
);

CREATE INDEX sync_state_due_idx ON sync_state (next_attempt_at) WHERE status = 'pending';

-- Transactional outbox for events CDM publishes back to Kafka.
CREATE TABLE outbox (
  id            bigserial PRIMARY KEY,
  topic         text NOT NULL,
  message_key   text,
  payload       jsonb NOT NULL,
  created_at    timestamptz NOT NULL DEFAULT now(),
  published_at  timestamptz
);

CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;
