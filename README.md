# Commercial Data Manager (PoC)

Bridge between Legend services and HubSpot CRM. Built with **NestJS 11 / TypeScript / Node 24 / Kafka / PostgreSQL / HubSpot CRM API**, plus an **MCP server** so AI agents can operate the integration.

```mermaid
flowchart LR
  subgraph Legend services
    AP[Affiliate platform]
    DS[Deals service]
    OT[Other consumers]
  end
  AP -- legend.affiliates.v1 --> K[(Kafka)]
  DS -- legend.deals.v1 --> K
  K --> IC[IngestionConsumer]
  IC -- "inbox + projection + sync request (1 tx)" --> PG[(Postgres)]
  PG --> SW[SyncWorker] -- "CRM v3/v4 API" --> HS[HubSpot]
  HS -- "webhook (signature v3)" --> WC[WebhookController]
  WC -- "inbox + state + outbox (1 tx)" --> PG
  PG --> OR[OutboxRelay] -- cdm.hubspot-changes.v1 --> K --> OT
  AI[AI agent / Copilot] -- MCP stdio --> MCP[CDM MCP server] -- REST --> API[CDM API]
```

## How it works

### 1. Legend → HubSpot (outbound sync)

```mermaid
sequenceDiagram
  autonumber
  participant AP as Affiliate platform
  participant K as Kafka
  participant IC as IngestionConsumer
  participant PG as Postgres
  participant SW as SyncWorker
  participant HS as HubSpot

  AP->>K: affiliate.upserted (key = affiliateId)
  K->>IC: deliver message (at-least-once)
  IC->>IC: validate with zod
  alt invalid payload
    IC->>K: publish to cdm.dead-letter.v1
  else valid
    rect rgb(235, 245, 255)
      note over IC,PG: single transaction
      IC->>PG: insert processed_events (stop if duplicate)
      IC->>PG: upsert affiliates (stop if event is older)
      IC->>PG: sync_state desired_version + 1, status = pending
    end
  end
  IC-->>K: commit offset

  loop every 500 ms
    SW->>PG: claim due rows (FOR UPDATE SKIP LOCKED, 60 s lease)
  end
  SW->>PG: load latest affiliate state
  SW->>HS: find-or-create company by legend_affiliate_id
  SW->>PG: save hubspot_company_id
  SW->>HS: find-or-create contact by email
  SW->>PG: save hubspot_contact_id
  SW->>HS: associate contact with company
  alt success
    SW->>PG: synced_version = v, status = synced
  else 429 / 5xx / 409
    SW->>PG: status = pending, retry in 2^n s (max 300 s)
  else 4xx or max attempts reached
    SW->>PG: status = failed, last_error saved
  end
```

Deals follow the same path. The worker waits (retryable error) until the affiliate's HubSpot company exists, then creates the deal in the default pipeline and associates it with the company.

### 2. HubSpot → Legend (inbound changes)

```mermaid
sequenceDiagram
  autonumber
  actor Rep as Sales rep
  participant HS as HubSpot
  participant WC as WebhookController
  participant PG as Postgres
  participant OR as OutboxRelay
  participant K as Kafka
  participant C as Legend consumers

  Rep->>HS: move deal to closedwon
  HS->>WC: POST /webhooks/hubspot with X-HubSpot-Signature-v3
  WC->>WC: verify HMAC-SHA256 and timestamp within 5 min
  alt invalid signature
    WC-->>HS: 401
  else valid
    rect rgb(235, 245, 255)
      note over WC,PG: single transaction
      WC->>PG: insert processed_events hubspot-eventId (stop if duplicate)
      WC->>PG: update deals.hubspot_stage
      WC->>PG: insert outbox row
    end
    WC-->>HS: 204
  end

  loop every 500 ms
    OR->>PG: lock unpublished outbox rows (SKIP LOCKED)
    OR->>K: publish to cdm.hubspot-changes.v1 (key = dealId)
    OR->>PG: set published_at
  end
  K->>C: hubspot.deal.stage-changed
```

### 3. Sync state lifecycle

```mermaid
stateDiagram-v2
  [*] --> pending: event applied
  pending --> in_flight: worker claims row
  in_flight --> synced: HubSpot OK
  in_flight --> pending: newer change arrived during sync
  in_flight --> pending: retryable error, backoff
  in_flight --> pending: lease expired (worker crashed)
  in_flight --> failed: non-retryable or max attempts
  synced --> pending: new event
  failed --> pending: manual retry via API or MCP
  note right of in_flight
    Not a separate status value.
    Row stays pending with locked_until set.
  end note
```

Several events for one entity during an outage collapse into one HubSpot call. The worker always pushes the latest stored state, not each event.

## Quick start

Requires Docker only.

```bash
docker compose up -d --build --wait     # postgres, kafka (KRaft), kafka-ui, hubspot-mock, cdm
npm ci && npm run test:e2e              # full round-trip test against the running stack
```

| Service      | URL                                   |
| ------------ | ------------------------------------- |
| CDM API      | http://localhost:3000                 |
| HubSpot mock | http://localhost:4010/__mock/state    |
| Kafka UI     | http://localhost:8080                 |
| Kafka (host) | localhost:29092                       |
| Postgres     | postgres://cdm:cdm@localhost:5432/cdm |

## Try it manually

```bash
# 1. Affiliate platform publishes an affiliate (dev endpoint -> Kafka)
curl -X POST localhost:3000/dev/events/affiliate -H 'content-type: application/json' -d '{
  "affiliateId": "aff-1", "companyName": "Lucky Reviews Ltd", "domain": "luckyreviews.example",
  "contact": { "email": "ada@luckyreviews.example", "firstName": "Ada" }, "country": "GB", "tier": "gold" }'

# 2. Deals service publishes a CPA deal (may arrive before the affiliate - it is retried until the company exists)
curl -X POST localhost:3000/dev/events/deal -H 'content-type: application/json' -d '{
  "dealId": "deal-1", "affiliateId": "aff-1", "name": "Q4 CPA - UK", "commissionModel": "CPA", "amount": 150 }'

# 3. Inspect the CDM projection + HubSpot IDs + sync state, and what landed in "HubSpot"
curl localhost:3000/affiliates/aff-1
curl localhost:4010/__mock/state

# 4. Sales rep moves the deal in HubSpot -> signed webhook -> CDM -> Kafka (cdm.hubspot-changes.v1, see Kafka UI)
HS_DEAL_ID=$(curl -s localhost:3000/deals/deal-1 | sed -E 's/.*"hubspotDealId":"([0-9]+)".*/\1/')
curl -X POST localhost:4010/__mock/deals/$HS_DEAL_ID/stage -H 'content-type: application/json' -d '{"stage":"closedwon"}'
curl localhost:3000/deals/deal-1

# 5. Operations
curl 'localhost:3000/sync?status=failed'
curl -X POST localhost:3000/sync/deal/deal-1/retry
```

You can also produce raw events with `kafka-console-producer` or Kafka UI. Invalid payloads go to `cdm.dead-letter.v1`.

**Chaos mode:** `MOCK_RATE_LIMIT_PERCENT=30 docker compose up -d --force-recreate hubspot-mock` makes the mock answer 30% of writes with `429`, which exercises the retry and backoff logic.

## Real HubSpot

Copy `.env.example` to `.env`, then set a private-app token and client secret. After that, run `docker compose up -d --build cdm`. On startup, CDM creates its custom properties (`legend_affiliate_id`, `legend_deal_id`, etc.). To receive webhooks, expose `/webhooks/hubspot` (e.g. with ngrok), set `WEBHOOK_PUBLIC_BASE_URL`, and subscribe to `deal.propertyChange` for `dealstage`.

## Agentic AI (MCP)

[.vscode/mcp.json](.vscode/mcp.json) registers the CDM MCP server ([src/mcp/main.ts](src/mcp/main.ts)). It runs inside the `cdm` container through `docker compose exec`, so no local setup is needed. Tools: `get_affiliate`, `get_deal`, `list_sync_states`, `retry_sync`, `publish_affiliate_event`, `publish_deal_event`.

Example prompt in Copilot agent mode: *"Create a gold affiliate 'Spin Media' with a REVSHARE deal, then check whether both synced to HubSpot. If anything failed, explain why."*

The MCP server is a thin adapter over the REST API, so agents get the same validation and guardrails as humans.

## Design decisions

| Concern | Approach |
| --- | --- |
| At-least-once Kafka delivery | **Inbox** table (`processed_events`) written in the same transaction as the projection, which makes processing idempotent |
| Out-of-order events | Upserts are guarded by `source_occurred_at`, so stale events are ignored. Kafka key = entity ID keeps per-entity ordering |
| HubSpot rate limits / outages | Ingestion is **decoupled** from HubSpot. `sync_state` is a desired-state reconciliation table (`desired_version` vs `synced_version`). Bursts of updates coalesce into a single API call. Retries use in-process handling for 429/5xx plus worker exponential backoff, and a job is marked `failed` after N attempts |
| Horizontal scaling | `FOR UPDATE SKIP LOCKED` plus a lease (`locked_until`), so multiple replicas never sync the same entity at once |
| Duplicate HubSpot records | Find-or-create by external ID: local mapping first (HubSpot search is eventually consistent), then search, then create. The `legend_*_id` properties are unique in HubSpot |
| Field ownership | Legend owns commercial terms. The sales team owns `dealstage` in HubSpot, so CDM sets it only when creating a deal |
| Publishing back to Kafka | **Transactional outbox** + relay, so a webhook is never acknowledged without its event being persisted |
| Webhook security | HubSpot signature v3 (HMAC-SHA256, constant-time compare, 5-minute replay window) |
| Poison messages | Schema validation (zod), with a DLQ topic so a bad message never blocks a partition |

## Project layout

```
src/
  contracts/     Kafka topics + event schemas (zod), shared with the MCP server
  ingestion/     Kafka consumer -> inbox + projection + sync request
  sync/          SyncWorker (Postgres -> HubSpot), backoff, leasing
  hubspot/       CRM client, mapping, custom properties, webhook controller/signature
  outbox/        Outbox relay (Postgres -> Kafka)
  persistence/   pg pool, SQL migrations runner, repositories
  api/           Read/ops REST API, dev event endpoints, health
  mcp/           MCP server for AI agents
migrations/      Plain SQL migrations (applied on startup with an advisory lock)
tools/hubspot-mock/  Dependency-free HubSpot API mock + webhook simulator
test/            Black-box e2e test against docker compose
.github/workflows/ci.yml  lint, typecheck, unit tests, build, e2e on docker compose
```

## Known PoC limitations / next steps

- If a record is deleted in HubSpot, the stale mapping returns 404 and the sync is marked `failed`. The next step is to clear the mapping and re-create the record.
- No auth on the internal API, no metrics/tracing yet (add OpenTelemetry and Prometheus).
- Schema registry (Avro/Protobuf) instead of JSON + zod for cross-team contracts.
- HubSpot batch APIs (`/batch/upsert`) for high-volume backfills.
- kafkajs prints a harmless `TimeoutNegativeWarning` on Node 24. `@confluentinc/kafka-javascript` would be a drop-in replacement.
