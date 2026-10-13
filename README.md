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
docker compose up -d --build --wait     # postgres, kafka (KRaft), kafka-ui, hubspot-mock, cdm, grafana (otel-lgtm)
npm ci && npm run test:e2e              # full round-trip test against the running stack
```

| Service      | URL                                   |
| ------------ | ------------------------------------- |
| CDM API      | http://localhost:3000                 |
| Grafana      | http://localhost:3001                 |
| HubSpot mock | http://localhost:4010/__mock/state    |
| Kafka UI     | http://localhost:8080                 |
| Kafka (host) | localhost:29092                       |
| Postgres     | postgres://cdm:cdm@localhost:5432/cdm |

## Watch it live (no commands needed)

```bash
docker compose --profile demo up -d --build --wait
```

The `demo` profile adds [tools/demo-traffic](tools/demo-traffic/run.ts). It seeds 200 affiliates + 400 deals once, then keeps simulating Legend services and sales reps:

- every 3 s: a new affiliate and 2 deals, sometimes a tier upgrade or a HubSpot deal-stage change (signed webhook)
- every 90 s: a burst of 120 affiliates, which switches the worker to the bulk (batch API) path

Open these and just observe:

| What | Where | What to look at |
| --- | --- | --- |
| Business dashboard | [localhost:3001/d/cdm-overview](http://localhost:3001/d/cdm-overview) | Sync lag SLI, backlog, single vs bulk results, HubSpot latency / 429s, webhooks, live logs |
| Distributed traces | Grafana -> Explore -> **Tempo** -> Search, service `commercial-data-manager` | `POST /dev/events/affiliate` -> Kafka produce -> consume -> Postgres; `bulk sync deal` -> HubSpot batch calls |
| Logs <-> traces | Grafana -> Explore -> **Loki**, `{service_name="commercial-data-manager"}` | Click a log's `trace_id` to jump to its trace |
| Kafka topics | [localhost:8080](http://localhost:8080) | `legend.affiliates.v1`, `legend.deals.v1`, `cdm.hubspot-changes.v1` (stage changes), `cdm.dead-letter.v1` |
| "HubSpot" | [localhost:4010/__mock/stats](http://localhost:4010/__mock/stats), [/__mock/state](http://localhost:4010/__mock/state) | API calls per endpoint, records and associations |
| CDM API | [localhost:3000/sync/stats](http://localhost:3000/sync/stats), `/affiliates/demo-aff-1`, `/deals/demo-deal-1` | Projection, HubSpot IDs, sync state |
| Generator log | `docker compose logs -f demo-traffic` | What is being simulated right now |

Try breaking things while watching the dashboard:

```bash
MOCK_RATE_LIMIT_PERCENT=30 docker compose up -d --force-recreate hubspot-mock   # 30% HubSpot 429s
docker compose stop hubspot-mock    # HubSpot outage: lag and backlog grow, ingestion continues
docker compose start hubspot-mock   # recovery: backlog drains through the bulk path
curl -X POST localhost:3000/sync/resync -H 'content-type: application/json' -d '{}'   # full backfill
```

Stop the generator with `docker compose stop demo-traffic`.

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

## Bulk sync and backfills (HubSpot batch APIs)

The worker claims up to `SYNC_BATCH_SIZE` (100) due entities per tick and picks a mode per entity type:

- **fewer than `SYNC_BULK_THRESHOLD` (10)**: per-record calls, lowest latency for live traffic
- **10 or more** (backfill, replay, recovery after an outage): batch APIs, 100 records per call

| Entity | Batch calls per 100 records | Why |
| --- | --- | --- |
| Affiliate | `companies/batch/upsert` (by `legend_affiliate_id`), `contacts/batch/upsert` (by `email`), `associations/contacts/companies/batch/associate/default` | No create-only fields, so upsert is safe |
| Deal | `deals/batch/read` (by `legend_deal_id`), `deals/batch/create` + `deals/batch/update`, `associations/deals/companies/batch/associate/default` | `dealstage` is owned by sales, so it is set only on create. Upsert would overwrite it |

Partial failures (HTTP 207) are mapped back per record, and only failed records are retried. Batch reads and upserts by unique property are strongly consistent, unlike search, so the bulk path also heals stale mappings (records deleted in HubSpot).

Trigger a backfill with `POST /sync/resync` (body `{"entityType": "affiliate"}`, or `{}` for all). In the e2e run, re-pushing 317 entities took 24 HubSpot calls instead of about 1,100.

## Observability (OpenTelemetry)

[src/observability/otel.ts](src/observability/otel.ts) starts the OTel Node SDK before anything else. It is a no-op unless `OTEL_EXPORTER_OTLP_ENDPOINT` is set. Everything goes over OTLP to [grafana/otel-lgtm](https://github.com/grafana/docker-otel-lgtm) (Tempo, Prometheus, Loki, Grafana).

- **Traces:** HTTP, Express/Nest, Postgres, kafkajs (context propagated in Kafka headers), fetch to HubSpot, plus manual `sync <entity>` / `bulk sync <entity>` spans. Poller queries are only traced inside real work (`requireParentSpan`).
- **Metrics:** `cdm.events.ingested`, `cdm.sync.results` (entity, mode, result), `cdm.sync.batch.size`, `cdm.hubspot.request.duration` (operation, status), `cdm.webhook.events`, `cdm.outbox.published`, and DB-backed gauges `cdm.sync.entities` and `cdm.sync.lag`.
- **Logs:** the Nest logger also emits OTLP log records, so every line carries `trace_id` / `span_id`.
- **Dashboard:** [observability/grafana/dashboards/cdm-overview.json](observability/grafana/dashboards/cdm-overview.json), provisioned automatically.

The SLI is **sync lag**: the age of the oldest change not yet in HubSpot. Alerting on it catches HubSpot outages, rate-limit storms and stuck workers with one rule.

## Real HubSpot

Copy `.env.example` to `.env`, then set a private-app token and client secret. After that, run `docker compose up -d --build cdm`. On startup, CDM creates its custom properties (`legend_affiliate_id`, `legend_deal_id`, etc.). To receive webhooks, expose `/webhooks/hubspot` (e.g. with ngrok), set `WEBHOOK_PUBLIC_BASE_URL`, and subscribe to `deal.propertyChange` for `dealstage`.

## Agentic AI (MCP)

[.vscode/mcp.json](.vscode/mcp.json) registers the CDM MCP server ([src/mcp/main.ts](src/mcp/main.ts)). It runs inside the `cdm` container through `docker compose exec`, so no local setup is needed. Tools: `get_affiliate`, `get_deal`, `get_sync_stats`, `list_sync_states`, `retry_sync`, `publish_affiliate_event`, `publish_deal_event`.

Example prompt in Copilot agent mode: *"Create a gold affiliate 'Spin Media' with a REVSHARE deal, then check whether both synced to HubSpot. If anything failed, explain why."*

The MCP server is a thin adapter over the REST API, so agents get the same validation and guardrails as humans. Each tool call is an OTel span whose trace continues into CDM, so you can see in Tempo exactly what an agent did.

### How the agent does the job

```mermaid
sequenceDiagram
  autonumber
  actor U as Engineer
  participant A as AI agent (Copilot / Claude)
  participant M as CDM MCP server (stdio)
  participant API as CDM REST API
  participant S as CDM internals (Kafka, Postgres, SyncWorker)
  participant HS as HubSpot

  U->>A: "Onboard Spin Media with a REVSHARE deal and verify it reached HubSpot"
  A->>M: tools/list
  M-->>A: 7 tools with JSON schemas (from the zod contracts)
  note over A: Plan: publish affiliate, publish deal, verify, diagnose if needed

  A->>M: publish_affiliate_event(affiliateId, companyName, contact, tier)
  M->>API: POST /dev/events/affiliate (schema-validated)
  API->>S: produce legend.affiliates.v1
  A->>M: publish_deal_event(dealId, affiliateId, REVSHARE, amount)
  M->>API: POST /dev/events/deal
  API->>S: produce legend.deals.v1
  S->>HS: async sync (company, contact, deal, associations)

  loop until synced or failed
    A->>M: get_deal(dealId)
    M->>API: GET /deals/:id
    API-->>A: hubspotDealId, sync.status, sync.lastError
  end

  alt sync failed
    A->>M: list_sync_states(status = failed)
    M-->>A: lastError per entity
    note over A: Reason about the root cause (e.g. affiliate missing, 4xx validation)
    A->>M: retry_sync(entityType, entityId)
  end
  A-->>U: Summary with HubSpot IDs, final status, and what was fixed
```

```mermaid
flowchart LR
  P[Prompt / goal] --> T[Discover tools<br/>tools/list]
  T --> PL[Plan]
  PL --> ACT[Act<br/>publish / retry]
  ACT --> OBS[Observe<br/>get_deal / get_sync_stats]
  OBS -->|not done| PL
  OBS -->|done| R[Report to human]
  subgraph Guardrails
    G1[zod schemas on every tool]
    G2[Same REST API + validation as humans]
    G3[Dev tools disabled in prod]
    G4[Every call traced in Tempo]
  end
  ACT -.-> Guardrails
```

The agent never touches Kafka, Postgres or HubSpot directly. It only uses the same audited API, and the asynchronous, idempotent design makes "act, then poll for the outcome" safe to repeat.

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
  observability/ OTel SDK bootstrap, business metrics, OTLP logger
migrations/      Plain SQL migrations (applied on startup with an advisory lock)
observability/   Grafana dashboard provisioning
tools/hubspot-mock/   Dependency-free HubSpot API mock (incl. batch APIs) + webhook simulator
tools/demo-traffic/   Continuous simulated traffic for demos (compose profile `demo`)
test/            Black-box e2e test against docker compose
.github/workflows/ci.yml  lint, typecheck, unit tests, build, e2e on docker compose
```

## Known PoC limitations / next steps

- The per-record path still marks a sync `failed` on 404 when a record was deleted in HubSpot. The bulk path heals it. Next step: clear the mapping and fall back to find-or-create.
- No auth on the internal API.
- The outbox relay starts a new trace. Storing `traceparent` in the outbox row would continue the webhook trace through Kafka to consumers.
- Schema registry (Avro/Protobuf) instead of JSON + zod for cross-team contracts.
- kafkajs prints a harmless `TimeoutNegativeWarning` on Node 24. `@confluentinc/kafka-javascript` would be a drop-in replacement.
