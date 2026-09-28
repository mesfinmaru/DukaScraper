# Duka Scraper — Complete System Architecture

Exact architecture of the running system, extracted from the source code
(`app/`, `workers/`, `ui/`, `docker-compose.yml`, `database/`). Every flow
below is traceable to specific modules.

---

## 1. System Overview

Duka Scraper is a **Kafka-driven distributed crawling and intelligence
pipeline** for Amharic and English web content, with a surface web, deep web
(browser-automated), and dark web (Tor) tier. Classification (source type,
topic, threat category) is performed **post-parsing** by an LLM worker, never
at job creation time.

```
 ┌──────────┐   HTTPS/WS    ┌─────────────┐
 │  React   │──────────────▶│  FastAPI    │
 │  UI (Vite)│  REST + WS   │  API (:8000)│
 └──────────┘               └──────┬──────┘
                                   │ publish
                          ┌────────▼────────┐
                          │  Kafka          │  crawl.requests / crawl.raw /
                          │  (KRaft :29092) │  crawl.parsed / .retry / .dlq
                          └────────┬────────┘
        ┌──────────────┬───────────┼──────────────┬──────────────┐
        ▼              ▼           ▼              ▼              ▼
 ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐ ┌────────────┐
 │  surface   │ │    deep    │ │    dark    │ │   parser   │ │  exporter  │
 │  worker    │ │   worker   │ │   worker   │ │   worker   │ │   worker   │
 │ (httpx)    │ │ (Patchright│ │ (Tor socks5│ │ (BS4/lxml) │ │ (multi-sink│
 │            │ │ +CAPTCHA)  │ │  httpx)    │ │            │ │  batcher)  │
 └─────┬──────┘ └─────┬──────┘ └─────┬──────┘ └─────┬──────┘ └─────┬──────┘
       │              │              │              ▼              ▼
       │   escalate ▲ │              │        ┌────────────┐  MinIO duka-exports
       └────to deep─┘ │              │        │ llm-worker │  Elasticsearch
                      │              │        │ (Groq LLM, │  ClickHouse
                      └──────────────┘        │ Ollama+RAG │  PostgreSQL
                                              │ + Qdrant)  │
                                              └────────────┘
```

## 2. Runtime Topology (docker-compose, network `duka-net`)

| Service | Image / Build | Host port | Role |
|---|---|---|---|
| `duka-api` | built from `Dockerfile` | 8000 | FastAPI backend: auth, jobs, articles, storage, analytics, credentials, exports, monitoring, WebSockets, `/metrics` |
| `ui` (dev) | Vite dev server | 5173 (dev) | React SPA (`ui/src`) |
| `surface-worker` | `workers/surface-worker/Dockerfile` | — (health 8080) | HTTP-only crawler (httpx), proxies via Tor SOCKS5 |
| `deep-worker` | `workers/deep-worker/Dockerfile` | — (health 8080) | Patchright (patched Playwright) browser crawler, CAPTCHA solver, logins, auto-signup |
| `dark-worker` | `workers/dark-worker/Dockerfile` | — (health 8080) | Tor-only crawler for `.onion`/`.i2p` |
| `parser-worker` | `workers/parser-worker/Dockerfile` | — (health 8080) | HTML → clean text/sections, language detection, MinIO + Postgres persistence |
| `exporter-worker` | `workers/exporter-worker/Dockerfile` | — (health 8080) | Batch exporter to MinIO + Elasticsearch + ClickHouse |
| `llm-worker` | `workers/llm-worker/Dockerfile` | — (health 8080) | Groq (or Ollama) intelligence classification + RAG embeddings |
| `kafka` | `apache/kafka:3.9.1` (KRaft, single node) | 9092 / 29092 | Event backbone; auto-created topics |
| `kafka-topics-init` | one-shot | — | Creates the 5 topics (§4) |
| `kafka-ui` | provectuslabs/kafka-ui | 8088 | Topic inspection |
| `postgres` | postgres:16 | 5432 | `duka_system` DB — source of truth (schema below) |
| `pgadmin` | dpage/pgadmin4:9 | 5050 | Postgres UI |
| `minio` | minio/minio | 9000 (API) / 9001 (console) | Object storage: 3 buckets (§6) |
| `elasticsearch` | ES 8.12, security off | 9200 | `duka_articles` full-text index |
| `kibana` | 8.12 | 5601 | ES UI |
| `clickhouse` | clickhouse-server | 8123 (HTTP) | `duka_scraper` analytics DB (3 tables, §6) |
| `redis` | redis:7 | 6379 | Real-time job event bus (pub/sub + replayable history) |
| `qdrant` | qdrant | 6333 | Vector DB, collection `duka_articles` |
| `ollama-embedding` | ollama | 11435→11434 | `nomic-embed-text` embeddings |
| `tor` | dperson/torproxy | 9050 | SOCKS5 proxy for surface/dark workers |
| `prometheus` | prom/prometheus | 9090 | Scrapes `/metrics` from API + workers |
| `alertmanager` | prom/alertmanager | 9093 | PagerDuty / Slack alerting |
| `grafana` | grafana | 3000 | Dashboards (embedded in UI Monitoring page) |

## 3. Backend (FastAPI) Layout — `app/`

- `app/main.py` — app factory + lifespan: connects Postgres (bootstraps initial admin), Kafka producer, MinIO, ClickHouse, Elasticsearch (ensures `duka_articles` index), starts the Redis→WebSocket job-event listener. `/`, `/health`, `/ready` (503 when degraded), `/metrics`.
- Middleware: `RequestIDMiddleware` (error responses carry `request_id`), `PrometheusMiddleware`.
- Router mount (`app/api/routes/api.py`) under `settings.API_V1_STR` (`/api/v1`):
  | Prefix | Module | Responsibility |
  |---|---|---|
  | `/auth` | `routes/auth.py` | login, register, email verification, password reset/change, refresh, logout; JWT + DB-backed `auth_sessions` |
  | `/jobs` | `routes/jobs.py` | `POST /trigger`, `POST /batch`, job status/list, external-link discovery & review, dedup stats |
  | `/articles` | `routes/articles.py` | article reads (Postgres/ES) |
  | `/storage` | `routes/storage.py` | MinIO browser: raw / parsed / exports buckets, ZIP download |
  | `/analytics` | `routes/analytics.py` | ClickHouse analytics queries |
  | `/credentials` | `routes/credentials.py` | portal credential CRUD (Fernet-encrypted secrets) |
  | `/exports` | `routes/exports.py` | export records & file download |
  | `/monitoring` | `routes/monitoring.py` | system/worker health |
  | `/ws/jobs/{job_id}`, `/ws/jobs` | `websocket/job_status.py` | live job status/log/stage streams |
- Security (`app/security/`): Argon2 (`pwdlib`) hashing; JWT access+refresh pairs with `sid` claims bound to `auth_sessions` rows (revocable); role-based deps (`require_admin`); Redis-backed rate limiting (`rate_limit.py`).
- Services (`app/services/`): recursive crawling, link extraction, dedup, fingerprints, politeness, page validation, portal handler, auto-signup, credentials, dead-letter, Gmail verification, email.
- Language NLP (`app/language/`): cleaning, language detection (Ethiopic + en, fr, es, …), quality scoring, normalization, tokenizer, SimHash deduplication.
- Worker-assignment rules engine (`app/common/constants/worker_assignment.py`) — §5.

## 4. Kafka Backbone — topics, groups, message flow

Topics (created by `kafka-topics-init`, names in `app/pipeline/topics/topics.py`):

| Topic | Partitions | Producer(s) | Consumer group |
|---|---|---|---|
| `crawl.requests` | 10 | API (`kafka_producer`), recursion (`recursive_crawl_service`) | surface-worker, deep-worker, dark-worker (own groups) |
| `crawl.raw` | 10 | surface-worker, dark-worker | parser-worker |
| `crawl.parsed` | 10 | parser-worker **and** deep-worker (deep parses itself) | exporter-worker, llm-worker |
| `crawl.requests.dlq` | 5 | `dead_letter_service.publish_crawl_dead_letter` (terminal validation failures) | manual/ops |

Message schemas (`app/pipeline/schemas.py`, Pydantic, keyed by `job_id`):

- **`CrawlRequest`** → `crawl.requests`: `job_id, url, language ('am'|'en'), worker_type, job_params, depth (0=seed), max_depth (default 5), parent_url, recursive_config, retry_count, escalation_reason, auto_signup, credential_email`.
- **`CrawlResult`** → `crawl.raw`: `job_id, item_id, url, html, status_code, worker, language, network, fetch_duration, depth, extracted_links, child_tasks_queued, duplicate_links_skipped, was_escalated, escalation_reason`.
- **`ParsedItem`** → `crawl.parsed`: `job_id, item_id, url, worker, language, data: ParsedItemData (extracted_text, character_count, title, publish_date, detected_language, fetch_duration, payload_size_bytes, proxy_ip, retry_count, portal_structured_data, requested_language, language_mismatch, language_rejection_reason), status, parse_duration`.
- **`IntelligenceAnalytics`** → ClickHouse (not Kafka): `job_id, item_id, url, source_type, topic, category, threat_severity (1–5), entities[], summary, language, llm_model, created_at`.

ID conventions (Postgres-generated): `USR#####` (users), `JOB########` (jobs), `ITEM##########` (parsed_items), `EXP########` (exports).

## 5. Worker Routing — Rules Engine (`WorkerAssignmentEngine`)

Priority chain, evaluated at submission time (pre-fetch, deterministic):

1. `worker_override` (user escape hatch)
2. `.onion`/`.i2p`/`.loki`/`.zeronet` TLD → **DARK**
3. Known WAF/CDN domains (cloudflare, shopify, banks…) → **DEEP**
4. Auth/JS-heavy platform whitelist (social, mail, github, amazon…) → **DEEP**
5. Ethiopian gov/telecom domains → **DEEP**
6. Ethiopian curated surface-safe domains (news/academic) → **SURFACE**
7. URL path heuristics (`/login`, `/admin`, `/checkout`, `/sso`, …) → **DEEP** (unknown domains only)
8. Query-param heuristics (`oauth`, `sso`, `token`, …) → **DEEP**
9. `.et` TLD → **SURFACE** (escalates automatically if wrong)
10. Default → **SURFACE**

Forced override in `app/pipeline/main.py`: any job with `auto_signup`, `allow_login`, `allow_signup`, or `credentials` in `job_params` is routed to **DEEP** (browser automation is deep-worker only).

## 6. Storage Layer

**PostgreSQL `duka_system`** (`database/postgres/01_duka_system.sql` — single source of truth; served by `app/storage/postgres/client.py`):

| Table | Purpose |
|---|---|
| `users` | auth + RBAC (`role`, `is_active`, `email_verified`, `must_change_password`) |
| `verification_tokens` | email verification / password reset (hashed, expiring) |
| `auth_sessions` | DB-backed refresh-token sessions (revocation, rotation) |
| `audit_logs` | admin/user action trail |
| `jobs` | orchestration container; `status ∈ {pending, running, completed, failed, skipped, needs_review}` |
| `credential_usage` | per email+domain portal credentials; usage tracking; IMAP/Gmail OAuth (encrypted columns) |
| `parsed_items` | per-page metadata **only** — text lives in MinIO; `worker_type`, `is_exported`, `intelligence_processed` |
| `exports` | export records (csv/json/parquet), status + `file_path` |
| `crawl_log` | per-URL crawl events (event_type, status, retry_count) |
| `discovered_external_links` | out-of-seed links found during recursion; `pending/approved/rejected/auto_approved` |
| `content_fingerprints` | 3-tier dedup: url_exact / content_exact (SHA) / near_duplicate (SimHash) |

**ClickHouse `duka_scraper`** (`database/clickhouse/01_init_db.sql`):
- `scraped_analytics` — per-item crawl/parse stats (MergeTree, ordered by domain+time)
- `crawler_performance` — per-attempt worker performance (latency, proxy_ip, retry_count, payload bytes); written by the three fetch workers — surface, dark, deep (`ch_client.write_crawler_performance`)
- `intelligence_analytics` — LLM output (source_type, category, threat_severity, entities, summary, llm_model)

**MinIO buckets** (`app/storage/minio/client.py`, naming in `app/common/utils/minio_naming.py`):
- `duka-raw-data` — raw HTML per crawl (`{job_id}/{host}_{sha16}.html`-style objects)
- `duka-parsed-data` — parsed JSON per item
- `duka-exports` — batched export files

**Elasticsearch** — `duka_articles` index (full-text `extracted_text`, created by exporter; ensured at API startup).

**Redis** — channel `duka:job_updates`; replayable per-job history lists `duka:job_logs:{job_id}` and `duka:job_stages:{job_id}` (capped at 500 entries, 7-day TTL).

**Qdrant** — collection `duka_articles`; vector = Ollama `nomic-embed-text` embedding of parsed text; used for RAG context by llm-worker.

---

## 7. End-to-End Flows (exact)

### Flow A — Job Submission (synchronous, request → Kafka)

1. UI (or external client) `POST /api/v1/jobs/trigger` (`ScrapeRequest`: url, user_id, language, optional `worker_override`, `max_depth`, `recursive_config`, `job_params`).
2. `submit_crawl_job` (`app/pipeline/main.py`):
   a. **Rules engine** assigns `worker_type` + `assignment_reason` (§5).
   b. Forced DEEP if `job_params` request auth/signup (§5).
   c. `pg_client.ensure_user(user_id)` (auto-normalizes/creates user row).
   d. `pg_client.create_job(...)` → `jobs` row, `status='pending'`, `job_id=JOB########`. Publishes `job_created` event.
   e. `recursive_config.seed_url` injected + `same_domain_only=True` by default.
   f. `update_job_status(job_id, 'running')` (event `job_status`).
   g. `kafka_producer.publish_crawl_request(CrawlRequest)` → **`crawl.requests`** (key=`job_id`).
3. Response `202` with `job_id`, `assigned_worker`, `assignment_reason`, `kafka_topic`.
   Batch variant `POST /jobs/batch` loops per URL, merging `allow_login/signup/email_verification` + `credential_email` into `job_params`.

### Flow B — Surface Crawl (HTTP tier)

`workers/surface-worker/main.py` — consumes `crawl.requests`, group `surface-worker`:

1. Validate `CrawlRequest`; robots/politeness check (`PolitenessService`) — disallowed → `record_crawl_log(... 'robots_disallowed')`, skip.
2. URL-dedup pre-check (`DedupService` Bloom filter + `content_fingerprints`); visited → log `dedup_url_exact`, skip.
3. Fetch via `httpx` through rule-based proxy pool (`shared_proxy_manager`; Tor SOCKS5 in compose) with agent rotation (`app/agents/`).
4. Escalation check `check_escalation(status_code, html, 'surface')` (`worker_assignment.py`): HTTP 401/403/407/429/451, auth-form/challenge HTML patterns, empty SPA shells → **`escalate_to_deep`**: bump `retry_count`, set `escalation_reason`, re-publish to `crawl.requests` (circuit breaker `MAX_RETRY_COUNT=3`).
5. Page validation (`PageValidationService`): reject → `record_crawl_log` + `publish_crawl_dead_letter` → **`crawl.requests.dlq`**.
6. On success: raw HTML → MinIO `duka-raw-data`; `CrawlResult` → **`crawl.raw`**; `ch_client.write_crawler_performance(...)` → ClickHouse; `record_crawl_log(..., 'completed')`.
7. Recursion: `extract_and_queue_children` (§ Flow F) queues child `CrawlRequest`s back onto `crawl.requests`.

### Flow C — Dark Crawl (Tor tier)

`workers/dark-worker/main.py` — consumes `crawl.requests`, produces `crawl.raw` (same contracts as surface):

1. Tor SOCKS5 (`TOR_SOCKS5_PROXY`) routed httpx fetch for `.onion`/`.i2p`.
2. Anti-bot/challenge detection and page validation → escalate to DEEP (`escalate_to_deep`, same `MAX_RETRY_COUNT=3` breaker) or DLQ on terminal failure.
3. Success → raw HTML to MinIO `duka-raw-data`, `CrawlResult` → `crawl.raw`, crawl_log entries, recursion children queued.

### Flow D — Deep Crawl (browser tier, parses in place)

`workers/deep-worker/main.py` — consumes `crawl.requests`, **publishes directly to `crawl.parsed`** (bypasses parser-worker):

1. Patchright (patched Playwright) Chromium: fingerprint spoofing (browserforge `Screen`), CDP/automation markers stripped, hardened Chromium flags.
2. Navigation: stage 1 `goto(wait_until="commit")` (headers captured without full DOM); stage 2 fallback `domcontentloaded` on timeout.
3. Challenge pipeline: `UniversalCaptchaSolver` — Cloudflare Turnstile, reCAPTCHA v2 audio (`playwright_recaptcha`), static-image OCR (`pytesseract`); publishes live stages `challenge_detected` / `challenge_solved` to Redis.
4. Portal handling: `PortalHandler` + `configs/domains/*` portal configs; `generic_portal_login`; HTTP interstitials.
5. Auto-signup path (`auto_signup_handler` + `credential_service` + `gmail_verification`): detects signup vs login, registers a managed email credential, reads verification codes (IMAP/Gmail API), verifies, logs in. Enabled only when `auto_signup=True`; every stage is published as `job_stage` events (`signup`, `login`, `verification`); credentials tracked in `credential_usage`.
6. On success: raw HTML → MinIO `duka-raw-data`, parsed JSON → MinIO `duka-parsed-data`, `ParsedItem` → **`crawl.parsed`** directly; language detection/quality scoring inline; recursion children queued. Deep is the **final** escalation handler — no further escalation.

### Flow E — Parser (normalizes surface/dark output)

`workers/parser-worker/main.py` — consumes `crawl.raw`, group `parser-worker`:

1. Read raw HTML from `CrawlResult`; skip own-loop items by `item_id` match.
2. BeautifulSoup/lxml extraction: boilerplate removal, heading-based section chunking, `clean_and_extract_text` (Amharic-aware), `detect_language_from_text` (Ethiopic set ∪ {en}); quality scoring.
3. **Persist `parsed_items` row FIRST** (Postgres, preserving the `item_id` minted at crawl time), with `worker_type`, counts, `raw_html_path` + `parsed_json_path`.
4. Content fingerprinting (`generate_fingerprint`: URL fingerprint + content SHA + SimHash) → `content_fingerprints` table; duplicates flagged (`DuplicateContentError` path → item marked failed/duplicate).
5. Language gate: mismatch/unsupported → item flagged `language_mismatch` / `language_rejection_reason`, marked `needs_review` — **not** published downstream.
6. Success: parsed JSON → MinIO `duka-parsed-data`; `ParsedItem` → **`crawl.parsed`** (key=`job_id`); job status event `item_parsed` published to Redis.

### Flow F — Recursive Crawling (all three crawl workers)

`app/services/recursive_crawl_service.extract_and_queue_children` (called by surface, dark, deep with the same producer):

1. Extract links (`LinkExtractionService`) from the fetched HTML → normalized absolute URLs (`CrawlResult.extracted_links`).
2. Filter: `recursive_config.link_filter_patterns`, `skip_domains`, `same_domain_as=seed_url`, `path_prefix` derived from the seed URL's path (keeps the crawl inside the seed's subtree, e.g. `bbc.com/amharic → /amharic/...` only).
3. Per link: `DedupService.is_visited` (Bloom filter + Redis) → `mark_visited`; re-run **rules engine** on the child URL (`assign_worker`) — children can change tiers (e.g., a `/login` child routes to DEEP).
4. Publish child `CrawlRequest` (same `job_id`, `depth+1`, `parent_url`) → `crawl.requests`. Stop at `max_depth` (default 5); Bloom filter released at the final level.
5. External-domain links (out of seed scope) are recorded in `discovered_external_links` (status `pending`) instead of being crawled — surfaced to the user via `GET /jobs/{job_id}/external-links[/domains]` and the review endpoint; `POST /jobs/{job_id}/external-links/review` with `auto_crawl=true` queues approved links as **new jobs**.

### Flow G — Exporter (fan-out sink)

`workers/exporter-worker/main.py` — consumes `crawl.parsed`, group `exporter-group`:

1. Batches items (`export_batch_size`, flush interval) with `BatchExportManager`.
2. Per batch: `pg_client.create_export(...)` → `exports` row (`EXP########`, status `pending`).
3. Per item: index into Elasticsearch `duka_articles`; insert ClickHouse `scraped_analytics`; then `mark_item_exported(item_id)`.
4. Batch files (CSV) written to MinIO `duka-exports/{job_id}/...`; export row updated with `file_path`, `status='completed'`, `file_size_mb` (or `failed`).
5. (UI-triggered JSON/Parquet exports go through `POST /exports` → `exports.py` on demand, reading from MinIO `duka-exports`.)

### Flow H — LLM Intelligence + RAG

`workers/llm-worker/main.py` — consumes `crawl.parsed`, group `llm-worker`:

1. For each parsed item: build prompt from `extracted_text`; optional **RAG**: embed text via Ollama (`nomic-embed-text` @ `ollama-embedding:11434`), store vector in Qdrant `duka_articles` (`embedding_rag.py`), retrieve similar docs as context.
2. Call LLM: `LLM_PROVIDER=hosted` → Groq `openai/gpt-oss-120b` (`HOSTED_LLM_URL` + key), or local Ollama fallback.
3. Structured output → `IntelligenceAnalytics`: `source_type` (news/forum/blog/social/gov/academic/ecommerce/other), `topic`, `category` (`data_leak`, `gov_issue`, `cyber_threat`, `physical_threat`, `misinformation`, `other`), `threat_severity` (1–5), `entities`, `summary`.
4. Insert into ClickHouse `intelligence_analytics` (HTTP interface via `clickhouse_connect`).
5. `pg_client.mark_item_intelligence_processed(item_id)` → `parsed_items.intelligence_processed = true`.
6. Re-failed/invalid items are skipped (no ClickHouse row).

### Flow I — Real-Time Job Events (Redis → WebSocket → UI)

1. Every state transition publishes to Redis:
   - `pg_client` publishes `job_created` / `job_status` / `item_parsed` on every job/item write (`app/common/job_events.publish_job_event`).
   - Deep worker publishes `job_stage` events (`queued, fetching, challenge_detected, challenge_solved, signup, login, verification, parsing, completed`) and console `job_log` lines (root-logger `JobLogRelayHandler` routes any log containing `[JOBxxxxx]`, or lines emitted under `set_current_job_id` context).
2. Publishing is **best-effort** — Redis failure never blocks the pipeline; history lists (`job_logs` / `job_stages`, 500 lines, 7-day TTL) allow late-joining clients to replay.
3. API background task (`start_job_updates_listener`) subscribes to `duka:job_updates` (`iter_job_events`) and fans every event out to:
   - `/api/v1/ws/jobs/{job_id}` (per-job; sends `connected`, `initial_status` from Postgres, then replays stored stage + log history before live events),
   - `/api/v1/ws/jobs` (feed; **JWT-authenticated** via `token` query param; session validated against `auth_sessions`; non-admins may only subscribe to their own `user_id`, admin gets all).
4. UI: `useRealtimeSocket` (`ui/src/useRealtime.ts`) drives `JobDetail.tsx` (live status + stage timeline + console) and `Jobs.tsx` (live feed); `api.ts` builds the WS URLs and falls back to periodic REST polling when Redis/WS is unavailable.

### Flow J — Authentication & Authorization

1. `POST /auth/login` → verify Argon2 hash → `create_token_pair`: access JWT (`typ=access`, `exp`, `sub`, `sid`) + refresh JWT (`typ=refresh`); a random `session_id` is registered in `auth_sessions` (expires per `REFRESH_TOKEN_EXPIRE_DAYS`).
2. `get_current_user` dependency: decode access token, require `typ=access` + `sub` + `sid`, check `is_auth_session_active(sid)`, load user, enforce `is_active`/`email_verified`; role checks via `require_admin`.
3. Refresh: `POST /auth/refresh` validates refresh token, revokes old session (`revoke_auth_session`), issues a **rotated** pair (new `sid`).
4. Registration → `verification_tokens` row (hashed token) → email via `email_service`/`gmail_verification` → `email_verified=true`.
5. WebSocket feed auth (§ Flow I) reuses the same JWT + session checks.
6. All admin/user actions land in `audit_logs`.

### Flow K — Content Deduplication (3 tiers)

1. **Pre-crawl (workers):** URL Bloom-filter/Redis check (`DedupService`) + `check_url_duplicate` (`content_fingerprints`, 24h staleness) — visited URLs skipped, logged as `dedup_url_exact`.
2. **Post-parse (parser-worker):** `content_fingerprint` (SHA-256 of normalized text) → exact-content duplicates flagged; `simhash` → near-duplicates (`duplicate_type ∈ {url_exact, content_exact, near_duplicate}`, `duplicate_of` link).
3. **Query-time:** `GET /jobs/{job_id}/dedup/stats`, per-item duplicates, and `GET /jobs/{job_id}/dedup/check?url=` for "already crawled" warnings in the UI.

## 8. Observability

- Every process (API + 6 workers) exposes Prometheus metrics: API `/metrics` (`PrometheusMiddleware`), workers via `workers/metrics.py` `WorkerMetrics` (messages consumed, consumer lag, escalation counters) on `HEALTH_PORT` + `/health` liveness (`workers/health.py`).
- Prometheus (`monitoring/prometheus.yml` + `monitoring/alerts/`) scrapes; Alertmanager (`monitoring/alertmanager/`) routes to PagerDuty/Slack; Grafana (provisioned datasources/dashboards, anonymous viewer, embedding enabled) is iframed in the UI **Monitoring** page.
- Structured JSON logging in docker (`app/common/logger/logger.setup_logging`); Kafka message processing logs partition+offset per worker.

## 9. Exact Kafka Message Journey (happy path, surface URL)

```
POST /api/v1/jobs/trigger (fanabc.com/news)
  └─ submit_crawl_job
       ├─ WorkerAssignmentEngine → SURFACE (ethiopian_surface_domain)
       ├─ jobs row (pending → running) + Redis job_created/job_status
       └─ crawl.requests  ← CrawlRequest{depth:0, max_depth:5}
surface-worker
  ├─ politeness ok, not visited
  ├─ httpx fetch (200) via proxy pool
  ├─ no escalation signals, page valid
  ├─ MinIO: duka-raw-data/{job_id}/...html
  ├─ ClickHouse: crawler_performance
  └─ crawl.raw ← CrawlResult{html, extracted_links...}
       └─ extract_and_queue_children → crawl.requests (depth+1 children, same job_id)
parser-worker
  ├─ parsed_items row (ITEM##########, worker_type=surface)
  ├─ content_fingerprints (url/content/simhash)
  ├─ MinIO: duka-parsed-data/{...}.json
  └─ crawl.parsed ← ParsedItem
        ├──────────────────────────▶ exporter-worker
        │      ├─ exports row (EXP########)
        │      ├─ ES: duka_articles doc
        │      ├─ ClickHouse: scraped_analytics
        │      ├─ parsed_items.is_exported = true
        │      └─ MinIO: duka-exports/{job_id}/...csv (completed)
        └──────────────────────────▶ llm-worker
               ├─ Qdrant: vector upsert (nomic-embed-text)
               ├─ Groq gpt-oss-120b → source_type/category/threat_severity/entities/summary
               ├─ ClickHouse: intelligence_analytics
               └─ parsed_items.intelligence_processed = true
UI (JobDetail) ◀── WS /api/v1/ws/jobs/JOB######## ◀── Redis duka:job_updates
                                                        (stage + log events throughout)
```

## 10. Failure & Resilience Paths (exact)

| Condition | Behavior |
|---|---|
| Kafka unavailable at submit | API returns `503` ("Kafka is unavailable") — no job row is created |
| Surface/dark fetch hits 401/403/407/429/451, auth form, bot challenge, empty SPA shell | `escalate_to_deep` → re-publish `CrawlRequest` (retry_count+1, escalation_reason) to `crawl.requests`; deep worker handles it in a browser |
| retry_count ≥ 3 | Circuit breaker: give up, log; no infinite escalation loop |
| Page validation failure (terminal) | `publish_crawl_dead_letter` → `crawl.requests.dlq`; crawl_log records reason |
| Language mismatch / unsupported language | Item marked `needs_review`, **not** exported or LLM-analyzed |
| Duplicate content | Item rejected at parse time (`DuplicateContentError`), recorded in `content_fingerprints` |
| Redis down | Job-event publish silently skipped; UI falls back to REST polling; pipeline unaffected |
| Postgres/MinIO/CH/ES down at API boot | Lifespan records `false` per dependency; `/health` stays 200 (liveness), `/ready` returns 503 (readiness) |
| Deep worker is final escalation tier | No further re-routing; failures surface as job `failed`/`needs_review` with `failure_reason` (`GET /jobs/{job_id}`) |
| LLM worker item failure | Skipped without ClickHouse insert; item keeps `intelligence_processed=false` |

## 11. Cross-Cutting Invariants

- **IDs:** all Postgres IDs are generated strings (`USR/JOB/ITEM/EXP` + zero-padded sequences); Kafka messages carry `job_id` as key (per-job partition ordering).
- **source_type is never known at submit time** — it is LLM-derived, stored only in ClickHouse `intelligence_analytics`.
- **Parsed items are metadata in Postgres, content in MinIO** — `raw_html_path` / `parsed_json_path` point at objects; text is only duplicated in ES (search) and Qdrant (vectors).
- **The parser-worker owns `item_id` generation truth** (preserves the `item_id` minted at crawl time); the exporter never mints IDs, it only marks `is_exported`.
- **A job is an orchestration container**; worker ownership is per-`parsed_items` row (`worker_type`), since one job can span surface/deep/dark items through recursion + escalation.
- **`crawl.parsed` has exactly two producers** — parser-worker (surface/dark items) and deep-worker (self-parsed items); both consumers (exporter, llm-worker) are independent groups so each sees the full stream.
