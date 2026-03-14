# Fortress Pipeline Contract

## Purpose

This document defines the execution contract for the Fortress enrichment pipeline.

The pipeline takes a user query (e.g. "transport 66"), finds matching French companies in the SIRENE registry, and enriches them with contact data from Google Maps and company websites. It produces qualified contacts with phone, email, website, and social links.

The pipeline does **not**: modify master company identity data, run inside the API request path, or perform any operation without tracked persistence.

## Source of Truth

* **Schema truth:** `database/schema.sql`
* **Runtime truth:** `runner.py`, `enricher.py`, `batch_processor.py`, `deduplicator.py`
* **This document:** human-readable contract derived from code inspection

If this document conflicts with application code, **code wins**, and this document must be corrected immediately.

---

## Enforcement Levels

* **Schema guarantee** — enforced by PostgreSQL
* **Application rule** — enforced in Fortress code
* **Operational policy** — required practice, not code-enforced
* **Current limitation** — known gap, not yet addressed

---

# 1. Pipeline Invariants

These rules are non-negotiable. Any code change that violates them is a regression.

1. **`companies` is master identity only** — the enrichment pipeline does not write to the `companies` table's identity fields. Only the SIRENE import pipeline owns those.
2. **Contact writes are idempotent upserts** — `ON CONFLICT (siren, source) DO UPDATE` with COALESCE semantics. Re-running the same batch cannot corrupt data.
3. **Qualified companies are persisted immediately** — the `on_save` callback writes each company+contact to DB the instant enrichment qualifies it, before the wave completes.
4. **Zero Maps presence is required before rejection** — a company is only written to `rejected_sirens` when Maps returns absolutely nothing.
5. **Location mismatch alone is not rejection** — companies with Maps data in a different city are kept (multi-office rule). Low-confidence results discard phone only, not the entire record.
6. **Pipeline must fill the target batch via replacement** — if a company fails enrichment, it is replaced with another from the same NAF+department pool until `batch_size` is satisfied or the pool is exhausted.
7. **Per-company failures do not crash the wave** — individual enrichment errors are logged and the company is replaced, not retried.
8. **Chrome starts before heavy DB queries** — browser must be warm before `interpret_query` runs (prevents resource contention failures).

---

# 2. Stage Model

## 2.1 Query Interpretation

| | |
|---|---|
| **Owner** | `module_a/query_interpreter.py` |
| **Input** | `query_name` (e.g. "transport 66"), `batch_offset`, `limit` |
| **Output** | `QueryResult` containing company list + metadata |
| **Side effects** | None — read-only SQL on `companies` table |
| **Failure mode** | `statement timeout` (60s) → job marked `failed`, pipeline exits |
| **Idempotency** | Yes — pure SELECT |

**Behavior:**
- Parses user input into NAF code prefixes + département code via fuzzy matching
- Runs `SELECT` against 14.7M-row `companies` table
- Fetches `requested_size × 2` candidates (headroom for replacement loop)
- **Application rule:** 60-second SQL statement timeout on all pool connections

---

## 2.2 Triage

| | |
|---|---|
| **Owner** | `module_a/triage.py` |
| **Input** | Company list from Query Interpretation + `raw_query` |
| **Output** | `TriageResult` with five buckets: BLACK, BLUE, GREEN, YELLOW, RED |
| **Side effects** | GREEN companies tagged in `query_tags` via `bulk_tag_query()` |
| **Failure mode** | Exception → propagates to runner → job `failed` |
| **Idempotency** | Yes — tagging is `ON CONFLICT DO NOTHING` |

**Classification rules:**

| Color | Condition | Action |
|-------|-----------|--------|
| **BLACK** | SIREN in `blacklisted_sirens` | Skip entirely |
| **BLUE** | SIREN in `client_sirens` | Skip (client already owns) |
| **GREEN** | All MVP fields exist in **Data Bank** | Tag immediately, no scraping (Smart Reuse) |
| **YELLOW** | Partial data in Data Bank | Queue for targeted enrichment |
| **RED** | Never enriched (not in Data Bank)| Queue for full pipeline |

**Post-triage:**
- Scrape queue = `YELLOW + RED` (YELLOW first — cheaper)
- Queue capped at `requested_size` (user's `batch_size`)
- `scrape_jobs` updated: `triage_*` counts, `total_companies`, `wave_total`, `wave_current=0`

---

## 2.3 Enrichment

| | |
|---|---|
| **Owner** | `module_d/enricher.py` |
| **Input** | Wave of companies (from batch_processor), `pool`, `curl_client`, `maps_scraper` |
| **Output** | `(list[Contact], replaced_count)` |
| **Side effects** | Writes to `contacts`, `companies`, `query_tags`, `scrape_audit`, `enrichment_log`, `rejected_sirens` (all via `on_save` and end-of-loop persistence) |
| **Failure mode** | Per-company exception → logged as `failed` in `enrichment_log`, company replaced |
| **Idempotency** | Yes — all writes use `ON CONFLICT` upserts |

### 2.3.1 Per-Company Enrichment Pipeline

**Step 1: Google Maps (Playwright)**
- Search: `"denomination ville"` on Google Maps
- Extracts: phone, website URL, address, rating, review_count, maps_url
- Match validation: `_assess_match()` compares Maps address against SIRENE postal code/city
- **Concurrency:** `asyncio.Lock()` — one Maps search at a time (current code)
- Per-search hard timeout: configured in `_HARD_TIMEOUT`

**Step 2: Website Crawl (curl_cffi)**
- Only runs if Step 1 found a website URL
- Crawls homepage + `/contact` + `/mentions-legales` + `/nous-contacter` + `/a-propos`
- Extracts: emails, phones (backup), social links (LinkedIn, Facebook, Twitter, 30+ networks)
- Uses `curl_cffi` with Chrome TLS impersonation
- Separate `CurlClient` instance per crawl: timeout 8s, max 1 retry, 0.3–0.5s delay
- Infrastructure failures (DNS/SSL) abort crawl immediately — Playwright fallback removed

### 2.3.2 Qualify-or-Replace Loop

The enricher processes companies sequentially from a candidate deque. For each company:

```
Maps search → assess confidence → qualify or replace
```

**Qualification rule (MVP Phone Gate):**
```python
qualified = (
    contact is not None
    and match_confidence != "none"
    and contact.phone is not None  # MVP: phone required
)
```

A company is qualified only if Maps returned **a valid phone number** (high or low confidence, as long as a phone is present). A company is replaced when Maps returned **nothing** or **no phone**.

**Replacement mechanics:**
- Replacement pool pre-fetched in bulk at enrichment start: `target_count × 3` candidates
- Pre-fetched from DB: same NAF prefix + département, excluding tried SIRENs and `[ND]` names
- On replacement: pop from deque (instant, no DB query), append to candidates
- Max total attempts: `target_count × 5` (safety cap)
- Loop exits when: `len(contacts) >= target_count` OR candidates exhausted OR max attempts hit

---

## 2.4 Wave Processing

| | |
|---|---|
| **Owner** | `module_d/batch_processor.py` |
| **Input** | `TriageResult`, `enrich_fn`, `pool`, `wave_size` |
| **Output** | None (all output is via DB writes and JSONL files) |
| **Side effects** | `contacts`, `companies`, `query_tags`, `scrape_audit` writes; JSONL card files; checkpoint files |
| **Failure mode** | Wave timeout → uses already-saved contacts; JSONL write failure → non-fatal |
| **Idempotency** | Yes — checkpoint enables resume from last completed wave |

**Sequence per wave:**
1. Build `on_save` callback (per-company immediate persistence)
2. Call `enrich_fn(wave_companies, on_save=_on_save)` with timeout
3. After enrichment: dedup remaining companies not saved via `on_save`
4. Write JSONL cards for entire wave
5. Save checkpoint
6. Cooldown before next wave (skip after last wave)

---

## 2.5 Completion

| | |
|---|---|
| **Owner** | `runner.py` |
| **Input** | All waves completed |
| **Output** | `scrape_jobs.status = 'completed'` |
| **Side effects** | Chrome closed, heartbeat task cancelled, status connection closed |
| **Failure mode** | Exception during any stage → `scrape_jobs.status = 'failed'` |

---

# 3. Job Lifecycle

## State Machine (current)

```
                          ┌─────────────────────────┐
                          │    API (batch.py)        │
                          │  Creates scrape_jobs row │
                          │  status = 'new'          │
                          │  Spawns subprocess       │
                          └────────────┬────────────┘
                                       ▼
                          ┌─────────────────────────┐
                          │   runner.py starts       │
                          │  status → 'in_progress'  │
                          └────────────┬────────────┘
                                       ▼
                          ┌─────────────────────────┐
                          │   interpret + triage     │
                          │   (triage_* columns set) │
                          └────────────┬────────────┘
                                       ▼
                          ┌─────────────────────────┐
                          │   wave processing loop   │
                          │   (wave_current updated)  │
                          └──────┬───────────┬──────┘
                                 │           │
                          success│           │exception
                                 ▼           ▼
                          ┌──────────┐ ┌──────────┐
                          │completed │ │ failed   │
                          └──────────┘ └──────────┘
```

## State Transitions

| From | To | Trigger | Enforced by |
|------|-----|---------|-------------|
| `new` | `in_progress` | runner.py starts processing | application |
| `in_progress` | `completed` | all waves finished successfully | application |
| `in_progress` | `failed` | unrecoverable exception OR SQL timeout | application |
| `in_progress` | `completed` | zero companies found (nothing to scrape) | application |
| `in_progress` | `completed` | all companies GREEN (nothing to scrape) | application |

**Notes:**
- **Application rule:** state machine is application-managed, not DB-enforced
- **Current limitation:** no retry path from `failed` — must launch a new batch
- **Application rule:** `updated_at` heartbeated every 60 seconds during `in_progress`

## Progress Columns (updated during execution)

| Column | Updated when | By |
|--------|-------------|-----|
| `status` | state transitions | `runner.py` |
| `triage_*` | after triage completes | `runner.py` |
| `total_companies` | after triage | `runner.py` |
| `wave_total` | after triage | `runner.py` |
| `wave_current` | after each wave completes | `runner.py` (via `enrich_fn` closure) |
| `companies_scraped` | after each company decision | `runner.py` (via `on_progress` callback) |
| `companies_qualified`| after each company decision | `runner.py` |
| `replaced_count` | after each replacement | `runner.py` (via `on_progress` callback) |
| `updated_at` | every 60s + every status write | heartbeat task + `_update_job_safe` |

---

# 4. Persistence Contract

## What is written, and when

| Table | When written | Append-only? | Idempotent? | Write failure fatal? |
|-------|-------------|:------------:|:-----------:|:-------------------:|
| `companies` | Per-company via `on_save`, wave-end catchup | No (upsert) | ✅ | No — caught per company |
| `contacts` | Per-company via `on_save` | No (upsert) | ✅ | No — caught per company |
| `query_tags` | GREEN: after triage. Others: per-company via `on_save` + wave-end | No (upsert) | ✅ | No |
| `scrape_audit` | Per-company via `on_save` | ✅ append | No | No |
| `enrichment_log` | Per-company (qualified, replaced, or failed) | ✅ append | No | No — explicitly non-blocking |
| `rejected_sirens` | End of enricher call (batch flush) | No (upsert DO NOTHING) | ✅ | No — caught |
| `scrape_jobs` | State transitions + progress updates + heartbeat | No (UPDATE) | ✅ | Reconnects on failure |

### Critical persistence rule: `on_save` is immediate

```
Company qualified by enricher
  → on_save(company, contact) fires IMMEDIATELY
    → upsert_company()
    → bulk_tag_query()
    → upsert_contact()
    → log_audit()
  → Data is in DB BEFORE the wave finishes
```

**Why this matters:** If a wave times out after processing 40 of 50 companies, the 40 already-saved companies are preserved. The old architecture lost entire waves on timeout.

### Checkpoint semantics

- Saved after each wave: `data/checkpoints/{query_id}/`
- Contains: `job_state` (wave_current, counts) + `seen_set` (already-processed SIRENs)
- On restart: resume from `wave_current + 1`, skip already-seen companies
- **Operational policy:** clear `data/checkpoints/` when testing to avoid stale resume

---

# 5. Decision Rules

## 5.1 Maps Confidence Policy

Match confidence is assessed by comparing Maps-returned address against the company's SIRENE postal code, city, and département:

| Confidence | Condition | Effect |
|-----------|-----------|--------|
| `high` | Maps address contains expected postal code OR city name | Keep all data |
| `low` | Maps returned data but no geographic match | **Discard phone** (false positive protection), keep address/rating/maps_url |
| `none` | Maps returned nothing | Company not qualified, trigger replacement |

**Application rule:** `_assess_match()` in `enricher.py` is the sole confidence assessor.

## 5.2 Qualification Rule (MVP Phone Gate)

```python
qualified = contact is not None and match_confidence != "none" and contact.phone is not None
```

This means:
- `high` confidence + has phone → qualified ✅
- `low` confidence + has phone → qualified ✅
- `high` or `low` confidence, but **no phone** → not qualified, replaced ❌
- `none` → not qualified, replaced ❌

## 5.3 Replacement Trigger

A company is replaced when:
- Maps returned zero results (`match_confidence == "none"` and `contact is None`)
- Maps returned data but no phone number (fails MVP gate)
- OR per-company enrichment threw an exception

Replacement is **not** triggered for:
- Low confidence (different city) — multi-office rule
- Website crawl failure — Maps data alone is sufficient

## 5.4 Rejection Trigger

Rejected SIRENs (`rejected_sirens` table) are written when:
- `match_confidence == "none"` → reason = `no_maps_data`
- `match_confidence == "low"` and company not qualified → reason = `low_confidence`

**Persisted in batch** at end of enricher call (not per-company).

## 5.5 Multi-Office Rule

A company with Maps presence in a different city than the SIRENE-registered address must NOT be rejected. Companies can have multiple offices across France. If Maps returns data for a different location, **keep it** — the phone is discarded (false positive protection) but other data is preserved.

## 5.6 Source Attribution

| Condition | Source label |
|-----------|-------------|
| Phone came from Maps | `google_maps` |
| Email came from crawl, no Maps phone | `website_crawl` |

The `source` field in `contacts` is set based on which step provided the **primary** data.

---

# 6. Runtime Controls

These are tunable settings. Changes here do not affect architecture.

| Control | Current value | Location | Notes |
|---------|--------------|----------|-------|
| Wave size | 50 (min 1, max 200) | `settings.wave_size`, capped by `_MAX_WAVE_SIZE` | |
| Wave timeout | `max(300s, wave_size × 15s)` | `batch_processor.py` line 181 | Dynamic per wave |
| Cooldown between waves | 5–15s (randomized) | `batch_processor.py` `delay_min`/`delay_max` | Skip after last wave |
| Maps concurrency | 1 (asyncio.Lock) | `playwright_maps_scraper.py` line 87 | One search at a time |
| Maps delay | 2–3s + jitter | `playwright_maps_scraper.py` | Per-search |
| Website crawl timeout | 8s | `enricher.py` `CurlClient(timeout=8.0)` | Per-page |
| Website crawl delay | 0.3–0.5s | `enricher.py` `CurlClient(delay_min=0.3)` | Per-page |
| Replacement pool size | `target_count × 3` | `enricher.py` line 338 | Pre-fetched in bulk |
| Max enrichment attempts | `target_count × 5` | `enricher.py` line 303 | Safety cap |
| Connection pool max | 5 | `runner.py` line 273 | Shared by all stages |
| Statement timeout | 60s | `runner.py` `_configure_conn` | On pool connections |
| Heartbeat interval | 60s | `runner.py` `_run_heartbeat` | Touches `updated_at` |
| TCP keepalive idle | 60s | `runner.py` `_KEEPALIVE_PARAMS` | |
| TCP keepalive interval | 10s | `runner.py` `_KEEPALIVE_PARAMS` | |
| TCP keepalive count | 5 | `runner.py` `_KEEPALIVE_PARAMS` | Dead after 5 failed probes |
| Query candidate fetch | `requested_size × 2` | `runner.py` line 291 | Headroom for replacements |

---

# 7. Failure and Recovery

## 7.1 Per-Company Failure

| Failure | Handling | Data impact |
|---------|----------|-------------|
| Maps search exception | Caught, logged as `failed` in `enrichment_log`, company replaced | No data loss |
| Maps timeout | Returns `{}`, confidence = `none`, company replaced | No data loss |
| Website crawl DNS/SSL | Crawl aborted, Maps data kept | Partial enrichment |
| Website crawl timeout | Page skipped, other pages still crawled | Partial enrichment |
| `on_save` exception | Caught and logged, contact still in memory list | Data not persisted until wave-end catchup |
| `enrichment_log` write fails | Silently caught (`pass`) | Diagnostic gap only |

## 7.2 Per-Wave Failure

| Failure | Handling | Data impact |
|---------|----------|-------------|
| Wave timeout (`asyncio.TimeoutError`) | Uses `saved_contacts` (already persisted via `on_save`) | No loss of already-saved data |
| JSONL write failure | Caught, non-fatal, logged as warning | Cards missing from file, DB unaffected |

## 7.3 Process-Level Failure

| Failure | Handling | Data impact |
|---------|----------|-------------|
| Unhandled exception in runner | `scrape_jobs.status → 'failed'`, exception logged | Companies saved via `on_save` before crash are preserved |
| SQL timeout during interpret_query | `scrape_jobs.status → 'failed'`, specific error logged | No data written |
| Status connection drops | Auto-reconnect via `_update_job_safe` | Progress may be delayed, not lost |
| Chrome fails to start | `maps_scraper = None`, pipeline runs without Maps | All companies get `confidence = 'none'` |
| Process killed (SIGKILL) | Checkpoint on disk enables resume from last completed wave | Data from current wave lost unless saved via `on_save` |

## 7.4 Crash Safety Assumptions

- **Per-company persistence (`on_save`)** means most data survives any crash
- **Checkpoint files** enable wave-level resume on restart
- **Idempotent upserts** mean re-running the same batch is safe
- **Current limitation:** no automatic retry from `failed` state — requires manual relaunch

## 7.5 Connection Resilience

- Status connection uses `_update_job_safe` with auto-reconnect on `OperationalError`/`InterfaceError`
- TCP keepalives prevent silent connection death during 2+ hour batches
- Heartbeat task touches `updated_at` every 60s (connection liveness probe)
- Pool connections have 60s statement timeout

---

# 8. Known Limitations

1. **Contacts model is lossy** — one row per `(siren, source)` cannot represent multiple offices, phones, or emails
2. **No per-field confidence** — contact data has no provenance or confidence score beyond the Maps match assessment
3. **Maps concurrency is 1** — `asyncio.Lock` limits throughput; Semaphore(2) is a documented target but not yet implemented in code
4. **No retry from `failed`** — failed jobs require manual relaunch as a new batch
5. **Rejected SIRENs are batch-flushed** — if the process crashes before the end of enricher call, rejected SIRENs for that batch are lost (must be re-discovered)
6. **Checkpoint is wave-level only** — partial wave progress is not checkpointed (but `on_save` preserves individual company data)
7. **No "fresher wins" for stale data** — COALESCE means first non-null value wins forever, even if newer data is better
8. **State machine is application-only** — no DB-enforced constraints on `scrape_jobs.status` transitions

---

# 9. Recommendations & Next Steps

### Multi-Worker & Concurrency (High Priority)
1. **Multi-Worker Job Locking (CRITICAL)** — The API endpoint that assigns jobs to workers MUST use atomic locking (`SELECT ... FOR UPDATE SKIP LOCKED` or explicit `worker_id` assignment) to prevent two workers from grabbing the same `new` job concurrently.
2. **Page Pool for Maps Concurrency** — To achieve >1 concurrency in Maps without race conditions, refactor `PlaywrightMapsScraper` to manage a pool of independent `BrowserContext` or `Page` objects. **Do not** simply swap the `Lock` for a `Semaphore` on a single shared Page, as actions will collide and corrupt data.

### Reliability & Resilience
3. **Add Retry Path for Failed Jobs** — Add an API endpoint and UI "Retry" button to cleanly flip a job's status from `failed` to `new` (and clear the `data/checkpoints/{id}/` folder) so users can recover from SQL timeouts or machine reboots.
4. **Playwright Memory Leak Mitigation** — In `playwright_maps_scraper.py`, instead of doing a hard `.reload()` and fully-blocking `gc.collect()` every 10 searches, simply `.close()` the `Page` and open a `new_page()` every 50 searches to drop the isolated DOM footprint without pausing the async event loop.
5. **Per-Company Rejection Persistence** — Flush rejected SIRENs immediately (via `_on_save` logic) instead of batch-end to prevent repeating dead Maps queries if the pipeline drops mid-batch.
6. **Playwright Fallback for Website Crawls** — If the `curl_cffi` crawler hits a 403 (Cloudflare/Datadome block) during website enrichment, hand the URL over to the already-warm Playwright Maps instance to bypass bot protection instead of abandoning the crawl.

### Data Quality
7. **Replace contacts with raw-facts + resolved view** — Separate observed facts from best-answer materialization.
8. **Add per-field confidence scoring** — Track Maps match confidence at field level, not just company level.
9. **Implement "fresher wins" for rating/review_count** — These grow stale over time and should accept updates instead of traditional `COALESCE` logic.
10. **DB-Enforce Status Transitions (Optional)** — Add `CHECK` constraint or trigger on `scrape_jobs.status` (Low priority, as it might block manual admin overrides).

### Performance
11. **Parallelize website crawl** — The crawl stage can run concurrently across companies since there are no IP anti-bot concerns with standard curl requests.
