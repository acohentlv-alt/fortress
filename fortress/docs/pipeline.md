# Enrichment Pipeline Architecture

## Overview

Every batch follows this exact sequence:

```
User Input ("transport 66")
    │
    ▼
┌─────────────────────┐
│ 1. Interpret Query   │  query_interpreter.py
│    → NAF + dept SQL  │  60s timeout on 14.7M rows
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ 2. Triage            │  triage.py
│    → BLACK/BLUE/     │  Classifies before any network call
│      GREEN/YELLOW/   │
│      RED             │
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ 3. Enrich            │  enricher.py
│    → Maps (Playwright│  Per company: Maps → website crawl
│    → curl_cffi crawl │  Qualify-or-replace loop
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ 4. Wave Processing   │  batch_processor.py
│    → Checkpoint      │  50 companies per wave
│    → Dedup to DB     │  on_save per company (crash safe)
└──────────┬──────────┘
           ▼
┌─────────────────────┐
│ 5. Complete          │  runner.py
│    → status=completed│  All contacts upserted
│    → Chrome cleanup  │  query_tags set
└─────────────────────┘
```

## Step 1: Interpret Query

`module_a/query_interpreter.py` converts user input to SQL.

- "transport 66" → NAF codes `49%`, `50%`, `51%`, `52%`, `53%` + département `66`
- Industry tokens → NAF codes via fuzzy matching
- Location → département code
- **60-second SQL timeout** — protects against full table scans
- Returns `QueryResult` with company list + metadata
- Fetches 2× `batch_size` candidates so the replace loop has headroom

## Step 2: Triage

`module_a/triage.py` classifies every company before any network call:

| Color | Meaning | Action |
|-------|---------|--------|
| **BLACK** | SIREN in `blacklisted_sirens` | Skip entirely |
| **BLUE** | SIREN in `client_sirens` | Skip (client already has) |
| **GREEN** | Phone + website + email exist | Instant tag, no scraping |
| **YELLOW** | Partial data (some MVP fields) | Targeted scrape |
| **RED** | Never enriched before | Full pipeline |

GREEN companies are tagged in `query_tags` immediately.

## Step 3: Enrichment

`module_d/enricher.py` — the 2-step pipeline per company:

### Step 3a: Google Maps (Playwright)
- Primary data source (~86% phone hit rate)
- Searches `"denomination ville"` on Google Maps
- Extracts: phone, website URL, address, rating, reviews, maps_url
- **Match validation**: compares Maps address against SIRENE postal code/city
  - `high` confidence → keep all data
  - `low` confidence → discard phone (false positive protection), keep address/rating
  - `none` → no Maps data found
- Protected by `asyncio.Lock` (one search at a time)

### Step 3b: Website Crawl (curl_cffi)
- Only runs if Maps found a website URL
- Crawls homepage + `/contact` + `/mentions-legales` + `/nous-contacter` + `/a-propos`
- Extracts: emails, phones (backup), social links
- Uses `curl_cffi` with Chrome TLS impersonation (fast, no browser overhead)
- **NOT Playwright** — company websites have zero anti-bot protection

### Qualify-or-Replace Loop
- If Maps returns no data → company is **replaced** with another from the same NAF+dept pool
- Replacements are **pre-fetched in bulk** at the start (eliminates ~17s DB delay per replacement)
- Loop continues until `batch_size` qualified companies or pool exhausted
- Max attempts: `target_count × 5` (safety cap against infinite loops)

### Rejected SIRENs
- Companies with `no_maps_data` are saved to `rejected_sirens` table
- Scoped per `(siren, naf_prefix, departement)` — rejection is per query pattern
- Pre-loaded at enrichment start → skips known dead ends immediately

### Enrichment Log
- Every company processed gets a row in `enrichment_log`
- Tracks: outcome (qualified/replaced/failed), timing, maps method, phones/emails found
- Non-blocking — never crashes the pipeline

## Step 4: Wave Processing

`module_d/batch_processor.py` — processes in waves of `wave_size` (default 50):

- **`on_save` callback**: Each company+contact is persisted to DB the **instant** enrichment qualifies it. This eliminates the "data vaporization" bug where a wave timeout would discard already-scraped data.
- **Dynamic timeout**: `max(300s, wave_size × 15s)` per wave
- **Checkpoint**: Saves progress after each wave for resume-on-restart
- **Cooldown**: 5-15 seconds between waves (randomized)
- Tags both original AND replacement company SIRENs in `query_tags`

## Step 5: Completion

`runner.py` orchestrates everything:

- **Chrome-before-DB**: Browser starts BEFORE heavy SQL queries (prevents resource contention)
- **Heartbeat**: Background task touches `scrape_jobs.updated_at` every 60s (connection keepalive)
- **Auto-reconnect**: Status connection reconnects on failure (`_update_job_safe`)
- **TCP keepalives**: Prevents silent connection death during 2+ hour batches

## Process Lifecycle

```
API (batch.py)
  └── spawns subprocess: python -m fortress.runner <query_id>
        └── runner.py
              ├── Start Playwright Chrome
              ├── Open status connection (with heartbeat)
              ├── Open connection pool (5 max)
              ├── interpret_query()
              ├── triage_companies()
              └── batch_processor.run_query()
                    └── enrich_companies() per wave
                          ├── Maps search (Playwright)
                          ├── Website crawl (curl_cffi)
                          └── on_save → upsert to DB
```

## Source Attribution

Every contact row has a `source` column:

| Source | Meaning |
|--------|---------|
| `google_maps` | Phone came from Maps |
| `website_crawl` | Email came from website crawl |
| `synthesized` | Generated (future) |
