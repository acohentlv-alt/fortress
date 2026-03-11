# FORTRESS — B2B Lead Collection & Enrichment System (France)

## Quick Start

```bash
cd fortress/
python3 -m fortress.api.main          # Start API on port 8082
# Then visit http://localhost:8082
```

## Architecture

```
Frontend (Vanilla JS SPA)      API (FastAPI)              Pipeline (Python async)
┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────────────┐
│ dashboard.js         │──▶│ routes/dashboard.py  │   │ runner.py (subprocess)   │
│ new-batch.js         │──▶│ routes/batch.py      │──▶│  → query_interpreter.py  │
│ monitor.js (polling) │──▶│ routes/jobs.py       │   │  → triage.py             │
│ search.js            │──▶│ routes/companies.py  │   │  → enricher.py           │
│ company.js           │──▶│ routes/export.py     │   │  → batch_processor.py    │
│ job.js               │──▶│ routes/departments.py│   │  → playwright_maps.py    │
│ department.js        │──▶│ routes/query.py      │   │  → deduplicator.py       │
│ open-query.js        │──▶│ routes/health.py     │   │  → checkpoint.py         │
└──────────────────────┘   └──────────────────────┘   └──────────────────────────┘
                                    │
                           PostgreSQL (fortress)
                    14.7M+ French companies (SIRENE registry)
```

## Pipeline Flow (per batch)

1. **Interpret Query** → user input ("transport 66") → SQL on 14.7M companies
2. **Triage** → classify: BLACK (blacklisted) / BLUE (client owns) / GREEN (complete) / YELLOW (partial) / RED (new)
3. **Enrich** → Google Maps via Playwright (phone, website, address, rating) → curl_cffi website crawl (emails, social links)
4. **Wave Processing** → checkpoint after each wave of 50, per-company `on_save` for crash safety
5. **Output** → contacts upserted, query_tags set, JSONL cards written

## Key Rules

- **NEVER delete data** — only add, update, or merge
- **Primary key**: `companies.siren` (VARCHAR 9)
- **Deduplication**: `ON CONFLICT ... DO UPDATE` for all merges
- **One Maps search at a time** (`asyncio.Lock`)
- **curl_cffi for website crawl** (Playwright is Maps only — do NOT route website crawl through Playwright)
- **60-second SQL timeout** protects against full table scans
- **Chrome must start BEFORE heavy DB queries** (resource contention prevention)
- **Per-company saving via `on_save`** — data survives wave timeouts

## Rate Limits

| Agent | Delay | Concurrency |
|-------|-------|-------------|
| Google Maps (Playwright) | 2-3s + jitter | One at a time (`asyncio.Lock`) |
| Website Crawl (curl_cffi) | 0.3-0.5s | Per-company, after Maps |

## Database (Critical Indexes)

```sql
idx_companies_dept_naf_statut ON (departement, naf_code, statut)  -- primary search path
idx_companies_naf_dept_statut ON (naf_code, departement, statut)  -- exact NAF filtering
idx_companies_naf_statut ON (naf_code, statut)                    -- nationwide queries
```

**All queries MUST hit indexes.** Run `EXPLAIN ANALYZE` on any new query.

## Testing Checklist

```bash
# Python syntax
python3 -c "import ast; ast.parse(open('fortress/module_d/enricher.py').read())"

# Start API
python3 -m fortress.api.main

# Small test batch
curl -X POST http://localhost:8082/api/batch/run \
  -H "Content-Type: application/json" \
  -d '{"sector":"transport","department":"34","size":5,"mode":"discovery"}'

# Check logs
tail -f fortress/data/logs/TRANSPORT_34_BATCH_*.log
```

## Diagnostic Tables

- `enrichment_log` — per-company outcome (qualified/replaced/failed), timing, method
- `rejected_sirens` — companies with no Maps presence, scoped per NAF+department
- `scrape_audit` — action log for all scraping operations

## File Reference

| Layer | Key Files |
|-------|-----------|
| Config | `config/settings.py` (.env via Pydantic) |
| Database | `database/schema.sql` (all tables + indexes) |
| API | `api/db.py` (async pool), `api/routes/*.py` |
| Pipeline | `runner.py` → `module_a/query_interpreter.py` → `module_a/triage.py` → `module_d/enricher.py` → `module_d/batch_processor.py` |
| Scraping | `module_c/playwright_maps_scraper.py` (Maps), `module_c/curl_client.py` (websites) |
| Frontend | `frontend/js/app.js` (SPA router), `frontend/js/pages/*.js` |

## Known Gotchas

1. **Stale `__pycache__`**: After code changes, always `find . -name __pycache__ -exec rm -rf {} +`
2. **Checkpoints**: Old checkpoints cause `batch.already_complete` skips — clear `data/checkpoints/` when testing
3. **Chrome sandbox**: Runner MUST run outside macOS sandbox (`sandbox-exec` blocks Chrome temp dirs)
4. **Port**: API runs on 8082 (check `config/settings.py`)
