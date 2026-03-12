# FORTRESS — AI Agent Operating Contract

## Purpose

This document defines rules and constraints for AI agents working on the Fortress codebase. It describes current system state, not aspirational behavior. If this document conflicts with application code, **code wins** and this document must be corrected.

## Source of Truth

| Document | Scope |
|----------|-------|
| `database/schema.sql` | Definitive schema |
| [Database Contract](fortress/docs/database.md) | Schema, merge semantics, performance, canonical queries |
| [Pipeline Contract](fortress/docs/pipeline.md) | Execution stages, persistence, decision rules, failure handling |
| Application code | Definitive runtime behavior |

---

## Quick Start

```bash
cd fortress/
python3 -m fortress.api.main          # API on port 8080
# Visit http://localhost:8080
```

---

# 1. Architecture

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

---

# 2. Pipeline Invariants

These rules are non-negotiable. See [Pipeline Contract](fortress/docs/pipeline.md) for full details.

1. **`companies` is master identity only** — enrichment pipeline must not modify identity fields
2. **Contact writes are idempotent upserts** — `ON CONFLICT (siren, source) DO UPDATE` with COALESCE
3. **Qualified companies persist immediately** — `on_save` writes before wave completes
4. **Zero Maps presence required before rejection** — location mismatch alone is not rejection
5. **Chrome starts before heavy DB queries** — prevents resource contention failures
6. **curl_cffi for website crawl** — Playwright is Maps-only. Do NOT route website crawl through Playwright.

---

# 3. Data Rules

| Rule | Enforcement | Details |
|------|------------|---------|
| Primary key: `companies.siren` (VARCHAR 9) | Schema guarantee | |
| One contact row per `(siren, source)` | Schema guarantee | |
| Deduplication: `ON CONFLICT DO UPDATE` | Application rule | COALESCE — first non-null wins |
| Empty strings normalized to NULL | Operational policy | Application must enforce before persistence |
| User role: cannot delete data | Operational policy | Add, update, or merge only |
| Admin role: can delete for cleanup/testing | Operational policy | |
| Ghost rows prevented | Application rule | Skip insert when all meaningful fields are NULL |

Full merge semantics: [Database Contract § 2](fortress/docs/database.md).

---

# 4. Rate Limits (Current State)

| Agent | Delay | Concurrency | Enforcement |
|-------|-------|-------------|-------------|
| Google Maps (Playwright) | 2–3s + jitter | **One at a time** (`asyncio.Lock`) | Application rule |
| Website Crawl (curl_cffi) | 0.3–0.5s | Per-company, after Maps | Application rule |

**Note:** Target is `asyncio.Semaphore(2)` for 2× Maps throughput, but current code uses `asyncio.Lock()` in `playwright_maps_scraper.py`. Any concurrency change must update this document.

---

# 5. Database Performance

```sql
-- Primary search path (dept + NAF queries)
idx_companies_dept_naf_statut ON (departement, naf_code, statut)
-- Exact NAF filtering
idx_companies_naf_dept_statut ON (naf_code, departement, statut)
-- Nationwide queries
idx_companies_naf_statut ON (naf_code, statut)
```

**Rules:**
- Every query MUST hit an index — no sequential scans on 14.7M rows
- `EXPLAIN ANALYZE` required for any new query
- Pagination mandatory — never `SELECT *` without `LIMIT`
- 60-second statement timeout on pipeline connections

Full index list and query patterns: [Database Contract § 3–4](fortress/docs/database.md).

---

# 6. File Reference

| Layer | Key Files |
|-------|-----------|
| Config | `config/settings.py` (.env via Pydantic) |
| Schema | `database/schema.sql` — definitive table structure |
| API | `api/db.py` (async pool), `api/routes/*.py` |
| Pipeline | `runner.py` → `module_a/query_interpreter.py` → `module_a/triage.py` → `module_d/enricher.py` → `module_d/batch_processor.py` |
| Scraping | `module_c/playwright_maps_scraper.py` (Maps), `module_c/curl_client.py` (websites) |
| Dedup | `module_d/deduplicator.py` — all upsert logic |
| Frontend | `frontend/js/app.js` (SPA router), `frontend/js/pages/*.js` |
| Contracts | `docs/database.md`, `docs/pipeline.md` |

---

# 7. Testing Checklist

### Before any code change

```bash
# Python syntax
python3 -c "import ast; ast.parse(open('file.py').read())"

# JS syntax
node --check file.js

# Verify API starts
python3 -m fortress.api.main
```

### After database changes

- Run `EXPLAIN ANALYZE` on affected queries
- Verify index scans (no sequential scans)
- Update `database/schema.sql`
- Update `docs/database.md` if schema contract changes

### After pipeline changes

```bash
# Small test batch
curl -X POST http://localhost:8080/api/batch/run \
  -H "Content-Type: application/json" \
  -d '{"sector":"transport","department":"34","size":5,"mode":"discovery"}'

# Monitor logs
tail -f fortress/data/logs/TRANSPORT_34_BATCH_*.log
```

- Verify `query_tags` includes both original and replacement SIRENs
- Update `docs/pipeline.md` if pipeline contract changes

### After frontend changes

- Hard-refresh browser (`Cmd+Shift+R`) — JS files served with 304 caching
- Verify navigation doesn't leak intervals (cleanup system in `app.js`)
- Test 503 error state (DB down)
- Test empty state (no data)

---

# 8. Diagnostic Tables

| Table | Purpose | Write pattern |
|-------|---------|--------------|
| `enrichment_log` | Per-company outcome (qualified/replaced/failed), timing, method | Append-only, non-blocking |
| `rejected_sirens` | Companies with zero Maps presence, scoped per NAF+dept | Upsert DO NOTHING |
| `scrape_audit` | Action log for all scraping operations | Append-only |

---

# 9. Known Gotchas

| Issue | Impact | Fix |
|-------|--------|-----|
| Stale `__pycache__` | Old bytecode masks code changes | `find . -name __pycache__ -exec rm -rf {} +` |
| Old checkpoints | `batch.already_complete` skips | Clear `data/checkpoints/` when testing |
| Chrome sandbox (macOS) | `sandbox-exec` blocks Chrome temp dirs | Run outside macOS sandbox |
| Port | API runs on **8080** | Defined in `api/main.py` |
| Lock vs Semaphore | Docs may say Semaphore(2), code is Lock | Check `playwright_maps_scraper.py` line 87 |
