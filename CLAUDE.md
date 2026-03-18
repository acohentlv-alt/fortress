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

# 1. Architecture

```
Frontend (Vanilla JS SPA)      API (FastAPI)              Pipeline (Python async)
┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────────────┐
│ dashboard.js         │──▶│ routes/dashboard.py  │   │ runner.py (SIRENE strat) │
│ new-batch.js         │──▶│ routes/batch.py      │──▶│ maps_discovery_runner.py │
│ monitor.js (polling) │──▶│ routes/jobs.py       │   │  → query_interpreter.py  │
│ search.js            │──▶│ routes/companies.py  │   │  → triage.py             │
│ company.js           │──▶│ routes/export.py     │   │  → enricher.py           │
│ contacts.js          │──▶│ routes/contacts_list │   │  → batch_processor.py    │
│ upload.js            │──▶│ routes/client.py     │   │  → playwright_maps.py    │
│ activity.js          │──▶│ routes/activity.py   │   │  → deduplicator.py       │
│ job.js               │──▶│ routes/notes.py      │   │  → checkpoint.py         │
│ login.js             │──▶│ routes/auth.py       │   └──────────────────────────┘
└──────────────────────┘   └──────────────────────┘
                                    │
                           PostgreSQL (Neon)
                    14.7M+ French companies (SIRENE)
```

---

# 2. API Routes (Complete)

| Router | Prefix | Purpose |
|--------|--------|---------|
| `auth.py` | `/api/auth` | Login/logout/session/me |
| `health.py` | `/api/health` | Health check (public) |
| `dashboard.py` | `/api/dashboard` | Stats, data bank, all-data, analysis |
| `departments.py` | `/api/departments` | Department breakdown |
| `jobs.py` | `/api/jobs` | Job CRUD, cancel, retry, resume |
| `companies.py` | `/api/companies` | Search, detail, enrich, inline edit |
| `contacts_list.py` | `/api/contacts` | Flat contact table view |
| `export.py` | `/api/export` | CSV/JSONL export |
| `batch.py` | `/api/batch` | Spawn pipeline subprocess |
| `client.py` | `/api/client` | CSV/XLSX upload + smart column mapping |
| `sirene.py` | `/api/sirene` | Raw SIRENE search (14.7M) |
| `notes.py` | `/api/notes` | Company notes (GET/POST/DELETE) |
| `activity.py` | `/api/activity` | Activity audit log |
| `admin.py` | `/api/admin` | Admin-only operations |

### Authentication

- Session-based via signed cookies (`fortress_session`)
- All `/api/*` routes require auth except: `/api/health`, `/api/auth/check`, `/api/auth/login`, `/api/auth/logout`
- Two roles: `admin` (full access), `user` (standard access)
- Activity log tracks user actions (notes, edits, enrichments)

---

# 3. Pipeline Invariants

These rules are non-negotiable. See [Pipeline Contract](fortress/docs/pipeline.md).

1. **`companies` is master identity only** — enrichment pipeline must not modify identity fields
2. **Contact writes are idempotent upserts** — `ON CONFLICT (siren, source) DO UPDATE` with COALESCE
3. **Qualified companies persist immediately** — `on_save` writes before wave completes
4. **Zero Maps presence required before rejection** — location mismatch alone is not rejection
5. **Chrome starts before heavy DB queries** — prevents resource contention failures
6. **curl_cffi for website crawl** — Playwright is Maps-only. Do NOT route website crawl through Playwright.

### Two Pipeline Strategies

| Strategy | Runner | How it works |
|----------|--------|-------------|
| **SIRENE** (default) | `runner.py` | Query 14.7M → filter by NAF+dept → triage → enrich (Maps + crawl) |
| **Maps Discovery** | `maps_discovery_runner.py` | Search Google Maps directly with user queries, bypass SIRENE pool |

---

# 4. Data Rules

| Rule | Enforcement |
|------|------------|
| Primary key: `companies.siren` (VARCHAR 9) | Schema |
| One contact row per `(siren, source)` | Schema |
| Deduplication: `ON CONFLICT DO UPDATE` | Application — COALESCE, first non-null wins |
| Empty strings normalized to NULL | Application — enforce before persistence |
| User role: cannot delete data | Operational — add, update, or merge only |
| Admin role: can delete for cleanup/testing | Operational |
| Ghost rows prevented | Application — skip insert when all meaningful fields are NULL |
| Notes: per-company, author can delete | Application — admin can also delete |

---

# 5. Database Tables

| Table | Purpose | Key |
|-------|---------|-----|
| `companies` | SIRENE registry + enriched fields | `siren` (PK) |
| `contacts` | Scraped contact data | `(siren, source)` UNIQUE |
| `officers` | Directors from INPI/uploads | `(siren, nom, prenom)` UNIQUE |
| `query_tags` | Batch/query → company mapping | `(siren, query_name)` UNIQUE |
| `company_notes` | Per-company text annotations | `id` (PK), FK `siren` |
| `activity_log` | User action audit trail | `id` (PK) |
| `scrape_jobs` | Batch job tracking | `query_id` (UNIQUE) |
| `users` | Authentication | `id` (PK) |
| `blacklisted_sirens` | Companies to skip | `siren` (PK) |

---

# 6. Rate Limits

| Agent | Delay | Concurrency | Enforcement |
|-------|-------|-------------|-------------|
| Google Maps (Playwright) | 2–3s + jitter | **One at a time** (`asyncio.Lock`) | Application |
| Website Crawl (curl_cffi) | 0.3–0.5s | Per-company, after Maps | Application |

---

# 7. Database Performance

```sql
-- Primary search path (dept + NAF queries)
idx_companies_dept_naf_statut ON (departement, naf_code, statut)
-- Exact NAF filtering
idx_companies_naf_dept_statut ON (naf_code, departement, statut)
-- Nationwide queries
idx_companies_naf_statut ON (naf_code, statut)
-- Company notes lookup
idx_company_notes_siren ON company_notes(siren)
```

**Rules:**
- Every query MUST hit an index — no sequential scans on 14.7M rows
- `EXPLAIN ANALYZE` required for any new query
- Pagination mandatory — never `SELECT *` without `LIMIT`
- 60-second statement timeout on pipeline connections

---

# 8. File Reference

| Layer | Key Files |
|-------|-----------|
| Config | `config/settings.py` (.env via Pydantic) |
| Schema | `database/schema.sql` — definitive table structure |
| Auth | `api/auth.py` (session tokens), `api/routes/auth.py` (login/logout) |
| API | `api/db.py` (async pool), `api/routes/*.py` (15 route files) |
| Pipeline | `runner.py` (SIRENE), `maps_discovery_runner.py` (Maps) |
| Enrichment | `module_d/enricher.py` → `module_c/playwright_maps_scraper.py` + `module_c/curl_client.py` |
| Dedup | `module_d/deduplicator.py` — all upsert logic |
| Upload | `api/routes/client.py` + `utils/column_mapper.py` |
| Frontend | `frontend/js/app.js` (SPA router), `frontend/js/pages/*.js` (13 pages) |
| Models | `models.py` — Pydantic data models (Company, Contact, Officer, QueryResult) |
| Users | `manage_users.py` (CLI), `setup_users.py` (initial setup) |

---

# 9. Frontend Pages

| Page | Route | File |
|------|-------|------|
| Dashboard | `#/` | `dashboard.js` |
| New Batch | `#/new-batch` | `new-batch.js` |
| Monitor | `#/monitor` | `monitor.js` |
| Search | `#/search` | `search.js` |
| Company Detail | `#/company/:siren` | `company.js` |
| Contacts List | `#/contacts` | `contacts.js` |
| Activity Log | `#/activity` | `activity.js` |
| Job Detail | `#/job/:id` | `job.js` |
| Department | `#/department/:code` | `department.js` |
| Upload | `#/upload` | `upload.js` |
| Login | `#/login` | `login.js` |

---

# 10. Testing Checklist

### Before any code change

```bash
python3 -c "import ast; ast.parse(open('file.py').read())"   # Python syntax
node --check file.js                                          # JS syntax
python3 -m fortress.api.main                                  # API starts
```

### After database changes

- Run `EXPLAIN ANALYZE` on affected queries
- Verify index scans (no sequential scans)
- Update `database/schema.sql`
- Apply migration on Neon SQL Editor

### After pipeline changes

- Run small test batch (3-5 entities)
- Check `data/logs/{query_id}.log`
- Verify `query_tags` includes replacement SIRENs

### After frontend changes

- Hard-refresh (`Cmd+Shift+R`) — JS files use 304 caching
- Verify cleanup system (no interval leaks)
- Test 503 error state (DB down)

---

# 11. Known Gotchas

| Issue | Fix |
|-------|-----|
| Stale `__pycache__` | `find . -name __pycache__ -exec rm -rf {} +` |
| Old checkpoints | Clear `data/checkpoints/` when testing |
| Chrome sandbox (macOS) | Run outside `sandbox-exec` |
| Port | API runs on **8080** (defined in `api/main.py`) |
| npm cache permissions | `sudo chown -R $(whoami) ~/.npm` |
| DB migration | Apply via Neon SQL Editor (no Docker today) |
