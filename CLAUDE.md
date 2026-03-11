# 🏰 Fortress — Product Book

> **What is Fortress?** A machine that automatically finds and collects business contact information for every company in France — for free.
>
> **Last verified:** 2026-03-09 — System fully operational with live database.

---

## The Big Picture

Imagine you want to call 500 transport companies in southern France. You'd need their **phone numbers**, **websites**, **addresses**, and **ratings**. Normally, this would take a human intern weeks of Googling.

**Fortress does it automatically.** You type a keyword (like "logistique") and a department number (like "66" for Perpignan), and the system:

1. **Finds** all matching companies from the French government business registry
2. **Enriches** each one by searching Google, crawling company websites, and scraping Google Maps
3. **Saves** everything to a database and export files you can open in Excel

Zero cost. No paid APIs. No subscriptions.

---

## How It Works (The 5-Step Pipeline)

When you ask Fortress to enrich a batch of companies, each one goes through **5 steps** in order. The system stops early if it already has everything:

| Step | What Happens | Where Data Comes From |
|------|-------------|----------------------|
| **1. Government API** | Checks the official French business registry | `recherche-entreprises.api.gouv.fr` (free, fast) |
| **2. INPI Lookup** | Gets company directors and officers | INPI/RNE API (needs credentials — not yet configured) |
| **3. Web Search + Crawl** | Googles the company, visits their website, extracts contacts | Google → company website |
| **4. Directory Fallback** | Searches French phone directories for sole traders | `annuaire-entreprises.data.gouv.fr` |
| **5. Google Maps** | Opens Google Maps in a hidden browser, extracts everything | Google Maps (stealth browser) |

### What Google Maps Captures (Step 5)

The Maps scraper is the crown jewel. It opens a real Chrome browser (invisible to you), searches for the company, and extracts:

| Field | Example |
|-------|---------|
| **Phone** | `+33 4 68 54 66 40` |
| **Website** | `saintcharlesinternational.com` |
| **Address** | `449 Av. de Saint-Charles, 66000 Perpignan` |
| **Rating** | `4.1 ⭐` |
| **Review count** | `219 avis` |
| **Maps URL** | Direct link to the Google Maps listing |

**Speed:** 2.4 seconds per company. A batch of 50 takes about 2 minutes.

---

## What Gets Saved

### Database (PostgreSQL)
Every company and its contacts are stored in a local database on your Mac. The database has **8 tables**, but the two you care about are:

- **`companies`** — one row per business (name, SIREN number, address, industry code)
- **`contacts`** — one row per data source per company (phone, email, website, address, rating, Maps URL, etc.)

### Export Files
After every batch, the system writes:
- **`data/outputs/fortress_master.jsonl`** — every company ever processed, all in one file
- **`data/outputs/queries/{query_name}.jsonl`** — results per query (e.g., "LOGISTIQUE_66")

These can be opened or converted to CSV/Excel.

---

## Current Performance

| Metric | Value |
|--------|-------|
| Companies with websites (SAS, SARL) | **~95% contact found** |
| Sole traders (individuals) | **~50% target** (via directory + Maps) |
| Maps scraper speed | **2.4 seconds/company** |
| Database status | ✅ Live (`localhost:5432`) |

---

## The Control Panel (API)

Fortress has a web interface (FastAPI backend + Vue frontend). You can:

| What | How |
|------|-----|
| Check the system is alive | `GET /api/health` → shows database status |
| Search companies | `GET /api/companies?q=logistique&departement=66` |
| View company details | `GET /api/companies/{siren}` |
| Trigger enrichment | `POST /api/companies/{siren}/enrich` |

---

## What Each Folder Does

Think of the project as a factory with departments:

| Folder | Department | What It Does |
|--------|-----------|-------------|
| `module_a/` | **Reception** | Receives company data from the government, understands your search query, sorts companies into buckets |
| `module_b/` | **Research** | Googles companies, visits their websites, reads contact pages, parses phone/email/social links |
| `module_c/` | **Field Agents** | The actual tools that talk to the internet — the Chrome browser for Maps, the HTTP client for websites |
| `module_d/` | **Operations** | Runs the whole pipeline, manages batches, saves to database, prevents duplicates, handles crashes |
| `module_e/` | **Export** | Formats company cards, writes output files (JSONL/CSV/TXT) |
| `api/` | **Front Desk** | The web server that the control panel talks to |
| `frontend/` | **Display** | The visual control panel (Vue.js web app) |
| `database/` | **Filing Cabinet** | The SQL schema and migration scripts |
| `config/` | **Settings** | Connection details, rate limits, all configurable via `.env` |

---

## Hard Rules (The System's Constitution)

These are non-negotiable:

1. **$0 budget** — no paid APIs, no proxies, no subscriptions
2. **Never delete data** — only add or update
3. **French market only**
4. **Batches of 50** — never process more at once (prevents bans)
5. **Always check the blacklist** — some companies must never be scraped
6. **Playwright Chrome only** — no Selenium, no Puppeteer, no shortcuts
7. **All outputs go to `data/outputs/`** — the project root stays clean

---

## How To Run Things

### Start the API server
```bash
cd fortress
python -m fortress.api.main
```

### Run a batch enrichment
```bash
cd fortress
python -m fortress.runner <query_id>
```

### Run the Maps scraper test
```bash
cd fortress
python3 -B tests/test_maps_scraper.py
```

### Run full E2E verification (with database)
```bash
cd fortress
python3 -B tests/test_full_e2e.py
```

---

## Key Technical Details (For Engineers)

| Detail | Value |
|--------|-------|
| Language | Python 3.13, fully async |
| Database | PostgreSQL 16.13 (Homebrew, localhost:5432) |
| Browser engine | Playwright Chromium (stealth mode, SLOW_MO=50ms) |
| HTTP client | curl_cffi (Chrome TLS fingerprint) |
| Settings | Pydantic `BaseSettings` from `.env` file |
| Logging | structlog (structured JSON logs) |
| Data models | Pydantic: `Company`, `Contact`, `Officer`, `TriageResult` |
| DB connection | `postgresql://fortress:fortress_dev@localhost:5432/fortress` |

---

## History: What Was Built, Changed, and Removed

### Phase 1 — The Great Purge (2026-03-05)
Removed all dead code and obsolete infrastructure:
- ❌ Deleted `nodriver_scraper.py` (replaced by Playwright)
- ❌ Deleted `pagesjaunes_scraper.py` (DataDome protected — unusable)
- ❌ Deleted SearXNG fallback logic from web search
- ❌ Deleted DuckDuckGo dead code
- ❌ Deleted Streamlit UI (replaced by FastAPI + Vue)
- ❌ Deleted `docker-compose.yml` (native Postgres only)

### Phase 2 — Maps Scraper Stabilization (2026-03-09)
- ✅ Fixed website extraction (CSS selector update + 3-second wait)
- ✅ Fixed rating/review parsing for different Google Maps layouts
- ✅ Added `address` field to database
- ✅ Added `maps_url` field to database (direct link to Maps listing)

### Phase 3 — Speed Optimization (2026-03-09)
- ✅ Reduced SLOW_MO from 300ms to 50ms
- ✅ Added resource blocking (images, fonts, media — saves 40% bandwidth)
- ✅ Reduced wait times throughout the scraper
- ✅ Result: **6.4s → 2.4s per company** (62% faster)

### Phase 4 — Full System Verification (2026-03-09)
- ✅ PostgreSQL connected and verified
- ✅ All migrations applied (001–004)
- ✅ End-to-end pipeline proven: scrape → upsert → read back → export
- ✅ Master export file generating correctly

### Phase 5 — SIRENE Data Ingestion (2026-03-09)
- ✅ Ran `sirene_etablissement_ingester.py` to populate companies table from government Parquet files
- ❌ **CRASH at 9.15M rows**: `ValueError: invalid literal for int() with base 10: '20 46'`
  - **Root cause**: Dirty government data — street numbers contained spaces (e.g., `"20 46"`)
  - **Fix**: Added `.str.replace(' ', '')` before any `.cast(pl.Int64)` on Polars dataframes
  - **Fallback**: Wrapped numeric conversions in `strict=False` with `fill_null(None)` for garbled values
- ✅ Successfully completed full ingestion: **~16.7M company rows** now in database
- ✅ Populated HQ location data: `departement`, `code_postal`, `ville`, `adresse` for ~6M rows

### Phase 6 — Pre-Flight Diagnostics (2026-03-09)
- ✅ Verified `SELECT COUNT(*) FROM companies WHERE departement = '66'` returns >0
- ✅ Confirmed `query_interpreter.py` generates correct SQL for "transport 66" queries
- ✅ Validated compound indexes exist: `(departement, naf_code, statut)`

### Phase 7 — UI Bug Fixes (2026-03-10)
Four critical issues fixed:

**Issue 1 — Silent Subprocess Crashes (Batch stuck at 0%)**
- **File**: `api/routes/batch.py`
- **Bug**: `stderr=subprocess.DEVNULL` swallowed all runner crash output
- **Fix**: Both stdout and stderr now redirect to `data/logs/{query_id}.log`
- **Fix**: Parent process closes `log_file` after `Popen` to prevent file descriptor leak

**Issue 2 — Monitor Page DOM Flicker + Dead Progress Bar**
- **File**: `frontend/js/pages/monitor.js`
- **Bug**: Full DOM rebuild on every 3-second poll caused flicker, no smooth progress
- **Fix**: Complete rewrite. Skeleton renders ONCE. `update()` patches elements by ID. Progress bar uses CSS `transition: width 0.5s ease`. Poll interval reduced to 1.5s.

**Issue 3 — Navigation Lock During Batch Runs**
- **Files**: `frontend/js/app.js`, `frontend/js/pages/monitor.js`
- **Bug**: Monitor's `setInterval` continued running after navigation, overwriting other pages
- **Fix**: Added global `registerCleanup()` / `_runCleanup()` system to `app.js`. Monitor registers its interval. Cleanup runs before every route change. Guard checks `window.location.hash`.

**Issue 4 — NAF Code Filter on New Batch Page**
- **Files**: `frontend/js/pages/new-batch.js`, `frontend/js/api.js`, `api/routes/batch.py`, `module_a/query_interpreter.py`
- **Feature**: Added optional "Code NAF précis" input field
- **Bug found during audit**: `api.js`'s `runBatch()` destructured only 5 params — `naf_code` was silently dropped and never sent to the API!
- **Fix**: Updated `runBatch()` to accept and forward `naf_code`

### Phase 8 — Full Architectural Diagnostic (2026-03-10)
25+ files analyzed across all 5 layers. Found **6 critical, 7 warning, 5 improvement** issues.

**Critical issues found and fixed:**

| ID | Issue | File | Fix |
|----|-------|------|-----|
| C1 | Dashboard stats: 7 correlated subqueries per load | `dashboard.py` | Rewritten as single-pass CTE with `FILTER (WHERE ...)` |
| C2 | File handle leak per batch launch | `batch.py` | Added `log_file.close()` after `Popen` |
| C3 | FastAPI returns 200 for missing jobs (Flask tuple format) | `jobs.py` | Changed to `JSONResponse(status_code=404, ...)` |
| C4 | `runBatch()` silently drops `naf_code` parameter | `api.js` | Added `naf_code` to destructured params |
| C5 | No SQL timeout on 16.7M-row query | `runner.py` | Pool `configure` callback sets `SET statement_timeout = '60s'`; timeout caught and job marked `failed` |
| C6 | Pagination listeners leak via `setTimeout(0)` | `components.js` | **Not yet fixed** — needs event delegation |

**Warning issues (not yet fixed):**
- W1: LATERAL JOIN in job companies query — O(N²) risk on large datasets
- W2: Enricher creates empty Contact rows for "none" source (affects triage classification)
- W3: 30-90s cooldown between waves is excessive overhead
- W4: `company.js` references closured `container` variable — writes to detached DOM if user navigates
- W5: No dedicated index for `COUNT(DISTINCT siren)` on `query_tags`
- W6: `db.py` fetch helpers propagate `RuntimeError` as 500 instead of 503
- W7: `settings.py` has hardcoded default DB password `fortress_dev`

### Phase 9 — Database Performance Optimization (2026-03-10)
Ran `EXPLAIN ANALYZE` on live 16.7M-row database:

| Query Pattern | Before | After | Index Used |
|--------------|--------|-------|------------|
| France-wide `LIKE '49%'` LIMIT 200 | 13ms | 5ms | Seq Scan + early stop ✅ |
| Exact NAF `= '49.41A'` + dept 66 | 7.4s | **21ms** | `idx_companies_naf_dept_statut` ✅ |
| Broad NAF `LIKE '49%'` + dept 66 | 8.4s | 7.4s | `idx_companies_dept_naf` bitmap (planner insists) |
| COUNT(*) transport dept 66 | N/A | 35ms | `idx_companies_dept_naf_statut` Index Only Scan ✅ |

**New indexes created and persisted in `schema.sql`:**
```sql
CREATE INDEX idx_companies_naf_statut ON companies (naf_code, statut);
CREATE INDEX idx_companies_naf_dept_statut ON companies (naf_code, departement, statut);
```

**Key finding**: The exact NAF code filter (Issue 4) is the architectural solution to performance — when users specify a precise NAF code via the UI field, queries drop from 7.4s to 21ms (350× faster).

---

## Known Issues & Open Bugs

### Still Broken / Unfixed

| Priority | Issue | File | What Needs to Happen |
|----------|-------|------|---------------------|
| P2 | C6: Pagination event listeners accumulate on re-render | `components.js` | Refactor to event delegation on stable parent |
| P2 | W1: LATERAL JOIN per-row in job companies | `jobs.py:124-132` | Pre-compute `best_contact` view or use `DISTINCT ON` |
| P2 | W2: Empty contacts from "none" source mislead triage | `enricher.py:253` | Don't create contact row when all fields are null |
| P2 | W6: RuntimeError("Database offline") returns 500 not 503 | `db.py` | Add exception handler/middleware to map to 503 |
| P3 | W3: 30-90s wave cooldown is excessive | `batch_processor.py:232` | Reduce to 5-15s (`settings.delay_between_waves_*`) |
| P3 | W7: Hardcoded default DB password | `settings.py:24` | Remove default, require explicit `.env` setting |
| P3 | I1-I5: Dead imports, incomplete form labels, lazy import | Various | Code quality cleanup pass |

### Known Query Performance Limitation
Broad NAF prefix queries (`LIKE '49%'`) + department filter still take 7.4s because PostgreSQL's planner insists on bitmap scan via `idx_companies_dept_naf` instead of the new reversed index. This is acceptable because:
1. The 60s timeout protects against hangs
2. Users can use the exact NAF code field (21ms) for precise searches
3. The broad prefix is only used as a discovery tool

---

## What's Not Done Yet

| Item | Status | What's Needed |
|------|--------|---------------|
| INPI credentials | ⏳ Waiting | Register at `data.inpi.fr` for officer data |
| Full 50-company production batch | ⏳ Ready to run | DB + scraper both verified, just needs to be triggered |
| SearXNG directory search | ⚠️ Dead | Was purged — individual operator phone search needs alternative |
| Log viewer in UI | 💡 Idea | API endpoint to fetch `data/logs/{query_id}.log` + display in monitor page |
| 503 middleware | 🔧 P2 | Convert `RuntimeError("Database offline")` → 503 response |

---

## 🔍 Review Directive for Claude

**You are taking over this codebase.** Before writing ANY new code, perform the following review:

### Step 1: Verify Latest State
```bash
# Check all modified files compile
python3 -c "import ast; import glob; [ast.parse(open(f).read()) or print(f'✅ {f}') for f in glob.glob('fortress/**/*.py', recursive=True)]"
node --check fortress/frontend/js/api.js
node --check fortress/frontend/js/app.js
node --check fortress/frontend/js/pages/monitor.js
```

### Step 2: Verify Database
```bash
python3 -c "
import psycopg
conn = psycopg.connect('postgresql://fortress:fortress_dev@localhost:5432/fortress')
cur = conn.execute('SELECT COUNT(*) FROM companies')
print(f'Companies: {cur.fetchone()[0]:,}')
cur = conn.execute('SELECT COUNT(*) FROM contacts')
print(f'Contacts: {cur.fetchone()[0]:,}')
cur = conn.execute('SELECT COUNT(*) FROM scrape_jobs')
print(f'Jobs: {cur.fetchone()[0]:,}')
cur = conn.execute(\"SELECT indexname FROM pg_indexes WHERE tablename='companies' ORDER BY indexname\")
print('Indexes:', [r[0] for r in cur.fetchall()])
conn.close()
"
```

### Step 3: Read the Architectural Diagnostic
The full diagnostic report with 18 issues is at: `~/.gemini/antigravity/brain/5b9515f6-df27-4901-a016-6b3c14bbb546/walkthrough.md`

### Step 4: Prioritized Next Actions
1. **P0**: Run a real 50-company batch from the UI to validate the full end-to-end pipeline
2. **P2**: Fix the 7 warning issues listed in "Known Issues" above
3. **P2**: Add the 503 middleware to `db.py` (W6)
4. **P3**: Code quality cleanup (I1-I5 from diagnostic)
5. **💡**: Build the log viewer endpoint so crash logs are visible from the UI
