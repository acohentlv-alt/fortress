# Fortress

B2B lead collection and enrichment system for France. Operates on a PostgreSQL database of 14.7M+ companies from the French SIRENE registry.

## Purpose

Fortress discovers, filters, enriches, and exports French company contact data through a web UI and background batch pipeline.

* **Input:** A search query like "transport 66" (transport companies in département 66) or a CSV/XLSX upload
* **Output:** Enriched company records with **phone**, email, website, Google Maps rating, social links, officer data
* **Data Bank:** Centralized Multi-Worker Data Bank that reuses previously enriched contacts to save scraping time

## Source of Truth

| Document | Scope |
|----------|-------|
| [Database Contract](fortress/docs/database.md) | Schema, merge semantics, performance, canonical queries |
| [Pipeline Contract](fortress/docs/pipeline.md) | Execution stages, persistence timing, decision rules, failure handling |
| [CLAUDE.md](CLAUDE.md) | AI agent operating rules |
| `database/schema.sql` | Definitive schema (wins over docs on conflict) |
| Application code | Definitive runtime behavior (wins over docs on conflict) |

---

## Tech Stack

| Layer | Technology |
|-------|-----------|
| Backend | Python 3.13, FastAPI, psycopg3 (async) |
| Frontend | Vanilla JS SPA (hash routing, no frameworks) |
| Database | PostgreSQL on Neon (14.7M+ companies) |
| Maps scraping | Playwright Chromium (Google Maps) |
| Website scraping | curl_cffi (Chrome TLS impersonation) |
| Deployment | Render (Docker, auto-deploy from `main`) |
| Configuration | Pydantic Settings from `.env` |

---

## Features

| Feature | Description |
|---------|-------------|
| **Batch Enrichment** | Search + enrich companies by sector, department, NAF code |
| **Maps Discovery** | Direct Google Maps search strategy (bypasses SIRENE pool) |
| **Smart Upload** | CSV/XLSX import with intelligent column mapping |
| **Company Notes** | Per-company comments/annotations (CRM step 1) |
| **Activity Log** | User action audit trail (admin-visible) |
| **Contacts List** | Flat table view of all contacts across companies |
| **Authentication** | Session-based auth with signed cookies, user roles (admin/user) |
| **Multi-Worker** | Multiple computers can run enrichment simultaneously |
| **Smart Reuse** | Previously enriched contacts are reused from Data Bank |
| **Export** | CSV/JSONL export per batch or master export |

---

## Setup

### Prerequisites

* Python 3.13+
* PostgreSQL (Neon or local) with the `fortress` database
* Playwright browsers: `playwright install chromium`

### Installation

```bash
cd fortress/
pip install -e .
cp .env.example .env   # Edit with your DB credentials
```

### Running

```bash
python3 -m fortress.api.main
# API + frontend at http://localhost:8080 (or $PORT on Render)
```

### User Management

```bash
# Create initial users
python3 -m fortress.setup_users

# Manage users (add/remove/change role)
python3 -m fortress.manage_users
```

---

## Project Structure

```
fortress/
├── fortress/                  # Python package
│   ├── api/                   # FastAPI routes + DB pool
│   │   ├── routes/            # Endpoint handlers
│   │   │   ├── auth.py        # Login/logout/session
│   │   │   ├── admin.py       # Admin-only operations
│   │   │   ├── batch.py       # POST /batch/run → spawns runner
│   │   │   ├── client.py      # CSV/XLSX upload + smart mapping
│   │   │   ├── companies.py   # Company search/detail/enrich/edit
│   │   │   ├── contacts_list.py # Flat contact table view
│   │   │   ├── dashboard.py   # Aggregate stats + data bank
│   │   │   ├── departments.py # Department breakdown
│   │   │   ├── export.py      # CSV/JSONL export
│   │   │   ├── health.py      # Health check
│   │   │   ├── jobs.py        # Job listing/detail/cancel/retry
│   │   │   ├── notes.py       # Company notes (comments)
│   │   │   ├── activity.py    # Activity audit log
│   │   │   └── sirene.py      # Raw SIRENE search
│   │   ├── auth.py            # Session token encode/decode
│   │   ├── db.py              # Async connection pool
│   │   └── main.py            # App entry point (port 8080)
│   ├── config/                # Pydantic settings
│   ├── database/              # schema.sql — definitive schema
│   ├── frontend/              # HTML/JS/CSS SPA
│   │   ├── css/               # design-system.css + components.css
│   │   ├── js/
│   │   │   ├── app.js         # SPA router + cleanup system
│   │   │   ├── api.js         # Fetch wrapper (all API calls)
│   │   │   ├── components.js  # Reusable UI components
│   │   │   └── pages/         # One file per page
│   │   │       ├── dashboard.js
│   │   │       ├── new-batch.js
│   │   │       ├── monitor.js
│   │   │       ├── search.js
│   │   │       ├── company.js
│   │   │       ├── contacts.js
│   │   │       ├── activity.js
│   │   │       ├── job.js
│   │   │       ├── upload.js
│   │   │       └── login.js
│   │   └── index.html
│   ├── query/                 # Query interpretation + triage
│   ├── matching/              # Contact parsing + web search
│   ├── scraping/              # Playwright Maps + curl client
│   ├── processing/            # Enrichment + batch processing
│   ├── export/                # Card formatting + export
│   ├── utils/                 # Column mapper + utilities
│   ├── runner.py              # SIRENE-strategy pipeline orchestrator
│   ├── discovery.py # Maps-strategy pipeline orchestrator
│   ├── models.py              # Pydantic data models
│   ├── manage_users.py        # User management CLI
│   └── setup_users.py         # Initial user creation
├── tests/                     # Unit + integration tests
├── docs/                      # Contract documents
│   ├── database.md            # Database contract
│   └── pipeline.md            # Pipeline contract
├── .env                       # Local config (not in git)
├── CLAUDE.md                  # AI agent operating rules
└── pyproject.toml             # Package config
```

---

## Architecture

```
Frontend (Vanilla JS SPA)      API (FastAPI)              Pipeline (Python async)
┌──────────────────────┐   ┌──────────────────────┐   ┌──────────────────────────┐
│ dashboard.js         │──▶│ routes/dashboard.py  │   │ runner.py (SIRENE strat) │
│ new-batch.js         │──▶│ routes/batch.py      │──▶│ discovery.py             │
│ monitor.js (polling) │──▶│ routes/jobs.py       │   │  → interpreter.py        │
│ search.js            │──▶│ routes/companies.py  │   │  → triage.py             │
│ company.js           │──▶│ routes/export.py     │   │  → enricher.py           │
│ contacts.js          │──▶│ routes/contacts_list │   │  → batch.py              │
│ upload.js            │──▶│ routes/client.py     │   │  → maps.py               │
│ activity.js          │──▶│ routes/activity.py   │   │  → dedup.py              │
│ job.js               │──▶│ routes/notes.py      │   │  → checkpoint.py         │
└──────────────────────┘   └──────────────────────┘   └──────────────────────────┘
         │                          │                            │
         └──────────────────────────┴────────────────────────────┘
                                    │
                           PostgreSQL (Neon)
                    14.7M+ French companies (SIRENE)
```

---

## Pipeline Summary

Full details in [Pipeline Contract](fortress/docs/pipeline.md).

| Stage | Module | Purpose |
|-------|--------|---------|
| 1. Interpret | `interpreter.py` | User input → SQL on 14.7M companies |
| 2. Triage | `triage.py` | Classify: BLACK / BLUE / GREEN (Data Bank) / YELLOW / RED |
| 3. Enrich | `enricher.py` | Maps (Playwright) → website crawl (curl_cffi). Qualify-or-replace loop. |
| 4. Wave Process | `batch.py` | Per-company save, checkpoint, cooldown |
| 5. Complete | `runner.py` | Status → completed, Chrome cleanup |

**Two pipeline strategies:**
- **SIRENE** (default): Query 14.7M companies → filter → enrich via Maps + crawl
- **Maps Discovery**: Skip SIRENE, search Google Maps directly with user queries

---

## Database Tables

| Table | Rows | Purpose |
|-------|------|---------|
| `companies` | 14.7M+ | SIRENE registry + enriched fields |
| `contacts` | ~200+ | Scraped contact data (phone, email, website, socials) |
| `officers` | ~200+ | Directors from INPI/uploads |
| `query_tags` | ~200+ | Which batch/query found a company |
| `company_notes` | – | Per-company text annotations |
| `activity_log` | – | User action audit trail |
| `scrape_jobs` | – | Batch enrichment job tracking |
| `users` | – | Authentication |
| `blacklisted_sirens` | – | Companies to skip |

Full schema: [Database Contract](fortress/docs/database.md) or `database/schema.sql`.

---

## Quick Testing

```bash
# Syntax check
python3 -c "import ast; ast.parse(open('fortress/processing/enricher.py').read())"

# Start API
python3 -m fortress.api.main

# Small test batch (SIRENE strategy)
curl -X POST http://localhost:8080/api/batch/run \
  -H "Content-Type: application/json" \
  -d '{"sector":"transport","department":"34","size":5,"mode":"discovery"}'

# Monitor logs
tail -f fortress/data/logs/TRANSPORT_34_BATCH_*.log
```

---

## Known Gotchas

| Issue | Fix |
|-------|-----|
| Stale `__pycache__` | `find . -name __pycache__ -exec rm -rf {} +` |
| Old checkpoints cause skips | Clear `data/checkpoints/` when testing |
| Chrome sandbox failure (macOS) | Run outside `sandbox-exec` — Chrome needs temp dir access |
| Port confusion | API runs on **8080** — defined in `api/main.py` |
| npm cache permissions | `sudo chown -R $(whoami) ~/.npm` |

---

## License

Private / Internal use only.
