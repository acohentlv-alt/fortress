# Fortress

B2B lead collection and enrichment system for France. Operates on a PostgreSQL database of 14.7M+ companies from the French SIRENE registry.

## Purpose

Fortress discovers, filters, enriches, and exports French company contact data through a web UI and background batch pipeline.

* **Input:** A search query like "transport 66" (transport companies in département 66)
* **Output:** Enriched company records with phone, email, website, Google Maps rating, social links

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
| Database | PostgreSQL (14.7M+ companies) |
| Maps scraping | Playwright Chromium (Google Maps) |
| Website scraping | curl_cffi (Chrome TLS impersonation) |
| Configuration | Pydantic Settings from `.env` |

---

## Setup

### Prerequisites

* Python 3.13+
* PostgreSQL with the `fortress` database loaded (SIRENE import)
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
# API + frontend at http://localhost:8080
```

---

## Project Structure

```
fortress/
├── fortress/                  # Python package
│   ├── api/                   # FastAPI routes + DB pool
│   │   ├── routes/            # Endpoint handlers
│   │   ├── db.py              # Async connection pool
│   │   └── main.py            # App entry point (port 8080)
│   ├── config/                # Pydantic settings
│   ├── database/              # schema.sql — definitive schema
│   ├── data/                  # Runtime data
│   │   ├── logs/              # Per-batch log files
│   │   ├── checkpoints/       # Wave resume bookmarks
│   │   └── outputs/           # JSONL card files
│   ├── frontend/              # HTML/JS/CSS SPA
│   ├── module_a/              # Query interpretation + triage
│   ├── module_b/              # Contact parsing + web search
│   ├── module_c/              # Playwright Maps + curl client
│   ├── module_d/              # Enrichment + batch processing
│   ├── module_e/              # Card formatting + export
│   ├── runner.py              # Background pipeline orchestrator
│   └── models.py              # Pydantic data models
├── docs/                      # Contract documents
│   ├── database.md            # Database contract
│   └── pipeline.md            # Pipeline contract
├── tests/                     # Unit + integration tests
├── .env                       # Local config (not in git)
├── CLAUDE.md                  # AI agent operating rules
└── pyproject.toml             # Package config
```

---

## Architecture Overview

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

## Pipeline Summary

Full details in [Pipeline Contract](fortress/docs/pipeline.md).

| Stage | Module | Purpose |
|-------|--------|---------|
| 1. Interpret | `query_interpreter.py` | User input → SQL on 14.7M companies |
| 2. Triage | `triage.py` | Classify: BLACK / BLUE / GREEN / YELLOW / RED |
| 3. Enrich | `enricher.py` | Maps (Playwright) → website crawl (curl_cffi) |
| 4. Wave Process | `batch_processor.py` | Per-company save, checkpoint, cooldown |
| 5. Complete | `runner.py` | Status → completed, Chrome cleanup |

---

## Quick Testing

```bash
# Syntax check
python3 -c "import ast; ast.parse(open('fortress/module_d/enricher.py').read())"

# Start API
python3 -m fortress.api.main

# Small test batch
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

---

## License

Private / Internal use only.
