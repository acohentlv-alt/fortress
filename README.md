# Fortress

B2B lead collection and enrichment system for France. Operates on a PostgreSQL database of 14.7M+ companies from the French SIRENE registry.

## What It Does

Fortress discovers, filters, enriches, and exports French company contact data through a web UI and background batch pipeline.

**Input:** A search query like "transport 66" (transport companies in département 66)  
**Output:** Enriched company records with phone, email, website, Google Maps rating, social links

## Tech Stack

- **Backend:** Python 3.13, FastAPI, psycopg3 (async)
- **Frontend:** Vanilla JS SPA (hash routing, no frameworks)
- **Database:** PostgreSQL (14.7M companies)
- **Scraping:** Playwright Chromium (Google Maps), curl_cffi (company websites)
- **Config:** Pydantic Settings from `.env`

## Setup

### Prerequisites
- Python 3.13+
- PostgreSQL with the `fortress` database loaded (SIRENE import)
- Playwright browsers installed (`playwright install chromium`)

### Installation

```bash
cd fortress/
pip install -e .
cp .env.example .env   # Edit with your DB credentials
```

### Running

```bash
# Start the API + frontend
python3 -m fortress.api.main

# Visit http://localhost:8082
```

## Project Structure

```
fortress/
├── fortress/                  # Python package
│   ├── api/                   # FastAPI routes + DB pool
│   │   ├── routes/            # Endpoint handlers
│   │   ├── db.py              # Async connection pool
│   │   └── main.py            # App entry point
│   ├── config/                # Pydantic settings
│   ├── database/              # Schema SQL, migrations
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
├── tests/                     # Unit + integration tests
├── .env                       # Local config (not in git)
└── pyproject.toml             # Package config
```

## Documentation

- [Pipeline Architecture](fortress/docs/pipeline.md) — How the enrichment engine works
- [Database Reference](fortress/docs/database.md) — Schema, indexes, important queries
- [CLAUDE.md](CLAUDE.md) — AI agent instructions

## License

Private / Internal use only.
