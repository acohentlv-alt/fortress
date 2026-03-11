# Database Reference

## Connection

```python
# Settings from .env
DATABASE_URL=postgresql://localhost/fortress
```

Async pool via `psycopg3` in `api/db.py`. Pipeline uses `psycopg_pool.AsyncConnectionPool`.

## Tables

### `companies` — Master company registry (14.7M+ rows)

| Column | Type | Description |
|--------|------|-------------|
| `siren` | VARCHAR(9) PK | French company ID |
| `denomination` | TEXT | Company name |
| `naf_code` | VARCHAR(10) | Industry code (e.g., "49.41A") |
| `departement` | VARCHAR(3) | Department code (e.g., "66") |
| `ville` | TEXT | City |
| `code_postal` | VARCHAR(10) | Postal code |
| `statut` | VARCHAR(1) | A=Active, C=Closed |

**Critical indexes (query performance depends on these):**
```sql
idx_companies_dept_naf_statut ON (departement, naf_code, statut)  -- dept + NAF queries
idx_companies_naf_dept_statut ON (naf_code, departement, statut)  -- exact NAF filtering
idx_companies_naf_statut      ON (naf_code, statut)               -- nationwide queries
```

### `contacts` — Collected contact data (multiple rows per SIREN)

| Column | Type | Description |
|--------|------|-------------|
| `siren` | VARCHAR(9) FK | References companies |
| `phone` | VARCHAR(20) | Best phone number |
| `email` | TEXT | Best email address |
| `website` | TEXT | Company website |
| `source` | VARCHAR(30) | `google_maps` / `website_crawl` |
| `rating` | NUMERIC(3,1) | Google Maps rating |
| `review_count` | INTEGER | Number of reviews |
| `maps_url` | TEXT | Google Maps link |
| `social_linkedin` | TEXT | LinkedIn URL |
| `social_facebook` | TEXT | Facebook URL |

**Unique constraint:** `(siren, source)` — one row per source per company  
**Merge rule:** `ON CONFLICT (siren, source) DO UPDATE` — never overwrite non-null fields

### `query_tags` — N:N mapping (companies ↔ queries)

| Column | Type | Description |
|--------|------|-------------|
| `siren` | VARCHAR(9) | Company |
| `query_name` | TEXT | Query (e.g., "transport 66") |
| PK | (siren, query_name) | |

Used by dashboard and job views to scope companies to queries.

### `scrape_jobs` — Batch job tracking

Key columns: `query_id`, `status`, `batch_size`, `companies_scraped`, `replaced_count`, `wave_current`, `wave_total`, `triage_*`

Status lifecycle: `queued → in_progress → completed / failed`

### `enrichment_log` — Per-company diagnostic (admin tool)

| Column | Type | Description |
|--------|------|-------------|
| `query_id` | VARCHAR(100) | Batch ID |
| `siren` | VARCHAR(9) | Company processed |
| `outcome` | VARCHAR(20) | `qualified` / `replaced` / `failed` |
| `maps_phone` | TEXT | Phone found by Maps |
| `maps_website` | TEXT | Website found by Maps |
| `emails_found` | INT | Count from crawl |
| `replace_reason` | VARCHAR(30) | `no_maps_data` / `low_confidence` |
| `time_ms` | INT | Processing time |

### `rejected_sirens` — Companies with no Maps presence

| Column | Type | Description |
|--------|------|-------------|
| `siren` | VARCHAR(9) | Company |
| `naf_prefix` | VARCHAR(5) | NAF code prefix (e.g., "52") |
| `departement` | VARCHAR(3) | Department |
| `reason` | TEXT | `no_maps_data` |
| PK | (siren, naf_prefix, departement) | Scoped per query pattern |

### Other tables

- `officers` — Directors from INPI (siren, nom, prenom, role)
- `blacklisted_sirens` — Companies that must never be scraped
- `client_sirens` — Companies the client already has (triage BLUE)
- `scrape_audit` — Action log for all scraping operations
- `inpi_usage` — Daily INPI API request counter

## Important Queries

### Best contact per company (used by jobs.py)
```sql
SELECT DISTINCT ON (c2.siren)
    c2.siren, c2.phone, c2.email, c2.website
FROM contacts c2
ORDER BY c2.siren,
    (CASE WHEN c2.phone IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN c2.email IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN c2.website IS NOT NULL THEN 1 ELSE 0 END) DESC
```

### Companies for a batch (scoped via scrape_audit)
```sql
SELECT DISTINCT siren FROM scrape_audit WHERE query_id = %s
```

### Replacement candidates
```sql
SELECT siren, denomination, naf_code, departement, code_postal, ville
FROM companies
WHERE departement = %s AND naf_code LIKE %s AND statut = 'A'
  AND denomination != '[ND]' AND denomination IS NOT NULL
  AND siren != ALL(%s)
LIMIT %s
```

## Performance Rules

1. **Every query MUST hit an index** — no sequential scans on 14.7M rows
2. **Always use `EXPLAIN ANALYZE`** to verify index usage
3. **Pagination mandatory** — never `SELECT *` without `LIMIT`
4. **60-second statement timeout** set on pipeline connections
5. **`ON CONFLICT DO UPDATE`** for all inserts — never raw INSERT
