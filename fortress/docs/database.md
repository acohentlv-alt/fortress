# Fortress Database Contract

## Purpose

This document defines the database contract for Fortress:

* schema-level structure
* application merge behavior
* performance requirements
* canonical query patterns

## Source of Truth

* **Schema truth:** `database/schema.sql`
* **Application/runtime truth:** DB access layer and pipeline upsert code
* **This document:** human-readable contract derived from both

If this document conflicts with live schema or application code:

* **schema files win** for table structure, indexes, constraints
* **application code wins** for current runtime behavior
* the conflict must then be fixed in documentation immediately

---

## Enforcement Levels

Each rule in this doc falls into one of four categories:

* **Schema guarantee** — enforced by PostgreSQL schema/constraints/indexes
* **Application rule** — enforced in Fortress code
* **Operational policy** — required team practice, not necessarily enforced
* **Future improvement** — planned but not implemented

---

## Connection Contract

* **Application rule:** API uses async `psycopg3` pool in `api/db.py`
* **Application rule:** pipeline uses separate `psycopg_pool.AsyncConnectionPool`
* **Application rule:** pipeline pool max size is **5**
* **Application rule:** all pipeline connections use **60-second statement timeout**

```text
DATABASE_URL=postgresql://localhost/fortress
```

---

# 1. Schema Contract

## 1.1 `companies` — Master company registry

**Purpose:** canonical identity registry imported from SIRENE.
**Cardinality:** 14.7M+ rows.
**Authority:** import pipeline, not enrichment pipeline.

| Column             | Type            | Nullable | Default | Contract                                      |
| ------------------ | --------------- | -------: | ------- | --------------------------------------------- |
| `siren`            | `VARCHAR(9)`    |       NO | —       | **PK**. Canonical company identity key        |
| `siret_siege`      | `VARCHAR(14)`   |      YES | `NULL`  | Head-office establishment ID                  |
| `denomination`     | `TEXT`          |      NO* | —       | Company name                                  |
| `naf_code`         | `VARCHAR(10)`   |      YES | `NULL`  | NAF/APE code                                  |
| `naf_libelle`      | `TEXT`          |      YES | `NULL`  | Human-readable NAF label                      |
| `forme_juridique`  | `TEXT`          |      YES | `NULL`  | Legal form                                    |
| `adresse`          | `TEXT`          |      YES | `NULL`  | Registry address                              |
| `code_postal`      | `VARCHAR(10)`   |      YES | `NULL`  | Postal code                                   |
| `ville`            | `TEXT`          |      YES | `NULL`  | City                                          |
| `departement`      | `VARCHAR(3)`    |      YES | `NULL`  | Department code                               |
| `region`           | `TEXT`          |      YES | `NULL`  | Region                                        |
| `statut`           | `VARCHAR(1)`    |       NO | `'A'`   | Activity status                               |
| `date_creation`    | `DATE`          |      YES | `NULL`  | Creation date                                 |
| `tranche_effectif` | `VARCHAR(10)`   |      YES | `NULL`  | Headcount bracket                             |
| `latitude`         | `NUMERIC(10,7)` |      YES | `NULL`  | Geo latitude                                  |
| `longitude`        | `NUMERIC(10,7)` |      YES | `NULL`  | Geo longitude                                 |
| `fortress_id`      | `SERIAL`        |       NO | auto    | Internal surrogate ID, not canonical identity |
| `created_at`       | `TIMESTAMP`     |       NO | `NOW()` | Insert timestamp                              |
| `updated_at`       | `TIMESTAMP`     |       NO | `NOW()` | Last update timestamp                         |

### Notes

* **Schema guarantee:** `siren` is the canonical primary key
* **Application rule:** enrichment pipeline does not mutate identity fields in `companies`
* **Operational policy:** `fortress_id` must not be used as the business identity key
* **Open point:** if `denomination` can be null in live schema or import data, this document must be corrected. Current contract assumes **NOT NULL**, but application queries still defensively filter null or placeholder names.

### Known data-quality caveat

Application code may still filter:

```sql
denomination IS NOT NULL AND denomination != '[ND]'
```

This is a **data-quality guard**, not proof that the schema column is nullable.

### Indexes

| Index                           | Columns                           | Type  | Purpose                            |
| ------------------------------- | --------------------------------- | ----- | ---------------------------------- |
| `companies_pkey`                | `(siren)`                         | PK    | Single-company lookup              |
| `idx_companies_dept_naf_statut` | `(departement, naf_code, statut)` | btree | Primary search path for dept + NAF |
| `idx_companies_naf_dept_statut` | `(naf_code, departement, statut)` | btree | Exact/prefix NAF-first filtering   |
| `idx_companies_naf_statut`      | `(naf_code, statut)`              | btree | Nationwide NAF filtering           |
| `idx_companies_naf`             | `(naf_code)`                      | btree | Simple NAF lookups                 |
| `idx_companies_dept`            | `(departement)`                   | btree | Simple department lookups          |
| `idx_companies_cp`              | `(code_postal)`                   | btree | Postal code lookups                |
| `idx_companies_fortress`        | `(fortress_id)`                   | btree | Internal surrogate lookup          |

### Constraints

* **Schema guarantee:** PK on `siren`
* **Not currently enforced:** `CHECK (statut IN ('A', 'C'))` — see recommendations

---

## 1.2 `contacts` — Collected contact data

**Purpose:** resolved source-level contact record storage.
**Granularity:** one row per `(siren, source)`.
**Known limitation:** too coarse for multi-office / multi-contact truth.

| Column            | Type           | Nullable | Default | Contract                                                |
| ----------------- | -------------- | -------: | ------- | ------------------------------------------------------- |
| `id`              | `SERIAL`       |       NO | auto    | PK                                                      |
| `siren`           | `VARCHAR(9)`   |       NO | —       | FK to `companies(siren)`                                |
| `phone`           | `VARCHAR(20)`  |      YES | `NULL`  | Resolved phone                                          |
| `email`           | `TEXT`         |      YES | `NULL`  | Resolved email                                          |
| `email_type`      | `VARCHAR(20)`  |      YES | `NULL`  | `found` / `synthesized` / `generic`                     |
| `website`         | `TEXT`         |      YES | `NULL`  | Resolved website                                        |
| `source`          | `VARCHAR(30)`  |       NO | —       | `google_maps` / `website_crawl` / `inpi` / `web_search` |
| `social_linkedin` | `TEXT`         |      YES | `NULL`  | LinkedIn URL                                            |
| `social_facebook` | `TEXT`         |      YES | `NULL`  | Facebook URL                                            |
| `social_twitter`  | `TEXT`         |      YES | `NULL`  | X/Twitter URL                                           |
| `address`         | `TEXT`         |      YES | `NULL`  | Maps-observed address                                   |
| `rating`          | `NUMERIC(3,1)` |      YES | `NULL`  | Maps rating                                             |
| `review_count`    | `INTEGER`      |      YES | `NULL`  | Maps review count                                       |
| `maps_url`        | `TEXT`         |      YES | `NULL`  | Maps listing URL                                        |
| `collected_at`    | `TIMESTAMP`    |       NO | `NOW()` | Last collection timestamp                               |

### Constraints and indexes

| Name                        | Definition                                         | Purpose                              |
| --------------------------- | -------------------------------------------------- | ------------------------------------ |
| `contacts_pkey`             | PK `(id)`                                          | Row identity                         |
| `contacts_siren_source_key` | UNIQUE `(siren, source)`                           | One resolved row per source          |
| `contacts_siren_fkey`       | FK `(siren) -> companies(siren) ON DELETE CASCADE` | Referential integrity                |
| `idx_contacts_siren`        | `(siren)`                                          | Required for company contact fetches |

### Notes

* **Schema guarantee:** one row per `(siren, source)`
* **Application rule:** rows with no usable data are skipped before insert
* **Operational policy:** empty strings must be normalized to `NULL` before persistence
* **Known limitation:** this model cannot represent multiple offices, multiple emails, or multiple phones from the same source without loss

---

## 1.3 `officers` — Directors and officers

| Column          | Type          | Nullable | Default  |
| --------------- | ------------- | -------: | -------- |
| `id`            | `SERIAL`      |       NO | auto     |
| `siren`         | `VARCHAR(9)`  |       NO | —        |
| `nom`           | `TEXT`        |       NO | —        |
| `prenom`        | `TEXT`        |      YES | `NULL`   |
| `role`          | `TEXT`        |      YES | `NULL`   |
| `civilite`      | `VARCHAR(10)` |      YES | `NULL`   |
| `email_direct`  | `VARCHAR(255)`|      YES | `NULL`   |
| `ligne_directe` | `VARCHAR(20)` |      YES | `NULL`   |
| `code_fonction` | `VARCHAR(20)` |      YES | `NULL`   |
| `type_fonction` | `VARCHAR(20)` |      YES | `NULL`   |
| `source`        | `VARCHAR(30)` |       NO | `'inpi'` |
| `collected_at`  | `TIMESTAMP`   |       NO | `NOW()`  |

### Contract

* **Application rule:** insert-only behavior
* **Application rule:** duplicates ignored via `ON CONFLICT DO NOTHING`
* **Logical uniqueness:** `(siren, nom, COALESCE(prenom, ''))` — expression index
* **New fields:** `ligne_directe` and `email_direct` hold direct officer contact info (populated via CSV upload or INPI)

---

## 1.4 `query_tags` — Company-to-query mapping

| Column       | Type         | Nullable | Default |
| ------------ | ------------ | -------: | ------- |
| `siren`      | `VARCHAR(9)` |       NO | —       |
| `query_name` | `TEXT`       |       NO | —       |
| `tagged_at`  | `TIMESTAMP`  |       NO | `NOW()` |

### Contract

* **Schema guarantee:** PK `(siren, query_name)`
* **Application rule:** re-tagging is idempotent (`DO NOTHING`)
* **Operational policy:** tags are treated as durable batch membership history unless explicitly cleared by admin

---

## 1.5 `scrape_jobs` — Batch job state

| Column              | Type          | Nullable | Default | Description                 |
| ------------------- | ------------- | -------: | ------- | --------------------------- |
| `id`                | `SERIAL`      |       NO | auto    | PK                          |
| `query_id`          | `TEXT`        |       NO | —       | External batch identifier   |
| `query_name`        | `TEXT`        |       NO | —       | Human query                 |
| `worker_id`         | `TEXT`        |      YES | `NULL`  | Processing worker ID        |
| `user_id`           | `INTEGER`     |      YES | `NULL`  | Requesting user ID          |
| `status`            | `VARCHAR(20)` |       NO | `'new'` | Job state                   |
| `batch_size`        | `INTEGER`     |      YES | `0`     | Requested target count      |
| `total_companies`   | `INTEGER`     |      YES | `0`     | Candidate pool size         |
| `companies_scraped` | `INTEGER`     |      YES | `0`     | Progress numerator          |
| `companies_qualified`| `INTEGER`    |      YES | `0`     | Phone-confirmed count (MVP) |
| `companies_failed`  | `INTEGER`     |      YES | `0`     | Failed enrichments          |
| `replaced_count`    | `INTEGER`     |      YES | `0`     | Replacements used           |
| `wave_current`      | `INTEGER`     |      YES | `0`     | Current wave                |
| `wave_total`        | `INTEGER`     |      YES | `0`     | Total waves                 |
| `triage_black`      | `INTEGER`     |      YES | `0`     | BLACK count                 |
| `triage_blue`       | `INTEGER`     |      YES | `0`     | BLUE count                  |
| `triage_green`      | `INTEGER`     |      YES | `0`     | GREEN count                 |
| `triage_yellow`     | `INTEGER`     |      YES | `0`     | YELLOW count                |
| `triage_red`        | `INTEGER`     |      YES | `0`     | RED count                   |
| `batch_number`      | `INT`         |       NO | `1`     | 1-based batch index         |
| `batch_offset`      | `INT`         |       NO | `0`     | Source offset               |
| `filters_json`      | `TEXT`        |      YES | `NULL`  | Serialized advanced filters |
| `strategy`          | `VARCHAR(20)` |       NO | `'sirene'` | `sirene` or `maps`       |
| `search_queries`    | `JSONB`       |      YES | `NULL`  | Maps discovery queries      |
| `mode`              | `VARCHAR(20)` |      YES | `'discovery'` | `discovery` or `upload` |
| `cancel_requested`  | `BOOLEAN`     |      YES | `false` | Cancellation flag           |
| `created_at`        | `TIMESTAMP`   |       NO | `NOW()` | Created time                |
| `updated_at`        | `TIMESTAMP`   |       NO | `NOW()` | Heartbeat/update time       |

### Job state machine (current)

```text
new → triage → queued → in_progress → completed | failed
```

| From          | To            | Enforced by |
| ------------- | ------------- | ----------- |
| `new`         | `triage`      | application |
| `triage`      | `queued`      | application |
| `queued`      | `in_progress` | application |
| `in_progress` | `completed`   | application |
| `in_progress` | `failed`      | application |

* **Application rule:** state machine is managed in application code, not DB-enforced
* **Application rule:** `cancel_requested` checked between waves for graceful stop
* **Operational policy:** `updated_at` is heartbeat-touched every 60s during active execution

---

## 1.6 `rejected_sirens` — No Maps presence cache

| Column        | Type         | Nullable | Default |
| ------------- | ------------ | -------: | ------- |
| `siren`       | `VARCHAR(9)` |       NO | —       |
| `naf_prefix`  | `VARCHAR(5)` |       NO | —       |
| `departement` | `VARCHAR(3)` |       NO | —       |
| `reason`      | `TEXT`       |      YES | `NULL`  |
| `rejected_at` | `TIMESTAMP`  |       NO | `NOW()` |

### Contract

* **Schema guarantee:** PK `(siren, naf_prefix, departement)`
* **Application rule:** only written when Maps returns zero results
* **Business rule:** do not reject companies merely because Maps location differs from expected city/postcode — companies may have multiple offices across France

---

## 1.7 `company_notes` — Per-company text annotations

| Column       | Type          | Nullable | Default  |
| ------------ | ------------- | -------: | -------- |
| `id`         | `SERIAL`      |       NO | auto     |
| `siren`      | `VARCHAR(9)`  |       NO | —        |
| `user_id`    | `INTEGER`     |       NO | —        |
| `username`   | `VARCHAR(50)` |       NO | —        |
| `text`       | `TEXT`        |       NO | —        |
| `created_at` | `TIMESTAMP`   |       NO | `NOW()`  |

### Contract

* **Schema guarantee:** PK `(id)`, FK to `companies(siren)` with CASCADE
* **Application rule:** author or admin can delete, all users can read
* **Index:** `idx_company_notes_siren` on `(siren)`

---

## 1.8 `activity_log` — User action audit trail

| Column        | Type          | Nullable | Default |
| ------------- | ------------- | -------: | ------- |
| `id`          | `SERIAL`      |       NO | auto    |
| `user_id`     | `INTEGER`     |       NO | —       |
| `username`    | `VARCHAR(50)` |       NO | —       |
| `action`      | `VARCHAR(50)` |       NO | —       |
| `target_type` | `VARCHAR(30)` |      YES | `NULL`  |
| `target_id`   | `TEXT`        |      YES | `NULL`  |
| `details`     | `TEXT`        |      YES | `NULL`  |
| `created_at`  | `TIMESTAMP`   |       NO | `NOW()` |

### Contract

* **Application rule:** append-only (no deletes except admin cleanup)
* **Application rule:** logged for: note_added, note_deleted, company_edited, enrichment_started
* **Index:** on `(created_at DESC)` for recent activity queries

---

## 1.9 `users` — Authentication

| Column     | Type          | Nullable | Default |
| ---------- | ------------- | -------: | ------- |
| `id`       | `SERIAL`      |       NO | auto    |
| `username` | `VARCHAR(50)` |       NO | —       |
| `password` | `TEXT`        |       NO | —       |
| `role`     | `VARCHAR(20)` |       NO | `'user'`|

### Contract

* **Schema guarantee:** PK `(id)`, UNIQUE `(username)`
* **Application rule:** password stored as bcrypt hash
* **Roles:** `admin` (full access), `user` (standard access)

---

## 1.10 Other tables

| Table                | Purpose                               | Contract type        |
| -------------------- | ------------------------------------- | -------------------- |
| `blacklisted_sirens` | SIRENs never scraped                  | Manual/admin-managed |
| `client_sirens`      | Client-owned company suppression list | Idempotent upload    |
| `scrape_audit`       | Append-only action log                | Application-managed  |
| `enrichment_log`     | Append-only diagnostic outcome log    | Application-managed  |
| `inpi_usage`         | Daily INPI usage counter              | Daily upsert         |

---

# 2. Merge Semantics

## 2.1 `companies` merge policy

### Current behavior

```sql
ON CONFLICT (siren) DO UPDATE SET
    denomination     = EXCLUDED.denomination,
    naf_code         = EXCLUDED.naf_code,
    -- ... identity fields overwritten ...
    latitude         = COALESCE(EXCLUDED.latitude, companies.latitude),
    longitude        = COALESCE(EXCLUDED.longitude, companies.longitude),
    updated_at       = NOW()
```

| Field class     | Current policy                      |
| --------------- | ----------------------------------- |
| identity fields | overwrite with authoritative import |
| geo coordinates | fill-only (COALESCE)                |
| timestamps      | update on write                     |

* **Application rule:** identity fields from SIRENE overwrite existing values
* **Application rule:** geo fields are fill-only
* **Operational policy:** enrichment pipeline must not write to identity fields

---

## 2.2 `contacts` merge policy

### Current behavior

```sql
ON CONFLICT (siren, source) DO UPDATE SET
    phone            = COALESCE(EXCLUDED.phone, contacts.phone),
    email            = COALESCE(EXCLUDED.email, contacts.email),
    website          = COALESCE(EXCLUDED.website, contacts.website),
    address          = COALESCE(EXCLUDED.address, contacts.address),
    social_linkedin  = COALESCE(EXCLUDED.social_linkedin, contacts.social_linkedin),
    social_facebook  = COALESCE(EXCLUDED.social_facebook, contacts.social_facebook),
    social_twitter   = COALESCE(EXCLUDED.social_twitter, contacts.social_twitter),
    rating           = COALESCE(EXCLUDED.rating, contacts.rating),
    review_count     = COALESCE(EXCLUDED.review_count, contacts.review_count),
    maps_url         = COALESCE(EXCLUDED.maps_url, contacts.maps_url),
    collected_at     = EXCLUDED.collected_at
```

### Field-level rules (current)

| Field          | Policy    | Notes                               |
| -------------- | --------- | ----------------------------------- |
| `phone`        | fill-only | first non-null wins, never overwritten |
| `email`        | fill-only | first non-null wins |
| `website`      | fill-only | first non-null wins |
| `address`      | fill-only | Maps-observed address preserved |
| `rating`       | fill-only | may become stale over time |
| `review_count` | fill-only | may become stale over time |
| `social_*`     | fill-only | first discovery wins |
| `maps_url`     | fill-only | first listing wins |
| `collected_at` | overwrite | always updated to latest touch |

### Edge cases (current behavior)

* Empty string (`''`) is NOT the same as NULL — `COALESCE` preserves empty strings. Pipeline should send NULL, never `''`.
* Bad/low-confidence data cannot currently downgrade a better existing row.
* If a source produces different data on a second run, the existing data wins. **No "fresher wins" logic exists today.**
* Ghost row prevention: `upsert_contact()` skips insert when all of (phone, email, website, address, rating, maps_url) are NULL.

---

## 2.3 `officers` merge policy

```sql
ON CONFLICT DO NOTHING
```

* Insert-only. Duplicate officers (same siren + name) are silently ignored.

---

## 2.4 `query_tags` merge policy

```sql
ON CONFLICT (siren, query_name) DO NOTHING
```

* Idempotent. Tags are durable and not auto-expired.

---

## 2.5 Best contact resolution (current)

When multiple rows exist in `contacts` for one SIREN, the application picks the "best" row.

### Current ORDER BY

```sql
ORDER BY c2.siren,
    (CASE WHEN c2.phone   IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN c2.email   IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN c2.website IS NOT NULL THEN 1 ELSE 0 END) DESC
```

* **Current behavior:** ranked by completeness score only (count of non-null MVP fields)
* **Current gap:** no tie-breaker — when two rows have equal completeness, PostgreSQL picks arbitrarily
* **No source hierarchy:** Maps does not automatically outrank crawl

---

# 3. Performance Contract

## Principles

1. **No sequential scans on hot paths over `companies`**
2. **Every new query must be validated with `EXPLAIN ANALYZE`**
3. **Pagination is mandatory on unbounded result sets**
4. **Pipeline connections use 60-second statement timeout**
5. **Canonical query shapes should not be rewritten ad hoc in routes**

## Required index use by query family

| Query pattern             | Expected supporting index       | Expected scale     |
| ------------------------- | ------------------------------- | ------------------ |
| department + NAF + status | `idx_companies_dept_naf_statut` | 100–50K rows       |
| NAF + department + status | `idx_companies_naf_dept_statut` | 100–5K rows        |
| nationwide NAF + status   | `idx_companies_naf_statut`      | 1K–200K rows       |
| single SIREN lookup       | `companies_pkey`                | 1 row              |
| contacts by SIREN         | `idx_contacts_siren`            | typically 1–5 rows |

**Note:** for prefix patterns such as `naf_code LIKE '49%'`, the expected index plan must be confirmed with `EXPLAIN ANALYZE`. Do not assume planner behavior without verification.

## Query ownership

* Canonical query shapes belong in shared DB access code, not duplicated across API routes
* API routes must not run pipeline-only scan-heavy selection logic
* Replacement selection queries are pipeline-only

## Retention policy (current)

| Table             | Current state   | Notes                           |
| ----------------- | --------------- | ------------------------------- |
| `companies`       | permanent       | 14.7M rows, never deleted       |
| `contacts`        | permanent       | Admin can delete for testing    |
| `query_tags`      | permanent       | Admin can clear for testing     |
| `scrape_jobs`     | permanent       | No auto-cleanup                 |
| `enrichment_log`  | no auto-cleanup | Append-only, can grow unbounded |
| `scrape_audit`    | no auto-cleanup | Append-only, can grow unbounded |
| `rejected_sirens` | permanent       | Prevents re-scraping dead ends  |

---

# 4. Canonical Query Patterns

## 4.1 Best contact per company

**Usage:** API, export, batch result presentation
**Scope:** bounded company sets only

```sql
SELECT DISTINCT ON (c2.siren)
    c2.siren, c2.phone, c2.email, c2.website, ...
FROM contacts c2
WHERE c2.siren IN (
    SELECT DISTINCT siren FROM scrape_audit WHERE query_id = %s
)
ORDER BY c2.siren,
    (CASE WHEN c2.phone   IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN c2.email   IS NOT NULL THEN 1 ELSE 0 END +
     CASE WHEN c2.website IS NOT NULL THEN 1 ELSE 0 END) DESC
```

* **Index:** `idx_contacts_siren`
* **Expected cardinality:** bounded by batch scope (max ~200)
* **API-safe:** yes (sub-50ms for typical batches)
* **Current gap:** no tie-breaker for equal completeness scores

## 4.2 Companies for a batch

**Usage:** API, export, pipeline reporting

```sql
SELECT DISTINCT siren FROM scrape_audit WHERE query_id = %s
```

* **Index:** `idx_audit_query`
* **Expected rows:** ≤ batch_size × 2 (includes replacements)
* **API-safe:** yes

## 4.3 Replacement candidates

**Usage:** pipeline only — NOT for API routes

```sql
SELECT siren, denomination, naf_code, departement, code_postal, ville
FROM companies
WHERE departement = %s
  AND naf_code LIKE %s
  AND statut = 'A'
  AND denomination IS NOT NULL
  AND denomination != '[ND]'
  AND siren != ALL(%s)
LIMIT %s
```

* **Index:** `idx_companies_dept_naf_statut`
* **Input requirement:** NAF pattern must be prefix-only (`49%`), never `%49%`
* **Pre-fetched in bulk at batch start.** Do not execute repeatedly per replacement.

## 4.4 Dashboard stats

**Usage:** dashboard bounded analytics

```sql
SELECT COUNT(*) AS total_companies,
       COUNT(phone) AS with_phone, COUNT(email) AS with_email
FROM (SELECT DISTINCT siren FROM query_tags) qt
JOIN companies co ON co.siren = qt.siren
LEFT JOIN (... best contact CTE ...) bc ON bc.siren = co.siren
```

* **Safety:** bounded by `query_tags` count (typically < 5,000), not 14.7M rows
* Whole-database dashboard queries are not allowed without explicit review

---

# 5. Known Limitations

1. `contacts` is source-resolved storage, not raw fact storage — multiple offices/numbers/emails are lossy
2. Fill-only COALESCE means freshness-sensitive fields (rating, review_count) may become stale
3. Job state machine is application-managed, not DB-enforced
4. Best contact resolution has no tie-breaker for equal completeness
5. Retention for append-only tables is advisory — no automated cleanup exists
6. Empty strings are not normalized to NULL at the DB level — application must enforce

---

# 6. Recommendations & Next Steps

### Schema improvements

1. **Add `CHECK (statut IN ('A', 'C'))`** to `companies` — currently not DB-enforced
2. **Add tie-breakers to best contact ORDER BY** — recommended:
   ```sql
   ORDER BY c2.siren,
       (CASE WHEN phone IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END +
        CASE WHEN website IS NOT NULL THEN 1 ELSE 0 END) DESC,
       CASE WHEN phone IS NOT NULL THEN 1 ELSE 0 END DESC,
       CASE WHEN email IS NOT NULL THEN 1 ELSE 0 END DESC,
       CASE WHEN website IS NOT NULL THEN 1 ELSE 0 END DESC,
       collected_at DESC,
       source ASC
   ```
3. **Verify `denomination` NOT NULL** against actual SIRENE import data

### Contact model redesign

4. **Replace `contacts` with raw fact + resolved materialization:**
   - `contact_facts_raw` — one row per observed fact
   - `best_contact_view` — materialized or query-time resolution
   - Enables per-field confidence, provenance, and multi-office support

### Operational improvements

5. **Add field-level confidence and provenance** to contact data
6. **Formalize retry/recovery transitions** for `scrape_jobs` failed state
7. **Implement 90-day automated retention** for `enrichment_log` and `scrape_audit`
8. **Add DB-enforced constraints** where practical (status enum, source enum)
9. **Document exact named constraints** directly from live schema generation
