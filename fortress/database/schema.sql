-- Fortress PostgreSQL Schema
-- Applied automatically on first docker-compose up via /docker-entrypoint-initdb.d/

-- ---------------------------------------------------------------------------
-- Companies — master company registry (one row per SIREN)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS companies (
    siren               VARCHAR(9)      PRIMARY KEY,
    siret_siege         VARCHAR(14),
    denomination        TEXT            NOT NULL,
    enseigne            TEXT,                                    -- commercial/trade name (from SIRENE StockEtablissement)
    naf_code            VARCHAR(10),
    naf_libelle         TEXT,
    forme_juridique     TEXT,
    adresse             TEXT,
    code_postal         VARCHAR(10),
    ville               TEXT,
    departement         VARCHAR(3),
    region              TEXT,
    statut              VARCHAR(1)      NOT NULL DEFAULT 'A',   -- A=Actif, C=Cessé
    date_creation       DATE,
    tranche_effectif    VARCHAR(10),
    latitude            NUMERIC(10, 7),
    longitude           NUMERIC(10, 7),
    fortress_id         SERIAL,                                 -- permanent unique ID, assigned on first insert
    created_at          TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at          TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_companies_naf        ON companies (naf_code);
CREATE INDEX IF NOT EXISTS idx_companies_dept       ON companies (departement);
CREATE INDEX IF NOT EXISTS idx_companies_cp         ON companies (code_postal);
CREATE INDEX IF NOT EXISTS idx_companies_statut     ON companies (statut);
CREATE INDEX IF NOT EXISTS idx_companies_fortress   ON companies (fortress_id);

-- Compound indexes for query_interpreter.py (WHERE departement + naf_code + statut)
CREATE INDEX IF NOT EXISTS idx_companies_dept_naf
    ON companies (departement, naf_code);
CREATE INDEX IF NOT EXISTS idx_companies_dept_naf_statut
    ON companies (departement, naf_code, statut);

-- Reversed compound: naf_code-first for exact NAF filtering + department
-- Used when users specify a precise NAF code (e.g. 49.41A) via the UI → 21ms vs 7.4s
CREATE INDEX IF NOT EXISTS idx_companies_naf_statut
    ON companies (naf_code, statut);
CREATE INDEX IF NOT EXISTS idx_companies_naf_dept_statut
    ON companies (naf_code, departement, statut);

-- ---------------------------------------------------------------------------
-- Contacts — collected contact data (multiple rows per SIREN, one per source)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS contacts (
    id              SERIAL          PRIMARY KEY,
    siren           VARCHAR(9)      NOT NULL REFERENCES companies (siren) ON DELETE CASCADE,
    phone           VARCHAR(20),
    email           TEXT,
    email_type      VARCHAR(20),    -- 'found' | 'synthesized' | 'generic'
    website         TEXT,
    source          VARCHAR(30)     NOT NULL,   -- 'website_crawl' | 'google_maps' | 'inpi' | ...
    social_linkedin TEXT,
    social_facebook TEXT,
    social_twitter  TEXT,
    address         TEXT,
    rating          NUMERIC(3, 1),
    review_count    INTEGER,
    maps_url        TEXT,
    collected_at    TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- Unique per (siren, source) so ON CONFLICT upserts work correctly
CREATE UNIQUE INDEX IF NOT EXISTS idx_contacts_siren_source ON contacts (siren, source);
CREATE INDEX IF NOT EXISTS idx_contacts_siren ON contacts (siren);
CREATE INDEX IF NOT EXISTS idx_contacts_phone ON contacts (phone);

-- ---------------------------------------------------------------------------
-- Officers — company directors / officers from INPI
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS officers (
    id              SERIAL          PRIMARY KEY,
    siren           VARCHAR(9)      NOT NULL REFERENCES companies (siren) ON DELETE CASCADE,
    nom             TEXT            NOT NULL,
    prenom          TEXT,
    role            TEXT,
    source          VARCHAR(30)     NOT NULL DEFAULT 'inpi',
    collected_at    TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- Unique per (siren, nom, prenom) — DO NOTHING on duplicate officers
CREATE UNIQUE INDEX IF NOT EXISTS idx_officers_siren_nom_prenom ON officers (siren, nom, COALESCE(prenom, ''));
CREATE INDEX IF NOT EXISTS idx_officers_siren ON officers (siren);

-- ---------------------------------------------------------------------------
-- Query tags — N:N mapping between companies and named queries
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS batch_tags (
    siren           VARCHAR(9)      NOT NULL REFERENCES companies (siren) ON DELETE CASCADE,
    batch_name      TEXT            NOT NULL,
    tagged_at       TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (siren, batch_name)
);

CREATE INDEX IF NOT EXISTS idx_batch_tags_query ON batch_tags (batch_name);

-- ---------------------------------------------------------------------------
-- Scrape jobs — one row per user query (tracks waves, triage stats, status)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS batch_data (
    id                      SERIAL          PRIMARY KEY,
    batch_id                TEXT            NOT NULL,   -- e.g. 'AGRICULTURE_66'
    batch_name              TEXT            NOT NULL,   -- e.g. 'AGRICULTURE 66'
    status                  VARCHAR(20)     NOT NULL DEFAULT 'new',
        -- 'new' | 'triage' | 'queued' | 'in_progress' | 'paused' | 'completed' | 'failed'
    total_companies         INTEGER         DEFAULT 0,
    triage_black            INTEGER         DEFAULT 0,
    triage_green            INTEGER         DEFAULT 0,
    triage_yellow           INTEGER         DEFAULT 0,
    triage_red              INTEGER         DEFAULT 0,
    wave_current            INTEGER         DEFAULT 0,
    wave_total              INTEGER         DEFAULT 0,
    companies_scraped       INTEGER         DEFAULT 0,
    companies_failed        INTEGER         DEFAULT 0,
    lambda_requests_used    INTEGER         DEFAULT 0,
    batch_number            INT             NOT NULL DEFAULT 1,   -- 1-based batch index
    batch_offset            INT             NOT NULL DEFAULT 0,   -- SIRENE row offset for this batch
    filters_json            TEXT,                                 -- JSON-serialized advanced filters from UI
    batch_size              INTEGER         DEFAULT 0,            -- user-requested company count (stays constant)
    replaced_count          INTEGER         DEFAULT 0,            -- companies replaced by qualify-or-replace loop
    companies_qualified     INTEGER         DEFAULT 0,            -- companies with confirmed phone (MVP field)
    strategy                VARCHAR(10)     NOT NULL DEFAULT 'sirene', -- 'sirene' or 'maps'
    search_queries          JSONB,                                 -- Maps-first: JSON array of search terms
    created_at              TIMESTAMP       NOT NULL DEFAULT NOW(),
    updated_at              TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_batch_data_batch_id ON batch_data (batch_id);
CREATE INDEX IF NOT EXISTS idx_batch_data_status   ON batch_data (status);

-- ---------------------------------------------------------------------------
-- Blacklisted SIRENs — companies that must never be scraped
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS blacklisted_sirens (
    siren       VARCHAR(9)      PRIMARY KEY,
    reason      TEXT,
    added_by    VARCHAR(50),
    added_at    TIMESTAMP       NOT NULL DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Company notes — CRM comments tied to a specific SIREN (Issue 6)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS company_notes (
    id          SERIAL          PRIMARY KEY,
    siren       VARCHAR(9)      NOT NULL,
    user_id     INTEGER,
    username    VARCHAR(100),
    text        TEXT            NOT NULL,
    created_at  TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_notes_siren ON company_notes (siren);

-- ---------------------------------------------------------------------------
-- Scrape audit — complete action log for all scraping operations
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS batch_log (
    id              SERIAL          PRIMARY KEY,
    batch_id        TEXT            NOT NULL,
    siren           VARCHAR(9)      NOT NULL,
    action          VARCHAR(50)     NOT NULL,   -- 'inpi_lookup' | 'web_search' | 'website_crawl' | 'maps_lookup'
    result          VARCHAR(20)     NOT NULL,   -- 'success' | 'fail' | 'blocked' | 'skipped'
    source_url      TEXT,
    search_query    TEXT,                        -- the exact Maps search term that found this entity
    duration_ms     INTEGER,
    timestamp       TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_batch_log_batch_id  ON batch_log (batch_id);
CREATE INDEX IF NOT EXISTS idx_batch_log_siren  ON batch_log (siren);
CREATE INDEX IF NOT EXISTS idx_batch_log_action ON batch_log (action);

-- ---------------------------------------------------------------------------
-- INPI usage tracker — daily request counter (10K/day limit)
-- Created by inpi_client.py on first run; mirrored here for DB rebuild safety.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS inpi_usage (
    usage_date      DATE                     NOT NULL PRIMARY KEY DEFAULT CURRENT_DATE,
    requests_count  INTEGER                  DEFAULT 0,
    updated_at      TIMESTAMPTZ              DEFAULT NOW()
);

-- ---------------------------------------------------------------------------
-- Rejected SIRENs — companies tried by the qualify-or-replace pipeline
-- that had no Maps presence. Skipped on future runs of the same query.
-- Key: (siren, naf_prefix, departement) so rejection is per query pattern.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS rejected_sirens (
    siren           VARCHAR(9)      NOT NULL,
    naf_prefix      VARCHAR(5)      NOT NULL,   -- e.g. '52' for logistique
    departement     VARCHAR(3)      NOT NULL,   -- e.g. '33'
    reason          TEXT,                        -- e.g. 'no_maps_data', 'false_positive'
    rejected_at     TIMESTAMP       NOT NULL DEFAULT NOW(),
    PRIMARY KEY (siren, naf_prefix, departement)
);

CREATE INDEX IF NOT EXISTS idx_rejected_naf_dept ON rejected_sirens (naf_prefix, departement);

-- ---------------------------------------------------------------------------
-- Client SIRENs — companies the client already has in their CRM.
-- Used by triage to classify as BLUE (skip — client already owns).
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS client_sirens (
    siren           VARCHAR(9)      PRIMARY KEY,
    client_id       VARCHAR(50)     NOT NULL DEFAULT 'default',  -- future multi-tenancy
    source_file     TEXT,                                        -- original CSV filename
    uploaded_at     TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_client_sirens_client ON client_sirens (client_id);

-- Add triage_blue column to batch_data if not present
ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS triage_blue INTEGER DEFAULT 0;

-- ---------------------------------------------------------------------------
-- Enrichment log — per-company outcome tracking (admin diagnostic tool)
-- Records what happened to every company the engine processed.
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS enrichment_log (
    id              SERIAL          PRIMARY KEY,
    batch_id        VARCHAR(100)    NOT NULL,
    siren           VARCHAR(9)      NOT NULL,
    denomination    TEXT,
    outcome         VARCHAR(20)     NOT NULL,       -- qualified, replaced, failed
    maps_method     VARCHAR(30),                    -- direct_hit, click_result, no_result, geographic, false_positive
    maps_phone      TEXT,
    maps_website    TEXT,
    maps_name       TEXT,                            -- business name from Maps panel h1
    crawl_method    VARCHAR(30),                    -- curl, skipped, infra_error
    emails_found    INT             DEFAULT 0,
    replace_reason  VARCHAR(30),                    -- no_maps_data, low_confidence, null
    time_ms         INT,
    created_at      TIMESTAMPTZ     DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_enrichment_log_query ON enrichment_log(batch_id);

-- Add cancel_requested flag for graceful pipeline cancellation
ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS cancel_requested BOOLEAN DEFAULT FALSE;

-- ---------------------------------------------------------------------------
-- Users — authentication and role-based access
-- Roles: 'admin' (sees all data), 'user' (sees own batches only)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS users (
    id              SERIAL          PRIMARY KEY,
    username        VARCHAR(50)     NOT NULL UNIQUE,
    password_hash   TEXT            NOT NULL,
    role            VARCHAR(20)     NOT NULL DEFAULT 'user',  -- 'admin' | 'user'
    display_name    TEXT,
    created_at      TIMESTAMP       NOT NULL DEFAULT NOW(),
    last_login      TIMESTAMP
);

-- Add user_id to batch_data so we can filter "my batches" per user
ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS user_id INTEGER REFERENCES users(id);

-- Worker tracking: which machine ran this batch
ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS worker_id VARCHAR(50);

-- Enseigne: commercial/trade name from SIRENE StockEtablissement
-- This is the name on the business sign (e.g. "Camping La Marende")
-- vs the legal denomination ("SCI LA MARENDE").
ALTER TABLE companies ADD COLUMN IF NOT EXISTS enseigne TEXT;

-- ---------------------------------------------------------------------------
-- Smart Upload Engine — schema expansion
-- Allows ingestion of rich external data (KOMPASS, CRMs, etc.)
-- ---------------------------------------------------------------------------

-- Companies: revenue, headcount, foundation date, and overflow storage
ALTER TABLE companies ADD COLUMN IF NOT EXISTS chiffre_affaires    BIGINT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS annee_ca            SMALLINT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS tranche_ca          TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS effectif_exact      TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS date_fondation      DATE;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS type_etablissement  TEXT;
ALTER TABLE companies ADD COLUMN IF NOT EXISTS extra_data          JSONB DEFAULT '{}';

-- Officers: richer contact info (direct email, direct line, function codes)
ALTER TABLE officers ADD COLUMN IF NOT EXISTS civilite       TEXT;
ALTER TABLE officers ADD COLUMN IF NOT EXISTS email_direct   TEXT;
ALTER TABLE officers ADD COLUMN IF NOT EXISTS ligne_directe  TEXT;
ALTER TABLE officers ADD COLUMN IF NOT EXISTS code_fonction  TEXT;
ALTER TABLE officers ADD COLUMN IF NOT EXISTS type_fonction  TEXT;

-- Scrape jobs: upload mode tracking
ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS mode VARCHAR(20) DEFAULT 'discovery';

-- ---------------------------------------------------------------------------
-- Company notes — per-company text annotations (CRM step 1)
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS company_notes (
    id          SERIAL          PRIMARY KEY,
    siren       VARCHAR(9)      NOT NULL REFERENCES companies(siren) ON DELETE CASCADE,
    user_id     INTEGER         NOT NULL REFERENCES users(id),
    username    VARCHAR(50)     NOT NULL,
    text        TEXT            NOT NULL,
    created_at  TIMESTAMP       NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_company_notes_siren ON company_notes(siren);

-- ---------------------------------------------------------------------------
-- Activity log — admin audit trail of all user actions
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS activity_log (
    id          SERIAL          PRIMARY KEY,
    user_id     INTEGER         REFERENCES users(id),
    username    VARCHAR(100),
    action      VARCHAR(50)     NOT NULL,
    target_type VARCHAR(50),
    target_id   TEXT,
    details     TEXT,
    created_at  TIMESTAMP       DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_activity_log_time ON activity_log (created_at DESC);
CREATE INDEX IF NOT EXISTS idx_activity_log_user ON activity_log (user_id, created_at DESC);

-- ---------------------------------------------------------------------------
-- Social media columns — Instagram + TikTok expansion
-- ---------------------------------------------------------------------------
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS social_instagram TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS social_tiktok TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS social_whatsapp TEXT;
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS social_youtube TEXT;

-- ---------------------------------------------------------------------------
-- SIREN mismatch flag — enricher sets this when mentions-légales SIREN ≠ company SIREN
-- ---------------------------------------------------------------------------
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS siren_match BOOLEAN;

-- ---------------------------------------------------------------------------
-- Financial data — resultat_net from Recherche Entreprises API
-- ---------------------------------------------------------------------------
ALTER TABLE companies ADD COLUMN IF NOT EXISTS resultat_net BIGINT;
