-- Migration 002: Add filters_json column to scrape_jobs
-- Stores the JSON-serialised advanced filters from the UI so the runner
-- can respect them (size filters, date filters, etc.).
--
-- Safe to run multiple times (uses IF NOT EXISTS pattern).
-- Applied: 2026-03-01

ALTER TABLE scrape_jobs
    ADD COLUMN IF NOT EXISTS filters_json TEXT;

COMMENT ON COLUMN scrape_jobs.filters_json IS 'JSON-serialised advanced filters from new_query.py UI (tranche_effectif, date_creation, etc.)';
