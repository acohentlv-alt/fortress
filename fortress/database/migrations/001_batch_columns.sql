-- Migration 001: Add batch_number and batch_offset columns to scrape_jobs
-- These columns were added via ALTER TABLE in production but were missing
-- from the original schema.sql DDL.
--
-- Safe to run multiple times (uses IF NOT EXISTS / DO NOTHING pattern).
-- Applied: 2026-03-01

ALTER TABLE scrape_jobs
    ADD COLUMN IF NOT EXISTS batch_number INT NOT NULL DEFAULT 1,
    ADD COLUMN IF NOT EXISTS batch_offset  INT NOT NULL DEFAULT 0;

COMMENT ON COLUMN scrape_jobs.batch_number IS '1-based batch index within a query (e.g. 1 for first 50, 2 for next 50)';
COMMENT ON COLUMN scrape_jobs.batch_offset IS 'SIRENE row offset for this batch (batch_number=2 → offset=50)';
