-- Migration 003: Add address column to contacts table
-- Required for Google Maps data extraction (address, rating, review_count)
-- rating + review_count already exist from schema.sql; address is new.

ALTER TABLE contacts ADD COLUMN IF NOT EXISTS address TEXT;
