-- Migration 004: Add maps_url column to contacts table
-- Stores the Google Maps URL for the business listing
ALTER TABLE contacts ADD COLUMN IF NOT EXISTS maps_url TEXT;
