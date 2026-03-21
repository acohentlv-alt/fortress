-- ============================================================
--  Fortress: ADD CASCADE Foreign Keys Migration
--  Run with: psql $DATABASE_URL -f migrations/add_cascade_fks.sql
-- ============================================================

BEGIN;

-- ── Step 1: Drop existing non-cascade siren FKs ─────────────
ALTER TABLE batch_tags    DROP CONSTRAINT IF EXISTS query_tags_siren_fkey;
ALTER TABLE contacts      DROP CONSTRAINT IF EXISTS contacts_siren_fkey;
ALTER TABLE officers      DROP CONSTRAINT IF EXISTS officers_siren_fkey;
ALTER TABLE company_notes DROP CONSTRAINT IF EXISTS company_notes_siren_fkey;

-- ── Step 2: Recreate with ON DELETE CASCADE ─────────────────
ALTER TABLE batch_tags ADD CONSTRAINT batch_tags_siren_fkey
  FOREIGN KEY (siren) REFERENCES companies(siren) ON DELETE CASCADE;

ALTER TABLE contacts ADD CONSTRAINT contacts_siren_fkey
  FOREIGN KEY (siren) REFERENCES companies(siren) ON DELETE CASCADE;

ALTER TABLE officers ADD CONSTRAINT officers_siren_fkey
  FOREIGN KEY (siren) REFERENCES companies(siren) ON DELETE CASCADE;

ALTER TABLE company_notes ADD CONSTRAINT company_notes_siren_fkey
  FOREIGN KEY (siren) REFERENCES companies(siren) ON DELETE CASCADE;

-- ── Step 3: Add NEW FKs for tables that had none ────────────
-- Clean orphan rows first (safe — no real data loss)
DELETE FROM batch_log bl
WHERE NOT EXISTS (SELECT 1 FROM companies c WHERE c.siren = bl.siren);

DELETE FROM enrichment_log el
WHERE NOT EXISTS (SELECT 1 FROM companies c WHERE c.siren = el.siren);

ALTER TABLE batch_log ADD CONSTRAINT batch_log_siren_fkey
  FOREIGN KEY (siren) REFERENCES companies(siren) ON DELETE CASCADE;

ALTER TABLE enrichment_log ADD CONSTRAINT enrichment_log_siren_fkey
  FOREIGN KEY (siren) REFERENCES companies(siren) ON DELETE CASCADE;

-- ── Step 4: User FKs — SET NULL on delete ───────────────────
ALTER TABLE activity_log  DROP CONSTRAINT IF EXISTS activity_log_user_id_fkey;
ALTER TABLE activity_log ADD CONSTRAINT activity_log_user_id_fkey
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL;

ALTER TABLE batch_data DROP CONSTRAINT IF EXISTS scrape_jobs_user_id_fkey;
ALTER TABLE batch_data ADD CONSTRAINT batch_data_user_id_fkey
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL;

ALTER TABLE company_notes DROP CONSTRAINT IF EXISTS company_notes_user_id_fkey;
ALTER TABLE company_notes ADD CONSTRAINT company_notes_user_id_fkey
  FOREIGN KEY (user_id) REFERENCES users(id) ON DELETE SET NULL;

COMMIT;

-- ── Verification ────────────────────────────────────────────
SELECT conname, conrelid::regclass AS table_name, confrelid::regclass AS references,
       CASE confdeltype WHEN 'c' THEN 'CASCADE' WHEN 'n' THEN 'SET NULL' WHEN 'a' THEN 'NO ACTION' ELSE confdeltype::text END AS on_delete
FROM pg_constraint
WHERE contype = 'f' AND connamespace = 'public'::regnamespace
ORDER BY conrelid::regclass::text, conname;
