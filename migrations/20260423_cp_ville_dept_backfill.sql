-- =====================================================
-- MIGRATION: 20260423_cp_ville_dept_backfill.sql
--
-- PURPOSE:
--   Backfill code_postal, ville, and departement on MAPS entities
--   (siren LIKE 'MAPS%') that already have an adresse containing a
--   5-digit CP but whose code_postal column is still NULL. These rows
--   were created by the pipeline before the live parser at
--   discovery.py:2190 started writing those columns.
--
-- SCOPE:
--   MAPS entities only — never touches real 9-digit SIREN rows.
--   COALESCE guards: existing non-NULL ville/departement are preserved.
--
-- ROLLOUT STAGES:
--   Stage A  — workspace 174 (test workspace) — ACTIVE (uncommented)
--   Stage B  — workspaces 1 + 417 (Cindy real client + ws417)
--              COMMENTED OUT — run only after Alan approves Stage A QA
--
-- ROWCOUNT TOLERANCE:
--   Stage A: 476–526 rows expected
--   Stage B: 771–852 rows expected
--
-- ROLLBACK STORY:
--   A backup table captures the pre-migration state of every affected row
--   across all three workspaces. Per-stage and full rollback SQL is at the
--   bottom of this file. The backup table is kept for 30 days, then dropped.
--
-- DO NOT TOUCH: migrations/20260422_frankenstein_cleanup.sql
--   That file has the same regex issue and is handled separately.
-- =====================================================


-- =====================================================
-- STEP 1: Backup table
--   Captures the BEFORE state of every row that will be updated
--   across all three target workspaces (174, 1, 417).
--   ON CONFLICT DO NOTHING means re-running this step is safe.
-- =====================================================

CREATE TABLE IF NOT EXISTS companies_cp_ville_dept_backup_20260423 (
    siren        TEXT PRIMARY KEY,
    workspace_id INTEGER,
    adresse      TEXT,
    code_postal  TEXT,
    ville        TEXT,
    departement  TEXT,
    backed_up_at TIMESTAMPTZ DEFAULT NOW()
);

INSERT INTO companies_cp_ville_dept_backup_20260423
    (siren, workspace_id, adresse, code_postal, ville, departement)
SELECT siren, workspace_id, adresse, code_postal, ville, departement
FROM companies
WHERE siren LIKE 'MAPS%'
  AND code_postal IS NULL
  AND adresse IS NOT NULL
  AND adresse ~ '\d{5}'
  AND workspace_id IN (174, 1, 417)
ON CONFLICT (siren) DO NOTHING;


-- =====================================================
-- STEP 2: Stage A UPDATE — workspace 174 only (ACTIVE)
--
-- Regex notes (verified on Neon — do not change):
--   CP:   substring(c.adresse FROM '.*(\d{5})')
--         Greedy .* takes the LAST 5-digit sequence.
--         Handles double-CP addresses ("63200 Chem..., 63200 Riom" → "63200").
--   Ville: strips leading/trailing spaces+commas and optional " France" suffix.
--   Dept:  mirrors Python _derive_departement (sirene_etab_ingest.py:117-141).
--         Corse (20xxx < 20200 → 2A, >= 20200 → 2B), DOM/TOM 97/98 → 3-digit,
--         everything else → first 2 digits.
-- =====================================================

UPDATE companies c
SET code_postal = substring(c.adresse FROM '.*(\d{5})'),
    ville       = COALESCE(c.ville,
                    NULLIF(TRIM(
                        regexp_replace(
                            substring(c.adresse FROM '(?:.*\d{5})(.*)$'),
                            '^[\s,]+|[\s,]+(France)?[\s,]*$',
                            '',
                            'g'
                        )
                    ), '')
                  ),
    departement = COALESCE(c.departement,
                    CASE
                        WHEN substring(c.adresse FROM '.*(\d{5})') IS NULL THEN NULL
                        WHEN LEFT(substring(c.adresse FROM '.*(\d{5})'), 2) = '20'
                             AND substring(c.adresse FROM '.*(\d{5})')::int < 20200 THEN '2A'
                        WHEN LEFT(substring(c.adresse FROM '.*(\d{5})'), 2) = '20'
                             AND substring(c.adresse FROM '.*(\d{5})')::int >= 20200 THEN '2B'
                        WHEN LEFT(substring(c.adresse FROM '.*(\d{5})'), 2) IN ('97','98')
                             THEN LEFT(substring(c.adresse FROM '.*(\d{5})'), 3)
                        ELSE LEFT(substring(c.adresse FROM '.*(\d{5})'), 2)
                    END
                  ),
    updated_at  = NOW()
WHERE c.siren LIKE 'MAPS%'
  AND c.code_postal IS NULL
  AND c.adresse IS NOT NULL
  AND c.adresse ~ '\d{5}'
  AND c.workspace_id = 174;


-- =====================================================
-- STEP 3: Stage B UPDATE — workspaces 1 + 417
--   Alan approved Stage A QA on 2026-04-23.
--   Stage B uncommented and executed by Executor Agent.
-- STAGE B EXECUTED 2026-04-23 BY EXECUTOR AGENT
-- =====================================================

UPDATE companies c
SET code_postal = substring(c.adresse FROM '.*(\d{5})'),
    ville       = COALESCE(c.ville,
                    NULLIF(TRIM(
                        regexp_replace(
                            substring(c.adresse FROM '(?:.*\d{5})(.*)$'),
                            '^[\s,]+|[\s,]+(France)?[\s,]*$',
                            '',
                            'g'
                        )
                    ), '')
                  ),
    departement = COALESCE(c.departement,
                    CASE
                        WHEN substring(c.adresse FROM '.*(\d{5})') IS NULL THEN NULL
                        WHEN LEFT(substring(c.adresse FROM '.*(\d{5})'), 2) = '20'
                             AND substring(c.adresse FROM '.*(\d{5})')::int < 20200 THEN '2A'
                        WHEN LEFT(substring(c.adresse FROM '.*(\d{5})'), 2) = '20'
                             AND substring(c.adresse FROM '.*(\d{5})')::int >= 20200 THEN '2B'
                        WHEN LEFT(substring(c.adresse FROM '.*(\d{5})'), 2) IN ('97','98')
                             THEN LEFT(substring(c.adresse FROM '.*(\d{5})'), 3)
                        ELSE LEFT(substring(c.adresse FROM '.*(\d{5})'), 2)
                    END
                  ),
    updated_at  = NOW()
WHERE c.siren LIKE 'MAPS%'
  AND c.code_postal IS NULL
  AND c.adresse IS NOT NULL
  AND c.adresse ~ '\d{5}'
  AND c.workspace_id IN (1, 417);


-- =====================================================
-- STEP 4: Verification queries
--   Run these inline after each stage to confirm correctness.
--
-- A3 (SHOW-STOPPER): any row in the backup that is still NULL in companies?
--   SELECT COUNT(*) AS still_null
--   FROM companies c
--   WHERE c.siren IN (
--       SELECT siren FROM companies_cp_ville_dept_backup_20260423
--       WHERE workspace_id = 174
--   )
--   AND c.code_postal IS NULL;
--   → MUST return 0. If > 0: run Stage A rollback immediately.
--
-- A4: any real SIREN (non-MAPS) in the backup?
--   SELECT COUNT(*) AS sirene_in_backup
--   FROM companies_cp_ville_dept_backup_20260423
--   WHERE siren NOT LIKE 'MAPS%';
--   → MUST return 0.
--
-- A5: any off-scope writes in the last 15 minutes?
--   SELECT COUNT(*) AS off_scope_writes
--   FROM companies c
--   WHERE c.updated_at > NOW() - INTERVAL '15 minutes'
--     AND c.siren LIKE 'MAPS%'
--     AND c.workspace_id NOT IN (174);
--   → MUST return 0 after Stage A.
--
-- A6: département correctness — any ws174 row that should now have a dept but still has NULL?
--   SELECT COUNT(*) AS wrong_dept
--   FROM companies c
--   JOIN companies_cp_ville_dept_backup_20260423 bk ON c.siren = bk.siren
--   WHERE bk.workspace_id = 174
--     AND bk.departement IS NULL
--     AND c.departement IS NULL;
--   → MUST return 0.
--
-- A8: double-CP edge case sample (greedy regex picks last CP):
--   SELECT siren, adresse, code_postal, ville, departement
--   FROM companies
--   WHERE siren IN (
--       SELECT siren FROM companies_cp_ville_dept_backup_20260423
--       WHERE workspace_id = 174
--   )
--   AND adresse ~ '\d{5}.*\d{5}'
--   LIMIT 5;
--   → code_postal should match the SECOND 5-digit sequence in adresse.
--
-- Random sample (5 rows):
--   SELECT siren, adresse, code_postal, ville, departement
--   FROM companies
--   WHERE siren IN (
--       SELECT siren FROM companies_cp_ville_dept_backup_20260423
--       WHERE workspace_id = 174
--   )
--   ORDER BY random() LIMIT 5;
-- =====================================================


-- =====================================================
-- ROLLBACK SQL
--
-- Run the relevant block if any verification fails.
-- All rollbacks restore from the backup table captured in Step 1.
-- =====================================================

-- -- Rollback Stage A only (ws174)
-- UPDATE companies c
-- SET code_postal = bk.code_postal,
--     ville       = bk.ville,
--     departement = bk.departement,
--     updated_at  = NOW()
-- FROM companies_cp_ville_dept_backup_20260423 bk
-- WHERE c.siren = bk.siren AND bk.workspace_id = 174;

-- -- Rollback Stage B only (ws1 + ws417)
-- UPDATE companies c
-- SET code_postal = bk.code_postal,
--     ville       = bk.ville,
--     departement = bk.departement,
--     updated_at  = NOW()
-- FROM companies_cp_ville_dept_backup_20260423 bk
-- WHERE c.siren = bk.siren AND bk.workspace_id IN (1, 417);

-- -- Full rollback (both stages)
-- UPDATE companies c
-- SET code_postal = bk.code_postal,
--     ville       = bk.ville,
--     departement = bk.departement,
--     updated_at  = NOW()
-- FROM companies_cp_ville_dept_backup_20260423 bk
-- WHERE c.siren = bk.siren;


-- =====================================================
-- DROP AFTER 30 DAYS (run on or after 2026-05-23):
--   DROP TABLE IF EXISTS companies_cp_ville_dept_backup_20260423;
-- =====================================================
