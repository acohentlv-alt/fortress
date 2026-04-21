-- ============================================================
-- FRANKENSTEIN CLEANUP — 2026-04-22
-- ============================================================
-- PURPOSE:
--   One-time repair of MAPS entities whose code_postal/ville was overwritten
--   by the SIRENE siège location instead of preserving the Google Maps storefront
--   location. This happened because _copy_sirene_reference_data() used plain
--   assignment (= %s) for code_postal and ville rather than COALESCE.
--   The bug is fixed in the application code (Frankenstein fix, Apr 22).
--   This script repairs historical rows already damaged in the database.
--
-- SCOPE:
--   - Only MAPS rows (siren LIKE 'MAPS%') — real SIRENE rows are never touched
--   - Only confirmed links (link_confidence = 'confirmed') with a linked_siren
--   - Only rows where the DB code_postal differs from the Google Maps CP found
--     in the contacts table (source='google_maps')
--   - CROSS-WORKSPACE: touches ALL workspaces including Workspace 1 (Cindy).
--     This is intentional — Cindy's exports are the primary victim of this bug.
--
-- WHAT IT DOES NOT DO:
--   - Does NOT unlink any companies. Confirmed links are left intact.
--   - Does NOT fix franchise HQ-leak links — those require Phase 3D Phase 1c.
--   - Does NOT touch tranche_effectif, naf_code, forme_juridique or any other
--     legal fields (those were always correct — only cp/ville were wrong).
--
-- BACKUP RETENTION: 30 days (Alan's decision, 2026-04-22 session).
--
-- ROLLBACK:
--   To undo this migration, run the ROLLBACK SQL at the bottom of this file.
--   The backup table companies_cp_ville_backup_20260422 holds the original values.
--
-- HOW TO RUN:
--   1. Run STEP B (dry-run SELECT) and review the candidate list.
--   2. If the count and sample look correct, run STEP C (the UPDATE).
--   3. Run STEP D (verification) to confirm the repair.
--   4. The backup table will be dropped after 30 days.
-- ============================================================


-- ============================================================
-- STEP A — Create backup of rows that will be modified
-- ============================================================
-- Captures the current (damaged) code_postal/ville values before the repair.
-- Rollback uses this table to restore if needed.

CREATE TABLE IF NOT EXISTS companies_cp_ville_backup_20260422 (
    siren          TEXT PRIMARY KEY,
    code_postal    TEXT,
    ville          TEXT,
    backed_up_at   TIMESTAMPTZ DEFAULT NOW()
);

-- Insert backup rows for all MAPS entities that will be affected by STEP C.
-- This INSERT is idempotent (ON CONFLICT DO NOTHING) so re-running is safe.
INSERT INTO companies_cp_ville_backup_20260422 (siren, code_postal, ville)
SELECT
    c.siren,
    c.code_postal,
    c.ville
FROM companies c
JOIN contacts ct
    ON ct.siren = c.siren
    AND ct.source = 'google_maps'
WHERE
    c.siren LIKE 'MAPS%'
    AND c.link_confidence = 'confirmed'
    AND c.linked_siren IS NOT NULL
    AND ct.address IS NOT NULL
    AND (
        -- Extract the LAST 5-digit postal code from the Maps address
        -- (mirrors _parse_maps_address "last CP wins" strategy)
        substring(
            ct.address
            FROM '(?:.*\b(\d{5})\b)'
        ) IS NOT NULL
    )
    AND c.code_postal IS DISTINCT FROM
        substring(ct.address FROM '(?:.*\b(\d{5})\b)')
    AND (
        c.link_signals->>'address_match' IS DISTINCT FROM 'true'
        OR c.link_signals IS NULL
    )
ON CONFLICT (siren) DO NOTHING;


-- ============================================================
-- STEP B — Dry run: review candidates before applying UPDATE
-- ============================================================
-- Run this SELECT first to see what will change.
-- Check: does the count look reasonable? Are the sample rows correct?
-- The STEP C UPDATE only runs on these same rows.

SELECT
    c.siren                                                     AS maps_siren,
    c.denomination,
    c.linked_siren,
    c.link_method,
    c.code_postal                                               AS current_cp,
    c.ville                                                     AS current_ville,
    substring(ct.address FROM '(?:.*\b(\d{5})\b)')             AS maps_cp,
    -- Ville is text after the last CP in the Maps address, stripped of ', France'
    trim(
        leading ', ' FROM
        substring(
            regexp_replace(ct.address, ',\s*France\s*$', '', 'i')
            FROM '(?:.*\d{5})(.*)$'
        )
    )                                                           AS maps_ville_approx,
    ct.address                                                  AS maps_address_raw,
    c.link_signals->>'address_match'                            AS address_match_signal
FROM companies c
JOIN contacts ct
    ON ct.siren = c.siren
    AND ct.source = 'google_maps'
WHERE
    c.siren LIKE 'MAPS%'
    AND c.link_confidence = 'confirmed'
    AND c.linked_siren IS NOT NULL
    AND ct.address IS NOT NULL
    AND substring(ct.address FROM '(?:.*\b(\d{5})\b)') IS NOT NULL
    AND c.code_postal IS DISTINCT FROM
        substring(ct.address FROM '(?:.*\b(\d{5})\b)')
    AND (
        c.link_signals->>'address_match' IS DISTINCT FROM 'true'
        OR c.link_signals IS NULL
    )
ORDER BY c.siren;


-- ============================================================
-- STEP C — Apply the repair (run only after reviewing STEP B)
-- ============================================================
-- Safety gates (all must be true for a row to be updated):
--   1. siren LIKE 'MAPS%'                  → never touch real SIRENE rows
--   2. link_confidence = 'confirmed'        → only confirmed links
--   3. linked_siren IS NOT NULL             → must have a linked SIREN
--   4. ct.source = 'google_maps'            → Maps contact must exist
--   5. code_postal IS DISTINCT FROM maps CP → skip rows already correct
--   6. address_match signal != 'true'       → skip rows where SIRENE confirmed
--      the Maps address as the official address (those were never wrong)

UPDATE companies c
SET
    code_postal = substring(ct.address FROM '(?:.*\b(\d{5})\b)'),
    ville = trim(
        leading ', ' FROM
        substring(
            regexp_replace(ct.address, ',\s*France\s*$', '', 'i')
            FROM '(?:.*\d{5})(.*)$'
        )
    ),
    updated_at = NOW()
FROM contacts ct
WHERE
    ct.siren = c.siren
    AND ct.source = 'google_maps'
    AND c.siren LIKE 'MAPS%'
    AND c.link_confidence = 'confirmed'
    AND c.linked_siren IS NOT NULL
    AND ct.address IS NOT NULL
    AND substring(ct.address FROM '(?:.*\b(\d{5})\b)') IS NOT NULL
    AND c.code_postal IS DISTINCT FROM
        substring(ct.address FROM '(?:.*\b(\d{5})\b)')
    AND (
        c.link_signals->>'address_match' IS DISTINCT FROM 'true'
        OR c.link_signals IS NULL
    );


-- ============================================================
-- STEP D — Verification queries (run after STEP C)
-- ============================================================

-- D1: How many rows were updated? (should match STEP B count)
SELECT COUNT(*) AS rows_in_backup
FROM companies_cp_ville_backup_20260422;

-- D2: Spot-check 10 repaired rows — compare backup vs current
SELECT
    bk.siren,
    bk.code_postal   AS old_cp,
    bk.ville         AS old_ville,
    c.code_postal    AS new_cp,
    c.ville          AS new_ville
FROM companies_cp_ville_backup_20260422 bk
JOIN companies c ON c.siren = bk.siren
WHERE bk.code_postal IS DISTINCT FROM c.code_postal
ORDER BY bk.siren
LIMIT 10;

-- D3: Confirm no more Frankenstein candidates remain
SELECT COUNT(*) AS remaining_frankenstein_candidates
FROM companies c
JOIN contacts ct
    ON ct.siren = c.siren
    AND ct.source = 'google_maps'
WHERE
    c.siren LIKE 'MAPS%'
    AND c.link_confidence = 'confirmed'
    AND c.linked_siren IS NOT NULL
    AND ct.address IS NOT NULL
    AND substring(ct.address FROM '(?:.*\b(\d{5})\b)') IS NOT NULL
    AND c.code_postal IS DISTINCT FROM
        substring(ct.address FROM '(?:.*\b(\d{5})\b)')
    AND (
        c.link_signals->>'address_match' IS DISTINCT FROM 'true'
        OR c.link_signals IS NULL
    );
-- Expected result: 0


-- ============================================================
-- ROLLBACK SQL — commented out, run manually only if needed
-- ============================================================
-- To restore the original (damaged) code_postal/ville values:
--
-- UPDATE companies c
-- SET
--     code_postal = bk.code_postal,
--     ville       = bk.ville,
--     updated_at  = NOW()
-- FROM companies_cp_ville_backup_20260422 bk
-- WHERE c.siren = bk.siren;
--
-- After verifying the rollback, drop the backup table:
-- DROP TABLE companies_cp_ville_backup_20260422;
