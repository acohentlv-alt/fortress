-- Phase 3 retroactive promotion — manual one-shot (committer runs)
-- Identifies past pending rows in workspaces sanctioned via
-- gemini_promote_workspace_ids that would be promoted under Phase 2's gate,
-- IF Phase 2 had been live during the original batch.
--
-- Requires Phase 2 to be deployed AND validated on at least 1 ws174 batch.
--
-- USAGE:
--   1. Update the WORKSPACE_IDS_TO_TOUCH placeholder below (NEVER include 1).
--   2. Run the SELECT block first — Alan eyeballs the count + 20-sample.
--   3. On Alan's GO, uncomment the UPDATE block and re-run.
--   4. Each promoted row gets a backdated audit row in batch_log with
--      action='auto_linked_gemini_promoted_retroactive' and a marker
--      'phase3_retroactive=true' in detail JSON.
--
-- ROLLBACK: same SQL as Phase 2 promote rollback below
-- (rescued_by='gemini_promoted' → restore from snapshot in link_signals).

-- ────────────────────────────────────────────────────────────────────
-- Step 1 — Set workspace allowlist (manual edit; NEVER include 1):
-- ────────────────────────────────────────────────────────────────────
\set ws_allow '{174}'

-- ────────────────────────────────────────────────────────────────────
-- Step 2 — Count of candidates per tier (read-only):
-- ────────────────────────────────────────────────────────────────────
WITH candidates AS (
    SELECT co.siren, co.linked_siren, co.link_method, co.link_signals,
           co.naf_status, co.naf_code, co.code_postal, co.workspace_id,
           bl.detail::jsonb AS shadow_detail
      FROM companies co
      JOIN batch_tags bt ON bt.siren = co.siren
      JOIN batch_data bd ON bd.batch_id = bt.batch_id
      JOIN batch_log bl ON bl.siren = co.siren AND bl.batch_id = bd.batch_id
     WHERE bd.workspace_id = ANY(:'ws_allow'::int[])
       AND bd.status = 'completed'
       AND bd.strict_naf = false
       AND bd.created_at::date >= CURRENT_DATE - INTERVAL '30 days'
       AND bl.action = 'gemini_shadow_yes'
       AND co.link_confidence = 'pending'
       AND (bl.detail::jsonb->>'picked_siren') IS NOT NULL
       AND (bl.detail::jsonb->>'confidence')::float >= 0.9
)
SELECT
    COUNT(*) AS total_candidates,
    COUNT(*) FILTER (WHERE shadow_detail->>'picked_siren' != COALESCE(linked_siren, '')) AS swap_candidates,
    COUNT(*) FILTER (WHERE shadow_detail->>'picked_siren' = COALESCE(linked_siren, '')) AS confirm_in_place_candidates
  FROM candidates;

-- ────────────────────────────────────────────────────────────────────
-- Step 3 — 20-row sample for Alan eyeball (read-only):
-- ────────────────────────────────────────────────────────────────────
SELECT bl.siren AS maps_id,
       co.denomination AS maps_name,
       co.linked_siren AS cascade_siren,
       co.link_method AS cascade_method,
       co.naf_status AS cascade_naf_status,
       co.naf_code AS cascade_naf,
       bl.detail::jsonb->>'picked_siren' AS gemini_siren,
       (bl.detail::jsonb->>'confidence')::float AS gemini_conf,
       LEFT(bl.detail::jsonb->>'reasoning', 200) AS gemini_reasoning,
       target.denomination AS gemini_picked_denom,
       target.naf_code AS gemini_picked_naf
  FROM batch_log bl
  JOIN batch_data bd ON bd.batch_id = bl.batch_id
  JOIN companies co ON co.siren = bl.siren
  LEFT JOIN companies target ON target.siren = (bl.detail::jsonb->>'picked_siren')
                              AND target.siren NOT LIKE 'MAPS%'
 WHERE bd.workspace_id = ANY(:'ws_allow'::int[])
   AND bd.strict_naf = false
   AND bd.created_at::date >= CURRENT_DATE - INTERVAL '30 days'
   AND bl.action = 'gemini_shadow_yes'
   AND co.link_confidence = 'pending'
   AND (bl.detail::jsonb->>'picked_siren') IS NOT NULL
   AND (bl.detail::jsonb->>'confidence')::float >= 0.9
 ORDER BY RANDOM()
 LIMIT 20;

-- ────────────────────────────────────────────────────────────────────
-- Step 4 — UPDATE block (commented out — uncomment after Alan's GO):
-- ────────────────────────────────────────────────────────────────────
-- COMMENTED OUT — manual uncomment after eyeball pass.
-- The Python script equivalent at fortress/scripts/phase3_retroactive_promote.py
-- (also created by executor) reuses the same tier-classification logic from
-- discovery.py to avoid duplicating the gate. Recommended over raw SQL.
