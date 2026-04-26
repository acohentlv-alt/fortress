# TOP PRIORITY 2 — Individual cat_jur 1000 matcher (agriculture-only)

**Plain-English summary for Alan:**

Cindy's matcher today filters SIRENE by NAF prefix + postal code. That works for companies (EARL, SCEA, GAEC) which carry a `denomination` like "EARL DOMAINE X" — pass 1 scores against `denomination` and matches them. But it misses the single biggest legal-entity family in France: **5.58 million `forme_juridique='1000'` individual entrepreneurs** — farmers, micro-entrepreneurs, sole traders. These rows live IN THE SAME NAF + CP pool as the companies, but pass 1's GREATEST(enseigne, denomination) similarity scoring fails on them because their `denomination` holds the legal name ("JEAN DUPONT") which has zero overlap with the Maps storefront name ("Domaine X"), and their `enseigne` (the trade name "Domaine X") gets washed out by the GREATEST in mixed pools.

**Mental model.** When the matcher hits an arboriculture Maps result like "Domaine X" at CP 51530, SIRENE has rows at NAF 01.24Z + CP 51530:
  - Some are EARL/SCEA/GAEC companies — `denomination='EARL DOMAINE X'` — pass 1 matches on denomination, hits Band A.
  - Many are individuals — `denomination='JEAN DUPONT'` (legal name), `enseigne='Domaine X'` (trade name) — pass 1's GREATEST scoring tops out below 0.90 because the company brand (not the legal name) is what matches the Maps storefront, and there's noise from other rows.

Pass 1 already FOUND these individual rows (they're in the same NAF + CP pool). Pass 1 just fails to rank them at Band A.

This brief adds a **second SQL pass** inside `_cp_name_disamb_match` that fires when **pass 1 returns rows but no candidate scores at Band A (0.90) — i.e., pass 1 fails to auto-confirm** — AND the picker is agriculture (NAF `01.*`). The fallback uses the SAME NAF prefix and CP from pass 1 but adds two filters: `forme_juridique='1000'` and `enseigne IS NOT NULL AND enseigne <> ''`. **Critically, pass 2 ranks against `enseigne` only** (not GREATEST with denomination) — the trade name is the load-bearing field for individual entrepreneurs. The threshold is **0.85** (vs existing Band A's 0.90) because individual-entrepreneur enseignes are typically declared verbatim on Maps storefronts ("Domaine de la Forêt"). Band B (low-similarity, narrow-pool) is intentionally skipped — at the dense CPs where individuals concentrate, Band B's pool guard would break.

Single file changes (`fortress/discovery.py`). New audit action `auto_linked_individual_match`, new method name `cp_name_disamb_indiv` added to backend `_STRONG_METHODS`. Frontend deliberately untouched — see "Frontend non-decision" below.

**Taxonomy category:** 2.B — Entrepreneurs individuels (code 1000), agricultural slice. The brief explicitly scopes to NAF `01.*` only; other 1000-heavy sectors (coiffeurs, bakers, libéraux) are deferred follow-ups, not in scope.

**Verified facts (live DB queries today, Apr 26 evening):**

- `companies.forme_juridique` is `TEXT` (schema.sql:15). Filter must use `IN ('1000')` (string), NEVER `= 1000` (integer). `sirene_ingest.py:215` populates it via `_coerce_str(row.get("categorieJuridiqueUniteLegale"))` — Parquet Int64 cast to string.
- Total active code-1000 SIREN rows (non-MAPS): **5,579,253**.
- In NAF 01.* (agriculture, all 01.x divisions): **576,368** rows.
- In NAF 01.* with non-NULL non-empty enseigne: **53,553** rows (~9.3% of agricultural code-1000 — much sparser than the global ~25% enseigne density, but enough to disambiguate at the CP level).
- Densest agricultural CP overall: 51530 (Marne) — 1,867 rows in 01.* code-1000, **drops to 53 with enseigne filter** (35× shrinkage). Even the worst case fits comfortably under `LIMIT 100`.
- `idx_companies_cp` is the primary access path. Tested SQL plan at CP 51530 with all new filters: 53 rows in 205ms — acceptable for a per-Maps-result fallback that only fires when pass 1 misses.
- `cp_name_disamb` is already in backend `_STRONG_METHODS` at `discovery.py:587`. Existing audit branch at `discovery.py:2939` (`elif method == "cp_name_disamb": audit_action = "auto_linked_cp_name_disamb"`). Prior exec ALREADY landed: the new constant at the constants block, `cp_name_disamb_indiv` in `_STRONG_METHODS` at line 588, and the audit branch at line 2941. Only the SQL/trigger inside `_cp_name_disamb_match` (Work Item 2) was structurally broken.
- `cp_name_disamb` is **NOT** in frontend `_STRONG_METHODS` at `frontend/js/pages/company.js:39`. This is a known existing parity gap; the brief intentionally **does not** fix it for the new method either, to keep behavior consistent across both `cp_name_disamb` variants. Any frontend tooltip parity work belongs in a separate brief.
- `_compute_naf_status` (discovery.py:69-101) — when matched NAF strict-prefixes the picker (e.g. `01.21Z` startswith `01.21Z`), returns `verified`. The new pass keeps the SAME NAF prefix from picker, so confirmed matches will land on `naf_status='verified'` and auto-confirm via the existing line 2707 branch. No Phase A signal counting needed; no new gate exemption needed.
- `SECTOR_EXPANSIONS` for keys starting `01.` is **empty** (config/naf_sector_expansion.py confirmed live). Cross-leaf agricultural matches (e.g. picker `01.24Z` matched to SIRENE `01.21Z`) would land on `naf_status='mismatch'`. **The brief explicitly does NOT broaden the NAF prefix in pass 2 — it stays strict, same as picker.** This means cross-leaf farmers (e.g. an arboriculture batch finding a viticulture-registered farmer) will simply not match under this lever; that's by design — broadening would create false positives at high-density rural CPs.

**What Alan sees after deploy:**

- Fresh agricultural batches (e.g. `arboriculture 51`, `arboriculture 47`) confirm a measurably higher percentage of entities under method `cp_name_disamb_indiv` — the lever now ACTUALLY fires on real batches (the prior exec's pass 2 was structurally unable to produce live matches; this revision corrects the trigger).
- New `batch_log.action = 'auto_linked_individual_match'` rows appear, distinct from `auto_linked_cp_name_disamb`. Cindy reporting can count this lever separately.
- Confirm rate on rural batches climbs by an estimated +3-5pp.
- Existing `cp_name_disamb` Band A behavior unchanged. Existing `cp_name_disamb` audit row count for non-agricultural sectors unchanged.
- No UI changes anywhere.

```
EXECUTOR BRIEF — START
================================================================================
TOP PRIORITY 2 — Individual cat_jur 1000 matcher (agriculture pass 2)
================================================================================

GOAL
  Extend _cp_name_disamb_match with a second SQL pass that fires when pass 1
  fails to identify a Band A candidate (i.e., pass 1's top similarity is below
  0.90 — including the empty-rows case) AND the picker is agriculture (NAF
  `01.*`). The fallback keeps the picker's NAF prefix and CP, adds
  `forme_juridique='1000' AND enseigne IS NOT NULL AND enseigne <> ''`, ranks
  candidates by `similarity(enseigne, maps_name)` ONLY (not GREATEST with
  denomination), uses threshold 0.85, and emits method `cp_name_disamb_indiv`
  so the auto-confirm gate routes it to a new audit action
  `auto_linked_individual_match`.

  Single file: fortress/discovery.py. No frontend, no schema, no migrations.

CROSS-LEVER DEPENDENCIES
  - Step 2.5 (`_cp_name_disamb_match`) is the host. Built April with explicit
    extensibility per its own docstring (line 737): "Designed as reusable
    primitive for future Priority 5 (taxonomy line 1026-1034)."
  - The new pass MUST NOT change pass 1's positive-Band-A behavior — pass 1
    is shipped and measured. Pass 2 only fires when pass 1 would otherwise
    return None (no rows OR rows present but top_sim < Band A threshold).
    Be surgical: pass 1's full body (band detection, return shaping for Band A
    and Band B) MUST stay byte-identical when top_sim ≥ 0.90.

WHY PASS 2 CAN'T FIRE ON `if not rows:` ALONE (BUG FROM PRIOR EXEC)
  A previous attempt fired pass 2 only when pass 1's cursor returned `[]`.
  This is structurally unable to produce live matches: pass 1 returns `[]`
  ONLY when no SIRENE rows exist at NAF prefix + CP at all. Pass 2 then
  queries the SAME NAF + same CP + ADDITIONAL filters
  (`forme_juridique='1000' AND enseigne IS NOT NULL`). A more-restrictive
  subset of an empty set is still empty — so pass 2 always returned None on
  real data. Unit tests passed only because mocked AsyncMock cursors could
  return different rows for pass 1 vs pass 2, which the real DB cannot do.

  CORRECT TRIGGER: pass 2 fires when pass 1 returned rows AND the top sim
  was below Band A (0.90). The individual-1000 candidates ARE in pass 1's
  result set; pass 1 just ranks them poorly because GREATEST(enseigne,
  denomination) gets washed out by EARL/SCEA `denomination` rows in the
  same pool. Pass 2 re-scores the same pool subset (filtered to
  forme_juridique='1000' + non-empty enseigne) using `similarity(enseigne,
  maps_name)` ONLY — that's the load-bearing field for individuals.
  The empty-pass-1 case is also handled (still drops into pass 2) but is
  not the primary path.

DESIGN CONSTANTS (all Alan-approved)
  Decision 1 — Use `forme_juridique` (TEXT). The TASKS.md "cat_jur_code"
               naming was wrong. Real column at schema.sql:15. Filter:
               `forme_juridique = '1000'` (string, NOT integer 1000).
  Decision 2 — Agriculture-only first. Trigger pass 2 only when EVERY entry
               in `picked_nafs` (or every entry in `naf_division_whitelist`
               when populated) starts with the literal prefix `'01.'`. Other
               sectors (coiffeurs, bakers, libéraux) are explicit follow-ups,
               not in this brief.
  Decision 3 — Same NAF prefix and same CP as pass 1. Pass 2 keeps the
               picker prefixes; it does NOT broaden to `01.%`. The pools
               OVERLAP — pass 2's rows are a strict subset of pass 1's
               (same NAF + CP + statut + non-MAPS, plus extra filters
               `forme_juridique='1000'` and non-empty enseigne). The
               difference between passes is the SCORING FIELD (enseigne
               only vs GREATEST(enseigne, denomination)) and threshold
               (0.85 vs Band A's 0.90). Strict prefix guarantees
               `naf_status='verified'` on hit, so the existing auto-confirm
               gate at line 2707 fires without any new exemption.
  Decision 4 — `enseigne IS NOT NULL AND enseigne <> ''` enforced in the
               SQL WHERE clause (NOT post-LIMIT). Drops empty-enseigne rows
               before ORDER BY similarity LIMIT 100, so sampling is
               deterministic. Verified live: at the densest agricultural CP
               (51530), this filter shrinks the candidate pool from 1,867 to
               53 — well under LIMIT 100. Pass 2 SCORES against `enseigne`
               only (`similarity(enseigne, maps_name)`), NOT against
               `GREATEST(enseigne, denomination)`. For code-1000 individuals
               the legal `denomination` is the person's name (e.g. "JEAN
               DUPONT") and has zero overlap with the Maps storefront — the
               trade name `enseigne` is the only useful field.
  Decision 5 — Band A only at threshold 0.85. No Band B for individuals
               (Band B's pool_size ≤ 5 guard breaks at high-density CPs).
               Lowered from 0.90 because individual enseignes tend to match
               Maps storefront names verbatim more than companies do, AND
               because pass 2 is already heavily gated (agriculture-only,
               code-1000-only, non-empty-enseigne-only, enseigne-scored-only).
  Decision 6 — New method name `cp_name_disamb_indiv`. New audit action
               `auto_linked_individual_match`. Both must be additions, not
               reuses — Cindy reporting needs separate counts.

================================================================================
WORK ITEMS
================================================================================

1) VERIFY CONSTANT — fortress/discovery.py
   ----------------------------------------------------------------------
   The prior exec already landed the constant. Verify it exists and is
   unchanged:

       _CP_NAME_DISAMB_INDIV_BAND_A_SIM = 0.85

   Should be located just below the existing CP-name-disamb constants
   block (around line 67-70). If for some reason it's missing, add it
   immediately below `_BAND_B_AUTO_CONFIRM_ENABLED: bool = False`. No
   action required if already present.

2) EXTEND HELPER — fortress/discovery.py:_cp_name_disamb_match (line 716)
   ----------------------------------------------------------------------
   The existing helper signature must NOT change. The new behavior lives
   inside the function body, after pass 1's similarity scoring computes
   `top_sim`, but BEFORE pass 1's band detection. The trigger condition
   change is the load-bearing fix relative to the prior failed exec.

   The current function (per discovery.py:769-868 read live Apr 26) does:
     1. Fetch rows from the SIRENE pool at NAF prefix + CP.
     2. `if not rows: return None`  ← prior exec wrongly hooked pass 2 here
     3. Compute `top_sim`, `second_sim`, detect band A or B.
     4. Return result for Band A or B; return None when band is None.

   We need pass 2 to fire whenever pass 1 would NOT auto-confirm at Band A,
   which is: rows empty (no candidates at all) OR top_sim < 0.90 (rows
   exist but pass 1 ranks them poorly). Both cases drop into pass 2; pass
   2 then checks the agriculture gate and re-scores against `enseigne`.

   STRUCTURAL CHANGE — replace the existing flow as follows. The diff
   touches lines 769-844 (the existing pass-2 attempt that the prior exec
   wrote, plus the immediate pass-1 fall-through).

   Find this block (current discovery.py:769-844):

       rows = await cur.fetchall()
       if not rows:
           # ── Pass 2: Individual cat_jur 1000 fallback (agriculture only) ──
           # Fires only when:
           #   - Pass 1 returned no rows at all (NAF-strict-prefix exhausted)
           #   - Every NAF prefix in the active filter starts with '01.'
           …  (existing 1000-matcher pass 2 block, ~75 lines through line 844)

   Replace it with the corrected structure below. Note the key changes:
     (a) Pass 2 trigger is no longer `if not rows:`. Instead, pass 1 first
         computes top_sim from any rows it found, then pass 2 fires when
         the empty-rows case OR top_sim < BAND A threshold.
     (b) Pass 2's SQL ranks by `similarity(COALESCE(enseigne,''), %s)` only,
         NOT GREATEST. This is the load-bearing scoring fix.
     (c) Pass 1's existing Band A / Band B logic is preserved and only
         runs when top_sim ≥ BAND A (i.e., pass 2 didn't fire). When pass 1
         finds Band B (≥0.55 with pool/dominance guards) but no Band A,
         we still drop into pass 2 — Band B is force-pending anyway under
         `_BAND_B_AUTO_CONFIRM_ENABLED=False`, and pass 2 may produce a
         confirmable individual match.

   EXACT REPLACEMENT (the entire region from `rows = await cur.fetchall()`
   down through the existing return dict):

       rows = await cur.fetchall()

       # Pass 1 scoring (always computed, used to decide if pass 2 fires).
       pool_size = len(rows)
       top_sim = float(rows[0][18] or 0.0) if rows else 0.0
       second_sim = float(rows[1][18] or 0.0) if pool_size >= 2 else 0.0

       # Detect Band A immediately so the pass-2 trigger has a single source
       # of truth: "did pass 1 land an auto-confirmable Band A candidate?"
       pass1_has_band_a = pool_size > 0 and top_sim >= _CP_NAME_DISAMB_BAND_A_SIM

       # ── Pass 2: Individual cat_jur 1000 fallback (agriculture only) ──
       # Fires whenever pass 1 fails to identify a Band A candidate AND the
       # picker is agriculture (every active NAF prefix starts with '01.').
       # Includes the empty-rows case (pass 1 found nothing in NAF+CP) and
       # the more common case (pass 1 found rows but the GREATEST(enseigne,
       # denomination) scoring topped out below 0.90 because EARL/SCEA
       # `denomination` rows in the same pool washed out the individuals'
       # `enseigne` similarity).
       if not pass1_has_band_a and all(p.startswith("01.") for p in naf_prefixes):
           cur2 = await conn.execute(
               f"""SELECT siren, siret_siege, denomination, enseigne, naf_code, naf_libelle,
                          forme_juridique, adresse, code_postal, ville, departement,
                          region, statut, date_creation, tranche_effectif,
                          latitude, longitude, fortress_id,
                          similarity(COALESCE(enseigne,''), %s) AS sim_enseigne
                   FROM companies
                   WHERE code_postal = %s
                     AND statut = 'A'
                     AND siren NOT LIKE 'MAPS%%'
                     AND ({naf_like_clauses})
                     AND forme_juridique = '1000'
                     AND enseigne IS NOT NULL
                     AND enseigne <> ''
                   ORDER BY sim_enseigne DESC
                   LIMIT 100""",  # noqa: S608
               [maps_name, maps_cp] + naf_params,
           )
           rows2 = await cur2.fetchall()
           if rows2:
               top2 = rows2[0]
               top2_sim = float(top2[18] or 0.0)
               if top2_sim >= _CP_NAME_DISAMB_INDIV_BAND_A_SIM:
                   log.info(
                       "discovery.cp_name_disamb_indiv_match",
                       maps_name=maps_name, maps_cp=maps_cp,
                       band="A_indiv",
                       siren=top2[0],
                       denomination=top2[2], enseigne=top2[3],
                       matched_naf=top2[4],
                       top_sim_enseigne=round(top2_sim, 3),
                       pool_size=len(rows2),
                       pass1_pool_size=pool_size,
                       pass1_top_sim=round(top_sim, 3),
                       naf_strict_prefix_matched=True,
                       candidates_considered=len(rows2),
                   )
                   return {
                       "siren": top2[0],
                       "denomination": top2[2] or "",
                       "enseigne": top2[3] or "",
                       "score": round(top2_sim, 2),
                       "method": "cp_name_disamb_indiv",
                       "adresse": top2[7] or "",
                       "ville": top2[9] or "",
                       "cp_name_disamb_meta": {
                           "top_sim_enseigne": round(top2_sim, 3),
                           "pool_size": len(rows2),
                           "candidates_considered": len(rows2),
                           "naf_strict_prefix_matched": True,
                           "forme_juridique_filter": "1000",
                           "pass": 2,
                           "pass1_pool_size": pool_size,
                           "pass1_top_sim": round(top_sim, 3),
                       },
                   }
               else:
                   log.info(
                       "discovery.cp_name_disamb_indiv_no_band",
                       maps_name=maps_name, maps_cp=maps_cp,
                       top_sim_enseigne=round(top2_sim, 3),
                       pool_size=len(rows2),
                       pass1_pool_size=pool_size,
                       pass1_top_sim=round(top_sim, 3),
                   )
                   # fall through to pass 1 band detection below
           # if rows2 is empty, also fall through to pass 1 band detection

       # ── Pass 1 band detection (unchanged from pre-Apr-26) ──
       if not rows:
           return None

       top_row = rows[0]

       band: str | None = None
       if top_sim >= _CP_NAME_DISAMB_BAND_A_SIM:
           band = "A"
       elif (
           top_sim >= _CP_NAME_DISAMB_BAND_B_SIM
           and pool_size <= _CP_NAME_DISAMB_BAND_B_POOL_MAX
           and (top_sim - second_sim) >= _CP_NAME_DISAMB_BAND_B_DOMINANCE
       ):
           band = "B"

       …  # rest of the existing pass-1 body (no_band log, return dict) unchanged

   Notes:
     - `naf_like_clauses` and `naf_params` (computed at discovery.py:747-748)
       are reused verbatim by pass 2.
     - The agriculture gate uses `naf_prefixes` (the variable already
       computed at line 743), NOT the original `picked_nafs` — this way
       the gate honors `naf_division_whitelist` correctly when a single
       section letter was picked.
     - Pass 2's SQL ranks by `similarity(COALESCE(enseigne,''), %s)` only.
       The two-`%s` parameter list `[maps_name, maps_name, ...]` from the
       prior failed exec must collapse to ONE `[maps_name, ...]` — only one
       similarity expression remains.
     - Do NOT include `cp_name_disamb_band` in the pass-2 return dict.
       That key is consumed at discovery.py:2684 for Band B forced-pending
       logic — pass 2 has no Band B, so omitting the key keeps it out of
       the Band B path entirely.
     - The return dict's `method` field MUST be the literal string
       `"cp_name_disamb_indiv"` (with the trailing `_indiv`). The auto-
       confirm gate dispatches on this exact value.
     - `cp_name_disamb_meta` includes `pass1_pool_size` and `pass1_top_sim`
       so observability logs / future debugging can trace whether pass 2
       fired because pass 1 was empty vs because pass 1 had rows but
       scored below Band A. This is structured logging only — no external
       contract depends on these keys.
     - The fall-through-to-pass-1 path (when pass 2 returns no usable
       candidate) preserves Band B behavior for the rare case where pass 1
       had Band B-eligible rows and pass 2 found none. Band B is forced to
       pending anyway, so this is not a confirm-rate concern; it just
       preserves the existing audit trail.

3) VERIFY METHOD IN STRONG SET — fortress/discovery.py:_STRONG_METHODS (line 581)
   ----------------------------------------------------------------------
   The prior exec already added the method. Verify the block at line 581
   contains:

       "cp_name_disamb_indiv",  # TOP 2 — code 1000 agriculture pass 2 (Apr 26)

   Should be the last entry inside the `_STRONG_METHODS = frozenset({...})`
   declaration. If missing, add it after `cp_name_disamb`. No action
   required if already present.

4) VERIFY AUDIT BRANCH — fortress/discovery.py (around line 2941)
   ----------------------------------------------------------------------
   The prior exec already added the audit branch at line 2941:

       elif method == "cp_name_disamb_indiv":
           audit_action = "auto_linked_individual_match"

   Verify it sits between the `elif method == "cp_name_disamb":` branch
   and the `elif picked_nafs == []:` branch. Order is logically irrelevant
   (the dispatch is mutually exclusive on `method` string), but adjacency
   keeps `git blame` readable. No action required if already present.

5) (DO NOT) UPDATE FRONTEND — fortress/frontend/js/pages/company.js
   ----------------------------------------------------------------------
   The frontend `_STRONG_METHODS` Set at line 39 currently does NOT include
   `cp_name_disamb`. This is an existing parity gap that predates this brief.
   Adding `cp_name_disamb_indiv` to the frontend without also adding the
   pre-existing `cp_name_disamb` would create asymmetric behavior: the new
   variant would render the rich context-aware tooltip while the older
   variant stays on the terse fallback.

   For this brief, **do nothing** to the frontend. Both `cp_name_disamb` and
   `cp_name_disamb_indiv` will fall through to the terse "company.nafMismatchTerse"
   tooltip when (rarely) `naf_status='mismatch'`. The hot path is
   `naf_status='verified'` (since pass 2 keeps the strict NAF prefix), in
   which case the frontend renders the green "verified" badge and never even
   inspects `link_method`. Frontend parity (adding both methods to the
   frontend Set) is a tracked follow-up but explicitly out of scope.

6) UNIT TESTS — fortress/tests/test_discovery_individual_matcher.py (REPLACE)
   ----------------------------------------------------------------------
   The prior executor created this file with 8 tests that drove pass 2 via
   the `if not rows:` trigger. THOSE TESTS ARE STRUCTURALLY WRONG (they
   prove a broken trigger works). DELETE the existing file and rewrite it
   with the test cases below.

   Test cases (use AsyncMock to drive cursor results — note the change in
   how pass 1 vs pass 2 is distinguished: pass 2 fires when pass 1 has
   rows but `top_sim < 0.90`, OR pass 1 empty):

   T1) test_indiv_threshold_exactly_085_accepts
       — Pass 1 returns rows with top_sim=0.20 (below Band A — would
         normally return no_band). Pass 2 fires. Pass 2's first cursor
         row has sim_enseigne=0.85 exactly. Helper must return candidate
         with method="cp_name_disamb_indiv".

   T2) test_indiv_threshold_0849_rejects
       — Pass 1 same as T1 (top_sim=0.20). Pass 2 fires. Pass 2's first
         row has sim_enseigne=0.849 → pass 2 falls through. Pass 1 then
         goes to no_band path → helper returns None. Logs
         `cp_name_disamb_indiv_no_band` AND
         `cp_name_disamb_no_band` (or just one — confirm in fixture).

   T3) test_pass2_skipped_when_pass1_band_a
       — Pass 1 returns rows with top_sim=0.95 (Band A hit). Pass 2 SQL
         must NOT fire (assert conn.execute called exactly ONCE — for
         pass 1). Helper returns method="cp_name_disamb" with the Band A
         candidate. THIS IS THE LOAD-BEARING REGRESSION GUARD: pass 1's
         positive-Band-A path must never invoke pass 2.

   T4) test_pass2_fires_when_pass1_below_band_a
       — Pass 1 returns rows with top_sim=0.40 (below 0.90, above 0.55
         pool guard NOT met — i.e., not even Band B). picked_nafs=['01.24Z'].
         Pass 2 fires (assert conn.execute called exactly TWICE). Pass 2's
         first row has sim_enseigne=0.92 → helper returns
         method="cp_name_disamb_indiv". This is the central test
         demonstrating the bug fix.

   T5) test_pass2_skipped_when_picker_not_agriculture
       — picked_nafs=['56.10A'] (restauration). Pass 1 returns rows with
         top_sim=0.30 (below Band A). Pass 2 must NOT fire — agriculture
         gate rejects. Helper returns None via pass 1's no_band path.

   T6) test_pass2_skipped_when_division_whitelist_nonagri
       — picked_nafs=['I'] (section letter), naf_division_whitelist=
         ['56.10', '56.21']. Pass 2 must NOT fire (whitelist entries
         don't all start with '01.'). Helper returns None.

   T7) test_pass2_handles_empty_pass1
       — Pass 1 returns []. picked_nafs=['01.24Z']. Pass 2 fires (gate
         passes; empty pass 1 is included in the trigger). Pass 2's
         first row has sim_enseigne=0.91 → helper returns
         method="cp_name_disamb_indiv". Demonstrates the empty-pass-1
         case still works (was the prior exec's only test path; we keep
         it as a regression test since the new trigger covers it too).

   T8) test_pass2_returns_none_when_all_enseignes_null
       — Pass 1 returns rows with top_sim=0.30. Pass 2 fires but its
         cursor returns [] (no code-1000 rows with non-empty enseigne
         exist at this CP+NAF). Helper falls through to pass 1's no_band
         path → returns None. THIS IS THE EDGE CASE the prior brief
         missed: pass 2 might find no candidates and pass 1 still has
         non-Band-A rows; we must not raise, and we must not return a
         partial pass-2 dict.

   T9) test_pass2_meta_includes_pass_marker_and_pass1_diagnostics
       — When pass 2 confirms (T1, T4, T7), returned dict has
         `cp_name_disamb_meta.pass == 2`, `forme_juridique_filter == "1000"`,
         AND `pass1_pool_size` (int ≥ 0) AND `pass1_top_sim` (float).
         Verifies observability metadata for downstream debugging.

   T10) test_pass2_does_not_emit_band_b_marker
       — When pass 2 confirms, returned dict must NOT contain key
         `cp_name_disamb_band`. (If present, the auto-confirm gate at
         line 2684 would treat it as Band B and force pending — guard
         against future drift.)

   T11) test_pass2_scores_by_enseigne_only_not_greatest
       — This test inspects the SQL that pass 2 issues. Capture the
         conn.execute call args (mock side_effect), assert the SQL
         string contains `similarity(COALESCE(enseigne,'')` exactly once
         and does NOT contain `GREATEST(`. Also assert the parameter
         list passed to pass 2 has length `1 + 1 + len(naf_prefixes)`
         (one `maps_name` for the similarity, one `maps_cp`, and one
         param per NAF prefix) — NOT `2 + 1 + len(naf_prefixes)` (which
         would indicate the GREATEST scoring was kept by mistake).

   T12) test_leak_prevention_pass2_never_fires_outside_agriculture
       — Parametrize: picked_nafs in [['56.10A'], ['I'], ['10.71B'],
         ['86.10Z'], ['56.30Z']]. Pass 1 returns rows with top_sim=0.20
         in every case. Assert pass 2 SQL never executes (conn.execute
         called exactly once per parametrization). Belt-and-braces
         regression guard for the agriculture gate.

   Use `pytest-asyncio` (already a dev dep — see existing async tests in
   test_contacts.py). Mock `conn.execute(...)` with `unittest.mock.AsyncMock`
   set up so the FIRST call returns a cursor whose `fetchall()` yields the
   pass-1 rows, and the SECOND call returns a cursor whose `fetchall()`
   yields the pass-2 rows. Use `side_effect=[...]` to drive cursor results
   sequentially across the two calls.

   The 12 tests collectively cover: trigger correctness (T3, T4, T7), gate
   correctness (T5, T6, T12), threshold correctness (T1, T2), edge cases
   (T8), and code-shape correctness (T9, T10, T11). The total count is
   12, NOT 8 — replacing the prior 8 tests + 4 new structural tests.

7) (DO NOT) ALTER picked_nafs PASSING — fortress/discovery.py:2076
   ----------------------------------------------------------------------
   No changes to the `_match_to_sirene(...)` call site at discovery.py:2076.
   `picked_nafs` and `naf_division_whitelist` are already plumbed through.
   The new behavior is wholly contained inside `_cp_name_disamb_match`.

8) REPLACE the prior failed pass-2 attempt — REQUIRED
   ----------------------------------------------------------------------
   The prior executor landed a broken pass-2 attempt at discovery.py:771-844
   (the `if not rows:` triggered block). That entire block must be DELETED
   and replaced by the corrected structure in Work Item 2. Specifically:
     - DELETE lines 771-844 (the existing `if not rows: ... pass 2 ... return ...`
       block, including its return dict).
     - REPLACE with the new structure where pass 2 fires on `not pass1_has_band_a`
       AND scores against `enseigne` only.
     - Pass 1's positive-Band-A behavior at lines 846-898 (the existing band
       detection + return) must remain BYTE-IDENTICAL when top_sim ≥ 0.90.

   Confirm via git diff that:
     - The function signature and pre-fetch SQL (lines 716-768) are
       byte-identical pre/post.
     - The new pass-2 code path scores by `similarity(COALESCE(enseigne,''), %s)`
       — a single `similarity()` call, NOT `GREATEST(similarity, similarity)`.
     - The new pass-2 trigger is `if not pass1_has_band_a and all(p.startswith("01.") ...)`,
       NOT `if not rows:`.
     - Pass 1's Band A return path produces an unchanged dict shape (same
       keys, same `method='cp_name_disamb'`).

================================================================================
ACCEPTANCE
================================================================================
  [1] `_cp_name_disamb_match` returns dict with method="cp_name_disamb_indiv"
      ONLY when:
        a. pass 1 did NOT auto-confirm at Band A (i.e., pass 1 returned no
           rows OR pass 1's top_sim < 0.90), AND
        b. every entry in naf_prefixes starts with '01.', AND
        c. ≥1 candidate exists with forme_juridique='1000' AND non-empty
           enseigne at the same CP and within the picker's NAF prefix, AND
        d. top `similarity(enseigne, maps_name)` (NOT GREATEST with
           denomination) ≥ 0.85.
  [2] `_STRONG_METHODS` contains 11 entries (was 10), including the new
      "cp_name_disamb_indiv".
  [3] `batch_log.action = 'auto_linked_individual_match'` rows produced by
      a fresh ws174 arboriculture batch (proof: at least 1 row after QA
      Section 1, on a Marne dept-51 arboriculture batch where pass 1 has
      historically returned zero confirms).
  [4] No regression: existing `auto_linked_cp_name_disamb` audit rows still
      produced for non-agricultural Step-2.5 hits (e.g. EHPAD batches), AND
      pass 1's positive-Band-A behavior remains byte-identical (verified
      by test T3).
  [5] Pass 2 SQL plan uses `idx_companies_cp` (verified live: 53 rows in
      ~205ms at densest agri CP 51530). Pass 2's `similarity(enseigne, ...)`
      ranking benefits from the same index.
  [6] No new lines deleted or modified in:
        - frontend/js/pages/company.js (frontend parity intentionally deferred)
        - database/schema.sql (no migration)
        - api/main.py (no startup migration block touched)
        - any pipeline file other than discovery.py
  [7] Unit test file rewritten with 12 tests (was 8 broken tests from prior
      exec). All 12 pass. Pre-existing tests in other files unchanged.
  [8] No bare `try/except: pass` introduced anywhere in the diff. If you
      find yourself silencing log/audit failures, use `log.debug(...)`
      per memory feedback_silent_audit_swallow.md.
  [9] No commit, no push. QA must pass first (separate session).
  [10] Pass 2's SQL contains exactly ONE `similarity()` call (against
       `enseigne`), NOT `GREATEST(similarity, similarity)`. The Python
       parameter list passed to pass 2's cursor is
       `[maps_name, maps_cp, *naf_params]` — single maps_name binding.
  [11] Pass 2 trigger is `if not pass1_has_band_a and all(p.startswith("01.") ...)`,
       NOT `if not rows:`. The empty-pass-1 case is included in the new
       trigger (because pass1_has_band_a is False when rows is empty).

EXECUTOR BRIEF — END
```

```
QA TEST PLAN — START
================================================================================
TOP 2 QA — Individual cat_jur 1000 matcher (LOCAL ONLY, no Render dependency)
================================================================================

SETUP (run once at start of QA session)
  1. cd "/Users/alancohen/Project Alan copy/fortress"
  2. Confirm DATABASE_URL set:
        echo "$DATABASE_URL" | head -c 30
     Should print neon connection prefix (postgresql://neondb_owner:…).
  3. Start uvicorn dev server in a background terminal:
        python3 -m uvicorn fortress.api.main:app --port 8080 --reload
     Wait for "🏰 Fortress API started — database connected".

  All commands below run from `cd fortress/` (the inner package dir).
  Workspace 174 ONLY for QA. NEVER touch ws1.

  Reference workspace credentials:
    head.test  / Test1234   (head of workspace 174)

================================================================================
SECTION 1 — AUTOMATED CHECKS (QA agent runs these in terminal)
================================================================================

CHECK 1.1 — Unit test suite passes (the 12 new/rewritten tests + full regression)
  Terminal:
      cd "/Users/alancohen/Project Alan copy/fortress"
      python -m pytest fortress/tests/test_discovery_individual_matcher.py -v
  Expect: 12 passed. 0 failures.

  Then full regression:
      python -m pytest fortress/tests/ -q
  Expect: previous green count - 8 (broken tests deleted) + 12 (new tests).
          Net delta: +4 vs the prior failing exec's count, OR +12 vs
          pre-Apr-26 baseline. No regression in unrelated test files.

  NOTE: do NOT add `--timeout=N`. pytest-timeout is not installed and the
        flag will error out the run.

  KEY TESTS TO INSPECT FOR THE BUG-FIX SIGNATURE:
    - T3 (test_pass2_skipped_when_pass1_band_a) — assert pass 2 doesn't
      fire when pass 1 finds Band A. Regression guard.
    - T4 (test_pass2_fires_when_pass1_below_band_a) — THE central test
      proving the new trigger works. If T4 passes, the structural bug
      from the prior exec is gone.
    - T11 (test_pass2_scores_by_enseigne_only_not_greatest) — proves the
      SQL itself ranks against `enseigne` only.

CHECK 1.2 — REPL helper sanity (verify the function actually fires pass 2
            when pass 1 has rows but no Band A — THE structural bug case)
  Run this Python snippet (paste into a `python -c '…'` or temp file).
  This test is specifically designed to exercise the structural fix: pass 1
  finds rows at NAF prefix + CP, but no row scores Band A; pass 2 must then
  re-score the code-1000 subset against `enseigne` only and produce a hit.

      import asyncio, os
      os.chdir('/Users/alancohen/Project Alan copy/fortress')
      from dotenv import load_dotenv
      load_dotenv()
      import psycopg
      from fortress.discovery import _cp_name_disamb_match

      async def main():
          conn_str = os.environ['DATABASE_URL']
          async with await psycopg.AsyncConnection.connect(conn_str) as conn:
              # Find a real CP with a known code-1000 enseigne in NAF 01.21Z
              # (Viticulture — Marne is the densest dept). We feed the helper
              # the EXACT enseigne string as maps_name to guarantee pass-2
              # similarity ≈ 1.0 against itself. Pass 1 will see the row in
              # its pool but GREATEST(enseigne, denomination) wash-out is
              # likely to produce a low top_sim because the same NAF+CP pool
              # contains many EARL/SCEA `denomination` rows that don't
              # match the trade-name maps_name at all.
              cur = await conn.execute(
                  """SELECT enseigne, code_postal, naf_code FROM companies
                     WHERE code_postal LIKE '51%'
                       AND forme_juridique='1000'
                       AND naf_code = '01.21Z'
                       AND enseigne IS NOT NULL AND enseigne<>''
                       AND statut='A'
                     LIMIT 1"""
              )
              row = await cur.fetchone()
              if row is None:
                  print("FAIL: no fixture row found in CP 51%, NAF 01.21Z, code-1000.")
                  return
              maps_name, maps_cp, expected_naf = row[0], row[1], row[2]
              print(f"FIXTURE: maps_name={maps_name!r} cp={maps_cp} expected_naf={expected_naf}")

              # Same-NAF picker: pass 1 fetches a wide pool at this CP+NAF,
              # but maps_name only matches the chosen enseigne strongly —
              # pass 1's GREATEST scoring still has to rank it #1; if
              # similarity(enseigne, maps_name)=1.0 dominates, pass 1 might
              # actually hit Band A for this fixture. To force pass 2:
              #   - Pick a maps_name that is the ENSEIGNE only (raw value),
              #     NOT a near-duplicate. Pass 1 GREATEST will produce
              #     sim=1.0 for THAT enseigne row but rank dominance is
              #     instant → Band A hits. So this fixture trips pass 1.
              # That's actually fine — it means CHECK 1.2 confirms pass 1
              # works when pass 1 SHOULD work, AND we still have the
              # auto_linked_cp_name_disamb path covered.

              result = await _cp_name_disamb_match(
                  conn, maps_name, maps_cp, [expected_naf], None
              )
              print("RESULT (pass-1-band-A path):", result)
              assert result is not None, "Pass 1 should hit Band A on this fixture"
              assert result["method"] in ("cp_name_disamb", "cp_name_disamb_indiv"), \
                     f"unexpected method {result['method']}"

              # Now force the structural pass-2 case: use a STRESSED maps_name
              # — small typo / partial match — so pass 1 GREATEST doesn't
              # cross Band A. Truncate the enseigne to ~60% of length:
              stressed = maps_name[: max(3, int(len(maps_name) * 0.6))]
              print(f"STRESS: stressed_maps_name={stressed!r}")
              result2 = await _cp_name_disamb_match(
                  conn, stressed, maps_cp, [expected_naf], None
              )
              print("RESULT (stressed path):", result2)
              # Expected outcomes:
              #   - method="cp_name_disamb_indiv"  → pass 2 fired and confirmed
              #     (proves the structural fix works on real DB)
              #   - method="cp_name_disamb"        → pass 1 still hit Band A
              #     (truncation wasn't aggressive enough; not a failure)
              #   - None                           → pass 1 below A, pass 2
              #     also below A_indiv. Acceptable but check log lines for
              #     `discovery.cp_name_disamb_indiv_no_band` to prove pass 2
              #     ran.

      asyncio.run(main())

  EXPECTED OUTPUT:
    First call (pass-1-band-A path): RESULT prints a dict with method in
       {"cp_name_disamb", "cp_name_disamb_indiv"}.
    Second call (stressed path): RESULT prints either a pass-2 dict, a
       pass-1 dict, or None — any of the three is acceptable IF a pass-2
       structlog line is present (matched OR no_band).

  TO PROVE PASS 2 FIRES STRUCTURALLY, check log lines from the run:
    Expect to see at least ONE of:
      - discovery.cp_name_disamb_indiv_match    (matched, sim ≥ 0.85)
      - discovery.cp_name_disamb_indiv_no_band  (rows present but sim < 0.85)
    Either log line proves pass 2 ran with the new trigger. If you see
    NEITHER, pass 2 is not wired correctly OR the fixture doesn't reach
    pass 2 (in which case CHECK 1.4 will catch it on a real batch).

  PASS condition: first call returns a dict with valid method; second call
                  emits a `discovery.cp_name_disamb_indiv_*` log line OR
                  returns method="cp_name_disamb_indiv".
  FAIL condition: NO `cp_name_disamb_indiv_*` log line emitted across both
                  calls AND no method="cp_name_disamb_indiv" returned —
                  the pass-2 trigger is broken.

CHECK 1.3 — Run a fresh ws174 arboriculture batch (Marne 51 — high density)
  Login as head.test (Test1234), launch batch:
      curl -s -c /tmp/t2cookies.txt -X POST http://localhost:8080/api/auth/login \
           -H "Content-Type: application/json" \
           -d '{"username":"head.test","password":"Test1234"}'
      curl -s -b /tmp/t2cookies.txt -X POST http://localhost:8080/api/batch/spawn \
           -H "Content-Type: application/json" \
           -d '{"queries":["arboriculture 51"],"target_count":30}'

  Why dept 51 (Marne, not 47 Lot-et-Garonne):
    - Dept 51 has 965 code-1000 agricultural rows with non-empty enseigne
    - Dept 47 has only 516
    - Dept 51's dominant NAF leaf is 01.21Z (Viticulture). Cindy's
      "arboriculture" picker would default to 01.24Z (Pome and stone fruit),
      so pass 1 (NAF-strict 01.24Z) finds few hits and pass 2 has the
      maximum chance of firing on a fresh batch.

  Wait for batch completion (poll /api/jobs or watch monitor page).

  IDENTIFY batch_id:
      SELECT batch_id, batch_name, status, companies_scraped, completed_at
      FROM batch_data
      WHERE workspace_id = 174 AND batch_name ILIKE 'ARBORICULTURE_51%'
      ORDER BY created_at DESC LIMIT 1;
  Expect: status='completed', companies_scraped > 0.

CHECK 1.4 — At least one auto_linked_individual_match row (THE acceptance gate)
  SQL:
      WITH bd AS (
          SELECT batch_id FROM batch_data
          WHERE workspace_id = 174
            AND batch_name ILIKE 'ARBORICULTURE_51%'
          ORDER BY created_at DESC LIMIT 1
      )
      SELECT bl.action, COUNT(*) AS n
      FROM batch_log bl
      JOIN bd ON bd.batch_id = bl.batch_id
      WHERE bl.action LIKE 'auto_linked%'
      GROUP BY bl.action
      ORDER BY n DESC;

  PASS condition: `auto_linked_individual_match` count ≥ 1.
  FAIL condition: zero rows for `auto_linked_individual_match`.

  If FAIL with zero matches:
    - Check `discovery.cp_name_disamb_indiv_no_band` log lines from the
      batch process — pass 2 fired but found nothing above 0.85.
    - This is rare but possible if the Maps results in dept 51 happen to
      have unusual enseigne forms ("Earl Champagne X" with no individual
      enseigne backing). Re-run with a bigger target_count (60-80) before
      declaring failure.
    - If still zero after a 60-result batch, escalate: pass 2 may not be
      wired up. Investigate by manually picking a known maps_name and
      re-running CHECK 1.2.

CHECK 1.4b — Verify enseigne is non-empty AND similar-to-maps for every match
  This new check catches the bug where pass 2 might (in some edge case)
  return a row with empty enseigne or a low-similarity enseigne. It
  cross-references the SIRENE row and the Maps query.

  SQL:
      WITH bd AS (
          SELECT batch_id FROM batch_data
          WHERE workspace_id = 174 AND batch_name ILIKE 'ARBORICULTURE_51%'
          ORDER BY created_at DESC LIMIT 1
      ),
      indiv_links AS (
          SELECT bl.siren AS maps_siren, bl.action
          FROM batch_log bl
          JOIN bd ON bd.batch_id = bl.batch_id
          WHERE bl.action = 'auto_linked_individual_match'
      )
      SELECT m.siren AS maps_id,
             m.denomination AS maps_name,
             c.siren AS sirene,
             c.enseigne,
             c.denomination AS legal_name,
             c.forme_juridique,
             c.naf_code,
             c.code_postal,
             ROUND(similarity(LOWER(COALESCE(c.enseigne,'')), LOWER(COALESCE(m.denomination,''))) :: numeric, 3) AS sim_enseigne
      FROM indiv_links il
      JOIN companies m ON m.siren = il.maps_siren
      JOIN companies c ON c.siren = m.linked_siren
      LIMIT 5;

  PASS condition:
    - Every row has c.enseigne IS NOT NULL AND c.enseigne <> ''
    - Every row has c.forme_juridique = '1000'
    - Every row has c.naf_code starting with '01.'
    - Every row has c.code_postal = m.code_postal (CP match)
    - Every row has sim_enseigne ≥ 0.85
    - At least one row's sim_enseigne is meaningfully higher than what
      pass 1 GREATEST would have computed against the maps_name (sanity:
      pass 2 actually picked a better match than pass 1 would)
  FAIL condition: any row violates the above. The bug to catch:
    - enseigne empty → SQL filter not applied or post-LIMIT
    - sim_enseigne < 0.85 → threshold not enforced or pass 2 used wrong scoring

CHECK 1.5 — link_confidence='confirmed' for those rows (auto-confirm gate works)
  SQL:
      WITH bd AS (
          SELECT batch_id FROM batch_data
          WHERE workspace_id = 174 AND batch_name ILIKE 'ARBORICULTURE_51%'
          ORDER BY created_at DESC LIMIT 1
      ),
      indiv_sirens AS (
          SELECT bl.siren FROM batch_log bl
          JOIN bd ON bd.batch_id = bl.batch_id
          WHERE bl.action = 'auto_linked_individual_match'
      )
      SELECT co.siren, co.linked_siren, co.link_confidence, co.link_method,
             co.naf_status
      FROM companies co
      JOIN indiv_sirens i ON i.siren = co.siren;

  PASS condition: every row has link_confidence='confirmed',
                  link_method='cp_name_disamb_indiv', naf_status='verified'.
  FAIL condition: any row pending OR with a different method/status —
                  the audit branch in step 4 of the brief is mis-wired.

CHECK 1.6 — No regression on existing cp_name_disamb hits
  SQL:
      WITH bd AS (
          SELECT batch_id FROM batch_data
          WHERE workspace_id = 174 AND batch_name ILIKE 'ARBORICULTURE_51%'
          ORDER BY created_at DESC LIMIT 1
      )
      SELECT bl.action, COUNT(*) AS n
      FROM batch_log bl
      JOIN bd ON bd.batch_id = bl.batch_id
      WHERE bl.action IN ('auto_linked_cp_name_disamb', 'auto_linked_individual_match',
                          'auto_linked_verified', 'auto_linked_mismatch_accepted',
                          'auto_linked_strong_no_filter')
      GROUP BY bl.action ORDER BY n DESC;

  PASS condition: existing audit branches still produce some rows
                  (auto_linked_verified count > 0 OR auto_linked_cp_name_disamb
                  > 0). Pass 2 must be additive, not a regression.
  FAIL condition: total auto_linked* count drops to zero — pass 1 broken.

CHECK 1.7 — No leakage onto non-agricultural sectors
  Re-run a SECOND ws174 batch in a non-agriculture sector (10 entities,
  fast):
      curl -s -b /tmp/t2cookies.txt -X POST http://localhost:8080/api/batch/spawn \
           -H "Content-Type: application/json" \
           -d '{"queries":["restaurant 51"],"target_count":10}'
  Wait for completion. Then SQL:
      WITH bd AS (
          SELECT batch_id FROM batch_data
          WHERE workspace_id = 174 AND batch_name ILIKE 'RESTAURANT_51%'
          ORDER BY created_at DESC LIMIT 1
      )
      SELECT COUNT(*) AS leak_count FROM batch_log bl
      JOIN bd ON bd.batch_id = bl.batch_id
      WHERE bl.action = 'auto_linked_individual_match';

  PASS condition: leak_count = 0. Pass 2 must NEVER fire on a non-01.* picker.
  FAIL condition: leak_count > 0 — agriculture gate is mis-implemented.
                  Inspect the `if not all(p.startswith("01.") for p in
                  naf_prefixes):` check in the brief.

CHECK 1.8 — 99% GOAL TRACKING (mandatory per CLAUDE.md April 24)
  SQL:
      WITH recent AS (
          SELECT DISTINCT co.siren, co.linked_siren, co.link_confidence
          FROM batch_data bd
          JOIN batch_log bl ON bl.batch_id = bd.batch_id
          JOIN companies co ON co.siren = bl.siren
          WHERE bd.workspace_id = 174 AND bd.status = 'completed'
            AND bd.created_at::date >= CURRENT_DATE - INTERVAL '7 days'
      )
      SELECT COUNT(DISTINCT siren) AS total,
             ROUND(100.0 * SUM(CASE WHEN link_confidence = 'confirmed' THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(DISTINCT siren), 0), 1) AS confirmed_pct
      FROM recent;

  REPORT (verbatim format, mandatory):
    "99% GOAL TRACKING: ws174 confirm rate now X.X% (previous QA: 45.2%,
    delta +/-Z.Zpp). Gap to 99%: N.Npp."

  Baseline source: HANDOFF.md (Apr 25 evening) recorded 45.2% as the most
  recent ws174 7-day confirm rate. This brief targets +3-5pp on rural
  batches; expect the headline number to creep up by 0.5-1.5pp once the
  arboriculture 51 batch from CHECK 1.3 is folded in (the new entities
  alone won't move it dramatically, but ANY confirms are net additions
  to the numerator).

  PASS condition: number reported in the canonical format. No specific
                  delta required — this check is observational, not
                  pass/fail.

================================================================================
SECTION 2 — MANUAL BROWSER CHECKS (QA agent runs via Playwright)
================================================================================

Per memory feedback_qa_runs_manual_checks.md: QA agent executes these
itself, not as homework for Alan.

  M.1 — Login as head.test, navigate to #/monitor.
        Verify the arboriculture 51 batch shows "completed".
        Screenshot for evidence.

  M.2 — Click into the arboriculture 51 batch detail page.
        Verify entity list renders (some MAPS, some real SIRENs).
        Verify no JS console errors.

  M.3 — Find one entity that batch_log SQL above showed as
        action='auto_linked_individual_match'. Click into its company
        detail page (#/company/<siren>).
        Verify:
          (a) The page loads with no errors.
          (b) The link badge shows "Vérifié" / "Confirmé" (green) — NOT
              "En attente" (amber).
          (c) The SIRENE block shows the linked real SIREN's name +
              `forme_juridique` rendered as "Entrepreneur individuel"
              (the FORME_LABELS dict in company.js:83 maps '1000' to that).
          (d) No "⚠ NAF incompatible" badge — naf_status should be
              `verified`. (If you DO see it, pass 2's NAF-strict-prefix
              guarantee is broken — investigate.)

  M.4 — Visit #/contacts. Search for the same SIREN. Verify the row
        renders with no "En attente d'approbation" amber badge.

  M.5 — Visit #/dashboard. Verify dashboard renders, no console errors,
        all widgets load.

  M.6 — Smoke-check: run a fresh search bar lookup on the same SIREN
        from #/search. The page should render the SIRENE detail
        (no enrichment trigger — search is lookup-only).

  If any manual step fails, take a screenshot and report the specific
  failure step + browser console output.

================================================================================
SECTION 3 — SCORECARD
================================================================================

  Step  | Check                                          | Expected         | Actual | Pass/Fail
  ------|------------------------------------------------|------------------|--------|----------
  1.1   | Unit tests + full regression                   | 12 new pass; 0 reg|       |
  1.2   | REPL helper sanity — pass 2 fires structurally | indiv log line emitted |  |
  1.3   | ws174 arboriculture 51 batch completes         | status=completed |        |
  1.4   | ≥1 auto_linked_individual_match row            | count ≥ 1        |        |
  1.4b  | enseigne non-empty + sim ≥ 0.85 + 01.* NAF     | every row OK     |        |
  1.5   | All such rows confirmed + verified             | every row OK     |        |
  1.6   | No regression on pass 1                        | total auto_linked > 0 |   |
  1.7   | No leakage onto restaurant 51                  | leak_count = 0   |        |
  1.8   | 99% GOAL TRACKING reported                     | format correct   |        |
  M.1   | Monitor shows batch completed                  | render OK        |        |
  M.2   | Batch detail page renders                      | no console err   |        |
  M.3   | Company detail — confirmed + verified          | green badge      |        |
  M.4   | Contacts page row clean                        | no amber badge   |        |
  M.5   | Dashboard renders                              | no console err   |        |
  M.6   | Search page lookup works                       | render OK        |        |

================================================================================
SECTION 4 — PASS / FAIL CRITERIA
================================================================================

  PASS:  All 9 automated checks (1.1, 1.2, 1.3, 1.4, 1.4b, 1.5, 1.6, 1.7,
         1.8) PASS + all 6 manual checks (M.1-M.6) PASS. Confirm rate
         reported in canonical format.

  FAIL:  Any one check FAILS — report which, with SQL output / screenshot
         / console log. Common failure modes:
           - 1.1 fails  → executor broke unit-test parity, didn't replace
                          old broken tests, or broke pass 1's Band A path
                          (T3 regression). Check T3, T4, T11 specifically.
           - 1.2 fails  → pass 2 trigger is broken; either still hooked to
                          `if not rows:` or agriculture gate broken
           - 1.4 fails  → THE structural bug fix didn't land — batch ran
                          but pass 2 never produced a confirm. THIS IS THE
                          QA THAT THE PRIOR EXEC FAILED ON. Likely cause:
                          pass 2 still triggers on `if not rows:` (only
                          fires when pass 1 empty, which is structurally
                          rare on agri batches because companies + indiv
                          live in the same NAF+CP pool).
           - 1.4b fails → pass 2 returned a row that doesn't actually
                          satisfy enseigne IS NOT NULL OR scored < 0.85.
                          Audit the SQL in Work Item 2.
           - 1.5 fails  → audit branch hits but auto-confirm gate didn't
                          confirm (regression in line 2702-2723 logic)
           - 1.7 fails  → agriculture gate disabled or `naf_prefixes`
                          variable shadowed
           - M.3.d fails → matched_naf doesn't strict-prefix picker;
                          pass 2's NAF guarantee broken

QA TEST PLAN — END
```

---

## TASKS.md update notes

The existing TOP PRIORITY 2 entry in `/Users/alancohen/Project Alan copy/TASKS.md` (lines 41-60) uses incorrect `cat_jur_code` naming and incorrect `cat_jur=[1000]` (integer) syntax. After Alan approves this brief, update TASKS.md to:

- Replace `cat_jur_code` references with `forme_juridique` (TEXT column).
- Replace `cat_jur=[1000]` with `forme_juridique='1000'` (string filter).
- Replace `cat_jur_filter: list[int] | None = None` with the actual implementation (no signature change — the new behavior is a body-internal pass 2 fallback).
- Reflect that scope is **agriculture-only NAF `01.*`** (not all sectors). Other sectors are explicit follow-ups.
- Reflect that Band B is intentionally skipped — not deferred.
- Mark routing as `/exec` brief printed (not "trivial extension; confirm cat_jur_code populated" — column is verified populated).

Suggested replacement text for the TOP PRIORITY 2 block (Alan should approve before edit):

> **TOP PRIORITY 2 — Individual cat_jur 1000 matcher (agriculture-only)** — extends `_cp_name_disamb_match` with a pass-2 SQL fallback when pass 1 fails to land a Band A candidate (top_sim < 0.90 OR no rows) AND every NAF prefix starts with `01.`. Filters by `forme_juridique='1000'` (TEXT, NOT integer) and `enseigne IS NOT NULL`. Pass 2 ranks by `similarity(enseigne, maps_name)` ONLY (not GREATEST with denomination — load-bearing fix vs prior failed exec). Threshold 0.85 (Band A only — no Band B). Emits new method `cp_name_disamb_indiv` and audit action `auto_linked_individual_match`. Single file change: `fortress/discovery.py`. Other 1000-heavy sectors (coiffeurs, bakers, libéraux) deferred to future briefs.

---

## Last flags for Alan

**Revision context (Apr 26 evening):** This brief is a re-issue. The first executor passed unit tests but failed QA: the pass-2 trigger was hooked to `if not rows:`, which is structurally unable to produce live matches because pass 2's SQL is a strict subset of pass 1's. This revision fixes the trigger (`not pass1_has_band_a` instead of empty rows) and the scoring field (`enseigne` only instead of GREATEST with denomination). Work Items 1, 3, 4 from the original brief are already landed and now show as "VERIFY" rather than "ADD" — only Work Item 2 (the SQL/trigger inside the helper) is being rewritten.

Three deliberate non-decisions in this brief, all reversible:

1. **Frontend `_STRONG_METHODS` (`company.js:39`) intentionally untouched.** The existing `cp_name_disamb` method is also missing from the frontend Set — this is a known parity gap. Adding only the new variant would create asymmetric tooltip behavior between the two `cp_name_disamb*` methods. Either (a) leave both off and accept the terse tooltip on the rare mismatch path, or (b) ship a separate frontend-parity brief that adds both. This brief takes path (a).

2. **Pass 2 keeps the SAME NAF prefix as pass 1.** It does NOT broaden to `01.%` even when picker is e.g. `01.24Z`. This is conservative — it means cross-leaf agricultural mismatches (an arboriculture batch finding a vit-registered farmer) won't be caught by this lever. The reasoning: with no `SECTOR_EXPANSIONS` entries for `01.*`, broadening would land on `naf_status='mismatch'` for cross-leaf matches, which would require Phase A signal logic to confirm — and individual entrepreneurs typically only produce one signal (enseigne_match), so confirms would land in pending instead of confirmed. If you want broader coverage, that's a separate brief once we have lat/lng (TOP 1) shipped.

3. **No QA pass 1 regression-forcing assertion.** I considered adding a CHECK that pass 1 *MUST* find ≥1 candidate on a normal arboriculture batch (to prove pass 1 still fires) but rejected it — pass 1 is shipped, audited, and our smoke-test SQL plan inspection (CHECK 1.6) covers it more cheaply. If Alan wants tighter pass-1-non-regression guarantees, add a pre-existing-test snapshot.

Verdict: ready for review.
