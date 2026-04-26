# PHASE 3 — REVISED — BAN backfill of ws174 historical MAPS entities + Step 2.6 sweep

## Plain-English intro (for Alan)

**What this phase does**

ws174 already has hundreds of MAPS entities created before the new geo proximity matcher existed. Those entities never had latitude/longitude captured, so the new matcher cannot match them — the geo signal is missing.

This phase:
1. Asks France's free national address-geocoder (BAN — Base Adresse Nationale, run by data.gouv.fr) to look up coordinates for those historical addresses, in bulk.
2. Stores the returned coordinates in `companies_geom` so the geo matcher can finally see them.
3. Re-runs ONLY Step 2.6 (the new geo matcher) on each of those entities. Steps 0-2.5 already ran historically — the only thing that could find a NEW match now is geo proximity.
4. Auto-confirms cautiously. Picker context (the NAF codes the user originally chose) is lost from history, so we can't trust the NAF gate. Instead we use VERY strict thresholds (top score 0.95 vs the fresh-batch 0.85; dominance 0.20 vs fresh 0.15) and tag every result with `geo_proximity_backfill: true` so it can be sample-audited.

**ws174 ONLY in V1.** The Cindy ws1 retrofit is a separate session AFTER we sample-validate precision. The endpoint hard-rejects any other workspace_id.

**What changed from the previous draft (3 BLOCKERS + 2 MAJORS + 1 MINOR fixed + BAN endpoint locked Apr 26)**

- BLOCKER 1: The replay path no longer calls `_compute_naf_status`. That function only returns `'verified'` or `'mismatch'` — never `'maps_only'` or `None`. With `picked_nafs=[]` it would always return `'mismatch'`, sending every backfill row to pending. Phase 3 now BYPASSES the function: sets `naf_status = NULL` directly and gates auto-confirm on conservative geo thresholds + the `geo_proximity_backfill` audit flag.
- BLOCKER 2: Conservative thresholds (0.95/0.20) require Phase 2 to expose `top_threshold` and `dominance_threshold` as keyword args on `_geo_proximity_match`. Stated as a hard cross-phase dependency at the top of the brief.
- BLOCKER 3: `log_audit` requires `batch_id: str`. Phase 3 runs outside batch context. Verified `batch_log.batch_id` is `TEXT` (no width limit) at `fortress/database/schema.sql:176`. Synthetic constant `BACKFILL_BATCH_ID = f"BACKFILL_BAN_GEO_{date.today().isoformat()}"` is safe (e.g. `BACKFILL_BAN_GEO_2026-04-26`, 30 chars — fits TEXT).
- BAN endpoint contract LOCKED (Apr 26 — `.ban_endpoint_report.md`): Pre-code checkpoint is COMPLETE. POST shape, response columns, quality mapping, and a CRITICAL postcode-verification safeguard are baked into the brief.
- CRITICAL postcode verification (NEW Apr 26): Score-only gating is UNSAFE. BAN returned 0.97 for `"8 boulevard du Port"` matching the WRONG city (Cergy, not Amiens). Backfill MUST verify `result_postcode == sirene.code_postal` before accepting any geocode, and pass the FULL address (street + postcode + city) on input. Rows that fail the postcode check are DROPPED and audited as `postcode_mismatch`.
- result_status filter (NEW Apr 26): BAN returns blank score on garbage inputs (`result_status='not-found'`) and on empty rows (`result_status='skipped'`). Parser checks `result_status == 'ok'` FIRST before applying score threshold.
- MAJOR (idempotency): Documented explicitly — rows with BAN `result_score < 0.5` are dropped (not inserted). Re-runs naturally retry them. INSERT-only-when-quality-good = idempotency guarantee.
- MINOR (ws1 phrasing): Reworded ws1 isolation check. Phase 2's `sirene_geo` rows ARE global. Only `source='ban_backfill'` rows are restricted to ws174. The check verifies ban_backfill respected the workspace gate, not that ws1 has zero rows in `companies_geom`.
- BONUS fix: Original brief said the non-admin endpoint should return 401. The actual `_get_admin` pattern at `admin.py:26-34` returns 403 with "Admin requis." French message. Updated.
- BONUS fix: Scripts directory is `fortress/scripts/` — confirmed by `ls`. Original draft had `fortress/fortress/scripts/` in one item but the touched-files block had it right; corrected throughout.

---

```
EXECUTOR BRIEF — START PHASE 3
================================================================================
PHASE 3 — BAN backfill of ws174 historical MAPS entities + Step 2.6 sweep
================================================================================

GOAL
  Retrofit Phase 2's geo proximity matcher onto ws174's existing
  historical MAPS entities (those created BEFORE Phase 1 shipped, hence
  no Maps panel coords). Approach:
   (a) Enumerate ws174 MAPS entities with no row in companies_geom.
   (b) Build a CSV of (siren, normalized_address) and POST to BAN's
       free /search/csv/ endpoint — France's national address-geocoder
       gateway (api-adresse.data.gouv.fr).
   (c) Insert returned WGS84 coords into companies_geom with
       source='ban_backfill', geocode_quality from BAN's result_score.
       Drop rows with result_score < 0.5 — DO NOT INSERT.
   (d) Re-invoke ONLY Step 2.6 of the matcher on those entities, with
       CONSERVATIVE thresholds (top_threshold=0.95, dominance_threshold=0.20).
   (e) Update companies.linked_siren / link_method='geo_proximity' /
       link_confidence='confirmed' on accept. naf_status is set to NULL
       directly (NEVER call _compute_naf_status — see BLOCKER 1 below).
       link_signals carries `{"geo_proximity_backfill": true,
       "top_score": <float>, "dominance": <float>, "ban_score": <float>}`.
   (f) Audit each successful retrofit as
       'auto_linked_geo_proximity_backfill' via log_audit with the
       synthetic batch_id constant.

  ws174 ONLY (Decision 7). Cindy ws1 retrofit is a separate session
  AFTER we sample-validate this phase's precision on ws174.

================================================================================
HARD CROSS-PHASE DEPENDENCIES (must already be in place)
================================================================================

A) Phase 1 in production:
   - companies_geom table exists (created in Phase 1's startup migration).

B) Phase 2 in production with the EXACT signature below:
   - `_geo_proximity_match` is exported from fortress.discovery.
   - `_GEO_TOP_THRESHOLD`, `_GEO_DOMINANCE_THRESHOLD` constants exist.
   - The function signature accepts override kwargs:
        async def _geo_proximity_match(
            conn,
            maps_name: str,
            lat: float,
            lng: float,
            *,
            picked_nafs: list[str] | None = None,
            naf_division_whitelist: list[str] | None = None,
            top_threshold: float = _GEO_TOP_THRESHOLD,         # default 0.85
            dominance_threshold: float = _GEO_DOMINANCE_THRESHOLD,  # default 0.15
        ) -> Candidate | None
   - sirene_geo bulk import has loaded ~17M rows (Phase 2 deliverable).

   If any of (A)/(B) is missing, the script crashes at import time or
   produces zero results. STOP THE EXECUTOR if Phase 2's signature does
   not include the two threshold kwargs. Phase 3 will not work without
   them.

================================================================================
BLOCKER 1 RESOLUTION — BYPASS _compute_naf_status
================================================================================

`_compute_naf_status` (discovery.py:69-101) returns ONLY 'verified' or
'mismatch' — never 'maps_only' or None. With picked_nafs=[] and
naf_division_whitelist=None, it falls past the for-loop (empty list,
no iterations) and lands on `return "mismatch"` at line 101.

Calling it from the backfill replay would push EVERY row to pending,
defeating the entire phase.

Fix: the backfill DOES NOT CALL `_compute_naf_status`. Instead:
  - On accept, write `naf_status = NULL` directly via UPDATE.
  - The decision to auto-confirm is gated entirely on:
        cand is not None AND cand.top_score >= 0.95 AND cand.dominance >= 0.20
    AND on the conservative thresholds being passed into Phase 2's
    function (see BLOCKER 2 resolution).
  - link_signals carries `geo_proximity_backfill: true` so any auditor
    or downstream UI knows this row bypassed the picker-NAF check.

================================================================================
BLOCKER 2 RESOLUTION — Phase 2 signature dependency
================================================================================

The conservative-threshold strategy requires Phase 2's
`_geo_proximity_match` to accept `top_threshold` and
`dominance_threshold` as keyword arguments (defaulting to Phase 2's
0.85 / 0.15 constants).

Phase 3 calls Phase 2 like so:
    cand = await _geo_proximity_match(
        conn,
        maps_name,
        lat,
        lng,
        picked_nafs=None,
        naf_division_whitelist=None,
        top_threshold=0.95,
        dominance_threshold=0.20,
    )

If Phase 2 hard-coded the thresholds inside the function body,
Phase 3 has no way to tighten them. STATE THIS DEPENDENCY EXPLICITLY
AT THE TOP OF PHASE 2's brief. This Phase 3 brief assumes the kwargs
exist.

================================================================================
BLOCKER 3 RESOLUTION — synthetic batch_id for log_audit
================================================================================

`log_audit` (fortress/processing/dedup.py:260) signature requires
`batch_id: str` as a keyword argument. The backfill runs outside any
real batch.

Schema verification:
  - `batch_log.batch_id TEXT NOT NULL` (schema.sql:176).
  - TEXT has no length cap, so a long descriptive string is fine.

Define near the top of the script:

    from datetime import date
    BACKFILL_BATCH_ID = f"BACKFILL_BAN_GEO_{date.today().isoformat()}"
    # e.g. "BACKFILL_BAN_GEO_2026-04-26" — 30 chars, fits TEXT.

Use this constant for ALL log_audit calls inside the script. Same
batch_id ties the run together when querying batch_log later. If the
script is run multiple times in the same day (idempotency check
during QA), all those audits share one batch_id — that's intentional;
per-row siren disambiguates them.

================================================================================
PRE-CODE CHECKPOINT — COMPLETED Apr 26, 2026
================================================================================

The BAN endpoint contract was tested and locked Apr 26. Full report at
`.ban_endpoint_report.md` (repo root). Executor does NOT need to re-run
the checkpoint — the contract below is authoritative. Proceed directly
to implementation.

LOCKED POST CONTRACT
--------------------
  curl -X POST \
    -F data=@<chunk.csv> \
    -F columns=q \
    https://api-adresse.data.gouv.fr/search/csv/

  - HTTP 200 on first attempt — no parameter variations needed.
  - `data` (file) and `columns=q` (input column name) are sufficient.
  - DO NOT pass `result_columns`. The default response returns the
    full 20-column schema, including `result_postcode` and
    `result_status` which we need for safety gates.
  - Any non-`q` input columns are echoed back unchanged on each
    output row — we use this to round-trip the `siren` column.
  - Input CSV: must have `siren` and `q` columns. `q` MUST contain
    the FULL address (street + postcode + city) — see CRITICAL note
    below.

LOCKED RESPONSE COLUMN NAMES (20 columns, in order)
---------------------------------------------------
  1.  siren                — echoed input
  2.  q                    — echoed input address
  3.  longitude            — float (note: longitude PRECEDES latitude
                             in CSV order, EPSG xy convention)
  4.  latitude             — float
  5.  result_score         — float, 0.0–1.0 (BLANK on garbage input)
  6.  result_score_next    — float, score of next-best candidate
  7.  result_label         — normalized French address
  8.  result_type          — housenumber|street|locality|municipality
  9.  result_id            — BAN ID
  10. result_housenumber
  11. result_name
  12. result_street
  13. result_postcode      — 5-digit, USED FOR SAFETY GATE
  14. result_city
  15. result_context
  16. result_citycode      — INSEE commune code
  17. result_oldcitycode
  18. result_oldcity
  19. result_district
  20. result_status        — ok|not-found|skipped, USED FOR FILTER

CRITICAL — DUAL-GATE SAFETY (NEW Apr 26 — DO NOT SKIP)
------------------------------------------------------
A high `result_score` does NOT guarantee the right city. Tested
example: input `"8 boulevard du Port"` (no city) returned
`result_score=0.97` matching Cergy when SIRENE expected Amiens.

Therefore the response handler MUST apply BOTH gates IN THIS ORDER:

  GATE 1 (status):
      if result_status != 'ok' or not result_score_str:
          drop_row("ban_no_match", siren=siren)
          continue

  GATE 2 (postcode verification — MANDATORY):
      result_pc = (result.get("result_postcode") or "").strip()
      if result_pc != sirene_code_postal:
          drop_row("postcode_mismatch",
                   siren=siren,
                   expected=sirene_code_postal,
                   got=result_pc)
          continue

  GATE 3 (quality threshold):
      score = float(result_score_str)
      if score >= 0.8:
          quality = 'bonne'
      elif score >= 0.5:
          quality = 'acceptable'
      else:
          drop_row("ban_low_score", siren=siren, score=score)
          continue

Both `result_postcode` and `companies.code_postal` are 5-char French
postal codes. Direct string equality (after `.strip()`) is correct.
DO NOT use citycode (INSEE) instead — it differs for Paris/Lyon/
Marseille arrondissements. Postal code is the right join key.

INPUT ADDRESS BUILDER (locked)
------------------------------
The `q` value MUST concat all available components:
    full = ", ".join(p for p in (adresse, code_postal, ville) if p)
Skipping any component (esp. city) causes high-score wrong-city
matches. If `code_postal` or `ville` is NULL the row STILL goes
through (BAN may still find a match at lower confidence), but the
postcode-verification gate above will then reject anything where
SIRENE's stored postcode is NULL — those rows are correctly dropped
(we have no way to verify them).

================================================================================
WORK ITEMS
================================================================================

1) NEW SCRIPT — fortress/scripts/backfill_ban_geo.py
   ----------------------------------------------------------------------
   Verified path: `fortress/scripts/` exists at the project root and
   contains existing scripts (cleanup_legacy_contacts.py,
   cleanup_orphan_maps.py, retrofit_d1b.py). DO NOT create
   `fortress/fortress/scripts/` — that directory does not exist.

   Standalone CLI script. Uses curl_cffi (already in deps). Skeleton:

       """BAN-geocoded backfill for ws174 historical MAPS entities.
       Idempotent. Re-runs Step 2.6 of the matcher on backfilled rows
       and auto-confirms with conservative thresholds (0.95 / 0.20).

       NEVER calls _compute_naf_status. The picker context is lost
       historically, so naf_status is set to NULL directly on accept,
       and link_signals carries geo_proximity_backfill:true.
       """

       from __future__ import annotations

       import argparse
       import asyncio
       import csv
       import io
       import json
       import os
       import sys
       from datetime import date
       from pathlib import Path

       # repo-root sys.path bootstrap (mirrors retrofit_d1b.py:36-39)
       _REPO = Path(__file__).resolve().parent.parent
       if str(_REPO) not in sys.path:
           sys.path.insert(0, str(_REPO))

       import curl_cffi
       from fortress.api.db import init_pool, close_pool, get_conn
       from fortress.discovery import (
           _geo_proximity_match,
           _copy_sirene_reference_data,
       )
       from fortress.processing.dedup import log_audit

       BAN_CSV_URL = "https://api-adresse.data.gouv.fr/search/csv/"
       BAN_CHUNK = 5000        # BAN limits ~50MB / 50 req/s
       BAN_RATE_DELAY_S = 1    # polite delay between chunks
       BAN_MIN_SCORE = 0.5     # below this — DROP, do not insert

       # Conservative thresholds for backfill auto-confirm
       # (vs fresh-batch 0.85 / 0.15 — picker context lost, so tighten).
       BACKFILL_TOP_THRESHOLD = 0.95
       BACKFILL_DOMINANCE_THRESHOLD = 0.20

       BACKFILL_BATCH_ID = f"BACKFILL_BAN_GEO_{date.today().isoformat()}"

       async def enumerate_targets(workspace_id: int = 174,
                                   limit: int | None = None):
           """ws174 MAPS entities with non-empty address and no companies_geom row."""
           lim = f"LIMIT {int(limit)}" if limit else ""
           async with get_conn() as conn:
               cur = await conn.execute(
                   f"""SELECT co.siren, co.adresse, co.code_postal, co.ville
                       FROM companies co
                       LEFT JOIN companies_geom cg ON cg.siren = co.siren
                       WHERE co.workspace_id = %s
                         AND co.siren LIKE 'MAPS%%'
                         AND co.adresse IS NOT NULL
                         AND co.adresse <> ''
                         AND cg.siren IS NULL
                       ORDER BY co.siren {lim}""",
                   (workspace_id,),
               )
               return await cur.fetchall()

       def build_csv(rows) -> bytes:
           """Build a CSV with siren + q columns. Filter empty parts to
           avoid double commas (e.g. '12 rue de la Paix, , 75002 Paris')."""
           buf = io.StringIO()
           w = csv.writer(buf)
           w.writerow(["siren", "q"])
           for siren, addr, cp, ville in rows:
               full = ", ".join(p for p in (addr, cp, ville) if p)
               w.writerow([siren, full])
           return buf.getvalue().encode("utf-8")

       def post_chunk_to_ban(csv_bytes: bytes) -> list[dict]:
           """POST to BAN /search/csv/. Synchronous — curl_cffi has no
           async API. Wrap in asyncio.to_thread from the orchestrator
           if needed.

           DO NOT pass result_columns — we need the FULL default
           response (20 cols) including `result_postcode` and
           `result_status` for the dual-gate safety check below."""
           resp = curl_cffi.requests.post(
               BAN_CSV_URL,
               files={"data": ("addresses.csv", csv_bytes, "text/csv")},
               data={"columns": "q"},
               timeout=120,
           )
           resp.raise_for_status()
           return list(csv.DictReader(io.StringIO(resp.text)))

       def evaluate_ban_row(result: dict,
                            sirene_code_postal: str | None
                            ) -> tuple[str | None, str]:
           """Apply the locked dual-gate to a single BAN response row.

           Returns (quality, drop_reason). Exactly one of the two is
           non-empty:
             - (quality, "")  → INSERT with this geocode_quality
             - (None, reason) → DROP, log audit with this reason

           Reasons (stable strings — used in audit detail and QA
           queries):
             - "ban_no_match"      result_status != 'ok' or blank score
             - "postcode_mismatch" BAN city ≠ SIRENE city (CRITICAL)
             - "ban_low_score"     score < 0.5
             - "sirene_no_postcode" SIRENE has no postcode to verify
                                    against (we cannot safely accept)
           """
           # GATE 1 — status filter
           status = (result.get("result_status") or "").strip()
           score_str = (result.get("result_score") or "").strip()
           if status != "ok" or not score_str:
               return None, "ban_no_match"

           # GATE 2 — postcode verification (CRITICAL safety gate).
           # Must run BEFORE the score threshold, because BAN can
           # return score=0.97 for the wrong city when input lacks
           # the city component. See .ban_endpoint_report.md.
           sirene_pc = (sirene_code_postal or "").strip()
           if not sirene_pc:
               return None, "sirene_no_postcode"
           result_pc = (result.get("result_postcode") or "").strip()
           if result_pc != sirene_pc:
               return None, "postcode_mismatch"

           # GATE 3 — quality threshold
           try:
               score = float(score_str)
           except ValueError:
               return None, "ban_no_match"
           if score >= 0.8:
               return "bonne", ""
           if score >= BAN_MIN_SCORE:
               return "acceptable", ""
           return None, "ban_low_score"

   The orchestrator (`async def main`) iterates chunks, posts to BAN,
   parses each response row through `evaluate_ban_row`, INSERTs the
   ones that pass all three gates, audits the drops, then for each
   successfully geocoded entity invokes Step 2.6 replay (item 2).

   Per-row flow inside the orchestrator:

       sirene_pc = sirene_lookup[siren]["code_postal"]
       quality, drop_reason = evaluate_ban_row(result, sirene_pc)
       if drop_reason:
           drop_counts[drop_reason] += 1
           # Audit the drop so QA can verify the safety gates fire.
           await log_audit(
               conn,
               batch_id=BACKFILL_BATCH_ID,
               siren=siren,
               action='ban_backfill_dropped',
               result='dropped',
               detail=(f"reason={drop_reason} "
                       f"expected_pc={sirene_pc or '-'} "
                       f"got_pc={result.get('result_postcode') or '-'} "
                       f"score={result.get('result_score') or '-'}"),
               workspace_id=174,
           )
           continue
       # Insert (lng/lat, NOT lat/lng — BAN CSV order is x,y).
       lng = float(result["longitude"])
       lat = float(result["latitude"])
       await conn.execute(UPSERT_SQL, (siren, lat, lng,
                                       'ban_backfill', quality))

   UPSERT (Phase-3 specific — never overwrite Phase 1 maps_panel or
   Phase 2 sirene_geo):

       INSERT INTO companies_geom (siren, lat, lng, source, geocode_quality)
       VALUES (%s, %s, %s, 'ban_backfill', %s)
       ON CONFLICT (siren) DO NOTHING

   Note: `DO NOTHING` (not DO UPDATE) — defense-in-depth. Phase 1's
   maps_panel and Phase 2's sirene_geo rows must never be touched.

   IMPORTANT — column order: BAN CSV returns `longitude` BEFORE
   `latitude` (EPSG xy convention). Always parse by NAME from the
   DictReader, never by position. The `companies_geom` schema stores
   them as (lat, lng) — the orchestrator is responsible for the
   reorder.

   Telemetry (use logging.info; no structlog dependency assumed):
     log.info("backfill.ban_request_count n=%s", n)
     log.info("backfill.ban_score_distribution %s", quality_counts)
     log.info("backfill.dropped reason=ban_no_match n=%s",
              drop_counts['ban_no_match'])
     log.info("backfill.dropped reason=postcode_mismatch n=%s",
              drop_counts['postcode_mismatch'])
     log.info("backfill.dropped reason=ban_low_score n=%s",
              drop_counts['ban_low_score'])
     log.info("backfill.dropped reason=sirene_no_postcode n=%s",
              drop_counts['sirene_no_postcode'])
     log.info("backfill.step_2_6_recoveries n=%s", recoveries)
     log.info("backfill.completed targets=%s inserted=%s confirmed=%s",
              targets, inserted, confirmed)


2) RE-RUN STEP 2.6 SWEEP — same script
   ----------------------------------------------------------------------
   For each successfully geocoded MAPS entity (i.e. each row whose
   companies_geom row was just inserted with quality 'bonne' or
   'acceptable'), invoke Step 2.6 with conservative thresholds:

       async def replay_step_2_6(conn, siren: str,
                                 lat: float, lng: float,
                                 ban_score: float):
           # Skip already-confirmed or already-geo-matched rows.
           cur = await conn.execute(
               """SELECT denomination, enseigne,
                         link_method, link_confidence
                  FROM companies WHERE siren = %s""",
               (siren,),
           )
           co = await cur.fetchone()
           if co is None:
               return None
           denom, enseigne, link_method, link_conf = co
           if link_method == 'geo_proximity':
               return None
           if link_conf == 'confirmed':
               return None

           maps_name = (enseigne or denom or "").strip()
           if not maps_name:
               return None

           # CRITICAL: picked_nafs=None (no filter mode) — picker
           # context is lost historically. Conservative thresholds
           # require Phase 2 signature exposing the kwargs.
           cand = await _geo_proximity_match(
               conn, maps_name, lat, lng,
               picked_nafs=None,
               naf_division_whitelist=None,
               top_threshold=BACKFILL_TOP_THRESHOLD,
               dominance_threshold=BACKFILL_DOMINANCE_THRESHOLD,
           )
           return cand

   On a non-None candidate (Phase 2 already enforced the conservative
   thresholds, so any returned cand satisfies them):

       UPDATE companies
       SET    linked_siren    = %s,
              link_method     = 'geo_proximity',
              link_confidence = 'confirmed',
              naf_status      = NULL,                    -- BLOCKER 1 fix
              link_signals    = %s::jsonb
       WHERE  siren = %s

       link_signals payload:
       {
         "geo_proximity_backfill": true,
         "top_score": <cand.top_score>,
         "dominance": <cand.dominance>,
         "ban_score": <ban_score>,
         "thresholds": {"top": 0.95, "dominance": 0.20}
       }

   Then:
     - await _copy_sirene_reference_data(conn, maps_siren=siren,
                                         target_siren=cand.siren)
     - await log_audit(
           conn,
           batch_id=BACKFILL_BATCH_ID,
           siren=siren,
           action='auto_linked_geo_proximity_backfill',
           result='success',
           detail=f"target={cand.siren} top={cand.top_score:.3f} "
                  f"dom={cand.dominance:.3f} ban={ban_score:.3f}",
           workspace_id=174,
       )


3) NEW ADMIN ENDPOINT — fortress/api/routes/admin.py
   ----------------------------------------------------------------------
   POST /api/admin/backfill-geo

   Admin-only, using the existing _get_admin pattern at admin.py:26-34.
   Spawns the script as a subprocess (not import) so the long job is
   isolated and the API returns immediately.

   Behaviour:
     - Non-admin → 403 "Admin requis." (matches existing pattern in
       admin.py — NOT 401. The existing _get_admin returns 403.)
     - workspace_id != 174 → 400 with French message (Decision 7
       hard-gate).
     - Otherwise → spawn subprocess, return 200 + ok message.

       @router.post("/backfill-geo")
       async def backfill_geo(request: Request):
           admin = _get_admin(request)
           if not admin:
               return JSONResponse(status_code=403,
                                   content={"error": "Admin requis."})
           body = await request.json()
           ws = int(body.get("workspace_id", 0))
           limit = body.get("limit")
           if ws != 174:
               return JSONResponse(
                   status_code=400,
                   content={
                       "error": "Backfill restreint au workspace 174 "
                                "dans la V1. Le retrofit ws1 (Cindy) "
                                "sera lancé séparément.",
                   },
               )
           import subprocess
           args = ["python3", "-m", "scripts.backfill_ban_geo",
                   "--workspace", "174"]
           if limit:
               args += ["--limit", str(int(limit))]
           # cwd = repo root so `-m scripts.backfill_ban_geo` resolves.
           repo_root = os.path.dirname(__file__) + "/../../.."
           subprocess.Popen(args, cwd=repo_root)
           logger.info("admin.backfill_geo_spawned by=%s",
                       admin.username)
           return JSONResponse({
               "ok": True,
               "message": "Backfill lancé en arrière-plan.",
           })

   NOTE: `import subprocess` and `import os` are added inside the
   function body (or at module top — match the existing admin.py
   convention; admin.py:1-20 imports at top, so add them there).


4) UNIT TESTS — fortress/tests/test_ban_backfill.py (NEW)
   ----------------------------------------------------------------------
   Test file location: `fortress/tests/test_ban_backfill.py` —
   confirmed via existing test layout (pyproject.toml testpaths
   resolves cwd-relative; tests must live under the inner suite).

   Mock curl_cffi to simulate BAN responses. 12 tests:

   a) test_evaluate_ban_row_quality_thresholds
      result_status='ok', score 0.9 + matching pc → ('bonne', '');
      score 0.6 + matching pc → ('acceptable', '');
      score 0.3 + matching pc → (None, 'ban_low_score').
   b) test_evaluate_ban_row_drops_postcode_mismatch
      result_status='ok', score 0.97, result_postcode='95000',
      sirene_pc='80000' → (None, 'postcode_mismatch').
      This is the Cergy/Amiens regression case from
      .ban_endpoint_report.md. MUST drop even at score 0.97.
   c) test_evaluate_ban_row_drops_not_found
      result_status='not-found', score blank → (None, 'ban_no_match').
   d) test_evaluate_ban_row_drops_skipped
      result_status='skipped', score blank → (None, 'ban_no_match').
   e) test_evaluate_ban_row_drops_when_sirene_pc_missing
      result_status='ok', score 0.9, but sirene_code_postal=None
      → (None, 'sirene_no_postcode'). We refuse to accept a geocode
      we cannot verify.
   f) test_build_csv_includes_full_address
      addr='12 rue X', cp='75001', ville='Paris' →
      single row "MAPSXXX,12 rue X, 75001, Paris". The full
      address (incl. city) is mandatory — passing street alone
      causes the postcode-mismatch failure mode.
   g) test_build_csv_handles_missing_components
      cp=None → output "MAPSXXX,12 rue X, Paris" (no double comma).
      (BAN may still match, but evaluate_ban_row will then drop
      the row as 'sirene_no_postcode' — that's correct.)
   h) test_post_handles_ban_error_gracefully
      Mock curl_cffi.requests.post to raise → orchestrator logs +
      continues with the next chunk.
   i) test_post_does_not_pass_result_columns
      Spy on curl_cffi.requests.post; assert that the `data` kwarg
      is exactly `{"columns": "q"}` — no `result_columns` key.
      We need the FULL default response (20 cols) for the gates.
   j) test_replay_step_2_6_skips_already_linked
      MAPS entity with link_confidence='confirmed' → replay returns
      None without calling _geo_proximity_match.
   k) test_replay_step_2_6_passes_conservative_thresholds
      Spy on _geo_proximity_match; assert it was called with
      top_threshold=0.95, dominance_threshold=0.20,
      picked_nafs=None.
   l) test_admin_endpoint_rejects_non_174
      POST to /api/admin/backfill-geo with workspace_id=1 → 400
      with French error mentioning "Workspace 174" and "V1".
   m) test_admin_endpoint_requires_admin_role
      Non-admin session → 403 "Admin requis." (not 401 — matches
      existing pattern).


================================================================================
TOUCHED FILES
================================================================================
  - fortress/scripts/backfill_ban_geo.py        (NEW)
  - fortress/tests/test_ban_backfill.py         (NEW — 13 tests)
  - fortress/api/routes/admin.py                (NEW endpoint
                                                  /api/admin/backfill-geo)

NOT TOUCHED:
  - discovery.py (Phase 2 owns _geo_proximity_match)
  - api/main.py
  - jobs.py / export.py / contacts_list.py
  - any frontend file

================================================================================
RULES
================================================================================
  - ws174 ONLY (Decision 7). Endpoint hard-gates on workspace_id.
  - Conservative thresholds 0.95 / 0.20 vs fresh batches 0.85 / 0.15.
  - DO NOT call _compute_naf_status from the backfill path. naf_status
    is set to NULL directly on accept (BLOCKER 1).
  - link_signals on backfill auto-confirms MUST include
    `geo_proximity_backfill: true` for sample-audit visibility.
  - DUAL-GATE SAFETY (Apr 26 — DO NOT SKIP):
      * GATE 1: drop if `result_status != 'ok'` or score is blank
        (handles `not-found` and `skipped`).
      * GATE 2: drop if `result_postcode != sirene.code_postal`
        (audit reason `postcode_mismatch`). High BAN scores can lie
        when the input lacks city — this gate is the ONLY thing
        that catches it.
      * GATE 3: drop if `result_score < 0.5` (audit reason
        `ban_low_score`).
      Drops are audited via `log_audit` with action
      'ban_backfill_dropped' and detail carrying the reason.
  - DO NOT pass `result_columns` to BAN. Default response (20 cols)
    is required so we have `result_postcode` and `result_status`.
  - Input `q` MUST concat (adresse, code_postal, ville). Passing
    street alone causes the postcode-mismatch false-positive
    documented in `.ban_endpoint_report.md`.
  - BAN CSV column order: `longitude` PRECEDES `latitude` (EPSG xy).
    Always parse by NAME via DictReader — never positionally.
  - BAN result_score < 0.5 → DROP. Do not INSERT into companies_geom.
    Idempotency comes from this rule: re-runs naturally retry rows
    not yet inserted.
  - BAN is free / rate-limited at 50 req/s — chunk 5000 + 1s delay.
  - log_audit batch_id=BACKFILL_BATCH_ID (synthetic constant defined
    once at script top; reused across the run).
  - Audit action: 'auto_linked_geo_proximity_backfill' (distinct from
    fresh-batch 'auto_linked_geo_proximity').
  - Non-admin endpoint response: 403 "Admin requis." (not 401).
  - DO NOT commit or push.

================================================================================
ACCEPTANCE CRITERIA
================================================================================
  [1] Pre-code checkpoint COMPLETED Apr 26 — see
      `.ban_endpoint_report.md`. Endpoint contract + quality mapping
      locked. Executor proceeds directly to implementation.
  [2] Unit tests pass (13 new + full regression).
  [3] Endpoint rejects non-ws174 (curl + check 400 + French msg).
  [4] Endpoint rejects non-admin (curl + check 403 + "Admin requis.").
  [5] Script enumerates ws174 historical MAPS entities (count > 0
      pre-run, 0 after on a re-run of all geocoded ones — idempotency).
  [6] BAN match rate ≥ 70%:
        new_ban_rows / target_count ≥ 0.7
      (Some addresses don't geocode — old MAPS entities had
      malformed Maps addresses. 70% is the floor.)
  [7] Step 2.6 replay produces ≥1 new auto_linked_geo_proximity_backfill
      row in batch_log:
        SELECT COUNT(*) FROM batch_log
        WHERE action = 'auto_linked_geo_proximity_backfill'
          AND workspace_id = 174;
  [8] ws174 7-day confirm rate moves UP further (target +3pp on
      this single phase, additive to Phase 2's +5pp).
  [9] No `source='ban_backfill'` row exists for any siren whose
      MAPS row belongs to a workspace other than 174:
        SELECT COUNT(*) FROM companies_geom cg
        JOIN companies co ON co.siren = cg.siren
        WHERE cg.source = 'ban_backfill'
          AND (co.workspace_id IS NULL OR co.workspace_id <> 174);
      MUST equal 0. (Phase 2's `sirene_geo` rows ARE global — this
      check ONLY scopes ban_backfill rows.)

EXECUTOR BRIEF — END PHASE 3
```

```
QA TEST PLAN — START PHASE 3
================================================================================
PHASE 3 QA — BAN backfill + Step 2.6 sweep (LOCAL ONLY — Workspace 174 only)
================================================================================

PRE-REQUISITE
  Phase 1 + Phase 2 must already be in production locally.
  - companies_geom table exists (Phase 1).
  - sirene_geo bulk import is loaded (Phase 2, ~17M rows).
  - _geo_proximity_match exposes top_threshold + dominance_threshold
    kwargs (Phase 2 — verify by `grep "top_threshold" discovery.py`).
  - Step 2.6 wired into _match_to_sirene (Phase 2).

  LOCAL TESTING ONLY. No Render. ws174 only.

================================================================================
SECTION 1 — AUTOMATED CHECKS (QA agent runs these in terminal)
================================================================================

CHECK 3.1 — Unit tests (LOCAL)
  cd /Users/alancohen/Project\ Alan\ copy/fortress
  python3 -m pytest fortress/tests/test_ban_backfill.py -v
  Expect: 13 passed.
  python3 -m pytest fortress/tests/ -q
  Expect: 0 regressions.

CHECK 3.2 — Endpoint exists + rejects non-174
  Login as alan (admin) — LOCAL only:
    curl -s -c /tmp/p3cookies.txt -X POST http://localhost:8080/api/auth/login \
         -H "Content-Type: application/json" \
         -d '{"username":"alan","password":"03052000"}'
  Reject ws1:
    curl -s -b /tmp/p3cookies.txt -X POST http://localhost:8080/api/admin/backfill-geo \
         -H "Content-Type: application/json" \
         -d '{"workspace_id":1}' | jq .
  PASS: 400 response, French error mentions "Workspace 174" and "V1".

CHECK 3.3 — Endpoint rejects non-admin
  Login head.test → POST /api/admin/backfill-geo → expect 403 +
  "Admin requis."
  PASS: 403, "Admin requis." (NOT 401 — matches existing
  admin.py:42 pattern).

CHECK 3.4 — Pre-run target count
  Run on local DB (psql with $DATABASE_URL):
    WITH targets AS (
        SELECT co.siren FROM companies co
        LEFT JOIN companies_geom cg ON cg.siren = co.siren
        WHERE co.workspace_id = 174
          AND co.siren LIKE 'MAPS%'
          AND co.adresse IS NOT NULL AND co.adresse <> ''
          AND cg.siren IS NULL
    )
    SELECT COUNT(*) AS targets_before FROM targets;
  Record the number — call it N_PRE.
  PASS: N_PRE > 0 (otherwise nothing to test).

CHECK 3.5 — Trigger backfill (admin)
  curl -s -b /tmp/p3cookies.txt -X POST http://localhost:8080/api/admin/backfill-geo \
       -H "Content-Type: application/json" \
       -d '{"workspace_id":174}'
  Expect: 200, "Backfill lancé en arrière-plan."

  Wait for the subprocess to finish. Tail the local console / log
  for the "backfill.completed" line. For ~hundreds of entities,
  expect < 5 minutes. If it exceeds 10 minutes, investigate (BAN
  rate-limit? sirene_geo missing?).

CHECK 3.6 — BAN match rate ≥ 70%
    SELECT COUNT(*) FROM companies_geom
    WHERE source = 'ban_backfill';
  Call this N_BAN.
  PASS: N_BAN / N_PRE ≥ 0.70.

CHECK 3.7 — Quality distribution (no nulls below threshold)
    SELECT geocode_quality, COUNT(*) FROM companies_geom
    WHERE source = 'ban_backfill' GROUP BY 1 ORDER BY 1;
  PASS: only 'bonne' and 'acceptable' rows. No NULL quality (rows
  with BAN score <0.5 must be dropped, not inserted).

CHECK 3.7a — Safety-gate drops were audited (NEW Apr 26)
  Verify the dual-gate fired and audited each drop reason. The
  safety gates are useless if they silently drop without trace.
    SELECT
        SUBSTRING(detail FROM 'reason=([a-z_]+)') AS reason,
        COUNT(*) AS n
    FROM batch_log
    WHERE batch_id LIKE 'BACKFILL_BAN_GEO_%'
      AND action = 'ban_backfill_dropped'
      AND workspace_id = 174
    GROUP BY 1
    ORDER BY n DESC;
  Expected reason buckets:
    - 'ban_no_match'       — BAN couldn't geocode (any > 0 OK).
    - 'postcode_mismatch'  — CRITICAL gate fired; report count.
    - 'ban_low_score'      — score < 0.5 dropped.
    - 'sirene_no_postcode' — SIRENE missing postcode (low expected).
  PASS: ≥1 row of action='ban_backfill_dropped' exists. The
  postcode_mismatch bucket may legitimately be 0 if no high-score-
  wrong-city case occurred in this run, but the audit infrastructure
  must be present (test b proves the code path).
  REPORT in QA output: the count for each reason. The
  postcode_mismatch number is the most important — it's the live
  telemetry on whether our Cergy/Amiens-style false positives are
  being caught in production data.

CHECK 3.7b — result_status filter is wired (NEW Apr 26)
  Verify no row with non-'ok' status was inserted. There is no
  direct DB column for status (it's BAN-side only), so we sanity-
  check via the audit log: every drop with reason 'ban_no_match'
  corresponds to status='not-found'/'skipped'/blank-score, and the
  count of inserted rows + dropped rows = total BAN responses.
    -- Inserted (passed all gates)
    WITH inserted AS (
        SELECT COUNT(*) AS n FROM companies_geom
        WHERE source = 'ban_backfill'
    ),
    -- Dropped (any reason)
    dropped AS (
        SELECT COUNT(*) AS n FROM batch_log
        WHERE batch_id LIKE 'BACKFILL_BAN_GEO_%'
          AND action = 'ban_backfill_dropped'
          AND workspace_id = 174
    )
    SELECT (SELECT n FROM inserted) AS inserted,
           (SELECT n FROM dropped)  AS dropped,
           (SELECT n FROM inserted) + (SELECT n FROM dropped) AS total;
  PASS: `total` equals N_PRE from CHECK 3.4 (every target row was
  either inserted or audited as dropped — none silently lost).

CHECK 3.8 — Step 2.6 replay produced new confirms
    SELECT COUNT(*) FROM batch_log
    WHERE action = 'auto_linked_geo_proximity_backfill'
      AND workspace_id = 174
      AND batch_id LIKE 'BACKFILL_BAN_GEO_%';
  PASS: ≥ 1.
  Cross-check: those SIRENs now have link_method='geo_proximity'
  + link_confidence='confirmed' on companies, AND naf_status IS NULL:
    SELECT siren, link_method, link_confidence, naf_status
    FROM companies
    WHERE siren IN (
        SELECT siren FROM batch_log
        WHERE action = 'auto_linked_geo_proximity_backfill'
        LIMIT 5
    );
  PASS: link_method='geo_proximity', link_confidence='confirmed',
  naf_status IS NULL (BLOCKER 1 fix verified).

CHECK 3.9 — link_signals tagged geo_proximity_backfill:true
    SELECT siren, link_signals FROM companies
    WHERE link_method = 'geo_proximity'
      AND link_confidence = 'confirmed'
      AND workspace_id = 174
      AND link_signals @> '{"geo_proximity_backfill": true}'::jsonb;
  PASS: ≥1 row, and `link_signals->>'top_score'` is ≥ 0.95
  (conservative threshold respected). Fresh-batch rows (Phase 2)
  DO NOT carry the geo_proximity_backfill flag.

CHECK 3.10 — Idempotency (re-run produces zero new rows)
  curl … /api/admin/backfill-geo … (second time)
  Wait for completion, then:
    SELECT COUNT(*) FROM companies_geom WHERE source='ban_backfill';
  Call this N_BAN_2.
  PASS: N_BAN_2 == N_BAN (no double-inserts; ON CONFLICT DO NOTHING
  + the dropped-low-score rule means re-running only attempts rows
  that have not yet been inserted).
  Re-run script logs "backfill.targets_remaining n=0" or similar.

CHECK 3.11 — ws1 isolation respected (Decision 7)
  Tightened phrasing: only `source='ban_backfill'` rows are
  ws174-restricted. `source='sirene_geo'` rows from Phase 2 are
  global (they are real-SIREN rows, not workspace-tagged). So this
  check ONLY counts ban_backfill rows joined to non-174 MAPS entities:
    SELECT COUNT(*) FROM companies_geom cg
    JOIN companies co ON co.siren = cg.siren
    WHERE cg.source = 'ban_backfill'
      AND (co.workspace_id IS NULL OR co.workspace_id <> 174);
  PASS: 0. The endpoint hard-gate prevents ban_backfill leaking
  outside ws174.

CHECK 3.12 — MANDATORY 99%-goal stats query (per CLAUDE.md)
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
  Report: "99% GOAL TRACKING: ws174 confirm rate now X.X%
  (previous QA: Y.Y% [Phase 2 result], delta +Z.Zpp).
  Gap to 99%: N.Npp."
  PASS condition: delta ≥ +3pp on this phase.
  If delta below threshold: investigate BAN match rate / Step 2.6
  thresholds / picker context loss.

================================================================================
SECTION 2 — MANUAL BROWSER CHECKS (QA agent runs via Playwright)
================================================================================

  M.1 — Login alan (admin) — LOCAL only.
        Step: navigate to http://localhost:8080/#/login
        Step: fill username='alan', password='03052000', click submit.
        Step: wait for dashboard route #/.
        Verify: dashboard renders without console error.
        PASS = no JS error in console, dashboard cards visible.

  M.2 — Liens en attente tab smoke.
        Step: from dashboard, click the "Liens en attente" tab.
        Step: filter or scroll for any row with link_method
              containing 'geo_proximity'.
        Step: click "Rejeter" on one such row.
        Verify: row disappears or status updates without console
        error.
        PASS = no JS error, UI updates.
        (Don't approve any row — this is read-only smoke.)

  M.3 — Spot-check a backfilled confirm.
        Step: pick one siren from CHECK 3.8 results.
        Step: navigate to /#/company/<siren>.
        Verify: SIREN data populated (legal name, NAF, address)
        — proves _copy_sirene_reference_data ran.
        Verify: activity log on the page lists the
        'auto_linked_geo_proximity_backfill' event.
        Verify: link badge or signals panel shows
        geo_proximity_backfill flag.
        PASS = all three visible.

  M.4 — Cindy ws1 visual isolation.
        Step: log out (click profile menu → logout).
        Step: log in as olivierhaddad / 1357o.
        Step: navigate to dashboard.
        Step: spot-check 3 ws1 MAPS entities (any from the existing
        Cindy data).
        Verify: NONE of them have link_method='geo_proximity' as a
        new addition.
        PASS = no new geo_proximity rows in ws1 since Phase 3 ran.
        Read-only check — DO NOT modify ws1 data.

================================================================================
SECTION 3 — SCORECARD
================================================================================
  Step  | What to Check                                    | Expected | Actual | Pass/Fail
  3.1   | Tests + regression                               | 13 + 0 fail
  3.2   | Endpoint rejects non-174                         | 400 + FR msg
  3.3   | Endpoint rejects non-admin                       | 403 + "Admin requis."
  3.4   | Pre-run target count                             | N_PRE > 0
  3.5   | Backfill triggers + completes                    | 200 + "lancé"
  3.6   | BAN match rate ≥70%                              | N_BAN/N_PRE ≥ 0.70
  3.7   | Quality bonne/acceptable, no nulls               | 2 buckets only
  3.7a  | Safety-gate drops audited (postcode_mismatch etc)| ≥1 'ban_backfill_dropped' row
  3.7b  | inserted+dropped = N_PRE (no silent losses)      | total = N_PRE
  3.8   | ≥1 backfill auto-confirm + naf_status NULL       | ≥1, NULL
  3.9   | link_signals tagged + top_score ≥0.95            | ≥1
  3.10  | Idempotency (no double-inserts)                  | N_BAN_2 == N_BAN
  3.11  | ban_backfill rows scoped to ws174                | 0 outside-ws174
  3.12  | 99% goal-tracking confirm rate                   | delta ≥ +3pp
  M.1   | Admin login + dashboard                          | no error
  M.2   | Liens en attente UI smoke                        | reject works
  M.3   | Backfill confirm spot-check                      | SIRENE+activity
  M.4   | ws1 visual isolation                             | unchanged

================================================================================
SECTION 4 — PASS / FAIL CRITERIA
================================================================================
  PASS = all 14 automated + all 4 manual.
  FAIL = any one. Report with SQL output + Playwright artefacts
  (screenshot/trace) attached.
  Stop and escalate if BAN endpoint params changed (the contract is
  locked Apr 26 — any drift from `.ban_endpoint_report.md` is a
  blocker), if Phase 2 thresholds aren't kwargs, if the 99%-goal
  delta is negative (regression), or if 3.7a returns ZERO drop
  audits across all reasons (means the gate code didn't run at all).

QA TEST PLAN — END PHASE 3
```

---

## Risks / open questions for Alan

1. **Phase 2 signature dependency.** This Phase 3 brief assumes Phase 2 exposes `top_threshold` and `dominance_threshold` as keyword arguments on `_geo_proximity_match`. If Phase 2 hard-codes the constants in the function body, Phase 3 cannot tighten them and the auto-confirm rule collapses to fresh-batch behaviour. The Phase 2 brief must reflect this dependency.
2. **BAN response columns — LOCKED Apr 26.** Endpoint contract was tested and recorded in `.ban_endpoint_report.md`. POST shape is `-F data=@file -F columns=q` (no `result_columns`). Response is the default 20-column schema. `latitude`/`longitude` (in that named order, but `longitude` precedes `latitude` positionally — EPSG xy). `result_postcode` and `result_status` are mandatory parser inputs. Resolved.
3. **Postcode-mismatch frequency in real ws174 data.** Test b proves the gate is wired. The CHECK 3.7a query gives the live count of how often BAN actually returns the wrong city for ws174 historical addresses. If the count is unexpectedly high (e.g. >20% of dropped rows), that's a signal that the address builder needs more aggressive normalization (e.g. `numero_voie type_voie libelle_voie` from SIRENE columns separately rather than the prepacked `adresse` field). Investigate before ws1 retrofit.
4. **Picker context truly lost.** Some historical batches may have logged picker NAFs in `batch_log.detail` — if so, a richer retrofit could re-use them. Out of scope for V1; tracked as an open follow-up.
5. **70% BAN match floor.** Old MAPS-only addresses can be malformed. If acceptance criterion [6] fails, we tune the address builder (e.g. drop `cp` when it's clearly invalid) rather than relax the floor. Note: with the new postcode-verification gate, "match rate" now means "passed all three gates" — the floor may need to be re-baselined after the first run.
6. **ws1 retrofit timing.** Per Decision 7, ws1 is deferred to a separate session AFTER sample-audit. The endpoint hard-rejects ws1 — if Alan wants to override, that's a follow-up brief, not a relax-the-gate edit.
