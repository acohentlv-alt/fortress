## PHASE 2 — INSEE SIRENE-Geo bulk import + Step 2.6 matcher (REVISED)

**What this phase does (plain English):** We download the French government's bulk file of every business with its precise GPS coordinates (~37.4 million rows; ~35.3M after dropping the random-in-commune rows), load it into our database, then teach the matcher one new trick: when all earlier steps have missed, look at every official business sitting within 400 metres of the Maps pin and pick the one whose name matches best — but only if the sector matches too. Sector mismatches go to a pending pile a human can review. Expected lift on this phase alone: roughly +5 percentage points toward the 99% confirm-rate goal. (Phase 3 brings the rest by also geocoding our existing historical entities.)

**Schema confirmed (pre-code checkpoint complete, Apr 26):** INSEE Parquet already provides WGS84 lat/lng — `y_latitude` and `x_longitude` Float32 columns. No reprojection needed. No pyproj dependency. See `.insee_schema_report.md` for the full schema dump and column distribution.

**Why this revision:** Reviewer caught three real bugs and several smaller cite errors in the prior draft. The big ones were (1) the empty-picker gate would have silently auto-confirmed `geo_proximity` on no-filter searches with zero secondary signals, (2) two frontend files needed `geo_proximity` added to their strong-method sets and reason labels — without that the new method would render with a generic fallback tooltip, (3) the helper signature lacked the threshold knobs Phase 3 needs to override.

```
EXECUTOR BRIEF — START PHASE 2
================================================================================
PHASE 2 — INSEE SIRENE-Geo bulk import + Step 2.6 proximity matcher
================================================================================

GOAL
  (a) Bulk-load all of INSEE's geocoded SIRENE establishments (~37.4M
      rows; ~35.3M after dropping qualite_xy='33') from the official
      Parquet into the `companies_geom` table that Phase 1 created.
      Lat/lng are already WGS84 in the Parquet — no reprojection step.
  (b) Add a new matcher Step 2.6 in fortress/discovery.py — fires when a
      MAPS entity has Maps panel coords but Steps 0–2.5 all missed. Finds
      SIRENE candidates within 400m, ranks by name similarity, gates
      auto-confirm on the shared NAF gate (with empty-picker treated as
      "needs ≥1 signal" — see WORK ITEM 5).
  (c) New audit action `auto_linked_geo_proximity` joins
      `auto_linked_verified` in the scoreboard (mapped to `naf_exact`).
      CSV export inherits existing rules — verified passes, mismatch
      (pending) excluded.

CROSS-PHASE DEPENDENCY
  - HARD requires Phase 1 in production: Phase 2's import script
    INSERTs into companies_geom; the matcher SELECTs from it. If
    Phase 1 has not shipped, Phase 2 crashes with relation-not-exist.
  - Phase 3 builds on Phase 2: the BAN backfill re-runs Step 2.6 on
    historical entities. Without Phase 2's helper, Phase 3 has nothing
    to invoke. Phase 3 will OVERRIDE the helper's threshold knobs
    (default 0.85/0.15 in v1; Phase 3 tightens to 0.95/0.20 for
    no-coords-at-discovery historical entities).

================================================================================
PRE-CODE CHECKPOINT (COMPLETED — Apr 26)
================================================================================
  Schema-inspection completed Apr 26. Findings recorded in
  `/Users/alancohen/Project Alan copy/.insee_schema_report.md`. Column
  mappings, quality enum, file size, and row count are LOCKED — no
  further pre-code investigation required before writing the import
  script.

  LOCKED FACTS (from .insee_schema_report.md):

    Source URL (env-configurable, default below):
      INSEE_GEO_URL=https://object.files.data.gouv.fr/data-pipeline-open/siren/geoloc/GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.parquet

    File size:    818.9 MB (859,041,792 bytes)
    Row count:    37,380,068 (~37.38M établissements)
    Column count: 19 columns
    Cadence:      INSEE replaces the file at the same URL every month.
                  Older versions are NOT kept online. Capture the
                  download date in import metadata so we can detect
                  staleness on next refresh.

    Column mappings to companies_geom:
      siret             (String, 14 chars) → derive siren = LEFT(siret, 9)
                        (no `siren` column exists in this Parquet)
      y_latitude        (Float32, WGS84 deg) → companies_geom.lat (DOUBLE PRECISION)
      x_longitude       (Float32, WGS84 deg) → companies_geom.lng (DOUBLE PRECISION)
      qualite_xy        (String, 2 chars)    → companies_geom.geocode_quality (TEXT)
                        (Phase 1 schema uses TEXT; we store the raw 2-char code.)

    qualite_xy enum (5 distinct values, with row counts):
      '11' — 29,806,351  (80%)  Voie Sûre, Numéro trouvé           — KEEP
      '12' —  4,274,991  (11%)  Voie Sûre, Position aléatoire      — KEEP
      '21' —    658,128  (2%)   Voie probable, Numéro trouvé       — KEEP
      '22' —    600,776  (2%)   Voie probable, Position aléatoire  — KEEP
      '33' —  2,039,822  (5%)   Voie inconnue, random in commune   — DROP

    KEY: lat/lng are ALREADY WGS84 in the Parquet (no reprojection
    needed). The `x` / `y` Float32 columns are Lambert-93 raw coords —
    ignore them. Outre-mer establishments use different source CRS
    (epsg ∈ {2154, 5490, 2972, 2975}) but `y_latitude`/`x_longitude`
    are uniform WGS84 across the whole file.

  RUNTIME ASSERTION REQUIRED. The import script must declare an
  `EXPECTED_SCHEMA` dict at module-level and assert
  `pl.scan_parquet(path).collect_schema() == EXPECTED_SCHEMA` before
  doing any work. This guards every monthly refresh against silent
  INSEE column renames. See WORK ITEM 2 for the exact snippet.

================================================================================
WORK ITEMS
================================================================================

1) DEPENDENCIES — fortress/requirements.txt
   ----------------------------------------------------------------------
   No new packages. polars and pyarrow are already present in
   requirements.txt (verified). The INSEE Parquet ships WGS84 lat/lng
   directly (`y_latitude`, `x_longitude` Float32) so no reprojection
   library (pyproj, etc.) is needed.

2) NEW SCRIPT — fortress/scripts/import_sirene_geo.py
   ----------------------------------------------------------------------
   Standalone CLI. The scripts directory is fortress/scripts/ (the
   OUTER one — same level as fortress/api/, fortress/utils/), NOT a
   nested fortress/fortress/scripts/. CLI is invoked from inside the
   fortress/ working dir as a module:

       cd fortress
       python -m scripts.import_sirene_geo --parquet /tmp/sirene_geo.parquet

   The Parquet must be pre-downloaded by the executor. Default URL is
   exposed as an env var so monthly refreshes don't require a code
   change:

       INSEE_GEO_URL (default):
         https://object.files.data.gouv.fr/data-pipeline-open/siren/geoloc/GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.parquet

   Skeleton:

       """Bulk-import INSEE geocoded SIRENE into companies_geom.
       Run once per monthly INSEE refresh. Idempotent — UPSERT
       semantics, never overwrites maps_panel or ban_backfill rows.

       Source schema is locked against EXPECTED_SCHEMA below; an
       assertion at the top of the run aborts on any drift in the
       monthly refresh.
       """

       import argparse, asyncio, os, sys, time
       import polars as pl
       from fortress.api.db import init_pool, close_pool, get_conn

       INSEE_GEO_URL = os.environ.get(
           "INSEE_GEO_URL",
           "https://object.files.data.gouv.fr/data-pipeline-open/"
           "siren/geoloc/"
           "GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.parquet",
       )

       _BATCH_SIZE = 50_000  # rows per INSERT batch

       # Locked against the Apr 26 schema dump (see .insee_schema_report.md).
       # Assert on every run — guards against silent INSEE column renames.
       EXPECTED_SCHEMA = {
           "siret": pl.String,
           "x": pl.Float32,
           "y": pl.Float32,
           "qualite_xy": pl.String,
           "epsg": pl.String,
           "plg_qp24": pl.String,
           "plg_iris": pl.String,
           "plg_zus": pl.String,
           "plg_qp15": pl.String,
           "plg_qva": pl.String,
           "plg_code_commune": pl.String,
           "distance_precision": pl.Float32,
           "qualite_qp24": pl.String,
           "qualite_iris": pl.String,
           "qualite_zus": pl.String,
           "qualite_qp15": pl.String,
           "qualite_qva": pl.String,
           "y_latitude": pl.Float32,
           "x_longitude": pl.Float32,
       }

       async def main(parquet_path: str, dry_run: bool = False):
           await init_pool()

           # 1. Schema-drift guard. Abort if INSEE renamed/added/removed cols.
           lf = pl.scan_parquet(parquet_path)
           actual_schema = dict(lf.collect_schema())
           if actual_schema != EXPECTED_SCHEMA:
               sys.exit(
                   "INSEE Parquet schema drift detected — refusing to import.\n"
                   f"Expected: {EXPECTED_SCHEMA}\n"
                   f"Actual:   {actual_schema}\n"
                   "Update EXPECTED_SCHEMA + brief if INSEE has changed columns."
               )

           # 2. Select + filter via lazy frame (streaming to keep RAM <1 GB).
           # qualite_xy='33' (random within commune) is dropped; everything
           # else (11/12/21/22) is kept — even '22' anchors to a real street.
           df = (
               lf.select([
                   pl.col("siret").str.slice(0, 9).alias("siren"),
                   pl.col("y_latitude").cast(pl.Float64).alias("lat"),
                   pl.col("x_longitude").cast(pl.Float64).alias("lng"),
                   pl.col("qualite_xy").alias("geocode_quality"),
               ])
               .filter(
                   (pl.col("geocode_quality") != "33")
                   & pl.col("lat").is_not_null()
                   & pl.col("lng").is_not_null()
               )
               # France WGS84 paranoia bounds (covers métropole + outre-mer).
               .filter(
                   pl.col("lat").is_between(-22.0, 51.5)
                   & pl.col("lng").is_between(-62.0, 56.0)
               )
               .collect(streaming=True)
           )

           # 3. UPSERT in batches of 50k. Commit-per-batch — never one
           #    transaction for all 35M rows (connection timeouts are real).
           # ...

   Critical UPSERT SQL (use this verbatim):

       INSERT INTO companies_geom
           (siren, lat, lng, source, geocode_quality)
       VALUES (%s, %s, %s, 'sirene_geo', %s)
       ON CONFLICT (siren) DO UPDATE SET
           lat = EXCLUDED.lat,
           lng = EXCLUDED.lng,
           geocode_quality = EXCLUDED.geocode_quality,
           updated_at = NOW()
       WHERE companies_geom.source = 'sirene_geo'

   The `WHERE companies_geom.source = 'sirene_geo'` clause is the
   "never overwrite Maps or BAN" guard.

   Quality drop policy (locked from .insee_schema_report.md):
     '11' (numéro trouvé, voie sûre)        -> KEEP, geocode_quality='11'
     '12' (random-in-street, voie sûre)     -> KEEP, geocode_quality='12'
     '21' (numéro trouvé, voie probable)    -> KEEP, geocode_quality='21'
     '22' (random-in-street, voie probable) -> KEEP, geocode_quality='22'
     '33' (random-in-commune)               -> DROP entirely (~2.04M rows)
     anything else                          -> EXPECTED_SCHEMA assertion
                                                already aborted; not reached.

   Rationale: Alan's prior policy was "drop mauvaise + non géolocalisé".
   The actual INSEE enum is the 5-code grid above (no 'mauvaise' label).
   Closest equivalent is dropping only '33' (random-in-commune is the
   only value where the position has no street anchor at all). Even
   '22' (probable street, random spot in it) keeps the établissement
   close to a real candidate street and is safe for 400m proximity
   matching. Per-quality confidence tiering (e.g. higher threshold for
   '12'/'22') is a future Phase 2.5 follow-up — not in scope for v1.

   Performance target: ~35.3M rows in ~15-20 min with 50k batches and
   commit-per-batch. Do NOT use a single transaction for all 35M rows
   — connection timeouts are real. After ingestion completes, run
   `ANALYZE companies_geom;` so the planner has fresh stats for the
   bounding-box queries.

3) NEW MATCHER HELPER — fortress/discovery.py
   ----------------------------------------------------------------------
   Add module-level constants near _STRONG_METHODS area (line 576 in
   the current file):

       # ── Phase 2 — Geo proximity matcher (Step 2.6) ────────────────
       # 400m radius (single global default — no per-dept variation in v1).
       # Lat/lng deltas at French latitudes:
       #   1° lat  ≈ 111km                  -> 400m ≈ 0.0036°
       #   1° lng  ≈ 111km × cos(46°N)≈77km -> 400m ≈ 0.0050°
       # Across France's lat range (42°N–51°N) the lng delta of 0.0050°
       # corresponds to roughly 349m–419m of east-west distance —
       # acceptable variance for v1; can be made dynamic later.
       _GEO_RADIUS_M = 400
       _GEO_LAT_DELTA = 0.0036
       _GEO_LNG_DELTA = 0.0050
       # Default auto-confirm thresholds (Phase 2 v1).
       # Phase 3 will OVERRIDE these via kwargs when sweeping
       # ban_backfilled historical entities (tighter: 0.95 / 0.20).
       _GEO_CONFIRM_TOP_SCORE = 0.85
       _GEO_CONFIRM_DOMINANCE = 0.15

   Add `geo_proximity` to _STRONG_METHODS frozenset:

       _STRONG_METHODS = frozenset({
           "inpi", "siren_website", "enseigne", "phone", "address",
           "inpi_fuzzy_agree",
           "inpi_mentions_legales",
           "chain",
           "gemini_judge",
           "cp_name_disamb",
           "geo_proximity",   # NEW — Phase 2 (TOP 1)
       })

   Add new helper `_geo_proximity_match` near the other step helpers
   (after _cp_name_disamb_match, around line 800). NOTE: signature
   accepts threshold overrides as keyword-only args — Phase 3 will
   call with tighter values.

       async def _geo_proximity_match(
           conn: Any,
           maps_name: str,
           maps_lat: float,
           maps_lng: float,
           picked_nafs: list[str] | None,
           naf_division_whitelist: list[str] | None,
           *,
           top_threshold: float = _GEO_CONFIRM_TOP_SCORE,
           dominance_threshold: float = _GEO_CONFIRM_DOMINANCE,
       ) -> dict | None:
           """Step 2.6: 400m proximity-restricted name disambiguation.

           Bounding box derived from _GEO_LAT_DELTA / _GEO_LNG_DELTA.
           Joins companies_geom (sirene_geo + ban_backfill rows) to
           companies for name/enseigne/naf/cp.

           Threshold kwargs (top_threshold / dominance_threshold):
             - Default 0.85 / 0.15 = Phase 2 v1 (Maps panel coords,
               relatively trustworthy).
             - Phase 3 callers override with 0.95 / 0.20 for BAN-
               geocoded historical entities (less trustworthy).

           Returns a candidate dict matching Step 2/3/4/5 shape:
             {"siren": str, "score": float, "method": "geo_proximity",
              "geo_proximity_distance_m": int,
              "geo_proximity_top_score": float,
              "geo_proximity_2nd_score": float,
              "geo_proximity_pool_size": int}
           or None when:
             - bounding box is empty (no candidates within 400m)
             - top score < top_threshold
             - top score - 2nd score < dominance_threshold
               (forces ambiguous pools to fail this step rather than
                produce a low-confidence pending — better to let later
                steps try.)

           NAF filtering: this helper does NOT pre-filter by picked_nafs
           inside the SQL. The bounding box is the discriminator.
           naf_status is computed in the caller (_match_to_sirene's
           shared decision block in discovery.py:2688-2698) using the
           candidate's matched_naf vs picker.
           """
           lat_min = maps_lat - _GEO_LAT_DELTA
           lat_max = maps_lat + _GEO_LAT_DELTA
           lng_min = maps_lng - _GEO_LNG_DELTA
           lng_max = maps_lng + _GEO_LNG_DELTA

           rows = await (await conn.execute(
               """
               SELECT cg.siren,
                      cg.lat, cg.lng, cg.geocode_quality,
                      co.denomination, co.enseigne, co.naf_code,
                      co.code_postal, co.adresse, co.statut
               FROM companies_geom cg
               JOIN companies co ON co.siren = cg.siren
               WHERE cg.source IN ('sirene_geo', 'ban_backfill')
                 AND cg.lat BETWEEN %s AND %s
                 AND cg.lng BETWEEN %s AND %s
                 AND co.statut = 'A'
                 AND co.siren NOT LIKE 'MAPS%%'
               """,
               (lat_min, lat_max, lng_min, lng_max),
           )).fetchall()

           if not rows:
               return None

           scored = []
           for row in rows:
               siren_, lat_, lng_, quality, denom, enseigne_, naf_, cp_, addr_, _statut = row
               best_name = max(
                   _name_match_score(maps_name, denom or ""),
                   _name_match_score(maps_name, enseigne_ or ""),
               )
               dist_m = _haversine_m(maps_lat, maps_lng, float(lat_), float(lng_))
               scored.append({
                   "siren": siren_, "score": best_name, "dist_m": int(dist_m),
                   "denom": denom, "enseigne": enseigne_, "naf": naf_, "cp": cp_,
                   "quality": quality,
               })

           scored.sort(key=lambda r: r["score"], reverse=True)
           top = scored[0]
           second_score = scored[1]["score"] if len(scored) >= 2 else 0.0

           if top["score"] < top_threshold:
               return None
           if (top["score"] - second_score) < dominance_threshold:
               return None

           return {
               "siren": top["siren"],
               "score": top["score"],
               "method": "geo_proximity",
               "geo_proximity_distance_m": top["dist_m"],
               "geo_proximity_top_score": top["score"],
               "geo_proximity_2nd_score": second_score,
               "geo_proximity_pool_size": len(scored),
           }

   Add `_haversine_m` if not already present (search the file first
   — if not found, add as a small module-level helper next to other
   geo constants):

       import math
       def _haversine_m(lat1, lng1, lat2, lng2) -> float:
           R = 6_371_000.0
           dlat = math.radians(lat2 - lat1)
           dlng = math.radians(lng2 - lng1)
           a = (math.sin(dlat/2)**2
                + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
                  * math.sin(dlng/2)**2)
           return 2 * R * math.asin(math.sqrt(a))

4) WIRE STEP 2.6 INTO CASCADE — fortress/discovery.py
   ----------------------------------------------------------------------
   _match_to_sirene currently has:
     Step 0   (line 953)     INPI
     Step 0.5 (line 1029)    Chain detector
     Step 1   (line 1049)    SIREN from website
     Step 2   (line 1103)    Enseigne
     Step 2.5 (line 1195)    cp_name_disamb (returns at line 1207 when found)
     Step 3   (line 1209)    Phone
     Step 4   ...            Address
     Step 4b  ...            Surname
     Step 5   ...            Fuzzy name

   Insert Step 2.6 between Step 2.5 and Step 3 — verbatim insertion
   point: AFTER line 1207's `return cp_cand`, BEFORE line 1209's
   `# ── Step 3: Phone match` comment.

   Step 2.6 also needs lat/lng — extend `_match_to_sirene` signature:

       async def _match_to_sirene(
           conn: Any,
           maps_name: str,
           maps_address: str | None,
           departement: str | None,
           maps_phone: str | None = None,
           extracted_siren: str | None = None,
           rejected_siren_sink: dict[str, str] | None = None,
           *,
           picked_nafs: list[str] | None = None,
           naf_division_whitelist: list[str] | None = None,
           maps_cp: str | None = None,
           maps_lat: float | None = None,    # NEW Phase 2
           maps_lng: float | None = None,    # NEW Phase 2
       ) -> dict | None:

   And update the call site at discovery.py:2073-2079 to pass them:

       candidate = await _match_to_sirene(
           conn, maps_name, maps_address, dept_filter, maps_phone, extracted_siren,
           rejected_siren_sink=_inpi_step0_rejected,
           picked_nafs=picked_nafs,
           naf_division_whitelist=naf_division_whitelist,
           maps_cp=maps_cp,
           maps_lat=maps_result.get("lat"),
           maps_lng=maps_result.get("lng"),
       )

   The Step 2.6 block to insert (between lines 1207 and 1209):

       # ── Step 2.6: Geo proximity match (Phase 2 of TOP 1) ─────────
       # Fires when prior steps missed AND Maps panel produced lat/lng.
       # Uses a 400m bounding box against companies_geom (sirene_geo +
       # ban_backfill). Auto-confirm gating happens in the caller's
       # shared NAF gate; this helper only returns the candidate.
       if maps_lat is not None and maps_lng is not None:
           geo_cand = await _geo_proximity_match(
               conn, maps_name, maps_lat, maps_lng,
               picked_nafs, naf_division_whitelist,
           )
           if geo_cand is not None:
               log.info(
                   "discovery.step_2_6_triggered",
                   maps_name=maps_name,
                   pool_size=geo_cand.get("geo_proximity_pool_size"),
                   top_score=geo_cand.get("geo_proximity_top_score"),
                   second_score=geo_cand.get("geo_proximity_2nd_score"),
                   distance_m=geo_cand.get("geo_proximity_distance_m"),
                   target_siren=geo_cand.get("siren"),
               )
               return geo_cand
       else:
           log.debug(
               "discovery.step_2_6_skipped_no_coords",
               maps_name=maps_name,
           )

5) AUTO-CONFIRM POLICY — empty-picker gate fix + sector-mismatch (Decision 5)
   ----------------------------------------------------------------------
   The shared NAF gate at discovery.py:2700-2723 already processes
   strong methods. Two things must change:

   5a) EMPTY-PICKER GATE (BUG FIX — reviewer-flagged).
       At line 2719 the existing exclusion set is:

           if method in {"inpi", "inpi_fuzzy_agree", "inpi_mentions_legales", "chain"}:
               auto_confirm = agree_count >= 1
           else:
               auto_confirm = True

       This means today, on a no-NAF-filter search (`picked_nafs == []`),
       ANY strong method except those four would auto-confirm with zero
       secondary signals. Without the fix below, `geo_proximity` would
       fall into the `else: auto_confirm = True` branch — meaning a
       no-filter "TRANSPORT 33" search could auto-confirm a 400m
       proximity hit on raw name-similarity alone with no agreeing
       phone/enseigne/address/CP signal.

       FIX — add `geo_proximity` to the exclusion set:

           if method in {
               "inpi", "inpi_fuzzy_agree", "inpi_mentions_legales",
               "chain", "geo_proximity",
           }:
               auto_confirm = agree_count >= 1
           else:
               auto_confirm = True

       Effect: empty-picker geo_proximity now requires ≥1 secondary
       signal from `_verify_signals` to auto-confirm. Same conservative
       posture as the other "name-only" strong methods.

   5b) SECTOR MISMATCH = PENDING.
       In the named-NAF case (`picked_nafs != []`):
         - naf_status == 'verified' -> auto_confirm = True (line 2708)
         - naf_status == 'mismatch' -> auto_confirm = False (fall-through
           on line 2723), unless one of the existing exception arms
           (siren_website 1-signal, 2-signal, Giclette enseigne, chain)
           triggers — none of which apply to geo_proximity.

       Decision 5: explicitly DO NOT add geo_proximity to the
       siren_website exception (line 2709) or chain override (line 2747).
       Geo proximity in sector-mismatch is high false-positive risk
       (hotel next to municipal building, café next to bank). Pending.

   5c) AUDIT ACTION BRANCH.
       When auto_confirm is True, the audit-action branch at
       lines 2786-2811 needs a new arm. Add BEFORE the
       `elif picked_nafs == []` arm (between current line 2802
       cp_name_disamb branch and the no-filter branch):

           elif method == "geo_proximity":
               audit_action = "auto_linked_geo_proximity"

   5d) PERSIST link_signals (geo telemetry).
       Modify the link_signals block at lines 2701-2706. After the
       `agree_count = sum(...)` line at 2706, add:

           if method == "geo_proximity" and _pending_link is not None:
               link_signals["geo_proximity_distance_m"] = _pending_link.get("geo_proximity_distance_m")
               link_signals["geo_proximity_top_score"] = _pending_link.get("geo_proximity_top_score")
               link_signals["geo_proximity_2nd_score"] = _pending_link.get("geo_proximity_2nd_score")
               link_signals["geo_proximity_pool_size"] = _pending_link.get("geo_proximity_pool_size")
               if naf_status == "mismatch":
                   link_signals["sector_mismatch"] = True

6) SCOREBOARD — fortress/api/routes/jobs.py:649 (Decision 8)
   ----------------------------------------------------------------------
   Existing query at line 643-651 is:

       SELECT bl.action, COUNT(DISTINCT bl.siren) AS n
       FROM batch_log bl
       JOIN companies co ON co.siren = bl.siren
       WHERE bl.batch_id = %s
         AND co.naf_status = 'verified'
         AND bl.action IN ('auto_linked_verified', 'auto_linked_expanded')
       GROUP BY bl.action

   Decision 8: when geo_proximity auto-confirms with
   naf_status='verified', count it under naf_exact in the scoreboard.

   Modify line 649:

         AND bl.action IN ('auto_linked_verified', 'auto_linked_expanded',
                           'auto_linked_geo_proximity')

   Update the for-loop at lines 654-658:

       for row in naf_split_rows:
           if row["action"] in ("auto_linked_verified", "auto_linked_geo_proximity"):
               naf_exact += int(row["n"] or 0)
           elif row["action"] == "auto_linked_expanded":
               naf_related = int(row["n"] or 0)

   (Note `naf_exact +=` instead of `=` because two actions now feed it.)

   Observability follow-up (NOT BLOCKING for this brief):
     `auto_linked_geo_proximity` confirms get bucketed indistinguishably
     into `naf_exact`, so we lose visibility into "how much of naf_exact
     is geo vs other methods?" Future ticket: expose
     `link_stats["naf_exact_geo"]` sub-key in the same response so the
     scoreboard UI (and Cindy) can see geo's contribution explicitly.
     Not landing in this brief.

7) FRONTEND — `_STRONG_METHODS` desync fix (BUG FIX — reviewer-flagged)
   ----------------------------------------------------------------------
   Two files reference link_method — both must be updated, or
   `geo_proximity` rows render with a generic fallback tooltip instead
   of the rich link_signals tooltip (silent UX regression).

   7a) fortress/fortress/frontend/js/pages/company.js — line 39:

       BEFORE:
       const _STRONG_METHODS = new Set([
         'inpi', 'siren_website', 'enseigne', 'phone', 'address',
         'inpi_fuzzy_agree', 'inpi_mentions_legales', 'chain',
         'gemini_judge'
       ]);

       AFTER (add 'geo_proximity'):
       const _STRONG_METHODS = new Set([
         'inpi', 'siren_website', 'enseigne', 'phone', 'address',
         'inpi_fuzzy_agree', 'inpi_mentions_legales', 'chain',
         'gemini_judge', 'geo_proximity'
       ]);

   7b) fortress/fortress/frontend/js/pages/company.js — line 225-240
       inside `_linkReasonLabel`:

       Add a new arm BEFORE the `return t('company.linkReasonAuto');`
       fallback at line 240:

           if (method === 'geo_proximity') return t('company.linkReasonGeoProximity');

   7c) fortress/fortress/frontend/js/pages/job.js — lines 51-64
       inside the `linkReasonByMethod` lookup table (verified pattern
       at line 63 for `chain:` and 64 for `gemini_judge:`):

       Add a new entry:

           geo_proximity: 'company.linkReasonGeoProximity',

   7d) fortress/fortress/frontend/translations/fr.json — after the
       existing `linkReasonGeminiJudge` line at line 280:

           "linkReasonGeoProximity": "Liaison par proximité géographique",

       AND fortress/fortress/frontend/translations/en.json — same
       position (after line 280):

           "linkReasonGeoProximity": "Match by geographic proximity",

   Without ALL FOUR sub-changes, the rich link_signals tooltip path
   (which gates on `_STRONG_METHODS.has(link_method) && link_signals`
   at company.js:55) will skip geo_proximity rows — they'll show the
   terse fallback only. Reviewer caught this; do not skip it.

8) CSV EXPORT — fortress/api/routes/export.py
   ----------------------------------------------------------------------
   No changes needed (Decision 6). Verified geo_proximity passes the
   existing `naf_status IS DISTINCT FROM 'mismatch'` filter at
   export.py:123, 149, 189, 262, 434, 445. Pending-as-mismatch
   geo_proximity is correctly excluded by that same filter — no
   need to add geo_proximity to the chain/gemini_judge exception
   list.

   Verification step (executor MUST run before reporting done):
        cd fortress && grep -n "naf_status IS DISTINCT FROM 'mismatch'" \
            fortress/api/routes/export.py
   Confirm 6 lines exist as of brief writing. Do NOT modify them.

9) UNIT TESTS — fortress/fortress/tests/test_geo_proximity.py (NEW)
   ----------------------------------------------------------------------
   Pure-unit tests + mocked-DB integration test.

   Mandatory coverage (10 tests):

   a) test_haversine_zero          haversine(45,3,45,3) == 0
   b) test_haversine_400m_eastward
      Compute eastward 400m at lat 46° (lng+0.005), assert haversine
      ≈ 400 ±10.
   c) test_bounding_box_dimensions
      Pure constants: _GEO_LAT_DELTA == 0.0036, _GEO_LNG_DELTA == 0.0050,
      _GEO_RADIUS_M == 400.
   d) test_match_returns_none_when_pool_empty
      (mock conn.fetchall returns [])
   e) test_match_returns_none_when_top_below_default_threshold
      (top=0.80, second=0.40 — top below default 0.85)
   f) test_match_returns_none_when_dominance_below_default
      (top=0.86, second=0.74 → reject; gap 0.12 < 0.15)
   g) test_match_returns_top_when_dominant
      (top=0.92, second=0.50 → return candidate)
   h) test_match_dict_shape
      Returned candidate has keys: siren, score, method='geo_proximity',
      geo_proximity_distance_m, geo_proximity_top_score,
      geo_proximity_2nd_score, geo_proximity_pool_size.
   i) test_expected_schema_dict_completeness
      Import EXPECTED_SCHEMA from scripts.import_sirene_geo and assert
      its 19 keys exactly equal the locked column list (siret, x, y,
      qualite_xy, epsg, plg_qp24, plg_iris, plg_zus, plg_qp15, plg_qva,
      plg_code_commune, distance_precision, qualite_qp24, qualite_iris,
      qualite_zus, qualite_qp15, qualite_qva, y_latitude, x_longitude).
      Sentinel against drift in the brief vs the implementation.
   j) test_match_threshold_override
      Verifies Phase 3's override path: pass top_threshold=0.95 to
      `_geo_proximity_match` — a candidate with score=0.90 that
      WOULD pass at default 0.85 must now be rejected.

   Use AsyncMock for the conn parameter — see existing pattern in
   fortress/fortress/tests/test_chains.py.

================================================================================
TOUCHED FILES
================================================================================
  - fortress/scripts/import_sirene_geo.py             (NEW)
  - fortress/discovery.py                             (constants, helper,
                                                       Step 2.6 wire,
                                                       audit-action branch,
                                                       link_signals stamping,
                                                       empty-picker gate fix)
  - fortress/api/routes/jobs.py                       (scoreboard query +
                                                       naf_exact aggregation)
  - fortress/fortress/frontend/js/pages/company.js    (_STRONG_METHODS,
                                                       _linkReasonLabel arm)
  - fortress/fortress/frontend/js/pages/job.js        (linkReasonByMethod arm)
  - fortress/fortress/frontend/translations/fr.json   (linkReasonGeoProximity)
  - fortress/fortress/frontend/translations/en.json   (linkReasonGeoProximity)
  - fortress/fortress/tests/test_geo_proximity.py     (NEW — 10 tests)

NOT TOUCHED:
  - fortress/api/main.py        (Phase 1 already created the table; no migration)
  - fortress/api/routes/export.py (filter inherits — verify, don't edit)
  - any pre-Phase 2 method's code

================================================================================
RULES (in addition to Phase 1's)
================================================================================
  - Sector mismatch on geo_proximity → pending. Do NOT auto-confirm.
    This is the ONLY strong method that doesn't have an override path.
  - Empty-picker geo_proximity needs ≥1 secondary signal — same
    posture as inpi/inpi_fuzzy_agree/inpi_mentions_legales/chain.
  - Front-end labels: keep i18n key naming consistent
    (linkReasonGeoProximity matches the Chain/Gemini precedent).
  - The bulk import script is one-shot. Do not bake into startup.
    Run manually, log to stdout, exit cleanly.
  - All log keys are English. The `link_signals.sector_mismatch` is
    a JSON Boolean — fine to leave English-keyed.
  - Run pytest from `fortress/` dir.
  - DO NOT commit or push.

================================================================================
ACCEPTANCE CRITERIA
================================================================================
  [1] PRE-CODE checkpoint completed Apr 26 (see
      `.insee_schema_report.md`). Schema, URL, quality enum, row count
      LOCKED. Import script must declare `EXPECTED_SCHEMA` and assert
      it on every monthly refresh.
  [2] No new packages in requirements.txt (no pyproj, no extra deps).
  [3] Bulk import script imports the expected row volume:
        SELECT COUNT(*) FROM companies_geom WHERE source = 'sirene_geo';
      should be in [34_000_000, 36_000_000] — i.e. ~35.3M after dropping
      qualite_xy='33' (~2.04M) from the ~37.38M source rows.
  [4] Quality distribution matches the locked enum:
        SELECT geocode_quality, COUNT(*) FROM companies_geom
        WHERE source='sirene_geo' GROUP BY 1 ORDER BY 2 DESC;
      should show '11' largest (~29.8M), '12' second (~4.3M), '21'
      and '22' tail buckets (~0.6M each), and ZERO rows with '33'.
  [5] Bounding-box query plan uses idx_companies_geom_latlng:
        EXPLAIN ANALYZE SELECT siren FROM companies_geom
        WHERE lat BETWEEN 43.0 AND 43.1 AND lng BETWEEN 1.0 AND 1.1;
      Plan node should mention "Index Scan using idx_companies_geom_latlng".
  [6] Unit suite passes (10 new + full regression green).
  [7] After ws174 small batch (camping 19 Corrèze suggested — fresh
      dept, not yet QA'd), ≥1 row in batch_log with
      action='auto_linked_geo_proximity'.
  [8] Empty-picker batch (no NAF filter) on a small fresh query: any
      `link_method='geo_proximity'` row that auto-confirms must show
      ≥1 agreeing signal in `link_signals`.
  [9] Frontend: confirmed geo_proximity card in browser shows the
      RICH tooltip ("Liaison par proximité géographique" + signals),
      not the terse fallback.
  [10] No regression on auto_linked_verified / inpi_mentions_legales /
       chain confirm counts.

EXECUTOR BRIEF — END PHASE 2
```

```
QA TEST PLAN — START PHASE 2
================================================================================
PHASE 2 QA — Step 2.6 + INSEE bulk (LOCAL ONLY, ws174 ONLY)
================================================================================

PRE-REQUISITE
  Phase 1 must already have shipped (companies_geom table created).
  Verify before starting:
      SELECT to_regclass('companies_geom');  -- must return 'companies_geom'

  Phase 2's import script must have run to completion:
      SELECT COUNT(*) FROM companies_geom WHERE source = 'sirene_geo';
      -- expect ~34M-36M (35.3M nominal, after dropping qualite_xy='33').

================================================================================
SECTION 1 — AUTOMATED CHECKS
================================================================================

CHECK 2.1 — Schema assertion is live in the import script
  Verify import_sirene_geo.py declares `EXPECTED_SCHEMA` matching the
  19-column dict in `.insee_schema_report.md` and asserts it on each
  run.
      grep -c "EXPECTED_SCHEMA" fortress/scripts/import_sirene_geo.py
      grep -c "y_latitude" fortress/scripts/import_sirene_geo.py
      grep -c "x_longitude" fortress/scripts/import_sirene_geo.py
      grep -c "qualite_xy" fortress/scripts/import_sirene_geo.py
  PASS: each grep returns ≥1. Also confirm the script aborts (non-zero
  exit) when the Parquet schema does NOT match — reviewer or executor
  can dry-run-fail-test by temporarily adding a phantom column to
  EXPECTED_SCHEMA and re-invoking.

CHECK 2.2 — Unit tests
  cd "/Users/alancohen/Project Alan copy/fortress"
  python -m pytest fortress/tests/test_geo_proximity.py -v
  Expect: 10 passed.
  Then full regression:
      python -m pytest fortress/tests/ -q
  Expect: 0 regressions.

CHECK 2.3 — Index uses bounding-box plan
  EXPLAIN ANALYZE
  SELECT cg.siren FROM companies_geom cg
  WHERE cg.source IN ('sirene_geo','ban_backfill')
    AND cg.lat BETWEEN 45.10 AND 45.11
    AND cg.lng BETWEEN 1.50 AND 1.51;
  PASS: plan includes "Index Scan using idx_companies_geom_latlng" or
  bitmap variant.

CHECK 2.4 — SIRENE coverage sanity
  SELECT geocode_quality, COUNT(*) AS n
  FROM companies_geom WHERE source = 'sirene_geo'
  GROUP BY geocode_quality ORDER BY n DESC;
  PASS: '11' is largest (~29.8M), '12' second (~4.3M), '21' and '22'
  tail buckets (~0.6M each), ZERO rows with geocode_quality='33',
  no other unexpected values.

  SELECT COUNT(*) FROM companies_geom WHERE source = 'sirene_geo';
  PASS: 34_000_000 ≤ count ≤ 36_000_000.

CHECK 2.5 — Run a fresh ws174 batch (camping 19 Corrèze)
  Login as head.test, spawn batch:
      curl -s -c /tmp/p2cookies.txt -X POST http://localhost:8080/api/auth/login \
           -H "Content-Type: application/json" \
           -d '{"username":"head.test","password":"Test1234"}'
      curl -s -b /tmp/p2cookies.txt -X POST http://localhost:8080/api/batch/spawn \
           -H "Content-Type: application/json" \
           -d '{"queries":["camping 19"],"target_count":15}'

  Wait for completion. Identify batch_id (referenced as <P2_BATCH_ID> below).

CHECK 2.6 — Step 2.6 actually fired (auto-confirms only)
  SELECT bl.siren, bl.action
  FROM batch_log bl
  WHERE bl.batch_id = '<P2_BATCH_ID>'
    AND bl.action = 'auto_linked_geo_proximity';
  PASS: ≥1 row.
  If 0 rows: investigate — check whether Maps panel coords are
  populated on the discovered entities, and whether any sirene_geo
  rows fall inside the bounding box for those coords.

CHECK 2.7 — Sector-mismatch geo_proximity is pending, no audit row
  SELECT co.siren, co.linked_siren, co.link_confidence,
         co.naf_status, co.link_method
  FROM companies co
  WHERE co.siren IN (
      SELECT bl.siren FROM batch_log bl WHERE bl.batch_id = '<P2_BATCH_ID>'
  )
    AND co.link_method = 'geo_proximity'
    AND co.naf_status = 'mismatch';
  PASS: every such row has link_confidence='pending' AND no row in
  batch_log with action='auto_linked_geo_proximity' for those SIRENs:

      SELECT COUNT(*) FROM batch_log bl
      WHERE bl.batch_id = '<P2_BATCH_ID>'
        AND bl.action = 'auto_linked_geo_proximity'
        AND bl.siren IN (
            SELECT co.siren FROM companies co
            WHERE co.link_method = 'geo_proximity'
              AND co.naf_status = 'mismatch'
        );
  PASS: result = 0. Decision 5 forbids auto-confirm on geo+mismatch.

  (Previously CHECK 2.8 — verified geo_proximity confirmed — is
  redundant with CHECK 2.6, which already requires ≥1
  auto_linked_geo_proximity row, all of which are by definition
  confirmed. Removed.)

CHECK 2.8 — link_signals carries geo telemetry
  SELECT siren, link_signals FROM companies
  WHERE link_method = 'geo_proximity'
  ORDER BY siren DESC LIMIT 5;
  PASS: link_signals JSON contains geo_proximity_distance_m,
  geo_proximity_top_score, geo_proximity_2nd_score,
  geo_proximity_pool_size keys.

CHECK 2.9 — Empty-picker gate enforced (BUG FIX VERIFICATION)
  Find any geo_proximity rows from a batch where the search query had
  no NAF filter. If you don't have one in the camping batch, run a
  small "transport 19" no-filter probe (target_count: 10 — keep cheap):

      curl -s -b /tmp/p2cookies.txt -X POST http://localhost:8080/api/batch/spawn \
           -H "Content-Type: application/json" \
           -d '{"queries":["transport 19"],"target_count":10}'

  Then SQL:
      SELECT co.siren, co.linked_siren, co.link_confidence, co.link_signals
      FROM companies co
      JOIN batch_log bl ON bl.siren = co.siren
      WHERE bl.batch_id = '<P2_PROBE_BATCH_ID>'
        AND co.link_method = 'geo_proximity'
        AND co.link_confidence = 'confirmed';
  PASS: every confirmed empty-picker geo_proximity row has at least
  one TRUE signal in link_signals (e.g. enseigne_match=true,
  phone_match=true, address_match=true, denomination_match=true).
  FAIL CONDITION (regression): a confirmed row whose link_signals has
  zero `True` values — that means the empty-picker gate fix didn't
  apply, treat as blocker.

CHECK 2.10 — Scoreboard maps geo_proximity into naf_exact (Decision 8)
  curl -s -b /tmp/p2cookies.txt \
       http://localhost:8080/api/jobs/<P2_BATCH_ID> | jq '.link_stats'
  PASS: naf_exact ≥ count of (action=auto_linked_geo_proximity AND
  naf_status=verified) for the batch. (Computable separately.)

CHECK 2.11 — CSV export includes verified geo_proximity, excludes pending
  curl -s -b /tmp/p2cookies.txt \
       "http://localhost:8080/api/export/csv?batch_id=<P2_BATCH_ID>" \
       -o /tmp/p2_export.csv
  Then SQL:
      SELECT siren FROM companies
      WHERE link_method='geo_proximity' AND naf_status='verified';
  Cross-check that all those SIRENs appear in /tmp/p2_export.csv (grep).
  Then:
      SELECT siren FROM companies
      WHERE link_method='geo_proximity' AND naf_status='mismatch'
        AND link_confidence='pending';
  Cross-check that NONE of those SIRENs appear in /tmp/p2_export.csv.
  PASS: verified geo rows present in CSV; pending geo rows absent.

CHECK 2.12 — Mandatory ws174 7-day confirm rate (CLAUDE.md goal-tracking)
  Run the standard query:
      WITH recent AS (
          SELECT DISTINCT co.siren, co.linked_siren, co.link_confidence
          FROM batch_data bd
          JOIN batch_log bl ON bl.batch_id = bd.batch_id
          JOIN companies co ON co.siren = bl.siren
          WHERE bd.workspace_id = 174 AND bd.status = 'completed'
            AND bd.created_at::date >= CURRENT_DATE - INTERVAL '7 days'
      )
      SELECT COUNT(DISTINCT siren) AS total,
             ROUND(100.0 * SUM(CASE WHEN link_confidence='confirmed' THEN 1 ELSE 0 END)
                   / NULLIF(COUNT(DISTINCT siren), 0), 1) AS confirmed_pct
      FROM recent;
  Report: "99% GOAL TRACKING: ws174 confirm rate now X.X% (previous QA:
  Y.Y%, delta +/-Z.Zpp). Gap to 99%: N.Npp."
  PASS condition: delta ≥ +3pp on this single phase (full +5pp
  expected per Alan's estimate; Phase 3 carries the rest of the
  +10-15pp).
  If delta < +3pp: investigate Step 2.6 trigger volume — is the parser
  capturing coords (Phase 1) and is the bounding box producing
  candidates (Phase 2)?

================================================================================
SECTION 2 — MANUAL BROWSER CHECKS (Playwright, ws174 only)
================================================================================

  M.1  Login head.test / Test1234 -> /#/monitor → camping 19 batch
       shows completed.
  M.2  Job detail page: scoreboard renders, "naf_exact" cell includes
       new geo_proximity confirms (cross-check with SQL count).
  M.3  Click into a confirmed geo_proximity entity → company detail
       page renders with the SIREN data merged. No console errors.
  M.4  Click into a pending geo_proximity entity (via /#/contacts
       page filter):
         "En attente d'approbation" badge visible.
         Hover the badge → tooltip says "Liaison par proximité
         géographique" (i.e. NOT the terse fallback) and surfaces
         the `link_signals` content (geo_proximity_distance_m,
         pool_size, top_score, sector_mismatch=true).
  M.5  Visit /#/dashboard "Liens en attente" tab → row should
       offer Confirmer / Rejeter inline buttons (per CLAUDE.md
       "Pending links: inline actions" rule).

================================================================================
SECTION 3 — SCORECARD
================================================================================
  2.1  EXPECTED_SCHEMA assertion live in importer      | PASS/FAIL
  2.2  Unit + regression tests (10 + green)            | PASS/FAIL
  2.3  EXPLAIN uses lat/lng index                      | PASS/FAIL
  2.4  SIRENE coverage 34-36M, no '33' rows            | PASS/FAIL
  2.5  ws174 camping 19 batch completes                | PASS/FAIL
  2.6  ≥1 auto_linked_geo_proximity in batch_log       | PASS/FAIL
  2.7  Geo+mismatch → pending, no audit row            | PASS/FAIL
  2.8  link_signals carries geo telemetry              | PASS/FAIL
  2.9  Empty-picker gate enforced (≥1 signal)          | PASS/FAIL
  2.10 Scoreboard counts geo_proximity into naf_exact  | PASS/FAIL
  2.11 CSV export rules respected                      | PASS/FAIL
  2.12 Confirm-rate +3pp+                              | PASS/FAIL
  M.1-5 UI smoke + rich pending tooltip                | PASS/FAIL

================================================================================
SECTION 4 — PASS / FAIL CRITERIA
================================================================================
  PASS = all 12 automated + all 5 manual.
  FAIL = any one. Report which, with SQL output / screenshot.

QA TEST PLAN — END PHASE 2
```
