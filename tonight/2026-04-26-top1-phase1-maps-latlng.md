# TOP PRIORITY 1 — INSEE Geocoded Proximity Matching: Phase 1 (REVISED)

**Plain-English summary for Alan:**

Cindy's matcher currently looks within a 2-5km postal-code radius. With latitude/longitude we can narrow that to 400m — turning a soup of 40 candidates into 1-5. That's the difference between guessing and deciding. We ship in three phases:

1. **Phase 1 — Capture the GPS:** Every business Maps shows us has lat/lng in its URL. We start saving that into a new side table.
2. **Phase 2 — Use the GPS:** Download INSEE's 17M-row geocoded SIRENE file once, load it into the same side table, then add a new matcher step that says "any SIRENE business within 400m of this Maps result is a candidate."
3. **Phase 3 — Backfill old data:** For ws174's existing MAPS entities (no GPS yet), call the free BAN address-geocoding API to add their coordinates, then re-run the new matcher on them.

Each phase is a separate `/exec` brief with its own QA. Phase 2 needs Phase 1's table. Phase 3 needs Phase 2's matcher. They ship in order.

**What this phase does:** The Maps scraper currently records the full panel URL but throws away the GPS coordinates inside it. This phase parses lat/lng from that URL, saves them to a new side table called `companies_geom`, and adds 8 unit tests. No matcher changes — pure data plumbing. After ship, every new MAPS entity should have a row in `companies_geom` with valid French-bounds coordinates (target ≥60% capture rate — see Check 1.4 reasoning). Alan sees nothing different in the UI; the change is observable via SQL only.

**Reviewer's deferred item:** Frontend `_STRONG_METHODS` desync was flagged but is out of scope for Phase 1. Phase 2 owns that fix. Phase 1 does not touch frontend or matcher logic.

```
EXECUTOR BRIEF — START PHASE 1
================================================================================
PHASE 1 — Maps lat/lng capture + create companies_geom table
================================================================================

GOAL
  Parse latitude/longitude from Google Maps panel URLs and persist them to a
  new side table `companies_geom`. No matcher logic changes. No frontend
  changes. Pure data plumbing — Phase 2 will use what this phase saves.

CROSS-PHASE DEPENDENCY
  - Nothing depends on prior code (this is the first of 3).
  - Phase 2 hard-depends on this phase: it queries `companies_geom`. If you
    skip Phase 1 and ship Phase 2, the matcher script crashes with
    "relation companies_geom does not exist".

DESIGN CONSTANT (Alan-approved Decision 3)
  `companies.latitude` / `companies.longitude` (NUMERIC, schema.sql:24-25)
  are LEGACY columns. We do NOT write to them. They stay NULL forever.
  All new geocode data goes to `companies_geom`.

DESIGN CONSTANT (Alan-approved Decision 3 + Decision 2 / Path A)
  Single side table `companies_geom` for ALL geocodes (Maps panel today,
  INSEE bulk in Phase 2, BAN backfill in Phase 3). The `source` column
  distinguishes them.

================================================================================
WORK ITEMS
================================================================================

1) NEW HELPER — fortress/scraping/maps.py
   ----------------------------------------------------------------------
   Add module-level constants (top of file, after imports):

       # French-bounds rectangle (mainland + Corsica). Drop coords outside.
       # Decision 3: DOM-TOM excluded from geo matching (out of scope, distant).
       _GEO_LAT_MIN = 41.0   # Corsica south tip
       _GEO_LAT_MAX = 51.0   # Dunkerque
       _GEO_LNG_MIN = -5.0   # Brest / Brittany
       _GEO_LNG_MAX = 10.0   # Strasbourg / Corsica east

   Add new helper function (place near the top of the MapsScraper class
   helpers, OR as a module-level function — module-level is preferred for
   testability):

       def parse_maps_lat_lng(url: str | None) -> tuple[float, float] | None:
           """Extract (lat, lng) from a Google Maps panel URL.

           Preferred forms in priority order:
             1. Place-detail URL with `!3d{lat}!4d{lng}` segment
                (most reliable — Google's place-anchor format).
             2. Camera URL with `@{lat},{lng},{zoom}z`
                (search-results URL — less precise but always present).

           Returns None if:
             - url is None / empty / not a Maps URL
             - neither form parses
             - lat or lng is non-numeric / non-finite
             - coords fall outside French mainland+Corsica bounds
               (lat ∈ [41, 51], lng ∈ [-5, 10]) — Decision 3
           """

   Implementation notes:
     - Form 1 regex:  r'!3d(-?\d+\.\d+)!4d(-?\d+\.\d+)'
     - Form 2 regex:  r'@(-?\d+\.\d+),(-?\d+\.\d+),'  (note the trailing comma
       before the zoom token; matches @46.6,2.3,6z)
     - After parsing, validate: math.isfinite(lat) and math.isfinite(lng)
       and _GEO_LAT_MIN <= lat <= _GEO_LAT_MAX
       and _GEO_LNG_MIN <= lng <= _GEO_LNG_MAX
     - Return tuple of (float, float) or None.
     - NEVER raise — always return None on any error. The caller is in a
       hot scrape loop.

2) WIRE CAPTURE — fortress/scraping/maps.py:1477-1481 area
   ----------------------------------------------------------------------
   Existing code (verbatim today):

       # ── Maps URL: capture the current page URL ─────────────────────
       current_url = page.url
       if current_url and "google.com/maps" in current_url:
           result["maps_url"] = current_url

   Insert immediately AFTER the `result["maps_url"] = current_url` line
   (still inside the `if current_url and "google.com/maps" in current_url:`
   block):

       coords = parse_maps_lat_lng(current_url)
       if coords is not None:
           result["lat"] = coords[0]
           result["lng"] = coords[1]
           # Otherwise leave both keys absent — discovery._persist_result
           # will treat missing as "no coords".

   Update the existing `log.info(...)` call ~line 1482 to include:
       has_lat_lng=bool(result.get("lat") is not None),

3) STARTUP MIGRATION — fortress/api/main.py
   ----------------------------------------------------------------------
   Insert into the existing startup block (in the same try/except that
   wraps the other CREATE TABLE / ALTER TABLE statements). Place it
   AFTER the existing block at lines 348-352 (the
   "ALTER TABLE batch_tags ADD COLUMN IF NOT EXISTS created_at" block)
   and BEFORE the Query Memory table (line 354). Use exactly this style
   (matches the existing `await conn.execute("""...""")` idiom):

       # ── companies_geom side table (TOP 1 Phase 1) ────────────────
       # Single source of truth for ALL geocodes (Maps panel, INSEE
       # bulk SIRENE, BAN backfill). companies.latitude/longitude
       # remain legacy / unused. See CLAUDE.md Decision 3 (Apr 26).
       await conn.execute("""
           CREATE TABLE IF NOT EXISTS companies_geom (
               siren            VARCHAR(9) PRIMARY KEY,
               lat              NUMERIC(10, 7) NOT NULL,
               lng              NUMERIC(10, 7) NOT NULL,
               source           TEXT NOT NULL,
               geocode_quality  TEXT,
               created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
               updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW()
           )
       """)
       # Bounding-box index for proximity queries (Phase 2 Step 2.6).
       await conn.execute("""
           CREATE INDEX IF NOT EXISTS idx_companies_geom_latlng
           ON companies_geom (lat, lng)
       """)
       # Source-filtered scans (admin queries, backfill enumeration).
       await conn.execute("""
           CREATE INDEX IF NOT EXISTS idx_companies_geom_source
           ON companies_geom (source)
       """)

   Notes:
     - VARCHAR(9) matches `companies.siren` width (real SIRENs are 9 digits;
       MAPS%%%%% IDs are also 9 chars: 'MAPS' + 5-digit). Prior draft used
       VARCHAR(20) — corrected per reviewer.
     - Do NOT add a foreign key. companies_geom holds rows for both real
       SIREN and MAPS entities. SIRENE rows live in `companies` but
       Phase 2 inserts millions of them via bulk import, before the
       MAPS entity ever exists. A FK with cascading semantics would also
       complicate Phase 3.
     - Source values used across phases:
         'maps_panel'   — Phase 1 (this brief)
         'sirene_geo'   — Phase 2 (INSEE Parquet bulk import)
         'ban_backfill' — Phase 3 (BAN address geocoding for ws174 backfill)
     - geocode_quality is NULL for maps_panel (no quality flag from Maps URL).
       Phase 2 will populate it for sirene_geo, Phase 3 for ban_backfill.

4) PERSIST CAPTURE — fortress/discovery.py
   ----------------------------------------------------------------------
   This step touches THREE distinct locations in discovery.py — do not
   conflate them. Reviewer caught the prior draft pointing all three
   changes at line 1957; the real layout is:

   (a) BATCH-LEVEL DECLARATION at lines 1848-1849
       ----------------------------------------------------------
       Find the block where batch-level counters are declared (alongside
       _gemini_cap_logged etc.). Add:

           _query_geo_capture_count: int = 0

       This declares the counter at batch scope so it survives across
       per-query iterations and is available to the end-of-batch summary.

   (b) PER-QUERY RESET at lines 3613-3614
       ----------------------------------------------------------
       Find the per-query loop body where counters reset each iteration
       (look for `_query_dedup_count = 0` and `_query_filtered_count = 0`).
       Add alongside them:

           _query_geo_capture_count = 0

       This resets the counter at the start of each query within the batch
       so the per-query telemetry isn't polluted by prior queries.

   (c) NONLOCAL BINDING + UPSERT at line 1957 (inside `_persist_result`)
       ----------------------------------------------------------
       Locate the existing `nonlocal` line at ~1957 inside `_persist_result`.
       Extend it to include the new counter:

           nonlocal companies_discovered, qualified, _query_dedup_count, _query_filtered_count, _gemini_cap_logged, _query_geo_capture_count

       Then find the existing block (verbatim today, around lines 2666-2670):

           # Persist to DB immediately
           async with pool.connection() as conn:
               await upsert_company(conn, company)
               await bulk_tag_query(conn, [siren], batch_name, workspace_id=batch_workspace_id, batch_id=batch_id)
               await upsert_contact(conn, contact)

       Insert ONE additional statement immediately after `await upsert_contact(conn, contact)`
       and before the existing `# Default link state: pending. Auto-confirm logic` comment:

           # ── Phase 1 — persist Maps panel coordinates if captured ───────
           _maps_lat = maps_result.get("lat")
           _maps_lng = maps_result.get("lng")
           if _maps_lat is not None and _maps_lng is not None:
               try:
                   await conn.execute(
                       """INSERT INTO companies_geom
                              (siren, lat, lng, source, geocode_quality)
                          VALUES (%s, %s, %s, 'maps_panel', NULL)
                          ON CONFLICT (siren) DO UPDATE SET
                              lat = EXCLUDED.lat,
                              lng = EXCLUDED.lng,
                              source = EXCLUDED.source,
                              updated_at = NOW()
                          WHERE companies_geom.source = 'maps_panel'""",
                       (siren, _maps_lat, _maps_lng),
                   )
                   _query_geo_capture_count += 1
               except Exception as e:
                   log.debug("discovery.geom_insert_failed",
                             siren=siren, error=str(e))

       The `WHERE companies_geom.source = 'maps_panel'` clause on the UPSERT
       is critical: Phase 2 will pre-populate sirene_geo rows for real SIRENs;
       we MUST NOT overwrite them when a MAPS entity later happens to be
       given the same siren (it can't — MAPS%% prefix — but the guard prevents
       future bug if an INPI strong-method auto-confirm path ever tries to
       write directly to a real SIREN).

   (d) END-OF-BATCH SUMMARY LOG
       ----------------------------------------------------------
       At end-of-batch summary (find the existing INFO log that summarises a
       batch — look for "discovery.batch_done" or similar around the run-loop
       tail), add:

           log.info(
               "discovery.batch_geo_capture_rate",
               batch_id=batch_id,
               geo_captured=_query_geo_capture_count,
               total_entities=companies_discovered,
               rate=(_query_geo_capture_count / companies_discovered) if companies_discovered else 0.0,
           )

   IMPORTANT: do NOT bare-pass on the audit. Always log.debug() per the
   silent-swallow memory feedback. The try/except above already does this.

5) UNIT TESTS — fortress/fortress/tests/test_maps_latlng.py (NEW FILE)
   ----------------------------------------------------------------------
   Pytest module with at least 8 distinct tests. Follow the style of the
   existing fortress/fortress/tests/test_phone_normalize.py — pure unit
   tests, no fixtures, no DB. Each test asserts a specific edge case.

   Mandatory tests (function names indicative; use clear French-or-English
   names per existing convention):

   a) test_parse_form_1_3d4d_preferred
      URL: 'https://www.google.com/maps/place/Camping+X/@43.5,3.2,15z/data=!3m4!4d3.2123!3d43.5456...'
      Expect: (43.5456, 3.2123) — form 1 wins even when form 2 is also present.

   b) test_parse_form_2_at_fallback
      URL: 'https://www.google.com/maps/search/camping+47/@44.20,0.62,12z'
      Expect: (44.20, 0.62) — only form 2 present, parser falls back.

   c) test_brittany_negative_longitude
      URL: 'https://www.google.com/maps/place/Quimper+Camping/@47.99,-4.10,15z/data=!4d-4.0987!3d47.9911'
      Expect: (47.9911, -4.0987) — negative lng for western Brittany passes bounds.

   d) test_corsica_passes
      URL: 'https://.../@42.0,9.0,15z/data=!4d9.1234!3d42.0567'
      Expect: (42.0567, 9.1234) — Corsica inside bounds [41-51, -5-10].

   e) test_dom_tom_rejected
      URL: 'https://.../@-21.115,55.536,15z/data=!4d55.5360!3d-21.1151'
      (Réunion island)
      Expect: None — out of mainland+Corsica bounds.

   f) test_decimal_zoom
      URL: 'https://www.google.com/maps/search/cafe/@45.1234,2.5678,12.5z'
      Expect: (45.1234, 2.5678) — decimal zoom doesn't break form 2 regex.

   g) test_malformed_returns_none
      URL: 'https://www.google.com/maps/' (no coords at all)
      Expect: None.

   h) test_none_input_returns_none
      URL: None
      Expect: None — must not crash.

   Bonus (encouraged, not required):

   i) test_search_results_url
      Like form 2 but with a search query path, not place path.
      Expect: form 2 parses cleanly.

   j) test_out_of_france_germany
      URL with German coords (lat 52, lng 13.4 — Berlin).
      Expect: None — lat 52 > 51 max.

================================================================================
TOUCHED FILES
================================================================================
  - fortress/scraping/maps.py            (constants + helper + capture call + log)
  - fortress/api/main.py                 (CREATE TABLE + 2× CREATE INDEX in startup block)
  - fortress/discovery.py                (3 locations: 1848-49 decl, 3613-14 reset, 1957 nonlocal+UPSERT, end-of-batch log)
  - fortress/fortress/tests/test_maps_latlng.py   (NEW — 8+ unit tests)

NOT TOUCHED (verify by `git diff`):
  - fortress/discovery.py:_match_to_sirene  (no matcher logic change in Phase 1)
  - fortress/processing/dedup.py            (upsert_company stays as-is)
  - fortress/database/schema.sql            (we use the startup migration block,
                                              not schema.sql)
  - any frontend file (frontend _STRONG_METHODS desync deferred to Phase 2)
  - any export route
  - fortress/scripts/ — Phase 1 adds no scripts. (Convention reminder: if
    any future phase adds a CLI script, place it in `fortress/scripts/`,
    NOT `fortress/fortress/scripts/`. The latter does not exist.)

================================================================================
RULES
================================================================================
  - SIRENE read-only: companies_geom is a side table. Never write to
    companies.latitude/longitude (they stay NULL forever per Decision 3).
  - ALTER/CREATE in api/main.py startup block, NOT migration file. Per
    CLAUDE.md "ALTER TABLE ADD COLUMN must ALWAYS use IF NOT EXISTS".
  - All log statements use structlog (`log.info` / `log.debug`). All
    log keys are English (only user-facing UI strings are French).
  - Never bare-pass: any try/except wraps log.debug() with action+error.
  - Run pytest from the `fortress/` dir. The pyproject testpaths is
    `["tests"]`, which means cwd-relative; running from repo root
    behaves differently. Use:
        cd fortress && python -m pytest fortress/tests/test_maps_latlng.py -v

  - Do NOT pass `--timeout=N` to pytest. `pytest-timeout` is not installed
    in this project. Use plain `-q` / `-v` flags only.

  - DO NOT commit or push. Alan commits per phase after QA.

================================================================================
ACCEPTANCE CRITERIA (the executor MUST verify before reporting "done")
================================================================================
  [1] `pytest fortress/tests/test_maps_latlng.py` from `cd fortress/`
      passes 8/8 (or more if you added bonus tests).
  [2] Full suite still green (no regression):
        cd fortress && python -m pytest fortress/tests/ -q
  [3] App starts cleanly (uvicorn boots, no migration errors).
  [4] After running an actual ws174 batch, SQL shows new rows:
        SELECT COUNT(*) FROM companies_geom WHERE source = 'maps_panel';
      should be > 0 (the QA plan checks ≥60% capture rate — see Check 1.4).
  [5] Zero rows for sirene_geo or ban_backfill (Phase 1 doesn't write those).
  [6] No row in `companies_geom` whose siren NOT LIKE 'MAPS%' — Phase 1
      only writes for MAPS entities. (Real SIRENs join Phase 2.)

EXECUTOR BRIEF — END PHASE 1
```

```
QA TEST PLAN — START PHASE 1
================================================================================
PHASE 1 QA — Maps lat/lng capture (LOCAL ONLY, no Render dependency)
================================================================================

SETUP (run once at start of QA session)
  1. cd "/Users/alancohen/Project Alan copy/fortress"
  2. Confirm DATABASE_URL set:
        echo "$DATABASE_URL" | head -c 30
     Should print neon connection prefix.
  3. Start uvicorn dev server in a background terminal:
        python3 -m uvicorn fortress.api.main:app --port 8080 --reload
     Wait for "🏰 Fortress API started — database connected".
     Confirm migration log line: "✅ contact_requests and company_notes tables ready"
     (this is the same try-block that creates companies_geom).

  All commands below run from `cd fortress/` (the inner package dir).
  Workspace 174 ONLY for QA. Never touch ws1.

================================================================================
SECTION 1 — AUTOMATED CHECKS (run in terminal, paste into QA agent)
================================================================================

CHECK 1.1 — Migration created the table + indexes
  SQL (psql / Neon SQL editor / `python3 -c "..."`):

      SELECT to_regclass('companies_geom') AS exists;
      -- Expect: 'companies_geom'

      SELECT indexname FROM pg_indexes WHERE tablename = 'companies_geom';
      -- Expect rows: idx_companies_geom_latlng, idx_companies_geom_source
      -- (PRIMARY KEY index on siren is automatic, name varies)

      \d+ companies_geom
      -- (or equivalent psql describe)
      -- Expect columns: siren VARCHAR(9) NOT NULL, lat NUMERIC(10,7) NOT NULL,
      -- lng NUMERIC(10,7) NOT NULL, source TEXT NOT NULL,
      -- geocode_quality TEXT, created_at TIMESTAMPTZ, updated_at TIMESTAMPTZ.

  PASS condition: table + 2 named indexes exist; column types match
    (siren is VARCHAR(9), not VARCHAR(20)).
  FAIL condition: any of the above missing or siren width != 9.

CHECK 1.2 — Unit test suite passes
  Terminal:
      cd "/Users/alancohen/Project Alan copy/fortress"
      python -m pytest fortress/tests/test_maps_latlng.py -v
  Expect: 8 passed (or more if bonus tests added). 0 failures.

  Then full regression:
      python -m pytest fortress/tests/ -q
  Expect: same green count as before Phase 1 + 8 new (no regression).

  NOTE: do NOT add `--timeout=N`. pytest-timeout is not installed.

CHECK 1.3 — Run a small ws174 batch (FRESH dept — camping 65 Hautes-Pyrénées)
  Login as head.test (Test1234), launch batch:
      curl -s -c /tmp/p1cookies.txt -X POST http://localhost:8080/api/auth/login \
           -H "Content-Type: application/json" \
           -d '{"username":"head.test","password":"Test1234"}'
      curl -s -b /tmp/p1cookies.txt -X POST http://localhost:8080/api/batch/spawn \
           -H "Content-Type: application/json" \
           -d '{"queries":["camping 65"],"target_count":15}'

  Wait for batch completion (poll /api/jobs or watch monitor page).

  IDENTIFY batch_id (executor + QA can use):
      SELECT batch_id, batch_name, status, companies_scraped, completed_at
      FROM batch_data
      WHERE workspace_id = 174
        AND batch_name ILIKE 'CAMPING_65%'
      ORDER BY created_at DESC LIMIT 1;

CHECK 1.4 — Geo capture rate ≥ 60% for the new batch
  SQL:
      WITH batch AS (
          SELECT batch_id FROM batch_data
          WHERE workspace_id = 174
            AND batch_name ILIKE 'CAMPING_65%'
          ORDER BY created_at DESC LIMIT 1
      ),
      maps_in_batch AS (
          SELECT DISTINCT bl.siren
          FROM batch_log bl
          JOIN batch b ON b.batch_id = bl.batch_id
          WHERE bl.siren LIKE 'MAPS%'
      )
      SELECT
        COUNT(*) AS total_maps,
        COUNT(cg.siren) AS with_geo,
        ROUND(100.0 * COUNT(cg.siren) / NULLIF(COUNT(*), 0), 1) AS pct
      FROM maps_in_batch m
      LEFT JOIN companies_geom cg
        ON cg.siren = m.siren AND cg.source = 'maps_panel';

  PASS condition: pct ≥ 60.0.
  FAIL condition: pct < 60.0 — investigate (parser regex broken? URL form
  changed?).

  WHY 60% (not 80%): Maps URLs come in two forms — place-detail (form 1,
  has !3d/!4d) and search-results (form 2, has @lat,lng). Some Maps cards
  close before the URL canonicalizes to form 1; some search-result clicks
  only ever produce form 2 with no place anchor. Real-world capture in
  testing has been ~60-75%. Setting the floor at 60% catches obvious
  parser regressions without flagging normal scraper behavior.

CHECK 1.5 — All captured coords are within French bounds
  SQL:
      SELECT COUNT(*) FROM companies_geom
      WHERE source = 'maps_panel'
        AND (lat NOT BETWEEN 41 AND 51 OR lng NOT BETWEEN -5 AND 10);

  PASS condition: 0 rows.
  FAIL condition: any row outside bounds — bounds check broken in parser.

CHECK 1.6 — No leakage onto real SIREN rows
  SQL:
      SELECT COUNT(*) FROM companies_geom
      WHERE source = 'maps_panel' AND siren NOT LIKE 'MAPS%';

  PASS condition: 0 rows. Phase 1 only writes for MAPS entities.

CHECK 1.7 — Confirm-rate sanity (matcher unchanged in Phase 1) + 99% goal tracking
  Compute the Apr 24 mandatory ws174 7-day stat. Expect: ±1pp vs the
  previous QA's number. (Phase 1 doesn't touch the matcher.)

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
  Expect: |delta| ≤ 1.0pp.

================================================================================
SECTION 2 — MANUAL BROWSER CHECKS (QA agent runs via Playwright)
================================================================================

Per memory feedback_qa_runs_manual_checks.md: QA agent executes these
itself, not as homework for Alan.

  M.1 — Login as head.test, navigate to #/monitor.
        Verify the camping 65 batch shows "completed".
        Screenshot for evidence.

  M.2 — Click into the camping 65 batch detail page.
        Verify it loads without JS console errors.
        Verify entities list renders (some may show MAPS IDs).

  M.3 — Click into one MAPS entity card.
        Verify the company-detail page renders normally — no broken UI
        from any geom-related change. (We didn't touch frontend; this is
        a regression sanity check.)

  M.4 — Visit /#/dashboard.
        Verify dashboard renders, all widgets load, no console errors.

If any manual step fails, take screenshot and report specific failure.

================================================================================
SECTION 3 — SCORECARD
================================================================================

  Step  | Check                                 | Expected         | Actual | Pass/Fail
  ------|---------------------------------------|------------------|--------|----------
  1.1   | companies_geom table + 2 indexes      | All exist; siren=VARCHAR(9) |  |
  1.2   | Unit tests + full regression          | 8+ new pass; 0 reg|       |
  1.3   | ws174 camping 65 batch completes      | status=completed |        |
  1.4   | Geo capture rate ≥60%                 | ≥ 60.0%          |        |
  1.5   | All coords inside French bounds       | 0 violations     |        |
  1.6   | No coords on real SIREN rows          | 0 rows           |        |
  1.7   | Confirm-rate stable (matcher untouched)| |Δ| ≤ 1.0pp     |        |
  M.1-4 | UI smoke screens                      | All render OK    |        |

================================================================================
SECTION 4 — PASS / FAIL CRITERIA
================================================================================

  PASS: all 7 automated checks PASS + all 4 manual checks PASS.
  FAIL: any one check FAILS — report which, with SQL output / screenshot.

QA TEST PLAN — END PHASE 1
```
