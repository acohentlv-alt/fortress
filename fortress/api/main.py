"""Fortress API — FastAPI entry point.

Run with:
    python -m fortress.api.main

Serves:
    - /api/* endpoints for data access
    - / static files for the frontend SPA

Authentication:
    Session-based via signed cookies. All /api/* routes require authentication
    except /api/health, /api/auth/check, /api/auth/login, /api/auth/logout.
    The session cookie is HttpOnly (JS can't read it), signed with SESSION_SECRET.
"""

from contextlib import asynccontextmanager
import logging
from pathlib import Path

import psutil

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest

from slowapi.errors import RateLimitExceeded

from fortress.api.auth import decode_session_token
from fortress.api.db import close_pool, init_pool, pool_status
from fortress.api.rate_limit import limiter
from fortress.api.routes import activity, admin, auth as auth_routes, batch, blacklist, bug_report, client, companies, contact, contacts_list, dashboard, departments, export, health, jobs, notes, sirene
from fortress.config.settings import settings

logger = logging.getLogger("fortress.api")
# Surface fortress.api logger.info() calls — stdlib defaults block them.
# Why basicConfig (not setLevel on the leaf logger): INFO records propagate
# from fortress.api to root. Root's default level is WARNING (filters them
# out) AND root has no handlers (no destination). setLevel on fortress.api
# alone doesn't fix either gate. basicConfig sets root level + adds a
# StreamHandler in one call. force=True overrides any future dictConfig.
# QA verified May 1 — empirically proven that setLevel alone is insufficient.
# Bonus: also unblocks the existing _periodic_orphan_sweeper INFO logs
# that have been silently dropped in production until now.
logging.basicConfig(level=logging.INFO, force=True)

# Sweeper double-spawn guard (added 2026-05-07).
# Prevents amplification when subprocess slow to write status='in_progress'.
_sweeper_recent_spawns: dict[str, float] = {}
_SWEEPER_RESPAWN_GUARD_SEC = 600.0  # 10 min — > 300s sweeper cycle


def _sweeper_should_spawn(batch_id: str) -> bool:
    """Return True if no recent spawn for this batch_id; record now if so.
    Prunes entries older than 2× guard interval to bound memory."""
    import time as _time
    _now = _time.monotonic()
    # Prune-on-insert: drop entries > 2 * GUARD ago
    _stale_cutoff = _now - (2 * _SWEEPER_RESPAWN_GUARD_SEC)
    for _k in [k for k, v in _sweeper_recent_spawns.items() if v < _stale_cutoff]:
        _sweeper_recent_spawns.pop(_k, None)
    # Guard check
    _last = _sweeper_recent_spawns.get(batch_id)
    if _last is not None and (_now - _last) < _SWEEPER_RESPAWN_GUARD_SEC:
        return False
    _sweeper_recent_spawns[batch_id] = _now
    return True


async def _resumable_interrupted_batch_id(conn) -> "tuple[str, int | None] | None":
    """Return (batch_id, workspace_id) of the oldest auto-resumable interrupted
    batch, or None.

    Eligibility:
      - status = 'interrupted'
      - resume_attempt_count < 2 (circuit breaker — manual Reprendre resets to 0)
      - cancel_requested = false (never auto-resume cancellations)
      - created_at > NOW() - INTERVAL '24 hours' (don't resurrect ancient batches)
      - updated_at > NOW() - INTERVAL '6 hours' (only batches with recent activity;
        a batch idle for 6 hours has been abandoned by the user)
      - companies_scraped < batch_size OR batch_size = 0 (don't resume already-full)

    Returns oldest first by created_at so a user with two interrupted batches
    in the same workspace gets the earlier one resumed first.
    """
    cur = await conn.execute(
        """SELECT batch_id, workspace_id FROM batch_data
           WHERE status = 'interrupted'
             AND COALESCE(resume_attempt_count, 0) < 2
             AND COALESCE(cancel_requested, FALSE) = FALSE
             AND created_at > NOW() - INTERVAL '24 hours'
             AND updated_at > NOW() - INTERVAL '6 hours'
             AND (COALESCE(batch_size, 0) = 0
                  OR COALESCE(companies_scraped, 0) < COALESCE(batch_size, 0))
           ORDER BY created_at ASC LIMIT 1"""
    )
    row = await cur.fetchone()
    if not row:
        return None
    return (row[0], row[1])


async def _auto_resume_spawn(batch_id: str, workspace_id: "int | None") -> bool:
    """Flip interrupted → queued, increment resume_attempt_count, spawn the
    discovery subprocess. Returns True if spawn was attempted (whether or not
    the subprocess Popen succeeded).

    Caller MUST have already verified:
      - no other batch is in_progress (global cap = 1)
      - _sweeper_should_spawn(batch_id) returned True (double-spawn guard)

    Race-safe via single-statement UPDATE that also checks NOT EXISTS in_progress
    AND replicates the same-workspace 15-min concurrency guard from manual
    /jobs/{id}/resume (jobs.py:497-531). Test workspaces (settings.test_workspace_ids)
    are exempt from the same-workspace guard, matching manual-resume semantics.
    """
    import subprocess as _sp
    import sys as _sys
    from pathlib import Path as _Path
    from fortress.api.db import get_conn as _gc
    from fortress.config.settings import settings as _settings

    is_test_ws = (
        workspace_id is not None and workspace_id in (_settings.test_workspace_ids or [])
    )

    # ── Atomic flip + increment + double-checks (Issue 1+2 from /review) ──
    # Single UPDATE atomically:
    # (a) WHERE status = 'interrupted'  — race-safe vs parallel auto-resume
    # (b) AND NOT EXISTS in_progress    — race-safe vs user-launched fresh batch
    # (c) AND NOT EXISTS recent same-workspace queued OR in_progress (skipped for test_ws)
    # Test workspaces skip (c) per jobs.py:499-503 manual-resume semantics.
    same_ws_guard = (
        ""
        if is_test_ws
        else """
        AND NOT EXISTS (
            SELECT 1 FROM batch_data b2
            WHERE b2.batch_id != batch_data.batch_id
              AND b2.workspace_id = batch_data.workspace_id
              AND b2.status IN ('queued', 'in_progress')
              AND b2.updated_at > NOW() - INTERVAL '15 minutes'
        )
        """
    )
    try:
        async with _gc() as _ar_conn:
            cur = await _ar_conn.execute(
                f"""UPDATE batch_data
                    SET status = 'queued',
                        resume_attempt_count = COALESCE(resume_attempt_count, 0) + 1,
                        cancel_requested = FALSE,
                        shortfall_reason = NULL,
                        updated_at = NOW()
                    WHERE batch_id = %s
                      AND status = 'interrupted'
                      AND NOT EXISTS (
                          SELECT 1 FROM batch_data b3
                          WHERE b3.status = 'in_progress'
                      )
                      {same_ws_guard}
                    RETURNING resume_attempt_count""",
                (batch_id,),
            )
            row = await cur.fetchone()
            await _ar_conn.commit()
            if not row:
                logger.info(
                    "auto_resume.race_lost batch=%s ws=%s test_ws=%s",
                    batch_id, workspace_id, is_test_ws,
                )
                return False
            attempt_n = row[0]
    except Exception as exc:
        logger.warning("auto_resume.flip_failed batch=%s err=%s", batch_id, exc)
        return False

    # ── Spawn subprocess (same pattern as catch-up + sweeper above) ──
    fortress_root = _Path(__file__).resolve().parent.parent
    runner_cmd = [_sys.executable, "-m", "fortress.discovery", batch_id]
    launcher = _Path("/tmp/fortress_launcher.py")
    if launcher.exists():
        runner_cmd = [_sys.executable, str(launcher), "runner", batch_id]
    try:
        _sp.Popen(
            runner_cmd, cwd=str(fortress_root),
            stdout=None, stderr=None,
            close_fds=False, start_new_session=True,
        )
        logger.info(
            "auto_resume.spawned batch=%s ws=%s attempt=%d",
            batch_id, workspace_id, attempt_n,
        )
    except Exception as exc:
        logger.warning(
            "auto_resume.popen_failed batch=%s err=%s", batch_id, exc
        )
    return True


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle — resilient to DB failures."""
    if settings.secure_cookies and settings.session_secret == "fortress-dev-secret-change-me":
        raise RuntimeError(
            "FATAL: SESSION_SECRET is still the public default value. "
            "Set SESSION_SECRET env var to a 32-byte random value on Render before booting."
        )

    # Test workspace bypass — must NEVER be set on Render production.
    # If FRONTEND_URL starts with https:// (i.e. secure_cookies is True), the
    # presence of TEST_WORKSPACE_IDS would disable Render's 2 GiB OOM safety.
    if settings.secure_cookies and settings.test_workspace_ids:
        raise RuntimeError(
            f"FATAL: TEST_WORKSPACE_IDS={settings.test_workspace_ids} on "
            "production. This bypasses Render's 2 GiB OOM safety. "
            "Unset TEST_WORKSPACE_IDS to boot."
        )

    if settings.test_workspace_ids:
        logger.info(
            f"⚠ Test workspaces bypass concurrency cap + advisory locks: "
            f"{settings.test_workspace_ids}"
        )

    await init_pool()
    db = pool_status()
    if db["connected"]:
        logger.info("🏰 Fortress API started — database connected")
        # Ensure activity_log table exists on startup
        try:
            from fortress.api.routes.activity import _ensure_table
            await _ensure_table()
        except Exception as e:
            logger.warning("Could not ensure activity_log table at startup: %s", e)
        # ── One-time migration: TIMESTAMP → TIMESTAMPTZ ────────────────────
        # Existing columns stored naive timestamps (no timezone info).
        # This converts them so the frontend can correctly display local time.
        # Idempotent: only runs ALTER if the column is still 'timestamp without time zone'.
        # Ensure pg_trgm extension + trigram index for fast ILIKE search
        try:
            from fortress.api.db import get_conn
            async with get_conn() as conn:
                await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
                await conn.execute("CREATE EXTENSION IF NOT EXISTS unaccent")
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_companies_denomination_trgm
                    ON companies USING gin (denomination gin_trgm_ops)
                """)
                await conn.execute(
                    """CREATE INDEX IF NOT EXISTS idx_companies_enseigne_trgm
                       ON companies USING gin (enseigne gin_trgm_ops)"""
                )
                await conn.commit()
                logger.info("✅ pg_trgm extension + trigram index ready")
        except Exception as e:
            logger.warning("Could not create trigram index at startup: %s", e)
        # Ensure contact_requests table exists
        try:
            from fortress.api.db import get_conn
            async with get_conn() as conn:
                # Prevent deadlocks with concurrent queries during deploy
                await conn.execute("SET lock_timeout = '5s'")

                # ── TIMESTAMPTZ migration (moved here to share single connection) ──
                cols_to_migrate = await (await conn.execute("""
                    SELECT table_name, column_name
                    FROM information_schema.columns
                    WHERE table_schema = 'public'
                      AND data_type = 'timestamp without time zone'
                    ORDER BY table_name, column_name
                """)).fetchall()
                if cols_to_migrate:
                    for col in cols_to_migrate:
                        tbl = col[0]
                        cname = col[1]
                        await conn.execute(
                            f'ALTER TABLE "{tbl}" ALTER COLUMN "{cname}" TYPE TIMESTAMPTZ'
                        )
                        logger.info("  TIMESTAMPTZ: %s.%s migrated", tbl, cname)
                    logger.info("Timezone migration complete -- %d columns upgraded", len(cols_to_migrate))
                else:
                    logger.info("Timezone migration: all columns already TIMESTAMPTZ")

                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS contact_requests (
                        id SERIAL PRIMARY KEY,
                        name VARCHAR(200) NOT NULL,
                        email VARCHAR(200) NOT NULL,
                        company VARCHAR(200) DEFAULT '',
                        message TEXT NOT NULL,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS bug_reports (
                        id SERIAL PRIMARY KEY,
                        username VARCHAR(100) NOT NULL,
                        role VARCHAR(20),
                        workspace_id INTEGER,
                        description TEXT NOT NULL,
                        context TEXT,
                        screenshot_name VARCHAR(255),
                        screenshot_data TEXT,
                        page_url TEXT,
                        created_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS company_notes (
                        id SERIAL PRIMARY KEY,
                        siren VARCHAR(9) NOT NULL,
                        user_id INTEGER,
                        username VARCHAR(100),
                        text TEXT NOT NULL,
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_notes_siren ON company_notes (siren)")
                await conn.execute("ALTER TABLE contacts ADD COLUMN IF NOT EXISTS social_instagram TEXT")
                await conn.execute("ALTER TABLE contacts ADD COLUMN IF NOT EXISTS social_tiktok TEXT")
                await conn.execute("ALTER TABLE contacts ADD COLUMN IF NOT EXISTS match_confidence VARCHAR(10)")
                await conn.execute("ALTER TABLE contacts ADD COLUMN IF NOT EXISTS siren_from_website VARCHAR(9)")
                await conn.execute("ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS shortfall_reason TEXT")
                await conn.execute("ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS current_query TEXT")
                await conn.execute("ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS queries_json JSONB")
                await conn.execute("ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS completed_queries_count INTEGER DEFAULT 0")
                await conn.execute("ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS time_cap_per_query_min INTEGER")
                await conn.execute("ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS time_cap_total_min INTEGER")
                await conn.execute("ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS current_widening_json JSONB")
                await conn.execute("ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS strict_naf BOOLEAN DEFAULT FALSE")
                await conn.execute("ALTER TABLE companies ADD COLUMN IF NOT EXISTS strict_match BOOLEAN")
                await conn.execute("ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS entity_cap_confirmed INTEGER")
                await conn.execute("ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS timing_breakdown JSONB")
                await conn.execute("ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS resume_attempt_count INTEGER DEFAULT 0")

                # Index for Enrichment History timeline rendering performance
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_log_siren_time ON batch_log (siren, timestamp DESC)")

                # Widen blacklist siren column to accept longer identifiers (e.g. MAPS IDs)
                await conn.execute("""
                    DO $$ BEGIN
                        ALTER TABLE blacklisted_sirens ALTER COLUMN siren TYPE VARCHAR(20);
                    EXCEPTION WHEN others THEN NULL;
                    END $$;
                """)

                # ── Table + column rename migration ─────────────────────────
                # scrape_jobs → batch_data, scrape_audit → batch_log, query_tags → batch_tags
                # Columns: query_id → batch_id, query_name → batch_name
                # Idempotent: only runs if old table names still exist.
                tables_exist = await (await conn.execute("""
                    SELECT table_name FROM information_schema.tables
                    WHERE table_schema = 'public'
                      AND table_name IN ('scrape_jobs', 'scrape_audit', 'query_tags')
                """)).fetchall()
                old_tables = {r[0] for r in tables_exist} if tables_exist else set()

                if 'scrape_jobs' in old_tables:
                    await conn.execute("ALTER TABLE scrape_jobs RENAME TO batch_data")
                    await conn.execute("ALTER TABLE batch_data RENAME COLUMN query_id TO batch_id")
                    await conn.execute("ALTER TABLE batch_data RENAME COLUMN query_name TO batch_name")
                    # Rename indexes
                    await conn.execute("ALTER INDEX IF EXISTS idx_scrape_jobs_query_id RENAME TO idx_batch_data_batch_id")
                    await conn.execute("ALTER INDEX IF EXISTS idx_scrape_jobs_status RENAME TO idx_batch_data_status")
                    logger.info("✅ Renamed scrape_jobs → batch_data (+ columns)")

                if 'scrape_audit' in old_tables:
                    await conn.execute("ALTER TABLE scrape_audit RENAME TO batch_log")
                    await conn.execute("ALTER TABLE batch_log RENAME COLUMN query_id TO batch_id")
                    # Rename indexes
                    await conn.execute("ALTER INDEX IF EXISTS idx_audit_query RENAME TO idx_batch_log_batch_id")
                    await conn.execute("ALTER INDEX IF EXISTS idx_audit_siren RENAME TO idx_batch_log_siren")
                    await conn.execute("ALTER INDEX IF EXISTS idx_audit_action RENAME TO idx_batch_log_action")
                    logger.info("✅ Renamed scrape_audit → batch_log (+ columns)")

                if 'query_tags' in old_tables:
                    await conn.execute("ALTER TABLE query_tags RENAME TO batch_tags")
                    await conn.execute("ALTER TABLE batch_tags RENAME COLUMN query_name TO batch_name")
                    await conn.execute("ALTER INDEX IF EXISTS idx_query_tags_query RENAME TO idx_batch_tags_batch_name")
                    logger.info("✅ Renamed query_tags → batch_tags (+ columns)")

                # Also rename enrichment_log.query_id → batch_id if needed
                if old_tables:  # only run if we just renamed something
                    try:
                        await conn.execute("ALTER TABLE enrichment_log RENAME COLUMN query_id TO batch_id")
                        await conn.execute("ALTER INDEX IF EXISTS idx_enrichment_log_query RENAME TO idx_enrichment_log_batch_id")
                        logger.info("✅ Renamed enrichment_log.query_id → batch_id")
                    except Exception:
                        pass  # column already renamed

                # ── One-time data fix: restore corrupted contacts ────────
                # PAROT VI (309467884): remove bad manual_edit with Medina's phone
                await conn.execute(
                    "DELETE FROM contacts WHERE siren = '309467884' AND source = 'manual_edit'"
                )
                # MEDINA (MAPS00005): remove bad website_crawl with anthedesign data
                await conn.execute(
                    "DELETE FROM contacts WHERE siren = 'MAPS00005' AND source = 'website_crawl'"
                )
                # MAPS00405: remove bad phone extracted from tracking ID
                await conn.execute(
                    "DELETE FROM contacts WHERE siren = 'MAPS00405' AND phone = '0105136289' AND source = 'website_crawl'"
                )
                logger.info("✅ Data fix: restored PAROT VI + MEDINA contacts")

                # ── Entity linking columns ────────────────────────────
                for col, col_type in [
                    ("linked_siren", "TEXT"),
                    ("link_confidence", "TEXT"),
                    ("link_method", "TEXT"),
                    ("resultat_net", "BIGINT"),
                    ("naf_status", "TEXT"),  # 'verified' | 'mismatch' | 'maps_only' | 'no_filter' | NULL (legacy)
                    ("link_signals", "JSONB"),  # {siren_website_match, phone_match, address_match, enseigne_match} — NULL for pre-Phase-A rows
                ]:
                    await conn.execute(
                        f"ALTER TABLE companies ADD COLUMN IF NOT EXISTS {col} {col_type} DEFAULT NULL"
                    )

                # Index for fast address matching on 14.7M rows
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_companies_dept_addr
                    ON companies (departement, LOWER(adresse))
                """)

                # ── INPI-harvested fields (Phase 2) ───────────────────
                for col, col_type in [
                    ("categorie_entreprise", "TEXT"),
                    ("nature_juridique", "TEXT"),
                    ("date_creation_inpi", "DATE"),
                    ("date_fermeture", "DATE"),
                    ("etat_administratif_inpi", "VARCHAR(1)"),
                    ("nombre_etablissements_ouverts", "INTEGER"),
                ]:
                    await conn.execute(
                        f"ALTER TABLE companies ADD COLUMN IF NOT EXISTS {col} {col_type} DEFAULT NULL"
                    )

                # Partial index for dead-company export filter
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_companies_etat_inpi
                    ON companies (etat_administratif_inpi)
                    WHERE etat_administratif_inpi = 'F'
                """)

                # ── Rescue tracking (denormalized for fast export filter) ─
                await conn.execute(
                    "ALTER TABLE companies ADD COLUMN IF NOT EXISTS rescued_by TEXT DEFAULT NULL"
                )
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_companies_rescued_by
                    ON companies (rescued_by)
                    WHERE rescued_by IS NOT NULL
                """)

                # Officers — birth year for disambiguation
                await conn.execute(
                    "ALTER TABLE officers ADD COLUMN IF NOT EXISTS annee_naissance VARCHAR(4) DEFAULT NULL"
                )

                # ── Workspace isolation ──────────────────────────────
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS workspaces (
                        id         SERIAL PRIMARY KEY,
                        name       VARCHAR(100) NOT NULL UNIQUE,
                        created_at TIMESTAMPTZ  NOT NULL DEFAULT NOW()
                    )
                """)
                await conn.execute("ALTER TABLE users       ADD COLUMN IF NOT EXISTS workspace_id INTEGER REFERENCES workspaces(id)")
                await conn.execute("ALTER TABLE batch_data  ADD COLUMN IF NOT EXISTS workspace_id INTEGER REFERENCES workspaces(id)")
                await conn.execute("ALTER TABLE batch_log   ADD COLUMN IF NOT EXISTS workspace_id INTEGER")
                await conn.execute("ALTER TABLE batch_tags  ADD COLUMN IF NOT EXISTS workspace_id INTEGER")
                await conn.execute("ALTER TABLE batch_tags  ADD COLUMN IF NOT EXISTS batch_id TEXT")
                await conn.execute("ALTER TABLE companies   ADD COLUMN IF NOT EXISTS workspace_id INTEGER")
                await conn.execute("ALTER TABLE company_notes ADD COLUMN IF NOT EXISTS approved_by_head BOOLEAN DEFAULT FALSE")
                await conn.execute("ALTER TABLE blacklisted_sirens ADD COLUMN IF NOT EXISTS workspace_id INTEGER")
                await conn.execute("ALTER TABLE contacts    ADD COLUMN IF NOT EXISTS approved_by_head BOOLEAN DEFAULT FALSE")
                await conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS active BOOLEAN DEFAULT TRUE")

                await conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_data_workspace ON batch_data(workspace_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_workspace  ON companies(workspace_id) WHERE workspace_id IS NOT NULL")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_tags_workspace ON batch_tags(workspace_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_tags_siren ON batch_tags(siren)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_contacts_siren ON contacts(siren)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_data_status ON batch_data(status)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_companies_link_confidence ON companies (link_confidence)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_tags_batch_id ON batch_tags (batch_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_batch_tags_upper_batch_name ON batch_tags (UPPER(batch_name))")

                # ── MAPS ID sequence (race-condition-safe) ────────────────
                # IMPORTANT: aggregate is MAX(siren) NOT MAX(SUBSTRING(siren, 5)::INTEGER).
                # Both produce the same value while all MAPS IDs are 9 chars (zero-padded
                # 5-digit suffix), but ONLY MAX(siren) uses companies_pkey via Index Only Scan
                # Backward (~0.05 ms). The legacy MAX(SUBSTRING(...)::INTEGER) form forces a
                # parallel seq scan over 14.7M rows (~11s on cold cache) — see Brief 06.
                #
                # SAFETY TRIPWIRE (defense-in-depth): at MAPS99999 the format f"MAPS{next_id:05d}"
                # would produce a 10-char MAPS100000. The schema bounds this — companies.siren
                # is varchar(9), so the INSERT itself raises StringDataRightTruncation. A 10-char
                # MAPS row CANNOT exist in the table with the current schema, which means lex-max
                # divergence from numeric-max cannot manifest in practice. Today: 6,590 rows;
                # runway to MAPS99999: ~14 years at current rate. The guard below is paranoia +
                # forward documentation: if a future schema-widening migration ever lands without
                # auditing this file, the WARNING + slow-path fallback gives us a graceful "log
                # and continue" instead of silent breakage.
                await conn.execute("CREATE SEQUENCE IF NOT EXISTS maps_id_seq")
                _maps_overflow_check = await (await conn.execute("""
                    SELECT EXISTS(
                        SELECT 1 FROM companies
                        WHERE siren ~ '^MAPS[0-9]{6,}$'
                    )
                """)).fetchone()
                if _maps_overflow_check and _maps_overflow_check[0]:
                    logger.warning(
                        "⚠ MAPS ID overflow detected: a row with 6+ digit suffix exists. "
                        "The lex-max → numeric-max equivalence no longer holds. "
                        "Falling back to legacy SUBSTRING-based aggregate (slow, ~11s). "
                        "Action required: widen the f-string format and re-pad existing rows."
                    )
                    await conn.execute("""
                        SELECT setval('maps_id_seq',
                            COALESCE(
                                (SELECT MAX(CAST(SUBSTRING(siren FROM 5) AS INTEGER))
                                 FROM companies WHERE siren LIKE 'MAPS%%'),
                                0
                            ) + 1,
                            false
                        )
                    """)
                else:
                    await conn.execute("""
                        SELECT setval('maps_id_seq',
                            COALESCE(
                                CAST(SUBSTRING(
                                    (SELECT MAX(siren)
                                     FROM companies WHERE siren LIKE 'MAPS%%')
                                    FROM 5
                                ) AS INTEGER),
                                0
                            ) + 1,
                            false
                        )
                    """)

                # Backfill batch_tags.batch_id from batch_data (idempotent — only updates NULLs)
                await conn.execute("""
                    UPDATE batch_tags bt SET batch_id = bd.batch_id
                    FROM batch_data bd
                    WHERE bt.batch_name = bd.batch_name AND bt.batch_id IS NULL
                """)

                # Seed first workspace if none exists — name-independent, idempotent
                await conn.execute("""
                    INSERT INTO workspaces (name) VALUES ('Cindy')
                    ON CONFLICT DO NOTHING
                """)
                # Assign non-admin users without a workspace to the first workspace
                first_ws = await (await conn.execute(
                    "SELECT id FROM workspaces ORDER BY id LIMIT 1"
                )).fetchone()
                if first_ws:
                    ws_id = first_ws[0]
                    await conn.execute("""
                        UPDATE users SET workspace_id = %s
                        WHERE workspace_id IS NULL AND role != 'admin'
                    """, (ws_id,))
                    await conn.execute("UPDATE users SET role = 'head' WHERE username = 'olivierhaddad' AND role != 'admin'")
                    # Assign MAPS companies created by non-admin users to their workspace
                    await conn.execute("""
                        UPDATE companies SET workspace_id = %s
                        WHERE siren LIKE 'MAPS%%' AND workspace_id IS NULL
                    """, (ws_id,))

                # ── Targeted workspace migration (idempotent) ─────────────────
                # Assign batch_data launched by non-admin users that still have NULL workspace_id
                # to that user's workspace. Admin batches stay NULL.
                await conn.execute("""
                    UPDATE batch_data bd SET workspace_id = u.workspace_id
                    FROM users u WHERE u.id = bd.user_id
                    AND bd.workspace_id IS NULL AND u.workspace_id IS NOT NULL AND u.role != 'admin'
                """)
                # Propagate workspace_id from batch_data down to batch_tags and batch_log
                await conn.execute("""
                    UPDATE batch_tags bt SET workspace_id = bd.workspace_id
                    FROM batch_data bd WHERE bd.batch_id = bt.batch_id
                    AND bt.workspace_id IS NULL AND bd.workspace_id IS NOT NULL
                """)
                await conn.execute("""
                    UPDATE batch_log bl SET workspace_id = bd.workspace_id
                    FROM batch_data bd WHERE bd.batch_id = bl.batch_id
                    AND bl.workspace_id IS NULL AND bd.workspace_id IS NOT NULL
                """)
                logger.info("✅ Workspace isolation migrations complete")

                # ── System log table ────────────────────────────────
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS system_log (
                        id          SERIAL PRIMARY KEY,
                        level       VARCHAR(10) NOT NULL DEFAULT 'ERROR',
                        source      VARCHAR(50) DEFAULT 'api',
                        message     TEXT NOT NULL,
                        traceback   TEXT,
                        path        TEXT,
                        created_at  TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_system_log_time ON system_log (created_at DESC)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_system_log_level ON system_log (level)")

                # Clean up entries older than 30 days
                await conn.execute("DELETE FROM system_log WHERE created_at < NOW() - INTERVAL '30 days'")

                # ── batch_tags enrichment tracking ───────────────────
                await conn.execute("""
                    ALTER TABLE batch_tags
                    ADD COLUMN IF NOT EXISTS created_at TIMESTAMPTZ DEFAULT NOW()
                """)

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

                # ── Query Memory table ────────────────────────────────
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS query_memory (
                        id SERIAL PRIMARY KEY,
                        workspace_id INTEGER,
                        sector_word TEXT NOT NULL,
                        dept_code TEXT NOT NULL,
                        query_text TEXT NOT NULL,
                        is_expansion BOOLEAN NOT NULL DEFAULT true,
                        cards_found INTEGER NOT NULL DEFAULT 0,
                        new_companies INTEGER NOT NULL DEFAULT 0,
                        batch_id TEXT,
                        executed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_query_memory_lookup
                    ON query_memory(workspace_id, sector_word, dept_code)
                """)

                # ── Index to accelerate /api/dashboard/top-queries ────
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_query_memory_recent
                    ON query_memory(workspace_id, executed_at DESC)
                    WHERE new_companies > 0
                """)

                # ── RGPD opposition table ────────────────────────────────
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS rgpd_oppositions (
                        id SERIAL PRIMARY KEY,
                        nom VARCHAR(200),
                        prenom VARCHAR(200),
                        email VARCHAR(200),
                        telephone VARCHAR(50),
                        motif TEXT NOT NULL,
                        result_summary TEXT,
                        processed_by VARCHAR(100),
                        processed_at TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_rgpd_oppositions_email
                    ON rgpd_oppositions (LOWER(email))
                """)

                # ── Pipeline instrumentation table ───────────────────────
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS pipeline_timings (
                        id           BIGSERIAL PRIMARY KEY,
                        batch_id     INTEGER REFERENCES batch_data(id) ON DELETE CASCADE,
                        siren        VARCHAR(20),
                        step         VARCHAR(40) NOT NULL,
                        duration_ms  INTEGER NOT NULL,
                        fired        BOOLEAN NOT NULL DEFAULT TRUE,
                        created_at   TIMESTAMPTZ DEFAULT NOW()
                    )
                """)
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_timings_batch ON pipeline_timings(batch_id)")
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_pipeline_timings_step ON pipeline_timings(step)")

                # ── RGPD data retention cleanup ──────────────────────────
                # Contact form submissions: 12 months (lead capture, consent-based)
                await conn.execute("DELETE FROM contact_requests WHERE created_at < NOW() - INTERVAL '12 months'")
                # Activity logs: 12 months (operational audit, not business data)
                await conn.execute("DELETE FROM activity_log WHERE created_at < NOW() - INTERVAL '12 months'")
                # Bug reports: 90 days (dev debugging, screenshots are heavy)
                await conn.execute("DELETE FROM bug_reports WHERE created_at < NOW() - INTERVAL '90 days'")

                # ── Drop FK on batch_log.siren (A2 audit observability fix) ──────
                # batch_log is an audit table — free-text sentinel SIRENs like
                # "A2PENDING" or "FILTERED_xxx" are valid audit content.
                # The FK adds no value and silently blocks observability writes.
                await conn.execute(
                    "ALTER TABLE batch_log DROP CONSTRAINT IF EXISTS batch_log_siren_fkey"
                )

                # ── TOP 3 widening: widen batch_log.siren to VARCHAR(50) ────────
                # Sentinel rows like "WIDEN_66_PERPIGNAN" exceed the 9-char VARCHAR(9)
                # default. ALTER COLUMN TYPE is idempotent when already widened.
                # Also fixes the Apr 25 silent-fail for FILTERED_* and A2PENDING rows.
                await conn.execute("ALTER TABLE batch_log ALTER COLUMN siren TYPE VARCHAR(50)")

                # ── SIRET-level establishments side table ────────────────────
                # Stores per-establishment NAF + address from the INSEE StockEtablissement
                # file (~30M active SIRETs). Enables Step 2.7 siret_address_naf matcher
                # to find businesses whose operating SIRET has a different NAF than the
                # SIREN head SIRET (communes running municipal services, multi-site SCIs,
                # franchise storefronts under a regional HQ, etc.).
                await conn.execute(
                    """
                    CREATE TABLE IF NOT EXISTS establishments (
                        siret VARCHAR(14) PRIMARY KEY,
                        siren VARCHAR(9) NOT NULL,
                        etablissement_siege BOOLEAN NOT NULL DEFAULT FALSE,
                        etat_administratif VARCHAR(1) NOT NULL DEFAULT 'A',
                        enseigne_etablissement TEXT,
                        denomination_usuelle TEXT,
                        naf_etablissement VARCHAR(10),
                        code_postal_etab VARCHAR(10),
                        adresse_etab TEXT,
                        libelle_commune VARCHAR(100),
                        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
                        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
                    )
                    """
                )
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_establishments_siren "
                    "ON establishments(siren)"
                )
                await conn.execute(
                    "CREATE INDEX IF NOT EXISTS idx_establishments_cp_naf "
                    "ON establishments(code_postal_etab, naf_etablissement)"
                )

                await conn.commit()
                logger.info("✅ contact_requests and company_notes tables ready")
        except Exception as e:
            logger.warning("Could not create dynamic tables: %s", e)

        # ── Startup catch-up — auto-resume interrupted, then spawn queued ──
        # Order: interrupted batches first (they had work; restarting Cindy's
        # wine batch is more valuable than starting a fresh queued one).
        # If no resumable interrupted batch, fall through to oldest queued.
        # Single-spawn semantics preserved: max ONE subprocess spawned per
        # startup pass, regardless of which path fires.
        try:
            from fortress.api.db import get_conn
            import subprocess, sys as _sys, os as _os
            from pathlib import Path as _Path
            async with get_conn() as _conn:
                _running_cur = await _conn.execute(
                    "SELECT 1 FROM batch_data WHERE status='in_progress' LIMIT 1"
                )
                _has_running = await _running_cur.fetchone()
                if _has_running:
                    pass  # something already in flight — neither path fires
                else:
                    # Path 1: try interrupted first
                    _resumable = await _resumable_interrupted_batch_id(_conn)
                    if _resumable is not None:
                        _bid, _ws = _resumable
                        if _sweeper_should_spawn(_bid):
                            await _auto_resume_spawn(_bid, _ws)
                        else:
                            logger.info(
                                "Catch-up: skip auto-resume (guard) batch=%s",
                                _bid,
                            )
                    else:
                        # Path 2: fall back to oldest queued (legacy behavior)
                        _cur = await _conn.execute(
                            """SELECT batch_id, workspace_id FROM batch_data
                               WHERE status = 'queued'
                               ORDER BY created_at ASC LIMIT 1"""
                        )
                        _orphaned = await _cur.fetchall()
                        if _orphaned:
                            _fortress_root = _Path(__file__).resolve().parent.parent
                            for _row in _orphaned:
                                _bid = _row[0] if isinstance(_row, tuple) else _row["batch_id"]
                                _ws = _row[1] if isinstance(_row, tuple) else _row["workspace_id"]
                                _runner_cmd = [_sys.executable, "-m", "fortress.discovery", _bid]
                                _launcher = _Path("/tmp/fortress_launcher.py")
                                if _launcher.exists():
                                    _runner_cmd = [_sys.executable, str(_launcher), "runner", _bid]
                                if not _sweeper_should_spawn(_bid):
                                    logger.info("Catch-up: skip respawn (guard) batch=%s", _bid)
                                    continue
                                try:
                                    subprocess.Popen(
                                        _runner_cmd,
                                        cwd=str(_fortress_root),
                                        stdout=None, stderr=None,
                                        close_fds=False, start_new_session=True,
                                    )
                                    logger.info("✅ Catch-up: spawned orphaned queued batch %s (ws=%s)", _bid, _ws)
                                except Exception as _e:
                                    logger.warning("Catch-up spawn failed for %s: %s", _bid, _e)
        except Exception as _e:
            logger.warning("Catch-up scan failed: %s", _e)
    else:
        logger.warning("🏰 Fortress API started — database OFFLINE: %s", db["error"])

    # ── E2: periodic orphan sweeper (5-min cadence) ──────────────────
    # Recovers from discovery.py SIGKILL/OOM-kill where the finally block
    # didn't fire (per memory reference_render_api.md — Render OOM-kill is
    # the literal "==> Detected service running on port 10000" log line).
    # Every 5 min: if no in_progress batch exists globally, spawn the single
    # oldest queued batch (>10 min old) across all workspaces.
    # Global concurrency cap = 1 (matches batch.py guard and E2 catch-up above).
    async def _periodic_orphan_sweeper():
        import asyncio as _asyncio
        import subprocess as _subprocess
        import sys as _sys
        import os as _os
        from pathlib import Path as _Path
        while True:
            try:
                await _asyncio.sleep(300)  # 5 min
                from fortress.api.db import get_conn as _get_conn
                async with _get_conn() as _swp_conn:
                    _running_cur = await _swp_conn.execute(
                        "SELECT 1 FROM batch_data WHERE status='in_progress' LIMIT 1"
                    )
                    _running = await _running_cur.fetchone()
                    if _running:
                        continue  # something already running — wait

                    # Path 1: auto-resume an interrupted batch first
                    _resumable = await _resumable_interrupted_batch_id(_swp_conn)
                    if _resumable is not None:
                        _bid, _ws = _resumable
                        if _sweeper_should_spawn(_bid):
                            await _auto_resume_spawn(_bid, _ws)
                            continue  # spawned — wait for next cycle
                        logger.info(
                            "🔄 Sweeper: skip auto-resume (guard) batch=%s",
                            _bid,
                        )
                        continue  # in guard window — wait

                    # Path 2: fall back to oldest queued (legacy behavior)
                    _swp_cur = await _swp_conn.execute(
                        """SELECT batch_id, workspace_id FROM batch_data
                           WHERE status = 'queued'
                             AND created_at < NOW() - INTERVAL '10 minutes'
                           ORDER BY created_at ASC LIMIT 1"""
                    )
                    _orphan_rows = await _swp_cur.fetchall()
                if not _orphan_rows:
                    continue
                _fr = _Path(__file__).resolve().parent.parent
                for _pr in _orphan_rows:
                    _next_bid = _pr[0] if isinstance(_pr, tuple) else _pr.get("batch_id")
                    _ws_id = _pr[1] if isinstance(_pr, tuple) else _pr.get("workspace_id")
                    _cmd = [_sys.executable, "-m", "fortress.discovery", _next_bid]
                    _lc = _Path("/tmp/fortress_launcher.py")
                    if _lc.exists():
                        _cmd = [_sys.executable, str(_lc), "runner", _next_bid]
                    if not _sweeper_should_spawn(_next_bid):
                        logger.info("🔄 Sweeper: skip respawn (guard) batch=%s", _next_bid)
                        continue
                    try:
                        _subprocess.Popen(
                            _cmd, cwd=str(_fr),
                            stdout=None, stderr=None,
                            close_fds=False, start_new_session=True,
                        )
                        logger.info("🔄 Sweeper: spawned orphan queued batch %s (ws=%s)", _next_bid, _ws_id)
                    except Exception as _e:
                        logger.warning("Sweeper spawn failed for %s: %s", _next_bid, _e)
            except Exception as _e:
                # Never let the sweeper crash — just log and loop
                logger.warning("orphan_sweeper_error: %s", _e)

    import asyncio as _asyncio_top
    _sweeper_task = _asyncio_top.create_task(_periodic_orphan_sweeper())

    # ── Web-service memory observability (Diagnostic A+B, May 1) ─────
    # Logs RSS of the web service process + summary of all python procs
    # in the container every 60s. Pure observability — no behavior
    # change, no schema change, no endpoint change. Companion to the
    # per-batch-subprocess heartbeat at discovery.py:2380.
    # Threshold 1500 MB = 75% of Render Standard plan's 2 GiB cap.
    _PYTHON_PROC_NAMES = ("python", "python3", "python3.13", "uvicorn", "chrome", "chromium", "chromium-browser", "node")

    async def _web_heartbeat():
        while True:
            try:
                rss_mb = psutil.Process().memory_info().rss / 1024 / 1024
                logger.info("web.heartbeat_rss rss_mb=%.1f", rss_mb)
                if rss_mb > 1500:
                    logger.warning(
                        "web.heartbeat_rss_high rss_mb=%.1f", rss_mb
                    )
            except Exception as _hb_exc:
                logger.debug("web.heartbeat_rss_failed err=%s", _hb_exc)

            # Process summary — total RSS + count of python processes
            try:
                total_rss = 0
                proc_count = 0
                for _proc in psutil.process_iter(['name', 'memory_info']):
                    try:
                        if _proc.info['name'] in _PYTHON_PROC_NAMES:
                            total_rss += _proc.info['memory_info'].rss
                            proc_count += 1
                    except (psutil.NoSuchProcess, psutil.AccessDenied):
                        continue
                logger.info(
                    "web.process_summary total_proc_rss_mb=%.1f "
                    "proc_count=%d",
                    total_rss / 1024 / 1024, proc_count,
                )
            except Exception as _ps_exc:
                logger.debug("web.process_summary_failed err=%s", _ps_exc)

            await _asyncio_top.sleep(60)

    _web_hb_task = _asyncio_top.create_task(_web_heartbeat())

    try:
        yield
    finally:
        _sweeper_task.cancel()
        _web_hb_task.cancel()
        try:
            await _sweeper_task
        except (Exception, BaseException):
            pass
        try:
            await _web_hb_task
        except (Exception, BaseException):
            pass
        await close_pool()


app = FastAPI(
    title="Fortress API",
    description="B2B Lead Collection Dashboard API",
    version="1.0.0",
    lifespan=lifespan,
)

app.state.limiter = limiter


async def _french_rate_limit_handler(request: Request, exc: RateLimitExceeded):
    return JSONResponse(
        status_code=429,
        content={"error": "Trop de tentatives. Réessayez dans quelques minutes."},
    )


app.add_exception_handler(RateLimitExceeded, _french_rate_limit_handler)


async def _log_system_error(level: str, message: str, source: str = "api", traceback: str = None, path: str = None):
    """Write an error to system_log. Fire-and-forget — never crashes the caller."""
    try:
        from fortress.api.db import get_conn
        async with get_conn() as conn:
            await conn.execute(
                """INSERT INTO system_log (level, source, message, traceback, path)
                   VALUES (%s, %s, %s, %s, %s)""",
                (level, source, message, traceback, path),
            )
            await conn.commit()
    except Exception:
        pass  # If DB is down, we can't log — fall back to console only


# Map database errors → 503 Service Unavailable
@app.exception_handler(RuntimeError)
async def _runtime_error_handler(request: Request, exc: RuntimeError):
    if "database offline" in str(exc).lower():
        await _log_system_error(level="WARNING", message=str(exc), source="database", path=str(request.url.path))
        return JSONResponse(
            status_code=503,
            content={"error": "Base de données hors ligne. Réessayez dans quelques instants."},
        )
    raise exc


import psycopg


@app.exception_handler(psycopg.OperationalError)
async def _pg_operational_handler(request: Request, exc: psycopg.OperationalError):
    logger.error("Database connection error: %s", exc)
    await _log_system_error(level="WARNING", message=str(exc), source="database", path=str(request.url.path))
    return JSONResponse(
        status_code=503,
        content={"error": "Base de données inaccessible. Réessayez dans quelques instants."},
    )


@app.exception_handler(psycopg.InterfaceError)
async def _pg_interface_handler(request: Request, exc: psycopg.InterfaceError):
    logger.error("Database interface error: %s", exc)
    await _log_system_error(level="WARNING", message=str(exc), source="database", path=str(request.url.path))
    return JSONResponse(
        status_code=503,
        content={"error": "Connexion à la base de données perdue. Réessayez dans quelques instants."},
    )


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Catch any unhandled exception, log to system_log, return 500."""
    import traceback as _tb
    tb = _tb.format_exc()
    logger.error(f"Unhandled exception on {request.url.path}: {exc}")
    await _log_system_error(
        level="ERROR",
        message=str(exc),
        source="api",
        traceback=tb,
        path=str(request.url.path),
    )
    return JSONResponse(status_code=500, content={"error": "Erreur interne du serveur."})


# ── Session Auth Middleware ─────────────────────────────────────────
# Protects all /api/* routes except public paths.
# Reads the session cookie, decodes it, and attaches user to request.state.

_PUBLIC_PATHS = {
    "/api/health",
    "/api/auth/check",
    "/api/auth/login",
    "/api/auth/logout",
    "/api/contact",
    "/api/internal/notify-batch-complete",
}

_COOKIE_NAME = "fortress_session"


class SessionAuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path

        # Only protect /api/* routes (not static files)
        if path.startswith("/api/") and path not in _PUBLIC_PATHS:
            token = request.cookies.get(_COOKIE_NAME)
            if not token:
                return JSONResponse(
                    status_code=401,
                    content={"error": "Authentification requise."},
                )
            user = decode_session_token(token)
            if not user:
                response = JSONResponse(
                    status_code=401,
                    content={"error": "Session expirée. Veuillez vous reconnecter."},
                )
                response.delete_cookie(_COOKIE_NAME)
                return response
            # Attach user to request so routes can access it
            request.state.user = user
        else:
            request.state.user = None

        return await call_next(request)


app.add_middleware(SessionAuthMiddleware)
logger.info("🔐 Session-based authentication ENABLED")


class CacheControlMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        response = await call_next(request)
        path = request.url.path
        if path.startswith('/js/') or path.startswith('/css/'):
            response.headers['Cache-Control'] = 'no-cache, must-revalidate'
        return response


app.add_middleware(CacheControlMiddleware)


class SecurityHeadersMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: StarletteRequest, call_next):
        response = await call_next(request)
        response.headers["Strict-Transport-Security"] = "max-age=31536000; includeSubDomains"
        response.headers["X-Frame-Options"] = "DENY"
        response.headers["X-Content-Type-Options"] = "nosniff"
        response.headers["Referrer-Policy"] = "strict-origin-when-cross-origin"
        response.headers["Permissions-Policy"] = "camera=(), microphone=(), geolocation=()"
        response.headers["Content-Security-Policy"] = (
            "default-src 'self'; "
            "script-src 'self' 'unsafe-inline' 'unsafe-eval' https://cdn.sheetjs.com https://html2canvas.hertzen.com; "
            "style-src 'self' 'unsafe-inline'; "
            "img-src 'self' data: https:; "
            "connect-src 'self' wss: https:; "
            "font-src 'self' data:"
        )
        return response


app.add_middleware(SecurityHeadersMiddleware)


# CORS — restrict to configured frontend origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Register API routers
app.include_router(auth_routes.router)
app.include_router(health.router)
app.include_router(dashboard.router)
app.include_router(departments.router)
app.include_router(jobs.router)
app.include_router(companies.router)
app.include_router(export.router)
app.include_router(batch.router)
app.include_router(client.router)
app.include_router(sirene.router)
app.include_router(admin.router)
app.include_router(notes.router)
app.include_router(contacts_list.router)
app.include_router(contact.router)
app.include_router(activity.router)
app.include_router(blacklist.router, prefix="/api/blacklist")
app.include_router(bug_report.router)

from fortress.api.routes.websocket import router as websocket_router
app.include_router(websocket_router)

# Serve frontend static files
_frontend_dir = Path(__file__).parent.parent / "frontend"
if _frontend_dir.exists():
    app.mount("/", StaticFiles(directory=str(_frontend_dir), html=True), name="frontend")


if __name__ == "__main__":
    import webbrowser

    import uvicorn

    webbrowser.open("http://localhost:8080")
    uvicorn.run("fortress.api.main:app", host="0.0.0.0", port=8080, reload=True)
