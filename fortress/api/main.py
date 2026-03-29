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

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from starlette.middleware.base import BaseHTTPMiddleware
from starlette.requests import Request as StarletteRequest

from fortress.api.auth import decode_session_token
from fortress.api.db import close_pool, init_pool, pool_status
from fortress.api.routes import activity, admin, auth as auth_routes, batch, blacklist, client, companies, contact, contacts_list, dashboard, departments, export, health, jobs, notes, sirene
from fortress.config.settings import settings

logger = logging.getLogger("fortress.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle — resilient to DB failures."""
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
        try:
            from fortress.api.db import get_conn, fetch_all
            cols_to_migrate = await fetch_all("""
                SELECT table_name, column_name
                FROM information_schema.columns
                WHERE table_schema = 'public'
                  AND data_type = 'timestamp without time zone'
                ORDER BY table_name, column_name
            """)
            if cols_to_migrate:
                async with get_conn() as conn:
                    for col in cols_to_migrate:
                        tbl = col['table_name']
                        cname = col['column_name']
                        await conn.execute(
                            f'ALTER TABLE "{tbl}" ALTER COLUMN "{cname}" TYPE TIMESTAMPTZ'
                        )
                        logger.info("  ✅ %s.%s → TIMESTAMPTZ", tbl, cname)
                    await conn.commit()
                logger.info("✅ Timezone migration complete — %d columns upgraded", len(cols_to_migrate))
            else:
                logger.info("✅ Timezone migration: all columns already TIMESTAMPTZ")
        except Exception as e:
            logger.warning("Timezone migration failed (non-fatal): %s", e)
        # Ensure pg_trgm extension + trigram index for fast ILIKE search
        try:
            from fortress.api.db import get_conn
            async with get_conn() as conn:
                await conn.execute("CREATE EXTENSION IF NOT EXISTS pg_trgm")
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_companies_denomination_trgm
                    ON companies USING gin (denomination gin_trgm_ops)
                """)
                await conn.commit()
                logger.info("✅ pg_trgm extension + trigram index ready")
        except Exception as e:
            logger.warning("Could not create trigram index at startup: %s", e)
        # Ensure contact_requests table exists
        try:
            from fortress.api.db import get_conn
            async with get_conn() as conn:
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
                await conn.execute("ALTER TABLE batch_data ADD COLUMN IF NOT EXISTS shortfall_reason TEXT")
                
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
                logger.info("✅ Data fix: restored PAROT VI + MEDINA contacts")

                # ── Entity linking columns ────────────────────────────
                for col, col_type in [
                    ("linked_siren", "TEXT"),
                    ("link_confidence", "TEXT"),
                    ("link_method", "TEXT"),
                    ("resultat_net", "BIGINT"),
                ]:
                    await conn.execute(
                        f"ALTER TABLE companies ADD COLUMN IF NOT EXISTS {col} {col_type} DEFAULT NULL"
                    )

                # Index for fast address matching on 14.7M rows
                await conn.execute("""
                    CREATE INDEX IF NOT EXISTS idx_companies_dept_addr
                    ON companies (departement, LOWER(adresse))
                """)

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

                # Backfill batch_tags.batch_id from batch_data (idempotent — only updates NULLs)
                await conn.execute("""
                    UPDATE batch_tags bt SET batch_id = bd.batch_id
                    FROM batch_data bd
                    WHERE bt.batch_name = bd.batch_name AND bt.batch_id IS NULL
                """)

                # Seed first workspace if none exists, and assign orphaned data (idempotent)
                await conn.execute("""
                    INSERT INTO workspaces (name) VALUES ('Cindy')
                    ON CONFLICT DO NOTHING
                """)
                # Use the first workspace (by id) for backfills — name-independent
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
                    await conn.execute("""
                        UPDATE companies SET workspace_id = %s
                        WHERE siren LIKE 'MAPS%%' AND workspace_id IS NULL
                    """, (ws_id,))
                    await conn.execute("""
                        UPDATE batch_data SET workspace_id = %s
                        WHERE workspace_id IS NULL
                    """, (ws_id,))
                    await conn.execute("""
                        UPDATE batch_log SET workspace_id = %s
                        WHERE workspace_id IS NULL
                    """, (ws_id,))
                    await conn.execute("""
                        UPDATE batch_tags SET workspace_id = %s
                        WHERE workspace_id IS NULL
                    """, (ws_id,))
                    await conn.execute("""
                        UPDATE blacklisted_sirens SET workspace_id = %s
                        WHERE workspace_id IS NULL
                    """, (ws_id,))
                logger.info("✅ Workspace isolation migrations complete")

                await conn.commit()
                logger.info("✅ contact_requests and company_notes tables ready")
        except Exception as e:
            logger.warning("Could not create dynamic tables: %s", e)
    else:
        logger.warning("🏰 Fortress API started — database OFFLINE: %s", db["error"])
    yield
    await close_pool()


app = FastAPI(
    title="Fortress API",
    description="B2B Lead Collection Dashboard API",
    version="1.0.0",
    lifespan=lifespan,
)

# Map database errors → 503 Service Unavailable
@app.exception_handler(RuntimeError)
async def _runtime_error_handler(request: Request, exc: RuntimeError):
    if "database offline" in str(exc).lower():
        return JSONResponse(
            status_code=503,
            content={"error": "Base de données hors ligne. Réessayez dans quelques instants."},
        )
    raise exc


import psycopg


@app.exception_handler(psycopg.OperationalError)
async def _pg_operational_handler(request: Request, exc: psycopg.OperationalError):
    logger.error("Database connection error: %s", exc)
    return JSONResponse(
        status_code=503,
        content={"error": "Base de données inaccessible. Réessayez dans quelques instants."},
    )


@app.exception_handler(psycopg.InterfaceError)
async def _pg_interface_handler(request: Request, exc: psycopg.InterfaceError):
    logger.error("Database interface error: %s", exc)
    return JSONResponse(
        status_code=503,
        content={"error": "Connexion à la base de données perdue. Réessayez dans quelques instants."},
    )


# ── Session Auth Middleware ─────────────────────────────────────────
# Protects all /api/* routes except public paths.
# Reads the session cookie, decodes it, and attaches user to request.state.

_PUBLIC_PATHS = {
    "/api/health",
    "/api/auth/check",
    "/api/auth/login",
    "/api/auth/logout",
    "/api/contact",
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
