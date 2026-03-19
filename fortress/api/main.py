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

from fortress.api.auth import decode_session_token
from fortress.api.db import close_pool, init_pool, pool_status
from fortress.api.routes import activity, admin, auth as auth_routes, batch, client, companies, contact, contacts_list, dashboard, departments, export, health, jobs, notes, sirene
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
                        created_at TIMESTAMP DEFAULT NOW()
                    )
                """)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS company_notes (
                        id SERIAL PRIMARY KEY,
                        siren VARCHAR(9) NOT NULL,
                        user_id INTEGER,
                        username VARCHAR(100),
                        text TEXT NOT NULL,
                        created_at TIMESTAMP NOT NULL DEFAULT NOW()
                    )
                """)
                await conn.execute("CREATE INDEX IF NOT EXISTS idx_notes_siren ON company_notes (siren)")
                # Social media columns — ensure Instagram + TikTok exist
                await conn.execute("ALTER TABLE contacts ADD COLUMN IF NOT EXISTS social_instagram TEXT")
                await conn.execute("ALTER TABLE contacts ADD COLUMN IF NOT EXISTS social_tiktok TEXT")

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

# Serve frontend static files
_frontend_dir = Path(__file__).parent.parent / "frontend"
if _frontend_dir.exists():
    app.mount("/", StaticFiles(directory=str(_frontend_dir), html=True), name="frontend")


if __name__ == "__main__":
    import webbrowser

    import uvicorn

    webbrowser.open("http://localhost:8080")
    uvicorn.run("fortress.api.main:app", host="0.0.0.0", port=8080, reload=True)
