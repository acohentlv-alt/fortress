"""Fortress API — FastAPI entry point.

Run with:
    python -m fortress.api.main

Serves:
    - /api/* endpoints for data access
    - / static files for the frontend SPA
"""

from contextlib import asynccontextmanager
import logging
from pathlib import Path

from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from starlette.middleware.base import BaseHTTPMiddleware

from fortress.api.db import close_pool, init_pool, pool_status
from fortress.api.routes import batch, client, companies, dashboard, departments, export, health, jobs
from fortress.config.settings import settings

logger = logging.getLogger("fortress.api")


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Startup/shutdown lifecycle — resilient to DB failures."""
    await init_pool()
    db = pool_status()
    if db["connected"]:
        logger.info("🏰 Fortress API started — database connected")
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


# ── API Key Auth Middleware ─────────────────────────────────────────
# Enabled when FORTRESS_API_KEY is set in .env or environment.
# Protects all /api/* routes except /api/health and /api/auth/check.
# Frontend sends the key via X-API-Key header (stored in localStorage).

_PUBLIC_PATHS = {"/api/health", "/api/auth/check", "/api/auth/login"}


class ApiKeyMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        # Only protect /api/* routes (not static files)
        if settings.api_key and path.startswith("/api/") and path not in _PUBLIC_PATHS:
            key = request.headers.get("x-api-key") or request.query_params.get("api_key")
            if key != settings.api_key:
                return JSONResponse(
                    status_code=401,
                    content={"error": "Clé API invalide ou manquante."},
                )
        return await call_next(request)


if settings.api_key:
    app.add_middleware(ApiKeyMiddleware)
    logger.info("🔐 API key protection ENABLED")
else:
    logger.info("🔓 API key protection DISABLED (set FORTRESS_API_KEY to enable)")


# CORS — restrict to configured frontend origin
app.add_middleware(
    CORSMiddleware,
    allow_origins=[settings.frontend_url],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ── Auth endpoints ─────────────────────────────────────────────────

@app.get("/api/auth/check")
async def auth_check():
    """Returns whether auth is required."""
    return {"auth_required": bool(settings.api_key)}


@app.post("/api/auth/login")
async def auth_login(request: Request):
    """Validate an API key. Returns ok if valid."""
    body = await request.json()
    key = body.get("api_key", "")
    if not settings.api_key:
        return {"status": "ok", "message": "Auth non requise."}
    if key == settings.api_key:
        return {"status": "ok"}
    return JSONResponse(status_code=401, content={"error": "Clé API invalide."})


# Register API routers
app.include_router(health.router)
app.include_router(dashboard.router)
app.include_router(departments.router)
app.include_router(jobs.router)
app.include_router(companies.router)
app.include_router(export.router)
app.include_router(batch.router)
app.include_router(client.router)

# Serve frontend static files
_frontend_dir = Path(__file__).parent.parent / "frontend"
if _frontend_dir.exists():
    app.mount("/", StaticFiles(directory=str(_frontend_dir), html=True), name="frontend")


if __name__ == "__main__":
    import webbrowser

    import uvicorn

    webbrowser.open("http://localhost:8080")
    uvicorn.run("fortress.api.main:app", host="0.0.0.0", port=8080, reload=True)
