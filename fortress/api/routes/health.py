"""Health check endpoint — infrastructure status probe.

GET /api/health returns:
  200 OK         → {"status": "ok", "database": "connected"}
  503 Degraded   → {"status": "degraded", "database": "offline", "error": "..."}

Used by: frontend connectivity checks, monitoring tools, load balancers.
"""

from fastapi import APIRouter
from fastapi.responses import JSONResponse

from fortress.api.db import pool_status

router = APIRouter(prefix="/api", tags=["health"])


@router.get("/health")
async def health_check():
    """Return API and database connectivity status.

    Actively probes the pool state (set during init_pool).
    Returns 200 if DB is connected, 503 if degraded.
    """
    db = pool_status()

    if db["connected"]:
        return {
            "status": "ok",
            "database": "connected",
        }
    else:
        return JSONResponse(
            status_code=503,
            content={
                "status": "degraded",
                "database": "offline",
                "error": db["error"],
            },
        )
