"""Async database connection pool for the API layer.

Reuses the same PostgreSQL settings as the rest of Fortress.

Resilient design:
  - init_pool() catches connection errors and logs CRITICAL, but does NOT crash.
  - The pool state is tracked via pool_status() for the health endpoint.
  - All query helpers raise RuntimeError if the pool is offline.
"""

import logging
from contextlib import asynccontextmanager

import psycopg
import psycopg_pool

from fortress.config.settings import settings

logger = logging.getLogger("fortress.api.db")

_pool: psycopg_pool.AsyncConnectionPool | None = None
_pool_error: str | None = None  # Stores the last connection error message


async def init_pool() -> None:
    """Create the async connection pool.

    If the database is unreachable, logs a CRITICAL error and sets the
    pool to None. The app stays alive in degraded mode — the health
    endpoint reports the failure, and all data endpoints return 503.
    """
    global _pool, _pool_error
    _pool_error = None

    try:
        _pool = psycopg_pool.AsyncConnectionPool(
            conninfo=settings.db_url,
            min_size=2,
            max_size=10,
            num_workers=2,
            open=False,
        )
        await _pool.open(wait=True, timeout=5.0)
        _pool_error = None
        logger.info("✅ Database pool initialized: %s", settings.db_url.split("@")[-1])
    except Exception as exc:
        _pool = None
        _pool_error = str(exc)
        logger.critical(
            "🔴 CRITICAL: Database connection failed — API running in DEGRADED mode. "
            "Reason: %s | URL: %s",
            exc,
            settings.db_url.split("@")[-1],  # Hide credentials
        )


async def close_pool() -> None:
    """Close the connection pool."""
    global _pool
    if _pool is not None:
        try:
            await _pool.close()
        except Exception:
            pass
        _pool = None


def pool_status() -> dict:
    """Return the current pool status for the health endpoint.

    Returns:
        {"connected": True/False, "error": str | None}
    """
    return {
        "connected": _pool is not None,
        "error": _pool_error,
    }


@asynccontextmanager
async def get_conn():
    """Yield an async connection from the pool.

    Raises RuntimeError with a clear message if the pool is offline.
    """
    if _pool is None:
        raise RuntimeError(
            f"Database offline: {_pool_error or 'pool not initialized'}"
        )
    async with _pool.connection() as conn:
        yield conn


async def fetch_all(query: str, params: tuple | None = None) -> list[dict]:
    """Execute a query and return all rows as dicts."""
    async with get_conn() as conn:
        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(query, params)
            return await cur.fetchall()


async def fetch_one(query: str, params: tuple | None = None) -> dict | None:
    """Execute a query and return a single row as dict, or None."""
    async with get_conn() as conn:
        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(query, params)
            return await cur.fetchone()
