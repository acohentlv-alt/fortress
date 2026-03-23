"""Standalone PostgreSQL connection pool for CLI tools.

Used by sirene_ingester.py and sirene_etablissement_ingester.py which run
as standalone scripts (python -m), NOT inside the API process.

The API process uses fortress.api.db instead (which has health checks,
degraded mode, and 503 error mapping).

Both pools connect to the same database via settings.db_url.
"""

from pathlib import Path

import psycopg_pool
import structlog

from fortress.config.settings import settings

log = structlog.get_logger(__name__)

_pool: psycopg_pool.AsyncConnectionPool | None = None


async def get_pool() -> psycopg_pool.AsyncConnectionPool:
    """Get or create the async connection pool for CLI tools."""
    global _pool
    if _pool is None:
        _pool = psycopg_pool.AsyncConnectionPool(
            conninfo=settings.db_url,
            min_size=2,
            max_size=10,
            open=False,
        )
        await _pool.open()
        log.info("cli_pool_opened", db=settings.db_name, host=settings.db_host)
    return _pool


async def close_pool() -> None:
    """Close the CLI connection pool."""
    global _pool
    if _pool is not None:
        await _pool.close()
        _pool = None
        log.info("cli_pool_closed")


async def init_db() -> None:
    """Initialize database schema from schema.sql."""
    schema_path = Path(__file__).parent / "schema.sql"
    if not schema_path.exists():
        log.warning("schema_not_found", path=str(schema_path))
        return

    schema_sql = schema_path.read_text()
    pool = await get_pool()
    async with pool.connection() as conn:
        await conn.execute(schema_sql)
        log.info("db_schema_applied")
