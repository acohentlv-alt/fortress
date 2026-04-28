"""Pipeline step timing utility.

Provides an async context manager `time_step` that records per-entity,
per-step durations into the `pipeline_timings` table.

Usage:
    # With a pool (preferred — acquires its own connection):
    async with time_step(pool, batch_int_id, siren, "crawl"):
        result = await crawl_website(...)

    # With an existing connection (e.g. inside _match_to_sirene):
    async with time_step(conn, batch_int_id, siren, "inpi_step0"):
        hit = await _inpi_search(...)

    # Skipped step (fired=False) — still writes a row so the skip is recorded:
    async with time_step(pool, batch_int_id, siren, "gemini_judge", fired=False):
        pass  # duration_ms=0, fired=false

ContextVars (for code paths without direct pool/batch_id access, e.g. Maps scraper):
    batch_id_var — integer primary key of batch_data row (batch_data.id).
    pool_var     — async connection pool (psycopg_pool.AsyncConnectionPool).

    Both are set once per batch in the runner's main coroutine:
        from fortress.utils.timing import batch_id_var, pool_var
        batch_id_var.set(batch_int_id)
        pool_var.set(pool)

    The Maps scraper reads them via:
        _bid = batch_id_var.get()
        _pool = pool_var.get()
        if _bid is not None and _pool is not None:
            async with time_step(_pool, _bid, None, "maps_scroll"):
                ...
"""

from __future__ import annotations

import time
from contextlib import asynccontextmanager
from contextvars import ContextVar
from typing import Any, Optional

import structlog

log = structlog.get_logger(__name__)

# Module-level ContextVars so Maps scraper can access batch context
# without threading arguments through callbacks.
# batch_id_var: INTEGER primary key (batch_data.id), NOT the TEXT batch_id string.
batch_id_var: ContextVar[Optional[int]] = ContextVar("batch_id", default=None)
# pool_var: the active psycopg_pool.AsyncConnectionPool for the current batch.
pool_var: ContextVar[Optional[Any]] = ContextVar("pool", default=None)


@asynccontextmanager
async def time_step(
    db: Any,  # psycopg_pool.AsyncConnectionPool OR psycopg.AsyncConnection
    batch_id: int,
    siren: Optional[str],
    step: str,
    fired: bool = True,
):
    """Async context manager that records duration of a pipeline step.

    Args:
        db:       psycopg async connection pool OR a single async connection.
                  Pool: acquires a fresh connection for the write (preferred).
                  Connection: writes inline on the existing connection.
        batch_id: INTEGER primary key of batch_data row (batch_data.id).
        siren:    MAPS or SIREN entity identifier (may be None before entity is created).
        step:     Step name, max 40 characters. E.g. "crawl", "match_cascade".
        fired:    True (default) = step actually ran. False = step was skipped.
                  When fired=False, duration_ms=0 is written to mark the skip.

    On DB write failure: logs a warning — never silently swallows the error.
    """
    if fired:
        t_start = time.perf_counter()
        try:
            yield
        finally:
            duration_ms = int((time.perf_counter() - t_start) * 1000)
            await write_timing(db, batch_id, siren, step, duration_ms, fired=True)
    else:
        # Skipped step: yield immediately, record duration=0
        yield
        await write_timing(db, batch_id, siren, step, 0, fired=False)


async def write_timing(
    db: Any,  # pool or connection
    batch_id: int,
    siren: Optional[str],
    step: str,
    duration_ms: int,
    fired: bool,
) -> None:
    """Write one row to pipeline_timings. Logs warning on failure — never bare except."""
    try:
        sql = """INSERT INTO pipeline_timings (batch_id, siren, step, duration_ms, fired)
                 VALUES (%s, %s, %s, %s, %s)"""
        params = (batch_id, siren, step[:40], duration_ms, fired)
        # Detect pool vs direct connection by checking for .connection() attribute
        if hasattr(db, "connection"):
            # It's a pool — acquire a fresh connection
            async with db.connection() as conn:
                await conn.execute(sql, params)
        else:
            # It's a direct psycopg connection — use inline
            await db.execute(sql, params)
    except Exception as e:
        log.warning("timing.write_failed", step=step, error=str(e))
