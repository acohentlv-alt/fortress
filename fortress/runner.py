"""Background runner — launched as subprocess by the UI.

# Ensure Chrome/Playwright can create temp dirs on macOS.
# Must be set BEFORE any imports that touch tempfile.
import os as _os
_os.environ.setdefault("TMPDIR", "/tmp")

Usage:
    python -m fortress.runner <query_id>

Reads the scrape_jobs row for query_id, runs the full pipeline
(interpret_query → triage → enrich → batch_processor.run_query),
and writes progress back to scrape_jobs after every wave.

scrape_jobs status lifecycle:
    queued → in_progress → completed
                       └→ failed  (on exception)

Progress columns updated throughout:
    wave_current      — incremented after each wave completes
    companies_scraped — cumulative companies processed by enrich_fn
    wave_total        — set once after triage (total waves to run)
    triage_*          — set once after triage completes
"""

from __future__ import annotations

import asyncio
import json
import math
import signal
import sys

import psycopg
import psycopg_pool
import structlog

from fortress.config.settings import settings
from fortress.module_a.query_interpreter import interpret_query
from fortress.module_a.triage import triage_companies
from fortress.module_c.curl_client import CurlClient
from fortress.module_d.batch_processor import run_query as bp_run_query
from fortress.module_d.enricher import enrich_companies

log = structlog.get_logger()

# Graceful shutdown flag — set by SIGTERM handler
_shutdown = False

def _handle_sigterm(signum, frame):
    """Handle SIGTERM from Render deploy — set shutdown flag."""
    global _shutdown
    _shutdown = True
    log.warning("runner.sigterm_received", msg="Graceful shutdown requested")

signal.signal(signal.SIGTERM, _handle_sigterm)

# Hard safety cap — never process more than 200 records per wave
# to prevent data holes from excessive in-memory processing.
_MAX_WAVE_SIZE = 200


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


async def _update_job(
    conn: psycopg.AsyncConnection,
    query_id: str,
    /,
    **fields: object,
) -> None:
    """UPDATE one or more scrape_jobs columns + updated_at for a given query_id.

    Column names come exclusively from internal call sites (never user input),
    so direct interpolation into the SET clause is safe.
    """
    if not fields:
        return
    # Build: "col1 = %s, col2 = %s, ..."
    set_clause = ", ".join(f"{col} = %s" for col in fields)
    params = [*fields.values(), query_id]
    await conn.execute(
        f"UPDATE scrape_jobs SET {set_clause}, updated_at = NOW() "  # noqa: S608
        f"WHERE query_id = %s",
        params,
    )
    await conn.commit()


async def _update_job_safe(
    conn_holder: list[psycopg.AsyncConnection],
    query_id: str,
    /,
    **fields: object,
) -> None:
    """Resilient wrapper around _update_job with reconnect-on-failure.

    If the existing connection is dead (network timeout, idle disconnect),
    opens a fresh connection, performs the update, and replaces the reference.
    This prevents cascading failures when the status connection drops mid-batch.

    Args:
        conn_holder: Single-element list holding the active connection reference.
                     Using a list allows in-place replacement on reconnect.
    """
    conn = conn_holder[0]
    try:
        await _update_job(conn, query_id, **fields)
    except (psycopg.OperationalError, psycopg.InterfaceError, OSError) as exc:
        log.warning(
            "runner.status_conn_lost",
            query_id=query_id,
            error=str(exc),
            action="reconnecting",
        )
        # Close the dead connection (ignore errors)
        try:
            await conn.close()
        except Exception:
            pass
        # Open a fresh connection with keepalives
        new_conn = await psycopg.AsyncConnection.connect(
            settings.db_url,
            autocommit=False,
            **_KEEPALIVE_PARAMS,
        )
        conn_holder[0] = new_conn
        # Retry the update on the new connection
        await _update_job(new_conn, query_id, **fields)
        log.info("runner.status_conn_reconnected", query_id=query_id)


# PostgreSQL TCP keepalive parameters — prevents silent connection death
# during long batches (2+ hours). OS sends TCP probes every 60s, declares
# connection dead after 5 failed probes (= 5 minutes).
_KEEPALIVE_PARAMS: dict[str, int] = {
    "keepalives": 1,
    "keepalives_idle": 60,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}


async def _run_heartbeat(
    conn_holder: list[psycopg.AsyncConnection],
    query_id: str,
    interval: float = 60.0,
) -> None:
    """Background task: touch scrape_jobs.updated_at every `interval` seconds.

    This serves two purposes:
      1. Keeps the status connection alive (prevents idle timeout disconnects).
      2. Provides a "last seen" timestamp for external monitoring — any job
         that hasn't heartbeated in 5 minutes is likely stuck.
    """
    while True:
        await asyncio.sleep(interval)
        try:
            await _update_job_safe(conn_holder, query_id)
            log.debug("runner.heartbeat", query_id=query_id)
        except Exception as exc:
            log.warning(
                "runner.heartbeat_failed",
                query_id=query_id,
                error=str(exc),
            )
            # Don't crash the heartbeat — keep trying



# ---------------------------------------------------------------------------
# Main pipeline coroutine
# ---------------------------------------------------------------------------


async def run(query_id: str) -> None:
    """Run the scraping pipeline for a given query_id.

    Uses two separate psycopg3 connections:
      - status_conn  — long-lived; used only for scrape_jobs status updates.
      - pool         — short-lived async pool; shared by interpret/triage/batch.

    This keeps the status writes isolated from the pipeline's connection usage.

    Chrome startup order (critical for reliability):
      Chrome is started BEFORE interpret_query/triage so it gets a clean event
      loop with minimal resource contention.  The heavy DB queries run while
      Chrome is already warm and connected.  Starting Chrome after the DB queries
      causes intermittent connection failures due to resource pressure.
    """
    import traceback as _traceback

    # Dynamic wave size: use settings.wave_size (default 50), capped at 200
    _wave_size = min(settings.wave_size, _MAX_WAVE_SIZE)

    log.info("runner_start", query_id=query_id, wave_size=_wave_size)

    # ── Clear stale checkpoint from previous runs ────────────────────────
    # Every runner invocation is a NEW job launch. Old checkpoint dirs
    # with the same query_id would cause batch.already_complete skip.
    from fortress.module_d import checkpoint as _ckpt
    if _ckpt.checkpoint_exists(query_id):
        _ckpt.clear(query_id)
        log.info("runner.stale_checkpoint_cleared", query_id=query_id)

    # ── Phase 4: start Google Maps scraper (Playwright Chromium) ─────────────
    # Browser must be started before the DB connection pool and heavy SIRENE
    # queries.  After interpret_query (20+ seconds of SQL), browser sometimes
    # fails to connect due to resource contention.
    maps_scraper = None
    try:
        from fortress.module_c.playwright_maps_scraper import PlaywrightMapsScraper
        maps_scraper = PlaywrightMapsScraper()
        await maps_scraper.start()
        log.info("runner.maps_scraper_started", engine="playwright_chromium")
    except Exception as exc:
        log.warning(
            "runner.maps_scraper_unavailable",
            error=str(exc),
            reason="Google Maps disabled for this batch",
        )
        maps_scraper = None


    try:
        status_conn = await psycopg.AsyncConnection.connect(
            settings.db_url,
            autocommit=False,
            **_KEEPALIVE_PARAMS,
        )
        # conn_holder allows _update_job_safe to swap the connection on reconnect
        conn_holder: list[psycopg.AsyncConnection] = [status_conn]

        # ── Start heartbeat background task ──────────────────────────────
        heartbeat_task = asyncio.create_task(
            _run_heartbeat(conn_holder, query_id),
            name=f"heartbeat-{query_id}",
        )

        try:
            # ── Mark job as in_progress ─────────────────────────────────────
            await _update_job_safe(conn_holder, query_id, status="in_progress")

            try:

                # ── Load job metadata from scrape_jobs ──────────────────────
                cur = await conn_holder[0].execute(
                    """SELECT query_name,
                              COALESCE(batch_number, 1),
                              COALESCE(batch_offset, 0),
                              filters_json,
                              total_companies,
                              COALESCE(batch_size, total_companies)
                       FROM scrape_jobs WHERE query_id = %s LIMIT 1""",
                    (query_id,),
                )
                row = await cur.fetchone()
                if not row:
                    raise RuntimeError(
                        f"No scrape_jobs row found for query_id={query_id!r}"
                    )
                query_name: str = row[0]
                batch_number: int = row[1]
                batch_offset: int = row[2]
                filters_raw: str | None = row[3]
                requested_size: int = row[5] or row[4] or _wave_size  # batch_size preferred
                # Parse filters JSON (None or empty string → no filters)
                query_filters: dict | None = None
                if filters_raw:
                    try:
                        query_filters = json.loads(filters_raw) or None
                    except (json.JSONDecodeError, ValueError):
                        log.warning("runner_filters_parse_error", filters_raw=filters_raw)
                log.info(
                    "runner_loaded_job",
                    query_id=query_id,
                    query_name=query_name,
                    batch_number=batch_number,
                    batch_offset=batch_offset,
                    requested_size=requested_size,
                )

                # ── Open async pool (shared by interpret / triage / batch) ──
                # Set statement_timeout to prevent 16.7M-row scans from
                # hanging indefinitely when the compound index isn't hit.
                async def _configure_conn(conn: psycopg.AsyncConnection) -> None:
                    await conn.execute("SET statement_timeout = '60s'")
                    await conn.commit()

                async with psycopg_pool.AsyncConnectionPool(
                    settings.db_url, min_size=1, max_size=5, open=True,
                    configure=_configure_conn,
                ) as pool:

                    # ── Step 1: interpret query → QueryResult ────────────────
                    # Each job is a single batch of 50 companies.
                    # batch_offset determines which slice of SIRENE results to fetch.
                    log.info(
                        "runner_interpret",
                        query_name=query_name,
                        batch_offset=batch_offset,
                    )
                    try:
                        # Fetch 2× requested size so the qualify-or-replace
                        # loop has enough candidates without extra DB queries.
                        qr = await interpret_query(
                            query_name, pool,
                            filters=query_filters,
                            limit=requested_size * 2,
                            offset=batch_offset,
                        )
                    except Exception as exc:
                        if "statement timeout" in str(exc).lower():
                            log.error(
                                "runner_query_timeout",
                                query_name=query_name,
                                error="SQL query exceeded 60s timeout on 16.7M-row table",
                            )
                            await _update_job_safe(
                                conn_holder, query_id,
                                status="failed",
                                companies_failed=0,
                            )
                            return
                        raise

                    if qr.company_count == 0:
                        log.warning(
                            "runner_no_companies",
                            query_name=query_name,
                        )
                        await _update_job_safe(conn_holder, query_id, status="completed")
                        return

                    # ── Step 2: triage → TriageResult ───────────────────────
                    log.info(
                        "runner_triage",
                        query_name=query_name,
                        companies=qr.company_count,
                    )
                    triage = await triage_companies(qr.sample, qr.raw_query, pool)

                    # Cap the scrape queue at the user's requested size
                    scrape_list = triage.yellow + triage.red
                    if len(scrape_list) > requested_size:
                        scrape_list = scrape_list[:requested_size]
                        log.info(
                            "runner.scrape_queue_capped",
                            original=len(triage.yellow) + len(triage.red),
                            capped_to=requested_size,
                        )
                        # Update triage lists so batch_processor only sees
                        # the capped companies (it builds its own queue from
                        # triage.yellow + triage.red internally).
                        yellow_count = min(len(triage.yellow), requested_size)
                        triage.yellow = scrape_list[:yellow_count]
                        triage.red = scrape_list[yellow_count:]

                    scrape_count = len(scrape_list)
                    total_waves = (
                        math.ceil(scrape_count / _wave_size) if scrape_count else 0
                    )

                    # Update triage stats + wave_total in scrape_jobs
                    await _update_job_safe(
                        conn_holder,
                        query_id,
                        triage_black=triage.black_count,
                        triage_blue=triage.blue_count,
                        triage_green=triage.green_count,
                        triage_yellow=triage.yellow_count,
                        triage_red=triage.red_count,
                        total_companies=qr.company_count,
                        wave_total=total_waves,
                        wave_current=0,
                    )

                    if scrape_count == 0:
                        # All GREEN — nothing to scrape
                        log.info("runner_all_green", query_name=query_name)
                        await _update_job_safe(conn_holder, query_id, status="completed")
                        return

                    # ── Step 3: enrich + batch process ──────────────────────
                    # Track waves via a counter incremented inside the enrich_fn
                    # closure, which is called exactly once per wave by batch_processor.
                    wave_counter = 0
                    companies_scraped = 0
                    total_replaced = 0

                    async with CurlClient() as curl_client:

                        # Extract domain/sector keywords from query_name
                        # e.g. "camping 66" → "camping", "LOGISTIQUE 75" → "LOGISTIQUE"
                        _domain_words = [
                            w for w in query_name.split()
                            if not w.isdigit() and len(w) > 1
                        ]
                        _query_domain = " ".join(_domain_words)

                        async def enrich_fn(companies, on_save=None):  # noqa: ANN001, ANN202
                            nonlocal wave_counter, companies_scraped, total_replaced

                            async def _on_progress(tried: int, replaced: int, qualified: int = 0) -> None:
                                """Fires after each company decision — updates DB mid-wave."""
                                nonlocal companies_scraped, total_replaced
                                # `tried` is cumulative within this wave's enricher call
                                companies_scraped = tried
                                total_replaced = replaced
                                await _update_job_safe(
                                    conn_holder, query_id,
                                    companies_scraped=companies_scraped,
                                    replaced_count=total_replaced,
                                    companies_qualified=qualified,
                                )

                            contacts, replaced = await enrich_companies(
                                companies,
                                pool=pool,
                                curl_client=curl_client,
                                maps_scraper=maps_scraper,
                                on_progress=_on_progress,
                                on_save=on_save,
                                query_id=query_id,
                                query_domain=_query_domain,
                            )
                            wave_counter += 1
                            total_replaced = replaced
                            # Final wave-level update
                            await _update_job_safe(
                                conn_holder,
                                query_id,
                                wave_current=wave_counter,
                                companies_scraped=companies_scraped,
                                replaced_count=total_replaced,
                            )
                            return contacts

                        await bp_run_query(
                            triage,
                            query_name,
                            query_id,
                            enrich_fn,
                            pool,
                            wave_size=_wave_size,
                        )

            # ── All waves done — mark completed or interrupted ────────────
                final_status = "interrupted" if _shutdown else "completed"
                await _update_job_safe(
                    conn_holder,
                    query_id,
                    status=final_status,
                    wave_current=wave_counter,
                    replaced_count=total_replaced,
                )
                log.info(f"runner_{final_status}", query_id=query_id, waves=wave_counter, shutdown=_shutdown)

            except Exception as exc:
                log.error(
                    "runner_failed",
                    query_id=query_id,
                    error=str(exc),
                    traceback=_traceback.format_exc(),
                )
                try:
                    await _update_job_safe(conn_holder, query_id, status="failed")
                except Exception:
                    pass  # Don't mask the original exception
                raise

        finally:
            # Stop the heartbeat background task
            heartbeat_task.cancel()
            try:
                await heartbeat_task
            except asyncio.CancelledError:
                pass
            # Close the status connection
            try:
                await conn_holder[0].close()
            except Exception:
                pass

    finally:
        # Always stop Chrome, regardless of how the pipeline exits.
        if maps_scraper is not None:
            await maps_scraper.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Entry point — called by `python -m fortress.runner <query_id>`."""
    if len(sys.argv) < 2:
        print("Usage: python -m fortress.runner <query_id>", file=sys.stderr)
        sys.exit(1)
    query_id = sys.argv[1]
    asyncio.run(run(query_id))


if __name__ == "__main__":
    main()
