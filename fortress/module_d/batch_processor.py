"""Batch processor — wave-based pipeline runner with checkpoint / resume.

Orchestrates the scraping loop for one query:
  1. GREEN companies → immediate card output (zero scraping).
  2. YELLOW + RED companies → split into waves of `settings.wave_size`.
  3. For each wave: enrich → checkpoint → dedup into DB → optional cooldown.
  4. On restart: detect last completed wave → resume from wave+1.

The `enrich_fn` parameter is the enrichment callable.
It receives a list of Company objects and returns a list of Contact objects.

Phase 2 (current): enrich_fn is a stub → `lambda companies: []`.
Phase 3: enrich_fn wires up INPI + web_search + website_crawler.
Phase 4: enrich_fn adds Google Maps fallback.

This allows the pipeline infrastructure to be tested independently of
the enrichment logic.
"""

from __future__ import annotations

import asyncio
import math
import random
from collections.abc import Awaitable, Callable
from datetime import datetime, timezone
from typing import Any

import structlog

from fortress.models import Company, Contact, TriageResult
from fortress.module_d import checkpoint as ckpt
from fortress.module_d.deduplicator import (
    bulk_tag_query,
    log_audit,
    upsert_company,
    upsert_contact,
)
from fortress.module_d.seen_set import SeenSet
from fortress.module_e import card_formatter as cf
from fortress.module_e import master_file as mf
from fortress.module_e import query_file as qf

log = structlog.get_logger(__name__)

# Type alias for the pluggable enrichment function
EnrichFn = Callable[[list[Company]], Awaitable[list[Contact]]]


async def run_query(
    triage_result: TriageResult,
    query_name: str,
    query_id: str,
    enrich_fn: EnrichFn,
    pool: Any,  # psycopg_pool.AsyncConnectionPool
    *,
    wave_size: int = 50,
    delay_min: float = 30.0,
    delay_max: float = 90.0,
    resume: bool = True,
) -> None:
    """Execute the full pipeline for one query.

    Args:
        triage_result: Output of triage_companies() — four company buckets.
        query_name:    Human-readable label (e.g. "AGRICULTURE 66").
        query_id:      Filesystem-safe ID (e.g. "AGRICULTURE_66").
        enrich_fn:     Async function: list[Company] → list[Contact].
        pool:          Async psycopg3 connection pool.
        wave_size:     Companies per wave (default 50).
        delay_min:     Min cooldown in seconds between waves.
        delay_max:     Max cooldown in seconds between waves.
        resume:        If True, skip already-completed waves on restart.
    """
    # ------------------------------------------------------------------
    # Phase A: instant cards for GREEN (zero network calls)
    # ------------------------------------------------------------------
    if triage_result.green:
        async with pool.connection() as conn:
            green_sirens = [c.siren for c in triage_result.green]
            await bulk_tag_query(conn, green_sirens, query_name)
        log.info(
            "batch.green_tagged",
            count=len(triage_result.green),
            query=query_name,
        )

    # ------------------------------------------------------------------
    # Phase B: build scrape queue (YELLOW first — cheaper, faster to complete)
    # ------------------------------------------------------------------
    scrape_queue: list[Company] = triage_result.yellow + triage_result.red
    if not scrape_queue:
        log.info("batch.nothing_to_scrape", query=query_name)
        return

    total_waves = math.ceil(len(scrape_queue) / wave_size)
    log.info(
        "batch.start",
        query=query_name,
        total_companies=len(scrape_queue),
        waves=total_waves,
    )

    # ------------------------------------------------------------------
    # Phase C: resume detection
    # ------------------------------------------------------------------
    start_wave = 1
    seen_set = SeenSet()

    if resume and ckpt.checkpoint_exists(query_id):
        job_state, seen_set = ckpt.load(query_id)
        if job_state:
            completed = job_state.get("wave_current", 0)
            if completed >= total_waves:
                log.info("batch.already_complete", query=query_name)
                return
            start_wave = completed + 1
            log.info(
                "batch.resume",
                query=query_name,
                resume_wave=start_wave,
                total_waves=total_waves,
            )

    # ------------------------------------------------------------------
    # Phase D: wave loop
    # ------------------------------------------------------------------
    for wave_num in range(start_wave, total_waves + 1):
        wave_start = (wave_num - 1) * wave_size
        wave_end = min(wave_start + wave_size, len(scrape_queue))
        wave_companies = scrape_queue[wave_start:wave_end]

        log.info(
            "batch.wave_start",
            wave=wave_num,
            total=total_waves,
            companies=len(wave_companies),
            query=query_name,
        )

        # --- Enrich (with 5-minute safety cap per wave) ---
        t0 = datetime.now(tz=timezone.utc)
        try:
            contacts = await asyncio.wait_for(
                enrich_fn(wave_companies),
                timeout=300,  # 5 minutes max per wave
            )
        except asyncio.TimeoutError:
            log.error(
                "batch.wave_timeout",
                wave=wave_num,
                query=query_name,
                companies=len(wave_companies),
                timeout_seconds=300,
            )
            contacts = []
        elapsed_ms = int(
            (datetime.now(tz=timezone.utc) - t0).total_seconds() * 1000
        )

        # --- Dedup into PostgreSQL ---
        wave_results: list[dict[str, Any]] = []
        contacts_by_siren: dict[str, Contact] = {c.siren: c for c in contacts}
        async with pool.connection() as conn:
            for company in wave_companies:
                await upsert_company(conn, company)
                await bulk_tag_query(conn, [company.siren], query_name)
                seen_set.mark_seen(company.model_dump())

            for contact in contacts:
                await upsert_contact(conn, contact)

                # Audit log — one entry per enriched contact
                await log_audit(
                    conn,
                    query_id=query_id,
                    siren=contact.siren,
                    action=_source_to_action(contact.source.value),
                    result="success",
                    source_url=contact.website,
                    duration_ms=None,
                )

            # Tag replacement companies that the enricher swapped in.
            # Their SIRENs differ from the wave companies, so without this
            # they'd have contacts but no query_tags entry → gauge mismatch.
            replacement_sirens = [
                c.siren for c in contacts
                if c.siren not in {co.siren for co in wave_companies}
            ]
            if replacement_sirens:
                await bulk_tag_query(conn, replacement_sirens, query_name)
                log.debug(
                    "batch.replacements_tagged",
                    count=len(replacement_sirens),
                    query=query_name,
                )

            # Build wave result records for checkpoint
            wave_results = [c.model_dump() for c in wave_companies]

        # --- Progressive output: write JSONL cards after DB commit ---
        # Officers are not loaded here (no DB round-trip per wave);
        # the JSONL records the company + contact data available at wave time.
        wave_cards: list[dict] = []
        for i, company in enumerate(wave_companies):
            contact = contacts_by_siren.get(company.siren)
            card = cf.format_card(
                company,
                contact,
                officers=[],                            # filled in by card_formatter default
                query_name=query_name,
                card_index=wave_start + i + 1,          # 1-based global index
            )
            wave_cards.append(card)
        try:
            qf.append_wave(query_id, wave_cards)
            mf.append_records(wave_cards)
            log.debug(
                "batch.jsonl_written",
                wave=wave_num,
                cards=len(wave_cards),
                query=query_name,
            )
        except Exception as exc:
            # JSONL write failure is non-fatal — pipeline continues
            log.warning("batch.jsonl_write_error", wave=wave_num, error=str(exc))

        # --- Checkpoint ---
        job_state = {
            "query": query_name,
            "query_id": query_id,
            "wave_current": wave_num,
            "wave_total": total_waves,
            "companies_total": len(scrape_queue),
            "green_instant": len(triage_result.green),
            "yellow_count": len(triage_result.yellow),
            "red_count": len(triage_result.red),
            "elapsed_ms_last_wave": elapsed_ms,
            "updated_at": datetime.now(tz=timezone.utc).isoformat(),
        }
        ckpt.save(
            query_id,
            wave_num,
            wave_results,
            seen_set,
            job_state=job_state,
        )

        log.info(
            "batch.wave_done",
            wave=wave_num,
            total=total_waves,
            contacts_found=len(contacts),
            elapsed_ms=elapsed_ms,
            query=query_name,
        )

        # --- Cooldown between waves (skip after last wave) ---
        if wave_num < total_waves:
            delay = random.uniform(delay_min, delay_max)
            log.debug("batch.cooldown", seconds=round(delay, 1), query=query_name)
            await asyncio.sleep(delay)

    log.info("batch.complete", query=query_name, total_waves=total_waves)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _source_to_action(source: str) -> str:
    """Map ContactSource value to scrape_audit action label."""
    mapping = {
        "inpi": "inpi_lookup",
        "google_search": "web_search",
        "website_crawl": "website_crawl",
        "google_maps": "maps_lookup",
        "sirene": "sirene",
        "synthesized": "website_crawl",
    }
    return mapping.get(source, source)
