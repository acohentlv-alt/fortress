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

from fortress.module_e import query_file as qf

log = structlog.get_logger(__name__)

# Type alias for the pluggable enrichment function.
# The enrich_fn now accepts an on_save callback for per-company DB persistence.
EnrichFn = Callable[..., Awaitable[list[Contact]]]


async def run_query(
    triage_result: TriageResult,
    query_name: str,
    query_id: str,
    enrich_fn: EnrichFn,
    pool: Any,  # psycopg_pool.AsyncConnectionPool
    *,
    wave_size: int = 50,
    delay_min: float = 5.0,
    delay_max: float = 15.0,
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

        # --- Per-company save callback ---
        # Each company+contact is persisted to DB the instant enrichment
        # qualifies it.  This eliminates the "data vaporization" bug where
        # a wave timeout discarded an entire batch of already-scraped data.
        saved_contacts: list[Contact] = []
        saved_companies: set[str] = set()  # SIRENs already upserted via on_save

        async def _on_save(company: Company, contact: Contact) -> None:
            """Persist one company+contact immediately after qualification."""
            async with pool.connection() as save_conn:
                await upsert_company(save_conn, company)
                await bulk_tag_query(save_conn, [company.siren], query_name)
                await upsert_contact(save_conn, contact)
                await log_audit(
                    save_conn,
                    query_id=query_id,
                    siren=contact.siren,
                    action=_source_to_action(contact.source.value),
                    result="success",
                    source_url=contact.website,
                    duration_ms=None,
                )
                # Tag replacement companies (SIREN differs from wave list)
                if company.siren not in {co.siren for co in wave_companies}:
                    await bulk_tag_query(save_conn, [company.siren], query_name)
            saved_contacts.append(contact)
            saved_companies.add(company.siren)
            seen_set.mark_seen(company.model_dump())
            log.debug(
                "batch.company_saved",
                siren=company.siren,
                wave=wave_num,
                saved_so_far=len(saved_contacts),
                query=query_name,
            )

        # --- Enrich (with 5-minute safety cap per wave) ---
        # 50 companies × ~4s each (Maps + curl crawl) = ~200s typical.
        # 300s gives 50% headroom for slow websites and rate-limit backoff.
        t0 = datetime.now(tz=timezone.utc)
        try:
            contacts = await asyncio.wait_for(
                enrich_fn(wave_companies, on_save=_on_save),
                timeout=300,  # 5 minutes max per wave
            )
        except asyncio.TimeoutError:
            log.error(
                "batch.wave_timeout",
                wave=wave_num,
                query=query_name,
                companies=len(wave_companies),
                timeout_seconds=300,
                saved_before_timeout=len(saved_contacts),
            )
            contacts = saved_contacts  # Use what was already persisted
        elapsed_ms = int(
            (datetime.now(tz=timezone.utc) - t0).total_seconds() * 1000
        )

        # --- Dedup remaining companies into PostgreSQL ---
        # Companies that weren't saved via on_save (e.g. non-qualified ones)
        # still need to be upserted + tagged so triage works on next run.
        wave_results: list[dict[str, Any]] = []
        contacts_by_siren: dict[str, Contact] = {c.siren: c for c in contacts}
        async with pool.connection() as conn:
            for company in wave_companies:
                if company.siren not in saved_companies:
                    await upsert_company(conn, company)
                    await bulk_tag_query(conn, [company.siren], query_name)
                    seen_set.mark_seen(company.model_dump())

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
