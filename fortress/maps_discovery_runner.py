"""Maps-first discovery runner — discover businesses from Google Maps,
then match to SIRENE database for legal enrichment.

Usage:
    python -m fortress.maps_discovery_runner <query_id>

Reads the scrape_jobs row for query_id, runs the Maps-first pipeline:
  1. search_all(query) for each search term → discover businesses
  2. SIRENE matching → find existing company by name/enseigne
  3. Website crawl → extract emails for companies with websites
  4. Persist companies + contacts + query_tags + scrape_audit

scrape_jobs status lifecycle:
    queued → in_progress → completed
                       └→ failed
"""

from __future__ import annotations

import asyncio
import json
import re
import sys
import time
import unicodedata
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

import psycopg
import psycopg_pool
import structlog

from fortress.config.settings import settings
from fortress.models import Company, Contact, ContactSource
from fortress.module_c.curl_client import CurlClient
from fortress.module_d.deduplicator import (
    bulk_tag_query,
    log_audit,
    upsert_company,
    upsert_contact,
)

log = structlog.get_logger()

# PostgreSQL TCP keepalive parameters
_KEEPALIVE_PARAMS: dict[str, int] = {
    "keepalives": 1,
    "keepalives_idle": 60,
    "keepalives_interval": 10,
    "keepalives_count": 5,
}


# ---------------------------------------------------------------------------
# SIRENE matching — find existing company by Maps-discovered name
# ---------------------------------------------------------------------------

def _normalize_name(name: str) -> str:
    """Normalize a business name for comparison.

    Removes accents, lowercases, strips legal forms and punctuation.
    """
    # Decompose accents
    nfkd = unicodedata.normalize("NFKD", name.lower())
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Remove punctuation except spaces
    cleaned = re.sub(r"[^a-z0-9\s]", "", ascii_name)
    # Remove common legal forms
    _LEGAL = {
        "sarl", "sas", "sasu", "eurl", "sa", "sci", "snc",
        "scs", "sca", "ei", "eirl", "asso", "association",
        "et", "cie", "fils", "freres", "groupe", "holding",
    }
    tokens = [t for t in cleaned.split() if t not in _LEGAL and len(t) > 0]
    return " ".join(tokens)


def _name_match_score(name_a: str, name_b: str) -> float:
    """Compute similarity between two normalized names (0.0 to 1.0)."""
    if not name_a or not name_b:
        return 0.0
    ta = _normalize_name(name_a).split()
    tb = _normalize_name(name_b).split()
    if not ta or not tb:
        return 0.0
    # Full containment
    ja = " ".join(ta)
    jb = " ".join(tb)
    if ja in jb or jb in ja:
        return 1.0
    # Token overlap
    overlap = sum(1 for t in ta if t in tb)
    return overlap / max(len(ta), len(tb))


async def _match_to_sirene(
    conn: Any,
    maps_name: str,
    maps_address: str | None,
    departement: str | None,
) -> Company | None:
    """Try to find a matching company in the SIRENE database.

    Search strategy:
      1. Search by enseigne (exact fuzzy match)
      2. Search by denomination (exact fuzzy match)
      3. Search by denomination + ville overlap

    Returns the best matching Company, or None if no good match.
    """
    if not maps_name or len(maps_name) < 2:
        return None

    normalized = _normalize_name(maps_name)
    search_terms = normalized.split()
    if not search_terms:
        return None

    # Build fuzzy search: look for companies where enseigne or denomination
    # contains words from the Maps name. Limited to department if known.
    conditions = []
    params: list[Any] = []

    # Primary term for search (longest meaningful word)
    primary_term = max(search_terms, key=len) if search_terms else ""
    if len(primary_term) < 3:
        return None

    # Use ILIKE with the primary term on both enseigne and denomination
    conditions.append(
        "(LOWER(enseigne) LIKE %s OR LOWER(denomination) LIKE %s)"
    )
    like_pattern = f"%{primary_term}%"
    params.extend([like_pattern, like_pattern])

    if departement and re.match(r"^\d{2,3}$", departement):
        conditions.append("departement = %s")
        params.append(departement)

    conditions.append("statut = 'A'")  # Only active companies

    where_clause = " AND ".join(conditions)
    query = f"""
        SELECT siren, siret_siege, denomination, enseigne, naf_code, naf_libelle,
               forme_juridique, adresse, code_postal, ville, departement,
               region, statut, date_creation, tranche_effectif,
               latitude, longitude, fortress_id
        FROM companies
        WHERE {where_clause}
        LIMIT 50
    """

    cur = await conn.execute(query, params)
    rows = await cur.fetchall()

    if not rows:
        return None

    # Score each candidate
    best_company: Company | None = None
    best_score = 0.0

    for row in rows:
        denom = row[2] or ""
        enseigne = row[3] or ""

        # Score against both enseigne and denomination
        score_enseigne = _name_match_score(maps_name, enseigne)
        score_denom = _name_match_score(maps_name, denom)
        score = max(score_enseigne, score_denom)

        # Boost if address matches
        if maps_address and row[7]:  # row[7] = adresse
            addr_norm = _normalize_name(maps_address)
            db_addr_norm = _normalize_name(row[7])
            # Check city overlap from address
            if any(t in addr_norm for t in db_addr_norm.split() if len(t) > 3):
                score += 0.15

        if score > best_score and score >= 0.6:
            best_score = score
            best_company = Company(
                siren=row[0],
                siret_siege=row[1],
                denomination=row[2],
                enseigne=row[3],
                naf_code=row[4],
                naf_libelle=row[5],
                forme_juridique=row[6],
                adresse=row[7],
                code_postal=row[8],
                ville=row[9],
                departement=row[10],
                region=row[11],
                statut=row[12],
                date_creation=row[13],
                tranche_effectif=row[14],
                latitude=row[15],
                longitude=row[16],
                fortress_id=row[17],
            )

    if best_company:
        log.info(
            "maps_discovery.sirene_match",
            maps_name=maps_name,
            siren=best_company.siren,
            denomination=best_company.denomination,
            enseigne=best_company.enseigne,
            score=round(best_score, 2),
        )
    else:
        log.info(
            "maps_discovery.sirene_no_match",
            maps_name=maps_name,
            candidates=len(rows),
        )

    return best_company


# ---------------------------------------------------------------------------
# Status update helpers (same pattern as runner.py)
# ---------------------------------------------------------------------------

async def _update_job(
    conn: psycopg.AsyncConnection, query_id: str, /, **fields: object
) -> None:
    if not fields:
        return
    set_clause = ", ".join(f"{col} = %s" for col in fields)
    params = [*fields.values(), query_id]
    await conn.execute(
        f"UPDATE scrape_jobs SET {set_clause}, updated_at = NOW() "  # noqa: S608
        f"WHERE query_id = %s",
        params,
    )
    await conn.commit()


async def _update_job_safe(
    conn_holder: list[psycopg.AsyncConnection], query_id: str, /, **fields: object
) -> None:
    conn = conn_holder[0]
    try:
        await _update_job(conn, query_id, **fields)
    except (psycopg.OperationalError, psycopg.InterfaceError, OSError) as exc:
        log.warning("maps_runner.status_conn_lost", error=str(exc))
        try:
            await conn.close()
        except Exception:
            pass
        new_conn = await psycopg.AsyncConnection.connect(
            settings.db_url, autocommit=False, **_KEEPALIVE_PARAMS,
        )
        conn_holder[0] = new_conn
        await _update_job(new_conn, query_id, **fields)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def run(query_id: str) -> None:
    """Run the Maps-first discovery pipeline for a given query_id."""
    import traceback as _traceback

    log.info("maps_discovery_runner.start", query_id=query_id)

    # ── Start Google Maps scraper ─────────────────────────────────────
    maps_scraper = None
    try:
        from fortress.module_c.playwright_maps_scraper import PlaywrightMapsScraper
        maps_scraper = PlaywrightMapsScraper()
        await maps_scraper.start()
        log.info("maps_discovery_runner.browser_started")
    except Exception as exc:
        log.error("maps_discovery_runner.browser_failed", error=str(exc))
        # Can't do Maps-first without Chrome — mark failed
        try:
            status_conn = await psycopg.AsyncConnection.connect(
                settings.db_url, autocommit=False, **_KEEPALIVE_PARAMS,
            )
            await _update_job(status_conn, query_id, status="failed")
            await status_conn.close()
        except Exception:
            pass
        return

    try:
        # ── Open status connection ────────────────────────────────────
        status_conn = await psycopg.AsyncConnection.connect(
            settings.db_url, autocommit=False, **_KEEPALIVE_PARAMS,
        )
        conn_holder: list[psycopg.AsyncConnection] = [status_conn]

        try:
            await _update_job_safe(conn_holder, query_id, status="in_progress")

            # ── Load job metadata ─────────────────────────────────────
            cur = await conn_holder[0].execute(
                """SELECT query_name, search_queries, filters_json
                   FROM scrape_jobs WHERE query_id = %s LIMIT 1""",
                (query_id,),
            )
            row = await cur.fetchone()
            if not row:
                raise RuntimeError(f"No scrape_jobs row for query_id={query_id!r}")

            query_name: str = row[0]
            raw_queries = row[1]
            filters_raw = row[2]

            # Parse search_queries from JSON
            search_queries: list[str] = []
            if raw_queries:
                if isinstance(raw_queries, str):
                    search_queries = json.loads(raw_queries)
                else:
                    search_queries = list(raw_queries)

            if not search_queries:
                # Fallback: use query_name as the search query
                search_queries = [query_name]

            log.info(
                "maps_discovery_runner.loaded_job",
                query_id=query_id,
                query_name=query_name,
                search_queries=search_queries,
            )

            # Parse optional department from filters
            dept_filter = None
            if filters_raw:
                try:
                    filters = json.loads(filters_raw) if isinstance(filters_raw, str) else filters_raw
                    dept_filter = filters.get("department")
                except Exception:
                    pass

            # ── Open async pool ───────────────────────────────────────
            async with psycopg_pool.AsyncConnectionPool(
                settings.db_url, min_size=1, max_size=5, open=True,
            ) as pool:

                total_queries = len(search_queries)
                companies_discovered = 0
                qualified = 0
                seen_names: set[str] = set()  # Cross-query dedup

                # ── Inline persist callback ────────────────────────────
                # This runs for EACH business extracted by search_all,
                # ensuring data is saved to DB immediately (not after
                # all queries finish). If Chrome crashes mid-batch,
                # already-saved businesses are retained.

                async def _persist_result(maps_result: dict[str, Any]) -> None:
                    nonlocal companies_discovered, qualified

                    maps_name = maps_result.get("maps_name", "")
                    maps_address = maps_result.get("address")
                    maps_phone = maps_result.get("phone")
                    maps_website = maps_result.get("website")

                    # Cross-query dedup
                    name_key = maps_name.lower().strip()
                    addr_key = (maps_address or "").lower().strip()
                    dedup_key = f"{name_key}|{addr_key}"
                    if dedup_key in seen_names or not name_key:
                        return
                    seen_names.add(dedup_key)

                    # SIRENE matching
                    async with pool.connection() as conn:
                        company = await _match_to_sirene(
                            conn, maps_name, maps_address, dept_filter,
                        )

                    companies_discovered += 1
                    idx = companies_discovered

                    if company:
                        siren = company.siren
                    else:
                        siren = f"MAPS{idx:06d}"
                        company = Company(
                            siren=siren,
                            denomination=maps_name,
                            enseigne=maps_name,
                            adresse=maps_address,
                            departement=dept_filter,
                            statut="A",
                        )
                        log.info(
                            "maps_discovery_runner.new_entity",
                            maps_name=maps_name,
                            temp_siren=siren,
                        )

                    # Build contact
                    raw_rating = maps_result.get("rating")
                    contact = Contact(
                        siren=siren,
                        source=ContactSource.GOOGLE_MAPS,
                        phone=maps_phone,
                        website=maps_website,
                        email=None,
                        address=maps_address,
                        maps_url=maps_result.get("maps_url"),
                        rating=Decimal(str(raw_rating)) if raw_rating else None,
                        review_count=maps_result.get("review_count"),
                    )

                    has_data = bool(maps_phone or maps_website)
                    if has_data:
                        qualified += 1

                    # Persist to DB immediately
                    async with pool.connection() as conn:
                        await upsert_company(conn, company)
                        await bulk_tag_query(conn, [siren], query_name)
                        await upsert_contact(conn, contact)
                        await log_audit(
                            conn,
                            query_id=query_id,
                            siren=siren,
                            action="maps_lookup",
                            result="success" if has_data else "no_data",
                            source_url=maps_result.get("maps_url"),
                            duration_ms=None,
                        )

                    # Update progress (keeps heartbeat alive)
                    await _update_job_safe(
                        conn_holder, query_id,
                        companies_scraped=companies_discovered,
                        companies_qualified=qualified,
                        total_companies=companies_discovered,
                        batch_size=companies_discovered,
                    )

                    if idx % 10 == 0:
                        log.info(
                            "maps_discovery_runner.progress",
                            processed=idx,
                            qualified=qualified,
                        )

                # ── Maps Discovery (with inline persistence) ──────────
                for q_idx, search_query in enumerate(search_queries, 1):
                    log.info(
                        "maps_discovery_runner.search_start",
                        query=search_query,
                        progress=f"{q_idx}/{total_queries}",
                    )

                    await _update_job_safe(
                        conn_holder, query_id,
                        wave_current=q_idx,
                        wave_total=total_queries,
                    )

                    # search_all calls _persist_result for each business
                    results = await maps_scraper.search_all(
                        search_query, on_result=_persist_result,
                    )

                    log.info(
                        "maps_discovery_runner.search_done",
                        query=search_query,
                        results=len(results),
                        total_saved=companies_discovered,
                        total_qualified=qualified,
                    )

                    # Small delay between searches to avoid detection
                    if q_idx < total_queries:
                        await asyncio.sleep(3)

                # ── Phase 3: Website crawl for missing emails ─────────
                # Find companies that need website crawling from DB
                # (already persisted by _persist_result callback)
                crawl_targets: list[tuple[str, str, str]] = []
                async with pool.connection() as conn:
                    rows = await conn.execute("""
                        SELECT c.siren, ct.website, c.denomination
                        FROM contacts ct
                        JOIN companies c ON c.siren = ct.siren
                        WHERE ct.siren IN (
                            SELECT siren FROM query_tags WHERE query_name = %s
                        )
                        AND ct.source = 'google_maps'
                        AND ct.website IS NOT NULL
                        AND ct.website != ''
                        AND NOT EXISTS (
                            SELECT 1 FROM contacts ct2
                            WHERE ct2.siren = ct.siren
                            AND ct2.source = 'website_crawl'
                        )
                    """, (query_name,))
                    for row in await rows.fetchall():
                        crawl_targets.append((row[0], row[1], row[2]))

                if crawl_targets:
                    log.info(
                        "maps_discovery_runner.crawl_phase",
                        targets=len(crawl_targets),
                    )
                    async with CurlClient() as curl_client:
                        for siren, website_url, maps_name in crawl_targets:
                            try:
                                crawl_result = await curl_client.crawl(
                                    website_url,
                                    extract_emails=True,
                                    extract_social=True,
                                )
                                if crawl_result and crawl_result.get("emails"):
                                    email = crawl_result["emails"][0]
                                    social = crawl_result.get("social") or {}
                                    crawl_contact = Contact(
                                        siren=siren,
                                        source=ContactSource.WEBSITE_CRAWL,
                                        email=email,
                                        website=website_url,
                                        social_linkedin=social.get("linkedin"),
                                        social_facebook=social.get("facebook"),
                                        social_twitter=social.get("twitter"),
                                    )
                                    async with pool.connection() as conn:
                                        await upsert_contact(conn, crawl_contact)
                            except Exception as exc:
                                log.debug(
                                    "maps_discovery_runner.crawl_error",
                                    url=website_url,
                                    error=str(exc),
                                )

                # ── Mark completed ────────────────────────────────────
                await _update_job_safe(
                    conn_holder, query_id,
                    status="completed",
                    companies_scraped=companies_discovered,
                    companies_qualified=qualified,
                )
                log.info(
                    "maps_discovery_runner.complete",
                    query_id=query_id,
                    discovered=companies_discovered,
                    qualified=qualified,
                )

        except Exception as exc:
            log.error(
                "maps_discovery_runner.failed",
                query_id=query_id,
                error=str(exc),
                traceback=_traceback.format_exc(),
            )
            try:
                await _update_job_safe(conn_holder, query_id, status="failed")
            except Exception:
                pass
            raise

        finally:
            try:
                await conn_holder[0].close()
            except Exception:
                pass

    finally:
        if maps_scraper is not None:
            await maps_scraper.close()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

def main() -> None:
    """Entry point — called by `python -m fortress.maps_discovery_runner <query_id>`."""
    if len(sys.argv) < 2:
        print(
            "Usage: python -m fortress.maps_discovery_runner <query_id>",
            file=sys.stderr,
        )
        sys.exit(1)
    query_id = sys.argv[1]
    asyncio.run(run(query_id))


if __name__ == "__main__":
    main()
