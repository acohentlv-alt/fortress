"""Maps-first discovery runner — discover businesses from Google Maps,
then match to SIRENE database for legal enrichment.

Usage:
    python -m fortress.discovery <batch_id>

Reads the batch_data row for batch_id, runs the Maps-first pipeline:
  1. search_all(query) for each search term → discover businesses
  2. SIRENE matching → find existing company by name/enseigne
  3. Website crawl → extract emails for companies with websites
  4. Persist companies + contacts + batch_tags + batch_log

batch_data status lifecycle:
    queued → in_progress → completed
                       └→ failed
"""

from __future__ import annotations

import asyncio
import json
import re
import signal
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
from fortress.scraping.http import CurlClient
from fortress.processing.dedup import (
    bulk_tag_query,
    log_audit,
    upsert_company,
    upsert_contact,
)

log = structlog.get_logger()

# Graceful shutdown flag — set by SIGTERM handler
_shutdown = False

def _handle_sigterm(signum, frame):
    """Handle SIGTERM from Render deploy — set shutdown flag."""
    global _shutdown
    _shutdown = True
    log.warning("discovery.sigterm_received", msg="Graceful shutdown requested")

signal.signal(signal.SIGTERM, _handle_sigterm)

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
    Splits on apostrophes so "d'agriculture" → "d agriculture" (matches SIRENE).
    """
    # Decompose accents
    nfkd = unicodedata.normalize("NFKD", name.lower())
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Split on apostrophes BEFORE removing punctuation
    # so "d'agriculture" → "d agriculture" not "dagriculture"
    ascii_name = ascii_name.replace("'", " ").replace("\u2019", " ")
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


# ---------------------------------------------------------------------------
# Smarter scoring — detect names/industries that need higher thresholds
# ---------------------------------------------------------------------------

# Industry words that are too generic to match on name alone
_INDUSTRY_WORDS = {
    "transport", "transports", "logistique", "logistiq", "camping",
    "hotel", "hotels", "restaurant", "boulangerie", "pharmacie",
    "garage", "plomberie", "electricite", "menuiserie", "pressing",
    "coiffure", "beaute", "auto", "taxi", "ambulance", "demenagement",
    "nettoyage", "securite", "formation", "conseil", "immobilier",
    "assurance", "agence", "bureau", "services", "solutions",
}


def _is_person_name(name: str) -> bool:
    """Detect if a name looks like a person name (e.g. 'LORENE PRIGENT').

    Heuristic: exactly 2 capitalized tokens, no digits, no common business words.
    """
    tokens = name.strip().split()
    if len(tokens) != 2:
        return False
    if any(c.isdigit() for c in name):
        return False
    # Both tokens should be alpha-only and start with uppercase
    return all(t[0].isupper() and t.isalpha() for t in tokens)


def _is_industry_generic(name: str) -> bool:
    """Check if the normalized name contains mostly industry/generic words."""
    tokens = set(_normalize_name(name).split())
    return bool(tokens & _INDUSTRY_WORDS)


def _get_match_threshold(maps_name: str, name_tokens: list[str], city_match: bool) -> float:
    """Return the appropriate match threshold based on name characteristics."""
    # Person names need very high confidence
    if _is_person_name(maps_name):
        return 0.95

    # Industry-generic names need very high confidence
    if _is_industry_generic(maps_name):
        return 0.95

    # Short/generic names (≤2 tokens) without city match need higher threshold
    if len(name_tokens) <= 2 and not city_match:
        return 0.95

    # Default threshold (raised from 0.6)
    return 0.80


async def _match_to_sirene(
    conn: Any,
    maps_name: str,
    maps_address: str | None,
    departement: str | None,
) -> dict | None:
    """Try to find a matching company in the SIRENE database.

    Search strategy:
      1. Search by enseigne (exact fuzzy match)
      2. Search by denomination (exact fuzzy match)
      3. Search by denomination + ville overlap

    Returns candidate metadata dict (siren, score, method, denomination) or None.
    Never auto-links — the caller stores this as a 'pending' suggestion.
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

    # Extract city from Maps address for cross-checking
    maps_city_tokens = set()
    if maps_address:
        # Maps address typically ends with "NNNNN City, France"
        addr_norm = _normalize_name(maps_address)
        # All meaningful tokens from the Maps address
        maps_city_tokens = {t for t in addr_norm.split() if len(t) > 3}

    # Score each candidate
    best_candidate: dict | None = None
    best_score = 0.0
    best_method = "fuzzy_name"

    # Address matching — import utils from entity_matcher
    try:
        from fortress.matching.entities import normalize_address, _extract_street_key
    except ImportError:
        normalize_address = None
        _extract_street_key = None

    # Pre-compute Maps street key for address comparison
    maps_street_key = None
    if maps_address and normalize_address and _extract_street_key:
        maps_norm_addr = normalize_address(maps_address)
        maps_street_key = _extract_street_key(maps_norm_addr)

    for row in rows:
        denom = row[2] or ""
        enseigne = row[3] or ""
        db_ville = row[9] or ""  # row[9] = ville
        db_addr = row[7] or ""   # row[7] = adresse

        # Score against both enseigne and denomination
        score_enseigne = _name_match_score(maps_name, enseigne)
        score_denom = _name_match_score(maps_name, denom)
        score = max(score_enseigne, score_denom)
        method = "fuzzy_name"

        # City cross-check: does the SIRENE city appear in the Maps address?
        city_match = False
        if maps_city_tokens and db_ville:
            db_ville_norm = _normalize_name(db_ville)
            db_ville_tokens = {t for t in db_ville_norm.split() if len(t) > 3}
            city_match = bool(db_ville_tokens & maps_city_tokens)

        if maps_city_tokens and db_addr:
            db_addr_norm = _normalize_name(db_addr)
            if any(t in db_addr_norm for t in maps_city_tokens):
                city_match = True

        # Street-level address match (high signal)
        address_match = False
        if maps_street_key and db_addr and normalize_address and _extract_street_key:
            db_norm_addr = normalize_address(db_addr)
            db_street_key = _extract_street_key(db_norm_addr)
            if maps_street_key and db_street_key and len(maps_street_key) > 5:
                if maps_street_key == db_street_key:
                    address_match = True
                    method = "address"
                    score += 0.25

        # Boost if city matches, penalize if cities are clearly different
        if city_match:
            score += 0.15
        elif maps_city_tokens and db_ville:
            score -= 0.25

        # Use smart threshold based on name characteristics
        name_tokens = _normalize_name(maps_name).split()
        threshold = _get_match_threshold(maps_name, name_tokens, city_match)

        if score > best_score and score >= threshold:
            best_score = score
            best_method = method
            best_candidate = {
                "siren": row[0],
                "denomination": denom,
                "enseigne": enseigne,
                "score": round(score, 2),
                "method": best_method,
                "adresse": db_addr,
                "ville": db_ville,
            }

    if best_candidate:
        log.info(
            "maps_discovery.sirene_candidate",
            maps_name=maps_name,
            siren=best_candidate["siren"],
            denomination=best_candidate["denomination"],
            score=best_candidate["score"],
            method=best_candidate["method"],
            status="pending",
        )
    else:
        log.info(
            "maps_discovery.sirene_no_match",
            maps_name=maps_name,
            candidates=len(rows),
        )

    return best_candidate


# ---------------------------------------------------------------------------
# Status update helpers (same pattern as runner.py)
# ---------------------------------------------------------------------------

async def _update_job(
    conn: psycopg.AsyncConnection, batch_id: str, /, **fields: object
) -> None:
    if not fields:
        return
    set_clause = ", ".join(f"{col} = %s" for col in fields)
    params = [*fields.values(), batch_id]
    await conn.execute(
        f"UPDATE batch_data SET {set_clause}, updated_at = NOW() "  # noqa: S608
        f"WHERE batch_id = %s",
        params,
    )
    await conn.commit()


async def _update_job_safe(
    conn_holder: list[psycopg.AsyncConnection], batch_id: str, /, **fields: object
) -> None:
    conn = conn_holder[0]
    try:
        await _update_job(conn, batch_id, **fields)
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
        await _update_job(new_conn, batch_id, **fields)


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def run(batch_id: str) -> None:
    """Run the Maps-first discovery pipeline for a given batch_id."""
    import traceback as _traceback

    log.info("discovery.start", batch_id=batch_id)

    # ── Start Google Maps scraper ─────────────────────────────────────
    maps_scraper = None
    try:
        from fortress.scraping.maps import PlaywrightMapsScraper
        maps_scraper = PlaywrightMapsScraper()
        await maps_scraper.start()
        log.info("discovery.browser_started")
    except Exception as exc:
        log.error("discovery.browser_failed", error=str(exc))
        # Can't do Maps-first without Chrome — mark failed
        try:
            status_conn = await psycopg.AsyncConnection.connect(
                settings.db_url, autocommit=False, **_KEEPALIVE_PARAMS,
            )
            await _update_job(status_conn, batch_id, status="failed")
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
            await _update_job_safe(conn_holder, batch_id, status="in_progress")

            # ── Load job metadata ─────────────────────────────────────
            cur = await conn_holder[0].execute(
                """SELECT batch_name, search_queries, filters_json, batch_size
                   FROM batch_data WHERE batch_id = %s LIMIT 1""",
                (batch_id,),
            )
            row = await cur.fetchone()
            if not row:
                raise RuntimeError(f"No batch_data row for batch_id={batch_id!r}")

            batch_name: str = row[0]
            raw_queries = row[1]
            filters_raw = row[2]
            batch_size: int = row[3] or 0  # 0 = collect all results

            # Parse search_queries from JSON
            search_queries: list[str] = []
            if raw_queries:
                if isinstance(raw_queries, str):
                    search_queries = json.loads(raw_queries)
                else:
                    search_queries = list(raw_queries)

            if not search_queries:
                # Fallback: use batch_name as the search query
                search_queries = [batch_name]

            log.info(
                "discovery.loaded_job",
                batch_id=batch_id,
                batch_name=batch_name,
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
                _current_search_query: str = ""  # Tracks which query is active

                # ── Resume support: skip already-processed companies ──
                async with pool.connection() as conn:
                    cur = await conn.execute(
                        """SELECT sa.siren, co.denomination, co.adresse
                           FROM batch_log sa
                           LEFT JOIN companies co ON co.siren = sa.siren
                           WHERE sa.batch_id = %s""",
                        (batch_id,),
                    )
                    existing_rows = await cur.fetchall()

                if existing_rows:
                    for row in existing_rows:
                        siren, denom, addr = row[0], row[1] or "", row[2] or ""
                        name_key = denom.lower().strip()
                        addr_key = addr.lower().strip()
                        seen_names.add(f"{name_key}|{addr_key}")
                    companies_discovered = len(existing_rows)
                    # Count qualified (those with phone or website)
                    async with pool.connection() as conn:
                        qr = await conn.execute(
                            """SELECT COUNT(DISTINCT c.siren) FROM contacts c
                               WHERE c.siren IN (SELECT siren FROM batch_log WHERE batch_id = %s)
                               AND (c.phone IS NOT NULL OR c.website IS NOT NULL)""",
                            (batch_id,),
                        )
                        qrow = await qr.fetchone()
                        qualified = qrow[0] if qrow else 0

                    log.info(
                        "discovery.resume_skip",
                        batch_id=batch_id,
                        already_processed=companies_discovered,
                        already_qualified=qualified,
                    )

                # ── Inline persist callback ────────────────────────────
                # This runs for EACH business extracted by search_all,
                # ensuring data is saved to DB immediately (not after
                # all queries finish). If Chrome crashes mid-batch,
                # already-saved businesses are retained.

                async def _persist_result(maps_result: dict[str, Any]) -> None:
                    nonlocal companies_discovered, qualified

                    # Stop collecting once we've reached the user's target
                    if batch_size > 0 and companies_discovered >= batch_size:
                        return

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

                    # SIRENE matching — find candidate but never auto-link
                    async with pool.connection() as conn:
                        candidate = await _match_to_sirene(
                            conn, maps_name, maps_address, dept_filter,
                        )

                    companies_discovered += 1
                    idx = companies_discovered

                    # ALWAYS create a MAPS entity — never use matched SIREN as entity ID
                    async with pool.connection() as id_conn:
                        cur = await id_conn.execute(
                            """SELECT MAX(CAST(SUBSTRING(siren FROM 5) AS INTEGER))
                               FROM companies WHERE siren LIKE 'MAPS%%'"""
                        )
                        max_row = await cur.fetchone()
                        next_id = (max_row[0] or 0) + 1 if max_row else 1
                    siren = f"MAPS{next_id:05d}"
                    company = Company(
                        siren=siren,
                        denomination=maps_name,
                        enseigne=maps_name,
                        adresse=maps_address,
                        departement=dept_filter,
                        statut="A",
                    )

                    # Store candidate link metadata
                    # Address match = auto-confirm (high confidence). Name match = pending (user decides).
                    _pending_link: dict | None = None
                    if candidate:
                        _pending_link = candidate
                        if candidate["method"] == "address":
                            log.info(
                                "discovery.auto_linked",
                                maps_name=maps_name,
                                maps_siren=siren,
                                candidate_siren=candidate["siren"],
                                score=candidate["score"],
                                method="address",
                            )
                        else:
                            log.info(
                                "discovery.pending_link",
                                maps_name=maps_name,
                                maps_siren=siren,
                                candidate_siren=candidate["siren"],
                                score=candidate["score"],
                                method=candidate["method"],
                            )
                    else:
                        log.info(
                            "discovery.new_entity",
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
                        await bulk_tag_query(conn, [siren], batch_name)
                        await upsert_contact(conn, contact)

                        # Store link metadata on the MAPS entity
                        # Address match → confirmed (auto-link). Name match → pending (user decides).
                        if _pending_link:
                            confidence = "confirmed" if _pending_link["method"] == "address" else "pending"
                            await conn.execute("""
                                UPDATE companies
                                SET linked_siren = %s, link_confidence = %s, link_method = %s
                                WHERE siren = %s
                            """, (_pending_link["siren"], confidence, _pending_link["method"], siren))

                        await log_audit(
                            conn,
                            batch_id=batch_id,
                            siren=siren,
                            action="maps_lookup",
                            result="success" if has_data else "no_data",
                            source_url=maps_result.get("maps_url"),
                            duration_ms=None,
                            search_query=_current_search_query or None,
                        )

                    # INPI: only for address-matched candidates (very high confidence)
                    # Fuzzy name matches wait for user confirmation before INPI call
                    if _pending_link and _pending_link["method"] == "address":
                        try:
                            from fortress.matching.inpi import fetch_dirigeants
                            from fortress.models import Officer, ContactSource as CS
                            from fortress.processing.dedup import upsert_officer

                            target_siren = _pending_link["siren"]
                            dirigeants, re_company_data = await fetch_dirigeants(target_siren)
                            async with pool.connection() as conn:
                                for d in dirigeants:
                                    officer = Officer(
                                        siren=target_siren,
                                        nom=d["nom"],
                                        prenom=d.get("prenom"),
                                        role=d.get("qualite"),
                                        civilite=d.get("civilite"),
                                        source=CS.RECHERCHE_ENTREPRISES,
                                    )
                                    await upsert_officer(conn, officer)

                                if dirigeants:
                                    await log_audit(
                                        conn, batch_id=batch_id, siren=siren,
                                        action="officers_found", result="success",
                                        detail=f"{len(dirigeants)} dirigeant(s) — Registre National (API)",
                                    )

                                if re_company_data:
                                    parts, vals = [], []
                                    if "chiffre_affaires" in re_company_data:
                                        parts.append("chiffre_affaires = %s")
                                        vals.append(re_company_data["chiffre_affaires"])
                                    if "resultat_net" in re_company_data:
                                        parts.append("resultat_net = %s")
                                        vals.append(re_company_data["resultat_net"])
                                    if "tranche_effectif" in re_company_data:
                                        parts.append("tranche_effectif = COALESCE(tranche_effectif, %s)")
                                        vals.append(re_company_data["tranche_effectif"])
                                    if parts:
                                        vals.append(target_siren)
                                        await conn.execute(
                                            f"UPDATE companies SET {', '.join(parts)} WHERE siren = %s",
                                            tuple(vals),
                                        )
                        except Exception as exc:
                            log.warning("discovery.inpi_failed", siren=siren, error=str(exc))

                    # Update progress (keeps heartbeat alive)
                    await _update_job_safe(
                        conn_holder, batch_id,
                        companies_scraped=companies_discovered,
                        companies_qualified=qualified,
                        total_companies=companies_discovered,
                    )

                    if idx % 10 == 0:
                        log.info(
                            "discovery.progress",
                            processed=idx,
                            qualified=qualified,
                        )

                # ── Maps Discovery (with inline persistence) ──────────
                for q_idx, search_query in enumerate(search_queries, 1):
                    # Check for graceful shutdown
                    if _shutdown:
                        log.warning(
                            "discovery.shutdown_before_query",
                            query=search_query,
                            progress=f"{q_idx}/{total_queries}",
                            saved=companies_discovered,
                        )
                        break

                    # Check if batch_size already reached (across queries)
                    if batch_size > 0 and companies_discovered >= batch_size:
                        log.info(
                            "discovery.batch_size_reached_global",
                            qualified=qualified,
                            target=batch_size,
                        )
                        break

                    # Set current search query for the closure
                    _current_search_query = search_query

                    log.info(
                        "discovery.search_start",
                        query=search_query,
                        progress=f"{q_idx}/{total_queries}",
                    )

                    await _update_job_safe(
                        conn_holder, batch_id,
                        wave_current=q_idx,
                        wave_total=total_queries,
                    )

                    # search_all calls _persist_result for each business
                    results = await maps_scraper.search_all(
                        search_query, on_result=_persist_result,
                    )

                    log.info(
                        "discovery.search_done",
                        query=search_query,
                        results=len(results),
                        total_saved=companies_discovered,
                        total_qualified=qualified,
                    )

                    # Small delay between searches to avoid detection
                    if q_idx < total_queries:
                        await asyncio.sleep(3)

                # Web crawl skipped in Maps-first discovery mode.
                # Maps provides phone + website with 90%+ hit rates.
                # Email enrichment via website crawl can be triggered
                # separately per-company or as a follow-up batch.

                # ── Mark completed or interrupted ─────────────────────
                final_status = "interrupted" if _shutdown else "completed"
                await _update_job_safe(
                    conn_holder, batch_id,
                    status=final_status,
                    companies_scraped=companies_discovered,
                    companies_qualified=qualified,
                )
                log.info(
                    f"discovery.{final_status}",
                    batch_id=batch_id,
                    discovered=companies_discovered,
                    qualified=qualified,
                    shutdown=_shutdown,
                )

        except Exception as exc:
            log.error(
                "discovery.failed",
                batch_id=batch_id,
                error=str(exc),
                traceback=_traceback.format_exc(),
            )
            try:
                await _update_job_safe(conn_holder, batch_id, status="failed")
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
    """Entry point — called by `python -m fortress.discovery <batch_id>`."""
    if len(sys.argv) < 2:
        print(
            "Usage: python -m fortress.discovery <batch_id>",
            file=sys.stderr,
        )
        sys.exit(1)
    batch_id = sys.argv[1]
    asyncio.run(run(batch_id))


if __name__ == "__main__":
    main()
