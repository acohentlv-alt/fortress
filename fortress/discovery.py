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
    maps_phone: str | None = None,
    extracted_siren: str | None = None,
) -> dict | None:
    """Try to find a matching company in the SIRENE database.

    Matching order (strongest signal first, stops at first confirmed match):
      1. Enseigne (trade name) match — dedicated trade name field
      2. Phone match — unique identifier, no postal code needed
      3. Address match (street + postal code) — high confidence
      4. SIREN from website (validated) — same dept OR name overlap required
      5. Name search + scoring — last resort, always produces pending
    """
    if not maps_name or len(maps_name) < 2:
        return None

    # Extract postal code from Maps address early — used in multiple fallbacks
    maps_cp_match = re.search(r"\b(\d{5})\b", maps_address or "")
    maps_cp = maps_cp_match.group(1) if maps_cp_match else None

    normalized = _normalize_name(maps_name)
    search_terms = normalized.split()
    if not search_terms:
        return None

    primary_term = max(search_terms, key=len) if search_terms else ""
    if len(primary_term) < 3:
        return None

    # ── Pre-compute address matching tools ───────────────────────────────
    try:
        from fortress.matching.entities import normalize_address, _extract_street_key, normalize_street_key
    except ImportError:
        normalize_address = None
        _extract_street_key = None

    maps_street_key = None
    if maps_address and normalize_address and _extract_street_key:
        maps_norm_addr = normalize_address(maps_address)
        maps_street_key = _extract_street_key(maps_norm_addr)

    maps_city_tokens: set[str] = set()
    if maps_address:
        addr_norm = _normalize_name(maps_address)
        maps_city_tokens = {t for t in addr_norm.split() if len(t) > 3}

    def _score_rows(candidate_rows: list) -> tuple[dict | None, float]:
        """Score a list of candidate rows, return best candidate and its score."""
        best: dict | None = None
        best_sc = 0.0

        for row in candidate_rows:
            denom = row[2] or ""
            enseigne = row[3] or ""
            db_ville = row[9] or ""
            db_addr = row[7] or ""

            score = max(_name_match_score(maps_name, enseigne), _name_match_score(maps_name, denom))
            method = "fuzzy_name"

            city_match = False
            if maps_city_tokens and db_ville:
                db_ville_tokens = {t for t in _normalize_name(db_ville).split() if len(t) > 3}
                city_match = bool(db_ville_tokens & maps_city_tokens)
            if maps_city_tokens and db_addr:
                if any(t in _normalize_name(db_addr) for t in maps_city_tokens):
                    city_match = True

            address_match = False
            if maps_street_key and db_addr and normalize_address and _extract_street_key:
                db_street_key = _extract_street_key(normalize_address(db_addr))
                if maps_street_key and db_street_key and len(maps_street_key) > 5:
                    if maps_street_key == db_street_key:
                        address_match = True
                        method = "address"
                        score += 0.25
                    elif normalize_street_key(maps_street_key) == normalize_street_key(db_street_key):
                        address_match = True
                        method = "address"
                        score += 0.25

            if city_match:
                score += 0.15
            elif maps_city_tokens and db_ville:
                score -= 0.25

            if not address_match and city_match and score >= 0.90:
                db_cp = row[8] or ""
                if maps_cp and db_cp and maps_cp == db_cp:
                    address_match = True
                    method = "address"
                    score += 0.15

            threshold = _get_match_threshold(maps_name, _normalize_name(maps_name).split(), city_match)

            if score > best_sc and score >= threshold:
                best_sc = score
                best = {
                    "siren": row[0],
                    "denomination": denom,
                    "enseigne": enseigne,
                    "score": round(score, 2),
                    "method": method,
                    "adresse": db_addr,
                    "ville": db_ville,
                }

        return best, best_sc

    # ── Step 1: Enseigne (trade name) match ──────────────────────────────
    # The enseigne field is the official trade name in SIRENE.
    # If it matches the Maps name well, it's the business — even if the
    # legal address is different (owner registered company elsewhere).
    if departement:
        ens_cur = await conn.execute(
            """SELECT siren, siret_siege, denomination, enseigne, naf_code, naf_libelle,
                      forme_juridique, adresse, code_postal, ville, departement,
                      region, statut, date_creation, tranche_effectif,
                      latitude, longitude, fortress_id
               FROM companies
               WHERE LOWER(enseigne) LIKE %s
                 AND statut = 'A'
                 AND departement = %s
                 AND siren NOT LIKE 'MAPS%%'
               LIMIT 20""",
            (f"%{primary_term}%", departement),
        )
        ens_rows = await ens_cur.fetchall()
        if ens_rows:
            best_ens: dict | None = None
            best_ens_score = 0.0
            for row in ens_rows:
                enseigne_val = row[3] or ""
                score = _name_match_score(maps_name, enseigne_val)
                if score > best_ens_score and score >= 0.75:
                    best_ens_score = score
                    best_ens = {
                        "siren": row[0],
                        "denomination": row[2] or "",
                        "enseigne": enseigne_val,
                        "score": round(score, 2),
                        "method": "enseigne",
                        "adresse": row[7] or "",
                        "ville": row[9] or "",
                    }
            if best_ens:
                log.info(
                    "maps_discovery.enseigne_match",
                    maps_name=maps_name,
                    siren=best_ens["siren"],
                    enseigne=best_ens["enseigne"],
                    score=best_ens["score"],
                )
                return best_ens

    # ── Step 2: Phone match (unique identifier, no postal code) ──────────
    # A phone number is unique to one business — postal code check not needed.
    if maps_phone:
        norm_phone = re.sub(r'[\s\-\.]', '', maps_phone)
        if norm_phone.startswith('0') and len(norm_phone) == 10:
            norm_phone = '+33' + norm_phone[1:]

        alt_phone = None
        if norm_phone.startswith('+33'):
            alt_phone = '0' + norm_phone[3:]
        elif norm_phone.startswith('0') and len(norm_phone) == 10:
            alt_phone = '+33' + norm_phone[1:]

        for phone_val in filter(None, [norm_phone, alt_phone]):
            phone_row = await (await conn.execute(
                """SELECT c.siren, co.denomination, co.enseigne, co.adresse, co.ville
                   FROM contacts c
                   JOIN companies co ON co.siren = c.siren
                   WHERE c.phone = %s
                     AND co.siren NOT LIKE 'MAPS%%'
                     AND co.statut = 'A'
                   LIMIT 1""",
                (phone_val,),
            )).fetchone()
            if phone_row:
                log.info(
                    "maps_discovery.phone_match",
                    maps_name=maps_name,
                    siren=phone_row[0],
                    phone=phone_val,
                )
                return {
                    "siren": phone_row[0],
                    "denomination": phone_row[1] or "",
                    "enseigne": phone_row[2] or "",
                    "score": 0.90,
                    "method": "phone",
                    "adresse": phone_row[3] or "",
                    "ville": phone_row[4] or "",
                }

    # ── Step 3: Address-first fallback ────────────────────────────────────
    best_candidate: dict | None = None
    best_score = 0.0
    if maps_address and departement and normalize_address and _extract_street_key:
        maps_street = _extract_street_key(normalize_address(maps_address))
        if maps_street and len(maps_street) > 5:
            norm_street = normalize_street_key(maps_street)
            num_match = re.match(r"(\d+)", norm_street)
            street_num = num_match.group(1) if num_match else None
            street_words = [w for w in norm_street.split() if len(w) > 3 and not w.isdigit()]
            longest_word = max(street_words, key=len) if street_words else None

            if street_num and longest_word:
                addr_params: list[Any] = [departement, f"%{street_num} %", f"%{longest_word}%"]
                addr_where = (
                    "departement = %s AND statut = 'A' AND siren NOT LIKE 'MAPS%%'"
                    " AND LOWER(adresse) LIKE %s AND LOWER(adresse) LIKE %s"
                )
                if maps_cp:
                    addr_where += " AND code_postal = %s"
                    addr_params.append(maps_cp)

                addr_cur = await conn.execute(
                    f"""SELECT siren, siret_siege, denomination, enseigne, naf_code, naf_libelle,
                               forme_juridique, adresse, code_postal, ville, departement,
                               region, statut, date_creation, tranche_effectif,
                               latitude, longitude, fortress_id
                        FROM companies WHERE {addr_where} LIMIT 20""",  # noqa: S608
                    addr_params,
                )
                addr_rows = await addr_cur.fetchall()
                if addr_rows:
                    log.info(
                        "maps_discovery.address_first_search",
                        maps_name=maps_name,
                        street=norm_street,
                        postal=maps_cp,
                        candidates=len(addr_rows),
                    )
                    best_candidate, best_score = _score_rows(addr_rows)

    if best_candidate:
        log.info(
            "maps_discovery.sirene_candidate",
            maps_name=maps_name,
            siren=best_candidate["siren"],
            denomination=best_candidate["denomination"],
            score=best_candidate["score"],
            method=best_candidate["method"],
        )
        return best_candidate

    # ── Step 4: SIREN from website (validated) ────────────────────────────
    # The SIREN from a website footer could be the hosting company's SIREN
    # (O2Switch, OVH, Gandi). Validate: same department OR name overlap required.
    if extracted_siren:
        siren_cur = await conn.execute(
            """SELECT siren, denomination, enseigne, adresse, ville, departement
               FROM companies
               WHERE siren = %s AND siren NOT LIKE 'MAPS%%'
               LIMIT 1""",
            (extracted_siren,),
        )
        siren_row = await siren_cur.fetchone()
        if siren_row:
            sirene_dept = siren_row[5] or ""
            sirene_denom = siren_row[1] or ""
            sirene_enseigne = siren_row[2] or ""
            maps_tokens = {t for t in _normalize_name(maps_name).split() if len(t) > 3}
            sirene_tokens = {t for t in _normalize_name(f"{sirene_denom} {sirene_enseigne}").split() if len(t) > 3}

            same_dept = (sirene_dept == departement) if departement else False
            name_overlap = bool(maps_tokens & sirene_tokens)

            if same_dept or name_overlap:
                validated_by = "dept" if same_dept else "name"
                log.info(
                    "discovery.siren_website_validated",
                    maps_name=maps_name,
                    siren=extracted_siren,
                    validated_by=validated_by,
                )
                return {
                    "siren": siren_row[0],
                    "denomination": sirene_denom,
                    "enseigne": sirene_enseigne,
                    "score": 0.95,
                    "method": "siren_website",
                    "adresse": siren_row[3] or "",
                    "ville": siren_row[4] or "",
                }
            else:
                log.warning(
                    "discovery.siren_website_rejected",
                    maps_name=maps_name,
                    siren=extracted_siren,
                    sirene_name=sirene_denom,
                    sirene_dept=sirene_dept,
                    expected_dept=departement,
                )

    # ── Step 5: Name search + scoring (last resort — always pending) ──────
    conditions = [
        "(LOWER(enseigne) LIKE %s OR LOWER(denomination) LIKE %s)",
        "statut = 'A'",
    ]
    params: list[Any] = [f"%{primary_term}%", f"%{primary_term}%"]

    if departement and re.match(r"^\d{2,3}$", departement):
        conditions.append("departement = %s")
        params.append(departement)

    where_clause = " AND ".join(conditions)
    cur = await conn.execute(
        f"""SELECT siren, siret_siege, denomination, enseigne, naf_code, naf_libelle,
                   forme_juridique, adresse, code_postal, ville, departement,
                   region, statut, date_creation, tranche_effectif,
                   latitude, longitude, fortress_id
            FROM companies WHERE {where_clause} LIMIT 50""",  # noqa: S608
        params,
    )
    rows = await cur.fetchall()
    name_candidate, _ = _score_rows(rows)

    if name_candidate:
        # Force method to fuzzy_name — this step always produces pending
        name_candidate["method"] = "fuzzy_name"
        log.info(
            "maps_discovery.sirene_candidate",
            maps_name=maps_name,
            siren=name_candidate["siren"],
            denomination=name_candidate["denomination"],
            score=name_candidate["score"],
            method=name_candidate["method"],
        )
    else:
        log.info("maps_discovery.sirene_no_match", maps_name=maps_name)

    return name_candidate


_SIREN_CONTEXT_RE = re.compile(
    r'(?:SIREN|RCS|immatricul|enregistr|n[°o]\s*d.immatricul)[^0-9]{0,40}(\d{3}[\s\u00a0]?\d{3}[\s\u00a0]?\d{3})',
    re.IGNORECASE,
)

# SIRET is 14 digits — first 9 are the SIREN
_SIRET_RE = re.compile(
    r'(?:SIRET)[^0-9]{0,20}(\d{3}[\s\u00a0]?\d{3}[\s\u00a0]?\d{3}[\s\u00a0]?\d{5})',
    re.IGNORECASE,
)

# Footer pattern: SIREN near end of page or inside <footer> tag
_FOOTER_SIREN_RE = re.compile(
    r'(?:SIREN|RCS|SIRET|N°\s*TVA)[^0-9]{0,30}(\d{3}[\s\u00a0\-]?\d{3}[\s\u00a0\-]?\d{3})',
    re.IGNORECASE,
)


def _extract_siren_from_html(html: str) -> str | None:
    """Extract SIREN from an HTML page using 4 strategies."""
    if not html:
        return None

    # Strategy 1: Contextual match anywhere (SIREN/RCS/SIRET + digits)
    match = _SIREN_CONTEXT_RE.search(html)
    if match:
        raw = match.group(1).replace(" ", "").replace("\u00a0", "").replace("-", "")
        if len(raw) == 9 and raw.isdigit() and raw != "000000000":
            return raw

    # Strategy 2: SIRET (14 digits) — first 9 are the SIREN
    siret_match = _SIRET_RE.search(html)
    if siret_match:
        raw = siret_match.group(1).replace(" ", "").replace("\u00a0", "").replace("-", "")
        if len(raw) == 14 and raw.isdigit():
            siren = raw[:9]
            if siren != "000000000":
                return siren

    # Strategy 3: Check the footer area (last 25% of page)
    footer_start = len(html) * 3 // 4
    footer_html = html[footer_start:]
    footer_match = _FOOTER_SIREN_RE.search(footer_html)
    if footer_match:
        raw = footer_match.group(1).replace(" ", "").replace("\u00a0", "").replace("-", "")
        if len(raw) == 9 and raw.isdigit() and raw != "000000000":
            return raw

    # Strategy 4: Check inside <footer> tag if present
    footer_tag = re.search(r'<footer[^>]*>(.*?)</footer>', html, re.DOTALL | re.IGNORECASE)
    if footer_tag:
        ft_match = _FOOTER_SIREN_RE.search(footer_tag.group(1))
        if ft_match:
            raw = ft_match.group(1).replace(" ", "").replace("\u00a0", "").replace("-", "")
            if len(raw) == 9 and raw.isdigit() and raw != "000000000":
                return raw

    return None


async def _extract_siren_from_website(
    website: str, curl_client: CurlClient
) -> tuple[str | None, dict[str, str]]:
    """Extract SIREN from a company website. Also returns all fetched page HTML.

    Strategy order (fastest to slowest):
    1. Homepage — most French sites put SIREN in the footer
    2. /mentions-legales
    3. /cgu
    4. /a-propos

    Returns (siren_or_None, {url: html}) — HTML is reused by the caller for
    contact/email/social extraction (zero extra HTTP requests).
    """
    if not website:
        return None, {}

    url = website.strip()
    if not url.startswith("http"):
        url = f"https://{url}"
    url = url.rstrip("/")

    html_pages: dict[str, str] = {}
    found_siren: str | None = None

    # Check homepage first — most French SMEs have SIREN in the footer
    try:
        resp = await curl_client.get(url, timeout=5)
        if resp and resp.status_code == 200 and resp.text:
            html_pages[url] = resp.text
            siren = _extract_siren_from_html(resp.text)
            if siren:
                found_siren = siren
    except Exception:
        pass

    # Try dedicated legal pages — keep fetching all pages even if SIREN already found
    # so that /mentions-legales HTML is available for email/contact extraction
    for path in ("/mentions-legales", "/mentions-legales.html", "/cgu", "/a-propos"):
        try:
            resp = await curl_client.get(f"{url}{path}", timeout=5)
            if resp and resp.status_code == 200 and resp.text:
                html_pages[f"{url}{path}"] = resp.text
                if not found_siren:
                    siren = _extract_siren_from_html(resp.text)
                    if siren:
                        found_siren = siren
        except Exception:
            continue

    return found_siren, html_pages


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
                """SELECT batch_name, search_queries, filters_json, batch_size, workspace_id
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
            batch_size = min(batch_size, 50) if batch_size > 0 else 50
            batch_workspace_id: int | None = row[4]

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
                seen_names: set[str] = set()     # Level 1: name+address dedup
                seen_websites: set[str] = set()  # Level 2: website domain dedup
                seen_sirens: set[str] = set()    # Level 4: SIREN dedup within batch
                seen_phones: set[str] = set()    # Level 5: phone dedup within batch
                triage_counts: dict[str, int] = {"black": 0, "green": 0, "yellow": 0, "red": 0}
                _current_search_query: str = ""  # Tracks which query is active
                prev_rows: list = []  # Cross-batch dedup results (used in shortfall msg)

                # ── Cross-batch dedup: skip businesses already discovered ──
                if dept_filter:
                    async with pool.connection() as conn:
                        cur = await conn.execute(
                            """SELECT LOWER(denomination), LOWER(COALESCE(adresse, ''))
                               FROM companies
                               WHERE siren LIKE 'MAPS%%'
                               AND departement = %s""",
                            (dept_filter,),
                        )
                        prev_rows = await cur.fetchall()
                        for r in prev_rows:
                            seen_names.add(f"{(r[0] or '').strip()}|{(r[1] or '').strip()}")
                    log.info(
                        "discovery.cross_batch_dedup",
                        existing_maps_entities=len(prev_rows),
                        dept=dept_filter,
                    )

                # ── Cross-batch SIREN dedup: skip real companies already enriched ──
                if dept_filter:
                    async with pool.connection() as conn:
                        cur = await conn.execute(
                            """SELECT siren FROM companies
                               WHERE departement = %s
                               AND siren NOT LIKE 'MAPS%%'
                               AND siren IN (
                                   SELECT siren FROM contacts
                                   WHERE source = 'google_maps'
                                   AND (phone IS NOT NULL OR website IS NOT NULL)
                               )""",
                            (dept_filter,),
                        )
                        existing_sirens = await cur.fetchall()
                        for r in existing_sirens:
                            seen_sirens.add(r[0])
                    log.info(
                        "discovery.siren_dedup_loaded",
                        existing=len(seen_sirens),
                        dept=dept_filter,
                    )

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

                # Fast HTTP client for mentions légales SIREN extraction
                # No rate-limit delays — this is a single page, not Maps scraping
                curl_client = CurlClient(timeout=5, delay_min=0, delay_max=0, max_retries=1)

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

                    # ── Level 1: Name+Address dedup ───────────────────────
                    name_key = maps_name.lower().strip()
                    addr_key = (maps_address or "").lower().strip()
                    dedup_key = f"{name_key}|{addr_key}"
                    if dedup_key in seen_names or not name_key:
                        return
                    seen_names.add(dedup_key)

                    # ── Level 2: Website domain dedup ─────────────────────
                    website_domain = None
                    if maps_website:
                        _d = re.sub(r'^https?://(www\.)?', '', maps_website.strip().lower())
                        website_domain = _d.split('/')[0].split('?')[0]
                        if website_domain in seen_websites:
                            log.info("discovery.website_dedup_skip", name=maps_name, domain=website_domain)
                            return
                        seen_websites.add(website_domain)

                    # ── Level 3: SIREN extraction from website ────────────
                    extracted_siren = None
                    html_pages: dict[str, str] = {}
                    if maps_website:
                        try:
                            extracted_siren, html_pages = await _extract_siren_from_website(maps_website, curl_client)
                            if extracted_siren:
                                log.info("discovery.siren_extracted", name=maps_name, siren=extracted_siren, website=maps_website)
                        except Exception as exc:
                            log.debug("discovery.siren_extract_failed", website=maps_website, error=str(exc))

                    # ── Level 4: SIREN dedup ──────────────────────────────
                    if extracted_siren:
                        if extracted_siren in seen_sirens:
                            log.info("discovery.siren_dedup_skip", name=maps_name, siren=extracted_siren)
                            return
                        seen_sirens.add(extracted_siren)

                    # ── Level 5: Phone dedup ──────────────────────────────
                    if maps_phone:
                        clean_phone = re.sub(r'\D', '', maps_phone)
                        if clean_phone in seen_phones:
                            log.info("discovery.phone_dedup_skip", name=maps_name, phone=maps_phone)
                            return
                        seen_phones.add(clean_phone)

                    # ── SIRENE matching — all methods in one function ──────────────
                    candidate = None
                    async with pool.connection() as conn:
                        candidate = await _match_to_sirene(
                            conn, maps_name, maps_address, dept_filter, maps_phone, extracted_siren,
                        )

                    # ── Triage: classify before expensive work ────────────
                    triage_bucket = "RED"  # Default: full pipeline
                    missing_fields = {"phone": True, "email": True, "website": True}

                    # BLACK check: no usable name
                    if not maps_name or len(maps_name.strip()) < 2:
                        triage_bucket = "BLACK"

                    # BLACK check: blacklisted SIREN (if we have a SIRENE match)
                    if triage_bucket != "BLACK" and candidate and candidate.get("siren"):
                        matched_siren = candidate["siren"]
                        try:
                            async with pool.connection() as bl_conn:
                                bl_cur = await bl_conn.execute(
                                    "SELECT 1 FROM blacklisted_sirens WHERE siren = %s", (matched_siren,)
                                )
                                if await bl_cur.fetchone():
                                    triage_bucket = "BLACK"
                        except Exception:
                            pass  # If blacklist check fails, proceed normally

                    if triage_bucket == "BLACK":
                        triage_counts["black"] += 1
                        log.info("discovery.triage_black", name=maps_name)
                        return

                    # GREEN/YELLOW check: what data do we already have?
                    if candidate and candidate.get("siren"):
                        lookup_siren = candidate["siren"]
                    else:
                        lookup_siren = None

                    existing_contact = None
                    if lookup_siren:
                        try:
                            async with pool.connection() as ec_conn:
                                ec_cur = await ec_conn.execute(
                                    """SELECT phone, email, website
                                       FROM contacts
                                       WHERE siren = %s AND source = 'google_maps'
                                       ORDER BY collected_at DESC LIMIT 1""",
                                    (lookup_siren,)
                                )
                                existing_contact = await ec_cur.fetchone()
                        except Exception:
                            pass

                    if existing_contact:
                        has_phone = bool(existing_contact[0])
                        has_email = bool(existing_contact[1])
                        has_website = bool(existing_contact[2])

                        if has_phone and has_email and has_website:
                            triage_bucket = "GREEN"
                            triage_counts["green"] += 1
                            log.info("discovery.triage_green", name=maps_name, siren=lookup_siren)
                            # Tag to this batch but skip re-scraping
                            async with pool.connection() as tag_conn:
                                await bulk_tag_query(tag_conn, [lookup_siren], batch_name,
                                                     workspace_id=batch_workspace_id, batch_id=batch_id)
                            companies_discovered += 1
                            return
                        else:
                            triage_bucket = "YELLOW"
                            triage_counts["yellow"] += 1
                            missing_fields = {
                                "phone": not has_phone,
                                "email": not has_email,
                                "website": not has_website,
                            }
                    else:
                        triage_bucket = "RED"
                        triage_counts["red"] += 1
                        missing_fields = {"phone": True, "email": True, "website": True}

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
                        workspace_id=batch_workspace_id,
                    )

                    # Store candidate link metadata
                    # Address match = auto-confirm (high confidence). Name match = pending (user decides).
                    _pending_link: dict | None = None
                    if candidate:
                        _pending_link = candidate
                        if candidate["method"] in ("enseigne", "phone", "address", "siren_website"):
                            log.info(
                                "discovery.auto_linked",
                                maps_name=maps_name,
                                maps_siren=siren,
                                candidate_siren=candidate["siren"],
                                score=candidate["score"],
                                method=candidate["method"],
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
                        await bulk_tag_query(conn, [siren], batch_name, workspace_id=batch_workspace_id, batch_id=batch_id)
                        await upsert_contact(conn, contact)

                        # Store link metadata on the MAPS entity
                        # Address match → confirmed (auto-link). Name match → pending (user decides).
                        if _pending_link:
                            confidence = "confirmed" if _pending_link["method"] in ("enseigne", "phone", "address", "siren_website") else "pending"
                            await conn.execute("""
                                UPDATE companies
                                SET linked_siren = %s, link_confidence = %s, link_method = %s
                                WHERE siren = %s
                            """, (_pending_link["siren"], confidence, _pending_link["method"], siren))

                            # Copy SIRENE reference data into the MAPS entity (confirmed links only)
                            # Populates NAF, legal form, postal code, etc. for a complete company card.
                            # denomination, enseigne, adresse are intentionally kept from Google Maps.
                            if confidence == "confirmed":
                                cur = await conn.execute(
                                    """SELECT siret_siege, naf_code, naf_libelle,
                                              forme_juridique, code_postal, ville,
                                              date_creation, tranche_effectif
                                       FROM companies WHERE siren = %s""",
                                    (_pending_link["siren"],)
                                )
                                sirene_row = await cur.fetchone()
                                if sirene_row:
                                    await conn.execute(
                                        """UPDATE companies
                                           SET siret_siege     = %s,
                                               naf_code        = %s,
                                               naf_libelle     = %s,
                                               forme_juridique = %s,
                                               code_postal     = %s,
                                               ville           = %s,
                                               date_creation   = %s,
                                               tranche_effectif = COALESCE(tranche_effectif, %s)
                                           WHERE siren = %s""",
                                        (
                                            sirene_row[0],
                                            sirene_row[1],
                                            sirene_row[2],
                                            sirene_row[3],
                                            sirene_row[4],
                                            sirene_row[5],
                                            sirene_row[6],
                                            sirene_row[7],
                                            siren,
                                        )
                                    )

                        await log_audit(
                            conn,
                            batch_id=batch_id,
                            siren=siren,
                            action="maps_lookup",
                            result="success" if has_data else "no_data",
                            source_url=maps_result.get("maps_url"),
                            duration_ms=None,
                            search_query=_current_search_query or None,
                            workspace_id=batch_workspace_id,
                        )

                    # ── Website crawl: extract contacts from already-fetched HTML ──────
                    # html_pages was collected during SIREN extraction — zero extra requests
                    # for homepage and /mentions-legales. Only /contact is a new request.
                    if maps_website and html_pages:
                        try:
                            from fortress.matching.contacts import (
                                extract_emails, extract_phones, extract_social_links,
                                extract_mentions_legales, parse_schema_org,
                                is_personal_email, is_agency_email,
                            )
                            from fortress.models import Officer as OfficerModel
                            from fortress.processing.dedup import upsert_officer

                            combined_html = "\n".join(html_pages.values())

                            phones_found = []
                            if missing_fields.get("phone", True):
                                phones_found = extract_phones(combined_html)

                            emails_found = []
                            raw_emails = []
                            if missing_fields.get("email", True):
                                raw_emails = extract_emails(combined_html)
                                emails_found = [
                                    e for e in raw_emails
                                    if not is_personal_email(e, maps_name)
                                    and not is_agency_email(e, maps_website)
                                ]
                                if raw_emails and not emails_found:
                                    log.info(
                                        "discovery.all_emails_filtered",
                                        siren=siren,
                                        website=maps_website,
                                        raw_emails=raw_emails[:5],
                                        reason="all emails rejected by personal/agency filters",
                                    )

                            social = extract_social_links(combined_html)
                            schema = parse_schema_org(combined_html)
                            log.info(
                                "discovery.website_crawl_debug",
                                siren=siren,
                                pages_fetched=list(html_pages.keys()),
                                html_length=len(combined_html),
                                raw_emails_found=len(raw_emails),
                                emails_after_filter=len(emails_found),
                                phones_found=len(phones_found),
                                social_keys=list(social.keys()),
                                triage=triage_bucket,
                            )

                            # If no email found yet, try /contact page (one extra request)
                            if missing_fields.get("email", True) and not emails_found:
                                _base_url = maps_website.strip().rstrip("/")
                                if not _base_url.startswith("http"):
                                    _base_url = f"https://{_base_url}"
                                try:
                                    _resp = await curl_client.get(f"{_base_url}/contact", timeout=5)
                                    if _resp and _resp.status_code == 200 and _resp.text:
                                        extra = extract_emails(_resp.text)
                                        emails_found = [
                                            e for e in extra
                                            if not is_personal_email(e, maps_name)
                                            and not is_agency_email(e, maps_website)
                                        ]
                                except Exception:
                                    pass

                            # Best phone: prefer landline (01-05, 09) over mobile (06-07)
                            best_phone = None
                            if phones_found:
                                def _digits(p: str) -> str:
                                    d = re.sub(r"[^0-9]", "", p)
                                    return "0" + d[2:] if d.startswith("33") and len(d) == 11 else d
                                landlines = [p for p in phones_found if _digits(p)[:2] in ("01","02","03","04","05","09")]
                                best_phone = landlines[0] if landlines else phones_found[0]
                            if not best_phone and schema.get("phone"):
                                best_phone = schema["phone"]

                            best_email = emails_found[0] if emails_found else schema.get("email")

                            # Save website_crawl contact row if we got anything useful
                            has_crawl_data = any([
                                best_email, best_phone,
                                social.get("linkedin"), social.get("facebook"),
                                social.get("instagram"), social.get("twitter"), social.get("tiktok"),
                            ])
                            if has_crawl_data:
                                crawl_contact = Contact(
                                    siren=siren,
                                    source=ContactSource.WEBSITE_CRAWL,
                                    phone=best_phone,
                                    email=best_email,
                                    website=maps_website,
                                    social_linkedin=social.get("linkedin"),
                                    social_facebook=social.get("facebook"),
                                    social_twitter=social.get("twitter"),
                                    social_instagram=social.get("instagram"),
                                    social_tiktok=social.get("tiktok"),
                                )
                                async with pool.connection() as crawl_conn:
                                    await upsert_contact(crawl_conn, crawl_contact)

                            # Director from mentions-légales (if that page was fetched)
                            ml_html = next(
                                (html for page_url, html in html_pages.items() if "mentions" in page_url.lower()),
                                None,
                            )
                            log.info(
                                "discovery.mentions_legales_lookup",
                                siren=siren,
                                ml_html_found=ml_html is not None,
                                html_pages_keys=list(html_pages.keys()) if not ml_html else None,
                            )
                            if ml_html:
                                ml_data = extract_mentions_legales(
                                    ml_html,
                                    company_siren=extracted_siren,
                                    website_domain=website_domain,
                                )
                                log.info(
                                    "discovery.mentions_legales_extracted",
                                    siren=siren,
                                    director_name=ml_data.get("director_name"),
                                    director_role=ml_data.get("director_role"),
                                    has_director=bool(ml_data.get("director_name")),
                                )
                                if ml_data.get("director_name"):
                                    director = OfficerModel(
                                        siren=siren,
                                        nom=ml_data["director_name"],
                                        role=ml_data.get("director_role"),
                                        civilite=ml_data.get("director_civilite"),
                                        email_direct=ml_data.get("director_email"),
                                        source=ContactSource.MENTIONS_LEGALES,
                                    )
                                    async with pool.connection() as officer_conn:
                                        await upsert_officer(officer_conn, director)
                                    log.info(
                                        "discovery.mentions_legales_officer_upserted",
                                        siren=siren,
                                        officer_name=ml_data["director_name"],
                                        officer_role=ml_data.get("director_role"),
                                    )

                        except Exception as exc:
                            log.warning("discovery.website_crawl_failed", siren=siren, error=str(exc), exc_type=type(exc).__name__, exc_info=True)

                    # Heartbeat before INPI — website crawl can be slow, keep updated_at fresh
                    await _update_job_safe(
                        conn_holder, batch_id,
                        companies_scraped=companies_discovered,
                        companies_qualified=qualified,
                        total_companies=companies_discovered,
                    )

                    # INPI: for high-confidence matches (address, website SIREN, phone+postal)
                    # Fuzzy name matches wait for user confirmation before INPI call
                    if _pending_link and _pending_link["method"] in ("enseigne", "phone", "address", "siren_website"):
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
                                        vals.append(siren)
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
                        triage_black=triage_counts["black"],
                        triage_green=triage_counts["green"],
                        triage_yellow=triage_counts["yellow"],
                        triage_red=triage_counts["red"],
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

                # ── Shortfall detection ────────────────────────────────
                shortfall_msg = None
                if batch_size > 0 and companies_discovered < batch_size:
                    already_known = len(prev_rows) if dept_filter and prev_rows else 0
                    if already_known > 0:
                        shortfall_msg = (
                            f"{qualified} nouvelles entreprises qualifiées "
                            f"(objectif : {batch_size}). "
                            f"{already_known} entreprises de cette zone avaient déjà été découvertes "
                            f"et ont été exclues pour éviter les doublons."
                        )
                    else:
                        shortfall_msg = (
                            f"Google Maps n'a trouvé que {qualified} entreprises "
                            f"qualifiées pour cette recherche (objectif : {batch_size}). "
                            f"Il n'y a pas plus de résultats disponibles."
                        )
                    log.info(
                        "discovery.shortfall",
                        found=companies_discovered,
                        target=batch_size,
                        shortfall=batch_size - companies_discovered,
                    )

                # ── Mark completed or interrupted ─────────────────────
                final_status = "interrupted" if _shutdown else "completed"
                _final_written = False
                for _attempt in range(3):
                    try:
                        await _update_job_safe(
                            conn_holder, batch_id,
                            status=final_status,
                            companies_scraped=companies_discovered,
                            companies_qualified=qualified,
                            shortfall_reason=shortfall_msg,
                            triage_black=triage_counts["black"],
                            triage_green=triage_counts["green"],
                            triage_yellow=triage_counts["yellow"],
                            triage_red=triage_counts["red"],
                        )
                        _final_written = True
                        break
                    except Exception as _exc:
                        log.warning(
                            "discovery.final_status_write_failed",
                            attempt=_attempt + 1,
                            error=str(_exc),
                        )
                        if _attempt < 2:
                            await asyncio.sleep(2)
                if not _final_written:
                    log.error(
                        "discovery.final_status_write_all_failed",
                        batch_id=batch_id,
                        final_status=final_status,
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
            try:
                await curl_client.close()
            except Exception:
                pass

    finally:
        if maps_scraper is not None:
            try:
                await asyncio.wait_for(maps_scraper.close(), timeout=5)
            except (asyncio.TimeoutError, Exception):
                log.warning("maps_scraper.close_timeout")


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
