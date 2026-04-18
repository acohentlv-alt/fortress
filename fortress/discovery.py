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

from fortress.config.departments import DEPARTMENTS  # noqa: F401
from fortress.config.sector_relevance import is_irrelevant_category, is_irrelevant_name
from fortress.config.settings import settings
from fortress.models import Company, Contact, ContactSource
from fortress.scraping.http import CurlClient
from fortress.scraping.crawl import crawl_website
from fortress.processing.dedup import (
    bulk_tag_query,
    log_audit,
    upsert_company,
    upsert_contact,
)

log = structlog.get_logger()


def _compute_naf_status(
    matched_naf: str | None,
    picked_naf: str,
    division_whitelist: list[str] | None,
) -> str:
    """Return 'verified' or 'mismatch' based on SIRENE NAF vs picked filter."""
    candidate = (matched_naf or "")
    if division_whitelist is not None:
        # Section letter: match if NAF starts with any division in whitelist
        return "verified" if any(candidate.startswith(d) for d in division_whitelist) else "mismatch"
    return "verified" if candidate.startswith(picked_naf) else "mismatch"


async def _copy_sirene_reference_data(conn, maps_siren: str, target_siren: str) -> None:
    """Copy SIRENE reference fields from the real SIREN row onto the MAPS entity.

    Populates: siret_siege, naf_code, naf_libelle, forme_juridique,
    code_postal, ville, date_creation, tranche_effectif.
    denomination / enseigne / adresse intentionally kept from Google Maps.

    Called on both pipeline auto-confirm and /link approve endpoint —
    single source of truth for SIRENE→MAPS copy semantics.
    """
    sirene_cur = await conn.execute(
        """SELECT siret_siege, naf_code, naf_libelle,
                  forme_juridique, code_postal, ville,
                  date_creation, tranche_effectif
           FROM companies WHERE siren = %s""",
        (target_siren,),
    )
    sirene_row = await sirene_cur.fetchone()
    if not sirene_row:
        return
    await conn.execute(
        """UPDATE companies
           SET siret_siege      = %s,
               naf_code         = %s,
               naf_libelle      = %s,
               forme_juridique  = %s,
               code_postal      = %s,
               ville            = %s,
               date_creation    = %s,
               tranche_effectif = COALESCE(tranche_effectif, %s)
           WHERE siren = %s""",
        (sirene_row[0], sirene_row[1], sirene_row[2], sirene_row[3],
         sirene_row[4], sirene_row[5], sirene_row[6], sirene_row[7], maps_siren),
    )


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
        "earl", "gaec", "scea", "scev",
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
    "clinique", "laboratoire", "cabinet", "boutique", "atelier",
    "studio", "institut", "societe", "entreprise", "groupe",
    "espace", "comptabilite", "expertise", "renovation",
    "construction", "batiment", "travaux", "distribution",
    "location", "maintenance", "depannage", "livraison",
    "commerce", "import", "export", "editions", "production",
    "communication", "informatique", "digital", "consulting",
    "ingenierie", "technique", "technologies", "systemes",
    "medical", "dentaire", "optique", "veterinaire",
    "village", "domaine", "chateau", "parc", "residence",
    "vacances", "loisirs", "tourisme", "club",
    "ferme", "auberge", "gite", "relais",
    "hotellerie", "restauration",
}


# Match methods strong enough for pipeline auto-confirm (with verified NAF).
# Weak variants (enseigne_weak, phone_weak, surname, fuzzy_name) never auto-confirm.
_STRONG_METHODS = frozenset({"inpi", "siren_website", "enseigne", "phone", "address"})

_LEGAL_FORM_TOKENS = frozenset({
    "earl", "gaec", "scea", "scev", "sci", "sarl", "sas",
    "sasu", "eurl", "sa", "snc", "eirl", "ei",
})

_SURNAME_PREFIXES = frozenset({
    "domaine", "mas", "chateau", "cave", "vignoble", "clos",
})

_FOREIGN_INDICATORS = frozenset({
    "espagne", "spain", "españa", "espanya",
    "italia", "italy", "italie",
    "deutschland", "germany", "allemagne",
    "belgique", "belgium", "belgie", "belgië",
    "suisse", "switzerland", "schweiz", "svizzera",
    "luxembourg", "luxemburg",
    "andorra", "andorre",
    "united kingdom", "royaume-uni",
})


def _is_in_france(address: str | None) -> bool:
    """Reject addresses clearly outside France."""
    if not address:
        return True  # benefit of the doubt
    lower = address.lower()
    # Check for foreign country/city names
    for indicator in _FOREIGN_INDICATORS:
        if indicator in lower:
            return False
    # Validate postal code if present
    m = re.search(r"\b(\d{5})\b", address)
    if m:
        cp = m.group(1)
        dept = int(cp[:2])
        # Metropolitan France: 01-95, Overseas: 971-976
        if 1 <= dept <= 95:
            return True
        overseas = int(cp[:3])
        if 971 <= overseas <= 976:
            return True
        return False  # invalid French postal code
    return True  # no postal code found, keep it


def _is_person_name(name: str) -> bool:
    """Detect if a name looks like a person name (e.g. 'LORENE PRIGENT').

    Heuristic: exactly 2 capitalized tokens, no digits, no legal form prefix.
    """
    tokens = name.strip().split()
    if len(tokens) != 2:
        return False
    if any(c.isdigit() for c in name):
        return False
    if not all(t[0].isupper() and t.isalpha() for t in tokens):
        return False
    # If the first token is a legal form, this is not a person name
    if tokens[0].lower() in _LEGAL_FORM_TOKENS:
        return False
    return True


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
      1. SIREN from website (validated) — strongest signal, company declared its own SIREN
      2. Enseigne (trade name) match — dedicated trade name field
      3. Phone match — unique identifier, no postal code needed
      4. Address match (street + postal code) — high confidence
      5. Name search + scoring — last resort, always produces pending
    """
    if not maps_name or len(maps_name) < 2:
        return None

    # Extract postal code from Maps address early — used in multiple fallbacks
    maps_cp_match = re.search(r"\b(\d{5})\b", maps_address or "")
    maps_cp = maps_cp_match.group(1) if maps_cp_match else None

    # ── Step 0: INPI name search (primary — fastest confirmed match) ──────────
    try:
        from fortress.matching.inpi import search_by_name as _inpi_search
        _inpi_hit = await _inpi_search(
            query=_normalize_name(maps_name),
            dept=departement,
            cp=maps_cp,
        )
        if _inpi_hit is not None:
            _inpi_siren, _inpi_naf, _inpi_nom, _inpi_cp = _inpi_hit
            # Validate against local SIRENE database
            _inpi_cur = await conn.execute(
                """SELECT siren, denomination, enseigne, adresse, code_postal, ville, departement
                   FROM companies
                   WHERE siren = %s AND statut = 'A' AND siren NOT LIKE 'MAPS%%'
                   LIMIT 1""",
                (_inpi_siren,),
            )
            _inpi_row = await _inpi_cur.fetchone()
            if _inpi_row:
                _denom = _inpi_row[1] or ""
                _enseigne = _inpi_row[2] or ""
                _adresse = _inpi_row[3] or ""
                _ville = _inpi_row[5] or ""
                log.info(
                    "discovery.inpi_primary_match",
                    maps_name=maps_name,
                    siren=_inpi_siren,
                    denomination=_denom,
                )
                return {
                    "siren": _inpi_siren,
                    "denomination": _denom,
                    "enseigne": _enseigne,
                    "score": 0.92,
                    "method": "inpi",
                    "adresse": _adresse,
                    "ville": _ville,
                }
            else:
                log.debug(
                    "inpi.siren_not_in_local_sirene",
                    siren=_inpi_siren,
                    maps_name=maps_name,
                )
                # Fall through to Steps 1-5
    except Exception:
        pass  # INPI outage — fall through to classic matcher

    normalized = _normalize_name(maps_name)
    search_terms = normalized.split()
    if not search_terms:
        return None

    maps_city_tokens: set[str] = set()
    if maps_address:
        addr_norm = _normalize_name(maps_address)
        maps_city_tokens = {t for t in addr_norm.split() if len(t) > 3}

    meaningful_terms = [
        t for t in search_terms
        if len(t) >= 3
        and t not in _INDUSTRY_WORDS
        and t not in maps_city_tokens
    ]
    if not meaningful_terms:
        meaningful_terms = [t for t in search_terms if len(t) >= 3]

    primary_term = max(meaningful_terms, key=len) if meaningful_terms else ""
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

    # ── Step 1: SIREN from website (validated) ────────────────────────────
    # The SIREN from a company's own website is the strongest possible signal.
    # Validate: same department OR name overlap required (to reject hosting SIRENs).
    if extracted_siren:
        siren_cur = await conn.execute(
            """SELECT siren, denomination, enseigne, adresse, ville, departement
               FROM companies
               WHERE siren = %s AND siren NOT LIKE 'MAPS%%' AND statut = 'A'
               LIMIT 1""",
            (extracted_siren,),
        )
        siren_row = await siren_cur.fetchone()
        if not siren_row:
            log.warning(
                "discovery.siren_website_closed",
                extracted_siren=extracted_siren,
            )
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

    # ── Step 2: Enseigne (trade name) match ──────────────────────────────
    # The enseigne field is the official trade name in SIRENE.
    # If it matches the Maps name well, it's the business — even if the
    # legal address is different (owner registered company elsewhere).
    if departement:
        term_clauses = " OR ".join(
            ["(LOWER(enseigne) LIKE %s OR LOWER(denomination) LIKE %s)"] * len(meaningful_terms)
        )
        ens_params: list[Any] = []
        for t in meaningful_terms:
            ens_params.append(f"%{t}%")   # for enseigne
            ens_params.append(f"%{t}%")   # for denomination
        ens_params.append(departement)
        ens_cur = await conn.execute(
            f"""SELECT siren, siret_siege, denomination, enseigne, naf_code, naf_libelle,
                      forme_juridique, adresse, code_postal, ville, departement,
                      region, statut, date_creation, tranche_effectif,
                      latitude, longitude, fortress_id
               FROM companies
               WHERE ({term_clauses})
                 AND statut = 'A'
                 AND departement = %s
                 AND siren NOT LIKE 'MAPS%%'
               LIMIT 20""",
            ens_params,
        )
        ens_rows = await ens_cur.fetchall()
        if ens_rows:
            best_ens: dict | None = None
            best_ens_score = 0.0
            for row in ens_rows:
                enseigne_val = row[3] or ""
                score = max(
                    _name_match_score(maps_name, enseigne_val),
                    _name_match_score(maps_name, row[2] or ""),
                )
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
                        "code_postal": row[8] or "",
                    }
            if best_ens:
                ens_cp = best_ens.get("code_postal", "")
                postal_match = bool(maps_cp and ens_cp and maps_cp == ens_cp)

                if postal_match:
                    # Same postal code → confirmed
                    log.info("maps_discovery.enseigne_match", maps_name=maps_name,
                             siren=best_ens["siren"], enseigne=best_ens["enseigne"],
                             score=best_ens["score"], maps_cp=maps_cp, enseigne_cp=ens_cp,
                             postal_match=postal_match)
                    best_ens.pop("code_postal", None)
                    return best_ens
                elif maps_cp and ens_cp and maps_cp[:2] == ens_cp[:2] and best_ens_score >= 0.85:
                    # Same department, good score → confirmed
                    log.info("maps_discovery.enseigne_match_nearby", maps_name=maps_name,
                             siren=best_ens["siren"], enseigne=best_ens["enseigne"],
                             score=best_ens["score"], maps_cp=maps_cp, enseigne_cp=ens_cp)
                    best_ens.pop("code_postal", None)
                    return best_ens
                elif maps_cp and ens_cp and maps_cp != ens_cp:
                    # Different postal code + score < 0.85 → downgrade to pending
                    log.warning("maps_discovery.enseigne_match_downgraded", maps_name=maps_name,
                                siren=best_ens["siren"], enseigne=best_ens["enseigne"],
                                score=best_ens["score"], maps_cp=maps_cp, enseigne_cp=ens_cp,
                                reason="postal_mismatch_weak_score")
                    best_ens["method"] = "enseigne_weak"
                    best_ens.pop("code_postal", None)
                    return best_ens
                else:
                    # No postal data available → confirmed (benefit of the doubt)
                    log.info("maps_discovery.enseigne_match_no_postal", maps_name=maps_name,
                             siren=best_ens["siren"], enseigne=best_ens["enseigne"],
                             score=best_ens["score"])
                    best_ens.pop("code_postal", None)
                    return best_ens

    # ── Step 3: Phone match (unique identifier, no postal code) ──────────
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
                """SELECT c.siren, co.denomination, co.enseigne, co.adresse, co.ville, co.code_postal
                   FROM contacts c
                   JOIN companies co ON co.siren = c.siren
                   WHERE c.phone = %s
                     AND c.source != 'google_maps'
                     AND co.siren NOT LIKE 'MAPS%%'
                     AND co.statut = 'A'
                   LIMIT 1""",
                (phone_val,),
            )).fetchone()
            if phone_row:
                # Verify names are compatible — shared phone with unrelated
                # names is likely legacy data or a recycled number.
                sirene_denom = phone_row[1] or ""
                sirene_enseigne = phone_row[2] or ""
                best_score = max(
                    _name_match_score(maps_name, sirene_denom),
                    _name_match_score(maps_name, sirene_enseigne),
                )

                # Industry-generic names need higher threshold
                maps_generic = _is_industry_generic(maps_name)
                sirene_generic = _is_industry_generic(sirene_denom) or _is_industry_generic(sirene_enseigne)
                threshold = 0.80 if (maps_generic or sirene_generic) else 0.30

                if best_score >= threshold:
                    need_city_check = best_score < 0.90

                    if need_city_check:
                        # Mid-range score — verify cities are compatible
                        sirene_ville = phone_row[4] or ""
                        sirene_cp = phone_row[5] or ""
                        city_ok = False
                        city_reason = "no_data"

                        if maps_city_tokens and sirene_ville:
                            sirene_ville_tokens = {t for t in _normalize_name(sirene_ville).split() if len(t) > 3}
                            if sirene_ville_tokens & maps_city_tokens:
                                city_ok = True
                                city_reason = "token_overlap"

                        if not city_ok and maps_cp and sirene_cp:
                            if maps_cp == sirene_cp:
                                city_ok = True
                                city_reason = "postal_code"

                        if not city_ok and (not maps_city_tokens or not sirene_ville):
                            # No city data — benefit of the doubt
                            city_ok = True
                            city_reason = "no_data"

                        log.info(
                            "maps_discovery.phone_city_check",
                            maps_name=maps_name,
                            siren=phone_row[0],
                            maps_cp=maps_cp,
                            sirene_cp=sirene_cp,
                            sirene_ville=sirene_ville,
                            city_match=city_ok,
                            reason=city_reason,
                            name_score=round(best_score, 2),
                        )

                        if not city_ok:
                            # Cities don't match — downgrade to phone_weak
                            log.warning(
                                "maps_discovery.phone_match_city_mismatch",
                                maps_name=maps_name,
                                siren=phone_row[0],
                                sirene_denom=sirene_denom,
                                phone=phone_val,
                                name_score=round(best_score, 2),
                                maps_cp=maps_cp,
                                sirene_ville=sirene_ville,
                            )
                            return {
                                "siren": phone_row[0],
                                "denomination": sirene_denom,
                                "enseigne": sirene_enseigne,
                                "score": round(best_score, 2),
                                "method": "phone_weak",
                                "adresse": phone_row[3] or "",
                                "ville": phone_row[4] or "",
                            }

                    # Score >= 0.90 OR city check passed → confirmed
                    log.info(
                        "maps_discovery.phone_match",
                        maps_name=maps_name,
                        siren=phone_row[0],
                        phone=phone_val,
                        name_score=round(best_score, 2),
                    )
                    return {
                        "siren": phone_row[0],
                        "denomination": sirene_denom,
                        "enseigne": sirene_enseigne,
                        "score": 0.90,
                        "method": "phone",
                        "adresse": phone_row[3] or "",
                        "ville": phone_row[4] or "",
                    }
                else:
                    log.warning(
                        "maps_discovery.phone_match_weak",
                        maps_name=maps_name,
                        siren=phone_row[0],
                        sirene_denom=sirene_denom,
                        phone=phone_val,
                        name_score=round(best_score, 2),
                        threshold=threshold,
                    )
                    return {
                        "siren": phone_row[0],
                        "denomination": sirene_denom,
                        "enseigne": sirene_enseigne,
                        "score": round(best_score, 2),
                        "method": "phone_weak",
                        "adresse": phone_row[3] or "",
                        "ville": phone_row[4] or "",
                    }

    # ── Step 4: Address-first fallback ────────────────────────────────────
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

    # ── Step 4b — Surname extraction (domaine / mas / chateau / cave / vignoble / clos)
    # If Maps label starts with a French agricultural/viniculture prefix, extract
    # the last token as a surname candidate and search SIRENE for it. Requires at
    # least 2 independent signals agree (surname + CP OR ville OR phone OR street).
    # Returns method='surname' which always lands as pending (user decides).
    name_tokens_4b = _normalize_name(maps_name).split()
    if (
        name_tokens_4b
        and name_tokens_4b[0] in _SURNAME_PREFIXES
        and len(name_tokens_4b) >= 2
        and len(name_tokens_4b[-1]) >= 3
        and name_tokens_4b[-1] not in _INDUSTRY_WORDS
    ):
        surname_candidate = name_tokens_4b[-1]
        pattern = f"%{surname_candidate}%"
        surname_cur = await conn.execute(
            """SELECT siren, siret_siege, denomination, enseigne, naf_code, naf_libelle,
                      forme_juridique, adresse, code_postal, ville, departement, region,
                      statut, date_creation, tranche_effectif, latitude, longitude, fortress_id
               FROM companies
               WHERE departement = %s
                 AND statut = 'A'
                 AND siren NOT LIKE 'MAPS%%'
                 AND (LOWER(denomination) LIKE %s OR LOWER(enseigne) LIKE %s)
               LIMIT 20""",
            (departement, pattern, pattern),
        )
        surname_rows = await surname_cur.fetchall()

        best_surname_match = None
        best_surname_signals = 1  # S1 surname always counts; need >= 2 total

        def _norm_phone_local(p: str) -> str:
            """Normalize a French phone number to 0XXXXXXXXX for comparison."""
            p = re.sub(r'[\s\-\.]', '', p or "")
            if p.startswith('+33') and len(p) == 12:
                return '0' + p[3:]
            return p

        for row in surname_rows:
            db_siren = row[0]
            db_cp = row[8] or ""
            db_ville = (row[9] or "").lower()
            db_adresse = (row[7] or "").lower()

            signals_met = []

            # S1 — Surname as a whole token in denomination or enseigne (after normalization)
            db_denom_tokens = _normalize_name(row[2] or "").split()
            db_enseigne_tokens = _normalize_name(row[3] or "").split()
            if surname_candidate in db_denom_tokens or surname_candidate in db_enseigne_tokens:
                signals_met.append("surname")
            else:
                continue  # S1 required; substring alone doesn't count

            # S2 — Postal code exact match
            if maps_cp and db_cp and maps_cp == db_cp:
                signals_met.append("cp")

            # S3 — Ville token overlap (any 4+ char token)
            if maps_city_tokens and db_ville:
                if any(t in db_ville for t in maps_city_tokens if len(t) >= 4):
                    signals_met.append("ville")

            # S4 — Phone match: check contacts table for this SIREN
            if maps_phone:
                phone_cur2 = await conn.execute(
                    "SELECT phone FROM contacts WHERE siren = %s AND phone IS NOT NULL",
                    (db_siren,),
                )
                db_phones = [r[0] for r in await phone_cur2.fetchall()]
                for db_phone in db_phones:
                    if _norm_phone_local(db_phone) == _norm_phone_local(maps_phone):
                        signals_met.append("phone")
                        break

            # S5 — Street-number + street match (reuse maps_street_key from Step 4)
            if maps_street_key and db_adresse and _extract_street_key:
                db_street_key = _extract_street_key(db_adresse)
                if db_street_key and db_street_key == maps_street_key:
                    signals_met.append("street")

            if len(signals_met) >= 2 and len(signals_met) > best_surname_signals:
                best_surname_match = row
                best_surname_signals = len(signals_met)

        if best_surname_match:
            log.info(
                "maps_discovery.surname_match",
                maps_name=maps_name,
                siren=best_surname_match[0],
                denomination=best_surname_match[2],
                surname=surname_candidate,
            )
            return {
                "siren": best_surname_match[0],
                "denomination": best_surname_match[2],
                "enseigne": best_surname_match[3] or "",
                "score": 0.75,
                "method": "surname",
                "adresse": best_surname_match[7] or "",
                "ville": best_surname_match[9] or "",
            }

    # ── Step 5: Name search + scoring (last resort — always pending) ──────
    # Each term needs 2 params: one for enseigne LIKE, one for denomination LIKE
    name_clauses = " OR ".join(
        ["(LOWER(enseigne) LIKE %s OR LOWER(denomination) LIKE %s)"] * len(meaningful_terms)
    )
    conditions = [
        f"({name_clauses})",
        "statut = 'A'",
        "siren NOT LIKE 'MAPS%%'",
    ]
    params: list[Any] = []
    for t in meaningful_terms:
        params.append(f"%{t}%")  # for enseigne
        params.append(f"%{t}%")  # for denomination

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




# ---------------------------------------------------------------------------
# Status update helpers
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


async def _heartbeat_loop(batch_id: str) -> None:
    """Touch updated_at every 60s so the watchdog knows we're alive."""
    conn = None
    try:
        conn = await psycopg.AsyncConnection.connect(
            settings.db_url, autocommit=True, **_KEEPALIVE_PARAMS
        )
        while True:
            try:
                await conn.execute(
                    "UPDATE batch_data SET updated_at = NOW() WHERE batch_id = %s",
                    (batch_id,),
                )
            except Exception:
                # Non-fatal — try to reconnect
                try:
                    await conn.close()
                except Exception:
                    pass
                conn = await psycopg.AsyncConnection.connect(
                    settings.db_url, autocommit=True, **_KEEPALIVE_PARAMS
                )
            await asyncio.sleep(60)
    except asyncio.CancelledError:
        pass
    finally:
        if conn is not None:
            try:
                await conn.close()
            except Exception:
                pass


# ---------------------------------------------------------------------------
# Main pipeline
# ---------------------------------------------------------------------------

async def run(batch_id: str) -> None:
    """Run the Maps-first discovery pipeline for a given batch_id."""
    import traceback as _traceback

    log.info("discovery.start", batch_id=batch_id)

    # Read workspace_id for per-workspace locking
    _ws_conn = await psycopg.AsyncConnection.connect(settings.db_url, autocommit=True, **_KEEPALIVE_PARAMS)
    try:
        _ws_cur = await _ws_conn.execute("SELECT workspace_id FROM batch_data WHERE batch_id = %s", (batch_id,))
        _ws_row = await _ws_cur.fetchone()
        _lock_ws_id = _ws_row[0] if _ws_row and _ws_row[0] is not None else 0
    finally:
        await _ws_conn.close()

    # ── Start Google Maps scraper ─────────────────────────────────────
    maps_scraper = None
    browser_lock_conn = None
    try:
        # Acquire browser lock first — blocks until no other batch has a browser running
        browser_lock_conn = await psycopg.AsyncConnection.connect(
            settings.db_url, autocommit=True, **_KEEPALIVE_PARAMS
        )
        await browser_lock_conn.execute("SELECT pg_advisory_lock(42424243, %s)", (_lock_ws_id,))
        log.info("discovery.browser_lock_acquired", batch_id=batch_id)

        from fortress.scraping.maps import PlaywrightMapsScraper
        maps_scraper = PlaywrightMapsScraper()
        await maps_scraper.start()
        log.info("discovery.browser_started")
    except Exception as exc:
        log.error("discovery.browser_failed", error=str(exc))
        # Release browser lock if it was acquired
        if browser_lock_conn is not None:
            try:
                await browser_lock_conn.execute("SELECT pg_advisory_unlock(42424243, %s)", (_lock_ws_id,))
            except Exception:
                pass
            try:
                await browser_lock_conn.close()
            except Exception:
                pass
            browser_lock_conn = None
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
        lock_conn = None  # Maps advisory lock connection

        # ── Open status connection ────────────────────────────────────
        status_conn = await psycopg.AsyncConnection.connect(
            settings.db_url, autocommit=False, **_KEEPALIVE_PARAMS,
        )
        conn_holder: list[psycopg.AsyncConnection] = [status_conn]

        heartbeat_task = None
        try:
            await _update_job_safe(conn_holder, batch_id, status="in_progress")
            heartbeat_task = asyncio.create_task(_heartbeat_loop(batch_id))

            # ── Load job metadata ─────────────────────────────────────
            cur = await conn_holder[0].execute(
                """SELECT batch_name, search_queries, filters_json, batch_size,
                          workspace_id, completed_queries_count, queries_json
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
            batch_workspace_id: int | None = row[4]
            _completed_queries_count: int = int(row[5] or 0)
            _prior_queries_json = row[6]

            # Parse optional department + NAF filter from filters
            dept_filter = None
            picked_naf = None
            if filters_raw:
                try:
                    filters = json.loads(filters_raw) if isinstance(filters_raw, str) else filters_raw
                    dept_filter = filters.get("department")
                    picked_naf = (filters.get("naf_code") or "").strip() or None
                except Exception:
                    pass

            batch_size = 2000  # Hard server-side ceiling — user-facing size control removed

            # Expand section letter → list of divisions (null if picked_naf is not a single letter)
            naf_division_whitelist: list[str] | None = None
            if picked_naf and len(picked_naf) == 1 and picked_naf.isalpha():
                from fortress.config.naf_codes import NAF_DIVISION_TO_SECTION
                naf_division_whitelist = [
                    div for div, section in NAF_DIVISION_TO_SECTION.items()
                    if section == picked_naf.upper()
                ]

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

            # Safety net: if frontend sent "FR" or empty, try to extract dept from queries
            if (not dept_filter or dept_filter == "FR") and search_queries:
                import re as _re
                import unicodedata as _ud
                from fortress.config.departments import CITY_TO_DEPT, _NAME_TO_CODE, postal_code_to_dept
                def _norm(s: str) -> str:
                    s = _ud.normalize("NFD", s.lower())
                    return _re.sub(r"[\u0300-\u036f]", "", s).strip()
                for _q in search_queries:
                    # Try 2-digit code
                    _m = _re.search(r"\b(\d{2})\b", _q)
                    if _m:
                        dept_filter = _m.group(1)
                        break
                    # Try 5-digit postal
                    _m = _re.search(r"\b(\d{5})\b", _q)
                    if _m:
                        _resolved = postal_code_to_dept(_m.group(1))
                        if _resolved:
                            dept_filter = _resolved
                            break
                    # Try city/department name lookup
                    _nq = _norm(_q)
                    _all_names = {**{k.replace("-", " ").lower(): v for k, v in _NAME_TO_CODE.items()}, **CITY_TO_DEPT}
                    for _name in sorted(_all_names, key=len, reverse=True):
                        if _name in _nq:
                            dept_filter = _all_names[_name]
                            break
                    if dept_filter and dept_filter != "FR":
                        break
                if dept_filter == "FR":
                    dept_filter = None  # Don't use "FR" as a department filter

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
                _query_dedup_count: int = 0
                _query_filtered_count: int = 0
                _current_search_query: str = ""  # Tracks which query is active
                prev_rows: list = []  # Cross-batch dedup results (used in shortfall msg)
                _sector_word: str = ""  # Sector word for relevance filtering
                _query_stats: list[dict] = []  # Tracks per-query results for queries_json

                # Pre-compute sector word for relevance filter (strip dept code from first query)
                if search_queries and dept_filter and re.match(r"^\d{2,3}$", dept_filter):
                    _sw = re.sub(r'\b' + re.escape(dept_filter) + r'\b', '', search_queries[0]).strip()
                    _sw = re.sub(r'\s{2,}', ' ', _sw).strip().rstrip(',').strip()
                    if _sw:
                        _sector_word = _sw

                # ── Resume state rehydration ──────────────────────────────────
                if _prior_queries_json:
                    try:
                        if isinstance(_prior_queries_json, str):
                            _query_stats = list(json.loads(_prior_queries_json))
                        else:
                            _query_stats = list(_prior_queries_json)
                        # Delta 1: drop expansion entries so rerun-expansion doesn't duplicate
                        _query_stats = [s for s in _query_stats if not s.get("is_expansion")]
                    except Exception:
                        _query_stats = []

                if _completed_queries_count > 0:
                    # Single connection for all three SELECTs — do NOT nest pool.connection()
                    async with pool.connection() as conn:
                        # 1. Distinct sirens already discovered + name/address for seen_names seeding
                        cur = await conn.execute(
                            """SELECT DISTINCT bl.siren,
                                      COALESCE(co.denomination, '') AS denom,
                                      COALESCE(co.adresse, '') AS addr
                                 FROM batch_log bl
                                 LEFT JOIN companies co ON co.siren = bl.siren
                                WHERE bl.batch_id = %s
                                  AND bl.action != 'relevance_filter'
                                  AND bl.siren IS NOT NULL""",
                            (batch_id,),
                        )
                        existing_rows = await cur.fetchall()
                        for row in existing_rows:
                            siren, denom, addr = row[0], row[1] or "", row[2] or ""
                            name_key = denom.lower().strip()
                            addr_key = addr.lower().strip()
                            seen_names.add(f"{name_key}|{addr_key}")
                            if siren:
                                seen_sirens.add(str(siren))
                        companies_discovered = len(existing_rows)

                        # 2. Contacts replay — rebuild website/phone dedup sets
                        contacts_cur = await conn.execute(
                            """SELECT DISTINCT ON (bl.siren) bl.siren, c.website, c.phone
                                 FROM batch_log bl
                                 LEFT JOIN contacts c ON c.siren = bl.siren
                                WHERE bl.batch_id = %s
                                  AND bl.action != 'relevance_filter'
                                  AND bl.siren IS NOT NULL
                                ORDER BY bl.siren, c.collected_at DESC NULLS LAST""",
                            (batch_id,),
                        )
                        contact_rows = await contacts_cur.fetchall()
                        for crow in contact_rows:
                            _siren, _website, _phone = crow[0], crow[1], crow[2]
                            if _website:
                                _d = re.sub(r'^https?://(www\.)?', '', _website.strip().lower())
                                _dom = _d.split('/')[0].split('?')[0]
                                if _dom:
                                    seen_websites.add(_dom)
                            if _phone:
                                _p = re.sub(r'\D', '', _phone)
                                if _p:
                                    seen_phones.add(_p)

                        # 3. Qualified count = distinct sirens with phone or website in contacts
                        qr = await conn.execute(
                            """SELECT COUNT(DISTINCT c.siren) FROM contacts c
                                WHERE c.siren IN (
                                    SELECT DISTINCT siren FROM batch_log
                                    WHERE batch_id = %s AND siren IS NOT NULL
                                )
                                AND (c.phone IS NOT NULL OR c.website IS NOT NULL)""",
                            (batch_id,),
                        )
                        qrow = await qr.fetchone()
                        qualified = int(qrow[0] or 0) if qrow else 0

                    log.info(
                        "discovery.resume",
                        batch_id=batch_id,
                        completed_queries=_completed_queries_count,
                        sirens=len(seen_sirens),
                        websites=len(seen_websites),
                        phones=len(seen_phones),
                        qualified=qualified,
                        prior_stats_kept=len(_query_stats),
                    )

                # HTTP client for website crawl — polite delays to avoid anti-bot blocks
                curl_client = CurlClient(timeout=5, delay_min=0.2, delay_max=0.4, max_retries=1)

                # ── Inline persist callback ────────────────────────────
                # This runs for EACH business extracted by search_all,
                # ensuring data is saved to DB immediately (not after
                # all queries finish). If Chrome crashes mid-batch,
                # already-saved businesses are retained.

                async def _persist_result(maps_result: dict[str, Any]) -> bool | None:
                    nonlocal companies_discovered, qualified, _query_dedup_count, _query_filtered_count

                    # Stop collecting once we've reached the user's target
                    if batch_size > 0 and companies_discovered >= batch_size:
                        return False  # Signal scraper to stop extracting cards

                    maps_name = maps_result.get("maps_name", "")
                    maps_address = maps_result.get("address")
                    maps_phone = maps_result.get("phone")
                    maps_website = maps_result.get("website")

                    # ── Level 1: Name+Address dedup ───────────────────────
                    name_key = maps_name.lower().strip()
                    addr_key = (maps_address or "").lower().strip()
                    dedup_key = f"{name_key}|{addr_key}"
                    if dedup_key in seen_names or not name_key:
                        _query_dedup_count += 1
                        return
                    seen_names.add(dedup_key)

                    # ── Level 2: Website domain dedup ─────────────────────
                    website_domain = None
                    if maps_website:
                        _d = re.sub(r'^https?://(www\.)?', '', maps_website.strip().lower())
                        website_domain = _d.split('/')[0].split('?')[0]
                        if website_domain in seen_websites:
                            log.info("discovery.website_dedup_skip", name=maps_name, domain=website_domain)
                            _query_dedup_count += 1
                            return
                        seen_websites.add(website_domain)

                    # ── Relevance filter: skip categories that don't match the sector ──
                    maps_category = maps_result.get("category", "")
                    if maps_category and _sector_word:
                        if is_irrelevant_category(_sector_word, maps_category):
                            log.info(
                                "discovery.relevance_filtered",
                                name=maps_name,
                                category=maps_category,
                                sector=_sector_word,
                            )
                            try:
                                async with pool.connection() as flt_conn:
                                    await log_audit(
                                        flt_conn,
                                        batch_id=batch_id,
                                        siren=f"FILTERED_{maps_name[:20]}",
                                        action="relevance_filter",
                                        result="filtered",
                                        detail=f"Catégorie '{maps_category}' non pertinente pour '{_sector_word}'",
                                        search_query=_current_search_query or None,
                                        workspace_id=batch_workspace_id,
                                    )
                            except Exception:
                                pass
                            _query_filtered_count += 1
                            return

                    # ── Name-based pre-filter: skip obviously wrong businesses ──
                    if _sector_word and maps_name:
                        if is_irrelevant_name(_sector_word, maps_name):
                            log.info(
                                "discovery.name_filtered",
                                name=maps_name,
                                sector=_sector_word,
                            )
                            _query_filtered_count += 1
                            return

                    # ── Level 3: SIREN extraction from website ────────────
                    extracted_siren = None
                    crawl_result = None
                    if maps_website:
                        try:
                            crawl_result = await crawl_website(
                                url=maps_website,
                                client=curl_client,
                                company_name=maps_name,
                                department=dept_filter or "",
                                siren="",
                            )
                            extracted_siren = crawl_result.siren_from_website
                            if extracted_siren:
                                log.info("discovery.siren_extracted", name=maps_name, siren=extracted_siren, website=maps_website)
                        except Exception as exc:
                            log.debug("discovery.siren_extract_failed", website=maps_website, error=str(exc))

                    # ── Level 4: SIREN dedup ──────────────────────────────
                    if extracted_siren:
                        if extracted_siren in seen_sirens:
                            log.info("discovery.siren_dedup_skip", name=maps_name, siren=extracted_siren)
                            _query_dedup_count += 1
                            return
                        seen_sirens.add(extracted_siren)

                    # ── Level 5: Phone dedup ──────────────────────────────
                    if maps_phone:
                        clean_phone = re.sub(r'\D', '', maps_phone)
                        if clean_phone in seen_phones:
                            log.info("discovery.phone_dedup_skip", name=maps_name, phone=maps_phone)
                            _query_dedup_count += 1
                            return
                        seen_phones.add(clean_phone)

                    # ── France country filter ────────────────────────────
                    if not _is_in_france(maps_address):
                        log.info("discovery.foreign_filtered", name=maps_name, address=maps_address)
                        _query_filtered_count += 1
                        return

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
                                await log_audit(
                                    tag_conn,
                                    batch_id=batch_id,
                                    siren=lookup_siren,
                                    action="green",
                                    result="skipped",
                                    detail="Entreprise déjà enrichie — aucune nouvelle extraction nécessaire",
                                    workspace_id=batch_workspace_id,
                                )
                            companies_discovered += 1
                            qualified += 1
                            # Write "completed" immediately if we just hit the target
                            if batch_size > 0 and companies_discovered >= batch_size:
                                try:
                                    await _update_job_safe(
                                        conn_holder, batch_id,
                                        status="completed",
                                        companies_scraped=companies_discovered,
                                        companies_qualified=qualified,
                                        total_companies=companies_discovered,
                                    )
                                except Exception:
                                    pass  # Best effort -- line ~1814 is the safety net
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
                    # Write "completed" immediately if we just hit the target
                    if batch_size > 0 and companies_discovered >= batch_size:
                        try:
                            await _update_job_safe(
                                conn_holder, batch_id,
                                status="completed",
                                companies_scraped=companies_discovered,
                                companies_qualified=qualified,
                                total_companies=companies_discovered,
                            )
                        except Exception:
                            pass  # Best effort -- line ~1814 is the safety net

                    # ALWAYS create a MAPS entity — never use matched SIREN as entity ID
                    async with pool.connection() as id_conn:
                        cur = await id_conn.execute("SELECT nextval('maps_id_seq')")
                        next_id = (await cur.fetchone())[0]
                    siren = f"MAPS{next_id:05d}"
                    # Derive department from Maps address postal code if dept_filter is missing
                    _company_dept = dept_filter
                    if not _company_dept and maps_address:
                        _cp = re.search(r"\b(\d{5})\b", maps_address)
                        if _cp:
                            _company_dept = _cp.group(1)[:2]

                    company = Company(
                        siren=siren,
                        denomination=maps_name,
                        enseigne=maps_name,
                        adresse=maps_address,
                        departement=_company_dept,
                        statut="A",
                        workspace_id=batch_workspace_id,
                    )

                    # Store candidate link metadata
                    # Address match = auto-confirm (high confidence). Name match = pending (user decides).
                    _pending_link: dict | None = None
                    if candidate:
                        _pending_link = candidate
                        if candidate["method"] in ("enseigne", "phone", "address", "siren_website", "inpi"):
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

                    # Strip RGPD-suppressed data from Maps contact before persisting
                    if contact.email and contact.email.lower().strip() in _opposition_emails:
                        contact.email = None
                    if contact.phone and contact.phone.strip() in _opposition_phones:
                        contact.phone = None

                    # Persist to DB immediately
                    async with pool.connection() as conn:
                        await upsert_company(conn, company)
                        await bulk_tag_query(conn, [siren], batch_name, workspace_id=batch_workspace_id, batch_id=batch_id)
                        await upsert_contact(conn, contact)

                        # Default link state: pending. Auto-confirm if method is strong AND NAF verifies against user's picker.
                        if _pending_link:
                            method = _pending_link["method"]
                            target_siren = _pending_link["siren"]

                            # Compute naf_status first so we can decide on auto-confirm
                            naf_status = None
                            if picked_naf:
                                naf_cur = await conn.execute(
                                    "SELECT naf_code FROM companies WHERE siren = %s",
                                    (target_siren,),
                                )
                                sirene_naf_row = await naf_cur.fetchone()
                                matched_naf = (sirene_naf_row[0] if sirene_naf_row else None) or ""
                                naf_status = _compute_naf_status(matched_naf, picked_naf, naf_division_whitelist)

                            auto_confirm = (method in _STRONG_METHODS) and (naf_status == "verified")
                            link_state = "confirmed" if auto_confirm else "pending"

                            await conn.execute(
                                """UPDATE companies
                                   SET linked_siren    = %s,
                                       link_confidence = %s,
                                       link_method     = %s
                                   WHERE siren = %s""",
                                (target_siren, link_state, method, siren),
                            )

                            if naf_status is not None:
                                await conn.execute(
                                    "UPDATE companies SET naf_status = %s WHERE siren = %s",
                                    (naf_status, siren),
                                )

                            if auto_confirm:
                                # Copy SIRENE reference data onto the MAPS row — same semantics
                                # as manual /link approve. Uses shared helper (single source of truth).
                                await _copy_sirene_reference_data(conn, siren, target_siren)
                                await log_audit(
                                    conn,
                                    batch_id=batch_id,
                                    siren=siren,
                                    action="auto_linked_verified",
                                    result="success",
                                    detail=f"Auto-confirmé → {target_siren} (method={method}, naf_status={naf_status})",
                                    workspace_id=batch_workspace_id,
                                )

                        # MAPS-only (no SIRENE candidate): mark naf_status if NAF filter was applied
                        if not _pending_link and picked_naf:
                            await conn.execute(
                                "UPDATE companies SET naf_status = 'maps_only' WHERE siren = %s",
                                (siren,),
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

                    # ── Website crawl: read contacts from CrawlResult ──────
                    if maps_website and crawl_result and crawl_result.pages_crawled > 0:
                        try:
                            from fortress.matching.contacts import extract_mentions_legales
                            from fortress.models import Officer as OfficerModel
                            from fortress.processing.dedup import upsert_officer

                            best_phone = crawl_result.best_phone
                            best_email = crawl_result.best_email
                            # Reject agency emails (web developer's email, not the company's)
                            if best_email and maps_website:
                                from fortress.matching.contacts import is_agency_email
                                if is_agency_email(best_email, maps_website):
                                    best_email = None
                            social = crawl_result.all_socials

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
                                    social_whatsapp=social.get("whatsapp"),
                                    social_youtube=social.get("youtube"),
                                    siren_from_website=extracted_siren,
                                )
                                # Strip RGPD-suppressed data from crawl contact before persisting
                                if crawl_contact.email and crawl_contact.email.lower().strip() in _opposition_emails:
                                    crawl_contact.email = None
                                if crawl_contact.phone and crawl_contact.phone.strip() in _opposition_phones:
                                    crawl_contact.phone = None

                                async with pool.connection() as crawl_conn:
                                    await upsert_contact(crawl_conn, crawl_contact)

                            # Director from mentions-legales
                            ml_html = next(
                                (html for page_url, html in crawl_result.all_html.items() if "mentions" in page_url.lower()),
                                None,
                            )
                            if ml_html:
                                ml_data = extract_mentions_legales(
                                    ml_html,
                                    company_siren=extracted_siren,
                                    website_domain=website_domain,
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
                                    # Check RGPD opposition before upserting officer
                                    _oname = (director.nom or '').lower().strip()
                                    _oprenom = (director.prenom or '').lower().strip()
                                    _oemail = (director.email_direct or '').lower().strip()
                                    if (_oemail and _oemail in _opposition_emails) or \
                                       (_oname, _oprenom) in _opposition_names:
                                        log.info("discovery.rgpd_skip_officer", nom=director.nom, prenom=director.prenom)
                                    else:
                                        async with pool.connection() as officer_conn:
                                            await upsert_officer(officer_conn, director)

                        except Exception as exc:
                            log.warning("discovery.website_crawl_failed", siren=siren, error=str(exc))

                    elif maps_website and crawl_result:
                        log.warning("discovery.crawl_no_pages", siren=siren, website=maps_website)

                    # ── Audit: website crawl result ──
                    if maps_website:
                        if crawl_result and crawl_result.pages_crawled > 0:
                            _crawl_had_data = any([
                                crawl_result.best_email, crawl_result.best_phone,
                                crawl_result.all_socials.get("linkedin"),
                                crawl_result.all_socials.get("facebook"),
                            ])
                            _crawl_status = "success" if _crawl_had_data else "no_data"
                        elif crawl_result:
                            _crawl_status = "no_data"
                        else:
                            _crawl_status = "failed"
                        try:
                            async with pool.connection() as _crawl_log_conn:
                                await log_audit(
                                    _crawl_log_conn,
                                    batch_id=batch_id,
                                    siren=siren,
                                    action="website_crawl",
                                    result=_crawl_status,
                                    source_url=maps_website,
                                    workspace_id=batch_workspace_id,
                                )
                        except Exception:
                            pass
                    else:
                        try:
                            async with pool.connection() as _crawl_log_conn:
                                await log_audit(
                                    _crawl_log_conn,
                                    batch_id=batch_id,
                                    siren=siren,
                                    action="website_crawl",
                                    result="skipped",
                                    detail="Pas de site web trouvé sur Google Maps",
                                    workspace_id=batch_workspace_id,
                                )
                        except Exception:
                            pass

                    # Heartbeat before INPI — website crawl can be slow, keep updated_at fresh
                    await _update_job_safe(
                        conn_holder, batch_id,
                        companies_scraped=companies_discovered,
                        companies_qualified=qualified,
                        total_companies=companies_discovered,
                    )

                    # INPI: for high-confidence matches (address, website SIREN, phone+postal)
                    # Fuzzy name matches wait for user confirmation before INPI call
                    if _pending_link and _pending_link["method"] in ("enseigne", "phone", "address", "siren_website", "inpi"):
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
                                    # Check RGPD opposition before upserting officer
                                    _oname = (officer.nom or '').lower().strip()
                                    _oprenom = (officer.prenom or '').lower().strip()
                                    _oemail = (officer.email_direct or '').lower().strip() if hasattr(officer, 'email_direct') and officer.email_direct else ''
                                    if (_oemail and _oemail in _opposition_emails) or \
                                       (_oname, _oprenom) in _opposition_names:
                                        log.info("discovery.rgpd_skip_officer", nom=officer.nom, prenom=officer.prenom)
                                        continue
                                    await upsert_officer(conn, officer)

                                if dirigeants:
                                    await log_audit(
                                        conn, batch_id=batch_id, siren=siren,
                                        action="officers_found", result="success",
                                        detail=f"{len(dirigeants)} dirigeant(s) — Registre National (API)",
                                        workspace_id=batch_workspace_id,
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
                                        _fin_fields = [k for k in ("chiffre_affaires", "resultat_net", "tranche_effectif") if k in re_company_data]
                                        await log_audit(
                                            conn, batch_id=batch_id, siren=siren,
                                            action="financial_data", result="success",
                                            detail=f"Données financières: {', '.join(_fin_fields)}",
                                            workspace_id=batch_workspace_id,
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

                # ── Acquire advisory lock (one Maps scrape at a time) ─
                try:
                    lock_conn = await psycopg.AsyncConnection.connect(
                        settings.db_url, autocommit=True, **_KEEPALIVE_PARAMS,
                    )
                    await lock_conn.execute("SELECT pg_advisory_lock(42424242, %s)", (_lock_ws_id,))
                    log.info("discovery.advisory_lock_acquired", batch_id=batch_id)
                except Exception as lock_exc:
                    log.error("discovery.advisory_lock_failed", error=str(lock_exc))
                    if lock_conn is not None:
                        try:
                            await lock_conn.close()
                        except Exception:
                            pass
                        lock_conn = None
                    raise RuntimeError(f"Cannot acquire Maps scraping lock: {lock_exc}")

                # ── Cross-batch dedup (after lock — sees batch A's results) ──
                if dept_filter:
                    async with pool.connection() as conn:
                        if batch_workspace_id is not None:
                            cur = await conn.execute(
                                """SELECT LOWER(c.denomination), LOWER(COALESCE(c.adresse, ''))
                                   FROM companies c
                                   WHERE c.siren LIKE 'MAPS%%'
                                   AND c.departement = %s
                                   AND c.workspace_id = %s
                                   AND EXISTS (SELECT 1 FROM batch_tags bt WHERE bt.siren = c.siren)""",
                                (dept_filter, batch_workspace_id),
                            )
                        else:
                            cur = await conn.execute(
                                """SELECT LOWER(c.denomination), LOWER(COALESCE(c.adresse, ''))
                                   FROM companies c
                                   WHERE c.siren LIKE 'MAPS%%'
                                   AND c.departement = %s
                                   AND c.workspace_id IS NULL
                                   AND EXISTS (SELECT 1 FROM batch_tags bt WHERE bt.siren = c.siren)""",
                                (dept_filter,),
                            )
                        prev_rows = await cur.fetchall()
                        for r in prev_rows:
                            seen_names.add(f"{(r[0] or '').strip()}|{(r[1] or '').strip()}")
                    log.info(
                        "discovery.cross_batch_dedup",
                        existing_maps_entities=len(prev_rows),
                        dept=dept_filter,
                        workspace_id=batch_workspace_id,
                    )

                # ── Build pre-dedup name set (for skipping cards before page visit) ──
                import unicodedata as _ud
                _known_names: set[str] = set()
                for entry in seen_names:
                    name_part = entry.split("|")[0].strip()
                    if name_part:
                        _nfkd = _ud.normalize("NFKD", name_part)
                        _known_names.add("".join(c for c in _nfkd if not _ud.combining(c)))

                def _should_skip_card(card_label: str) -> bool:
                    """Return True if this card name is already known in the workspace."""
                    label_lower = card_label.lower().strip()
                    nfkd = _ud.normalize("NFKD", label_lower)
                    clean = "".join(c for c in nfkd if not _ud.combining(c))
                    return clean in _known_names

                log.info(
                    "discovery.pre_dedup_ready",
                    known_names=len(_known_names),
                )

                # ── Cross-batch SIREN dedup (workspace-scoped via linked_siren) ──
                if dept_filter:
                    async with pool.connection() as conn:
                        if batch_workspace_id is not None:
                            cur = await conn.execute(
                                """SELECT c.linked_siren FROM companies c
                                   WHERE c.departement = %s
                                   AND c.siren LIKE 'MAPS%%'
                                   AND c.linked_siren IS NOT NULL
                                   AND c.workspace_id = %s
                                   AND EXISTS (SELECT 1 FROM batch_tags bt WHERE bt.siren = c.siren)""",
                                (dept_filter, batch_workspace_id),
                            )
                        else:
                            cur = await conn.execute(
                                """SELECT c.linked_siren FROM companies c
                                   WHERE c.departement = %s
                                   AND c.siren LIKE 'MAPS%%'
                                   AND c.linked_siren IS NOT NULL
                                   AND c.workspace_id IS NULL
                                   AND EXISTS (SELECT 1 FROM batch_tags bt WHERE bt.siren = c.siren)""",
                                (dept_filter,),
                            )
                        existing_sirens = await cur.fetchall()
                        for r in existing_sirens:
                            seen_sirens.add(r[0])
                    log.info(
                        "discovery.siren_dedup_loaded",
                        existing=len(seen_sirens),
                        dept=dept_filter,
                        workspace_id=batch_workspace_id,
                    )

                # ── RGPD opposition list (preloaded for in-memory checks) ──
                _opposition_emails: set[str] = set()
                _opposition_phones: set[str] = set()
                _opposition_names: set[tuple[str, str]] = set()
                try:
                    async with pool.connection() as opp_conn:
                        cur = await opp_conn.execute(
                            "SELECT email, telephone, nom, prenom FROM rgpd_oppositions"
                        )
                        for opp_row in await cur.fetchall():
                            if opp_row[0]:
                                _opposition_emails.add(opp_row[0].lower().strip())
                            if opp_row[1]:
                                _opposition_phones.add(opp_row[1].strip())
                            if opp_row[2]:
                                _opposition_names.add(
                                    (opp_row[2].lower().strip(), (opp_row[3] or '').lower().strip())
                                )
                    log.info(
                        "discovery.rgpd_loaded",
                        emails=len(_opposition_emails),
                        phones=len(_opposition_phones),
                        names=len(_opposition_names),
                    )
                except Exception as _rgpd_exc:
                    log.warning("discovery.rgpd_load_failed", error=str(_rgpd_exc))

                # ── Maps Discovery (with inline persistence) ──────────
                for q_idx, search_query in enumerate(search_queries, 1):
                    # Resume: skip queries that completed in a prior run
                    if q_idx <= _completed_queries_count:
                        continue
                    # Check for graceful shutdown
                    if _shutdown:
                        log.warning(
                            "discovery.shutdown_before_query",
                            query=search_query,
                            progress=f"{q_idx}/{total_queries}",
                            saved=companies_discovered,
                        )
                        break

                    # Check for admin cancellation request
                    try:
                        cancel_row = await (await conn_holder[0].execute(
                            "SELECT cancel_requested FROM batch_data WHERE batch_id = %s",
                            (batch_id,),
                        )).fetchone()
                        if cancel_row and cancel_row[0]:
                            log.info(
                                "discovery.cancellation_requested",
                                query=search_query,
                                progress=f"{q_idx}/{total_queries}",
                                saved=companies_discovered,
                            )
                            await conn_holder[0].execute(
                                """UPDATE batch_data SET status = 'cancelled',
                                   shortfall_reason = %s,
                                   updated_at = NOW()
                                   WHERE batch_id = %s""",
                                (
                                    f"Annulé par l'administrateur après {companies_discovered} entreprises",
                                    batch_id,
                                ),
                            )
                            await conn_holder[0].commit()
                            return  # Exit cleanly — data already saved
                    except Exception as _cancel_exc:
                        log.debug("discovery.cancel_check_error", error=str(_cancel_exc))

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
                        current_query=search_query,
                        triage_black=triage_counts["black"],
                        triage_green=triage_counts["green"],
                        triage_yellow=triage_counts["yellow"],
                        triage_red=triage_counts["red"],
                    )

                    # Strip department code from query text — viewport already handles geography
                    clean_query = search_query
                    if dept_filter and re.match(r"^\d{2,3}$", dept_filter):
                        clean_query = re.sub(r'\b' + re.escape(dept_filter) + r'\b', '', search_query).strip()
                        clean_query = re.sub(r'\s{2,}', ' ', clean_query).strip().rstrip(',').strip()
                        if clean_query:
                            log.info("discovery.query_cleaned", original=search_query, cleaned=clean_query)
                        else:
                            clean_query = search_query

                    # search_all calls _persist_result for each business
                    _remaining = max(0, batch_size - companies_discovered) if batch_size > 0 else 0
                    _max_cards = _remaining * 3 if _remaining > 0 else 0
                    _query_dedup_count = 0
                    _query_filtered_count = 0
                    _pre_query_discovered = companies_discovered
                    _query_start = time.monotonic()
                    results = await maps_scraper.search_all(
                        clean_query, on_result=_persist_result,
                        dept_code=dept_filter,
                        max_results=_max_cards,
                        sector_word=_sector_word,
                        should_skip=_should_skip_card,
                    )

                    log.info(
                        "discovery.search_done",
                        query=search_query,
                        results=len(results),
                        total_saved=companies_discovered,
                        total_qualified=qualified,
                    )
                    _query_stats.append({
                        "query": search_query,
                        "cards_found": len(results),
                        "new_companies": companies_discovered - _pre_query_discovered,
                        "filtered_count": _query_filtered_count,
                        "dedup_count": _query_dedup_count,
                        "is_expansion": False,
                        "duration_sec": round(time.monotonic() - _query_start, 1),
                    })
                    # ── Record query in memory ──
                    try:
                        async with pool.connection() as qm_conn:
                            await qm_conn.execute(
                                """INSERT INTO query_memory
                                   (workspace_id, sector_word, dept_code, query_text,
                                    is_expansion, cards_found, new_companies, batch_id)
                                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                                (
                                    batch_workspace_id,
                                    _sector_word or "",
                                    dept_filter or "",
                                    search_query,
                                    False,
                                    len(results),
                                    companies_discovered - _pre_query_discovered,
                                    batch_id,
                                ),
                            )
                            await qm_conn.commit()
                    except Exception:
                        pass  # Memory is best-effort

                    # Persist per-query checkpoint (runs for every query, including the last)
                    try:
                        async with pool.connection() as cp_conn:
                            await cp_conn.execute(
                                "UPDATE batch_data SET completed_queries_count = %s, queries_json = %s WHERE batch_id = %s",
                                (q_idx, json.dumps(_query_stats), batch_id),
                            )
                            await cp_conn.commit()
                    except Exception as _e:
                        log.warning("discovery.checkpoint_failed", batch_id=batch_id, q_idx=q_idx, err=str(_e))

                    # Small delay between searches to avoid detection
                    if q_idx < total_queries:
                        await asyncio.sleep(3)

                # Web crawl skipped in Maps-first discovery mode.
                # Maps provides phone + website with 90%+ hit rates.
                # Email enrichment via website crawl can be triggered
                # separately per-company or as a follow-up batch.

                # ── Shortfall detection ────────────────────────────────
                shortfall_msg = None
                if companies_discovered >= 2000:
                    shortfall_msg = (
                        "Limite de sécurité atteinte (2000 entités). "
                        "Affinez vos requêtes pour cibler plus précisément."
                    )
                elif batch_size > 0 and companies_discovered < batch_size:
                    already_known = len(prev_rows) if dept_filter and prev_rows else 0
                    if already_known > 0:
                        shortfall_msg = (
                            f"{qualified} nouvelles entités. "
                            f"{already_known} entreprises de cette zone avaient déjà été découvertes "
                            f"et ont été exclues."
                        )
                    else:
                        shortfall_msg = (
                            f"Google Maps a épuisé les résultats pour cette recherche. "
                            f"{qualified} entités trouvées."
                        )
                    log.info(
                        "discovery.shortfall",
                        found=companies_discovered,
                        target=batch_size,
                        shortfall=batch_size - companies_discovered,
                    )

                # ── Mark completed or interrupted ─────────────────────
                # Check if a cancel was requested while we were finishing up —
                # if so, honour 'cancelled' instead of overwriting with 'completed'.
                try:
                    _cancel_check = await (await conn_holder[0].execute(
                        "SELECT cancel_requested FROM batch_data WHERE batch_id = %s",
                        (batch_id,),
                    )).fetchone()
                    _was_cancelled = bool(_cancel_check and _cancel_check[0])
                except Exception:
                    _was_cancelled = False

                if _was_cancelled:
                    final_status = "cancelled"
                elif _shutdown:
                    final_status = "interrupted"
                else:
                    final_status = "completed"
                _final_written = False
                for _attempt in range(3):
                    try:
                        await _update_job_safe(
                            conn_holder, batch_id,
                            status=final_status,
                            companies_scraped=companies_discovered,
                            companies_qualified=qualified,
                            shortfall_reason=shortfall_msg,
                            current_query=None,
                            queries_json=json.dumps(_query_stats),
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

                # ── Workspace notification on batch complete ──────────────
                if batch_workspace_id is not None and final_status == "completed":
                    try:
                        import os as _os, httpx as _httpx
                        _port = int(_os.environ.get("PORT", "8080"))
                        async with _httpx.AsyncClient(timeout=2.0) as _cli:
                            await _cli.post(
                                f"http://127.0.0.1:{_port}/api/internal/notify-batch-complete",
                                json={
                                    "workspace_id": batch_workspace_id,
                                    "batch_id": batch_id,
                                    "batch_name": batch_name,
                                    "count": companies_discovered,
                                },
                            )
                    except Exception:
                        pass  # best-effort

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
            # Stop heartbeat
            if heartbeat_task is not None:
                heartbeat_task.cancel()
                try:
                    await heartbeat_task
                except (asyncio.CancelledError, Exception):
                    pass
            # Release Maps advisory lock
            if lock_conn is not None:
                try:
                    await lock_conn.execute("SELECT pg_advisory_unlock(42424242, %s)", (_lock_ws_id,))
                    log.info("discovery.advisory_lock_released", batch_id=batch_id)
                except Exception:
                    pass
                try:
                    await lock_conn.close()
                except Exception:
                    pass
                lock_conn = None
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
        # Release browser lock
        if browser_lock_conn is not None:
            try:
                await browser_lock_conn.execute("SELECT pg_advisory_unlock(42424243, %s)", (_lock_ws_id,))
                log.info("discovery.browser_lock_released", batch_id=batch_id)
            except Exception:
                pass
            try:
                await browser_lock_conn.close()
            except Exception:
                pass


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
