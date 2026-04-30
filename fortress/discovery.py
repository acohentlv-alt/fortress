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
import math
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

from fortress.config.departments import DEPT_CITIES
from fortress.config.naf_codes import get_section_for_code
from fortress.config.naf_sector_expansion import SECTOR_EXPANSIONS
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
from fortress.utils.phone import normalize_phone, PHONE_NORMALIZE_SQL
from fortress.utils.timing import time_step, write_timing, batch_id_var, pool_var
from fortress.matching.budget_tracker import BudgetTracker
from fortress.matching import gemini as _gemini_judge

log = structlog.get_logger()

# ── TOP 3 Twin Discovery Widening ────────────────────────────────────────────
# DEPT_CITIES imported from fortress.config.departments above (single source of truth).
# Index 0 = prefecture, skipped in widening (already covered by the primary query).

# Module-level postal-code density cache — populated once per process restart.
_DEPT_POSTAL_CODES_BY_DENSITY: dict[str, list[str]] | None = None


async def _load_dept_postal_codes(pool) -> dict[str, list[str]]:
    """Load top-N postal codes per dept, ranked by company count.

    Cached at module level; only refreshed on process restart.
    Cost: one-time SCAN of companies (~14M rows GROUP BY) ~= 1-2 sec.
    Memory: ~3000 dept-CP combinations x ~30 bytes ~= <100KB.
    """
    global _DEPT_POSTAL_CODES_BY_DENSITY
    if _DEPT_POSTAL_CODES_BY_DENSITY is not None:
        return _DEPT_POSTAL_CODES_BY_DENSITY
    result: dict[str, list[str]] = {}
    async with pool.connection() as conn:
        # Column name is `code_postal` (NOT `postal_code`) per schema.sql:17.
        # `departement` column is dept code as string per schema.sql:19.
        cur = await conn.execute(
            """SELECT departement, code_postal, COUNT(*) AS n
                 FROM companies
                WHERE departement IS NOT NULL
                  AND code_postal IS NOT NULL
                  AND code_postal != ''
             GROUP BY departement, code_postal
             ORDER BY departement, n DESC"""
        )
        rows = await cur.fetchall()
        for dept, cp, _n in rows:
            if not dept or not cp:
                continue
            result.setdefault(str(dept), []).append(str(cp))
    _DEPT_POSTAL_CODES_BY_DENSITY = result
    return result


def _wid_slug(value: str) -> str:
    """Slug for batch_log.siren sentinel: ASCII alnum + dashes, <=30 chars."""
    s = re.sub(r"[^a-zA-Z0-9]+", "-", value).strip("-").upper()
    return s[:30] or "X"


def _parse_dept_hint_from_query(query: str) -> str | None:
    """Extract a dept code hint from a primary query string.

    Returns the dept code (e.g., "47", "66") if the query contains an explicit
    2-digit dept code or a 5-digit postal code (resolved via postal_code_to_dept).
    Returns None if no clear hint is found.

    Used to detect dept-mismatch primaries in batches with mixed-dept queries
    (e.g., a batch locked to dept 51 with a primary "arboriculture 47") so the
    widening logic can skip rather than fire with the wrong dept's cities.
    """
    if not query:
        return None
    # Try 5-digit postal code first (more specific than dept code)
    m = re.search(r"\b(\d{5})\b", query)
    if m:
        try:
            from fortress.config.departments import postal_code_to_dept
            resolved = postal_code_to_dept(m.group(1))
            if resolved:
                return resolved
        except Exception:
            pass
    # Fallback: 2-3 digit dept code (handles 01-95 + 2A/2B + 971-976)
    m = re.search(r"\b(\d{2,3})\b", query)
    if m:
        return m.group(1)
    return None


# ─────────────────────────────────────────────────────────────────────────────

# Depts where postal code MUST match exactly — arrondissements are different neighborhoods.
# V1 scope: Paris only. Dept 69 (Lyon/Villeurbanne) and 13 (Marseille/Aix) deferred
# pending data on edge cases (registered-vs-operational-address mismatches).
_DENSE_URBAN_DEPTS = frozenset({"75"})

# Step 2.5 — CP-restricted name disambiguation thresholds.
# Brief targets taxonomy Section 3.8 (Dense urban vs rural matching).
# Foundation primitive for future Priority 5 (Section 2.B Individual-name code 1000).
_CP_NAME_DISAMB_BAND_A_SIM = 0.90        # Auto-confirm >= this (Phase A still applies)
_CP_NAME_DISAMB_BAND_B_SIM = 0.55        # Pool-safe pending >= this
_CP_NAME_DISAMB_BAND_B_POOL_MAX = 5      # Band B: pool size ceiling
_CP_NAME_DISAMB_BAND_B_DOMINANCE = 0.15  # Band B: top-1 must beat top-2 by this
# Flip to True after 7 days of clean Band B audit (manual decision per Alan).
_BAND_B_AUTO_CONFIRM_ENABLED: bool = False
# TOP 2 — Individual cat_jur 1000 pass-2 threshold (Apr 26).
# Lower than Band A (0.90) because individual enseignes tend to match Maps storefronts
# verbatim, and pass 2 is already heavily gated (agriculture-only, code-1000-only,
# non-empty-enseigne-only, enseigne-scored-only).
_CP_NAME_DISAMB_INDIV_BAND_A_SIM = 0.85


def _compute_naf_status(
    matched_naf: str | None,
    picked_nafs: list[str],
    division_whitelist: list[str] | None,
) -> str:
    """Return 'verified' or 'mismatch' based on SIRENE NAF vs user's picker list.

    Picker is a list of 1..10 leaf/division/section codes (already validated
    same-sector-group upstream). Status is 'verified' if ANY picker resolves
    to verified, 'mismatch' if all miss.

    NOTE: this function does NOT enforce the same-sector-group rule itself.
    That check belongs to the backend validator in batch.py. This function will
    happily verify a status against a cross-sector pair like ['10.71C', '56.10A']
    if asked — by design, so unit tests can exercise edge cases.

    Order of checks per picker (first match wins):
      1. division_whitelist set (only when a single section letter was picked)
          → section-letter broadening
      2. strict prefix match of picker on matched NAF
      3. picker is a leaf in SECTOR_EXPANSIONS and matched NAF is a curated sibling
    """
    candidate = (matched_naf or "")
    if division_whitelist is not None:
        # Section letter case — only ever set when picked_nafs is exactly [section_letter].
        return "verified" if any(candidate.startswith(d) for d in division_whitelist) else "mismatch"
    for picked in picked_nafs:
        if candidate.startswith(picked):
            return "verified"
        expansion = SECTOR_EXPANSIONS.get(picked)
        if expansion is not None and candidate in expansion:
            return "verified"
    return "mismatch"


async def _copy_sirene_reference_data(conn, maps_siren: str, target_siren: str) -> None:
    """Copy SIRENE reference fields from the real SIREN row onto the MAPS entity.

    Populates: siret_siege, naf_code, naf_libelle, forme_juridique,
    code_postal (fill-if-null), ville (fill-if-null), date_creation, tranche_effectif.

    code_postal and ville use COALESCE so the Maps-derived location is preserved
    when already set (Frankenstein fix, Apr 22 — prevents SIRENE siège address
    overwriting the actual storefront location found on Google Maps).

    siret_siege, naf_code, naf_libelle, forme_juridique, date_creation are always
    overwritten from SIRENE — they are legal/identity fields with no Maps equivalent.
    tranche_effectif is fill-if-null (same as before).

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
               code_postal      = COALESCE(code_postal, %s),
               ville            = COALESCE(ville, %s),
               date_creation    = %s,
               tranche_effectif = COALESCE(tranche_effectif, %s)
           WHERE siren = %s""",
        (sirene_row[0], sirene_row[1], sirene_row[2], sirene_row[3],
         sirene_row[4], sirene_row[5], sirene_row[6], sirene_row[7], maps_siren),
    )



async def _verify_signals(
    conn,
    target_siren: str,
    maps_name: str | None,
    maps_phone: str | None,
    maps_address: str | None,
    extracted_siren: str | None,
) -> tuple[dict, str]:
    """Check how many independent signals agree that the MAPS entity matches target_siren.

    Returns a dict with boolean (or None) per signal:
        siren_website_match — True if the SIREN extracted from the website matches target_siren
        phone_match         — True if the phone from SIRENE matches the Maps phone
        address_match       — True if the SIRENE address key matches the Maps address key
        enseigne_match      — True if the SIRENE enseigne/denomination strongly matches the Maps name

    None means the signal could not be computed (missing data).
    """
    from fortress.matching.entities import normalize_address, _extract_street_key

    signals: dict[str, bool | None] = {
        "siren_website_match": None,
        "phone_match": None,
        "address_match": None,
        "enseigne_match": None,
    }

    # Signal 1: SIREN extracted from website
    if extracted_siren:
        signals["siren_website_match"] = (extracted_siren == target_siren)

    # Fetch SIRENE row for phone / address / enseigne comparison
    # Fix B: also fetch denomination — restaurants often have denomination="SARL XXX"
    # while enseigne holds the trade name; we must score against BOTH.
    cur = await conn.execute(
        "SELECT enseigne, adresse, siren, denomination, code_postal FROM companies WHERE siren = %s",
        (target_siren,),
    )
    row = await cur.fetchone()
    sirene_enseigne = (row[0] if row else None) or ""
    sirene_adresse = (row[1] if row else None) or ""
    sirene_denomination = (row[3] if row else None) or ""
    sirene_cp = (row[4] if row else None) or ""

    # Fetch phone from contacts table (best available phone for this SIREN)
    phone_cur = await conn.execute(
        """SELECT phone FROM contacts
           WHERE siren = %s AND phone IS NOT NULL
           ORDER BY collected_at DESC LIMIT 1""",
        (target_siren,),
    )
    phone_row = await phone_cur.fetchone()
    sirene_phone = (phone_row[0] if phone_row else None) or ""

    # Signal 2: Phone match
    if maps_phone and sirene_phone:
        maps_ph_norm = normalize_phone(maps_phone)
        sirene_ph_norm = normalize_phone(sirene_phone)
        if maps_ph_norm and sirene_ph_norm:
            signals["phone_match"] = (maps_ph_norm == sirene_ph_norm)

    # Signal 3: Address match (street key comparison)
    if maps_address and sirene_adresse:
        maps_norm = normalize_address(maps_address)
        sirene_norm = normalize_address(sirene_adresse)
        maps_key = _extract_street_key(maps_norm)
        sirene_key = _extract_street_key(sirene_norm)
        if maps_key and sirene_key and len(maps_key) >= 5:
            signals["address_match"] = (maps_key == sirene_key)

    # Signal 4: Enseigne/name match — reuse existing _name_match_score
    # Fix B: check BOTH enseigne (trade name) and denomination (legal name),
    # take the max. Restaurants especially often have maps_name matching enseigne
    # while denomination is "SARL X Y Z". Without this we miss real matches.
    if maps_name and (sirene_enseigne or sirene_denomination):
        score_enseigne = _name_match_score(maps_name, sirene_enseigne) if sirene_enseigne else 0.0
        score_denom = _name_match_score(maps_name, sirene_denomination) if sirene_denomination else 0.0
        best_score = max(score_enseigne, score_denom)
        signals["enseigne_match"] = (best_score >= 0.8)

    return signals, sirene_cp


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
    # Replace punctuation with space so "PIC&MIE" → "pic mie" (2 tokens),
    # not "picmie" (1 token). Enables validator to see token overlap when
    # one side uses & / - / . without surrounding spaces.
    cleaned = re.sub(r"[^a-z0-9\s]", " ", ascii_name)
    # Normalize French ordinal abbreviations: "29e", "1er", "3ere" → "29eme", "1eme", "3eme"
    # Helps match Maps "29e Coiffure" against SIRENE "29EME RUE COIFFURE".
    cleaned = re.sub(r"(\d+)(?:er|ere|e)(?=\s|$)", r"\1eme", cleaned)
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
    """Compute similarity between two normalized names (0.0 to 1.0).

    Subset rule (score=1.0) requires either:
      - token sets are EQUAL, OR
      - shorter has ≥ 2 meaningful tokens (length ≥ 4) AND shorter ⊂ longer.

    Single-token containment like {"poulet"} ⊂ {"o", "poulet", "grille"}
    does NOT return 1.0 — it falls to Jaccard overlap. This blocks the
    false-positive pattern that dominates dense SIRENE populations (Paris),
    where a 1-token denomination like "POULET" would otherwise match any
    business whose name contains that token.
    """
    if not name_a or not name_b:
        return 0.0
    ta = _normalize_name(name_a).split()
    tb = _normalize_name(name_b).split()
    if not ta or not tb:
        return 0.0
    set_a, set_b = set(ta), set(tb)
    # Equal token sets → 1.0 (exact match after normalization)
    if set_a == set_b:
        return 1.0
    # Proper subset: require the shorter side to carry enough semantic content
    shorter, longer = (set_a, set_b) if len(set_a) <= len(set_b) else (set_b, set_a)
    meaningful_in_shorter = [t for t in shorter if len(t) >= 4]
    if len(meaningful_in_shorter) >= 2 and shorter.issubset(longer):
        return 1.0
    # Jaccard-like overlap on token sets
    overlap = len(set_a & set_b)
    return overlap / max(len(set_a), len(set_b))


def _is_frankenstein_parent_siren(
    maps_name: str,
    sirene_denom: str | None,
    sirene_enseigne: str | None,
) -> bool:
    """Detect the Frankenstein display-bug pattern (Agent C Phase 1 signature).

    Returns True when the matcher legitimately linked the MAPS entity to its
    real parent/holding SIREN, but the SIRENE row's denomination is an unrelated
    shell (e.g. OSG TWO, MERCIERE YG) while SIRENE enseigne holds the real trade
    name. These cases look like "wrong match" to Gemini (addresses don't match
    because SIRENE stores the siège, not the storefront), but the link is
    correct. They must NOT be D1b-quarantined.

    Signature (per Alan's locked decision, Apr 22 — dept dropped):
      - name_overlap(sirene_denom, maps_name) < 0.3   (denom is unrelated shell)
      - enseigne_overlap(sirene_enseigne, maps_name) >= 0.5   (enseigne matches Maps)
      - sirene_enseigne is populated with >= 1 meaningful token (length >= 2) after normalization

    Returns False when enseigne is NULL or has no meaningful tokens — those
    cases fall through to D1b quarantine evaluation (no false protection for
    cases we cannot verify).
    """
    if not maps_name or not sirene_enseigne:
        return False

    # Enseigne must have >=1 meaningful token after normalization.
    enseigne_tokens = [t for t in _normalize_name(sirene_enseigne).split() if len(t) >= 2]
    if not enseigne_tokens:
        return False

    maps_tokens = set(_normalize_name(maps_name).split())
    if not maps_tokens:
        return False

    denom_tokens = set(_normalize_name(sirene_denom or "").split())
    ens_token_set = set(enseigne_tokens)

    # Jaccard overlap (intersection / union) for both denom and enseigne.
    def _jaccard(a: set, b: set) -> float:
        if not a or not b:
            return 0.0
        inter = len(a & b)
        union = len(a | b)
        return inter / union if union else 0.0

    name_overlap = _jaccard(denom_tokens, maps_tokens)
    enseigne_overlap = _jaccard(ens_token_set, maps_tokens)

    return name_overlap < 0.3 and enseigne_overlap >= 0.5


def _validate_inpi_step0_hit(
    maps_cp: str | None,
    departement: str | None,
    meaningful_terms: list[str],
    local_denom: str,
    local_enseigne: str,
    local_cp: str,
    local_dept: str,
) -> bool:
    """Validate an INPI Step 0 hit before accepting it as method='inpi'.

    Accepts when dept check passes AND one of two paths match:
      (1) Whole-token overlap — at least one meaningful Maps token is a
          complete token of SIRENE denomination/enseigne (original behavior).
      (2) Substring-pair path (A1.1, Apr 22) — punctuation-driven splits.
          Fires ONLY when meaningful_terms has exactly 2 tokens (each >=3 chars,
          combined >=6 chars) and BOTH appear as substrings of the same SIRENE
          token (>=6 chars) with coverage >= 0.9. Catches "pic mie" <-> "picmie"
          without opening to generic-word pairs like "bel art" <-> "belartiste"
          (coverage 0.6 -> rejected).
    """
    dept_prefix = (maps_cp or "")[:2]
    strict_postal = dept_prefix in _DENSE_URBAN_DEPTS

    if strict_postal:
        dept_ok = bool(maps_cp and local_cp and maps_cp == local_cp)
    else:
        dept_ok = (not departement) or (not local_dept) or (departement == local_dept)

    if not dept_ok:
        return False

    sirene_tokens = (
        set(_normalize_name(local_denom).split())
        | set(_normalize_name(local_enseigne).split())
    )
    # Path 1: whole-token overlap (original behavior)
    if set(meaningful_terms) & sirene_tokens:
        return True

    # Path 2: substring pair (A1.1, Apr 22)
    if len(meaningful_terms) == 2:
        t1, t2 = meaningful_terms[0], meaningful_terms[1]
        if len(t1) >= 3 and len(t2) >= 3 and (len(t1) + len(t2)) >= 6:
            for st in sirene_tokens:
                if len(st) < 6:
                    continue
                if t1 in st and t2 in st:
                    coverage = (len(t1) + len(t2)) / len(st)
                    if coverage >= 0.9:
                        return True
    return False


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
    "glacier", "patisserie",  # Apr 27 — protège la dernière-token gate de Step 4b contre les FP "Maison X Pâtisserie", "Maison X Glacier".
}


def _naf_section_matches(sirene_naf: str | None, picked_nafs: list[str]) -> bool:
    """True if SIRENE NAF shares an INSEE section letter with ANY picked NAF.

    Used by Giclette/LES TONTONS recovery path — allows auto-confirm when
    enseigne + exact postal match in dense-urban dept AND the sector distance
    is 'soft' (same INSEE section letter, e.g. 56.30Z bar ↔ 56.10A restaurant
    both in section I).
    """
    sirene_section = get_section_for_code(sirene_naf) if sirene_naf else None
    if not sirene_section:
        return False
    return any(get_section_for_code(p) == sirene_section for p in picked_nafs)


# =========================================================================
# Name-tier thresholds — APPLIES ONLY TO STEP 5's inpi_fuzzy_agree GATE.
#
# These thresholds are used SOLELY by the inpi_fuzzy_agree agreement check
# in Step 5. They determine whether a fuzzy_name candidate is strong enough
# to COMBINE with an agreeing INPI SIREN result to earn auto-confirmation.
#
# The existing `_get_match_threshold()` at line 382 continues to serve all
# other weak-match paths (fuzzy_name pending threshold, phone_weak, surname,
# enseigne_weak). It is NOT replaced by this tier system — the two live
# side-by-side by design.
# =========================================================================
NAME_TIER_THRESHOLDS = {
    "person": 0.90,
    "industry_generic": 0.90,
    "short": 0.85,
    "default": 0.75,
}

def get_name_threshold(maps_name: str, name_tokens: list[str], city_match: bool) -> float:
    """Threshold for Step 5's inpi_fuzzy_agree upgrade ONLY."""
    if _is_person_name(maps_name):
        return NAME_TIER_THRESHOLDS["person"]
    if _is_industry_generic(maps_name):
        return NAME_TIER_THRESHOLDS["industry_generic"]
    if len(name_tokens) <= 2 and not city_match:
        return NAME_TIER_THRESHOLDS["short"]
    return NAME_TIER_THRESHOLDS["default"]


async def _fetch_trigram_candidates(
    conn,
    maps_name: str,
    dept: str | None,
    *,
    limit: int = 10,
    min_sim: float = 0.3,
) -> list[dict]:
    """Fetch top-N SIRENE candidates by trigram similarity.

    Uses the canonical ILIKE + similarity() pattern that works with the
    existing GIN trigram indexes at api/main.py:67-75 (gin_trgm_ops).
    Same pattern as discovery.py:766-782 (enseigne step) and
    discovery.py:1160-1172 (fuzzy_name step).

    Post-filters similarity >= min_sim (default 0.3). Returns at most `limit` rows.
    Excludes MAPS%% entities.
    """
    # Extract a 4+-char token from maps_name for the ILIKE prefilter.
    # The token is the longest "word" in the name, length >= 4.
    tokens = [t for t in re.findall(r"[a-zA-Z0-9]{4,}", maps_name) if len(t) >= 4]
    if not tokens:
        # Nothing long enough — fall back to full name with fuzzy similarity only
        like_pattern = f"%{maps_name.lower()}%"
    else:
        # Use the longest token for the LIKE prefilter (most selective)
        token = max(tokens, key=len).lower()
        like_pattern = f"%{token}%"

    base_sql = """
        SELECT siren, denomination, enseigne, adresse, ville, naf_code,
               GREATEST(
                 similarity(COALESCE(enseigne, ''), %s),
                 similarity(COALESCE(denomination, ''), %s)
               ) AS sim_score
          FROM companies
         WHERE (LOWER(COALESCE(enseigne, '')) LIKE %s
                OR LOWER(COALESCE(denomination, '')) LIKE %s)
           AND statut = 'A'
           AND siren NOT LIKE 'MAPS%%'
           {dept_clause}
         ORDER BY sim_score DESC
         LIMIT %s
    """
    params: list = [maps_name, maps_name, like_pattern, like_pattern]
    if dept and re.match(r"^\d{2,3}$", dept):
        sql = base_sql.replace("{dept_clause}", "AND departement = %s")
        params.append(dept)
    else:
        sql = base_sql.replace("{dept_clause}", "")
    params.append(limit * 3)  # Fetch 3x limit; Python post-filter will cut to `limit`

    cur = await conn.execute(sql, params)
    rows = await cur.fetchall()

    candidates: list[dict] = []
    for r in rows:
        sim = float(r[6])
        if sim < min_sim:
            continue
        candidates.append({
            "siren": r[0],
            "denomination": r[1] or "",
            "enseigne": r[2] or "",
            "adresse": r[3] or "",
            "ville": r[4] or "",
            "naf_code": r[5] or "",
            "method": "trigram_pool",
            "score": round(sim, 3),
        })
        if len(candidates) >= limit:
            break
    return candidates


# Match methods strong enough for pipeline auto-confirm (with verified NAF).
# Weak variants (enseigne_weak, phone_weak, surname, fuzzy_name) never auto-confirm.
# inpi_fuzzy_agree = fuzzy_name candidate confirmed by agreeing INPI SIREN result.
_STRONG_METHODS = frozenset({
    "inpi", "siren_website", "enseigne", "phone", "address",
    "inpi_fuzzy_agree",
    "inpi_mentions_legales",  # Lever A2
    "chain",  # Agent B — franchise/chain detector
    "gemini_judge",  # D1b rescue (April 22) — Gemini upgraded a weak/maps_only candidate
    "cp_name_disamb",  # P3 Step 2.5 — CP-restricted NAF-filtered name disambiguation
    "cp_name_disamb_indiv",  # TOP 2 — code 1000 agriculture pass 2 (Apr 26)
    "geo_proximity",  # Phase 2 (TOP 1) — 400m bounding-box proximity matcher
    "commune_municipal",  # Apr 29 — municipal-service pseudo-chain (public FJ + maps_cp + service NAF)
    "siret_address_naf",  # Track 2 — SIRET-level CP+NAF lookup (communes, multi-site SCIs, etc.)
})

# Weak match methods — A2 mentions-légales rescue is allowed to replace these.
# A2 fires when candidate is None OR candidate.method ∈ _A2_WEAK_ELIGIBLE.
# Keep this set conservative — only methods that are explicitly weak (never
# auto-confirm) belong here. Do NOT add strong methods even if they got
# `link_confidence='pending'` via the NAF gate — those are strong-weak pending
# and A2 must not override them.
_A2_WEAK_ELIGIBLE = frozenset({
    "fuzzy_name",
    "surname",
    "enseigne_weak",
    "phone_weak",
})

# ── Phase 2 — Geo proximity matcher (Step 2.6) ────────────────────────────
# 400m radius (single global default — no per-dept variation in v1).
# Lat/lng deltas at French latitudes:
#   1° lat  ≈ 111km                  -> 400m ≈ 0.0036°
#   1° lng  ≈ 111km × cos(46°N)≈77km -> 400m ≈ 0.0050°
# Across France's lat range (42°N–51°N) the lng delta of 0.0050°
# corresponds to roughly 349m–419m of east-west distance —
# acceptable variance for v1; can be made dynamic later.
_GEO_RADIUS_M = 400
_GEO_LAT_DELTA = 0.0036
_GEO_LNG_DELTA = 0.0050
# Default auto-confirm thresholds (Phase 2 v1).
# Phase 3 will OVERRIDE these via kwargs when sweeping
# ban_backfilled historical entities (tighter: 0.95 / 0.20).
_GEO_CONFIRM_TOP_SCORE = 0.85
_GEO_CONFIRM_DOMINANCE = 0.15
# INSEE geocode_quality enum: '11' exact, '12' street-interpolated,
# '21'/'22' "voie probable" (geocode itself is up to ~200m fuzzy).
# For loose-quality candidates we tighten the name-similarity bar to
# 0.95 — the spatial side is already noisy, so the name side has to
# carry more of the burden. Quality '11'/'12'/NULL keep the default.
_GEO_LOOSE_QUALITIES = frozenset({"21", "22"})
_GEO_LOOSE_QUALITY_TOP_SCORE = 0.95


def _haversine_m(lat1: float, lng1: float, lat2: float, lng2: float) -> float:
    """Great-circle distance in metres between two WGS84 points."""
    R = 6_371_000.0
    dlat = math.radians(lat2 - lat1)
    dlng = math.radians(lng2 - lng1)
    a = (
        math.sin(dlat / 2) ** 2
        + math.cos(math.radians(lat1)) * math.cos(math.radians(lat2))
        * math.sin(dlng / 2) ** 2
    )
    return 2 * R * math.asin(math.sqrt(a))


_LEGAL_FORM_TOKENS = frozenset({
    "earl", "gaec", "scea", "scev", "sci", "sarl", "sas",
    "sasu", "eurl", "sa", "snc", "eirl", "ei",
})

_SURNAME_PREFIXES = frozenset({
    "bastide", "bergerie", "cave", "chateau", "clos", "domaine", "ferme",
    "maison", "manoir", "mas", "moulin", "verger", "villa", "vignoble",
    # "bergerie" — élevage ovin/caprin (ws174 dept 66) ; "verger" — arboriculture fruitière (ws174 30j)
})

# Tokens qui, présents N'IMPORTE OÙ dans le nom Maps, bloquent l'extraction Step 4b.
# Plus large que _INDUSTRY_WORDS (qui ne protège que la dernière-token gate).
# Cible : EHPAD avec dernière-token non-industriel (ex. "Maison de Retraite Publique"
# où dernière-token = "publique" passe la gate _INDUSTRY_WORDS).
_HARD_REJECT_TOKENS = frozenset({
    "retraite",  # "Maison de Retraite [X]" — EHPAD, pas un nom de famille
    "ehpad",     # explicite EHPAD
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

# Storefront-descriptor fallback for associations/fondations.
# Pattern: "Association/Asso/Fondation <legal-name> - <site descriptor>"
# (e.g., "Association Vivre le 3ème âge - EHPAD Jean Rostand"). Used by the
# Step 0 INPI fallback to retry with the prefix-only name when the full search
# misses. Gated narrowly to associations/fondations — generic " - " fallback
# would catch too many false-positive patterns (lawyer names, branch suffixes).
_ASSOC_PREFIX_RE = re.compile(r"^\s*(association|asso|fondation)\b", re.IGNORECASE)


def _parse_maps_address(addr: str | None) -> tuple[str | None, str | None]:
    """Extract (code_postal, ville) from a Google Maps address string.

    Strategy:
      - Strip trailing ", France" (99.8% of real Maps addresses have it)
      - Take the LAST 5-digit match, not the first (streets may start with 5 digits,
        e.g. "63200 Chem. des Coteaux, 63200 Riom, France" — last is the real CP)
      - Ville is the text AFTER the last CP, stripped of whitespace/punctuation

    Returns (None, None) gracefully when no CP present.
    Cedex suffix is PRESERVED in ville (matches SIRENE libelle_commune convention).
    """
    if not addr:
        return None, None
    _addr_clean = re.sub(r",\s*France\s*$", "", addr.strip(), flags=re.IGNORECASE)
    _cp_matches = list(re.finditer(r"\b(\d{5})\b", _addr_clean))
    if not _cp_matches:
        return None, None
    _last = _cp_matches[-1]
    _cp = _last.group(1)
    _tail = _addr_clean[_last.end():].strip(" ,\t")
    _ville = _tail or None
    return _cp, _ville


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


async def _cp_name_disamb_match(
    conn: Any,
    maps_name: str,
    maps_cp: str | None,
    picked_nafs: list[str],
    naf_division_whitelist: list[str] | None,
) -> dict | None:
    """Step 2.5: CP-restricted NAF-filtered name disambiguation.

    Returns a candidate dict compatible with Step 2/3/4/5 return shape.
    Band A (sim >= 0.90) -> method='cp_name_disamb', Phase A-eligible auto-confirm.
    Band B (sim >= 0.55 + pool/dominance guards) -> method='cp_name_disamb' with
       cp_name_disamb_band='B' marker consumed by the auto-confirm gate to force
       pending while _BAND_B_AUTO_CONFIRM_ENABLED=False.

    Early-outs (return None):
      - maps_cp missing
      - picked_nafs empty (no NAF filter = no disambiguation, Nansouty risk)
      - no NAF-filtered candidates at CP
      - Band B fails pool/dominance guards

    Designed as reusable primitive for future Priority 5 (taxonomy line 1026-1034).
    """
    if not maps_cp or not picked_nafs:
        return None

    # Honor section-letter-only whitelist (when naf_division_whitelist is populated).
    naf_prefixes = naf_division_whitelist if naf_division_whitelist else picked_nafs
    if not naf_prefixes:
        return None

    naf_like_clauses = " OR ".join(["naf_code LIKE %s"] * len(naf_prefixes))
    naf_params = [f"{p}%" for p in naf_prefixes]

    # SIRENE read-only guard: siren NOT LIKE 'MAPS%'
    cur = await conn.execute(
        f"""SELECT siren, siret_siege, denomination, enseigne, naf_code, naf_libelle,
                   forme_juridique, adresse, code_postal, ville, departement,
                   region, statut, date_creation, tranche_effectif,
                   latitude, longitude, fortress_id,
                   GREATEST(
                     similarity(COALESCE(enseigne,''), %s),
                     similarity(COALESCE(denomination,''), %s)
                   ) AS sim
            FROM companies
            WHERE code_postal = %s
              AND statut = 'A'
              AND siren NOT LIKE 'MAPS%%'
              AND ({naf_like_clauses})
            ORDER BY sim DESC
            LIMIT 100""",  # noqa: S608
        [maps_name, maps_name, maps_cp] + naf_params,
    )
    rows = await cur.fetchall()

    # Pass 1 scoring (always computed, used to decide if pass 2 fires).
    pool_size = len(rows)
    top_sim = float(rows[0][18] or 0.0) if rows else 0.0
    second_sim = float(rows[1][18] or 0.0) if pool_size >= 2 else 0.0

    # Detect Band A immediately so the pass-2 trigger has a single source of truth:
    # "did pass 1 land an auto-confirmable Band A candidate?"
    pass1_has_band_a = pool_size > 0 and top_sim >= _CP_NAME_DISAMB_BAND_A_SIM

    # ── Pass 2: Individual cat_jur 1000 fallback (agriculture only) ──
    # Fires whenever pass 1 fails to identify a Band A candidate AND the picker is
    # agriculture (every active NAF prefix starts with '01.').
    # Includes the empty-rows case (pass 1 found nothing in NAF+CP) and the more
    # common case (pass 1 found rows but the GREATEST(enseigne, denomination) scoring
    # topped out below 0.90 because EARL/SCEA `denomination` rows in the same pool
    # washed out the individuals' `enseigne` similarity).
    # Pass 2 re-scores the same pool subset (filtered to forme_juridique='1000' +
    # non-empty enseigne) using `similarity(enseigne, maps_name)` ONLY — the trade
    # name `enseigne` is the load-bearing field for individual entrepreneurs.
    if not pass1_has_band_a and all(p.startswith("01.") for p in naf_prefixes):
        cur2 = await conn.execute(
            f"""SELECT siren, siret_siege, denomination, enseigne, naf_code, naf_libelle,
                       forme_juridique, adresse, code_postal, ville, departement,
                       region, statut, date_creation, tranche_effectif,
                       latitude, longitude, fortress_id,
                       similarity(COALESCE(enseigne,''), %s) AS sim_enseigne
                FROM companies
                WHERE code_postal = %s
                  AND statut = 'A'
                  AND siren NOT LIKE 'MAPS%%'
                  AND ({naf_like_clauses})
                  AND forme_juridique = '1000'
                  AND enseigne IS NOT NULL
                  AND enseigne <> ''
                ORDER BY sim_enseigne DESC
                LIMIT 100""",  # noqa: S608
            [maps_name, maps_cp] + naf_params,
        )
        rows2 = await cur2.fetchall()
        if rows2:
            top2 = rows2[0]
            top2_sim = float(top2[18] or 0.0)
            if top2_sim >= _CP_NAME_DISAMB_INDIV_BAND_A_SIM:
                log.info(
                    "discovery.cp_name_disamb_indiv_match",
                    maps_name=maps_name, maps_cp=maps_cp,
                    band="A_indiv",
                    siren=top2[0],
                    denomination=top2[2], enseigne=top2[3],
                    matched_naf=top2[4],
                    top_sim_enseigne=round(top2_sim, 3),
                    pool_size=len(rows2),
                    pass1_pool_size=pool_size,
                    pass1_top_sim=round(top_sim, 3),
                    naf_strict_prefix_matched=True,
                    candidates_considered=len(rows2),
                )
                return {
                    "siren": top2[0],
                    "denomination": top2[2] or "",
                    "enseigne": top2[3] or "",
                    "score": round(top2_sim, 2),
                    "method": "cp_name_disamb_indiv",
                    "adresse": top2[7] or "",
                    "ville": top2[9] or "",
                    "cp_name_disamb_meta": {
                        "top_sim_enseigne": round(top2_sim, 3),
                        "pool_size": len(rows2),
                        "candidates_considered": len(rows2),
                        "naf_strict_prefix_matched": True,
                        "forme_juridique_filter": "1000",
                        "pass": 2,
                        "pass1_pool_size": pool_size,
                        "pass1_top_sim": round(top_sim, 3),
                    },
                }
            else:
                log.info(
                    "discovery.cp_name_disamb_indiv_no_band",
                    maps_name=maps_name, maps_cp=maps_cp,
                    top_sim_enseigne=round(top2_sim, 3),
                    pool_size=len(rows2),
                    pass1_pool_size=pool_size,
                    pass1_top_sim=round(top_sim, 3),
                )
                # fall through to pass 1 band detection below
        # if rows2 is empty, also fall through to pass 1 band detection

    # ── Pass 1 band detection (unchanged from pre-Apr-26) ──
    if not rows:
        return None

    top_row = rows[0]

    band: str | None = None
    if top_sim >= _CP_NAME_DISAMB_BAND_A_SIM:
        band = "A"
    elif (
        top_sim >= _CP_NAME_DISAMB_BAND_B_SIM
        and pool_size <= _CP_NAME_DISAMB_BAND_B_POOL_MAX
        and (top_sim - second_sim) >= _CP_NAME_DISAMB_BAND_B_DOMINANCE
    ):
        band = "B"

    if band is None:
        log.info(
            "discovery.cp_name_disamb_no_band",
            maps_name=maps_name, maps_cp=maps_cp,
            top_sim=round(top_sim, 3), second_sim=round(second_sim, 3),
            pool_size=pool_size,
        )
        return None

    log.info(
        "discovery.cp_name_disamb_match",
        maps_name=maps_name, maps_cp=maps_cp,
        band=band,
        siren=top_row[0],
        denomination=top_row[2], enseigne=top_row[3],
        matched_naf=top_row[4],
        top_sim=round(top_sim, 3), second_sim=round(second_sim, 3),
        pool_size=pool_size,
        naf_strict_prefix_matched=True,
        candidates_considered=pool_size,
    )
    return {
        "siren": top_row[0],
        "denomination": top_row[2] or "",
        "enseigne": top_row[3] or "",
        "score": round(top_sim, 2),
        "method": "cp_name_disamb",
        "adresse": top_row[7] or "",
        "ville": top_row[9] or "",
        "cp_name_disamb_band": band,
        "cp_name_disamb_meta": {
            "top_sim": round(top_sim, 3),
            "second_sim": round(second_sim, 3),
            "pool_size": pool_size,
            "candidates_considered": pool_size,
            "naf_strict_prefix_matched": True,
        },
    }


async def _is_franchise_live(
    conn: Any,
    proposed_siren: str,
    current_maps_cp: str | None,
) -> tuple[bool, int]:
    """Phase 2 live check: reject siren_website match when SIREN is already
    confirmed for a different MAPS entity at a DIFFERENT code_postal.

    Returns (should_reject: bool, conflicting_count: int).

    Logic:
    - Queries the companies table for MAPS entities that already have
      linked_siren == proposed_siren AND link_confidence == 'confirmed'
      AND code_postal != current_maps_cp.
    - If any such row exists, this is a franchise HQ leak (same parent SIREN
      confirmed for multiple local sites in different postal codes).
    - Same-CP case is NOT a leak (same business, duplicate entry handled by
      existing dedup) — deliberately excluded from the reject logic.
    - If current_maps_cp is None we cannot safely compare → do not reject.
    """
    if not current_maps_cp:
        return False, 0
    try:
        cur = await conn.execute(
            """SELECT COUNT(*) FROM companies
               WHERE linked_siren = %s
                 AND link_confidence = 'confirmed'
                 AND code_postal != %s
                 AND siren LIKE 'MAPS%%'""",
            (proposed_siren, current_maps_cp),
        )
        row = await cur.fetchone()
        count = int(row[0]) if row else 0
        return count > 0, count
    except Exception as exc:
        log.debug("discovery.franchise_live_check_error", error=str(exc))
        return False, 0


async def _geo_proximity_match(
    conn: Any,
    maps_name: str,
    maps_lat: float,
    maps_lng: float,
    picked_nafs: list[str] | None,
    naf_division_whitelist: list[str] | None,
    *,
    top_threshold: float = _GEO_CONFIRM_TOP_SCORE,
    dominance_threshold: float = _GEO_CONFIRM_DOMINANCE,
) -> dict | None:
    """Step 2.6: 400m proximity-restricted name disambiguation.

    Bounding box derived from _GEO_LAT_DELTA / _GEO_LNG_DELTA.
    Joins companies_geom (sirene_geo + ban_backfill rows) to
    companies for name/enseigne/naf/cp.

    Threshold kwargs (top_threshold / dominance_threshold):
      - Default 0.85 / 0.15 = Phase 2 v1 (Maps panel coords,
        relatively trustworthy).
      - Phase 3 callers override with 0.95 / 0.20 for BAN-
        geocoded historical entities (less trustworthy).

    Returns a candidate dict matching Step 2/3/4/5 shape:
      {"siren": str, "score": float, "method": "geo_proximity",
       "geo_proximity_distance_m": int,
       "geo_proximity_top_score": float,
       "geo_proximity_2nd_score": float,
       "geo_proximity_pool_size": int}
    or None when:
      - bounding box is empty (no candidates within 400m)
      - top score < top_threshold
      - top score - 2nd score < dominance_threshold
        (forces ambiguous pools to fail this step rather than
         produce a low-confidence pending — better to let later
         steps try.)

    NAF filtering: this helper does NOT pre-filter by picked_nafs
    inside the SQL. The bounding box is the discriminator.
    naf_status is computed in the caller (_match_to_sirene's
    shared decision block) using the candidate's matched_naf vs picker.
    """
    lat_min = maps_lat - _GEO_LAT_DELTA
    lat_max = maps_lat + _GEO_LAT_DELTA
    lng_min = maps_lng - _GEO_LNG_DELTA
    lng_max = maps_lng + _GEO_LNG_DELTA

    cur = await conn.execute(
        """
        SELECT cg.siren,
               cg.lat, cg.lng, cg.geocode_quality,
               co.denomination, co.enseigne, co.naf_code,
               co.code_postal, co.adresse, co.ville, co.statut
        FROM companies_geom cg
        JOIN companies co ON co.siren = cg.siren
        WHERE cg.source IN ('sirene_geo', 'ban_backfill')
          AND cg.lat BETWEEN %s AND %s
          AND cg.lng BETWEEN %s AND %s
          AND co.statut = 'A'
          AND co.siren NOT LIKE 'MAPS%%'
        """,
        (lat_min, lat_max, lng_min, lng_max),
    )
    rows = await cur.fetchall()

    if not rows:
        return None

    scored = []
    for row in rows:
        siren_, lat_, lng_, quality, denom, enseigne_, naf_, cp_, addr_, ville_, _statut = row
        best_name = max(
            _name_match_score(maps_name, denom or ""),
            _name_match_score(maps_name, enseigne_ or ""),
        )
        dist_m = _haversine_m(maps_lat, maps_lng, float(lat_), float(lng_))
        scored.append({
            "siren": siren_, "score": best_name, "dist_m": int(dist_m),
            "denom": denom, "enseigne": enseigne_, "naf": naf_, "cp": cp_,
            "addr": addr_, "ville": ville_,
            "quality": quality,
        })

    scored.sort(key=lambda r: r["score"], reverse=True)
    top = scored[0]
    second_score = scored[1]["score"] if len(scored) >= 2 else 0.0

    # Tighten the top-score bar when the matched coord has loose INSEE
    # quality ('21'/'22' = "voie probable"). Caller-provided override
    # (Phase 3 BAN backfill) wins over the loose-quality bump only when
    # the override is already stricter.
    effective_top = top_threshold
    top_quality = top.get("quality")
    if top_quality in _GEO_LOOSE_QUALITIES:
        effective_top = max(effective_top, _GEO_LOOSE_QUALITY_TOP_SCORE)

    if top["score"] < effective_top:
        return None
    if (top["score"] - second_score) < dominance_threshold:
        return None

    return {
        "siren": top["siren"],
        "score": top["score"],
        "method": "geo_proximity",
        "denomination": top.get("denom") or "",
        "enseigne": top.get("enseigne") or "",
        "adresse": top.get("addr") or "",
        "ville": top.get("ville") or "",
        "naf_code": top.get("naf"),
        "geo_proximity_distance_m": top["dist_m"],
        "geo_proximity_top_score": top["score"],
        "geo_proximity_2nd_score": second_score,
        "geo_proximity_pool_size": len(scored),
        "geo_proximity_quality": top_quality,
    }


async def _geo_proximity_top_n(
    conn,
    maps_name: str,
    maps_lat: float | None,
    maps_lng: float | None,
    *,
    k: int = 3,
    exclude_siren: str | None = None,
) -> list[dict]:
    """Return up to k SIRENE candidates in the geo bounding box, sorted by name score.

    Unlike _geo_proximity_match, applies NO threshold or dominance gate —
    the caller (_gather_alternatives) decides how to use these candidates.
    Used to expand the Gemini multi-candidate prompt with geo-local alternatives.
    """
    if maps_lat is None or maps_lng is None:
        return []

    lat_min = maps_lat - _GEO_LAT_DELTA
    lat_max = maps_lat + _GEO_LAT_DELTA
    lng_min = maps_lng - _GEO_LNG_DELTA
    lng_max = maps_lng + _GEO_LNG_DELTA

    cur = await conn.execute(
        """
        SELECT cg.siren,
               co.denomination, co.enseigne, co.adresse, co.ville, co.naf_code
        FROM companies_geom cg
        JOIN companies co ON co.siren = cg.siren
        WHERE cg.source IN ('sirene_geo', 'ban_backfill')
          AND cg.lat BETWEEN %s AND %s
          AND cg.lng BETWEEN %s AND %s
          AND co.statut = 'A'
          AND co.siren NOT LIKE 'MAPS%%'
        """,
        (lat_min, lat_max, lng_min, lng_max),
    )
    rows = await cur.fetchall()

    if not rows:
        return []

    scored = []
    for row in rows:
        siren_, denom, enseigne_, adresse_, ville_, naf_ = row
        name_score = max(
            _name_match_score(maps_name, denom or ""),
            _name_match_score(maps_name, enseigne_ or ""),
        )
        scored.append({
            "siren": siren_,
            "score": name_score,
            "method": "geo_proximity_alt",
            "denomination": denom or "",
            "enseigne": enseigne_ or "",
            "adresse": adresse_ or "",
            "ville": ville_ or "",
            "naf_code": naf_,
        })

    scored.sort(key=lambda r: r["score"], reverse=True)

    # Filter out exclude_siren before capping at k
    result = [r for r in scored if r["siren"] != exclude_siren]
    return result[:k]


async def _gather_alternatives(
    conn,
    maps_name: str,
    maps_lat: float | None,
    maps_lng: float | None,
    dept_filter: str | None,
    *,
    exclude_siren: str,
    k: int = 3,
) -> list[dict]:
    """Gather up to k alternative SIRENE candidates from trigram + geo sources.

    Used to build the multi-candidate prompt for the Gemini swap path.
    Returns [] on any internal failure (must never crash the pipeline).
    """
    try:
        # Trigram candidates (department-filtered)
        trigram_cands = await _fetch_trigram_candidates(conn, maps_name, dept_filter, limit=k)

        # Geo proximity candidates (only when coordinates are available)
        if maps_lat is not None and maps_lng is not None:
            geo_cands = await _geo_proximity_top_n(
                conn, maps_name, maps_lat, maps_lng, k=k, exclude_siren=exclude_siren
            )
        else:
            geo_cands = []

        # Merge: trigram first, then geo; dedup by siren; drop exclude_siren; cap at k
        seen: set[str] = set()
        merged: list[dict] = []
        for cand in trigram_cands + geo_cands:
            siren_ = cand["siren"]
            if siren_ == exclude_siren:
                continue
            if siren_ in seen:
                continue
            seen.add(siren_)
            merged.append(cand)
            if len(merged) >= k:
                break

        return merged
    except Exception:
        log.warning("discovery.gather_alternatives_failed", maps_name=maps_name)
        return []


async def _match_to_sirene(
    conn: Any,
    maps_name: str,
    maps_address: str | None,
    departement: str | None,
    maps_phone: str | None = None,
    extracted_siren: str | None = None,
    rejected_siren_sink: dict[str, str] | None = None,
    *,
    picked_nafs: list[str] | None = None,               # NEW — Step 2.5 CP name disamb
    naf_division_whitelist: list[str] | None = None,    # NEW — Step 2.5 CP name disamb
    maps_cp: str | None = None,                         # NEW — Step 2.5 CP name disamb
    maps_lat: float | None = None,                      # NEW — Phase 2 geo proximity
    maps_lng: float | None = None,                      # NEW — Phase 2 geo proximity
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

    # INPI result cache — populated by Step 0 (primary) or Step 5 (fallback).
    # Shape: maps_name -> (siren, naf, nom, cp) tuple or None (miss).
    _inpi_cache: dict[str, tuple | None] = {}

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

    # ── Step 0: INPI primary matcher (Recherche Entreprises API) ──────────
    # Fires only when it has a chance of a useful result:
    #   - No website-footer SIREN (Step 1 will handle that — deterministic)
    #   - At least one meaningful search term (name isn't all industry/city words)
    #   - Have SOME location context (dept or CP)
    # On miss or validation fail: falls through to Steps 1-5 unchanged.
    # Populates `_inpi_cache` so Step 5 inpi_fuzzy_agree arbitration can reuse it.
    # Guard: require dept, OR dense-urban CP (where CP is kept as location filter).
    # Prevents firing a global INPI name-search when dept is unknown and CP is stripped.
    _step0_dept_prefix = (maps_cp or "")[:2]
    _inpi_step0_fires = (
        not extracted_siren
        and meaningful_terms
        and (departement or (maps_cp and _step0_dept_prefix in _DENSE_URBAN_DEPTS))
    )
    _timing_batch_id = batch_id_var.get()
    if _inpi_step0_fires:
        try:
            from fortress.matching.inpi import search_by_name as _inpi_search
            # Pass CP only for dense-urban depts (arrondissements are distinct neighborhoods).
            # For other depts, CP would over-narrow — SIREN siège can be at any CP in dept.
            # Aligns with _validate_inpi_step0_hit's strict_postal logic.
            _inpi_cp = maps_cp if _step0_dept_prefix in _DENSE_URBAN_DEPTS else None
            if _timing_batch_id is not None:
                async with time_step(conn, _timing_batch_id, None, "inpi_step0"):
                    _inpi_hit = await _inpi_search(
                        query=_normalize_name(maps_name),
                        dept=departement,
                        cp=_inpi_cp,
                    )
            else:
                _inpi_hit = await _inpi_search(
                    query=_normalize_name(maps_name),
                    dept=departement,
                    cp=_inpi_cp,
                )
            _inpi_cache[maps_name] = _inpi_hit
        except Exception:
            _inpi_cache[maps_name] = None
            _inpi_hit = None

        # Storefront-descriptor fallback (Apr 28, Taxonomy 7): when the full
        # Step 0 search misses AND the name starts with Association/Asso/Fondation
        # AND has a " - <site>" descriptor (e.g., "Fondation Partage et Vie -
        # EHPAD Jean Balat"), retry with only the prefix before " - ". Catches
        # association-run EHPADs whose Maps name embeds the storefront site name.
        if _inpi_hit is None and " - " in maps_name and _ASSOC_PREFIX_RE.match(maps_name):
            _stripped_prefix = maps_name.split(" - ", 1)[0].strip()
            _stripped_norm = _normalize_name(_stripped_prefix)
            if _stripped_norm and _stripped_norm != _normalize_name(maps_name):
                try:
                    if _timing_batch_id is not None:
                        async with time_step(conn, _timing_batch_id, None, "inpi_step0_assoc_fallback"):
                            _inpi_hit = await _inpi_search(
                                query=_stripped_norm,
                                dept=departement,
                                cp=_inpi_cp,
                            )
                    else:
                        _inpi_hit = await _inpi_search(
                            query=_stripped_norm,
                            dept=departement,
                            cp=_inpi_cp,
                        )
                    log.info(
                        "discovery.inpi_storefront_fallback",
                        maps_name=maps_name,
                        stripped=_stripped_prefix,
                        hit=bool(_inpi_hit),
                        hit_siren=_inpi_hit[0] if _inpi_hit else None,
                    )
                    _inpi_cache[maps_name] = _inpi_hit
                except Exception:
                    pass  # _inpi_hit stays None — fall through to other steps

        if _inpi_hit:
            inpi_siren, _inpi_naf, _inpi_nom, _inpi_cp = _inpi_hit
            local_cur = await conn.execute(
                """SELECT siren, denomination, enseigne, adresse, code_postal, ville, departement
                   FROM companies
                   WHERE siren = %s AND statut = 'A' AND siren NOT LIKE 'MAPS%%'
                   LIMIT 1""",
                (inpi_siren,),
            )
            local_row = await local_cur.fetchone()
            if local_row:
                local_denom = local_row[1] or ""
                local_enseigne = local_row[2] or ""
                local_cp = local_row[4] or ""
                local_dept = local_row[6] or ""

                if _validate_inpi_step0_hit(
                    maps_cp=maps_cp,
                    departement=departement,
                    meaningful_terms=meaningful_terms,
                    local_denom=local_denom,
                    local_enseigne=local_enseigne,
                    local_cp=local_cp,
                    local_dept=local_dept,
                ):
                    log.info("discovery.inpi_primary_match",
                             maps_name=maps_name, siren=inpi_siren,
                             local_denom=local_denom, local_enseigne=local_enseigne)
                    return {
                        "siren": inpi_siren,
                        "denomination": local_denom,
                        "enseigne": local_enseigne,
                        "score": 0.92,
                        "method": "inpi",
                        "adresse": local_row[3] or "",
                        "ville": local_row[5] or "",
                    }
                else:
                    log.info("discovery.inpi_primary_rejected",
                             maps_name=maps_name, siren=inpi_siren,
                             reason="dept_or_overlap_fail")
                    if rejected_siren_sink is not None:
                        rejected_siren_sink[maps_name] = inpi_siren
        # fall through to Steps 0.5, 1-5
    else:
        if _timing_batch_id is not None:
            async with time_step(conn, _timing_batch_id, None, "inpi_step0", fired=False):
                pass

    # ── Step 0.5: Chain/franchise detector (Agent B, v2 — moved from Step 4.5) ──
    # Fires BEFORE Step 1 (siren_website) to prevent the franchise HQ-leak bug:
    # a national brand's HQ SIREN appears in the website footer, Step 1 validates
    # via name-overlap, and every local storefront gets linked to HQ. Chain detector
    # matches on (brand + maps_cp) and finds the specific storefront before Step 1
    # can misfire. CP required — dept-only would over-match across franchisees.
    # Evidence (2026-04-21, camping 83): Siblu/Capfun/Huttopia/Sandaya all HQ-leaked
    # via Step 1. This placement catches them.
    if settings.chain_detector_enabled and maps_cp:
        from fortress.matching.chains import (
            match_chain,
            match_ehpad_pseudo_chain,
            match_municipal_pseudo_chain,
            find_chain_siret,
        )
        # Public-collectivité forme_juridique whitelist — only commune/EPCI/syndicat
        # legal forms can answer to municipal-service pseudo-chain hits.
        _PUBLIC_FORME_JURIDIQUE = (
            "7210", "7220", "7225", "7229", "7230",
            "7311", "7312", "7313", "7314", "7321", "7322", "7323",
            "7331", "7332", "7333", "7340", "7341", "7342", "7343",
            "7344", "7345", "7346", "7347", "7348",
            "7351", "7352", "7353", "7354", "7355",
            "7361", "7362", "7363", "7364", "7365",
            "7366", "7367", "7368", "7369",
            "7371", "7372", "7373", "7378", "7379",
            "7381", "7382", "7383", "7384", "7385", "7389",
            "7410",
        )
        chain_hit = match_chain(maps_name)
        pj_filter: tuple[str, ...] | None = None
        if not chain_hit:
            chain_hit = match_ehpad_pseudo_chain(maps_name)
        if not chain_hit:
            muni_hit = match_municipal_pseudo_chain(maps_name)
            if muni_hit:
                chain_hit = muni_hit
                pj_filter = _PUBLIC_FORME_JURIDIQUE
        if chain_hit:
            chain_candidate = await find_chain_siret(
                conn, chain_hit, maps_cp,
                forme_juridique_filter=pj_filter,
            )
            if chain_candidate:
                # Municipal hits stamp method='commune_municipal' instead of 'chain'
                # so the auto-confirm gate can apply the public-FJ override.
                if chain_hit.sector == "commune_municipal":
                    chain_candidate["method"] = "commune_municipal"
                log.info("discovery.chain_match",
                         maps_name=maps_name, chain=chain_hit.chain_name,
                         siren=chain_candidate["siren"], maps_cp=maps_cp,
                         sector=chain_hit.sector,
                         method=chain_candidate.get("method", "chain"))
                return chain_candidate

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
                # ── Phase 2 live franchise-leak check ────────────────────
                # Before accepting: verify this SIREN isn't already confirmed
                # for a DIFFERENT MAPS entity at a DIFFERENT postal code.
                # That pattern means it's a franchise HQ (e.g. Sandaya, Siblu)
                # whose parent SIREN appears in every storefront's footer.
                _is_franchise, _conflict_count = await _is_franchise_live(
                    conn, extracted_siren, maps_cp
                )
                if _is_franchise:
                    log.warning(
                        "discovery.siren_website_rejected",
                        action="siren_website_rejected_franchise_live",
                        maps_name=maps_name,
                        siren=extracted_siren,
                        maps_cp=maps_cp,
                        conflicting_cp_count=_conflict_count,
                        reason="franchise_hq_already_confirmed_at_different_cp",
                    )
                    # Fall through to next matcher step (enseigne, phone, etc.)
                else:
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
               ORDER BY GREATEST(
                 similarity(COALESCE(enseigne,''), %s),
                 similarity(COALESCE(denomination,''), %s)
               ) DESC
               LIMIT 100""",
            ens_params + [maps_name, maps_name],
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
                dept_prefix = maps_cp[:2] if maps_cp else ""
                strict_postal = dept_prefix in _DENSE_URBAN_DEPTS

                if postal_match:
                    log.info("maps_discovery.enseigne_match", maps_name=maps_name,
                             siren=best_ens["siren"], enseigne=best_ens["enseigne"],
                             score=best_ens["score"], maps_cp=maps_cp, enseigne_cp=ens_cp,
                             postal_match=postal_match)
                    best_ens.pop("code_postal", None)
                    return best_ens
                elif (
                    not strict_postal
                    and maps_cp and ens_cp
                    and maps_cp[:2] == ens_cp[:2]
                    and best_ens_score >= 0.85
                ):
                    log.info("maps_discovery.enseigne_match_nearby", maps_name=maps_name,
                             siren=best_ens["siren"], enseigne=best_ens["enseigne"],
                             score=best_ens["score"], maps_cp=maps_cp, enseigne_cp=ens_cp)
                    best_ens.pop("code_postal", None)
                    return best_ens
                elif maps_cp and ens_cp and maps_cp != ens_cp:
                    log.warning("maps_discovery.enseigne_match_downgraded", maps_name=maps_name,
                                siren=best_ens["siren"], enseigne=best_ens["enseigne"],
                                score=best_ens["score"], maps_cp=maps_cp, enseigne_cp=ens_cp,
                                strict_postal=strict_postal,
                                reason="postal_mismatch_or_dense_urban")
                    best_ens["method"] = "enseigne_weak"
                    best_ens.pop("code_postal", None)
                    return best_ens
                else:
                    log.info("maps_discovery.enseigne_match_no_postal", maps_name=maps_name,
                             siren=best_ens["siren"], enseigne=best_ens["enseigne"],
                             score=best_ens["score"])
                    best_ens.pop("code_postal", None)
                    return best_ens

    # ── Step 2.5: CP-restricted name disambiguation ──────────────────────
    # Taxonomy: Section 3.8 Dense urban vs rural matching.
    # Foundation primitive for Priority 5 (Section 2.B Individual-name).
    # Runs only when a picker + CP are available. SIRENE is filtered by
    # NAF strict-prefix + exact postal code inside the SQL, so any match
    # is NAF-aligned by construction. Band A auto-confirms via Phase A;
    # Band B is gated by pool/dominance + first-week pending forcing.
    if picked_nafs and maps_cp:
        cp_cand = await _cp_name_disamb_match(
            conn, maps_name, maps_cp, picked_nafs, naf_division_whitelist
        )
        if cp_cand is not None:
            return cp_cand

    # ── Step 2.6: Geo proximity match (Phase 2 of TOP 1) ─────────────────
    # Fires when prior steps missed AND Maps panel produced lat/lng.
    # Uses a 400m bounding box against companies_geom (sirene_geo +
    # ban_backfill). Auto-confirm gating happens in the caller's
    # shared NAF gate; this helper only returns the candidate.
    if maps_lat is not None and maps_lng is not None:
        geo_cand = await _geo_proximity_match(
            conn, maps_name, maps_lat, maps_lng,
            picked_nafs, naf_division_whitelist,
        )
        if geo_cand is not None:
            log.info(
                "discovery.step_2_6_triggered",
                maps_name=maps_name,
                pool_size=geo_cand.get("geo_proximity_pool_size"),
                top_score=geo_cand.get("geo_proximity_top_score"),
                second_score=geo_cand.get("geo_proximity_2nd_score"),
                distance_m=geo_cand.get("geo_proximity_distance_m"),
                target_siren=geo_cand.get("siren"),
            )
            return geo_cand
    else:
        log.debug(
            "discovery.step_2_6_skipped_no_coords",
            maps_name=maps_name,
        )

    # ── Step 3: Phone match (unique identifier, no postal code) ──────────
    # A phone number is unique to one business — postal code check not needed.
    # Both sides are normalised to 0XXXXXXXXX before comparison so format
    # differences (+33/0033/0X) never produce false mismatches or duplicates.
    if maps_phone:
        maps_phone_norm = normalize_phone(maps_phone)
        if maps_phone_norm:
            phone_sql = PHONE_NORMALIZE_SQL.format(col="c.phone")
            phone_row = await (await conn.execute(
                f"""SELECT c.siren, co.denomination, co.enseigne, co.adresse, co.ville, co.code_postal
                   FROM contacts c
                   JOIN companies co ON co.siren = c.siren
                   WHERE ({phone_sql}) = %s
                     AND c.source != 'google_maps'
                     AND co.siren NOT LIKE 'MAPS%%'
                     AND co.statut = 'A'
                   LIMIT 1""",
                (maps_phone_norm,),
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
                                phone=maps_phone_norm,
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
                        phone=maps_phone_norm,
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
                        phone=maps_phone_norm,
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
        and not (set(name_tokens_4b) & _HARD_REJECT_TOKENS)  # Apr 27 — bloque "Maison de Retraite X"
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
                    if normalize_phone(db_phone) == normalize_phone(maps_phone):
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

    # ── Step 2.7: SIRET-level establishment lookup ───────────────────────
    # Backfills the gap where the SIREN-level row's NAF differs from the
    # operating SIRET's NAF (commune municipal services, multi-site SCIs,
    # franchise storefronts under a regional HQ, etc.). Required: maps_cp,
    # at least one picked NAF. Queries the establishments side table which
    # is populated by scripts/import_etablissements.py from the INSEE
    # StockEtablissement file (~30M active SIRETs).
    if maps_cp and picked_nafs:
        etab_cur = await conn.execute(
            """SELECT e.siret, e.siren, e.naf_etablissement, e.code_postal_etab,
                      c.denomination, c.enseigne, c.adresse, c.ville, c.statut
                 FROM establishments e
                 JOIN companies c ON c.siren = e.siren
                WHERE e.code_postal_etab = %s
                  AND e.naf_etablissement = ANY(%s)
                  AND e.etat_administratif = 'A'
                  AND c.statut = 'A'
                  AND c.siren NOT LIKE 'MAPS%%'
                LIMIT 50""",
            (maps_cp, list(picked_nafs)),
        )
        etab_rows = await etab_cur.fetchall()
        if len(etab_rows) == 1:
            row = etab_rows[0]
            log.info(
                "discovery.siret_address_naf_match",
                maps_name=maps_name,
                maps_cp=maps_cp,
                siret=row[0],
                siren=row[1],
                naf=row[2],
            )
            return {
                "siren": row[1],
                "denomination": row[4] or "",
                "enseigne": row[5] or "",
                "score": 0.92,
                "method": "siret_address_naf",
                "adresse": row[6] or "",
                "ville": row[7] or "",
            }
        elif len(etab_rows) > 1:
            # Disambiguate by maps_name token overlap
            maps_tokens = set(_normalize_name(maps_name).split())
            scored = []
            for r in etab_rows:
                name_tokens = set(_normalize_name(f"{r[4] or ''} {r[5] or ''}").split())
                overlap = len(maps_tokens & name_tokens)
                scored.append((overlap, r))
            scored.sort(key=lambda x: x[0], reverse=True)
            if scored and scored[0][0] >= 1 and (
                len(scored) == 1 or scored[0][0] > scored[1][0]
            ):
                r = scored[0][1]
                log.info(
                    "discovery.siret_address_naf_disamb",
                    maps_name=maps_name,
                    maps_cp=maps_cp,
                    siret=r[0],
                    siren=r[1],
                    naf=r[2],
                    token_overlap=scored[0][0],
                )
                return {
                    "siren": r[1],
                    "denomination": r[4] or "",
                    "enseigne": r[5] or "",
                    "score": 0.88,
                    "method": "siret_address_naf",
                    "adresse": r[6] or "",
                    "ville": r[7] or "",
                }
            # else fall through to Step 5

    # ── Step 5: Name search + scoring (last resort — always pending) ──────
    # Step 5 fallback INPI prefetch — fires only when Step 0 was skipped
    # (e.g., extracted_siren was set but Step 1 rejected it and fell through).
    # Uses the same search_by_name as Step 0 for cache-shape consistency.
    # Same dense-urban gating as Step 0 — require dept or dense-urban CP.
    _step5_dept_prefix = (maps_cp or "")[:2]
    _step5_inpi_fires = (
        maps_name not in _inpi_cache
        and (departement or (maps_cp and _step5_dept_prefix in _DENSE_URBAN_DEPTS))
    )
    if _step5_inpi_fires:
        try:
            from fortress.matching.inpi import search_by_name as _inpi_search
            _inpi_cp = maps_cp if _step5_dept_prefix in _DENSE_URBAN_DEPTS else None
            if _timing_batch_id is not None:
                async with time_step(conn, _timing_batch_id, None, "inpi_step0"):
                    _inpi_hit = await _inpi_search(
                        query=_normalize_name(maps_name),
                        dept=departement,
                        cp=_inpi_cp,
                    )
            else:
                _inpi_hit = await _inpi_search(
                    query=_normalize_name(maps_name),
                    dept=departement,
                    cp=_inpi_cp,
                )
            _inpi_cache[maps_name] = _inpi_hit
        except Exception:
            _inpi_cache[maps_name] = None

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
            FROM companies WHERE {where_clause}
            ORDER BY GREATEST(
              similarity(COALESCE(enseigne,''), %s),
              similarity(COALESCE(denomination,''), %s)
            ) DESC
            LIMIT 100""",  # noqa: S608
        params + [maps_name, maps_name],
    )
    rows = await cur.fetchall()
    name_candidate, name_score = _score_rows(rows)  # capture score, was discarded

    if name_candidate:
        name_candidate["method"] = "fuzzy_name"  # default: weak, pending

        # INPI arbitration: if fuzzy_name candidate AND INPI both point to same SIREN
        _cached_hit = _inpi_cache.get(maps_name)
        inpi_siren = _cached_hit[0] if _cached_hit else None
        if inpi_siren and inpi_siren == name_candidate["siren"]:
            name_tokens = _normalize_name(maps_name).split()
            # Compute city_match inline — candidate's ville vs maps address tokens
            candidate_ville = name_candidate.get("ville") or ""
            city_match = False
            if maps_city_tokens and candidate_ville:
                cand_ville_tokens = {t for t in _normalize_name(candidate_ville).split() if len(t) > 3}
                city_match = bool(cand_ville_tokens & maps_city_tokens)
            tier_threshold = get_name_threshold(maps_name, name_tokens, city_match)
            if name_score >= tier_threshold:
                name_candidate["method"] = "inpi_fuzzy_agree"
                log.info(
                    "discovery.inpi_fuzzy_agree",
                    maps_name=maps_name,
                    siren=name_candidate["siren"],
                    name_score=name_score,
                    tier_threshold=tier_threshold,
                )

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
    """Touch updated_at every 60s so the watchdog knows we're alive.

    Also logs process RSS in MB every cycle so OOM regressions are visible early.
    Threshold 1500 MB = 75% of Render standard plan's 2 GB cap.
    """
    conn = None
    try:
        conn = await psycopg.AsyncConnection.connect(
            settings.db_url, autocommit=True, **_KEEPALIVE_PARAMS
        )
        while True:
            # ── Memory telemetry — log RSS every cycle, warn if >1500MB (75% of 2GB cap) ──
            try:
                import psutil
                rss_mb = psutil.Process().memory_info().rss / 1024 / 1024
                log.info("discovery.heartbeat_rss", batch_id=batch_id, rss_mb=round(rss_mb, 1))
                if rss_mb > 1500:
                    log.warning("discovery.heartbeat_rss_high", batch_id=batch_id, rss_mb=round(rss_mb, 1))
            except Exception as _rss_exc:
                # Heartbeat must continue even if psutil somehow fails (container quirk, etc.)
                log.debug("discovery.heartbeat_rss_failed", error=str(_rss_exc))

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
                          workspace_id, completed_queries_count, queries_json, id,
                          time_cap_per_query_min, time_cap_total_min, created_at
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
            batch_int_id: int = int(row[7])  # INTEGER PK of batch_data row — used for pipeline_timings FK
            _time_cap_min: int | None = row[8] if row[8] is not None else None
            _time_cap_total_min: int | None = row[9] if row[9] is not None else None
            _batch_created_at: datetime = row[10]

            # Set ContextVar so Maps scraper can write timing rows without threading batch_id through callbacks
            batch_id_var.set(batch_int_id)

            # Parse optional department + NAF filter list from filters.
            # Accepts new key `naf_codes` (list) written by batch.py after the multi-NAF
            # migration. Falls back to legacy single `naf_code` for in-flight batches
            # launched before the migration.
            dept_filter = None
            picked_nafs: list[str] = []
            if filters_raw:
                try:
                    filters = json.loads(filters_raw) if isinstance(filters_raw, str) else filters_raw
                    dept_filter = filters.get("department")
                    raw_list = filters.get("naf_codes")
                    if isinstance(raw_list, list):
                        picked_nafs = [str(c).strip() for c in raw_list if c and str(c).strip()]
                    else:
                        legacy = (filters.get("naf_code") or "").strip()
                        if legacy:
                            picked_nafs = [legacy]
                except Exception:
                    pass

            batch_size = 2000  # Hard server-side ceiling — user-facing size control removed

            # Section-letter broadening only applies when user picked EXACTLY one section letter.
            # This is guaranteed by batch.py validation (section letters must stand alone), but
            # we re-check here for defense in depth against manually-crafted filters_json.
            naf_division_whitelist: list[str] | None = None
            if len(picked_nafs) == 1 and len(picked_nafs[0]) == 1 and picked_nafs[0].isalpha():
                from fortress.config.naf_codes import NAF_DIVISION_TO_SECTION
                section_letter = picked_nafs[0].upper()
                naf_division_whitelist = [
                    div for div, section in NAF_DIVISION_TO_SECTION.items()
                    if section == section_letter
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
                settings.db_url, min_size=1, max_size=12, open=True,
            ) as pool:
                # Publish pool to ContextVar so Maps scraper can write timing rows
                pool_var.set(pool)

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
                _query_geo_capture_count: int = 0
                _current_search_query: str = ""  # Tracks which query is active
                prev_rows: list = []  # Cross-batch dedup results (used in shortfall msg)
                _sector_word: str = ""  # Sector word for relevance filtering
                _query_stats: list[dict] = []  # Tracks per-query results for queries_json
                _widening_summary: dict[str, dict] = {}  # Per-primary widening telemetry

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
                            _website, _phone = crow[1], crow[2]
                            if _website:
                                _d = re.sub(r'^https?://(www\.)?', '', _website.strip().lower())
                                _dom = _d.split('/')[0].split('?')[0]
                                if _dom:
                                    seen_websites.add(_dom)
                            if _phone:
                                _p = normalize_phone(_phone)
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

                # Per-query effective dept. Reassigned at top of per-query loop;
                # read by _persist_result via nonlocal.
                _effective_dept: str | None = dept_filter

                # W — anti-bot watchdog state. List wrapper so _persist_result can mutate
                # without nonlocal. Reset to [None] at the top of each per-query iteration.
                _first_card_seen_at: list[float | None] = [None]

                async def _persist_result(maps_result: dict[str, Any]) -> bool | None:
                    nonlocal companies_discovered, qualified, _query_dedup_count, _query_filtered_count, _gemini_cap_logged, _query_geo_capture_count, _effective_dept

                    # Stop collecting once we've reached the user's target
                    if batch_size > 0 and companies_discovered >= batch_size:
                        return False  # Signal scraper to stop extracting cards

                    maps_name = maps_result.get("maps_name", "")
                    # W — record first card timestamp for anti-bot watchdog
                    if _first_card_seen_at[0] is None:
                        _first_card_seen_at[0] = time.monotonic()
                    maps_address = maps_result.get("address")
                    maps_phone = maps_result.get("phone")
                    maps_website = maps_result.get("website")
                    _maps_cp_match = re.search(r"\b(\d{5})\b", maps_address or "")
                    maps_cp = _maps_cp_match.group(1) if _maps_cp_match else None

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
                            async with time_step(pool, batch_int_id, None, "crawl"):
                                crawl_result = await crawl_website(
                                    url=maps_website,
                                    client=curl_client,
                                    company_name=maps_name,
                                    department=_effective_dept or "",
                                    siren="",
                                )
                            extracted_siren = crawl_result.siren_from_website if crawl_result else None
                            if extracted_siren:
                                log.info("discovery.siren_extracted", name=maps_name, siren=extracted_siren, website=maps_website)
                        except Exception as exc:
                            log.debug("discovery.siren_extract_failed", website=maps_website, error=str(exc))
                    else:
                        async with time_step(pool, batch_int_id, None, "crawl", fired=False):
                            pass

                    # ── Level 4: SIREN dedup ──────────────────────────────
                    if extracted_siren:
                        if extracted_siren in seen_sirens:
                            log.info("discovery.siren_dedup_skip", name=maps_name, siren=extracted_siren)
                            _query_dedup_count += 1
                            return
                        seen_sirens.add(extracted_siren)

                    # ── Level 5: Phone dedup ──────────────────────────────
                    if maps_phone:
                        clean_phone = normalize_phone(maps_phone)
                        if clean_phone and clean_phone in seen_phones:
                            log.info("discovery.phone_dedup_skip", name=maps_name, phone=maps_phone)
                            _query_dedup_count += 1
                            return
                        if clean_phone:
                            seen_phones.add(clean_phone)

                    # ── France country filter ────────────────────────────
                    if not _is_in_france(maps_address):
                        log.info("discovery.foreign_filtered", name=maps_name, address=maps_address)
                        _query_filtered_count += 1
                        return

                    # ── entity_total timer — records full per-entity processing time ──
                    # Placed after early-exit filters; covers SIRENE match through final DB write.
                    _entity_t0 = time.perf_counter()

                    # ── SIRENE matching — all methods in one function ──────────────
                    candidate = None
                    async with pool.connection() as conn:
                        async with time_step(pool, batch_int_id, None, "match_cascade"):
                            candidate = await _match_to_sirene(
                                conn, maps_name, maps_address, _effective_dept, maps_phone, extracted_siren,
                                rejected_siren_sink=_inpi_step0_rejected,
                                picked_nafs=picked_nafs,
                                naf_division_whitelist=naf_division_whitelist,
                                maps_cp=maps_cp,
                                maps_lat=maps_result.get("lat"),
                                maps_lng=maps_result.get("lng"),
                            )

                    log.info(
                        "discovery.a2_entry",
                        maps_name=maps_name,
                        has_candidate=candidate is not None,
                        a2_enabled=settings.a2_mentions_legales_enabled,
                        has_website=bool(maps_website),
                    )
                    try:
                        async with pool.connection() as _a2_log_conn:
                            await log_audit(
                                _a2_log_conn,
                                batch_id=batch_id,
                                siren="A2PENDING",
                                action="a2_entry",
                                result="success",
                                detail=f"maps_name={maps_name[:200]} | has_candidate={candidate is not None} | candidate_siren={(candidate or {}).get('siren', '')} | a2_enabled={settings.a2_mentions_legales_enabled} | has_website={bool(maps_website)}",
                                search_query=_current_search_query or None,
                                workspace_id=batch_workspace_id,
                            )
                    except Exception as e:
                        log.debug("a2_audit_write_failed", action="a2_entry", error=str(e))

                    # ── Lever A2 — Legal name from mentions-légales → INPI retry ──────────
                    # Fires only when Step 0-5 all missed (candidate is None). Extracts the
                    # registered legal name from the website's mentions-légales page and
                    # retries INPI with that name. Reuses Step 0's validator for safety.
                    _a2_candidate_is_weak = (
                        candidate is not None
                        and candidate.get("method") in _A2_WEAK_ELIGIBLE
                    )
                    if (
                        (candidate is None or _a2_candidate_is_weak)
                        and settings.a2_mentions_legales_enabled
                        and crawl_result is not None
                        and getattr(crawl_result, "all_html", None)
                    ):
                        # Find mentions-légales HTML (URL containing "mention" or "legal")
                        _mentions_html = None
                        for _url, _html in crawl_result.all_html.items():
                            if "mention" in _url.lower() or "legal" in _url.lower():
                                _mentions_html = _html
                                break

                        if _mentions_html:
                            log.info(
                                "discovery.a2_html_found",
                                maps_name=maps_name,
                                mentions_url=next(
                                    (u for u in crawl_result.all_html.keys()
                                     if "mention" in u.lower() or "legal" in u.lower()),
                                    None,
                                ),
                                html_len=len(_mentions_html),
                            )
                            try:
                                from fortress.matching.contacts import extract_legal_denomination
                                _legal_name = extract_legal_denomination(_mentions_html)
                            except Exception:
                                _legal_name = None

                            if _legal_name:
                                log.info(
                                    "discovery.a2_legal_name_extracted",
                                    maps_name=maps_name,
                                    legal_name=_legal_name,
                                )
                                try:
                                    async with pool.connection() as _a2_log_conn:
                                        await log_audit(
                                            _a2_log_conn,
                                            batch_id=batch_id,
                                            siren="A2PENDING",
                                            action="a2_legal_name_extracted",
                                            result="success",
                                            detail=f"maps_name={maps_name[:120]} | legal_name={_legal_name[:120]}",
                                            search_query=_current_search_query or None,
                                            workspace_id=batch_workspace_id,
                                        )
                                except Exception as e:
                                    log.debug("a2_audit_write_failed", action="a2_legal_name_extracted", error=str(e))
                                # Skip if legal name is effectively the same as Maps name
                                # (Step 0 already tried that — no point re-querying)
                                _maps_tokens = set(_normalize_name(maps_name).split())
                                _legal_tokens = set(_normalize_name(_legal_name).split())
                                _overlap = len(_maps_tokens & _legal_tokens)
                                _max_len = max(len(_maps_tokens), len(_legal_tokens))
                                _overlap_ratio = (_overlap / _max_len) if _max_len else 0.0

                                if _overlap_ratio < 0.8:
                                    log.info(
                                        "discovery.a2_retry_start",
                                        maps_name=maps_name,
                                        legal_name=_legal_name,
                                        overlap_ratio=round(_overlap_ratio, 2),
                                    )
                                    # Compute meaningful terms from legal name — same filter
                                    # semantics as Step 0's validator expects
                                    _legal_meaningful = [
                                        t for t in _normalize_name(_legal_name).split()
                                        if len(t) >= 3 and t not in _INDUSTRY_WORDS
                                    ]
                                    # Trim to first 5 meaningful tokens for INPI query (avoids
                                    # rate-limit pressure from long query strings)
                                    _legal_query_terms = _legal_meaningful[:5]
                                    if _legal_query_terms:
                                        _a2_query = " ".join(_legal_query_terms)
                                        _a2_rate_limited = False  # A2c: track retry exhaustion
                                        try:
                                            from fortress.matching import inpi as _inpi_mod
                                            from fortress.matching.inpi import search_by_name as _a2_search
                                            _a2_dept_prefix = (maps_cp or "")[:2]
                                            _a2_cp = maps_cp if _a2_dept_prefix in _DENSE_URBAN_DEPTS else None
                                            # A2c: opt into 429 retry ladder (2s / 5s / 15s).
                                            # A2 is a last-resort lever — we've already invested
                                            # crawl + regex work on this entity, so 22s worst-case
                                            # wait to rescue a transient rate-limit is worth it.
                                            # Step 0 and Step 5 keep fail-fast behavior by default.
                                            _a2_hit = await _a2_search(
                                                query=_a2_query,
                                                dept=_effective_dept,
                                                cp=_a2_cp,
                                                retry_on_rate_limit=True,
                                            )
                                            # Read-and-clear the module flag immediately
                                            # after await (single-threaded async, no race).
                                            _a2_rate_limited = _inpi_mod._LAST_A2_RATE_LIMIT_EXHAUSTED
                                            _inpi_mod._LAST_A2_RATE_LIMIT_EXHAUSTED = False
                                        except Exception as _a2_exc:
                                            log.warning(
                                                "discovery.a2_inpi_error",
                                                maps_name=maps_name,
                                                error=str(_a2_exc),
                                            )
                                            _a2_hit = None

                                        if _a2_hit:
                                            _a2_siren, _a2_naf, _a2_nom, _a2_cp = _a2_hit
                                            log.info(
                                                "discovery.a2_sirene_searched",
                                                maps_name=maps_name,
                                                legal_name=_legal_name,
                                                inpi_siren=_a2_hit[0],
                                            )
                                            async with pool.connection() as conn:
                                                _a2_local_cur = await conn.execute(
                                                    """SELECT siren, denomination, enseigne, adresse,
                                                              code_postal, ville, departement
                                                       FROM companies
                                                       WHERE siren = %s AND statut = 'A' AND siren NOT LIKE 'MAPS%%'
                                                       LIMIT 1""",
                                                    (_a2_siren,),
                                                )
                                                _a2_local_row = await _a2_local_cur.fetchone()
                                            if _a2_local_row:
                                                log.info(
                                                    "discovery.a2_candidate_found",
                                                    maps_name=maps_name,
                                                    legal_name=_legal_name,
                                                    siren=_a2_siren,
                                                    local_denom=_a2_local_row[1] or "",
                                                )
                                                _a2_local_denom = _a2_local_row[1] or ""
                                                _a2_local_enseigne = _a2_local_row[2] or ""
                                                _a2_local_cp = _a2_local_row[4] or ""
                                                _a2_local_dept = _a2_local_row[6] or ""

                                                if _validate_inpi_step0_hit(
                                                    maps_cp=maps_cp,
                                                    departement=_effective_dept,
                                                    meaningful_terms=_legal_meaningful,
                                                    local_denom=_a2_local_denom,
                                                    local_enseigne=_a2_local_enseigne,
                                                    local_cp=_a2_local_cp,
                                                    local_dept=_a2_local_dept,
                                                ):
                                                    log.info(
                                                        "discovery.a2_match_confirmed",
                                                        maps_name=maps_name,
                                                        legal_name=_legal_name,
                                                        siren=_a2_siren,
                                                    )
                                                    log.info(
                                                        "discovery.a2_confirmed",
                                                        maps_name=maps_name,
                                                        legal_name=_legal_name,
                                                        siren=_a2_siren,
                                                    )
                                                    try:
                                                        async with pool.connection() as _a2_log_conn:
                                                            await log_audit(
                                                                _a2_log_conn,
                                                                batch_id=batch_id,
                                                                siren=_a2_siren,
                                                                action="a2_match_confirmed",
                                                                result="success",
                                                                detail=f"maps_name={maps_name[:120]} | legal_name={_legal_name[:120]} | prev_method={(candidate or {}).get('method')}",
                                                                search_query=_current_search_query or None,
                                                                workspace_id=batch_workspace_id,
                                                            )
                                                    except Exception as e:
                                                        log.debug("a2_audit_write_failed", action="a2_match_confirmed", error=str(e))
                                                    candidate = {
                                                        "siren": _a2_siren,
                                                        "denomination": _a2_local_denom,
                                                        "enseigne": _a2_local_enseigne,
                                                        "score": 0.92,
                                                        "method": "inpi_mentions_legales",
                                                        "adresse": _a2_local_row[3] or "",
                                                        "ville": _a2_local_row[5] or "",
                                                    }
                                                else:
                                                    log.info(
                                                        "discovery.a2_match_rejected",
                                                        maps_name=maps_name,
                                                        legal_name=_legal_name,
                                                        siren=_a2_siren,
                                                        reason="dept_or_overlap_fail",
                                                    )
                                                    log.info(
                                                        "discovery.a2_rejected",
                                                        maps_name=maps_name,
                                                        legal_name=_legal_name,
                                                        siren=_a2_siren,
                                                        reason="dept_or_overlap_fail",
                                                    )
                                                    try:
                                                        async with pool.connection() as _a2_log_conn:
                                                            await log_audit(
                                                                _a2_log_conn,
                                                                batch_id=batch_id,
                                                                siren=_a2_siren,
                                                                action="a2_rejected",
                                                                result="fail",
                                                                detail=f"maps_name={maps_name[:120]} | legal_name={_legal_name[:120]} | reason=dept_or_overlap_fail",
                                                                search_query=_current_search_query or None,
                                                                workspace_id=batch_workspace_id,
                                                            )
                                                    except Exception as e:
                                                        log.debug("a2_audit_write_failed", action="a2_rejected", error=str(e))
                                            else:
                                                log.info(
                                                    "discovery.a2_no_local_row",
                                                    maps_name=maps_name,
                                                    inpi_siren=_a2_siren,
                                                )
                                        else:
                                            # A2c: dual-emit — a2_inpi_rate_limited fires IN ADDITION
                                            # to a2_inpi_no_hit when retries were exhausted.
                                            # Preserves existing a2_inpi_no_hit counter for historical
                                            # funnel comparisons.
                                            if _a2_rate_limited:
                                                log.info(
                                                    "discovery.a2_inpi_rate_limited",
                                                    maps_name=maps_name,
                                                    legal_name=_legal_name,
                                                    retries_attempted=3,  # matches len(inpi._A2_RETRY_DELAYS); hardcoded to avoid cross-module import
                                                )
                                            log.info(
                                                "discovery.a2_inpi_no_hit",
                                                maps_name=maps_name,
                                                legal_name=_legal_name,
                                            )
                                            try:
                                                async with pool.connection() as _a2_log_conn:
                                                    await log_audit(
                                                        _a2_log_conn,
                                                        batch_id=batch_id,
                                                        siren="A2PENDING",
                                                        action="a2_inpi_no_hit",
                                                        result="fail",
                                                        detail=f"maps_name={maps_name[:120]} | legal_name={_legal_name[:120]} | rate_limited={_a2_rate_limited}",
                                                        search_query=_current_search_query or None,
                                                        workspace_id=batch_workspace_id,
                                                    )
                                            except Exception as e:
                                                log.debug("a2_audit_write_failed", action="a2_inpi_no_hit", error=str(e))
                                    else:
                                        log.info(
                                            "discovery.a2_no_meaningful_terms",
                                            maps_name=maps_name,
                                            legal_name=_legal_name,
                                        )
                                        try:
                                            async with pool.connection() as _a2_log_conn:
                                                await log_audit(
                                                    _a2_log_conn,
                                                    batch_id=batch_id,
                                                    siren="A2PENDING",
                                                    action="a2_no_meaningful_terms",
                                                    result="skipped",
                                                    detail=f"maps_name={maps_name[:120]} | legal_name={_legal_name[:120]}",
                                                    search_query=_current_search_query or None,
                                                    workspace_id=batch_workspace_id,
                                                )
                                        except Exception as e:
                                            log.debug("a2_audit_write_failed", action="a2_no_meaningful_terms", error=str(e))
                                else:
                                    log.info(
                                        "discovery.a2_skip_same_name",
                                        maps_name=maps_name,
                                        legal_name=_legal_name,
                                        overlap_ratio=round(_overlap_ratio, 2),
                                    )
                                    try:
                                        async with pool.connection() as _a2_log_conn:
                                            await log_audit(
                                                _a2_log_conn,
                                                batch_id=batch_id,
                                                siren="A2PENDING",
                                                action="a2_skip_same_name",
                                                result="skipped",
                                                detail=f"maps_name={maps_name[:120]} | legal_name={_legal_name[:120]} | overlap_ratio={round(_overlap_ratio, 2)}",
                                                search_query=_current_search_query or None,
                                                workspace_id=batch_workspace_id,
                                            )
                                    except Exception as e:
                                        log.debug("a2_audit_write_failed", action="a2_skip_same_name", error=str(e))
                            else:
                                log.info(
                                    "discovery.a2_extract_returned_none",
                                    maps_name=maps_name,
                                    html_len=len(_mentions_html),
                                )
                                try:
                                    async with pool.connection() as _a2_log_conn:
                                        await log_audit(
                                            _a2_log_conn,
                                            batch_id=batch_id,
                                            siren="A2PENDING",
                                            action="a2_extract_returned_none",
                                            result="fail",
                                            detail=f"maps_name={maps_name[:120]} | html_len={len(_mentions_html)}",
                                            search_query=_current_search_query or None,
                                            workspace_id=batch_workspace_id,
                                        )
                                except Exception as e:
                                    log.debug("a2_audit_write_failed", action="a2_extract_returned_none", error=str(e))
                        else:
                            log.info(
                                "discovery.a2_no_mentions_page",
                                maps_name=maps_name,
                                pages_crawled=len(crawl_result.all_html),
                                urls=[
                                    u.split("/", 3)[-1][:60] if "://" in u else u[:60]
                                    for u in list(crawl_result.all_html.keys())[:5]
                                ],
                            )
                            try:
                                async with pool.connection() as _a2_log_conn:
                                    await log_audit(
                                        _a2_log_conn,
                                        batch_id=batch_id,
                                        siren="A2PENDING",
                                        action="a2_no_mentions_page",
                                        result="fail",
                                        detail=f"maps_name={maps_name[:120]} | pages_crawled={len(crawl_result.all_html)}",
                                        search_query=_current_search_query or None,
                                        workspace_id=batch_workspace_id,
                                    )
                            except Exception as e:
                                log.debug("a2_audit_write_failed", action="a2_no_mentions_page", error=str(e))
                    elif candidate is not None and settings.a2_mentions_legales_enabled and maps_website:
                        log.info(
                            "discovery.a2_skip_has_candidate",
                            maps_name=maps_name,
                            siren=candidate.get("siren"),
                            method=candidate.get("method"),
                        )

                    # ── Edit C: inpi_fuzzy_agree demotion + shadow-judge eligibility ──
                    # DETERMINISTIC — does NOT use Gemini. Demotes when Step 5
                    # re-proposes a SIREN that Step 0 already rejected for this entity.
                    # This prevents silent override of Step 0's rejection.
                    _step0_rejected_for_this = _inpi_step0_rejected.get(maps_name)
                    if (
                        candidate
                        and candidate.get("method") == "inpi_fuzzy_agree"
                        and _step0_rejected_for_this
                        and candidate.get("siren") == _step0_rejected_for_this
                    ):
                        log.info(
                            "discovery.inpi_fuzzy_agree_rejected_by_step0",
                            maps_name=maps_name,
                            siren=candidate["siren"],
                        )
                        candidate["method"] = "inpi_fuzzy_agree_rejected_by_step0"

                    # Shadow-judge eligibility (D1a observer):
                    # Patch A (April 21): observe ALL rows with a candidate (strong + weak)
                    #   as well as no-candidate rows. Broader scope means more ground truth for
                    #   D1b threshold tuning. Verdict is still purely informational.
                    # Patch B (April 21): also observe zero-candidate rows with no Step 0
                    #   rejection — a trigram pool will be seeded at call site.
                    if settings.gemini_enabled and settings.gemini_api_key:
                        if candidate is None and _step0_rejected_for_this:
                            _shadow_judge_needed.add((maps_name, maps_address or ""))
                        elif candidate is not None:
                            # Observe ALL rows with a candidate (strong + weak).
                            _shadow_judge_needed.add((maps_name, maps_address or ""))
                        elif candidate is None and not _step0_rejected_for_this:
                            # Patch B: trigram pool will be fetched at call site.
                            _shadow_judge_needed.add((maps_name, maps_address or ""))

                    # ── Triage: classify before expensive work ────────────
                    _triage_t0 = time.perf_counter()
                    triage_bucket = "RED"  # Default: full pipeline

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
                        await write_timing(pool, batch_int_id, None, "triage_check", int((time.perf_counter() - _triage_t0) * 1000), True)
                        await write_timing(pool, batch_int_id, None, "entity_total", int((time.perf_counter() - _entity_t0) * 1000), True)
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
                                    """SELECT phone, email, website,
                                              social_linkedin, social_facebook, social_twitter,
                                              social_instagram, social_tiktok,
                                              collected_at
                                       FROM contacts
                                       WHERE siren = %s AND source = 'google_maps'
                                         AND collected_at > NOW() - INTERVAL '30 days'
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
                        has_social = bool(
                            existing_contact[3] or  # social_linkedin
                            existing_contact[4] or  # social_facebook
                            existing_contact[5] or  # social_twitter
                            existing_contact[6] or  # social_instagram
                            existing_contact[7]     # social_tiktok
                        )

                        if has_phone and has_email and has_website and has_social:
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
                                    action="triage_green",
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
                            await write_timing(pool, batch_int_id, lookup_siren, "triage_check", int((time.perf_counter() - _triage_t0) * 1000), True)
                            await write_timing(pool, batch_int_id, lookup_siren, "entity_total", int((time.perf_counter() - _entity_t0) * 1000), True)
                            return
                        else:
                            triage_bucket = "YELLOW"
                            triage_counts["yellow"] += 1
                    else:
                        triage_bucket = "RED"
                        triage_counts["red"] += 1

                    # ── triage_check: record classification time (normal YELLOW/RED path) ──
                    await write_timing(pool, batch_int_id, None, "triage_check", int((time.perf_counter() - _triage_t0) * 1000), True)

                    # ── Dispatch to worker body (queue or inline) ────────
                    # companies_discovered is incremented here (producer side)
                    # so the counter is always accurate even if workers lag.
                    companies_discovered += 1
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

                    _entity_data = (
                        maps_result, candidate, crawl_result, extracted_siren,
                        maps_name, maps_address, maps_phone, maps_website, maps_cp,
                        website_domain, triage_bucket, companies_discovered,
                    )
                    if settings.worker_pool_enabled:
                        await _entity_queue.put(_entity_data)
                    else:
                        await _process_entity_post_triage(*_entity_data)

                # ── Worker body (runs inline or in pool worker task) ──────────
                # Contains everything after triage: MAPS entity creation, DB persist,
                # SIRENE link, Gemini judge, officer fetch, and audit logging.
                async def _process_entity_post_triage(
                    maps_result: dict[str, Any],
                    candidate: dict | None,
                    crawl_result: Any,
                    extracted_siren: str | None,
                    maps_name: str,
                    maps_address: str | None,
                    maps_phone: str | None,
                    maps_website: str | None,
                    maps_cp: str | None,
                    website_domain: str | None,
                    triage_bucket: str,
                    idx: int,
                ) -> None:
                    nonlocal qualified, _gemini_cap_logged, _query_geo_capture_count
                    # Local timer for entity_total — works in both serial (inline call from
                    # _persist_result) and worker-pool (called from queue.get loop) modes.
                    # Without this, line ~4214's read of _entity_t0 hits NameError under workers
                    # because the producer's _entity_t0 (line 2657) isn't in the worker's scope.
                    _entity_t0 = time.perf_counter()

                    # ALWAYS create a MAPS entity — never use matched SIREN as entity ID
                    async with pool.connection() as id_conn:
                        cur = await id_conn.execute("SELECT nextval('maps_id_seq')")
                        next_id = (await cur.fetchone())[0]
                    siren = f"MAPS{next_id:05d}"
                    # NEW: use _parse_maps_address helper so CP/ville are populated at insert time,
                    # enabling _copy_sirene_reference_data's COALESCE to preserve the Maps location
                    # instead of being overwritten by the SIRENE siège (Frankenstein fix, Apr 22).
                    _company_cp, _company_ville = _parse_maps_address(maps_address)
                    _company_dept = (_company_cp[:2] if _company_cp else None) or _effective_dept

                    company = Company(
                        siren=siren,
                        denomination=maps_name,
                        enseigne=maps_name,
                        adresse=maps_address,
                        code_postal=_company_cp,
                        ville=_company_ville,
                        departement=_company_dept,
                        statut="A",
                        workspace_id=batch_workspace_id,
                    )

                    # Store candidate link metadata
                    # Address match = auto-confirm (high confidence). Name match = pending (user decides).
                    _pending_link: dict | None = None
                    if candidate:
                        _pending_link = candidate
                        if candidate["method"] in _STRONG_METHODS:
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
                        async with time_step(pool, batch_int_id, siren, "db_write"):
                            await upsert_company(conn, company)
                            await bulk_tag_query(conn, [siren], batch_name, workspace_id=batch_workspace_id, batch_id=batch_id)
                            await upsert_contact(conn, contact)

                        # ── Phase 1 — persist Maps panel coordinates if captured ───────
                        _maps_lat = maps_result.get("lat")
                        _maps_lng = maps_result.get("lng")
                        if _maps_lat is not None and _maps_lng is not None:
                            try:
                                await conn.execute(
                                    """INSERT INTO companies_geom
                                           (siren, lat, lng, source, geocode_quality)
                                       VALUES (%s, %s, %s, 'maps_panel', NULL)
                                       ON CONFLICT (siren) DO UPDATE SET
                                           lat = EXCLUDED.lat,
                                           lng = EXCLUDED.lng,
                                           source = EXCLUDED.source,
                                           updated_at = NOW()
                                       WHERE companies_geom.source = 'maps_panel'""",
                                    (siren, _maps_lat, _maps_lng),
                                )
                                _query_geo_capture_count += 1
                            except Exception as e:
                                log.debug("discovery.geom_insert_failed",
                                          siren=siren, error=str(e))

                        # Default link state: pending. Auto-confirm logic (Phase A):
                        # - Strong method + NAF verified → always auto-confirm
                        # - Strong method + NAF mismatch but 2+ signals agree → auto-confirm
                        # - Strong method + no NAF filter (empty picker) → auto-confirm
                        # - Weak methods → never auto-confirm
                        if _pending_link:
                            method = _pending_link["method"]
                            target_siren = _pending_link["siren"]

                            # Step 2.5 Band B forced-pending: keep pending while
                            # _BAND_B_AUTO_CONFIRM_ENABLED=False (first 7-day audit window).
                            _band_b_forced_pending = (
                                _pending_link.get("cp_name_disamb_band") == "B"
                                and not _BAND_B_AUTO_CONFIRM_ENABLED
                            )

                            # Compute naf_status first so we can decide on auto-confirm
                            naf_status = None
                            matched_naf = ""
                            if picked_nafs:
                                naf_cur = await conn.execute(
                                    "SELECT naf_code FROM companies WHERE siren = %s",
                                    (target_siren,),
                                )
                                sirene_naf_row = await naf_cur.fetchone()
                                matched_naf = (sirene_naf_row[0] if sirene_naf_row else None) or ""
                                naf_status = _compute_naf_status(matched_naf, picked_nafs, naf_division_whitelist)

                            # Phase A auto-confirm gate: multi-signal approach
                            link_signals = None
                            if method in _STRONG_METHODS:
                                link_signals, sirene_cp = await _verify_signals(
                                    conn, target_siren, maps_name, maps_phone, maps_address, extracted_siren
                                )
                                agree_count = sum(1 for v in link_signals.values() if v is True)
                                # Step 2.6 geo telemetry — stamp proximity metadata into link_signals
                                # so the frontend tooltip and QA SQL can surface it.
                                if method == "geo_proximity" and _pending_link is not None:
                                    link_signals["geo_proximity_distance_m"] = _pending_link.get("geo_proximity_distance_m")
                                    link_signals["geo_proximity_top_score"] = _pending_link.get("geo_proximity_top_score")
                                    link_signals["geo_proximity_2nd_score"] = _pending_link.get("geo_proximity_2nd_score")
                                    link_signals["geo_proximity_pool_size"] = _pending_link.get("geo_proximity_pool_size")
                                    link_signals["geo_proximity_quality"] = _pending_link.get("geo_proximity_quality")
                                    if naf_status == "mismatch":
                                        link_signals["sector_mismatch"] = True
                                if naf_status == "verified":
                                    auto_confirm = True
                                elif naf_status == "mismatch" and method == "siren_website" and link_signals.get("siren_website_match"):
                                    # Fix A: self-declared SIREN on website footer is deterministic proof.
                                    # 1 signal (the siren_website_match itself) is enough, even with NAF mismatch.
                                    auto_confirm = True
                                elif naf_status == "mismatch" and agree_count >= 2:
                                    auto_confirm = True
                                elif picked_nafs == []:
                                    # Safeguard: name-only strong methods with no NAF filter
                                    # require ≥ 1 secondary signal for auto-confirm.
                                    # geo_proximity added here (Phase 2 fix): a 400m bounding-box
                                    # match on name-similarity alone with no sector filter is
                                    # too permissive — requires ≥1 signal same as INPI-family.
                                    if method in {
                                        "inpi", "inpi_fuzzy_agree", "inpi_mentions_legales",
                                        "chain", "geo_proximity",
                                    }:
                                        auto_confirm = agree_count >= 1
                                    else:
                                        # Y2 (Apr 30 — Inno'vin fix): for methods that have a
                                        # directly-verifiable signal in _verify_signals(), require
                                        # that signal to be True before auto-confirming. Without a
                                        # NAF gate to force verification, this is the only check
                                        # stopping address/enseigne/phone/siren_website matches
                                        # from confirming when their own evidence disagrees.
                                        _METHOD_OWN_SIGNAL = {
                                            "address": "address_match",
                                            "enseigne": "enseigne_match",
                                            "phone": "phone_match",
                                            "siren_website": "siren_website_match",
                                        }
                                        _own_sig = _METHOD_OWN_SIGNAL.get(method)
                                        if _own_sig is not None:
                                            auto_confirm = link_signals.get(_own_sig) is True
                                        else:
                                            # Other strong methods (gemini_judge, etc.) have their
                                            # own confidence mechanisms upstream — keep prior behaviour.
                                            auto_confirm = True
                                else:
                                    auto_confirm = False
                                # Giclette/LES TONTONS recovery: enseigne + exact CP in dense-urban dept
                                # + NAF section-letter match → treat as implicit 2nd signal.
                                # Reuses auto_linked_mismatch_accepted audit action.
                                if (
                                    not auto_confirm
                                    and method == "enseigne"
                                    and naf_status == "mismatch"
                                    and agree_count == 1
                                    and link_signals.get("enseigne_match") is True
                                    and (_company_dept or "")[:2] in _DENSE_URBAN_DEPTS
                                    and maps_cp and sirene_cp and maps_cp == sirene_cp
                                    and _naf_section_matches(matched_naf, picked_nafs)
                                ):
                                    auto_confirm = True
                                    log.info("discovery.giclette_recovery",
                                             maps_name=maps_name, siren=target_siren,
                                             maps_cp=maps_cp, sirene_cp=sirene_cp,
                                             sirene_naf=matched_naf, picked_nafs=picked_nafs)
                                # Municipal pseudo-chain auto-confirm — public-collectivité override.
                                # When method=='commune_municipal', the matched SIRENE is a commune/EPCI
                                # (forme_juridique 72xx/73xx/74xx) acting as the legal owner of a service
                                # whose operational NAF differs from the FJ-implied admin NAF (84.11Z).
                                # Example: SIREN 214002669 (COMMUNE DE ST JULIEN EN BORN, FJ=7210) is
                                # the owner of SIRET 21400266900072 (NAF 55.30Z, the campground). At
                                # the SIREN-aggregate level we only see 84.11Z. NAF mismatch is EXPECTED;
                                # (brand prefix + maps_cp + public-FJ filter at the picker) is the trust.
                                if (
                                    not auto_confirm
                                    and method == "commune_municipal"
                                ):
                                    # Re-fetch matched SIRENE's forme_juridique to confirm public-collectivité status.
                                    fj_cur = await conn.execute(
                                        "SELECT forme_juridique FROM companies WHERE siren = %s",
                                        (target_siren,),
                                    )
                                    fj_row = await fj_cur.fetchone()
                                    matched_fj = (fj_row[0] if fj_row else None) or ""
                                    if matched_fj.startswith(("72", "73", "74")):
                                        auto_confirm = True
                                        log.info("discovery.commune_municipal_auto_confirm",
                                                 maps_name=maps_name, siren=target_siren,
                                                 maps_cp=maps_cp, matched_fj=matched_fj,
                                                 matched_naf=matched_naf, picked_nafs=picked_nafs)
                                # Chain post-else override: franchise storefronts only ever
                                # produce 1 agreeing signal (enseigne). The (brand + CP +
                                # sector-aligned NAF) triple already makes the match
                                # high-confidence — section-letter NAF alignment is the
                                # final safeguard. Independent of Giclette (different method).
                                if (
                                    not auto_confirm
                                    and method == "chain"
                                    and naf_status == "mismatch"
                                    and agree_count >= 1
                                    and _naf_section_matches(matched_naf, picked_nafs)
                                ):
                                    auto_confirm = True
                                    log.info("discovery.chain_section_match_auto_confirm",
                                             maps_name=maps_name, siren=target_siren,
                                             matched_naf=matched_naf, picked_nafs=picked_nafs)
                                # Track 2 SIRET-address-NAF override: Step 2.7's SQL
                                # already enforces exact CP + establishment-level NAF
                                # in picker (`naf_etablissement = ANY(picked_nafs)`,
                                # see lines 2102-2114) + active SIREN/SIRET +
                                # 1-row-or-token-dominant disambiguation. Signal
                                # agreement is structurally inapplicable: head SIREN
                                # signals (phone, enseigne, denomination) belong to a
                                # different entity by design (commune mairie ≠
                                # operated campground), so agree_count is routinely
                                # 0. Section-letter alignment is also inapplicable:
                                # head NAF 84.11Z (section O) will never match
                                # establishment NAF 55.30Z (section I) — but the
                                # establishment NAF DOES strict-prefix-match the
                                # picker. Trust the SQL constraints.
                                if (
                                    not auto_confirm
                                    and method == "siret_address_naf"
                                    and naf_status == "mismatch"
                                ):
                                    auto_confirm = True
                                    log.info("discovery.siret_address_naf_auto_confirm",
                                             maps_name=maps_name, siren=target_siren,
                                             matched_naf=matched_naf, picked_nafs=picked_nafs,
                                             agree_count=agree_count,
                                             signals=link_signals)
                            else:
                                auto_confirm = False

                            # Step 2.5 Band B: override auto_confirm to False during
                            # first-week audit window (_BAND_B_AUTO_CONFIRM_ENABLED=False).
                            if _band_b_forced_pending:
                                auto_confirm = False

                            link_state = "confirmed" if auto_confirm else "pending"

                            await conn.execute(
                                """UPDATE companies
                                   SET linked_siren    = %s,
                                       link_confidence = %s,
                                       link_method     = %s,
                                       link_signals    = %s
                                   WHERE siren = %s""",
                                (target_siren, link_state, method,
                                 json.dumps(link_signals) if link_signals is not None else None,
                                 siren),
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
                                # 5-branch audit: distinguish by method, NAF filter state and outcome
                                strict_prefix = bool(
                                    matched_naf and any(matched_naf.startswith(p) for p in picked_nafs)
                                )
                                if method == "inpi_fuzzy_agree":
                                    audit_action = "auto_linked_inpi_agree"
                                elif method == "inpi_mentions_legales":
                                    audit_action = "auto_linked_mentions_legales"
                                elif method == "chain":
                                    audit_action = "auto_linked_chain"
                                elif method == "commune_municipal":
                                    audit_action = "auto_linked_municipal"
                                elif method == "cp_name_disamb":
                                    audit_action = "auto_linked_cp_name_disamb"
                                elif method == "cp_name_disamb_indiv":
                                    audit_action = "auto_linked_individual_match"
                                elif method == "geo_proximity":
                                    audit_action = "auto_linked_geo_proximity"
                                elif method == "siret_address_naf":
                                    audit_action = "auto_linked_siret_address_naf"
                                elif picked_nafs == []:
                                    audit_action = "auto_linked_strong_no_filter"
                                elif naf_status == "verified":
                                    if strict_prefix or naf_division_whitelist is not None:
                                        audit_action = "auto_linked_verified"
                                    else:
                                        audit_action = "auto_linked_expanded"
                                else:
                                    # naf_status == "mismatch" AND agree_count >= 2
                                    audit_action = "auto_linked_mismatch_accepted"
                                await log_audit(
                                    conn,
                                    batch_id=batch_id,
                                    siren=siren,
                                    action=audit_action,
                                    result="success",
                                    detail=f"Auto-confirmé → {target_siren} (method={method}, naf_status={naf_status}, audit={audit_action})",
                                    workspace_id=batch_workspace_id,
                                )

                            # Step 2.5 Band B pending audit — emitted when Band B is
                            # forced to pending (first-week audit window).
                            if (
                                not auto_confirm
                                and _pending_link.get("cp_name_disamb_band") == "B"
                            ):
                                await log_audit(
                                    conn,
                                    batch_id=batch_id,
                                    siren=siren,
                                    action="pending_cp_name_disamb",
                                    result="success",
                                    detail=(
                                        f"Pending (Band B audit) → {target_siren} "
                                        f"(sim={_pending_link.get('cp_name_disamb_meta', {}).get('top_sim')}, "
                                        f"pool={_pending_link.get('cp_name_disamb_meta', {}).get('pool_size')})"
                                    ),
                                    workspace_id=batch_workspace_id,
                                )
                            # Y2 audit: track downgrades caused by the no-filter signal-verification gate.
                            # Fires when picked_nafs == [] AND method ∈ {address, enseigne, phone, siren_website}
                            # AND link_signals[method's own signal] is not True.
                            if (
                                not auto_confirm
                                and picked_nafs == []
                                and method in {"address", "enseigne", "phone", "siren_website"}
                                and link_signals is not None
                            ):
                                _own_sig_key = {
                                    "address": "address_match", "enseigne": "enseigne_match",
                                    "phone": "phone_match", "siren_website": "siren_website_match",
                                }[method]
                                await log_audit(
                                    conn, batch_id=batch_id, siren=siren,
                                    action="pending_no_filter_signal_disagree",
                                    result="success",
                                    detail=f"Y2 gate: method={method}, signal={_own_sig_key}={link_signals.get(_own_sig_key)}, downgraded to pending",
                                    workspace_id=batch_workspace_id,
                                )

                        # MAPS-only (no SIRENE candidate): mark naf_status if NAF filter was applied
                        if not _pending_link and picked_nafs:
                            await conn.execute(
                                "UPDATE companies SET naf_status = 'maps_only' WHERE siren = %s",
                                (siren,),
                            )

                        # ── Edit D: Gemini shadow judge (Wave D1a) + D1b Hybrid ──
                        # D1a: verdict is always LOGGED (shadow audit).
                        # D1b: when gemini_d1b_hybrid_enabled=True, Gemini can influence linking:
                        #   quarantine path — downgrade strong auto-confirm to pending on high-confidence no_match
                        #   rescue path    — upgrade weak/maps_only to confirmed on high-confidence match
                        # _just_auto_confirmed: True when the auto-confirm path above ran AND copied SIRENE fields.
                        # Used by quarantine path to know if rollback is needed.
                        _just_auto_confirmed = bool(_pending_link and auto_confirm)
                        _gemini_judge_needed = (maps_name, maps_address or "") in _shadow_judge_needed
                        if _gemini_judge_needed:
                            try:
                                _cost = _gemini_judge._COST_PER_CALL_USD
                                if await _gemini_budget.would_exceed(_cost):
                                    if not _gemini_cap_logged:
                                        _gemini_cap_logged = True
                                        await log_audit(
                                            conn,
                                            batch_id=batch_id,
                                            siren=siren,
                                            action="gemini_budget_exceeded",
                                            result="skipped",
                                            detail=json.dumps({
                                                "cap_usd": settings.gemini_batch_budget_usd,
                                                "calls_so_far": _gemini_budget.calls,
                                            }),
                                            workspace_id=batch_workspace_id,
                                        )
                                else:
                                    # RGPD-strip phone only (email is never in prompt).
                                    _maps_phone_norm = normalize_phone(maps_phone) if maps_phone else None
                                    if _maps_phone_norm and _maps_phone_norm in _opposition_phones:
                                        _maps_phone_for_prompt = None
                                    else:
                                        _maps_phone_for_prompt = maps_phone
                                    _rejected_for_this = _inpi_step0_rejected.get(maps_name)

                                    # Decide candidate list:
                                    #  (a) matcher returned candidate → list of that one
                                    #  (b) no candidate + Step 0 rejection → empty list (Gemini sees "no candidate")
                                    #  (c) no candidate + no Step 0 rejection → trigram pool
                                    _trigram_path_needed = (
                                        candidate is None and not _rejected_for_this
                                    )
                                    if candidate is not None:
                                        if (
                                            settings.gemini_multi_candidate_enabled
                                            and candidate.get("method") != "chain"
                                        ):
                                            _alternatives = await _gather_alternatives(
                                                conn, maps_name, _maps_lat, _maps_lng,
                                                _effective_dept, exclude_siren=candidate["siren"], k=3,
                                            )
                                            _candidates_for_prompt = [candidate] + _alternatives
                                        else:
                                            _candidates_for_prompt = [candidate]
                                    elif _trigram_path_needed:
                                        try:
                                            _candidates_for_prompt = await _fetch_trigram_candidates(
                                                conn,
                                                maps_name,
                                                _effective_dept,  # per-query dept, not batch-level
                                                limit=10,
                                            )
                                        except Exception as _trg_exc:
                                            log.warning("discovery.trigram_pool_failed",
                                                        maps_name=maps_name, error=str(_trg_exc))
                                            _candidates_for_prompt = []
                                        if not _candidates_for_prompt:
                                            await log_audit(
                                                conn,
                                                batch_id=batch_id,
                                                siren=siren,
                                                action="gemini_shadow_no_candidates",
                                                result="skipped",
                                                detail="trigram_pool_empty",
                                                workspace_id=batch_workspace_id,
                                            )
                                            raise _gemini_judge._SkipGemini()
                                    else:
                                        _candidates_for_prompt = []

                                    async with time_step(pool, batch_int_id, siren, "gemini_judge"):
                                        _verdict = await _gemini_judge.judge_match(
                                            api_key=settings.gemini_api_key,
                                            maps_name=maps_name,
                                            maps_address=maps_address,
                                            maps_phone=_maps_phone_for_prompt,
                                            candidates=_candidates_for_prompt,
                                            rejected_siren=_rejected_for_this,
                                            fallback_model=(settings.gemini_model_fallback or None),
                                        )
                                    if _verdict is not None:
                                        await _gemini_budget.spend(_cost)
                                        _v = _verdict.get("verdict")
                                        _vconf = _verdict.get("confidence") or 0.0
                                        _vpicked = _verdict.get("picked_siren")

                                        # Shadow-mode audit action (preserved for backward compat + D1b dashboard)
                                        if _v == "match":
                                            _action = "gemini_shadow_yes"
                                        elif _v == "no_match":
                                            _action = "gemini_shadow_no"
                                        else:
                                            _action = "gemini_shadow_ambiguous"

                                        await log_audit(
                                            conn,
                                            batch_id=batch_id,
                                            siren=siren,
                                            action=_action,
                                            result="success",
                                            detail=json.dumps({
                                                "verdict": _v,
                                                "confidence": _vconf,
                                                "picked_siren": _vpicked,
                                                "reasoning": (_verdict.get("reasoning") or "")[:200],
                                                "candidate_count": len(_candidates_for_prompt),
                                                "candidate_method": (
                                                    _candidates_for_prompt[0].get("method")
                                                    if _candidates_for_prompt else None
                                                ),
                                                "path": ("trigram_pool" if _trigram_path_needed
                                                         else ("strong" if candidate and candidate.get("method") in _STRONG_METHODS
                                                               else "weak_or_none")),
                                                "rejected_siren": _rejected_for_this,
                                                "model": _gemini_judge._MODEL_NAME,
                                            }, ensure_ascii=False),
                                            workspace_id=batch_workspace_id,
                                        )

                                        # ── D1b Hybrid: act on Gemini verdict ──
                                        if settings.gemini_d1b_hybrid_enabled:
                                            # ── Swap path (NEW) ──
                                            # Fire when: currently auto-confirmed, Gemini says match
                                            # but to a DIFFERENT SIREN than the one we just confirmed,
                                            # at high confidence, AND this is NOT a chain match.
                                            if (
                                                _v == "match"
                                                and _vpicked is not None
                                                and _pending_link is not None
                                                and _just_auto_confirmed
                                                and _vpicked != _pending_link["siren"]
                                                and _vconf >= settings.gemini_d1b_quarantine_threshold
                                                and _pending_link["method"] != "chain"
                                            ):
                                                _target_for_swap = _vpicked
                                                _original_siren = _pending_link["siren"]
                                                _original_method = _pending_link["method"]

                                                # (a) Hosting-SIREN guardrail — placed BEFORE any DB write.
                                                from fortress.matching.contacts import _HOSTING_SIRENS
                                                if _target_for_swap in _HOSTING_SIRENS:
                                                    await log_audit(
                                                        conn, batch_id=batch_id, siren=siren,
                                                        action="gemini_swap_rejected_hosting", result="skipped",
                                                        detail=json.dumps({
                                                            "original_siren": _original_siren,
                                                            "picked_siren": _target_for_swap,
                                                            "gemini_confidence": _vconf,
                                                        }, ensure_ascii=False),
                                                        workspace_id=batch_workspace_id,
                                                    )
                                                else:
                                                    # (b) Verify target SIRENE row exists and is active.
                                                    _swap_cur = await conn.execute(
                                                        """SELECT siren, denomination, enseigne, adresse, ville, naf_code
                                                             FROM companies
                                                            WHERE siren = %s AND statut = 'A' AND siren NOT LIKE 'MAPS%%'
                                                            LIMIT 1""",
                                                        (_target_for_swap,),
                                                    )
                                                    _swap_row = await _swap_cur.fetchone()
                                                    if not _swap_row:
                                                        await log_audit(
                                                            conn, batch_id=batch_id, siren=siren,
                                                            action="gemini_swap_rejected_inactive", result="skipped",
                                                            detail=json.dumps({
                                                                "original_siren": _original_siren,
                                                                "picked_siren": _target_for_swap,
                                                            }, ensure_ascii=False),
                                                            workspace_id=batch_workspace_id,
                                                        )
                                                    else:
                                                        async with conn.transaction():
                                                            # (c) Roll back original SIRENE-data copy; NULL link_signals + naf_status.
                                                            await conn.execute(
                                                                """UPDATE companies
                                                                      SET siret_siege = NULL, naf_code = NULL, naf_libelle = NULL,
                                                                          forme_juridique = NULL, date_creation = NULL,
                                                                          tranche_effectif = NULL,
                                                                          link_signals = NULL, naf_status = NULL
                                                                    WHERE siren = %s""",
                                                                (siren,),
                                                            )
                                                            # (d) Apply new SIREN's reference data + recompute naf_status.
                                                            await _copy_sirene_reference_data(conn, siren, _target_for_swap)
                                                            _swap_naf = _swap_row[5] or ""
                                                            _swap_status = None
                                                            if picked_nafs:
                                                                _swap_status = _compute_naf_status(
                                                                    _swap_naf, picked_nafs, naf_division_whitelist,
                                                                )
                                                                await conn.execute(
                                                                    "UPDATE companies SET naf_status = %s WHERE siren = %s",
                                                                    (_swap_status, siren),
                                                                )
                                                            # (e) Update link state.
                                                            await conn.execute(
                                                                """UPDATE companies
                                                                      SET linked_siren = %s,
                                                                          link_confidence = 'confirmed',
                                                                          link_method = 'gemini_judge'
                                                                    WHERE siren = %s""",
                                                                (_target_for_swap, siren),
                                                            )
                                                            # (f) Success audit — INSIDE transaction.
                                                            await log_audit(
                                                                conn, batch_id=batch_id, siren=siren,
                                                                action="auto_linked_gemini_swap", result="success",
                                                                detail=json.dumps({
                                                                    "original_siren": _original_siren,
                                                                    "original_method": _original_method,
                                                                    "swapped_to_siren": _target_for_swap,
                                                                    "gemini_confidence": _vconf,
                                                                    "naf_status": _swap_status,
                                                                    "gemini_reasoning": (_verdict.get("reasoning") or "")[:200],
                                                                }, ensure_ascii=False),
                                                                workspace_id=batch_workspace_id,
                                                            )

                                                        # Outside transaction:
                                                        _pending_link = {
                                                            "siren": _target_for_swap,
                                                            "denomination": _swap_row[1] or "",
                                                            "enseigne": _swap_row[2] or "",
                                                            "adresse": _swap_row[3] or "",
                                                            "ville": _swap_row[4] or "",
                                                            "method": "gemini_judge",
                                                            "score": _vconf,
                                                        }
                                                        log.info("discovery.gemini_swap",
                                                                 maps_name=maps_name, maps_siren=siren,
                                                                 original_siren=_original_siren,
                                                                 swapped_to_siren=_target_for_swap, confidence=_vconf)

                                            # ── Quarantine path ──
                                            # Fire when: currently auto-confirmed, strong method,
                                            #            Gemini says no_match with high confidence,
                                            #            AND this is NOT a chain match,
                                            #            AND the Frankenstein signature does NOT trigger.
                                            elif (
                                                _v == "no_match"
                                                and _vconf >= settings.gemini_d1b_quarantine_threshold
                                                and _pending_link is not None
                                                and _just_auto_confirmed  # was auto-confirmed pre-Gemini
                                                and _pending_link["method"] != "chain"  # exclude chain (Agent B territory)
                                            ):
                                                _target_for_frank = _pending_link["siren"]
                                                # Fetch SIRENE denom+enseigne for Frankenstein check.
                                                _frank_cur = await conn.execute(
                                                    "SELECT denomination, enseigne FROM companies WHERE siren = %s",
                                                    (_target_for_frank,),
                                                )
                                                _frank_row = await _frank_cur.fetchone()
                                                _frank_denom = (_frank_row[0] if _frank_row else None) or ""
                                                _frank_enseigne = (_frank_row[1] if _frank_row else None) or ""
                                                _is_frank = _is_frankenstein_parent_siren(
                                                    maps_name, _frank_denom, _frank_enseigne,
                                                )
                                                if not _is_frank:
                                                    # Capture the original method BEFORE mutating _pending_link.
                                                    # _pending_link aliases `candidate` (line ~2958), so the next
                                                    # mutation also overwrites candidate["method"]. The audit
                                                    # detail below reads original_method via this captured value.
                                                    _original_method = _pending_link["method"]
                                                    # Quarantine: flip to pending, retag method, roll back companies row.
                                                    _pending_link["method"] = "gemini_quarantine"
                                                    await conn.execute(
                                                        """UPDATE companies
                                                              SET link_confidence = 'pending',
                                                                  link_method = 'gemini_quarantine',
                                                                  siret_siege = NULL,
                                                                  naf_code = NULL,
                                                                  naf_libelle = NULL,
                                                                  forme_juridique = NULL,
                                                                  date_creation = NULL
                                                            WHERE siren = %s""",
                                                        (siren,),
                                                    )
                                                    await log_audit(
                                                        conn,
                                                        batch_id=batch_id,
                                                        siren=siren,
                                                        action="gemini_quarantine",
                                                        result="success",
                                                        detail=json.dumps({
                                                            "quarantined_siren": _target_for_frank,
                                                            "original_method": _original_method,
                                                            "gemini_confidence": _vconf,
                                                            "gemini_reasoning": (_verdict.get("reasoning") or "")[:200],
                                                            "frankenstein_checked": True,
                                                            "frankenstein_result": False,
                                                        }, ensure_ascii=False),
                                                        workspace_id=batch_workspace_id,
                                                    )
                                                    log.info("discovery.gemini_quarantine",
                                                             maps_name=maps_name, maps_siren=siren,
                                                             quarantined_siren=_target_for_frank,
                                                             confidence=_vconf)
                                                else:
                                                    # Frankenstein protection fired — leave auto-confirm intact.
                                                    await log_audit(
                                                        conn,
                                                        batch_id=batch_id,
                                                        siren=siren,
                                                        action="gemini_frankenstein_protected",
                                                        result="skipped",
                                                        detail=json.dumps({
                                                            "target_siren": _target_for_frank,
                                                            "sirene_denom": _frank_denom,
                                                            "sirene_enseigne": _frank_enseigne,
                                                            "gemini_confidence": _vconf,
                                                        }, ensure_ascii=False),
                                                        workspace_id=batch_workspace_id,
                                                    )

                                            # ── Rescue path ──
                                            # Fire when: no auto-confirm happened (weak candidate or maps_only),
                                            #            Gemini said match at high confidence with a picked_siren,
                                            #            the picked SIREN passes section-letter NAF check
                                            #            (if picker non-empty), AND is not on hosting blacklist.
                                            elif (
                                                _v == "match"
                                                and _vconf >= settings.gemini_d1b_rescue_threshold
                                                and _vpicked is not None
                                                and not _just_auto_confirmed  # currently pending or maps_only
                                            ):
                                                from fortress.matching.contacts import _HOSTING_SIRENS
                                                if _vpicked in _HOSTING_SIRENS:
                                                    # Never rescue into a known hosting/umbrella SIREN.
                                                    await log_audit(
                                                        conn,
                                                        batch_id=batch_id,
                                                        siren=siren,
                                                        action="gemini_rescue_rejected_hosting",
                                                        result="skipped",
                                                        detail=json.dumps({
                                                            "picked_siren": _vpicked,
                                                            "gemini_confidence": _vconf,
                                                        }, ensure_ascii=False),
                                                        workspace_id=batch_workspace_id,
                                                    )
                                                else:
                                                    # Fetch target SIRENE row for NAF + display fields.
                                                    _rescue_cur = await conn.execute(
                                                        """SELECT siren, denomination, enseigne, adresse, ville, naf_code
                                                             FROM companies
                                                            WHERE siren = %s AND statut = 'A' AND siren NOT LIKE 'MAPS%%'
                                                            LIMIT 1""",
                                                        (_vpicked,),
                                                    )
                                                    _rescue_row = await _rescue_cur.fetchone()
                                                    if _rescue_row:
                                                        _rescue_naf = _rescue_row[5] or ""
                                                        # NAF check: section-letter (matches chain detector policy).
                                                        # Pass when picker is empty OR section letter aligns.
                                                        _rescue_naf_ok = (
                                                            not picked_nafs
                                                            or _naf_section_matches(_rescue_naf, picked_nafs)
                                                        )
                                                        if _rescue_naf_ok:
                                                            # Perform rescue: upgrade to confirmed with method=gemini_judge.
                                                            await conn.execute(
                                                                """UPDATE companies
                                                                      SET linked_siren = %s,
                                                                          link_confidence = 'confirmed',
                                                                          link_method = 'gemini_judge'
                                                                    WHERE siren = %s""",
                                                                (_vpicked, siren),
                                                            )
                                                            # Recompute naf_status if picker was applied.
                                                            _rescue_status = None
                                                            if picked_nafs:
                                                                _rescue_status = _compute_naf_status(
                                                                    _rescue_naf, picked_nafs, naf_division_whitelist,
                                                                )
                                                                await conn.execute(
                                                                    "UPDATE companies SET naf_status = %s WHERE siren = %s",
                                                                    (_rescue_status, siren),
                                                                )
                                                            # Copy SIRENE reference data (same semantics as manual /link approve).
                                                            await _copy_sirene_reference_data(conn, siren, _vpicked)
                                                            # Mutate _pending_link so officer-fetch gate fires.
                                                            _pending_link = {
                                                                "siren": _vpicked,
                                                                "denomination": _rescue_row[1] or "",
                                                                "enseigne": _rescue_row[2] or "",
                                                                "adresse": _rescue_row[3] or "",
                                                                "ville": _rescue_row[4] or "",
                                                                "method": "gemini_judge",
                                                                "score": _vconf,
                                                            }
                                                            await log_audit(
                                                                conn,
                                                                batch_id=batch_id,
                                                                siren=siren,
                                                                action="auto_linked_gemini_rescue",
                                                                result="success",
                                                                detail=json.dumps({
                                                                    "rescued_siren": _vpicked,
                                                                    "gemini_confidence": _vconf,
                                                                    "naf_status": _rescue_status,
                                                                    "gemini_reasoning": (_verdict.get("reasoning") or "")[:200],
                                                                }, ensure_ascii=False),
                                                                workspace_id=batch_workspace_id,
                                                            )
                                                            log.info("discovery.gemini_rescue",
                                                                     maps_name=maps_name, maps_siren=siren,
                                                                     rescued_siren=_vpicked, confidence=_vconf)
                                                        else:
                                                            await log_audit(
                                                                conn,
                                                                batch_id=batch_id,
                                                                siren=siren,
                                                                action="gemini_rescue_rejected_naf",
                                                                result="skipped",
                                                                detail=json.dumps({
                                                                    "picked_siren": _vpicked,
                                                                    "rescue_naf": _rescue_naf,
                                                                    "picked_nafs": picked_nafs,
                                                                    "gemini_confidence": _vconf,
                                                                }, ensure_ascii=False),
                                                                workspace_id=batch_workspace_id,
                                                            )
                            except _gemini_judge._SkipGemini:
                                pass  # already logged gemini_shadow_no_candidates
                            except Exception as _gem_exc:
                                # Shadow judge must NEVER crash the pipeline.
                                log.warning(
                                    "discovery.gemini_shadow_exception",
                                    maps_name=maps_name,
                                    error=str(_gem_exc),
                                )
                        else:
                            # Entity did not trigger Gemini — record skip row
                            async with time_step(pool, batch_int_id, siren, "gemini_judge", fired=False):
                                pass

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

                    # ── Free crawl HTML — last reader was the mentions-légales scan at line ~4049 ──
                    # Each crawl_result.all_html dict can hold 5-50MB of full-page HTML across multiple
                    # URLs (homepage + /contact + /mentions-legales etc). With N workers in flight,
                    # not freeing this means N × 50MB of dead bytes lingering until function exit.
                    # Clearing here, ~30s before function return, drops steady-state RSS by O(workers × 50MB).
                    if crawl_result is not None:
                        crawl_result.all_html.clear()

                    # Heartbeat before INPI — website crawl can be slow, keep updated_at fresh
                    await _update_job_safe(
                        conn_holder, batch_id,
                        companies_scraped=companies_discovered,
                        companies_qualified=qualified,
                        total_companies=companies_discovered,
                    )

                    # INPI: for high-confidence matches (address, website SIREN, phone+postal)
                    # Fuzzy name matches wait for user confirmation before INPI call.
                    _inpi_officers_fires = bool(_pending_link and _pending_link["method"] in _STRONG_METHODS)
                    if _inpi_officers_fires:
                        try:
                            from fortress.matching.inpi import fetch_dirigeants
                            from fortress.models import Officer, ContactSource as CS
                            from fortress.processing.dedup import upsert_officer

                            target_siren = _pending_link["siren"]
                            async with time_step(pool, batch_int_id, siren, "inpi_officers"):
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
                    else:
                        # Not a strong-method confirm — record skip row
                        async with time_step(pool, batch_int_id, siren, "inpi_officers", fired=False):
                            pass

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

                    # ── entity_total: record full per-entity duration (normal path) ──
                    await write_timing(pool, batch_int_id, siren, "entity_total", int((time.perf_counter() - _entity_t0) * 1000), True)

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

                # ── Build effective-dept set across all queries (for cross-batch dedup) ──
                _batch_effective_depts: set[str] = set()
                for _q in search_queries:
                    _d = _parse_dept_hint_from_query(_q) or dept_filter
                    if _d:
                        _batch_effective_depts.add(_d)
                _dedup_dept_list: list[str] | None = (
                    list(_batch_effective_depts) if _batch_effective_depts else None
                )

                # ── Cross-batch dedup (after lock — sees batch A's results) ──
                # Cross-batch dedup (Apr 29): dept filter removed because MAPS rows can
                # carry wrong dept stamps from old batches (see _company_dept fix at line 3340).
                # Filtering dedup by dept missed legitimate prior matches stored under a
                # different dept code, producing duplicate MAPS entities.
                async with pool.connection() as conn:
                    if batch_workspace_id is not None:
                        cur = await conn.execute(
                            """SELECT LOWER(c.denomination), LOWER(COALESCE(c.adresse, ''))
                               FROM companies c
                               WHERE c.siren LIKE 'MAPS%%'
                               AND c.workspace_id = %s
                               AND EXISTS (SELECT 1 FROM batch_tags bt WHERE bt.siren = c.siren)""",
                            (batch_workspace_id,),
                        )
                    else:
                        cur = await conn.execute(
                            """SELECT LOWER(c.denomination), LOWER(COALESCE(c.adresse, ''))
                               FROM companies c
                               WHERE c.siren LIKE 'MAPS%%'
                               AND c.workspace_id IS NULL
                               AND EXISTS (SELECT 1 FROM batch_tags bt WHERE bt.siren = c.siren)""",
                        )
                    prev_rows = await cur.fetchall()
                    for r in prev_rows:
                        seen_names.add(f"{(r[0] or '').strip()}|{(r[1] or '').strip()}")
                log.info(
                    "discovery.cross_batch_dedup",
                    existing_maps_entities=len(prev_rows),
                    workspace_id=batch_workspace_id,
                )

                # ── Build pre-dedup name set (for skipping cards before page visit) ──
                _known_names: set[str] = set()
                for entry in seen_names:
                    name_part = entry.split("|")[0].strip()
                    if name_part:
                        _nfkd = unicodedata.normalize("NFKD", name_part)
                        _known_names.add("".join(c for c in _nfkd if not unicodedata.combining(c)))

                def _should_skip_card(card_label: str) -> bool:
                    """Return True if this card name is already known in the workspace."""
                    label_lower = card_label.lower().strip()
                    nfkd = unicodedata.normalize("NFKD", label_lower)
                    clean = "".join(c for c in nfkd if not unicodedata.combining(c))
                    return clean in _known_names

                log.info(
                    "discovery.pre_dedup_ready",
                    known_names=len(_known_names),
                )

                # ── Cross-batch SIREN dedup (workspace-scoped via linked_siren) ──
                if _dedup_dept_list:
                    async with pool.connection() as conn:
                        if batch_workspace_id is not None:
                            cur = await conn.execute(
                                """SELECT c.linked_siren FROM companies c
                                   WHERE c.departement = ANY(%s)
                                   AND c.siren LIKE 'MAPS%%'
                                   AND c.linked_siren IS NOT NULL
                                   AND c.workspace_id = %s
                                   AND EXISTS (SELECT 1 FROM batch_tags bt WHERE bt.siren = c.siren)""",
                                (_dedup_dept_list, batch_workspace_id),
                            )
                        else:
                            cur = await conn.execute(
                                """SELECT c.linked_siren FROM companies c
                                   WHERE c.departement = ANY(%s)
                                   AND c.siren LIKE 'MAPS%%'
                                   AND c.linked_siren IS NOT NULL
                                   AND c.workspace_id IS NULL
                                   AND EXISTS (SELECT 1 FROM batch_tags bt WHERE bt.siren = c.siren)""",
                                (_dedup_dept_list,),
                            )
                        existing_sirens = await cur.fetchall()
                        for r in existing_sirens:
                            seen_sirens.add(r[0])
                    log.info(
                        "discovery.siren_dedup_loaded",
                        existing=len(seen_sirens),
                        dept_codes=_dedup_dept_list,
                        workspace_id=batch_workspace_id,
                    )

                # ── Gemini shadow judge state (Wave D1a) ─────────────────
                # Per-batch mutable state. Shadow-only: these structures
                # never modify candidate/link decisions.
                _gemini_budget = BudgetTracker(cap_usd=settings.gemini_batch_budget_usd)
                _inpi_step0_rejected: dict[str, str] = {}   # maps_name → rejected SIREN
                _shadow_judge_needed: set[tuple[str, str]] = set()
                _gemini_cap_logged = False  # one 'gemini_budget_exceeded' row per batch

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

                # ── Preload postal-code density cache for widening (no-op if disabled) ──
                if settings.cp_widening_enabled and _dedup_dept_list:
                    try:
                        await _load_dept_postal_codes(pool)
                    except Exception as _cp_exc:
                        log.warning("discovery.widening_cp_cache_failed", error=str(_cp_exc))

                # ── Worker pool setup (kill switch: WORKER_POOL_ENABLED) ──────────
                # When enabled: Maps producer pushes entity data to a bounded queue,
                # N worker tasks drain it concurrently — decoupling Playwright scraping
                # from the expensive post-Maps enrichment (crawl + SIRENE + INPI +
                # Gemini) and achieving a 2-3× throughput lift.
                # When disabled (default): _persist_result calls _process_entity_post_triage
                # inline — identical to the serial behaviour shipped before Brief 3.
                _WORKER_SENTINEL = object()  # unique sentinel to signal worker shutdown
                _entity_queue: asyncio.Queue = asyncio.Queue(
                    maxsize=settings.worker_pool_queue_maxsize
                )
                _worker_tasks: list[asyncio.Task] = []

                if settings.worker_pool_enabled:
                    async def _worker_task() -> None:
                        while True:
                            item = await _entity_queue.get()
                            try:
                                if item is _WORKER_SENTINEL:
                                    break
                                await _process_entity_post_triage(*item)
                            except Exception as _worker_exc:
                                log.exception(
                                    "worker.entity_error",
                                    error=str(_worker_exc),
                                )
                                # Write a batch_log error row so the failure is visible.
                                try:
                                    async with pool.connection() as _err_conn:
                                        await log_audit(
                                            _err_conn,
                                            batch_id=batch_id,
                                            siren="WORKER_ERROR",
                                            action="entity_error",
                                            result="fail",
                                            detail=str(_worker_exc)[:500],
                                            workspace_id=batch_workspace_id,
                                        )
                                except Exception:
                                    pass  # Best-effort — never crash the worker
                            finally:
                                _entity_queue.task_done()  # CRITICAL: fires even for sentinel

                    _worker_tasks = [
                        asyncio.create_task(_worker_task())
                        for _ in range(settings.worker_pool_size)
                    ]
                    log.info(
                        "discovery.worker_pool_started",
                        workers=settings.worker_pool_size,
                        queue_maxsize=settings.worker_pool_queue_maxsize,
                    )

                # ── Total time cap setup (resume-aware) ───────────────
                _total_cap_sec: float | None = (_time_cap_total_min * 60) if _time_cap_total_min else None
                _total_cap_buffer_sec = 30  # mirrors per-query drain buffer

                def _total_elapsed_sec() -> float:
                    return (datetime.now(timezone.utc) - _batch_created_at).total_seconds()

                # ── Maps Discovery (with inline persistence) ──────────
                for q_idx, search_query in enumerate(search_queries, 1):
                    # Resume: skip queries that completed in a prior run
                    if q_idx <= _completed_queries_count:
                        continue
                    # Resolve per-query effective dept (overrides batch-level dept_filter)
                    _effective_dept = _parse_dept_hint_from_query(search_query) or dept_filter
                    # Total-cap check between queries
                    if _total_cap_sec is not None and _total_elapsed_sec() >= _total_cap_sec:
                        log.warning("discovery.total_time_cap_hit_between_queries", batch_id=batch_id, cap_min=_time_cap_total_min, elapsed_min=round(_total_elapsed_sec()/60, 1))
                        break
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
                    if _effective_dept and re.match(r"^\d{2,3}$", _effective_dept):
                        clean_query = re.sub(r'\b' + re.escape(_effective_dept) + r'\b', '', search_query).strip()
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
                    _query_geo_capture_count = 0
                    _pre_query_discovered = companies_discovered
                    _query_start = time.monotonic()
                    _first_card_seen_at[0] = None   # W — reset for new query (var defined near _persist_result)

                    # Z — compute hard deadline before any await
                    _query_deadline_sec: float | None = (_time_cap_min * 60) if _time_cap_min else None

                    # W — anti-bot watchdog: cancel scrape if no first card within 90 sec
                    _ANTIBOT_FIRST_CARD_TIMEOUT_SEC = 90

                    _scraper_task = asyncio.create_task(maps_scraper.search_all(
                        clean_query, on_result=_persist_result,
                        dept_code=_effective_dept,
                        max_results=_max_cards,
                        sector_word=_sector_word,
                        should_skip=_should_skip_card,
                    ))

                    async def _antibot_watchdog():
                        await asyncio.sleep(_ANTIBOT_FIRST_CARD_TIMEOUT_SEC)
                        if _first_card_seen_at[0] is None:
                            log.warning(
                                "discovery.antibot_block_detected",
                                query=search_query,
                                threshold_sec=_ANTIBOT_FIRST_CARD_TIMEOUT_SEC,
                            )
                            _scraper_task.cancel()

                    _watchdog_task = asyncio.create_task(_antibot_watchdog())

                    results: list = []
                    _query_abort_reason: str | None = None
                    # Compute effective timeout: whichever cap fires first wins
                    _per_query_remaining: float | None = _query_deadline_sec
                    _total_remaining: float | None = (_total_cap_sec - _total_elapsed_sec()) if _total_cap_sec is not None else None
                    if _total_remaining is not None and _total_remaining < 0:
                        _total_remaining = 0

                    _effective_timeout: float | None
                    _effective_reason_on_timeout: str
                    if _total_remaining is not None and (_per_query_remaining is None or _total_remaining < _per_query_remaining):
                        _effective_timeout = _total_remaining
                        _effective_reason_on_timeout = "total_time_cap_reached"
                    else:
                        _effective_timeout = _per_query_remaining
                        _effective_reason_on_timeout = "time_cap_reached_primary"

                    try:
                        if _effective_timeout is not None:
                            results = await asyncio.wait_for(_scraper_task, timeout=_effective_timeout)
                        else:
                            results = await _scraper_task
                    except asyncio.TimeoutError:
                        log.warning(
                            "discovery.query_time_cap_hit",
                            query=search_query, cap_min=_time_cap_min,
                        )
                        _query_abort_reason = _effective_reason_on_timeout
                        _scraper_task.cancel()
                        if settings.worker_pool_enabled:
                            try:
                                await asyncio.wait_for(_entity_queue.join(), timeout=30)
                            except asyncio.TimeoutError:
                                pass
                    except asyncio.CancelledError:
                        # Anti-bot watchdog cancelled the scrape (W)
                        _query_abort_reason = "antibot_block"
                        log.warning("discovery.antibot_query_aborted", query=search_query)
                    finally:
                        _watchdog_task.cancel()
                        try:
                            await _watchdog_task
                        except (asyncio.CancelledError, Exception):
                            pass

                    if _query_abort_reason:
                        log.info(
                            "discovery.query_aborted",
                            query=search_query,
                            reason=_query_abort_reason,
                            results_so_far=len(results),
                        )
                        _query_stats.append({
                            "query": search_query,
                            "cards_found": len(results),
                            "new_companies": companies_discovered - _pre_query_discovered,
                            "filtered_count": _query_filtered_count,
                            "dedup_count": _query_dedup_count,
                            "is_expansion": False,
                            "duration_sec": round(time.monotonic() - _query_start, 1),
                            "abort_reason": _query_abort_reason,
                        })
                        if _query_abort_reason == "total_time_cap_reached":
                            break
                        continue   # bypass expansion loop for this query

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

                    if (
                        settings.cp_widening_enabled
                        and _effective_dept
                        and not _shutdown
                        and companies_discovered < 2000
                    ):
                        # Check cancel flag before starting widening
                        _widen_cancelled = False
                        try:
                            _c_row = await (await conn_holder[0].execute(
                                "SELECT cancel_requested FROM batch_data WHERE batch_id = %s",
                                (batch_id,),
                            )).fetchone()
                            _widen_cancelled = bool(_c_row and _c_row[0])
                        except Exception:
                            pass

                        if not _widen_cancelled:
                            # Load density-ranked postal codes (one-shot cache)
                            _dept_cps = (await _load_dept_postal_codes(pool)).get(_effective_dept, [])
                            _postal_codes_candidates = _dept_cps[:settings.cp_widening_postal_codes_max]

                            # Skip index 0 (prefecture, already covered by primary query)
                            _cities_candidates = DEPT_CITIES.get(_effective_dept, [])[1:]

                            # Per-primary yield at start of widening
                            _primary_yield_at_start = companies_discovered - _pre_query_discovered
                            _primary_pre_widening = _pre_query_discovered
                            _consecutive_dry = 0
                            _widened_count = 0
                            _cities_tried: list[str] = []
                            _codes_tried: list[str] = []
                            _widen_stop_reason = None

                            # Pass 1: secondary cities — Pass 2: postal codes by SIRENE density
                            _widen_candidates = (
                                [("city", c) for c in _cities_candidates]
                                + [("postal_code", cp) for cp in _postal_codes_candidates]
                            )

                            # Drain workers before reading companies_discovered for widening
                            # — otherwise dry-streak detection misfires because the counter
                            # doesn't yet reflect entities still being processed by workers.
                            if settings.worker_pool_enabled:
                                await _entity_queue.join()

                            for _widen_type, _widen_value in _widen_candidates:
                                # Stop conditions
                                if _widened_count >= settings.cp_widening_max_per_primary:
                                    _widen_stop_reason = "max_per_primary"
                                    break
                                if companies_discovered >= 2000:
                                    _widen_stop_reason = "ceiling"
                                    break
                                if _shutdown:
                                    _widen_stop_reason = "shutdown"
                                    break
                                # Total-cap check inside widening loop
                                if _total_cap_sec is not None and _total_elapsed_sec() >= _total_cap_sec:
                                    _widen_stop_reason = "total_time_cap_reached"
                                    break
                                # Soft cap: covers primary→expansion cumulative time. Primary search_all itself is not killable mid-scrape.
                                if _time_cap_min is not None and (time.monotonic() - _query_start) >= _time_cap_min * 60:
                                    _widen_stop_reason = "time_cap_reached"
                                    break
                                # Re-check cancel
                                try:
                                    _c2_row = await (await conn_holder[0].execute(
                                        "SELECT cancel_requested FROM batch_data WHERE batch_id = %s",
                                        (batch_id,),
                                    )).fetchone()
                                    if _c2_row and _c2_row[0]:
                                        _widen_stop_reason = "cancel"
                                        break
                                except Exception:
                                    pass

                                # Cumulative yield for this primary so far
                                _cumulative = (
                                    (companies_discovered - _primary_pre_widening)
                                    + _primary_yield_at_start
                                )
                                # Above the floor: apply dry-streak stop
                                if (
                                    _cumulative >= settings.cp_widening_min_useful_yield
                                    and _consecutive_dry >= settings.cp_widening_dry_streak_max
                                ):
                                    _widen_stop_reason = "threshold_met_dry_streak"
                                    break

                                # Build widened query — sector word + city/postal code
                                _widened_query = f"{_sector_word} {_widen_value}".strip() if _sector_word else _widen_value

                                # Remaining capacity for this widened search
                                _w_remaining = max(0, 2000 - companies_discovered)
                                _w_max_cards = _w_remaining * 3 if _w_remaining > 0 else 0

                                _widen_query_start = time.monotonic()
                                _widen_error: str | None = None
                                _pre_widen_count = companies_discovered
                                _w_cards_found = 0
                                try:
                                    _w_results = await maps_scraper.search_all(
                                        _widened_query,
                                        on_result=_persist_result,
                                        dept_code=_effective_dept,
                                        max_results=_w_max_cards,
                                        sector_word=_sector_word,
                                        should_skip=_should_skip_card,
                                    )
                                    _w_cards_found = len(_w_results)
                                except Exception as _w_exc:
                                    log.warning("discovery.widening_query_failed", query=_widened_query, error=str(_w_exc))
                                    _w_cards_found = 0
                                    _widen_error = str(_w_exc)[:200]

                                _w_new_companies = companies_discovered - _pre_widen_count
                                _widened_count += 1

                                if _widen_type == "city":
                                    _cities_tried.append(_widen_value)
                                else:
                                    _codes_tried.append(_widen_value)

                                # Update dry-streak
                                if _w_new_companies < settings.cp_widening_dry_threshold:
                                    _consecutive_dry += 1
                                else:
                                    _consecutive_dry = 0

                                _cumulative_after = (
                                    (companies_discovered - _primary_pre_widening)
                                    + _primary_yield_at_start
                                )

                                _query_stats.append({
                                    "query": _widened_query,
                                    "is_expansion": True,
                                    "expansion_reason": "cp_widening",
                                    "widening_type": _widen_type,
                                    "value": _widen_value,
                                    "primary_query": search_query,
                                    "new_companies": _w_new_companies,
                                    "cards_found": _w_cards_found,
                                    "consecutive_dry_streak_after": _consecutive_dry,
                                    "primary_cumulative_yield_after": _cumulative_after,
                                    "duration_sec": round(time.monotonic() - _widen_query_start, 1),
                                    "error": _widen_error,
                                })

                                # Audit row in batch_log
                                _wid_siren_slug = f"WIDEN_{_effective_dept}_{_wid_slug(_widen_value)}"[:50]
                                try:
                                    async with pool.connection() as _wid_conn:
                                        await log_audit(
                                            _wid_conn,
                                            batch_id=batch_id,
                                            siren=_wid_siren_slug,
                                            action="auto_widened",
                                            result="success" if _w_new_companies >= settings.cp_widening_dry_threshold else "no_new_results",
                                            search_query=_widened_query,
                                            detail=json.dumps({
                                                "primary_query": search_query,
                                                "widened_query": _widened_query,
                                                "widening_type": _widen_type,
                                                "value": _widen_value,
                                                "dept": _effective_dept,
                                                "new_companies": _w_new_companies,
                                                "cards_found": _w_cards_found,
                                                "consecutive_dry_streak_after": _consecutive_dry,
                                                "primary_cumulative_yield_after": _cumulative_after,
                                            }),
                                            workspace_id=batch_workspace_id,
                                        )
                                        await _wid_conn.commit()
                                except Exception as _audit_exc:
                                    log.debug("discovery.widening_audit_failed", error=str(_audit_exc))

                                await asyncio.sleep(settings.cp_widening_inter_query_sleep_sec)

                            # End of widening loop
                            if _widen_stop_reason is None:
                                _widen_stop_reason = "candidates_exhausted"

                            # Attach stop_reason to last expansion row for frontend
                            if _cities_tried or _codes_tried:
                                for _ws_entry in reversed(_query_stats):
                                    if _ws_entry.get("is_expansion") and _ws_entry.get("primary_query") == search_query:
                                        _ws_entry["stop_reason"] = _widen_stop_reason
                                        break

                            # Store summary for shortfall message
                            _widening_summary[search_query] = {
                                "cities_tried": len(_cities_tried),
                                "codes_tried": len(_codes_tried),
                                "stop_reason": _widen_stop_reason,
                                "total_widened": _widened_count,
                            }
                            if _widen_stop_reason == "total_time_cap_reached":
                                break  # break OUTER `for q_idx in enumerate(search_queries, 1):` loop

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
                                    _effective_dept or "",
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

                # Snapshot _shutdown at the moment the search-queries loop ends.
                # Late SIGTERMs that arrive during cleanup (worker-pool drain,
                # shortfall computation, etc.) should NOT relabel a naturally
                # completed run as "interrupted" — the work is already done.
                _shutdown_at_loop_end = _shutdown

                # ── Worker pool shutdown (when enabled) ──────────────────────────
                # Drain all remaining queued entities, then send sentinel to each
                # worker and wait for them to finish cleanly.
                if settings.worker_pool_enabled and _worker_tasks:
                    # Wait for all real items to be processed
                    await _entity_queue.join()
                    # Signal shutdown: one sentinel per worker
                    for _ in range(settings.worker_pool_size):
                        await _entity_queue.put(_WORKER_SENTINEL)
                    await asyncio.gather(*_worker_tasks, return_exceptions=True)
                    log.info(
                        "discovery.worker_pool_stopped",
                        workers=settings.worker_pool_size,
                    )

                # Web crawl skipped in Maps-first discovery mode.
                # Maps provides phone + website with 90%+ hit rates.
                # Email enrichment via website crawl can be triggered
                # separately per-company or as a follow-up batch.

                # ── Gemini shadow judge summary (Wave D1a) ────────────
                log.info(
                    "discovery.gemini_shadow_summary",
                    batch_id=batch_id,
                    calls=_gemini_budget.calls,
                    spent_usd=round(_gemini_budget.spent, 6),
                    cap_hit=_gemini_budget.hit_cap,
                    cap_usd=settings.gemini_batch_budget_usd,
                )

                # ── Shortfall detection ────────────────────────────────
                # Build human-readable dept label for shortfall messages
                if _dedup_dept_list and len(_dedup_dept_list) == 1:
                    _dept_msg = f"du département {_dedup_dept_list[0]}"
                elif _dedup_dept_list:
                    _dept_msg = f"des départements {', '.join(sorted(_dedup_dept_list))}"
                else:
                    _dept_msg = ""

                # Z/W — surface query-level abort reasons (time-cap-hit-on-primary, anti-bot)
                _abort_reasons = [qs.get("abort_reason") for qs in _query_stats if qs.get("abort_reason")]
                if _abort_reasons:
                    _first_abort = _abort_reasons[0]
                    _abort_prefix = f"Abandon: {_first_abort} ({len(_abort_reasons)} requête(s)). "
                else:
                    _abort_prefix = ""

                shortfall_msg = None
                if companies_discovered >= 2000:
                    shortfall_msg = _abort_prefix + (
                        "Limite de sécurité atteinte (2000 entités). "
                        "Le plafond de 2000 entités a été atteint. "
                        "Lancez une nouvelle recherche avec la même requête pour découvrir d'autres entités."
                    )
                    # Append widening summary for ceiling-hit branch
                    if settings.cp_widening_enabled and _dedup_dept_list and _widening_summary:
                        _last_summary = _widening_summary.get(search_queries[-1] if search_queries else "")
                        if _last_summary and (_last_summary.get("cities_tried", 0) + _last_summary.get("codes_tried", 0)) > 0:
                            _wc = _last_summary["cities_tried"]
                            _wp = _last_summary["codes_tried"]
                            shortfall_msg += (
                                f" Élargissement automatique : exploré "
                                f"{_wc} ville{'s' if _wc > 1 else ''} et "
                                f"{_wp} code{'s' if _wp > 1 else ''} postaux {_dept_msg}."
                            )
                elif batch_size > 0 and companies_discovered < batch_size:
                    already_known = len(prev_rows) if _dedup_dept_list and prev_rows else 0
                    if already_known > 0:
                        shortfall_msg = _abort_prefix + (
                            f"{qualified} nouvelles entités. "
                            f"{already_known} entreprises de cette zone avaient déjà été découvertes "
                            f"et ont été exclues."
                        )
                    else:
                        shortfall_msg = _abort_prefix + (
                            f"Google Maps a épuisé les résultats pour cette recherche. "
                            f"{qualified} entités trouvées."
                        )
                    # Append widening summary for Maps-exhausted branch
                    if shortfall_msg and settings.cp_widening_enabled and _dedup_dept_list and _widening_summary:
                        _last_summary = _widening_summary.get(search_queries[-1] if search_queries else "")
                        if _last_summary and (_last_summary.get("cities_tried", 0) + _last_summary.get("codes_tried", 0)) > 0:
                            _wc = _last_summary["cities_tried"]
                            _wp = _last_summary["codes_tried"]
                            shortfall_msg += (
                                f" Élargissement automatique : exploré "
                                f"{_wc} ville{'s' if _wc > 1 else ''} et "
                                f"{_wp} code{'s' if _wp > 1 else ''} postaux {_dept_msg}."
                            )
                    log.info(
                        "discovery.shortfall",
                        found=companies_discovered,
                        target=batch_size,
                        shortfall=batch_size - companies_discovered,
                    )
                elif _abort_prefix:
                    # Abort happened but no other shortfall condition applies —
                    # e.g. time cap hit on primary with 0 results collected.
                    shortfall_msg = _abort_prefix.strip()

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
                elif _shutdown_at_loop_end:
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
                log.info(
                    "discovery.batch_geo_capture_rate",
                    batch_id=batch_id,
                    geo_captured=_query_geo_capture_count,
                    total_entities=companies_discovered,
                    rate=(_query_geo_capture_count / companies_discovered) if companies_discovered else 0.0,
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
