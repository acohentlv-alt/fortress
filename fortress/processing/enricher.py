"""Phase 3 enrichment — wires all data sources into a single callable.

Pipeline per company (reordered based on real-world hit rates):

  1. Google Maps via Playwright (primary, 86% phone hit rate)
       → phone, website URL, address, rating, reviews, maps_url
       → Protected by asyncio.Lock (one search at a time)

  2. Website crawl (only if Step 1 found a website URL)
       → email, social links (LinkedIn, Facebook, 30+ networks)
       → Uses curl_cffi — company websites have no anti-bot

  Individual operators (sole traders) use a separate path:
       → Directory phone search (Google → 118712.fr, local.fr, etc.)
       → Maps fallback if directory search fails

  INPI officer lookup runs separately when credentials are configured.
  Recherche Entreprises API removed: produces 0 contacts in production.

Source attribution is tracked per company and summarised in a batch-level
log line: "Sources: google_maps=N, web_search=N, directory=N, none=N"

Usage (from runner / __main__):
    async with CurlClient() as client:
        async def enrich_fn(companies):
            return await enrich_companies(companies, pool=pool, curl_client=client)
        await run_query(triage, batch_name, batch_id, enrich_fn, pool)

The `enrich_fn` signature must match:
    Callable[[list[Company]], Awaitable[list[Contact]]]
"""

from __future__ import annotations

import re
from collections import Counter
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any
import time
from urllib.parse import urlparse

import structlog

from fortress.config.settings import settings
from fortress.models import Company, Contact, ContactSource
from fortress.matching.contacts import _is_valid_french_phone, is_junk_email, is_personal_email
from fortress.matching.web_search import (
    _is_individual_operator,
    classify_social_url,
    is_directory_url,
)
from fortress.scraping.http import CurlClient, CurlClientError

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Maps match validation
# ---------------------------------------------------------------------------

def _assess_match(
    maps_result: dict[str, Any],
    company: Any,
) -> str:
    """Assess whether a Maps result is for the correct company.

    Compares BOTH the business name AND address returned by Maps against
    the company's known denomination, ville, and code_postal from SIRENE.

    Scoring matrix:
        Name match + same city/postal   → "high"
        Name match + no address          → "high" (Maps confirmed the name)
        No name match + same city/postal → "low"  (probably wrong business)
        No name match + no address       → "low"
        No data at all                   → "none"

    Returns:
        'high'  — Maps name matches AND/OR address matches
        'low'   — Maps returned data but name/geo mismatch
        'none'  — Maps returned nothing
    """
    if not maps_result:
        return "none"

    # ── Name match ─────────────────────────────────
    maps_name = (maps_result.get("maps_name") or "").strip()
    denomination = (getattr(company, "denomination", None) or "").strip()
    name_matches = _names_match(maps_name, denomination)

    # ── Geographic match ───────────────────────────
    maps_address = (maps_result.get("address") or "").lower()
    geo_matches = _geo_matches(maps_address, company)

    # ── Decision matrix ────────────────────────────
    if name_matches:
        return "high"  # Name confirmed — geography is secondary

    if not maps_name:
        # Couldn't extract name (panel issue) — fall back to geo-only
        if geo_matches:
            return "high"
        return "low" if maps_result.get("phone") else "none"

    # Name was extracted from Maps but didn't match our denomination
    return "low"  # Wrong business, even if same city


# French legal form suffixes to strip during name comparison
_LEGAL_FORMS = frozenset({
    "sarl", "sas", "sasu", "eurl", "sa", "sci", "snc",
    "scs", "sca", "ei", "eirl", "asso", "association",
    "et", "cie", "fils", "freres", "groupe", "holding",
})


def _normalize_name(name: str) -> list[str]:
    """Normalize a business name: lowercase, strip accents + legal forms.

    Keeps single-char tokens when the name is mostly initials/acronyms
    (e.g. "A T N" → ["a", "t", "n"], not []).
    """
    import unicodedata
    # Strip accents: é→e, à→a, ç→c, etc.
    nfkd = unicodedata.normalize('NFKD', name.lower())
    ascii_name = ''.join(c for c in nfkd if not unicodedata.combining(c))
    tokens = re.sub(r'[^a-z0-9\s]', '', ascii_name).split()
    filtered = [t for t in tokens if t not in _LEGAL_FORMS]
    if not filtered:
        return []
    # If >= 50% of tokens are single chars, it's likely an acronym or important initial (e.g. "A T N" or "H CONVOYAGE")
    single_chars = sum(1 for t in filtered if len(t) == 1)
    if single_chars >= len(filtered) / 2:
        return filtered
    return [t for t in filtered if len(t) > 1]


def _names_match(maps_name: str, denomination: str) -> bool:
    """Fuzzy compare Maps business name with SIRENE denomination.

    Strategy:
        1. Normalize: lowercase, strip legal forms (SARL, SAS, EURL, etc.)
        2. Containment: if one normalized string contains the other → match
        3. Token overlap: if ≥50% of denomination tokens appear in maps_name → match

    Returns True if the names plausibly refer to the same business.
    """
    if not maps_name or not denomination:
        return False

    maps_tokens = _normalize_name(maps_name)
    denom_tokens = _normalize_name(denomination)

    if not maps_tokens or not denom_tokens:
        return False

    # Single-token guard: if denomination is just 1 word (e.g. "TAXI"),
    # require it to be the FIRST token of the Maps name (not just anywhere).
    # Prevents "TAXI" matching "Restaurant Chez Taxi" or "Les Frères" matching "Frères D'Armes".
    # This check MUST run before general containment to avoid false positives.
    if len(denom_tokens) == 1:
        return denom_tokens[0] == maps_tokens[0]

    # Containment check (handles "BAILLOEUIL" vs "Bailloeuil Perpignan")
    maps_joined = " ".join(maps_tokens)
    denom_joined = " ".join(denom_tokens)
    if maps_joined in denom_joined or denom_joined in maps_joined:
        return True

    # Acronym-join: "A T N" → "atn", check if any Maps token starts with it
    # Handles Maps returning "ATN Transport" for SIRENE denomination "A T N"
    if all(len(t) == 1 for t in denom_tokens) and len(denom_tokens) >= 2:
        joined_acronym = "".join(denom_tokens)
        if any(t.startswith(joined_acronym) or joined_acronym.startswith(t)
               for t in maps_tokens if len(t) >= 2):
            return True

    # Token overlap: ≥70% of denomination tokens found in maps name
    # AND at least 2 matching tokens to prevent single-word false positives.
    overlap = sum(1 for t in denom_tokens if t in maps_tokens)

    # Already handled at top of function
    pass

    # 2-word names: 1 match is sufficient (geo-check in _assess_match is safety net)
    if len(denom_tokens) == 2:
        return overlap >= 1

    # 3+ words: require at least 2 tokens AND 70% overlap
    threshold = max(2, len(denom_tokens) * 0.7)
    return overlap >= threshold


def _geo_matches(maps_address: str, company: Any) -> bool:
    """Check if Maps address is in the same city/postal/dept as SIRENE data."""
    if not maps_address:
        return False

    maps_address = maps_address.lower()

    code_postal = getattr(company, "code_postal", None)
    if code_postal and code_postal in maps_address:
        return True

    ville = getattr(company, "ville", None)
    if ville and ville.lower() in maps_address:
        return True

    departement = getattr(company, "departement", None)
    if departement:
        postal_matches = re.findall(r"\b(\d{5})\b", maps_address)
        for postal in postal_matches:
            if postal[:2] == departement or (
                len(departement) == 3 and postal[:3] == departement
            ):
                return True
    return False


def _domain_confirms_name(url_or_email: str, denomination: str) -> bool:
    """Check if a website domain or email domain contains the business name.

    Examples:
        hconvoyage.fr + "H CONVOYAGE"     → True  ("hconvoyage" contains "hconvoyage")
        bailloeuil.fr + "BAILLOEUIL"      → True
        thefrenchlane.com + "LUXURY DRIVER" → False
    """
    if not url_or_email or not denomination:
        return False

    # Extract domain base (strip www, TLD, path)
    try:
        if "@" in url_or_email:
            domain = url_or_email.split("@")[1].split(".")[0].lower()
        else:
            from urllib.parse import urlparse as _up
            domain = _up(url_or_email).netloc.lower().replace("www.", "")
            domain = domain.split(".")[0]  # "hconvoyage" from "hconvoyage.fr"
    except Exception:
        return False

    if not domain or len(domain) < 3:
        return False

    # Clean denomination to alphanumeric only
    denom_clean = re.sub(r'[^a-z0-9]', '', denomination.lower())
    if not denom_clean or len(denom_clean) < 3:
        return False

    # Check mutual containment
    return denom_clean in domain or domain in denom_clean


# MVP fields used to decide what a YELLOW company still needs.
# Note: "website" matches the Contact model field name (not "website_url").
_MVP_FIELDS = ("website", "phone", "email")

# ---------------------------------------------------------------------------
# Phone digit normalisation (used locally for _best_phone priority sorting)
# ---------------------------------------------------------------------------

_PHONE_DIGITS_RE = re.compile(r"[\s.\-()]")

# _is_valid_french_phone is imported from contact_parser (moved there to avoid
# circular imports with web_search.py which also needs it for directory searches).

# ---------------------------------------------------------------------------
# Email domain-aware selection
# ---------------------------------------------------------------------------

# Email prefixes in priority order for business contact selection.
# "contact@" is the gold standard; "info@" is very common; others are valid.
_PREFERRED_EMAIL_PREFIXES: tuple[str, ...] = (
    "contact",
    "info",
    "commercial",
    "vente",
    "ventes",
    "accueil",
    "bonjour",
    "hello",
    "secretariat",
    "direction",
)


def _extract_domain(url: str) -> str | None:
    """Return the registered domain (SLD + TLD) from a URL, or None."""
    try:
        netloc = urlparse(url).netloc.lower().lstrip("www.")
        parts = netloc.split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:])
        return parts[0] if parts else None
    except ValueError:
        return None


def _email_domain_matches(email: str, website_url: str | None) -> bool:
    """Return True if the email domain is plausibly related to the company website.

    'Related' means:
      - Same registered domain (contact@alyzia.com ↔ www.alyzia.com)
      - Or one contains the other as a word fragment (g3s-alyzia.com ↔ alyzia.com)
    """
    if not website_url:
        return True  # No website known — can't filter, accept anything non-junk

    website_domain = _extract_domain(website_url)
    if not website_domain:
        return True

    _, _, email_domain = email.partition("@")
    email_sld = _extract_domain("https://" + email_domain)
    if not email_sld:
        return False

    if email_sld == website_domain:
        return True

    # Partial overlap: alyzia-cargo.fr ↔ alyzia.com (share "alyzia")
    site_root = website_domain.split(".")[0]   # "alyzia"
    mail_root = email_sld.split(".")[0]         # "alyzia-cargo" → stripped later
    mail_root_clean = re.sub(r"[^a-z0-9]", "", mail_root)
    site_root_clean = re.sub(r"[^a-z0-9]", "", site_root)
    if len(site_root_clean) >= 4 and (
        site_root_clean in mail_root_clean or mail_root_clean in site_root_clean
    ):
        return True

    return False


def _best_email(
    emails: list[str],
    website_url: str | None,
    siren: str,
    company_name: str | None = None,
) -> str | None:
    """Pick the single best business email from a list.

    Selection strategy (in order):
      1. Remove junk emails and personal-domain emails (unless they reference
         the company name, e.g. leparadismedoc@gmail.com).
      2. Keep only emails whose domain matches the company website.
      3. Prefer emails with a preferred prefix (contact@ > info@ > commercial@…).
      4. Fall back to the first domain-matching email if no preferred prefix found.
      5. If nothing matches the domain, accept business-Gmail if company-name-related.
      6. Return None if nothing usable.
    """
    if not emails:
        return None

    # Step 0: filter out personal emails that don't reference the company
    usable = [
        e for e in emails
        if not is_personal_email(e, company_name)
    ]
    if not usable:
        # All were personal/junk — nothing usable
        return None

    # Step 1: filter by domain match
    domain_matched: list[str] = [
        e for e in usable if _email_domain_matches(e, website_url)
    ]
    candidates = domain_matched if domain_matched else usable

    # Step 2: pick preferred prefix
    local_map: dict[str, str] = {}
    for email in candidates:
        local = email.split("@")[0]
        local_map[local] = email

    for prefix in _PREFERRED_EMAIL_PREFIXES:
        if prefix in local_map:
            chosen = local_map[prefix]
            if chosen not in domain_matched:
                log.debug(
                    "enricher.email_domain_mismatch_accepted",
                    email=chosen,
                    website=website_url,
                    siren=siren,
                )
            return chosen

    # Step 3+4: return first candidate
    return candidates[0]


# ---------------------------------------------------------------------------
# Département → phone prefix geographic mapping
# ---------------------------------------------------------------------------

# French geographic phone prefixes by zone:
#   01 = Île-de-France (Paris region)
#   02 = Nord-Ouest (Bretagne, Normandie, Pays de la Loire, Centre-Val de Loire)
#   03 = Nord-Est (Alsace, Lorraine, Champagne, Bourgogne, Franche-Comté, Picardie)
#   04 = Sud-Est (PACA, Auvergne-Rhône-Alpes, Corse, Occitanie Est)
#   05 = Sud-Ouest (Nouvelle-Aquitaine, Occitanie Ouest)
_DEPT_TO_PHONE_PREFIX: dict[str, str] = {}

# 01 — Île-de-France
for d in ("75", "77", "78", "91", "92", "93", "94", "95"):
    _DEPT_TO_PHONE_PREFIX[d] = "01"

# 02 — Nord-Ouest
for d in ("14", "22", "27", "28", "29", "35", "36", "37", "41", "44", "45",
          "49", "50", "53", "56", "61", "72", "76", "85"):
    _DEPT_TO_PHONE_PREFIX[d] = "02"

# 03 — Nord-Est
for d in ("02", "08", "10", "18", "21", "25", "39", "51", "52", "54", "55",
          "57", "58", "59", "60", "62", "67", "68", "70", "71", "80", "88",
          "89", "90"):
    _DEPT_TO_PHONE_PREFIX[d] = "03"

# 04 — Sud-Est
for d in ("01", "03", "04", "05", "06", "07", "11", "13", "15", "26", "30",
          "34", "38", "42", "43", "48", "63", "66", "69", "73", "74", "83",
          "84", "2A", "2B"):
    _DEPT_TO_PHONE_PREFIX[d] = "04"

# 05 — Sud-Ouest
for d in ("09", "12", "16", "17", "19", "23", "24", "31", "32", "33", "40",
          "46", "47", "64", "65", "79", "81", "82", "86", "87"):
    _DEPT_TO_PHONE_PREFIX[d] = "05"


def _best_phone(
    phones: list[str],
    siren: str,
    departement: str | None = None,
) -> str | None:
    """Pick the best phone from a list, preferring geographic match + landlines.

    Priority:
      1. Landline matching company's département (e.g. 05 for dépt 33)
      2. Other geographic landlines (01-05)
      3. Mobile (06-07)
      4. VoIP (09)
    """
    if not phones:
        return None

    valid = [p for p in phones if _is_valid_french_phone(p)]
    if not valid:
        log.debug("enricher.all_phones_invalid", phones=phones, siren=siren)
        return None

    # Determine expected phone prefix from département
    expected_prefix = _DEPT_TO_PHONE_PREFIX.get(departement or "", None)

    def _phone_priority(p: str) -> int:
        digits = _PHONE_DIGITS_RE.sub("", p)
        if digits.startswith("+33") and len(digits) == 12:
            digits = "0" + digits[3:]
        prefix = digits[:2]

        # Exact geographic match = absolute best
        if expected_prefix and prefix == expected_prefix:
            return 0  # Geographic match — best possible

        if prefix in ("01", "02", "03", "04", "05"):
            return 1  # Landline but wrong region
        if prefix in ("06", "07"):
            return 2  # Mobile
        if prefix == "09":
            return 3  # VoIP
        return 4

    chosen = sorted(valid, key=_phone_priority)[0]
    if expected_prefix:
        chosen_digits = _PHONE_DIGITS_RE.sub("", chosen)
        if chosen_digits.startswith("+33"):
            chosen_digits = "0" + chosen_digits[3:]
        if chosen_digits[:2] != expected_prefix:
            log.debug(
                "enricher.phone_geo_mismatch",
                siren=siren,
                departement=departement,
                expected_prefix=expected_prefix,
                chosen_prefix=chosen_digits[:2],
                chosen=chosen,
            )
    return chosen


async def enrich_companies(
    companies: list[Company],
    *,
    pool: Any,
    curl_client: CurlClient,
    maps_scraper: Any | None = None,
    on_progress: Any | None = None,
    on_save: Any | None = None,
    batch_id: str = "",
    query_domain: str = "",
) -> tuple[list[Contact], int]:
    """Qualify-or-replace enrichment pipeline.

    Every company gets one Maps search. After Maps returns:
      - QUALIFIED (high confidence match) → keep + crawl website
      - NOT QUALIFIED (no data or wrong city) → replace with next DB candidate

    This ensures only Maps-confirmed companies appear in the output.
    Replacements are fetched per-company, not at batch end.

    Args:
        companies:    Initial candidate list from DB query.
        pool:         Async psycopg3 connection pool.
        curl_client:  Shared CurlClient (Chrome TLS impersonation).
        maps_scraper: PlaywrightMapsScraper instance (required).

    Returns:
        list[Contact] — one Contact per qualified company.
    """
    from collections import deque

    if not companies:
        return []

    target_count = len(companies)
    contacts: list[Contact] = []
    tried_sirens: set[str] = {c.siren for c in companies}
    candidates: deque[Company] = deque(companies)

    # Reference company for backfill queries (NAF/dept matching)
    reference_company = companies[0]
    naf_prefix = (reference_company.naf_code or "")[:2]
    dept = reference_company.departement or ""

    # Max total companies to try before giving up (prevent infinite loops).
    # With ALWAYS ACCEPT, replacements only fire on _enrich_one exceptions.
    # Each attempt = 2-4s on Maps. 2× is enough safety margin without
    # causing the batch to "run forever" when Maps is flaky.
    max_attempts = target_count * 2

    # ── Pre-load rejected SIRENs for this query pattern ────────────────────
    # Skip SIRENs that were rejected in previous runs of the same NAF+dept.
    if naf_prefix and dept:
        try:
            async with pool.connection() as conn:
                rows = await conn.execute(
                    "SELECT siren FROM rejected_sirens WHERE naf_prefix = %s AND departement = %s",
                    (naf_prefix, dept),
                )
                prev_rejected = {row[0] for row in await rows.fetchall()}
                if prev_rejected:
                    # Remove pre-rejected from candidates
                    candidates = deque(
                        c for c in candidates if c.siren not in prev_rejected
                    )
                    tried_sirens |= prev_rejected
                    log.info(
                        "enricher.pre_rejected_loaded",
                        count=len(prev_rejected),
                        naf_prefix=naf_prefix,
                        dept=dept,
                    )
        except Exception as exc:
            log.debug("enricher.pre_rejected_load_error", error=str(exc))

    # ── INPI officer lookup — disabled (no credentials configured) ──────────
    # INPI client was removed during architecture consolidation (2026-03-11).
    # To re-enable: implement INPI client, set INPI_USERNAME/INPI_PASSWORD in .env.

    # ── Pre-fetch replacement pool (one bulk query instead of per-company) ──
    replacement_pool: deque[Company] = deque()
    if naf_prefix and dept:
        try:
            prefetch_count = target_count * 3  # 3× safety margin
            bulk_replacements = await _fetch_replacement_companies(
                pool, tried_sirens, reference_company, prefetch_count,
            )
            replacement_pool = deque(bulk_replacements)
            log.info(
                "enricher.replacement_pool_prefetched",
                count=len(replacement_pool),
                naf_prefix=naf_prefix,
                dept=dept,
            )
        except Exception as exc:
            log.debug("enricher.replacement_pool_error", error=str(exc))

    # ── Qualify-or-Replace loop ────────────────────────────────────────────
    source_counts: Counter[str] = Counter()
    source_phone: Counter[str] = Counter()
    source_email: Counter[str] = Counter()
    replaced_count = 0
    rejected_count = 0  # Track how many SIRENs were rejected
    seen_phones: set[str] = set()  # Intra-batch phone dedup

    while len(contacts) < target_count and candidates and len(tried_sirens) < max_attempts:
        company = candidates.popleft()
        _t0 = time.monotonic()

        try:
            result = await _enrich_one(
                company,
                curl_client=curl_client,
                maps_scraper=maps_scraper,
                query_domain=query_domain,
            )
        except Exception as exc:
            log.warning(
                "enricher_company_error",
                siren=company.siren,
                error=str(exc),
            )
            source_counts["error"] += 1
            # Error → replace from pre-fetched pool
            if not replacement_pool and naf_prefix and dept:
                try:
                    more_repl = await _fetch_replacement_companies(
                        pool, tried_sirens, reference_company, target_count * 5
                    )
                    replacement_pool.extend(more_repl)
                except Exception as pool_exc:
                    log.debug("enricher.replacement_refill_error", error=str(pool_exc))
            
            if replacement_pool:
                repl = replacement_pool.popleft()
                tried_sirens.add(repl.siren)
                candidates.append(repl)
            # Log for admin
            await _log_enrichment(
                pool, batch_id, company, "failed", None, None, None, None, 0, None,
                int((time.monotonic() - _t0) * 1000),
            )
            continue

        contact, source_label, match_confidence, maps_name, ml_result = result
        source_counts[source_label] += 1

        # ── ALWAYS ACCEPT — Maps is enrichment, not a gatekeeper ──────
        # Every SIRENE company is a valid B2B lead by default.
        # Maps data makes it BETTER (phone, website, email), but its
        # absence never disqualifies a real registered business.
        has_enrichment = (
            contact is not None
            and match_confidence != "none"
            and any([contact.phone, contact.email, contact.website])
        )

        if not has_enrichment:
            # Maps didn't find useful data — still keep the company,
            # just create a minimal contact from SIRENE data.
            # NOTE: This is NOT a "replacement" — the company stays as-is.
            # replaced_count only increments for actual pool replacements
            # (in the error handler above, not here).
            log.info(
                "enricher.company_no_maps_data",
                siren=company.siren,
                denomination=company.denomination,
                match_confidence=match_confidence,
                qualified_so_far=len(contacts),
                target=target_count,
            )
            # Build a SIRENE-only contact (no phone/email but still valid)
            from fortress.models import ContactSource
            contact = Contact(
                siren=company.siren,
                source=ContactSource.SIRENE,
                address=company.adresse,
                website=None,
                phone=None,
                email=None,
            )
        else:
            if contact.phone:
                source_phone[source_label] += 1
            if contact.email:
                source_email[source_label] += 1

        # ── Always add to output + persist immediately ────────────────
        contacts.append(contact)

        # Real-time save: persist this company+contact to DB immediately
        # so data survives even if the wave is interrupted.
        if on_save:
            try:
                await on_save(company, contact)
            except Exception as save_exc:
                log.warning(
                    "enricher.on_save_error",
                    siren=company.siren,
                    error=str(save_exc),
                )

        # Notify runner of incremental progress
        if on_progress:
            await on_progress(len(tried_sirens), replaced_count, len(contacts))

        # ── Step 3: INPI — Officers + Financials (ALWAYS — only needs SIREN)
        # Decoupled from Maps/crawl: the INPI API only requires a 9-digit SIREN.
        # Every Discovery batch company gets officers and financials, regardless
        # of whether Maps found a website or phone number.
        try:
            async with pool.connection() as conn:
                from fortress.processing.dedup import log_audit
                total_officers = 0
                officer_sources = []

                # 3a: Mentions Légales director (only if Maps found a website)
                if has_enrichment and ml_result and ml_result.get("director_name"):
                    from fortress.models import Officer, ContactSource
                    from fortress.processing.dedup import upsert_officer
                    # Split name into nom/prenom (best effort)
                    name_parts = ml_result["director_name"].split()
                    if len(name_parts) >= 2:
                        prenom = name_parts[0].title()
                        nom = " ".join(name_parts[1:]).upper()
                    else:
                        prenom = None
                        nom = name_parts[0].upper()

                    officer = Officer(
                        siren=company.siren,
                        nom=nom,
                        prenom=prenom,
                        role=ml_result.get("director_role"),
                        civilite=ml_result.get("director_civilite"),
                        email_direct=ml_result.get("director_email"),
                        source=ContactSource.MENTIONS_LEGALES,
                    )
                    await upsert_officer(conn, officer)
                    log.info(
                        "enricher.officer_from_mentions_legales",
                        siren=company.siren,
                        name=ml_result["director_name"],
                    )
                    total_officers += 1
                    officer_sources.append("Site web (Mentions légales)")

                # 3b: Recherche Entreprises API — official directors (always runs)
                from fortress.matching.inpi import fetch_dirigeants
                from fortress.models import Officer, ContactSource
                from fortress.processing.dedup import upsert_officer
                dirigeants, re_company_data = await fetch_dirigeants(
                    company.siren,
                    curl_client=curl_client,
                )
                for d in dirigeants:
                    officer = Officer(
                        siren=company.siren,
                        nom=d["nom"],
                        prenom=d.get("prenom"),
                        role=d.get("qualite"),
                        civilite=d.get("civilite"),
                        source=ContactSource.RECHERCHE_ENTREPRISES,
                    )
                    await upsert_officer(conn, officer)

                if dirigeants:
                    total_officers += len(dirigeants)
                    officer_sources.append("Registre National (API INPI)")

                if total_officers > 0:
                    det = f"{total_officers} dirigeant(s) identifié(s). Sources: {', '.join(officer_sources)}."
                    await log_audit(conn, batch_id=batch_id, siren=company.siren, action="officers_found", result="success", detail=det)

                # Store company-level financial data from API (revenue, effectif)
                if re_company_data:
                    _update_parts = []
                    _update_vals = []
                    if "chiffre_affaires" in re_company_data:
                        _update_parts.append("chiffre_affaires = %s")
                        _update_vals.append(re_company_data["chiffre_affaires"])
                    if "resultat_net" in re_company_data:
                        _update_parts.append("resultat_net = %s")
                        _update_vals.append(re_company_data["resultat_net"])
                    if "tranche_effectif" in re_company_data:
                        _update_parts.append("tranche_effectif = COALESCE(tranche_effectif, %s)")
                        _update_vals.append(re_company_data["tranche_effectif"])
                    if _update_parts:
                        _update_vals.append(company.siren)
                        # chiffre_affaires = direct overwrite (API has latest fiscal year)
                        # tranche_effectif = COALESCE (keep SIRENE data if already set)
                        await conn.execute(
                            f"UPDATE companies SET {', '.join(_update_parts)} WHERE siren = %s",
                            tuple(_update_vals),
                        )

                    # Log financial data events
                    fin_details = []
                    if re_company_data.get("chiffre_affaires"):
                        ca_val = re_company_data["chiffre_affaires"]
                        fin_details.append(f"CA: {ca_val} €")
                    if re_company_data.get("resultat_net"):
                        rn_val = re_company_data["resultat_net"]
                        fin_details.append(f"Résultat net: {rn_val} €")
                    if re_company_data.get("tranche_effectif"):
                        fin_details.append(f"Effectif INSEE: {re_company_data['tranche_effectif']}")

                    if fin_details:
                        fin_text = " • ".join(fin_details) + ". Source: API INPI"
                        await log_audit(conn, batch_id=batch_id, siren=company.siren, action="financial_data", result="success", detail=fin_text)

                if dirigeants:
                    log.info(
                        "enricher.officers_from_api",
                        siren=company.siren,
                        count=len(dirigeants),
                        ca=re_company_data.get("chiffre_affaires") if re_company_data else None,
                    )

                # SIREN Match Validation Logging (only if Maps found a website)
                if has_enrichment:
                    if contact.siren_match is True:
                        await log_audit(conn, batch_id=batch_id, siren=company.siren, action="siren_verified", result="success", detail=f"SIREN vérifié sur le site web ({contact.website}).")
                    elif contact.siren_match is False:
                        await log_audit(conn, batch_id=batch_id, siren=company.siren, action="siren_mismatch", result="fail", detail="Alerte : Le numéro SIREN extrait des mentions légales du site web ne correspond pas à l'entité recherchée.")

                await conn.commit()
        except Exception as officer_exc:
            log.debug(
                "enricher.officer_upsert_error",
                siren=company.siren,
                error=str(officer_exc),
            )

        # Log for admin
        outcome = "qualified" if has_enrichment else "sirene_only"
        await _log_enrichment(
            pool, batch_id, company, outcome, match_confidence,
            contact.phone, contact.website, maps_name,
            1 if contact.email else 0, None,
            int((time.monotonic() - _t0) * 1000),
        )

    # (rejected SIRENs are now persisted immediately per-company above)


    # ── Summary stats ─────────────────────────────────────────────────────
    total_tried = len(tried_sirens)
    phones_found = sum(source_phone.values())
    emails_found = sum(source_email.values())

    log.info(
        "enricher_batch_done",
        target=target_count,
        companies_tried=total_tried,
        contact_count=len(contacts),
        replaced=replaced_count,
        phone_hit_rate=f"{phones_found}/{len(contacts)} ({round(100 * phones_found / len(contacts))}%)" if contacts else "0/0",
        email_hit_rate=f"{emails_found}/{len(contacts)} ({round(100 * emails_found / len(contacts))}%)" if contacts else "0/0",
        sources=dict(source_counts),
        phones_by_source=dict(source_phone),
        emails_by_source=dict(source_email),
    )
    return contacts, replaced_count


def _is_residential_address(adresse: str) -> bool:
    """Detect if a SIRENE address looks residential (owner's home, PO box, etc.).

    Residential addresses should NOT be used in Maps queries because they
    point to the owner's home when they registered the business, not the
    actual business location. Maps would find the building, not the company.

    Patterns detected:
        CHEZ ...          — living at someone's address
        DOMICILE ...      — registered at home
        DOMICILIATION ... — registered via a domiciliation service
        BP / BOITE POSTALE — PO box
        APT / APPT / APPARTEMENT — apartment
        ETAGE / ESC / BAT — floor/staircase/building indicators
        RESIDENCE ...     — residential complex
    """
    if not adresse:
        return False
    upper = adresse.upper().strip()
    _RESIDENTIAL_PREFIXES = (
        "CHEZ ", "DOMICILE", "DOMICILIATION",
        "BP ", "BOITE POSTALE", "B.P.",
        "APT ", "APPT ", "APPARTEMENT ",
        "RESIDENCE ", "RES ",
    )
    _RESIDENTIAL_KEYWORDS = (
        " CHEZ ", " ETAGE ", " ESC ", " ESCALIER ",
        " BAT ", " BATIMENT ", " LOGE ",
        " APPT ", " APT ",
    )
    for prefix in _RESIDENTIAL_PREFIXES:
        if upper.startswith(prefix):
            return True
    for kw in _RESIDENTIAL_KEYWORDS:
        if kw in upper:
            return True
    return False


async def _enrich_one(
    company: Company,
    *,
    curl_client: CurlClient,
    maps_scraper: Any | None = None,
    query_domain: str = "",
) -> tuple[Contact | None, str, str, str | None, dict]:
    """Enrich a single company: Maps first, then website crawl.

    Returns:
        (Contact | None, source_label, match_confidence, maps_name, ml_result).
        match_confidence is 'high', 'low', or 'none'.
        ml_result is the mentions-légales extraction dict (may be empty).
        Returns (None, "none", "none", None, {}) when no data was found.
    """
    siren = company.siren
    denomination = company.denomination or ""

    # ── Pick the best name for Maps search ─────────────────────────────────
    # Priority: enseigne (commercial sign) > denomination (legal name).
    # Enseigne is what appears on business signs and Google Maps, e.g.
    # "Camping La Marende ****" vs the legal denomination "SCI LA MARENDE".
    raw_search_name = company.enseigne or denomination

    # ── Clean search name — strip legal form prefixes ──────────────────────
    # "SARL DUPONT TRANSPORT" → "DUPONT TRANSPORT"
    _LEGAL_PREFIXES = (
        "SARL", "SAS", "SASU", "EURL", "SA", "SCI", "SNC", "SELARL",
        "SELAFA", "SCP", "SEL", "GFA", "GIE", "EARL", "GAEC", "SCOP",
        "SEM", "SELAS", "SELURL", "SPL", "SPLA", "SICAV", "FCP",
        "SCCV", "SCPI", "SCM", "SEP", "SCEA",
        "INDIVISION",  # Legal property term — confuses Maps
    )
    maps_denomination = raw_search_name
    upper = raw_search_name.upper().strip()
    for prefix in _LEGAL_PREFIXES:
        if upper.startswith(prefix + " "):
            maps_denomination = raw_search_name[len(prefix):].strip()
            break

    # ── Skip Maps for legal forms that never have public presence ──────────
    # SCI (Société Civile Immobilière) = property management shells
    # GFA (Groupement Foncier Agricole) = agricultural land holding
    # SEP (Société en Participation) = silent partnerships
    _SKIP_MAPS_FORMS = {"SCI", "GFA", "SEP", "SCCV", "SCPI", "FCP", "SICAV"}
    skip_maps = False
    forme = (company.forme_juridique or "").upper()
    if any(f in upper[:10] for f in _SKIP_MAPS_FORMS) or any(f in forme for f in _SKIP_MAPS_FORMS):
        skip_maps = True
        log.info(
            "enricher.skip_maps_legal_form",
            siren=siren,
            denomination=denomination,
            forme_juridique=company.forme_juridique,
            reason="Legal form has no public Maps presence",
        )

    # ── Per-company start log ──────────────────────────────────────────────
    log.info(
        "enricher.company_start",
        siren=siren,
        denomination=denomination,
        enseigne=company.enseigne,
        maps_search_name=maps_denomination,
        ville=company.ville,
        departement=company.departement,
        code_postal=company.code_postal,
        is_individual=_is_individual_operator(denomination),
        skip_maps=skip_maps,
    )

    # ── Step 1: Google Maps (Playwright) ───────────────────────────────────
    # The primary source: phone, website URL, address, rating, reviews.
    maps_result: dict[str, Any] = {}
    maps_phone: str | None = None
    maps_website: str | None = None
    social: dict[str, str] = {}  # Initialised here so Maps social URLs can be stored
    match_confidence: str = "none"  # Default: no Maps data

    if maps_scraper is not None and not skip_maps:
        # Build a richer Maps query using cleaned denomination + city + postal.
        # "DUPONT TRANSPORT PARIS 75001" instead of "SARL DUPONT TRANSPORT PARIS"
        loc_parts = []
        if company.ville:
            loc_parts.append(company.ville)
        if company.code_postal:
            loc_parts.append(company.code_postal)
        search_location = " ".join(loc_parts) or company.departement or ""

        # Option D: join single-letter initials for better Maps search
        # "A T N" → "ATN" (Google finds "ATN Transport" better than "A T N")
        search_name = maps_denomination
        words = maps_denomination.split()
        alpha_words = [w for w in words if w.isalpha()]
        if alpha_words and all(len(w) == 1 for w in alpha_words):
            search_name = "".join(words)

        # ── 2-try Maps query strategy ─────────────────────────────────
        # Try 1 (broad): name + city + postal → catches moved businesses
        # Try 2 (fallback): name + full SIRENE address → pinpoints exact location
        try:
            maps_result = await maps_scraper.search(
                search_name,
                search_location,
                siren=siren,
                query_hint=query_domain,
            )
        except Exception as exc:
            log.debug(
                "enricher.maps_error",
                siren=siren,
                error=str(exc),
            )
            maps_result = {}

        # Try 2: If broad search returned nothing, retry with full address
        has_useful_data = maps_result.get("phone") or maps_result.get("website")
        if not has_useful_data and company.adresse and not _is_residential_address(company.adresse):
            addr_location = " ".join(filter(None, [
                company.adresse, company.ville, company.code_postal,
            ]))
            log.info(
                "enricher.maps_retry_with_address",
                siren=siren,
                denomination=search_name,
                location=addr_location,
                reason="Broad search returned no useful data, retrying with full address",
            )
            try:
                maps_result_2 = await maps_scraper.search(
                    search_name,
                    addr_location,
                    siren=siren,
                    query_hint=query_domain,
                )
                # Use retry result only if it has more data
                if maps_result_2.get("phone") or maps_result_2.get("website"):
                    maps_result = maps_result_2
                    log.info(
                        "enricher.maps_retry_success",
                        siren=siren,
                        has_phone=bool(maps_result_2.get("phone")),
                        has_website=bool(maps_result_2.get("website")),
                    )
            except Exception as exc:
                log.debug("enricher.maps_retry_error", siren=siren, error=str(exc))

        # ── Validate Maps match ───────────────────────────────────────
        match_confidence = _assess_match(maps_result, company)
        log.info(
            "enricher.maps_result",
            siren=siren,
            match_confidence=match_confidence,
            maps_name=maps_result.get("maps_name"),
            maps_phone=maps_result.get("phone"),
            maps_website=maps_result.get("website"),
            maps_address=maps_result.get("address"),
            maps_rating=maps_result.get("rating"),
        )

        # ── STRICT: If confidence is not HIGH, discard ALL Maps data ──────
        # "low" = Maps returned a DIFFERENT business (wrong name).
        # We MUST NOT use their phone, website, email, address, or anything.
        # This prevents "2 M FINANCE" from getting data from "2M Consulting"
        # or "12.5" from getting data from "Parking Douzep".
        if match_confidence != "high":
            if maps_result:
                log.warning(
                    "enricher.maps_mismatch_discarded",
                    siren=siren,
                    denomination=denomination,
                    maps_name=maps_result.get("maps_name"),
                    maps_address=maps_result.get("address"),
                    expected_ville=company.ville,
                    reason="Name mismatch — discarding ALL Maps data",
                )
            maps_result = {}  # Wipe everything

        maps_phone = _best_phone(
            [maps_result["phone"]] if maps_result.get("phone") else [],
            siren,
            departement=getattr(company, "departement", None),
        )
        # ── Intra-batch phone dedup ───────────────────────────────────
        # If this phone was already assigned to another company in this batch,
        # strip it. Prevents "2H TRANSPORTS" and "2H TRANSPORT" from sharing
        # identical contact data from the same Maps business.
        if maps_phone and maps_phone in seen_phones:
            log.warning(
                "enricher.duplicate_phone_stripped",
                siren=siren,
                denomination=denomination,
                phone=maps_phone,
                reason="Phone already assigned to another company in this batch",
            )
            maps_phone = None
            maps_result.pop("phone", None)
        elif maps_phone:
            seen_phones.add(maps_phone)
        raw_website = maps_result.get("website")
        if raw_website and not is_directory_url(raw_website):
            # ── Social URL reclassification ───────────────────────
            # Maps often returns Facebook/Instagram as the "website".
            # Detect and reclassify into the correct social_* field.
            social_col = classify_social_url(raw_website)
            if social_col:
                # It's a social platform (Facebook, Instagram, etc.)
                # Store in the correct social column, NOT as website
                social[social_col] = raw_website
                log.info(
                    "enricher.social_url_reclassified",
                    siren=siren,
                    url=raw_website,
                    column=social_col,
                )
            elif social_col is None:
                # WhatsApp/YouTube — no contact value, discard
                log.debug(
                    "enricher.social_url_discarded",
                    siren=siren,
                    url=raw_website,
                    reason="No contact value (WhatsApp/YouTube)",
                )
            else:
                # Regular website — normalise and use for crawl
                try:
                    parsed = urlparse(raw_website)
                    if parsed.scheme and parsed.netloc:
                        maps_website = f"{parsed.scheme}://{parsed.netloc}"
                    else:
                        maps_website = raw_website
                except ValueError:
                    maps_website = raw_website
    else:
        log.warning(
            "enricher.no_maps_scraper",
            siren=siren,
            reason="Maps scraper not available — primary data source missing",
        )

    # ── Step 2: Website crawl ─────────────────────────────────────────
    # Primary: curl_cffi (1 retry only — dead sites shouldn't waste time).
    # Crawls homepage AND /contact page for better email extraction.
    # Fallback: Playwright only if curl got a response but found nothing
    #           (NOT for DNS/SSL failures — those won't work in Playwright either).
    crawl_result: dict[str, Any] = {}
    best_email: str | None = None
    # social is initialised above (before Maps) so Maps social URLs can be stored

    if maps_website:
        from fortress.matching.contacts import (
            extract_emails as _extract_emails,
            extract_phones as _extract_phones,
            extract_social_links as _extract_social,
            extract_mentions_legales as _extract_ml,
        )
        from urllib.parse import urlparse as _urlparse

        # ── Primary: curl_cffi (homepage + /contact page) ────────────
        curl_success = False
        curl_infra_failure = False  # DNS/SSL = don't bother with Playwright
        all_emails: list[str] = []
        all_phones: list[str] = []
        all_social: dict[str, str] = {}

        # Ensure clean root URL
        try:
            _parsed = _urlparse(maps_website)
            root_url = f"{_parsed.scheme}://{_parsed.netloc}"
        except Exception:
            root_url = maps_website

        # Crawl homepage + French contact/legal pages (most emails are on these)
        pages_to_crawl = [
            root_url,
            f"{root_url}/contact",
            f"{root_url}/mentions-legales",
            f"{root_url}/nous-contacter",
            f"{root_url}/a-propos",
        ]
        pages_visited = 0
        mentions_legales_html: str | None = None  # Capture for structured parsing

        curl_crawl = CurlClient(timeout=8.0, max_retries=1, delay_min=0.3, delay_max=0.5, delay_jitter=0.0)
        try:
            for page_url in pages_to_crawl:
                try:
                    resp = await curl_crawl.get(page_url)
                    if resp.status_code == 200 and len(resp.text) > 500:
                        all_emails.extend(_extract_emails(resp.text))
                        all_phones.extend(_extract_phones(resp.text))
                        page_social = _extract_social(resp.text)
                        all_social.update(page_social)
                        pages_visited += 1
                        # Capture mentions-légales HTML for structured parsing
                        if "mentions-legales" in page_url or "mentions_legales" in page_url:
                            mentions_legales_html = resp.text
                except CurlClientError as exc:
                    # DNS or SSL failure — Playwright won't help either
                    err_str = str(exc).lower()
                    if "resolve" in err_str or "ssl" in err_str or "certificate" in err_str:
                        curl_infra_failure = True
                        log.debug(
                            "enricher.curl_infra_failure",
                            siren=siren, url=page_url, error=str(exc),
                        )
                        break  # Don't try /contact if homepage DNS failed
                except Exception as exc:
                    log.debug(
                        "enricher.curl_crawl_error",
                        siren=siren, url=page_url, error=str(exc),
                    )
        finally:
            await curl_crawl.close()

        if pages_visited > 0:
            # Deduplicate
            crawl_result = {
                "emails": list(set(all_emails)),
                "phones": list(set(all_phones)),
                "social": all_social,
                "pages_visited": pages_visited,
            }
            curl_success = True
            log.debug(
                "enricher.curl_crawl_ok",
                siren=siren, url=maps_website,
                emails=len(crawl_result["emails"]),
                phones=len(crawl_result["phones"]),
                pages=pages_visited,
            )

        # ── Playwright fallback for bot-blocked sites (403/Cloudflare) ────
        # If curl got zero usable pages AND it's not a DNS/SSL issue,
        # try the already-running stealth Playwright to render the page.
        # This is "free" — the browser is already warm for Google Maps.
        if not curl_success and not curl_infra_failure and maps_scraper is not None:
            try:
                log.info(
                    "enricher.playwright_crawl_fallback",
                    siren=siren, url=root_url,
                )
                async with maps_scraper._lock:
                    page = maps_scraper._page
                    if page is not None:
                        await page.goto(
                            root_url,
                            wait_until="domcontentloaded",
                            timeout=10000,
                        )
                        html = await page.content()
                        if html and len(html) > 500:
                            all_emails.extend(_extract_emails(html))
                            all_phones.extend(_extract_phones(html))
                            page_social = _extract_social(html)
                            all_social.update(page_social)
                            pages_visited += 1
                            crawl_result = {
                                "emails": list(set(all_emails)),
                                "phones": list(set(all_phones)),
                                "social": all_social,
                                "pages_visited": pages_visited,
                            }
                            curl_success = True
                            log.info(
                                "enricher.playwright_crawl_ok",
                                siren=siren, url=root_url,
                                emails=len(crawl_result["emails"]),
                            )
                        # Navigate back to Maps so the next search works
                        await page.goto(
                            "https://www.google.com/maps?hl=fr",
                            wait_until="domcontentloaded",
                            timeout=10000,
                        )
            except Exception as pw_exc:
                log.debug(
                    "enricher.playwright_crawl_error",
                    siren=siren, url=root_url, error=str(pw_exc),
                )

        raw_emails: list[str] = crawl_result.get("emails", [])
        clean_emails = [e for e in raw_emails if not is_junk_email(e)]
        best_email = _best_email(clean_emails, maps_website, siren, company_name=denomination)
        crawl_social = crawl_result.get("social", {})
        social.update(crawl_social)  # Merge crawl social into Maps social

        # ── Step 2b: Mentions Légales structured parsing ──────────────────
        # If we fetched the mentions-legales page, run the structured parser
        # to extract director info, employee count, and cross-validate SIREN.
        ml_result: dict = {}
        if mentions_legales_html:
            try:
                _parsed_domain = _urlparse(maps_website)
                website_domain = _parsed_domain.netloc.lower().lstrip("www.")
            except Exception:
                website_domain = None

            ml_result = _extract_ml(
                mentions_legales_html,
                company_siren=siren,
                website_domain=website_domain,
            )
            log.info(
                "enricher.mentions_legales_parsed",
                siren=siren,
                director_name=ml_result.get("director_name"),
                director_email=ml_result.get("director_email"),
                effectif=ml_result.get("effectif"),
                siren_match=ml_result.get("siren_match"),
            )

            # Use director email if we haven't found one yet
            if not best_email and ml_result.get("director_email"):
                best_email = ml_result["director_email"]
                log.info(
                    "enricher.email_from_mentions_legales",
                    siren=siren,
                    email=best_email,
                )

        # Crawl may also find phone numbers — merge with Maps phone
        if not maps_phone:
            crawl_phones = crawl_result.get("phones", [])
            maps_phone = _best_phone(crawl_phones, siren, departement=getattr(company, "departement", None))

    # ── Layer 3: Website/email cross-validation ─────────────────────────
    # If name matching (Layer 2) said "low" confidence, check if the website
    # domain or email domain contains the business name. If so, it confirms
    # we have the right company — upgrade confidence to "high".
    if match_confidence == "low":
        domain_match = False
        if maps_website and _domain_confirms_name(maps_website, denomination):
            domain_match = True
            log.info("enricher.domain_confirms_name",
                     siren=siren, website=maps_website, denomination=denomination)
        elif best_email and _domain_confirms_name(best_email, denomination):
            domain_match = True
            log.info("enricher.email_confirms_name",
                     siren=siren, email=best_email, denomination=denomination)

        if domain_match:
            match_confidence = "high"
            # Restore phone if it was discarded by low-confidence filter
            if not maps_phone and maps_result.get("phone"):
                maps_phone = maps_result["phone"]
                log.debug("enricher.phone_restored_by_domain",
                          siren=siren, phone=maps_phone)

    # ── Build final contact ────────────────────────────────────────────────
    maps_rating_raw = maps_result.get("rating")
    maps_rating = Decimal(str(maps_rating_raw)) if maps_rating_raw is not None else None

    # Only create a Contact if we have at least one useful field
    has_data = any([
        maps_phone, best_email, maps_website, maps_rating,
        maps_result.get("address"), maps_result.get("maps_url"),
    ])

    if not has_data:
        log.debug("enricher.no_data_found", siren=siren, denomination=denomination)
        return (None, "none", match_confidence if maps_scraper else "none", maps_result.get("maps_name"), ml_result if maps_website else {})

    # Source attribution: Maps when phone came from Maps, website_crawl when email came from crawl
    source = ContactSource.GOOGLE_MAPS
    source_label = "google_maps"
    if best_email and not maps_result.get("phone"):
        source = ContactSource.WEBSITE_CRAWL
        source_label = "web_search"

    log.info(
        "enricher.company_enriched",
        siren=siren,
        has_phone=bool(maps_phone),
        has_email=bool(best_email),
        has_website=bool(maps_website),
        has_rating=bool(maps_rating),
        source=source_label,
    )

    return (
        Contact(
            siren=siren,
            phone=maps_phone,
            email=best_email,
            website=maps_website,
            address=maps_result.get("address"),
            source=source,
            rating=maps_rating,
            review_count=maps_result.get("review_count"),
            maps_url=maps_result.get("maps_url"),
            social_linkedin=social.get("linkedin"),
            social_facebook=social.get("facebook"),
            social_twitter=social.get("twitter"),
            social_instagram=social.get("instagram"),
            social_tiktok=social.get("tiktok"),
            social_whatsapp=social.get("whatsapp"),
            social_youtube=social.get("youtube"),
            siren_match=ml_result.get("siren_match") if ml_result else None,
            match_confidence=match_confidence if maps_scraper else None,
            collected_at=datetime.now(tz=timezone.utc),
        ),
        source_label,
        match_confidence if maps_scraper else "none",
        maps_result.get("maps_name"),
        ml_result if maps_website else {},
    )


# ---------------------------------------------------------------------------
# Backfill helper
# ---------------------------------------------------------------------------

async def _log_enrichment(
    pool: Any,
    batch_id: str,
    company: Any,
    outcome: str,
    maps_method: str | None,
    maps_phone: str | None,
    maps_website: str | None,
    maps_name: str | None,
    emails_found: int,
    replace_reason: str | None,
    time_ms: int,
) -> None:
    """Insert one row into enrichment_log (admin diagnostic, non-blocking)."""
    try:
        async with pool.connection() as conn:
            await conn.execute(
                """INSERT INTO enrichment_log
                   (batch_id, siren, denomination, outcome, maps_method,
                    maps_phone, maps_website, maps_name, crawl_method,
                    emails_found, replace_reason, time_ms)
                   VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (batch_id, company.siren, company.denomination, outcome,
                 maps_method, maps_phone, maps_website, maps_name, None,
                 emails_found, replace_reason, time_ms),
            )
            await conn.commit()
    except Exception:
        pass  # Non-blocking — never crash the pipeline for logging


async def _fetch_replacement_companies(
    pool: Any,
    tried_sirens: set[str],
    reference_company: Any | None,
    count: int,
) -> list[Company]:
    """Fetch replacement companies from the DB for backfill.

    Finds companies matching the same NAF code prefix + department as the
    reference company, excluding already-tried SIRENs and [ND] names.

    Args:
        pool:              Async psycopg3 connection pool.
        tried_sirens:      Set of SIRENs already attempted.
        reference_company: A Company from the original batch (for NAF/dept).
        count:             Number of replacements to fetch.

    Returns:
        list[Company] — fresh candidates from the DB.
    """
    if reference_company is None or count <= 0:
        return []

    naf = reference_company.naf_code
    dept = reference_company.departement

    if not naf or not dept:
        log.debug(
            "enricher.backfill_skip",
            reason="no NAF or department on reference company",
        )
        return []

    # Use the NAF prefix (first 2 digits) for broad matching
    naf_prefix = naf[:2] + "%"

    try:
        async with pool.connection() as conn:
            import psycopg.rows

            async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                # Fast random selection: use a counted offset instead of
                # ORDER BY RANDOM() which sorts the entire filtered heap.
                # Step 1: get approximate count of candidates
                await cur.execute(
                    """
                    SELECT COUNT(*)
                    FROM companies
                    WHERE departement = %s
                      AND naf_code LIKE %s
                      AND statut = 'A'
                      AND denomination != '[ND]'
                      AND denomination IS NOT NULL
                      AND siren != ALL(%s)
                    """,
                    (dept, naf_prefix, list(tried_sirens)),
                )
                count_row = await cur.fetchone()
                pool_size = count_row["count"] if count_row else 0

                if pool_size == 0:
                    rows = []
                else:
                    # Step 2: pick a random offset within the pool
                    await cur.execute(
                        """
                        SELECT siren, denomination, naf_code, departement,
                               code_postal, ville, adresse, statut, enseigne
                        FROM companies
                        WHERE departement = %s
                          AND naf_code LIKE %s
                          AND statut = 'A'
                          AND denomination != '[ND]'
                          AND denomination IS NOT NULL
                          AND siren != ALL(%s)
                        ORDER BY
                            CASE WHEN enseigne IS NOT NULL AND enseigne != '' THEN 0 ELSE 1 END,
                            CASE WHEN forme_juridique != '1000' THEN 0 ELSE 1 END,
                            CASE WHEN adresse IS NOT NULL AND adresse != '' THEN 0 ELSE 1 END,
                            RANDOM()
                        LIMIT %s
                        """,
                        (dept, naf_prefix, list(tried_sirens), count),
                    )
                    rows = await cur.fetchall()

        return [
            Company(
                siren=row["siren"],
                denomination=row["denomination"],
                enseigne=row.get("enseigne"),
                naf_code=row["naf_code"],
                departement=row["departement"],
                code_postal=row.get("code_postal"),
                ville=row.get("ville"),
                adresse=row.get("adresse"),
            )
            for row in rows
        ]
    except Exception as exc:
        log.warning("enricher.backfill_query_error", error=str(exc))
        return []

