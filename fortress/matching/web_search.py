"""Web search — find official website URL for a company via Google.

Uses CurlClient (Chrome TLS impersonation) — no browser needed.
Google FR is the sole search engine.

When the primary query ("NAME" "CITY" site officiel) finds nothing, three
fallback queries are tried in order:
  1. "NAME" telephone CITY    — finds directory pages with phone data
  2. "NAME" contact DEPT      — catches regional results
  3. "SIREN" contact          — last resort using the SIREN number

Fallback queries also accept directory sites (societe.com, verif.com) since
those pages often contain phone numbers not found elsewhere.  The enricher is
responsible for NOT storing directory URLs as the company website field.

Rate limiting is enforced via CurlClient's built-in delay
(settings.delay_between_requests_min / max).

API quota safety: tests must mock CurlClient.get() — never call real search engines.
"""

from __future__ import annotations

import re
from urllib.parse import quote_plus, urlparse

import structlog

from fortress.matching.contacts import _is_valid_french_phone, extract_phones
from fortress.scraping.http import CurlClient, CurlClientError

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Individual operator detection
# ---------------------------------------------------------------------------

# INSEE legal form codes that always indicate an individual person (not a company)
# 1000 = Entrepreneur individuel, 1100 = EIRL
_INDIVIDUAL_FORME_JURIDIQUE_CODES: frozenset[str] = frozenset({"1000", "1100"})

# Known legal entity suffixes — presence means it's a company, not an individual
_LEGAL_SUFFIXES: frozenset[str] = frozenset({
    "SARL", "SAS", "SASU", "EURL", "SA", "SCI", "EARL", "GAEC",
    "SCEA", "SCEV", "GFA", "SICA", "SCL", "GIE", "ASSOCIATION",
    "COOP", "SCOP", "SNC", "SELARL", "SELARLU", "SELAFA", "SELCA",
    "SCP", "SCM", "SELAS", "GFV", "SCIC", "CAE", "EIRL",
})

# Business activity words — presence means it's a company name, not a person name
_BUSINESS_INDICATORS: frozenset[str] = frozenset({
    "DOMAINE", "DOMAINES", "FERME", "FERMES", "EXPLOITATION", "EXPLOITATIONS",
    "VIGNOBLES", "VIGNOBLE", "CAVE", "CAVES", "COOPERATIVE", "COOPERATIVES",
    "MAISON", "ETS", "ETABLISSEMENTS", "GROUPE", "GROUPEMENT",
    "BOULANGERIE", "RESTAURANT", "CAFE", "HOTEL", "CHATEAU", "CHATEAUX",
    "JARDINS", "FLEURS", "PLANTES", "ELEVAGE", "ELEVAGES",
    "CULTURES", "CULTURE", "JARDINAGE", "PEPINIERE", "PEPINIERES",
    "VERGER", "VERGERS", "APICULTURE", "MAGASIN", "BOUTIQUE",
    "EPICERIE", "FROMAGERIE", "CHARCUTERIE", "BOUCHERIE",
    "DISTILLERIE", "BRASSERIE", "LAITERIE",
    "AGRI", "AGRO", "AGRICOLE", "AGRICOLES",
    "CUMA", "SCEA", "GAEC",
    # Transport / logistics sector
    "TRANSPORT", "TRANSPORTS", "LOGISTIQUE", "FRET", "MESSAGERIE",
    "AEROPORT", "AEROPORTS", "AVIATION", "AIR", "AIRBUS",
    "SERVICES", "SERVICE", "SOCIETE", "ENTREPRISE", "ENTREPRISES",
    "INDUSTRIE", "INDUSTRIES", "COMMERCE", "COMMERCES",
    "TAXI", "TAXIS", "AMBULANCE", "AMBULANCES", "SANTE", "MEDICAL",
    "MARITIME", "REMORQUAGE", "STOCKAGE", "DEPANNAGE", "ASSISTANCE",
    "DISTRIBUTION", "CONCEPT", "TRUCK", "RADIO",
    # English-language business names (airports, handling, logistics)
    "LOGISTICS", "HANDLING", "GLOBAL", "PARTNER", "PARTNERS",
    "AIRPORT", "AIRPORTS", "CARGO", "EXPRESS", "SOLUTIONS",
    "CONSULTING", "MANAGEMENT", "INTERNATIONAL", "SERVICES",
    "AUTO", "SECOURS", "BOX", "CONNEXION",
    # More French business indicators
    "AFFRETEMENT", "AFFRETEMENTS", "GESTION", "CONSEIL",
    "LOCATION", "LOCATIONS", "HOLDING", "INVESTISSEMENT",
    # Aviation sector
    "AVIA", "AVIAPARTNER", "AVIATEK", "AVIATRANS",
    # Tech / digital
    "TECH", "DIGITAL", "DATA", "CLOUD", "SMART", "NET",
    # Regional / sector brands
    "OCCITANIE", "OCCITAN",
    # Geographic/scale descriptors — no person uses these as a surname
    "PROVINCE", "PROVINCES", "REGION", "REGIONS", "NATIONAL", "NATIONALE",
    "FRANCE", "FRENCH", "EUROPE", "EUROPEEN", "EUROPEENNE",
    "NORD", "SUD", "EST", "OUEST", "CENTRE",
    # Known logistics/aviation brand names (single distinctive word)
    "ALYZIA", "GEODIS", "BOLLORÉ", "BOLORE", "KUEHNE",
})


def _is_individual_operator(denomination: str) -> bool:
    """Return True if denomination looks like a person's name (no legal entity suffix).

    Used to skip web search for individual entrepreneurs who have no corporate website.

    Rules:
      1. Must be 1–3 words (longer = likely a business name with prepositions)
      2. No known legal entity suffix (SARL, SAS, EARL, etc.)
      3. No known business activity indicator word
      4. No digits in any word (business names often contain numbers)

    Examples:
      "ABDELMAJID SOUIDI"   → True  (individual)
      "AGNES DE VOLONTAT"   → True  (individual with particle)
      "AGRI BRIAL"          → False (AGRI = business indicator)
      "DOMAINE DUPONT SARL" → False (has SARL suffix)
      "EARL DES VIGNES"     → False (EARL = legal suffix)
      "AGRI 66"             → False (has digit)
    """
    words = denomination.upper().strip().split()

    if not words:
        return False

    # Single word: almost always a brand name (e.g. ALYZIA, GEODIS, ATABOX),
    # not a person's name (individuals use at least firstname + lastname).
    if len(words) == 1:
        return False

    # More than 3 words: likely a business name with prepositions/articles
    if len(words) > 3:
        return False

    # Any word is a known legal suffix → it's a registered entity
    if any(w in _LEGAL_SUFFIXES for w in words):
        return False

    # Any word is a business activity indicator → it's a business
    if any(w in _BUSINESS_INDICATORS for w in words):
        return False

    # Any word starts with a known business prefix (AVIA→AVIAPARTNER, AGRI→AGRIBRIAL)
    _BUSINESS_PREFIXES = ("AVIA", "AGRI", "AGRO", "AERO", "AUTO", "MULTI",
                          "EURO", "INTER", "TRANS", "DIGI", "TELE", "STOCK")
    if any(w.startswith(pfx) for w in words for pfx in _BUSINESS_PREFIXES):
        return False

    # French business nominalization: words ending in -AGE are activity nouns,
    # never person names (STOCKAGE, REMORQUAGE, CAMIONNAGE, GARDIENNAGE, …)
    if any(w.endswith("AGE") and len(w) > 5 for w in words):
        return False

    # Any word contains a digit → likely a business code or trade name
    if any(any(c.isdigit() for c in w) for w in words):
        return False

    # Any word contains punctuation/special chars (e.g. A.R.C, A.O.C) → business code
    if any("." in w or "-" in w for w in words):
        return False

    # 2-3 words with no business indicators, no digits, no punctuation
    # → treat as individual operator (person name)
    return True

# ---------------------------------------------------------------------------
# Directory sites — useful for phone/email data but NOT valid as company website
# ---------------------------------------------------------------------------

# These sites aggregate company data and often have phone numbers.
# They are allowed through in fallback searches so the crawler can extract
# contact data from them, but the enricher must NOT store these URLs as the
# company's `website` field.
DIRECTORY_DOMAINS: frozenset[str] = frozenset({
    "societe.com",
    "verif.com",
    "manageo.fr",
    "infogreffe.fr",
    "annuaire-entreprises.data.gouv.fr",
    "societe.ninja",
    "europages.fr",
})


def is_directory_url(url: str) -> bool:
    """Return True if the URL belongs to a company directory site.

    Directory sites may contain useful phone/email data but should never be
    stored as the company's official website.
    """
    try:
        netloc = urlparse(url).netloc.lower().lstrip("www.")
    except ValueError:
        return False
    for d in DIRECTORY_DOMAINS:
        if netloc == d or netloc.endswith("." + d):
            return True
    return False


# ---------------------------------------------------------------------------
# Social media platform detection — reclassify Maps "website" → social field
# ---------------------------------------------------------------------------

# Maps often returns a social page as the business "website".
# These should be stored in the correct social_* column, not as website.
# Value = contacts column name, or None to discard entirely.
SOCIAL_DOMAINS: dict[str, str | None] = {
    "facebook.com": "social_facebook",
    "fb.com": "social_facebook",
    "fb.me": "social_facebook",
    "instagram.com": "social_instagram",
    "twitter.com": "social_twitter",
    "x.com": "social_twitter",
    "linkedin.com": "social_linkedin",
    "tiktok.com": "social_tiktok",
    "wa.me": "social_whatsapp",
    "api.whatsapp.com": "social_whatsapp",
    "whatsapp.com": "social_whatsapp",
    "youtube.com": "social_youtube",
    "youtu.be": "social_youtube",
}


def classify_social_url(url: str) -> str | None:
    """Classify a URL as a social media platform.

    Returns:
        Column name (e.g. "social_facebook") if it's a social platform.
        None if it's a social platform with no contact value (WhatsApp, YouTube).
        Empty string "" if it's NOT a social platform (treat as regular website).

    Usage:
        result = classify_social_url(maps_url)
        if result:          # e.g. "social_facebook" — store in that column
        elif result is None: # WhatsApp/YouTube — discard
        else:               # "" — regular website, proceed with crawl
    """
    try:
        netloc = urlparse(url).netloc.lower().lstrip("www.")
    except ValueError:
        return ""
    for domain, column in SOCIAL_DOMAINS.items():
        if netloc == domain or netloc.endswith("." + domain):
            return column
    return ""


# Minimum character overlap required between domain and company name
# to consider a URL match valid (prevents random unrelated domains).
_MIN_DOMAIN_OVERLAP = 3

# Google search URL (HTML endpoint, no JS required)
_GOOGLE_SEARCH_URL = "https://www.google.fr/search"



# Regex to extract URLs from Google search result snippets
_GOOGLE_RESULT_RE = re.compile(
    # Capture the full href — either a /url?q= redirect or a direct https:// link.
    # The original pattern (/url\?q=|https?://...) was broken: the alternation
    # captured only the literal prefix "/url?q=" instead of the full redirect URL.
    r'<a[^>]+href="(/url\?q=[^"]*|https?://[^"]+)"',
    re.IGNORECASE,
)

# Regex to extract Google's /url?q= redirect targets
_GOOGLE_REDIRECT_RE = re.compile(r"/url\?q=(https?://[^&]+)")

# Prefer .fr domains for French companies
_PREFER_FR = True


async def find_website_url(
    client: CurlClient,
    denomination: str,
    city: str,
    *,
    siren: str | None = None,
    dept: str | None = None,
) -> str | None:
    """Search for the official website of a French company.

    Tries the primary Google query first.  If nothing is found,
    runs up to 3 fallback queries that use different search terms and also
    accept directory sites (societe.com, verif.com) as results.

    Args:
        client:       CurlClient instance (Chrome TLS impersonation).
        denomination: Legal company name (e.g. "DOMAINE DUPONT SARL").
        city:         City name (e.g. "Thuir").
        siren:        Optional SIREN — used in fallback query and logging.
        dept:         Optional department code (e.g. "31") — used in fallback query.

    Returns:
        URL string if found, or None.  May return a directory site URL for
        fallbacks; the caller (enricher) should use is_directory_url() to
        decide whether to store it as the company website.
    """
    primary_query = f'"{denomination}" "{city}" site officiel'
    log.debug("web_search.google", denomination=denomination, city=city, siren=siren)

    url = await _try_google(client, primary_query)
    if url and _is_plausible_match(url, denomination):
        log.info("web_search.found_google", url=url, siren=siren)
        return url

    # ── Fallback queries ────────────────────────────────────────────────────
    # When the primary query finds nothing, try alternative terms.
    # These are lower-precision but can surface directory pages (societe.com,
    # verif.com) that carry phone numbers even if no corporate website exists.
    fallback_queries: list[str] = []

    # Fallback 1: telephone + city (high chance of finding directory listing)
    if city:
        fallback_queries.append(f'"{denomination}" telephone {city}')

    # Fallback 2: contact + department (useful when city is not prominent)
    if dept:
        fallback_queries.append(f'"{denomination}" contact {dept}')
    elif city:
        fallback_queries.append(f'"{denomination}" contact {city}')

    # Fallback 3: SIREN + contact (unique identifier always yields results)
    if siren:
        fallback_queries.append(f'"{siren}" contact')

    for fallback_query in fallback_queries:
        log.debug(
            "web_search.fallback_query",
            query=fallback_query,
            siren=siren,
        )
        # For fallbacks, allow directory sites through — the enricher will
        # decide whether to use the URL as the company website or just for
        # contact data extraction.
        url = await _try_google(client, fallback_query, allow_directories=True)
        if url and (_is_plausible_match(url, denomination) or is_directory_url(url)):
            log.info("web_search.found_google_fallback", url=url, siren=siren, query=fallback_query)
            return url

    log.debug("web_search.not_found", denomination=denomination, siren=siren)
    return None


# ---------------------------------------------------------------------------
# Individual operator directory phone search
# ---------------------------------------------------------------------------

# Public French telephone directories known to list sole traders with phone numbers.
# Used by find_phone_from_directories() to decide which pages to fetch when a
# search snippet contains no phone number.
#
# ⚠️ DOMAIN BYPASS NOTE: Some of these domains (118712.fr, horaires.lefigaro.fr)
# are listed in _is_useful_url()'s always_blocked set, which is correct: they
# should never appear as a company's official website. However,
# find_phone_from_directories() does NOT call _is_useful_url() — it has its OWN
# whitelist check against this set. This intentional bypass is how the two
# concerns ("not a company website" vs "valid phone directory") coexist.
# If you refactor domain checking, preserve this separation.
_INDIVIDUAL_DIRECTORY_DOMAINS: frozenset[str] = frozenset({
    "118712.fr",
    "horaires.lefigaro.fr",
    "local.fr",
    "cylex.fr",
    "118000.fr",
    "lacartedesmetiers.fr",
    "telephoneannuaire.fr",  # Aggregate phone directory, snippets often contain phones
})


async def find_phone_from_directories(
    client: CurlClient,
    denomination: str,
    city: str,
    *,
    siren: str | None = None,
    dept: str | None = None,
) -> list[str]:
    """Search French phone directories for an individual operator's phone number.

    Used when _is_individual_operator() returns True — sole traders don't have
    corporate websites but are often listed in public telephone directories.

    Strategy:
      1. Extract last name (last word of denomination).
      2. Build 2-3 queries: '"LASTNAME" "CITY" telephone', etc.
      3. Query Google FR and parse HTML snippets for phone numbers.
      4. Snippet-first: extract phones from search result text directly.
      5. For results whose URL belongs to _INDIVIDUAL_DIRECTORY_DOMAINS and whose
         snippet had no phone, fetch the page and extract from HTML.
      6. Stop after the first query that yields phones.

    Args:
        client:       CurlClient instance (Chrome TLS impersonation).
        denomination: Full legal denomination (e.g. "ABDEL KHAMASSI").
        city:         City name (e.g. "Perpignan").
        siren:        Optional SIREN — used for logging only.
        dept:         Optional department code (e.g. "66") — used in query 2.

    Returns:
        Deduplicated list of valid French phone numbers, empty list if nothing found.
    """
    words = denomination.strip().split()
    last_name = words[-1] if words else denomination.strip()
    full_name = denomination.strip()

    # Build 2–3 search queries in order of specificity.
    queries: list[str] = []
    if city:
        queries.append(f'"{ last_name}" "{city}" telephone')
    if dept:
        queries.append(f'"{full_name}" "{dept}" professionnel')
    elif city:
        queries.append(f'"{full_name}" contact {city}')
    # Third fallback: last name + dept without quotes on dept
    if dept and len(queries) < 3:
        queries.append(f'"{last_name}" agriculteur {dept}')

    found_phones: list[str] = []
    fetched_urls: set[str] = set()

    # Regex to parse Google result snippets for phone-bearing content.
    # Google wraps each result in a div; we extract href links and nearby text.
    _google_result_block_re = re.compile(
        r'<a[^>]+href="(/url\?q=[^"]*|https?://[^"]+)"[^>]*>(.*?)</a>',
        re.DOTALL | re.IGNORECASE,
    )
    _strip_tags = re.compile(r"<[^>]+>")

    for query in queries[:3]:
        log.debug(
            "web_search.individual_directory_search",
            query=query,
            siren=siren,
        )

        # Google FR search with extra results for directory coverage.
        params = {
            "q": query,
            "hl": "fr",
            "gl": "fr",
            "num": "10",
        }
        search_url = _GOOGLE_SEARCH_URL + "?" + "&".join(
            f"{k}={quote_plus(v)}" for k, v in params.items()
        )
        try:
            response = await client.get(search_url)
        except CurlClientError as exc:
            log.debug(
                "web_search.individual_google_unavailable",
                error=str(exc),
                siren=siren,
            )
            continue

        if response.status_code in (429, 403):
            log.debug(
                "web_search.individual_google_blocked",
                status=response.status_code,
                siren=siren,
            )
            continue

        if response.status_code != 200:
            continue

        # Extract phone numbers from Google result snippets.
        html = response.text
        snippet_text = _strip_tags.sub(" ", html)
        snippet_phones = [
            p for p in extract_phones(snippet_text) if _is_valid_french_phone(p)
        ]
        if snippet_phones:
            log.info(
                "web_search.individual_snippet_phone_found",
                phones=snippet_phones[:5],
                siren=siren,
            )
            found_phones.extend(snippet_phones)

        # Also fetch whitelisted directory pages from the results for deeper extraction.
        for link_match in _GOOGLE_RESULT_RE.finditer(html):
            href = link_match.group(1)
            redirect = _GOOGLE_REDIRECT_RE.match(href)
            result_url = redirect.group(1) if redirect else (href if href.startswith("http") else None)
            if not result_url or result_url in fetched_urls:
                continue

            try:
                result_netloc = urlparse(result_url).netloc.lower().lstrip("www.")
            except ValueError:
                continue

            is_individual_dir = any(
                result_netloc == d or result_netloc.endswith("." + d)
                for d in _INDIVIDUAL_DIRECTORY_DOMAINS
            )
            if not is_individual_dir:
                continue

            fetched_urls.add(result_url)
            try:
                page_response = await client.get(result_url)
            except CurlClientError as exc:
                log.debug(
                    "web_search.individual_page_fetch_error",
                    url=result_url,
                    error=str(exc),
                )
                continue

            if page_response.status_code != 200:
                continue

            page_phones = [
                p for p in extract_phones(page_response.text)
                if _is_valid_french_phone(p)
            ]
            if page_phones:
                log.info(
                    "web_search.individual_page_phone_found",
                    phones=page_phones,
                    siren=siren,
                    url=result_url,
                )
                found_phones.extend(page_phones)

        if found_phones:
            # First query that yielded phones — no need to continue.
            break

    if not found_phones:
        log.debug(
            "web_search.individual_not_found",
            denomination=denomination,
            siren=siren,
        )

    # Deduplicate while preserving insertion order.
    seen: set[str] = set()
    result_phones: list[str] = []
    for p in found_phones:
        if p not in seen:
            seen.add(p)
            result_phones.append(p)

    return result_phones


# ---------------------------------------------------------------------------
# Internal search engine implementations
# ---------------------------------------------------------------------------


async def _try_google(
    client: CurlClient,
    query: str,
    *,
    allow_directories: bool = False,
) -> str | None:
    """Attempt to find a URL via Google FR search.

    Args:
        allow_directories: If True, directory sites are allowed through.
    """
    params = {
        "q": query,
        "hl": "fr",
        "gl": "fr",
        "num": "5",
    }
    url = _GOOGLE_SEARCH_URL + "?" + "&".join(
        f"{k}={quote_plus(v)}" for k, v in params.items()
    )

    try:
        response = await client.get(url)
    except CurlClientError as exc:
        log.warning("web_search.google_error", error=str(exc))
        return None

    if response.status_code in (429, 403):
        log.warning("web_search.google_blocked", status=response.status_code)
        return None

    if response.status_code != 200:
        return None

    return _extract_google_url(response.text, allow_directories=allow_directories)





# ---------------------------------------------------------------------------
# URL extraction helpers
# ---------------------------------------------------------------------------


def _extract_google_url(html: str, *, allow_directories: bool = False) -> str | None:
    """Extract the first organic result URL from a Google HTML response.

    Args:
        allow_directories: If True, directory sites are allowed through.
    """
    for match in _GOOGLE_RESULT_RE.finditer(html):
        href = match.group(1)

        # Handle Google's redirect format /url?q=https://...
        redirect = _GOOGLE_REDIRECT_RE.match(href)
        if redirect:
            url = redirect.group(1)
        elif href.startswith("http"):
            url = href
        else:
            continue

        url = _clean_url(url)
        if _is_useful_url(url, allow_directories=allow_directories):
            return url

    return None




# ---------------------------------------------------------------------------
# URL validation helpers
# ---------------------------------------------------------------------------


def _is_useful_url(url: str, *, allow_directories: bool = False) -> bool:
    """Return True if the URL looks like a real corporate website.

    Filters out: Google properties, social media, aggregators, gov sites.

    Args:
        allow_directories: If True, directory sites (societe.com, verif.com…)
            are not blocked.  Use for fallback queries where the goal is to
            extract phone/email data rather than find the company website.
    """
    try:
        parsed = urlparse(url)
    except ValueError:
        return False

    if not parsed.scheme or not parsed.netloc:
        return False

    domain = parsed.netloc.lower().lstrip("www.")

    # Always-blocked: search engines, social media, news, job boards.
    always_blocked: set[str] = {
        # Search engines & trackers
        "google.fr",
        "google.com",
        "maps.google.fr",
        "maps.google.com",
        "duckduckgo.com",
        "bing.com",
        # Social media
        "facebook.com",
        "instagram.com",
        "linkedin.com",
        "twitter.com",
        "x.com",
        "youtube.com",
        "tiktok.com",
        # Encyclopedias
        "wikipedia.org",
        "wikidata.org",
        # Gov portals (not company sites)
        "annuaire.gouv.fr",
        "data.gouv.fr",
        "bodacc.fr",
        # News sites
        "lagazettefrance.fr",
        "entreprises.lagazettefrance.fr",
        "bfmtv.com",
        "lefigaro.fr",
        "leparisien.fr",
        "lemonde.fr",
        "capital.fr",
        "challenges.fr",
        # Review sites (no contact data useful here)
        "yelp.fr",
        "pagesjaunes.fr",
        "tripadvisor.fr",
        "trustpilot.com",
        "avisverifies.com",
        "horaires-et-num.fr",
        "118712.fr",
        "118000.fr",
        # Job boards
        "indeed.fr",
        "welcometothejungle.com",
        "glassdoor.fr",
        "apec.fr",
        "pole-emploi.fr",
        # Paid registries (never used)
        "pappers.fr",
        # Blocked directories (return 403)
        "kompass.com",
    }

    for blocked in always_blocked:
        if domain == blocked or domain.endswith("." + blocked):
            return False

    # Directory sites: blocked for primary queries (we want the company's own
    # site), but allowed for fallback queries when hunting for phone/email.
    if not allow_directories:
        for dir_domain in DIRECTORY_DOMAINS:
            if domain == dir_domain or domain.endswith("." + dir_domain):
                return False

    return True


def _is_plausible_match(url: str, denomination: str) -> bool:
    """Return True if the second-level domain has meaningful overlap with the company name.

    Uses word-level substring matching rather than character counting to avoid
    false positives from aggregator sites.

    Strategy:
      - Extract SLD (part before TLD, e.g. 'aeroport' from 'toulouse.aeroport.fr')
      - Normalise both SLD and company name to lowercase alphanumeric
      - Pass if: any word (≥4 chars) from company name appears in domain,
                 OR the full normalised domain is a substring of the company name,
                 OR the full normalised company name is a substring of the domain.

    Examples:
      'https://www.toulouse.aeroport.fr/'  × 'AEROPORT TOULOUSE-BLAGNAC' → True
      'https://geodis.com/fr/'              × 'GEODIS'                    → True
      'https://entreprises.lagazettefrance.fr' × '3S GESTION'             → False
    """
    try:
        netloc = urlparse(url).netloc.lower()
    except ValueError:
        return False

    # Get the second-level domain (just before the TLD)
    parts = netloc.split(".")
    sld = parts[-2] if len(parts) >= 2 else parts[0]
    sld_clean = re.sub(r"[^a-z0-9]", "", sld)

    if not sld_clean or len(sld_clean) < 2:
        return False

    # Normalise denomination: remove legal suffixes, lowercase, alphanumeric only
    name = denomination.lower()
    for suffix in (" sarl", " sas", " sa", " eurl", " sasu", " sci", " earl",
                   " sca", " snc", " gie", " scop"):
        name = name.replace(suffix, "")
    name_clean = re.sub(r"[^a-z0-9]", "", name)

    if not name_clean:
        return False

    # Full name is substring of domain, or domain is substring of full name.
    # Require both to be ≥4 chars to avoid spurious matches like "3s" ⊂ "en3s".
    if len(name_clean) >= 4 and len(sld_clean) >= 4:
        if sld_clean in name_clean or name_clean in sld_clean:
            return True

    # Any meaningful word (≥4 chars) from the company name appears in the domain
    words = [re.sub(r"[^a-z0-9]", "", w) for w in name.split()]
    if any(len(w) >= 4 and w in sld_clean for w in words):
        return True

    # Any meaningful part of the domain appears in the company name
    if len(sld_clean) >= 4 and sld_clean in name_clean:
        return True

    return False


def _clean_url(url: str) -> str:
    """Strip tracking parameters and fragments from a URL."""
    try:
        parsed = urlparse(url)
        # Remove query string and fragment — just keep scheme + netloc + path
        return f"{parsed.scheme}://{parsed.netloc}{parsed.path}".rstrip("/")
    except ValueError:
        return url
