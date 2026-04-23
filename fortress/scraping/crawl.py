"""Shared website crawling — homepage first, then concurrent page fetch.

Used by both the Maps discovery pipeline (discovery.py) and the on-demand
company enrichment endpoint (api/routes/companies.py).

The key behaviour:
  1. Fetch homepage synchronously to scan nav links.
  2. Build a deduped list of contact/legal pages (seed paths + discovered links).
  3. Fire ALL remaining pages concurrently via asyncio.gather with a wall-clock
     timeout, so the total crawl time is bounded even for slow sites.
  4. Run all extraction (email, phone, socials, SIREN, Schema.org) over every
     page's HTML and return a unified CrawlResult.
"""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urljoin, urlparse

import structlog

log = structlog.get_logger("fortress.scraping.crawl")

# ---------------------------------------------------------------------------
# Contact-keyword regex (shared between discovery + companies)
# ---------------------------------------------------------------------------

_CONTACT_KEYWORDS = re.compile(
    r"contact|mention|legal|propos|equipe|coordonn|societe|qui-sommes|nous-contacter|impressum",
    re.IGNORECASE,
)

# Seed paths tried on every site (relative to root_url).
# Mentions-légales variants first — A2 (legal-name extraction) relies on them.
# Order matters: budget truncation is positional (see line 266).
_SEED_PATHS = [
    "/contact",                    # Highest hit-rate on general sites
    "/mentions-legales",
    "/mentions-legales.html",
    "/mentions-legales/",          # Trailing-slash variant
    "/mentions-legales.php",       # PHP sites
    "/mentions",
    "/mention-legale",
    "/page/mentions-legales",      # CMS route (Drupal/WordPress)
    "/informations-legales",       # H2 new: plural
    "/information-legale",         # H2 new: singular
    "/legal",
    "/legal-notice",
    "/notice-legale",
    "/cgu",
    "/cgu-cgv",                    # H2 new: combined
    "/cgv",                        # H2 new
    "/conditions-generales",       # H2 new
    "/a-propos",                   # Tail — cheapest to lose under budget
]


# ---------------------------------------------------------------------------
# Result dataclass
# ---------------------------------------------------------------------------


@dataclass
class CrawlResult:
    """Unified result returned by crawl_website()."""

    best_email: str | None = None
    best_phone: str | None = None
    all_emails: list[str] = field(default_factory=list)
    all_phones: list[str] = field(default_factory=list)
    all_socials: dict[str, str] = field(default_factory=dict)  # {platform: url}
    siren_from_website: str | None = None
    pages_crawled: int = 0
    all_html: dict[str, str] = field(default_factory=dict)  # url -> html
    schema_org: dict[str, Any] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalise_url(url: str) -> str:
    """Strip query strings and fragments for dedup purposes."""
    try:
        p = urlparse(url)
        return p._replace(query="", fragment="").geturl().rstrip("/")
    except Exception:
        return url.rstrip("/")


async def _fetch_single(client: Any, url: str, timeout: float = 5.0) -> str | None:
    """Fetch one URL with the given CurlClient. Returns HTML text or None."""
    try:
        resp = await client.get(url, timeout=timeout)
        if resp and resp.status_code == 200 and resp.text and len(resp.text) > 200:
            return resp.text
    except Exception:
        pass
    return None


async def _fetch_pages_concurrent(
    client: Any,
    urls: list[str],
    wall_clock_limit: float,
) -> dict[str, str]:
    """Fetch multiple URLs concurrently using asyncio.gather.

    CurlClient.get() is already async (uses curl_cffi AsyncSession), so we
    can gather tasks directly without a ThreadPoolExecutor.

    Returns {url: html} for successfully fetched pages only.
    """
    if not urls:
        return {}

    async def _safe_fetch(url: str) -> tuple[str, str | None]:
        html = await _fetch_single(client, url)
        return url, html

    try:
        tasks = [asyncio.create_task(_safe_fetch(url)) for url in urls]
        results = await asyncio.wait_for(
            asyncio.gather(*tasks, return_exceptions=True),
            timeout=wall_clock_limit,
        )
    except asyncio.TimeoutError:
        # Cancel any still-running tasks
        for t in tasks:
            t.cancel()
        # Collect whatever finished before the timeout
        results = []
        for t in tasks:
            if not t.cancelled() and t.done() and not t.exception():
                results.append(t.result())
        # Return what we have
        return {url: html for url, html in results if html}

    out: dict[str, str] = {}
    for res in results:
        if isinstance(res, tuple):
            url, html = res
            if html:
                out[url] = html
    return out


# ---------------------------------------------------------------------------
# Main public function
# ---------------------------------------------------------------------------


async def crawl_website(
    url: str,
    client: Any,  # CurlClient instance
    company_name: str = "",
    department: str = "",
    siren: str = "",
    pre_fetched: dict[str, str] | None = None,
    max_pages: int = 18,  # homepage + 17 seeds, discovered links truncated under budget
    wall_clock_limit: float = 13.0,
) -> CrawlResult:
    """Crawl a website and extract contact/legal information.

    Steps:
      1. Fetch homepage (or use pre_fetched if provided).
      2. Scan homepage for nav links containing contact keywords.
      3. Build page list: seed paths + discovered links (deduped).
      4. Fetch all remaining pages concurrently.
      5. Run extraction on every page's HTML.
      6. Return CrawlResult with best values and all raw data.

    Parameters
    ----------
    url:            Company website URL (scheme optional — https:// added if missing).
    client:         CurlClient instance (caller controls timeout/delay settings).
    company_name:   Used for email ranking / personal-email detection.
    department:     2-digit French department code for phone priority.
    siren:          Entity SIREN (passed to _best_phone / _best_email for logging).
    pre_fetched:    {url: html} already fetched by the caller (reuse, no re-fetch).
    max_pages:      Maximum total pages to crawl (including homepage + pre_fetched).
    wall_clock_limit: Total seconds allowed for concurrent fetches.
    """
    # Import extraction helpers here to avoid circular imports
    from fortress.matching.contacts import (
        extract_emails,
        extract_phones,
        extract_social_links,
        extract_siren_from_html,
        parse_schema_org,
        is_personal_email,
        is_agency_email,
        _best_email,
        _best_phone,
    )

    pre_fetched = pre_fetched or {}

    # Normalise the root URL
    raw_url = url.strip()
    if not raw_url.startswith("http"):
        raw_url = f"https://{raw_url}"
    root_url = raw_url.rstrip("/")

    try:
        parsed = urlparse(root_url)
        root_base = f"{parsed.scheme}://{parsed.netloc}"
    except Exception:
        root_base = root_url

    # ------------------------------------------------------------------
    # Step 1: Homepage
    # ------------------------------------------------------------------
    all_html: dict[str, str] = {}  # url -> html (all pages fetched/pre_fetched)

    homepage_html: str | None = pre_fetched.get(root_url)
    if homepage_html is None:
        # Try fetching homepage
        try:
            resp = await client.get(root_url, timeout=5.0)
            if resp and resp.status_code == 200 and resp.text and len(resp.text) > 200:
                homepage_html = resp.text
            elif resp and resp.status_code in (0,):
                # Connection error — site unreachable
                log.info("crawl.homepage_unreachable", url=root_url)
                return CrawlResult()
        except Exception as exc:
            err_str = str(exc).lower()
            if any(k in err_str for k in ("resolve", "ssl", "certificate", "connect", "name or service")):
                log.info("crawl.homepage_dns_ssl_fail", url=root_url, error=str(exc))
                return CrawlResult()
            # Other errors (timeout, etc.) — continue with no homepage HTML
            log.debug("crawl.homepage_fetch_error", url=root_url, error=str(exc))

    if homepage_html:
        all_html[root_url] = homepage_html

    # ------------------------------------------------------------------
    # Step 2: Discover nav links from homepage
    # ------------------------------------------------------------------
    discovered_urls: list[str] = []
    if homepage_html:
        for href_match in re.finditer(r'href=["\']([^"\']+)["\']', homepage_html):
            href = href_match.group(1)
            if _CONTACT_KEYWORDS.search(href):
                abs_url = urljoin(root_base, href)
                if abs_url.startswith(root_base):
                    norm = _normalise_url(abs_url)
                    if norm not in discovered_urls:
                        discovered_urls.append(norm)

    # ------------------------------------------------------------------
    # Step 3: Build full page list (seed + discovered, deduped)
    # ------------------------------------------------------------------
    already_fetched: set[str] = set()
    already_fetched.add(_normalise_url(root_url))
    for pf_url in pre_fetched:
        already_fetched.add(_normalise_url(pf_url))
        all_html[pf_url] = pre_fetched[pf_url]

    pages_budget = max_pages - len(already_fetched)  # How many more we can fetch

    pages_to_fetch: list[str] = []

    # Seed paths first
    for path in _SEED_PATHS:
        candidate = _normalise_url(f"{root_base}{path}")
        if candidate not in already_fetched and candidate not in pages_to_fetch:
            pages_to_fetch.append(candidate)

    # Then discovered links
    for disc_url in discovered_urls:
        if disc_url not in already_fetched and disc_url not in pages_to_fetch:
            pages_to_fetch.append(disc_url)

    # Apply budget cap
    pages_to_fetch = pages_to_fetch[:max(pages_budget, 0)]

    # ------------------------------------------------------------------
    # Step 4: Concurrent fetch of remaining pages
    # ------------------------------------------------------------------
    if pages_to_fetch:
        fetched = await _fetch_pages_concurrent(client, pages_to_fetch, wall_clock_limit)
        all_html.update(fetched)

    # ------------------------------------------------------------------
    # Step 5: Extract from every page
    # ------------------------------------------------------------------
    raw_emails: list[str] = []
    raw_phones: list[str] = []
    socials: dict[str, str] = {}
    schema: dict[str, Any] = {}
    found_siren: str | None = None

    combined_html_parts: list[str] = list(all_html.values())
    combined_html = "\n".join(combined_html_parts)

    for page_html in combined_html_parts:
        raw_emails.extend(extract_emails(page_html))
        raw_phones.extend(extract_phones(page_html))
        page_socials = extract_social_links(page_html)
        for k, v in page_socials.items():
            if k not in socials:
                socials[k] = v
        if not schema:
            page_schema = parse_schema_org(page_html)
            if page_schema:
                schema = page_schema

    # SIREN extraction on combined HTML (most thorough approach)
    if combined_html:
        found_siren = extract_siren_from_html(combined_html)

    # Deduplicate
    raw_emails = list(dict.fromkeys(raw_emails))  # preserve order, dedup
    raw_phones = list(dict.fromkeys(raw_phones))

    # ------------------------------------------------------------------
    # Step 6: Select best values
    # ------------------------------------------------------------------
    # Filter emails
    filtered_emails = [
        e for e in raw_emails
        if not is_personal_email(e, company_name)
        and not is_agency_email(e, root_url)
    ]

    best_email = _best_email(filtered_emails, root_url, siren or "UNKNOWN", company_name=company_name)

    # Schema.org fallback for email
    if not best_email and schema.get("email"):
        schema_email = schema["email"]
        if not is_personal_email(schema_email, company_name) and not is_agency_email(schema_email, root_url):
            best_email = schema_email

    best_phone = _best_phone(raw_phones, siren or "UNKNOWN", departement=department or None)

    # Schema.org fallback for phone
    if not best_phone and schema.get("phone"):
        best_phone = schema["phone"]

    # Pop google_maps from socials — it's display-only, not stored in contacts
    socials.pop("google_maps", None)

    log.info(
        "crawl.complete",
        url=root_url,
        pages_crawled=len(all_html),
        emails_raw=len(raw_emails),
        emails_filtered=len(filtered_emails),
        phones_raw=len(raw_phones),
        best_email=best_email,
        best_phone=best_phone,
        siren_found=found_siren,
        socials=list(socials.keys()),
    )

    return CrawlResult(
        best_email=best_email,
        best_phone=best_phone,
        all_emails=raw_emails,
        all_phones=raw_phones,
        all_socials=socials,
        siren_from_website=found_siren,
        pages_crawled=len(all_html),
        all_html=all_html,
        schema_org=schema,
    )
