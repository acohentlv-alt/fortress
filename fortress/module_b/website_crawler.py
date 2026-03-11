"""Website crawler — visit corporate pages and extract contact data.

No browser. No JavaScript execution. Pure HTTP via CurlClient (curl_cffi).
Passes fetched HTML to contact_parser for extraction.

Strategy:
  1. Try the root page first (often has contact info in footer).
  2. Scan homepage HTML for <a href> links pointing to contact pages.
     If found, add the discovered contact page to the crawl queue.
  3. Try a small set of high-value static paths (/contact, /nous-contacter…).
  4. Stop early if all MVP fields (phone + email) are found.
  5. Deduplicate extracted values across all pages visited.

Junk pages like /mentions-legales and /politique-de-confidentialite are
intentionally excluded from the candidate list — they contain GDPR boilerplate
with DPO/legal emails, not real business contact data.

Rate limiting: enforced by CurlClient's built-in delay.
Max pages per domain: 5 (configurable via MAX_PAGES_PER_DOMAIN).
"""

from __future__ import annotations

import re
from typing import Any
from urllib.parse import urljoin, urlparse

import structlog

from fortress.module_b.contact_parser import (
    extract_emails,
    extract_phones,
    extract_social_links,
    parse_schema_org,
)
from fortress.module_c.curl_client import CurlClient, CurlClientError

log = structlog.get_logger(__name__)

# Maximum pages to visit per domain before giving up.
MAX_PAGES_PER_DOMAIN = 5

# Page paths to attempt, in priority order (after homepage + discovered page).
# Root "/" is always first. We stop early when MVP fields are satisfied.
# NOTE: /mentions-legales and /legal are intentionally excluded — they reliably
# contain GDPR/DPO emails but almost never have real business contact data.
_CANDIDATE_PATHS = [
    "/",
    "/contact",
    "/contact.html",
    "/contact.php",
    "/nous-contacter",
    "/contactez-nous",
    "/a-propos",
    "/qui-sommes-nous",
    "/about",
    "/equipe",
]

# URL path fragments that signal a contact page — used for dynamic discovery.
_CONTACT_PATH_KEYWORDS = (
    "/contact",
    "/nous-contacter",
    "/contactez-nous",
    "/a-propos",
    "/qui-sommes-nous",
    "/about",
    "/equipe",
    "/team",
)

# Regex to find all <a href="..."> anchors in HTML.
_ANCHOR_RE = re.compile(r'<a[^>]+href=["\']([^"\']+)["\']', re.IGNORECASE)


async def crawl_website(
    client: CurlClient,
    url: str,
    *,
    siren: str | None = None,
) -> dict[str, Any]:
    """Crawl a corporate website and extract contact information.

    Visits up to MAX_PAGES_PER_DOMAIN pages. Stops early once both
    a phone number and email address have been found.

    Strategy:
      1. Normalise URL to homepage (strip deep paths like /mentions-legales).
      2. Fetch homepage. Scan for a dynamically discovered contact page link.
      3. If found, add the contact page immediately after the homepage in the queue.
      4. Continue with the static candidate path list until MVP fields are found.

    Args:
        client: CurlClient instance (Chrome TLS impersonation).
        url:    URL found by web search (may include a deep path).
        siren:  Optional SIREN for log context.

    Returns:
        Dict with:
            "phones":  list[str]       — normalised French phone numbers
            "emails":  list[str]       — business email addresses
            "social":  dict[str, str]  — platform → URL
            "schema":  dict[str, Any]  — data from JSON-LD structured data
            "pages_visited": int       — how many pages were fetched
    """
    base = _normalise_base_url(url)
    log.debug("crawl.start", url=base, siren=siren)

    phones: set[str] = set()
    emails: set[str] = set()
    social: dict[str, str] = {}
    schema: dict[str, Any] = {}
    pages_visited = 0
    visited_urls: set[str] = set()

    # Build initial URL list: homepage + static candidate paths
    urls_to_visit: list[str] = _build_url_list(base)
    homepage_html: str | None = None

    for i, page_url in enumerate(urls_to_visit):
        if pages_visited >= MAX_PAGES_PER_DOMAIN:
            break
        if page_url in visited_urls:
            continue

        html = await _fetch_page(client, page_url, siren=siren)
        if html is None:
            visited_urls.add(page_url)
            continue

        visited_urls.add(page_url)
        pages_visited += 1

        # After fetching the homepage, scan for a contact page link.
        # Insert it at position 1 so it's visited before the static candidates.
        if i == 0:
            homepage_html = html
            contact_url = _discover_contact_page(base, html)
            if contact_url and contact_url not in visited_urls:
                # Insert immediately after the homepage in the visit queue
                urls_to_visit.insert(1, contact_url)
                log.debug(
                    "crawl.contact_page_discovered",
                    url=contact_url,
                    siren=siren,
                )

        # Extract from this page
        page_phones = extract_phones(html)
        page_emails = extract_emails(html)
        page_social = extract_social_links(html)
        page_schema = parse_schema_org(html) if not schema else {}

        phones.update(page_phones)
        emails.update(page_emails)
        # Social links: first match per platform wins
        for platform, link in page_social.items():
            if platform not in social:
                social[platform] = link
        if page_schema and not schema:
            schema = page_schema

        log.debug(
            "crawl.page_done",
            url=page_url,
            phones=len(phones),
            emails=len(emails),
            siren=siren,
        )

        # Early exit: both MVP contact fields found — no need to visit more pages
        if phones and emails:
            log.debug("crawl.early_exit", url=base, pages=pages_visited, siren=siren)
            break

    result: dict[str, Any] = {
        "phones": sorted(phones),
        "emails": sorted(emails),
        "social": social,
        "schema": schema,
        "pages_visited": pages_visited,
    }

    log.info(
        "crawl.done",
        url=base,
        phones=len(phones),
        emails=len(emails),
        social=list(social.keys()),
        pages_visited=pages_visited,
        siren=siren,
    )
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _discover_contact_page(base_url: str, homepage_html: str) -> str | None:
    """Scan homepage HTML for a link to the company's contact page.

    Looks for <a href="..."> anchors whose path matches _CONTACT_PATH_KEYWORDS.
    Returns the absolute URL of the best match (highest priority keyword first),
    or None if no contact page link is found.

    Priority order follows _CONTACT_PATH_KEYWORDS (contact > nous-contacter > …).
    """
    anchors: list[str] = _ANCHOR_RE.findall(homepage_html)
    # Build absolute URLs and score them by keyword priority
    best_priority = len(_CONTACT_PATH_KEYWORDS)  # lower = better
    best_url: str | None = None

    for href in anchors:
        href = href.strip()
        if not href or href.startswith(("#", "javascript:", "mailto:", "tel:")):
            continue

        # Make absolute
        if href.startswith("http://") or href.startswith("https://"):
            abs_url = href
        elif href.startswith("/"):
            abs_url = base_url.rstrip("/") + href
        else:
            continue  # relative paths without leading slash — skip

        # Only follow links on the same domain
        try:
            if urlparse(abs_url).netloc != urlparse(base_url).netloc:
                continue
        except ValueError:
            continue

        path = urlparse(abs_url).path.lower()
        for priority, keyword in enumerate(_CONTACT_PATH_KEYWORDS):
            if keyword in path and priority < best_priority:
                best_priority = priority
                best_url = abs_url.split("?")[0].split("#")[0]  # strip query/fragment
                break

    return best_url


async def _fetch_page(
    client: CurlClient,
    url: str,
    *,
    siren: str | None = None,
) -> str | None:
    """Fetch a single page. Returns HTML string or None on failure."""
    try:
        response = await client.get(url)
    except CurlClientError as exc:
        log.debug("crawl.fetch_error", url=url, error=str(exc), siren=siren)
        return None

    if response.status_code == 200:
        return response.text

    # 301/302 redirect would have been followed by curl_cffi automatically.
    # Non-200, non-redirect status codes are soft failures — just skip this page.
    log.debug("crawl.non200", url=url, status=response.status_code, siren=siren)
    return None


def _build_url_list(base_url: str) -> list[str]:
    """Return the ordered list of URLs to attempt for this domain.

    Deduplicates: if a candidate path resolves to the base URL, skip it.
    Always puts the base URL first.
    """
    seen: set[str] = set()
    urls: list[str] = []

    # Always start with the canonical base
    urls.append(base_url)
    seen.add(base_url)

    for path in _CANDIDATE_PATHS:
        if path == "/":
            # "/" is already covered by the base URL — skip to avoid duplicate
            candidate = base_url
        else:
            candidate = urljoin(base_url + "/", path.lstrip("/"))

        if candidate not in seen:
            urls.append(candidate)
            seen.add(candidate)

    return urls


def _normalise_base_url(url: str) -> str:
    """Ensure URL has a scheme and strip trailing slash / path.

    Examples:
        "https://example.fr/about" → "https://example.fr"
        "example.fr"               → "https://example.fr"
    """
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    try:
        parsed = urlparse(url)
        # Keep scheme + netloc only (discard path, query, fragment)
        return f"{parsed.scheme}://{parsed.netloc}"
    except ValueError:
        return url
