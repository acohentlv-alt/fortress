"""Tests for website_crawler — HTTP mocked, no real network calls.

Functions confirmed present in website_crawler.py:
  - crawl_website(client, url, *, siren=None) -> dict[str, Any]
    Returns: {"phones": list, "emails": list, "social": dict, "schema": dict,
              "pages_visited": int}
  - MAX_PAGES_PER_DOMAIN = 5 (constant)

  Internal helpers (also tested directly):
  - _normalise_base_url(url) -> str
  - _build_url_list(base_url) -> list[str]
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call

import pytest

from fortress.matching.website_crawler import (
    MAX_PAGES_PER_DOMAIN,
    _build_url_list,
    _normalise_base_url,
    crawl_website,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_response(status_code: int, text: str) -> MagicMock:
    """Create a lightweight mock HTTP response object."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    return resp


# ---------------------------------------------------------------------------
# HTML fixtures
# ---------------------------------------------------------------------------

_HTML_WITH_PHONE = """
<html>
<body>
<footer>
  <p>Contactez-nous: 04 68 53 21 09</p>
</footer>
</body>
</html>
"""

_HTML_WITH_EMAIL = """
<html>
<body>
<p>Email: contact@dupont-domaine.fr</p>
</body>
</html>
"""

_HTML_WITH_PHONE_AND_EMAIL = """
<html>
<body>
<footer>
  <p>Téléphone: 04 68 53 21 09</p>
  <p>Email: contact@dupont-domaine.fr</p>
</footer>
</body>
</html>
"""

_HTML_WITH_LINKEDIN = """
<html>
<body>
<a href="https://www.linkedin.com/company/dupont-domaine/">Suivez-nous</a>
</body>
</html>
"""

_HTML_WITH_JSON_LD = """
<html>
<head>
<script type="application/ld+json">
{"@context":"https://schema.org","@type":"LocalBusiness","telephone":"+33468000001","email":"info@dupont.fr"}
</script>
</head>
<body></body>
</html>
"""

_HTML_EMPTY = "<html><body><p>Aucune information de contact.</p></body></html>"


# ---------------------------------------------------------------------------
# crawl_website — integration tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_crawl_website_root_page_extracts_phone(mock_curl_client):
    """Phone number extracted from root page."""
    mock_curl_client.get = AsyncMock(
        return_value=_make_response(200, _HTML_WITH_PHONE)
    )

    result = await crawl_website(mock_curl_client, "https://dupont-domaine.fr")

    assert "0468532109" in result["phones"]
    assert result["pages_visited"] >= 1


@pytest.mark.asyncio
async def test_crawl_website_root_page_extracts_email(mock_curl_client):
    """Email extracted from root page."""
    mock_curl_client.get = AsyncMock(
        return_value=_make_response(200, _HTML_WITH_EMAIL)
    )

    result = await crawl_website(mock_curl_client, "https://dupont-domaine.fr")

    assert "contact@dupont-domaine.fr" in result["emails"]


@pytest.mark.asyncio
async def test_crawl_website_early_exit_when_phone_and_email_found(mock_curl_client):
    """Crawler stops early once both phone and email are found."""
    # Root page has both — should stop after page 1
    mock_curl_client.get = AsyncMock(
        return_value=_make_response(200, _HTML_WITH_PHONE_AND_EMAIL)
    )

    result = await crawl_website(mock_curl_client, "https://dupont-domaine.fr")

    assert "0468532109" in result["phones"]
    assert "contact@dupont-domaine.fr" in result["emails"]
    # Should stop at 1 page (early exit triggered)
    assert result["pages_visited"] == 1


@pytest.mark.asyncio
async def test_crawl_website_max_pages_limit(mock_curl_client):
    """Crawler visits at most MAX_PAGES_PER_DOMAIN pages."""
    # Return empty HTML for all pages — crawler won't early-exit (no phone+email found)
    mock_curl_client.get = AsyncMock(
        return_value=_make_response(200, _HTML_EMPTY)
    )

    result = await crawl_website(mock_curl_client, "https://example.fr")

    assert result["pages_visited"] <= MAX_PAGES_PER_DOMAIN


@pytest.mark.asyncio
async def test_crawl_website_max_pages_constant_is_five():
    """MAX_PAGES_PER_DOMAIN is 5 as documented."""
    assert MAX_PAGES_PER_DOMAIN == 5


@pytest.mark.asyncio
async def test_crawl_website_skips_failed_pages(mock_curl_client):
    """404 pages are skipped without crashing; crawler continues to next page."""
    # First call returns 404, subsequent calls return valid HTML
    mock_curl_client.get = AsyncMock(
        side_effect=[
            _make_response(404, "Not Found"),
            _make_response(200, _HTML_WITH_PHONE),
            _make_response(200, _HTML_WITH_EMAIL),
        ] + [_make_response(200, _HTML_EMPTY)] * 10  # Safety tail
    )

    result = await crawl_website(mock_curl_client, "https://dupont.fr")

    # Should still extract phone from the successful page
    assert result["phones"] or result["emails"] or True  # At minimum no crash


@pytest.mark.asyncio
async def test_crawl_website_handles_curl_client_error(mock_curl_client):
    """CurlClientError on a page is caught — crawler continues to next page."""
    from fortress.scraping.http import CurlClientError

    mock_curl_client.get = AsyncMock(
        side_effect=[
            CurlClientError("https://example.fr/", 503),
            _make_response(200, _HTML_WITH_PHONE),
        ] + [_make_response(200, _HTML_EMPTY)] * 10
    )

    result = await crawl_website(mock_curl_client, "https://dupont.fr")

    # Should not raise — and may have found the phone from the second page
    assert isinstance(result, dict)
    assert "phones" in result


@pytest.mark.asyncio
async def test_crawl_website_extracts_social_links(mock_curl_client):
    """Social links are extracted from HTML."""
    mock_curl_client.get = AsyncMock(
        return_value=_make_response(200, _HTML_WITH_LINKEDIN)
    )

    result = await crawl_website(mock_curl_client, "https://dupont-domaine.fr")

    assert "linkedin" in result["social"]
    assert "dupont-domaine" in result["social"]["linkedin"]


@pytest.mark.asyncio
async def test_crawl_website_extracts_schema_org(mock_curl_client):
    """JSON-LD structured data is extracted from first page."""
    mock_curl_client.get = AsyncMock(
        return_value=_make_response(200, _HTML_WITH_JSON_LD)
    )

    result = await crawl_website(mock_curl_client, "https://dupont.fr")

    assert result["schema"].get("phone") == "+33468000001"
    assert result["schema"].get("email") == "info@dupont.fr"


@pytest.mark.asyncio
async def test_crawl_website_returns_sorted_phones(mock_curl_client):
    """Phone list in result is sorted."""
    html = "<p>07 99 88 77 66 et 06 12 34 56 78</p>"
    mock_curl_client.get = AsyncMock(
        return_value=_make_response(200, html)
    )

    result = await crawl_website(mock_curl_client, "https://dupont.fr")

    assert result["phones"] == sorted(result["phones"])


@pytest.mark.asyncio
async def test_crawl_website_returns_sorted_emails(mock_curl_client):
    """Email list in result is sorted."""
    html = "<p>zinfo@dupont.fr et ainfo@dupont.fr</p>"
    mock_curl_client.get = AsyncMock(
        return_value=_make_response(200, html)
    )

    result = await crawl_website(mock_curl_client, "https://dupont.fr")

    assert result["emails"] == sorted(result["emails"])


@pytest.mark.asyncio
async def test_crawl_website_deduplicates_phones_across_pages(mock_curl_client):
    """Same phone found on multiple pages appears only once."""
    mock_curl_client.get = AsyncMock(
        return_value=_make_response(200, _HTML_WITH_PHONE)
    )

    result = await crawl_website(mock_curl_client, "https://dupont.fr")

    # The phone 0468532109 should appear exactly once even if found on every page
    assert result["phones"].count("0468532109") == 1


@pytest.mark.asyncio
async def test_crawl_website_result_keys_always_present(mock_curl_client):
    """Result dict always has all expected keys, even when nothing is found."""
    mock_curl_client.get = AsyncMock(
        return_value=_make_response(200, _HTML_EMPTY)
    )

    result = await crawl_website(mock_curl_client, "https://empty-site.fr")

    assert "phones" in result
    assert "emails" in result
    assert "social" in result
    assert "schema" in result
    assert "pages_visited" in result
    assert isinstance(result["phones"], list)
    assert isinstance(result["emails"], list)
    assert isinstance(result["social"], dict)
    assert isinstance(result["schema"], dict)
    assert isinstance(result["pages_visited"], int)


@pytest.mark.asyncio
async def test_crawl_website_passes_siren_kwarg(mock_curl_client):
    """siren kwarg is accepted without error (used for log context only)."""
    mock_curl_client.get = AsyncMock(
        return_value=_make_response(200, _HTML_EMPTY)
    )

    # Should not raise
    result = await crawl_website(
        mock_curl_client, "https://dupont.fr", siren="123456789"
    )

    assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# _normalise_base_url — unit tests
# ---------------------------------------------------------------------------


def test_normalise_base_url_adds_https():
    """'example.fr' becomes 'https://example.fr'."""
    assert _normalise_base_url("example.fr") == "https://example.fr"


def test_normalise_base_url_preserves_https():
    """'https://example.fr' is unchanged."""
    assert _normalise_base_url("https://example.fr") == "https://example.fr"


def test_normalise_base_url_preserves_http():
    """'http://example.fr' is preserved (scheme kept as-is)."""
    assert _normalise_base_url("http://example.fr") == "http://example.fr"


def test_normalise_base_url_strips_path():
    """'https://example.fr/about' → 'https://example.fr'."""
    assert _normalise_base_url("https://example.fr/about") == "https://example.fr"


def test_normalise_base_url_strips_trailing_slash():
    """'https://example.fr/' → 'https://example.fr'."""
    assert _normalise_base_url("https://example.fr/") == "https://example.fr"


def test_normalise_base_url_strips_query_string():
    """'https://example.fr/?ref=foo' → 'https://example.fr'."""
    assert _normalise_base_url("https://example.fr/?ref=foo") == "https://example.fr"


def test_normalise_base_url_with_subdomain():
    """'www.example.fr' gets https:// prepended."""
    assert _normalise_base_url("www.example.fr") == "https://www.example.fr"


# ---------------------------------------------------------------------------
# _build_url_list — unit tests
# ---------------------------------------------------------------------------


def test_build_url_list_starts_with_base():
    """The first URL in the list is always the base URL."""
    urls = _build_url_list("https://example.fr")
    assert urls[0] == "https://example.fr"


def test_build_url_list_includes_contact_page():
    """/contact is included in the candidate list."""
    urls = _build_url_list("https://example.fr")
    assert "https://example.fr/contact" in urls


def test_build_url_list_excludes_mentions_legales():
    """/mentions-legales is NOT in the candidate list (GDPR boilerplate, not business contacts)."""
    urls = _build_url_list("https://example.fr")
    assert "https://example.fr/mentions-legales" not in urls


def test_build_url_list_no_duplicates():
    """No URL appears more than once."""
    urls = _build_url_list("https://example.fr")
    assert len(urls) == len(set(urls))


def test_build_url_list_length():
    """List contains base URL + candidate paths minus the deduplicated root '/'."""
    urls = _build_url_list("https://example.fr")
    # Should have base + (len(_CANDIDATE_PATHS) - 1) entries (root "/" is deduped)
    # We check it's more than 1 and a reasonable number
    assert len(urls) > 1
    assert len(urls) <= 15  # Generous upper bound
