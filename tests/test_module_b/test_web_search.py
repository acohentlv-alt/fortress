"""Tests for web_search — all HTTP mocked, no real network calls.

Functions confirmed present in web_search.py:
  - find_website_url(client, denomination, city, *, siren=None) -> str | None
    Tries Google primary query, then up to 3 fallback queries.

  Internal helpers (also tested directly where useful):
  - _extract_google_url(html) -> str | None
  - _is_useful_url(url) -> bool
  - _is_plausible_match(url, denomination) -> bool
"""
from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from fortress.module_b.web_search import (
    _extract_google_url,
    _is_plausible_match,
    _is_useful_url,
    find_website_url,
)


# ---------------------------------------------------------------------------
# Helpers: build mock HTTP responses
# ---------------------------------------------------------------------------


def _make_response(status_code: int, text: str) -> MagicMock:
    """Create a lightweight mock HTTP response object."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    return resp


# ---------------------------------------------------------------------------
# Realistic HTML fixtures
# ---------------------------------------------------------------------------

# Google HTML snippet — organic result with /url?q= redirect
_GOOGLE_HTML_WITH_RESULT = """
<html>
<body>
<div class="g">
  <a href="/url?q=https://www.dupont-domaine.fr&amp;sa=U&amp;ved=2ahUKEwi">
    Domaine Dupont — Site Officiel
  </a>
</div>
</body>
</html>
"""

# Google HTML snippet — direct https:// href without redirect wrapper
_GOOGLE_HTML_DIRECT_HREF = """
<html>
<body>
<div class="g">
  <a href="https://www.domaine-martin.fr">
    Domaine Martin SARL
  </a>
</div>
</body>
</html>
"""

# Google HTML snippet — only aggregator results (should be filtered)
_GOOGLE_HTML_ONLY_AGGREGATORS = """
<html>
<body>
<div class="g">
  <a href="/url?q=https://www.societe.com/societe/dupont-123456789.html">societe.com</a>
  <a href="/url?q=https://www.pappers.fr/entreprise/dupont-123456789">pappers</a>
</div>
</body>
</html>
"""

# Google HTML snippet — empty / no results
_GOOGLE_HTML_NO_RESULTS = """
<html><body><p>Aucun résultat trouvé.</p></body></html>
"""



# ---------------------------------------------------------------------------
# find_website_url — integration tests (mocking Google and SearXNG)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_find_website_url_google_success(mock_curl_client):
    """Returns URL from Google search results when Google succeeds."""
    mock_curl_client.get = AsyncMock(
        return_value=_make_response(200, _GOOGLE_HTML_WITH_RESULT)
    )

    result = await find_website_url(
        mock_curl_client,
        denomination="Dupont Domaine SARL",
        city="Thuir",
    )

    assert result is not None
    assert "dupont-domaine.fr" in result
    # Google was tried (one get call)
    mock_curl_client.get.assert_called_once()


@pytest.mark.asyncio
async def test_find_website_url_google_direct_href(mock_curl_client):
    """Returns URL from a direct https:// href in Google results."""
    mock_curl_client.get = AsyncMock(
        return_value=_make_response(200, _GOOGLE_HTML_DIRECT_HREF)
    )

    result = await find_website_url(
        mock_curl_client,
        denomination="Domaine Martin SARL",
        city="Perpignan",
    )

    assert result is not None
    assert "domaine-martin.fr" in result


@pytest.mark.asyncio
async def test_find_website_url_google_blocked_tries_fallbacks(mock_curl_client):
    """When primary Google returns 429, fallback queries are attempted."""
    mock_curl_client.get = AsyncMock(side_effect=[
        _make_response(429, "Too Many Requests"),  # primary Google
        _make_response(200, _GOOGLE_HTML_WITH_RESULT),  # fallback 1: Google
    ])

    result = await find_website_url(
        mock_curl_client,
        denomination="Dupont Domaine SARL",
        city="Maury",
    )

    assert result is not None
    assert "dupont-domaine.fr" in result
    assert mock_curl_client.get.call_count == 2


@pytest.mark.asyncio
async def test_find_website_url_google_403_tries_fallbacks(mock_curl_client):
    """When primary Google returns 403, fallback queries are attempted."""
    mock_curl_client.get = AsyncMock(side_effect=[
        _make_response(403, "Forbidden"),  # primary Google
        _make_response(200, _GOOGLE_HTML_WITH_RESULT),  # fallback 1
    ])

    result = await find_website_url(
        mock_curl_client,
        denomination="Dupont Domaine SARL",
        city="Maury",
    )

    assert result is not None
    assert mock_curl_client.get.call_count == 2


@pytest.mark.asyncio
async def test_find_website_url_filters_aggregators(mock_curl_client):
    """URLs from societe.com / pappers.fr are rejected for primary query.
    Fallback queries are also tried but return nothing — result is None.
    """
    _empty = _make_response(200, _GOOGLE_HTML_NO_RESULTS)
    mock_curl_client.get = AsyncMock(side_effect=[
        _make_response(200, _GOOGLE_HTML_ONLY_AGGREGATORS),  # primary: aggregators filtered
        _empty,                                               # fallback 1: nothing
        _empty,                                               # fallback 2: nothing
    ])

    result = await find_website_url(
        mock_curl_client,
        denomination="Dupont SARL",
        city="Perpignan",
    )

    assert result is None


@pytest.mark.asyncio
async def test_find_website_url_no_results(mock_curl_client):
    """All Google queries return empty results — returns None."""
    _empty = _make_response(200, _GOOGLE_HTML_NO_RESULTS)
    mock_curl_client.get = AsyncMock(side_effect=[
        _empty,  # primary Google
        _empty,  # fallback 1
        _empty,  # fallback 2
    ])

    result = await find_website_url(
        mock_curl_client,
        denomination="Entreprise Inconnue SARL",
        city="Paris",
    )

    assert result is None


@pytest.mark.asyncio
async def test_find_website_url_google_non200_tries_fallback(mock_curl_client):
    """Google 500 error on primary causes fallback queries to be tried."""
    mock_curl_client.get = AsyncMock(side_effect=[
        _make_response(500, "Server Error"),  # primary Google
        _make_response(200, _GOOGLE_HTML_WITH_RESULT),  # fallback 1
    ])

    result = await find_website_url(
        mock_curl_client,
        denomination="Dupont Domaine SARL",
        city="Maury",
    )

    assert result is not None


@pytest.mark.asyncio
async def test_find_website_url_passes_siren_to_logger(mock_curl_client):
    """siren kwarg is accepted without error (used for log context only)."""
    mock_curl_client.get = AsyncMock(
        return_value=_make_response(200, _GOOGLE_HTML_WITH_RESULT)
    )

    # Should not raise
    result = await find_website_url(
        mock_curl_client,
        denomination="Dupont Domaine SARL",
        city="Thuir",
        siren="123456789",
    )

    assert result is not None


@pytest.mark.asyncio
async def test_find_website_url_curl_client_error_tries_fallback(mock_curl_client):
    """CurlClientError from primary Google triggers fallback queries (not a crash)."""
    from fortress.module_c.curl_client import CurlClientError

    mock_curl_client.get = AsyncMock(side_effect=[
        CurlClientError("https://google.com/search", 429),  # primary raises
        _make_response(200, _GOOGLE_HTML_WITH_RESULT),  # fallback 1 succeeds
    ])

    result = await find_website_url(
        mock_curl_client,
        denomination="Dupont Domaine SARL",
        city="Maury",
    )

    assert result is not None


# ---------------------------------------------------------------------------
# _extract_google_url — unit tests for the HTML parser
# ---------------------------------------------------------------------------


def test_extract_google_url_redirect_format():
    """Parses /url?q= redirect format correctly."""
    result = _extract_google_url(_GOOGLE_HTML_WITH_RESULT)
    assert result == "https://www.dupont-domaine.fr"


def test_extract_google_url_direct_href():
    """Parses direct https:// href correctly."""
    result = _extract_google_url(_GOOGLE_HTML_DIRECT_HREF)
    assert result == "https://www.domaine-martin.fr"


def test_extract_google_url_none_when_empty():
    """Returns None when no useful URLs found."""
    result = _extract_google_url(_GOOGLE_HTML_NO_RESULTS)
    assert result is None


def test_extract_google_url_skips_aggregators():
    """Filters out societe.com and pappers.fr."""
    result = _extract_google_url(_GOOGLE_HTML_ONLY_AGGREGATORS)
    assert result is None


# ---------------------------------------------------------------------------
# _is_useful_url — unit tests for the URL filter
# ---------------------------------------------------------------------------


def test_is_useful_url_valid_corporate_site():
    """A real corporate website is useful."""
    assert _is_useful_url("https://www.dupont.fr") is True


def test_is_useful_url_rejects_google():
    """google.fr is not useful."""
    assert _is_useful_url("https://www.google.fr/search?q=foo") is False


def test_is_useful_url_rejects_societe_com():
    """societe.com is blocked."""
    assert _is_useful_url("https://www.societe.com/societe/foo-123.html") is False


def test_is_useful_url_rejects_pappers_fr():
    """pappers.fr is blocked."""
    assert _is_useful_url("https://www.pappers.fr/entreprise/foo") is False


def test_is_useful_url_rejects_linkedin():
    """linkedin.com is blocked."""
    assert _is_useful_url("https://www.linkedin.com/company/foo") is False


def test_is_useful_url_rejects_facebook():
    """facebook.com is blocked."""
    assert _is_useful_url("https://www.facebook.com/foo") is False


def test_is_useful_url_rejects_pagesjaunes():
    """pagesjaunes.fr is blocked."""
    assert _is_useful_url("https://www.pagesjaunes.fr/pros/foo") is False


def test_is_useful_url_rejects_wikipedia():
    """wikipedia.org is blocked."""
    assert _is_useful_url("https://fr.wikipedia.org/wiki/foo") is False


def test_is_useful_url_rejects_kompass():
    """kompass.com is always blocked (returns 403)."""
    assert _is_useful_url("https://www.kompass.com/c/foo/bar") is False
    # Also blocked even when allow_directories=True
    assert _is_useful_url("https://www.kompass.com/c/foo/bar", allow_directories=True) is False


def test_is_useful_url_rejects_no_scheme():
    """URL without scheme is not useful."""
    assert _is_useful_url("dupont.fr") is False


# ---------------------------------------------------------------------------
# _is_plausible_match — unit tests for the domain/name overlap check
# ---------------------------------------------------------------------------


def test_is_plausible_match_direct_overlap():
    """Domain containing the company name root is a plausible match."""
    assert _is_plausible_match("https://www.dupont.fr", "Dupont SARL") is True


def test_is_plausible_match_strips_legal_suffix():
    """Legal suffixes (SARL, SAS, etc.) are stripped before comparison."""
    assert _is_plausible_match("https://www.dupont.fr", "Dupont SAS") is True
    assert _is_plausible_match("https://www.dupont.fr", "Dupont EURL") is True


def test_is_plausible_match_unrelated_domain():
    """A completely unrelated domain is not a plausible match."""
    # "xyz" has no characters in common with "dupont" exceeding the threshold
    assert _is_plausible_match("https://www.xyz.fr", "Dupont SARL") is False


def test_is_plausible_match_partial_overlap():
    """Company name contained in domain (or domain in name) is a match."""
    # "dupont" is a prefix of "dupontvins" → name_clean in sld_clean
    assert _is_plausible_match("https://www.dupont-vins.fr", "Dupont SARL") is True
