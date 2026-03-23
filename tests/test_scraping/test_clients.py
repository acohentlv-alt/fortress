"""Tests for Module C — CurlClient.

All tests are zero-network: every HTTP call is mocked.

Run with:
    pytest tests/test_module_c/test_clients.py -v
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fortress.scraping.http import (
    USER_AGENTS,
    CurlClient,
    CurlClientError,
    CurlResponse,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_curl_response(
    status_code: int = 200,
    text: str = "OK",
    url: str = "https://example.com",
    headers: dict[str, str] | None = None,
) -> MagicMock:
    """Build a mock curl_cffi response object."""
    resp = MagicMock()
    resp.status_code = status_code
    resp.text = text
    resp.url = url
    resp.headers = headers or {}
    return resp


# ---------------------------------------------------------------------------
# CurlClient tests
# ---------------------------------------------------------------------------


class TestCurlClientGet:
    """Tests for CurlClient.get()."""

    @pytest.mark.asyncio
    async def test_successful_get(self) -> None:
        """A 200 response is returned as a CurlResponse with correct fields."""
        mock_resp = _make_curl_response(
            status_code=200,
            text="<html>Bonjour</html>",
            url="https://example.com/final",
            headers={"content-type": "text/html"},
        )

        with patch(
            "fortress.scraping.http.AsyncSession",
            autospec=True,
        ) as MockSession:
            instance = MockSession.return_value
            instance.request = AsyncMock(return_value=mock_resp)
            instance.close = AsyncMock()

            # Patch sleep so the test doesn't wait for rate-limit delay
            with patch("fortress.scraping.http.asyncio.sleep", new_callable=AsyncMock):
                client = CurlClient()
                result = await client.get("https://example.com")
                await client.close()

        assert isinstance(result, CurlResponse)
        assert result.status_code == 200
        assert result.text == "<html>Bonjour</html>"
        assert result.url == "https://example.com/final"
        assert result.headers["content-type"] == "text/html"

    @pytest.mark.asyncio
    async def test_retry_on_429(self) -> None:
        """A 429 on the first call triggers exactly one retry and returns on 200."""
        resp_429 = _make_curl_response(status_code=429, text="Too Many Requests")
        resp_200 = _make_curl_response(status_code=200, text="OK")

        with patch(
            "fortress.scraping.http.AsyncSession",
            autospec=True,
        ) as MockSession:
            instance = MockSession.return_value
            # First call → 429, second call → 200
            instance.request = AsyncMock(side_effect=[resp_429, resp_200])
            instance.close = AsyncMock()

            with patch("fortress.scraping.http.asyncio.sleep", new_callable=AsyncMock):
                client = CurlClient(max_retries=3)
                result = await client.get("https://example.com")
                await client.close()

        assert result.status_code == 200
        # Must have been called exactly twice (1 failure + 1 retry)
        assert instance.request.call_count == 2

    @pytest.mark.asyncio
    async def test_no_retry_on_404(self) -> None:
        """A 404 is returned immediately — no retry is attempted."""
        resp_404 = _make_curl_response(status_code=404, text="Not Found")

        with patch(
            "fortress.scraping.http.AsyncSession",
            autospec=True,
        ) as MockSession:
            instance = MockSession.return_value
            instance.request = AsyncMock(return_value=resp_404)
            instance.close = AsyncMock()

            with patch("fortress.scraping.http.asyncio.sleep", new_callable=AsyncMock):
                client = CurlClient(max_retries=3)
                result = await client.get("https://example.com/missing")
                await client.close()

        assert result.status_code == 404
        # Must have been called exactly once — no retry
        assert instance.request.call_count == 1

    @pytest.mark.asyncio
    async def test_no_retry_on_403(self) -> None:
        """A 403 Forbidden is returned immediately without retry."""
        resp_403 = _make_curl_response(status_code=403, text="Forbidden")

        with patch(
            "fortress.scraping.http.AsyncSession",
            autospec=True,
        ) as MockSession:
            instance = MockSession.return_value
            instance.request = AsyncMock(return_value=resp_403)
            instance.close = AsyncMock()

            with patch("fortress.scraping.http.asyncio.sleep", new_callable=AsyncMock):
                client = CurlClient(max_retries=3)
                result = await client.get("https://example.com/forbidden")
                await client.close()

        assert result.status_code == 403
        assert instance.request.call_count == 1

    @pytest.mark.asyncio
    async def test_raises_after_exhausted_retries(self) -> None:
        """Three consecutive 503 responses exhaust retries and raise CurlClientError."""
        resp_503 = _make_curl_response(status_code=503, text="Service Unavailable")

        with patch(
            "fortress.scraping.http.AsyncSession",
            autospec=True,
        ) as MockSession:
            instance = MockSession.return_value
            instance.request = AsyncMock(return_value=resp_503)
            instance.close = AsyncMock()

            with patch("fortress.scraping.http.asyncio.sleep", new_callable=AsyncMock):
                client = CurlClient(max_retries=3)
                with pytest.raises(CurlClientError) as exc_info:
                    await client.get("https://example.com")
                await client.close()

        err = exc_info.value
        assert err.url == "https://example.com"
        assert err.status_code == 503
        # Exactly max_retries calls were made
        assert instance.request.call_count == 3

    @pytest.mark.asyncio
    async def test_raises_after_exhausted_connection_errors(self) -> None:
        """Three consecutive connection errors exhaust retries and re-raise the exception."""
        with patch(
            "fortress.scraping.http.AsyncSession",
            autospec=True,
        ) as MockSession:
            instance = MockSession.return_value
            instance.request = AsyncMock(side_effect=OSError("Connection refused"))
            instance.close = AsyncMock()

            with patch("fortress.scraping.http.asyncio.sleep", new_callable=AsyncMock):
                client = CurlClient(max_retries=3)
                with pytest.raises(CurlClientError):
                    await client.get("https://example.com")
                await client.close()

        assert instance.request.call_count == 3

    @pytest.mark.asyncio
    async def test_random_user_agent_used(self) -> None:
        """Each request sets a User-Agent header that is a non-empty string
        and comes from the known UA pool."""
        mock_resp = _make_curl_response(status_code=200, text="OK")
        captured_headers: list[dict[str, str]] = []

        async def capture_request(method: str, url: str, **kwargs: object) -> MagicMock:
            captured_headers.append(dict(kwargs.get("headers", {})))  # type: ignore[arg-type]
            return mock_resp

        with patch(
            "fortress.scraping.http.AsyncSession",
            autospec=True,
        ) as MockSession:
            instance = MockSession.return_value
            instance.request = AsyncMock(side_effect=capture_request)
            instance.close = AsyncMock()

            with patch("fortress.scraping.http.asyncio.sleep", new_callable=AsyncMock):
                client = CurlClient()
                await client.get("https://example.com")
                await client.close()

        assert len(captured_headers) == 1
        ua = captured_headers[0].get("User-Agent")
        assert isinstance(ua, str)
        assert len(ua) > 10
        assert ua in USER_AGENTS

    @pytest.mark.asyncio
    async def test_context_manager(self) -> None:
        """CurlClient can be used as an async context manager."""
        mock_resp = _make_curl_response(status_code=200, text="context OK")

        with patch(
            "fortress.scraping.http.AsyncSession",
            autospec=True,
        ) as MockSession:
            instance = MockSession.return_value
            instance.request = AsyncMock(return_value=mock_resp)
            instance.close = AsyncMock()

            with patch("fortress.scraping.http.asyncio.sleep", new_callable=AsyncMock):
                async with CurlClient() as client:
                    result = await client.get("https://example.com")

        assert result.status_code == 200
        assert result.text == "context OK"

    @pytest.mark.asyncio
    async def test_post_with_json_body(self) -> None:
        """CurlClient.post() forwards the json kwarg to the session."""
        mock_resp = _make_curl_response(status_code=200, text="created")
        captured_kwargs: list[dict[str, object]] = []

        async def capture_request(method: str, url: str, **kwargs: object) -> MagicMock:
            captured_kwargs.append(kwargs)
            return mock_resp

        with patch(
            "fortress.scraping.http.AsyncSession",
            autospec=True,
        ) as MockSession:
            instance = MockSession.return_value
            instance.request = AsyncMock(side_effect=capture_request)
            instance.close = AsyncMock()

            with patch("fortress.scraping.http.asyncio.sleep", new_callable=AsyncMock):
                client = CurlClient()
                result = await client.post(
                    "https://example.com/api",
                    json={"key": "value"},
                )
                await client.close()

        assert result.status_code == 200
        assert any("json" in kw for kw in captured_kwargs)
