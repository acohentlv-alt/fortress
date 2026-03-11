"""curl_cffi-based async HTTP client with Chrome TLS impersonation.

Used for all anti-bot-protected traffic: Google Search, corporate website crawls,
etc. Impersonates Chrome 131's TLS fingerprint (JA3/JA4) to bypass basic WAF checks.

Usage:
    async with CurlClient() as client:
        response = await client.get("https://example.com")
        print(response.status_code, response.text[:200])
"""

from __future__ import annotations

import asyncio
import random
import time
from dataclasses import dataclass, field

import structlog
from curl_cffi.requests import AsyncSession

from fortress.config.settings import settings

logger = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

# Realistic Chrome 131 / Chrome 132 (2024-2025) user-agents, Windows + macOS mix.
USER_AGENTS: list[str] = [
    # Chrome 131 — Windows 10
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Chrome 131 — macOS Sequoia
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Chrome 130 — Windows 11
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    # Chrome 130 — macOS Ventura
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_6_1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36",
    # Chrome 129 — Windows 10
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36",
    # Chrome 132 — Windows 11 (early 2025)
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
    # Chrome 131 — Linux
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/131.0.0.0 Safari/537.36",
    # Chrome 132 — macOS Sonoma (early 2025)
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_0) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/132.0.0.0 Safari/537.36",
]

# Headers that a real Chrome browser sends on a top-level navigation request.
# Merged with per-request headers; per-request values take priority.
DEFAULT_HEADERS: dict[str, str] = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
    "Accept-Language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "Accept-Encoding": "gzip, deflate, br",
    "DNT": "1",
    "Upgrade-Insecure-Requests": "1",
    "Sec-Fetch-Dest": "document",
    "Sec-Fetch-Mode": "navigate",
    "Sec-Fetch-Site": "none",
}

# HTTP status codes on which we retry.
RETRYABLE_STATUSES: frozenset[int] = frozenset({429, 503})

# Status codes that are final — no retry, return immediately.
NON_RETRYABLE_STATUSES: frozenset[int] = frozenset({401, 403, 404})


# ---------------------------------------------------------------------------
# Exceptions
# ---------------------------------------------------------------------------


class CurlClientError(Exception):
    """Raised when all retry attempts are exhausted.

    Attributes:
        url:         The URL that could not be fetched.
        status_code: The last HTTP status code received (or 0 for connection errors).
    """

    def __init__(self, url: str, status_code: int) -> None:
        self.url = url
        self.status_code = status_code
        super().__init__(
            f"CurlClient exhausted all retries for {url!r} "
            f"(last status: {status_code})"
        )


# ---------------------------------------------------------------------------
# Response dataclass
# ---------------------------------------------------------------------------


@dataclass
class CurlResponse:
    """Normalised response returned by :class:`CurlClient`.

    We intentionally avoid exposing the raw curl_cffi response object so that
    callers remain decoupled from the underlying transport library.
    """

    status_code: int
    text: str
    url: str          # final URL after any redirects
    headers: dict[str, str] = field(default_factory=dict)


# ---------------------------------------------------------------------------
# Client
# ---------------------------------------------------------------------------


class CurlClient:
    """Async HTTP client using curl_cffi with Chrome 131 TLS impersonation.

    Suitable for anti-bot protected targets (Google Search, lightly-protected
    corporate sites).  NOT suitable for heavy DataDome/Cloudflare Turnstile
    targets — use the Nodriver / Camoufox engine for those.

    Rate limiting
    -------------
    A minimum inter-request delay is enforced per session instance.  The delay
    is drawn uniformly from [settings.delay_between_requests_min,
    settings.delay_between_requests_max] plus a jitter component.

    Retry policy
    ------------
    Connection errors, timeouts, and HTTP 429/503 responses trigger an
    exponential backoff retry (up to settings.max_retries attempts).
    HTTP 401/403/404 responses are returned immediately without retrying.
    On total exhaustion :class:`CurlClientError` is raised.
    """

    def __init__(
        self,
        timeout: float | None = None,
        max_retries: int | None = None,
        delay_min: float | None = None,
        delay_max: float | None = None,
        delay_jitter: float | None = None,
    ) -> None:
        self._timeout = timeout or float(settings.request_timeout)
        self._max_retries = max_retries if max_retries is not None else settings.max_retries
        self._delay_min = delay_min if delay_min is not None else settings.delay_between_requests_min
        self._delay_max = delay_max if delay_max is not None else settings.delay_between_requests_max
        self._delay_jitter = delay_jitter if delay_jitter is not None else settings.delay_jitter
        self._session: AsyncSession | None = None
        self._last_request_at: float = 0.0

    # ------------------------------------------------------------------
    # Session lifecycle
    # ------------------------------------------------------------------

    async def _get_session(self) -> AsyncSession:
        """Lazily create and return the underlying curl_cffi AsyncSession."""
        if self._session is None:
            self._session = AsyncSession(impersonate="chrome131")
        return self._session

    async def close(self) -> None:
        """Close the underlying curl_cffi session and release resources."""
        if self._session is not None:
            await self._session.close()
            self._session = None

    # ------------------------------------------------------------------
    # Context manager
    # ------------------------------------------------------------------

    async def __aenter__(self) -> "CurlClient":
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Rate limiting helper
    # ------------------------------------------------------------------

    async def _enforce_rate_limit(self) -> None:
        """Sleep if the minimum inter-request delay has not yet elapsed."""
        desired_delay = random.uniform(self._delay_min, self._delay_max) + random.uniform(0, self._delay_jitter)
        elapsed = time.monotonic() - self._last_request_at
        sleep_for = desired_delay - elapsed
        if sleep_for > 0:
            logger.debug("curl_client.rate_limit_sleep", sleep_seconds=round(sleep_for, 2))
            await asyncio.sleep(sleep_for)

    # ------------------------------------------------------------------
    # Header helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_headers(extra: dict[str, str] | None) -> dict[str, str]:
        """Merge DEFAULT_HEADERS + a random User-Agent + any caller-supplied headers."""
        ua = random.choice(USER_AGENTS)
        headers = {**DEFAULT_HEADERS, "User-Agent": ua}
        if extra:
            headers.update(extra)
        return headers

    # ------------------------------------------------------------------
    # Core request dispatcher
    # ------------------------------------------------------------------

    async def _request(
        self,
        method: str,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
        **kwargs: object,
    ) -> CurlResponse:
        """Execute an HTTP request with retry + exponential backoff.

        Parameters
        ----------
        method:  HTTP verb ("GET", "POST", …).
        url:     Target URL.
        headers: Optional extra headers merged on top of DEFAULT_HEADERS.
        timeout: Per-request timeout override in seconds.
        **kwargs: Forwarded verbatim to curl_cffi (e.g. ``data=``, ``json=``).

        Returns
        -------
        :class:`CurlResponse` — always; never raises on non-200 status.

        Raises
        ------
        :class:`CurlClientError` — when all retry attempts are exhausted.
        """
        session = await self._get_session()
        effective_timeout = timeout or self._timeout
        merged_headers = self._build_headers(headers)

        last_status: int = 0
        base_delay: float = 2.0
        max_delay: float = 30.0

        for attempt in range(1, self._max_retries + 1):
            await self._enforce_rate_limit()

            log = logger.bind(url=url, method=method, attempt=attempt)

            try:
                raw = await session.request(
                    method,
                    url,
                    headers=merged_headers,
                    timeout=effective_timeout,
                    **kwargs,  # type: ignore[arg-type]
                )
                self._last_request_at = time.monotonic()
                last_status = raw.status_code

                log.debug(
                    "curl_client.response",
                    status=last_status,
                    final_url=str(raw.url),
                )

                # Non-retryable statuses — return immediately
                if last_status in NON_RETRYABLE_STATUSES:
                    log.info("curl_client.non_retryable_status", status=last_status)
                    return CurlResponse(
                        status_code=last_status,
                        text=raw.text,
                        url=str(raw.url),
                        headers=dict(raw.headers),
                    )

                # Retryable HTTP status
                if last_status in RETRYABLE_STATUSES:
                    if attempt < self._max_retries:
                        delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                        log.warning(
                            "curl_client.retrying",
                            status=last_status,
                            retry_delay=delay,
                        )
                        await asyncio.sleep(delay)
                        continue
                    else:
                        break  # Exhausted — exit loop, raise CurlClientError below

                # Success or any other non-retryable status — return immediately
                return CurlResponse(
                    status_code=last_status,
                    text=raw.text,
                    url=str(raw.url),
                    headers=dict(raw.headers),
                )

            except Exception as exc:
                # Connection errors and timeouts
                self._last_request_at = time.monotonic()
                if attempt < self._max_retries:
                    delay = min(base_delay * (2 ** (attempt - 1)), max_delay)
                    log.warning(
                        "curl_client.connection_error",
                        error=str(exc),
                        retry_delay=delay,
                    )
                    await asyncio.sleep(delay)
                else:
                    log.error("curl_client.exhausted", error=str(exc))

        raise CurlClientError(url, last_status)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def get(
        self,
        url: str,
        *,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> CurlResponse:
        """Perform an async GET request.

        Parameters
        ----------
        url:     Target URL.
        headers: Extra headers merged with DEFAULT_HEADERS. Optional.
        timeout: Override the default session timeout. Optional.

        Returns
        -------
        :class:`CurlResponse`

        Raises
        ------
        :class:`CurlClientError` — on total retry exhaustion.
        """
        return await self._request("GET", url, headers=headers, timeout=timeout)

    async def post(
        self,
        url: str,
        *,
        data: dict[str, str] | None = None,
        json: dict[str, object] | None = None,
        headers: dict[str, str] | None = None,
        timeout: float | None = None,
    ) -> CurlResponse:
        """Perform an async POST request.

        Parameters
        ----------
        url:     Target URL.
        data:    Form-encoded body. Mutually exclusive with ``json``.
        json:    JSON-serialisable body. Mutually exclusive with ``data``.
        headers: Extra headers merged with DEFAULT_HEADERS. Optional.
        timeout: Per-request timeout override in seconds. Optional.

        Returns
        -------
        :class:`CurlResponse`

        Raises
        ------
        :class:`CurlClientError` — on total retry exhaustion.
        """
        kwargs: dict[str, object] = {}
        if data is not None:
            kwargs["data"] = data
        if json is not None:
            kwargs["json"] = json
        return await self._request("POST", url, headers=headers, timeout=timeout, **kwargs)
