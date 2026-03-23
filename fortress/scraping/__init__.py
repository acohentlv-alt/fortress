"""Scraping — Transport layer.

Provides the HTTP client used across the pipeline:

    CurlClient          curl_cffi-backed, Chrome TLS impersonation.
                        Use for: Google Search, corporate website crawls, any
                        target with basic anti-bot protection.

    PlaywrightMapsScraper
                        Chromium stealth engine for Google Maps extraction.

Exceptions:

    CurlClientError     Raised when CurlClient exhausts all retries.
"""

from fortress.scraping.http import CurlClient, CurlClientError, CurlResponse

__all__ = [
    "CurlClient",
    "CurlResponse",
    "CurlClientError",
]
