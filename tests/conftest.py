"""Shared pytest fixtures for the Fortress test suite."""
from __future__ import annotations

import pytest
import pytest_asyncio
from unittest.mock import AsyncMock, MagicMock


@pytest.fixture
def mock_curl_client():
    """CurlClient with mocked get() and post() — no real HTTP calls."""
    client = MagicMock()
    client.get = AsyncMock()
    client.post = AsyncMock()
    # Make it work as async context manager if needed
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)
    return client


@pytest_asyncio.fixture
async def test_pool():
    """Async psycopg3 connection pool for fortress_test DB.

    Requires fortress_test database to exist. Truncates all tables on teardown.
    Set TEST_DB_URL env var or use default localhost fortress_test.
    """
    import os
    import psycopg_pool
    from fortress.config.settings import settings

    db_url = os.environ.get("TEST_DB_URL") or settings.effective_db_url.replace(
        "/fortress", "/fortress_test"
    )

    pool = psycopg_pool.AsyncConnectionPool(
        db_url,
        min_size=1,
        max_size=2,
        open=False,
    )
    await pool.open()
    yield pool
    await pool.close()
