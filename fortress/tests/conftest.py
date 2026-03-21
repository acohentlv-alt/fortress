"""Fortress Test Infrastructure — Real DB + TestClient + Auth Fixtures.

Two test modes:
  1. INTEGRATION: hits the real Neon DB (read-only queries, no mutations)
  2. MOCK: uses mocked DB for fast unit tests

Usage:
  pytest fortress/tests/ -v --timeout=30
"""

import os
import pytest
import pytest_asyncio
import httpx
from unittest.mock import patch, MagicMock, AsyncMock

# ── Ensure DATABASE_URL is set for integration tests ─────────────────
NEON_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_1bgBYwTSa5UP@ep-noisy-tree-agzjuw4w-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require",
)
os.environ["DATABASE_URL"] = NEON_URL


# ── 1. Real DB connection fixture (integration tests) ────────────────

@pytest_asyncio.fixture(scope="session")
async def db_conn():
    """A single real psycopg async connection to Neon, shared across all tests.
    
    All queries should be READ-ONLY or wrapped in a SAVEPOINT+ROLLBACK.
    """
    import psycopg
    conn = await psycopg.AsyncConnection.connect(NEON_URL, row_factory=psycopg.rows.dict_row)
    yield conn
    await conn.close()


# ── 2. FastAPI TestClient fixture (real app, mocked pool) ────────────

@pytest_asyncio.fixture(scope="session")
async def app_client():
    """Async httpx client for the real Fortress FastAPI app.
    
    - Uses the real lifespan (connects to Neon DB)
    - Uses the real middleware, real routes
    - Auth: admin session cookie injected automatically
    """
    from fortress.api.auth import create_session_token
    from fortress.api.main import app

    # Create admin token for tests
    admin_token = create_session_token(user_id=1, username="test_admin", role="admin")

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://testserver",
        cookies={"fortress_session": admin_token},
    ) as client:
        yield client


# ── 3. Auth helper fixtures ──────────────────────────────────────────

@pytest.fixture
def admin_cookie():
    """Returns a valid admin session cookie value."""
    from fortress.api.auth import create_session_token
    return create_session_token(user_id=1, username="test_admin", role="admin")


@pytest.fixture
def user_cookie():
    """Returns a valid non-admin session cookie value."""
    from fortress.api.auth import create_session_token
    return create_session_token(user_id=2, username="test_user", role="user")


# ── 4. Mock DB fixture (for unit tests that don't need real DB) ──────

@pytest.fixture
def mock_db():
    """Provides mock DB functions for fast unit tests."""
    m_fetch_one = AsyncMock(return_value=None)
    m_fetch_all = AsyncMock(return_value=[])
    m_conn = AsyncMock()
    m_cursor = AsyncMock()
    
    m_cursor_acm = MagicMock()
    m_cursor_acm.__aenter__ = AsyncMock(return_value=m_cursor)
    m_cursor_acm.__aexit__ = AsyncMock(return_value=None)
    m_conn.cursor = MagicMock(return_value=m_cursor_acm)
    
    m_cm = MagicMock()
    m_cm.__aenter__ = AsyncMock(return_value=m_conn)
    m_cm.__aexit__ = AsyncMock(return_value=None)
    
    return {
        "fetch_one": m_fetch_one,
        "fetch_all": m_fetch_all,
        "conn": m_conn,
        "cursor": m_cursor,
        "get_conn": MagicMock(return_value=m_cm),
    }


# ── 5. Backend URL for live server tests (Playwright) ────────────────

LIVE_URL = os.environ.get("FORTRESS_URL", "https://fortress-m4sd.onrender.com")

@pytest.fixture(scope="session")
def live_url():
    """Base URL of the running Fortress instance (for Playwright E2E tests)."""
    return LIVE_URL


# ── 6. Session-scoped asyncio backend ────────────────────────────────

@pytest.fixture(scope="session")
def anyio_backend():
    return "asyncio"
