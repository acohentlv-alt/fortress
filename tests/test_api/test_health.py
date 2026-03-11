"""Infrastructure resilience tests — health endpoint and DB failure handling.

Tests:
  A) HEALTHY: Real DB connected → GET /api/health returns 200 + "connected"
  B) DEGRADED: Simulated offline → health returns 503 + error, app stays alive
  C) init_pool() failure → catches exception, sets pool=None, no crash
"""

import asyncio
from unittest.mock import patch

import pytest


@pytest.fixture(scope="module")
def client():
    """TestClient with real DB pool (Docker fortress-db must be running)."""
    from fortress.api.main import app
    from fortress.api.db import init_pool, close_pool

    loop = asyncio.new_event_loop()
    loop.run_until_complete(init_pool())

    from starlette.testclient import TestClient
    with TestClient(app, raise_server_exceptions=False) as tc:
        yield tc

    loop.run_until_complete(close_pool())
    loop.close()


# ---------------------------------------------------------------------------
# A) HEALTHY — database connected
# ---------------------------------------------------------------------------

class TestHealthyScenario:

    def test_health_returns_200(self, client):
        resp = client.get("/api/health")
        assert resp.status_code == 200
        data = resp.json()
        assert data["status"] == "ok"
        assert data["database"] == "connected"
        print(f"   ✅ Health: {data}")

    def test_search_works(self, client):
        resp = client.get("/api/companies/search?q=transport&limit=1")
        assert resp.status_code == 200
        print(f"   ✅ Search OK ({resp.json()['count']} results)")

    def test_health_no_error(self, client):
        resp = client.get("/api/health")
        assert "error" not in resp.json()


# ---------------------------------------------------------------------------
# B) DEGRADED — simulate database offline via pool manipulation
# ---------------------------------------------------------------------------

class TestDegradedScenario:

    def test_health_503_when_offline(self, client):
        """Simulate DB offline → health must return 503 + degraded."""
        from fortress.api import db as db_mod
        saved_pool, saved_err = db_mod._pool, db_mod._pool_error
        try:
            db_mod._pool = None
            db_mod._pool_error = 'connection refused: Is the server running?'

            resp = client.get("/api/health")
            assert resp.status_code == 503
            data = resp.json()
            assert data["status"] == "degraded"
            assert data["database"] == "offline"
            assert "connection refused" in data["error"]
            print(f"   ✅ Degraded health: {data}")
        finally:
            db_mod._pool, db_mod._pool_error = saved_pool, saved_err

    def test_data_endpoint_500_when_offline(self, client):
        """Data endpoints return 500 (not crash) when pool is None."""
        from fortress.api import db as db_mod
        saved_pool, saved_err = db_mod._pool, db_mod._pool_error
        try:
            db_mod._pool = None
            db_mod._pool_error = "simulated offline"

            resp = client.get("/api/companies/search?q=test&limit=1")
            assert resp.status_code == 500
            print(f"   ✅ Data endpoint: HTTP {resp.status_code} (not crash)")
        finally:
            db_mod._pool, db_mod._pool_error = saved_pool, saved_err

    def test_recovery_after_restore(self, client):
        """After restore, health returns 200 again."""
        resp = client.get("/api/health")
        assert resp.status_code == 200
        print(f"   ✅ Recovered: {resp.json()}")


# ---------------------------------------------------------------------------
# C) init_pool() graceful failure — actual bad URL
# ---------------------------------------------------------------------------

class TestInitPoolFailure:

    def test_bad_url_no_crash(self):
        """init_pool() with a bad conninfo sets pool=None, no crash."""
        from fortress.api import db as db_mod

        saved_pool, saved_err = db_mod._pool, db_mod._pool_error
        loop = asyncio.new_event_loop()
        try:
            # Patch settings.db_url at the point init_pool reads it
            bad_url = "postgresql://x:x@127.0.0.1:1/nonexistent"
            with patch.object(type(db_mod.settings), "db_url",
                              property(fget=lambda self: bad_url)):
                loop.run_until_complete(db_mod.init_pool())

            assert db_mod._pool is None, "Pool should be None"
            assert db_mod._pool_error is not None
            assert len(db_mod._pool_error) > 0
            print(f"   ✅ Graceful failure: {db_mod._pool_error[:80]}...")
        finally:
            db_mod._pool, db_mod._pool_error = saved_pool, saved_err
            loop.close()

    def test_pool_status_when_offline(self):
        """pool_status() reports offline after failed init."""
        from fortress.api.db import pool_status
        from fortress.api import db as db_mod

        saved_pool, saved_err = db_mod._pool, db_mod._pool_error
        try:
            db_mod._pool = None
            db_mod._pool_error = "test: connection refused"
            status = pool_status()
            assert status["connected"] is False
            assert status["error"] == "test: connection refused"
            print(f"   ✅ pool_status: {status}")
        finally:
            db_mod._pool, db_mod._pool_error = saved_pool, saved_err
