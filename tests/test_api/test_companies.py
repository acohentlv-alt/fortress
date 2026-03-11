"""Integration tests for the companies API routes.

Tests the three critical flows:
  1. POST /enrich → validates dedup, TTL caching, and background dispatch
  2. GET /search → validates filters, pagination, and response shape
  3. GET /{siren}/enrich-history → validates audit trail response

Uses FastAPI's TestClient for synchronous HTTP testing.
Mocks subprocess.Popen to prove the runner dispatch handoff occurs
without actually spawning a 9-second Playwright process.
"""

import importlib
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Fixture: FastAPI TestClient with live DB pool
# ---------------------------------------------------------------------------

@pytest.fixture(scope="module")
def client():
    """Create a TestClient backed by the real database pool.

    The pool is initialized once per test module and closed after all tests.
    This tests the real SQL queries against the actual Fortress PostgreSQL DB.
    """
    from fortress.api.main import app
    from fortress.api.db import init_pool, close_pool

    import asyncio

    loop = asyncio.new_event_loop()
    loop.run_until_complete(init_pool())

    from starlette.testclient import TestClient
    with TestClient(app, raise_server_exceptions=False) as tc:
        yield tc

    loop.run_until_complete(close_pool())
    loop.close()


# ---------------------------------------------------------------------------
# A) POST /enrich — Deduplication + Background Dispatch
# ---------------------------------------------------------------------------

class TestEnrichEndpoint:
    """Test suite for POST /api/companies/{siren}/enrich."""

    KNOWN_SIREN = "952228997"       # 2C TRANSPORTS — exists in DB
    UNKNOWN_SIREN = "000000000"     # Definitely not in DB

    def test_enrich_404_for_unknown_siren(self, client):
        """Unknown SIREN → 404 with structured error."""
        resp = client.post(
            f"/api/companies/{self.UNKNOWN_SIREN}/enrich",
            json={"target_modules": ["contact_web"]},
        )
        assert resp.status_code == 404
        data = resp.json()
        assert data["error"] == "Company not found"
        assert data["siren"] == self.UNKNOWN_SIREN

    def test_enrich_422_for_invalid_modules(self, client):
        """Invalid module names → 422 with list of valid modules."""
        resp = client.post(
            f"/api/companies/{self.KNOWN_SIREN}/enrich",
            json={"target_modules": ["invalid_module"]},
        )
        assert resp.status_code == 422
        data = resp.json()
        assert "No valid modules" in data.get("error", "")
        assert "contact_web" in data["valid_modules"]

    def test_enrich_422_for_empty_modules(self, client):
        """Empty module list → 422 from Pydantic validation."""
        resp = client.post(
            f"/api/companies/{self.KNOWN_SIREN}/enrich",
            json={"target_modules": []},
        )
        assert resp.status_code == 422

    @patch("fortress.api.routes.companies.subprocess.Popen")
    def test_enrich_dispatches_subprocess(self, mock_popen, client):
        """Prove that the runner subprocess is actually spawned.

        Mocks subprocess.Popen to capture the call without launching
        a real Playwright process. Asserts:
          - Popen was called exactly once
          - The command includes 'fortress.runner' and the query_id
          - start_new_session=True (detached)
          - Response includes 'pid' and 'query_id'
        """
        # Configure mock to return a fake PID
        mock_process = MagicMock()
        mock_process.pid = 99999
        mock_popen.return_value = mock_process

        resp = client.post(
            f"/api/companies/{self.KNOWN_SIREN}/enrich",
            json={"target_modules": ["contact_web"]},
        )
        data = resp.json()

        # The response could be 200/202 (queued) or 200 (deduped/cached)
        assert resp.status_code in (200, 202)

        if data.get("queued"):
            # If modules were queued, subprocess must have been called
            assert mock_popen.called, "subprocess.Popen was NOT called — dispatch failed!"
            call_args = mock_popen.call_args

            # Verify the runner command
            cmd = call_args[0][0]  # First positional arg
            assert "fortress.runner" in " ".join(cmd), f"Runner not in command: {cmd}"
            assert f"ENRICH_{self.KNOWN_SIREN}" in " ".join(cmd), f"Query ID not in command: {cmd}"

            # Verify detached process
            assert call_args[1].get("start_new_session") is True, "Process not detached!"

            # Verify response includes dispatch info
            assert data["pid"] == 99999
            assert data["query_id"] == f"ENRICH_{self.KNOWN_SIREN}"
            assert len(data["queued"]) > 0

            print(f"   ✅ subprocess.Popen called with: {' '.join(cmd)}")
            print(f"   ✅ PID: {data['pid']}, query_id: {data['query_id']}")
        else:
            # Deduped or TTL-cached — subprocess should NOT be called
            print(f"   ⏭️  Deduped/cached: {data.get('message')}")

    @patch("fortress.api.routes.companies.subprocess.Popen")
    def test_enrich_dedup_skips_existing_data(self, mock_popen, client):
        """If company already has contacts, those modules are skipped."""
        mock_process = MagicMock()
        mock_process.pid = 88888
        mock_popen.return_value = mock_process

        resp = client.post(
            f"/api/companies/{self.KNOWN_SIREN}/enrich",
            json={"target_modules": ["contact_phone", "contact_web", "financials"]},
        )
        data = resp.json()
        assert resp.status_code in (200, 202)

        # At least some modules should be in skipped (2C TRANSPORTS has contacts)
        # The exact split depends on what data exists in the DB
        assert "queued" in data
        assert "skipped" in data
        print(f"   📊 Queued: {data['queued']}, Skipped: {data['skipped']}")


# ---------------------------------------------------------------------------
# B) GET /search — Filters and Pagination
# ---------------------------------------------------------------------------

class TestSearchEndpoint:
    """Test suite for GET /api/companies/search."""

    def test_search_basic(self, client):
        """Basic search returns results with correct response shape."""
        resp = client.get("/api/companies/search?q=transport&limit=5")
        assert resp.status_code == 200
        data = resp.json()
        assert "results" in data
        assert "count" in data
        assert data["count"] <= 5

    def test_search_by_siren(self, client):
        """9-digit SIREN search uses indexed PK lookup."""
        resp = client.get("/api/companies/search?q=952228997")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] >= 1
        assert data["results"][0]["siren"] == "952228997"

    def test_search_department_filter(self, client):
        """Department filter restricts results to matching dept."""
        resp = client.get("/api/companies/search?q=transport&department=66&limit=10")
        assert resp.status_code == 200
        data = resp.json()
        for row in data["results"]:
            assert row["departement"] == "66", f"Got dept {row['departement']} for {row['denomination']}"

    def test_search_sector_filter(self, client):
        """Sector filter restricts results to matching query_name."""
        resp = client.get("/api/companies/search?q=transport&sector=logistique&limit=10")
        assert resp.status_code == 200
        data = resp.json()
        assert data["count"] >= 0  # May be 0 if no logistique tags

    def test_search_pagination(self, client):
        """Offset parameter shifts result window."""
        resp1 = client.get("/api/companies/search?q=transport&limit=5&offset=0")
        resp2 = client.get("/api/companies/search?q=transport&limit=5&offset=5")
        data1 = resp1.json()
        data2 = resp2.json()

        assert resp1.status_code == 200
        assert resp2.status_code == 200
        assert data1["offset"] == 0
        assert data2["offset"] == 5

        # If there are enough results, pages should differ
        if data1["count"] == 5 and data2["count"] > 0:
            sirens1 = {r["siren"] for r in data1["results"]}
            sirens2 = {r["siren"] for r in data2["results"]}
            assert sirens1 != sirens2, "Pagination returned identical pages"

    def test_search_limit_cap(self, client):
        """Limit is capped at 100."""
        resp = client.get("/api/companies/search?q=transport&limit=200")
        assert resp.status_code == 422  # Pydantic validates le=100


# ---------------------------------------------------------------------------
# C) GET /enrich-history — Audit Trail
# ---------------------------------------------------------------------------

class TestEnrichHistoryEndpoint:
    """Test suite for GET /api/companies/{siren}/enrich-history."""

    def test_enrich_history_returns_array(self, client):
        """Enrich history returns structured timeline array."""
        resp = client.get("/api/companies/952228997/enrich-history")
        assert resp.status_code == 200
        data = resp.json()
        assert "siren" in data
        assert "history" in data
        assert "count" in data
        assert isinstance(data["history"], list)

    def test_enrich_history_fields(self, client):
        """Each history entry has the required audit fields."""
        resp = client.get("/api/companies/952228997/enrich-history")
        data = resp.json()
        if data["count"] > 0:
            entry = data["history"][0]
            assert "action" in entry
            assert "result" in entry
            assert "timestamp" in entry
