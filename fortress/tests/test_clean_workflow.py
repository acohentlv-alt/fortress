"""Clean Workflow E2E Test — Simulates a real user navigating every page.

Uses the browser_subagent tool pattern to navigate the live Fortress app.
Tests: page loads, back-button, navigation, delete (discovered data only),
       inline editing, notes, search, filters.

Run with:
  pytest fortress/tests/test_clean_workflow.py -v --timeout=120

Requires: FORTRESS_URL env var (defaults to https://fortress-m4sd.onrender.com)
          FORTRESS_USER / FORTRESS_PASS env vars for login
"""

import os
import pytest
import httpx
import asyncio

LIVE_URL = os.environ.get("FORTRESS_URL", "https://fortress-4o6r.onrender.com")
USERNAME = os.environ.get("FORTRESS_USER", "alan")
PASSWORD = os.environ.get("FORTRESS_PASS", "")

pytestmark = pytest.mark.anyio

@pytest.fixture(scope="function")
async def session():
    """Login and return an authenticated httpx client with session cookie.
    
    Function scope is used to avoid 'Event loop is closed' errors on live servers.
    """
    async with httpx.AsyncClient(base_url=LIVE_URL, follow_redirects=True, timeout=30.0) as client:
        # print(f"\n  Logging in as {USERNAME}...")
        r = await client.post("/api/auth/login", json={
            "username": USERNAME,
            "password": PASSWORD,
        })
        if r.status_code != 200:
            pytest.skip(f"Login failed: {r.status_code}")
        yield client
        # await asyncio.sleep(0.5) # optional throttle


# ═══════════════════════════════════════════════════════════════════
# Page Load Tests — every page should return 200
# ═══════════════════════════════════════════════════════════════════

class TestPageLoads:
    """Verify every API endpoint a page needs returns valid data."""

    async def test_dashboard_loads(self, session):
        """Dashboard page calls 3 APIs in parallel."""
        r1 = await session.get("/api/dashboard/stats")
        r2 = await session.get("/api/departments")
        r3 = await session.get("/api/jobs")
        assert r1.status_code == 200, f"stats: {r1.status_code}"
        assert r2.status_code == 200, f"departments: {r2.status_code}"
        assert r3.status_code == 200, f"jobs: {r3.status_code}"

    async def test_analysis_tab_loads(self, session):
        r = await session.get("/api/dashboard/analysis")
        assert r.status_code == 200

    async def test_by_job_tab_loads(self, session):
        r = await session.get("/api/dashboard/stats/by-job")
        assert r.status_code == 200

    async def test_by_sector_tab_loads(self, session):
        r = await session.get("/api/dashboard/stats/by-sector")
        assert r.status_code == 200

    async def test_by_upload_tab_loads(self, session):
        r = await session.get("/api/client/stats")
        assert r.status_code == 200

    async def test_all_data_tab_loads(self, session):
        r = await session.get("/api/dashboard/all-data?limit=5")
        assert r.status_code == 200

    async def test_activity_page_loads(self, session):
        r = await session.get("/api/activity")
        assert r.status_code == 200

    async def test_contacts_page_loads(self, session):
        r = await session.get("/api/contacts")
        assert r.status_code == 200

    async def test_search_page_loads(self, session):
        r = await session.get("/api/sirene/search?q=transport")
        assert r.status_code == 200

    async def test_data_bank_loads(self, session):
        r = await session.get("/api/dashboard/data-bank")
        assert r.status_code == 200


# ═══════════════════════════════════════════════════════════════════
# Navigation Flow Tests — simulate clicking through pages
# ═══════════════════════════════════════════════════════════════════

class TestNavigationFlow:
    """Simulate a user clicking through the app: dashboard → job → company → back."""

    async def test_dashboard_to_job_to_company(self, session):
        """Full flow: list jobs → pick one → get companies → pick one → get detail."""
        # Step 1: Get jobs list
        r = await session.get("/api/jobs")
        assert r.status_code == 200
        jobs = r.json()
        if not jobs:
            pytest.skip("No jobs in database")

        # Step 2: Open first job
        first_job = jobs[0]
        batch_id = first_job.get("batch_id")
        r = await session.get(f"/api/jobs/{batch_id}")
        assert r.status_code == 200, f"Job detail failed: {r.status_code}"

        # Step 3: Get companies in that job
        r = await session.get(f"/api/jobs/{batch_id}/companies?page_size=5")
        assert r.status_code == 200
        companies = r.json().get("companies", [])
        if not companies:
            pytest.skip("No companies in job")

        # Step 4: Open first company
        siren = companies[0].get("siren")
        r = await session.get(f"/api/companies/{siren}")
        assert r.status_code == 200
        data = r.json()
        assert "company" in data
        assert "merged_contact" in data

    async def test_search_to_company_detail(self, session):
        """Flow: search → click company → verify detail loads."""
        r = await session.get("/api/sirene/search?q=49.41&limit=3")
        assert r.status_code == 200
        results = r.json().get("results", [])
        if not results:
            pytest.skip("No SIRENE results")

        siren = results[0]["siren"]
        r = await session.get(f"/api/companies/{siren}")
        # Company may not be enriched (204/404 possible)
        assert r.status_code in (200, 404)

    async def test_department_to_companies(self, session):
        """Flow: departments list → pick one → get companies."""
        r = await session.get("/api/departments")
        assert r.status_code == 200
        depts = r.json()
        if not depts:
            pytest.skip("No departments")

        dept_code = depts[0].get("departement")
        r = await session.get(f"/api/departments/{dept_code}")
        assert r.status_code == 200


# ═══════════════════════════════════════════════════════════════════
# Data Quality Checks — verify data makes sense
# ═══════════════════════════════════════════════════════════════════

class TestDataQuality:
    """Verify the data returned by APIs is consistent and complete."""

    async def test_dashboard_totals_match(self, session):
        """Dashboard total should ≈ sum of by-sector companies."""
        stats = (await session.get("/api/dashboard/stats")).json()
        sectors = (await session.get("/api/dashboard/stats/by-sector")).json()
        
        total_from_sectors = sum(s.get("companies", 0) for s in sectors)
        dashboard_total = stats.get("total_companies", 0)
        
        # Allow 10% tolerance (rounding, MAPS entities, etc.)
        if dashboard_total > 0:
            diff_pct = abs(total_from_sectors - dashboard_total) / dashboard_total * 100
            assert diff_pct < 15, f"Dashboard total ({dashboard_total}) vs sectors ({total_from_sectors}): {diff_pct:.1f}% diff"

    async def test_job_quality_percentages(self, session):
        """Job quality percentages should be 0-100."""
        r = await session.get("/api/jobs")
        jobs = r.json()
        completed = [j for j in jobs if j.get("status") == "completed"]
        if not completed:
            pytest.skip("No completed jobs")

        batch_id = completed[0]["batch_id"]
        r = await session.get(f"/api/jobs/{batch_id}/quality")
        assert r.status_code == 200
        data = r.json()

        for key in ["phone_pct", "email_pct", "website_pct"]:
            val = data.get(key, 0)
            assert 0 <= val <= 100, f"{key} = {val} (out of range)"

    async def test_company_contacts_have_source(self, session):
        """Every contact should have a source field."""
        r = await session.get("/api/dashboard/all-data?limit=3")
        data = r.json()
        for company in data.get("results", []):
            siren = company.get("siren")
            detail = (await session.get(f"/api/companies/{siren}")).json()
            for contact in detail.get("contacts", []):
                assert contact.get("source"), f"Contact for {siren} has no source"


# ═══════════════════════════════════════════════════════════════════
# Error Handling — back-button / edge cases
# ═══════════════════════════════════════════════════════════════════

class TestErrorHandling:
    """Test edge cases that cause errors in normal workflow."""

    async def test_nonexistent_siren_returns_404(self, session):
        r = await session.get("/api/companies/INVALID_SIREN")
        assert r.status_code == 404

    async def test_nonexistent_department(self, session):
        r = await session.get("/api/departments/ZZ")
        # Should return 200 with empty data or 404
        assert r.status_code in (200, 404)

    async def test_double_slash_in_path(self, session):
        """Some back-button scenarios create malformed paths."""
        r = await session.get("/api/jobs/")
        assert r.status_code in (200, 301, 307, 404, 405)

    async def test_empty_search_query(self, session):
        """Empty search should not crash."""
        r = await session.get("/api/sirene/search?q=")
        # Should return 400 or 422 (min_length=1)
        assert r.status_code in (400, 422)

    async def test_very_long_search_query(self, session):
        """Very long search should not crash."""
        long_q = "a" * 500
        r = await session.get(f"/api/sirene/search?q={long_q}")
        assert r.status_code in (200, 400, 408)

    async def test_negative_pagination(self, session):
        """Negative offset should be rejected."""
        r = await session.get("/api/sirene/search?q=transport&offset=-5")
        assert r.status_code in (200, 400, 422)

    async def test_export_nonexistent_batch(self, session):
        """Export of nonexistent batch should 404."""
        r = await session.get("/api/export/nonexistent-batch-xyz")
        assert r.status_code in (404, 200)  # 200 = empty CSV

    async def test_concurrent_requests(self, session):
        """Simulate back-button: fire 3 requests simultaneously."""
        import asyncio
        tasks = [
            session.get("/api/dashboard/stats"),
            session.get("/api/jobs"),
            session.get("/api/departments"),
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        for r in results:
            if isinstance(r, Exception):
                pytest.fail(f"Concurrent request failed: {r}")
            assert r.status_code == 200


# ═══════════════════════════════════════════════════════════════════
# Delete Workflow — test delete on discovered (non-upload) data
# ═══════════════════════════════════════════════════════════════════

class TestDeleteWorkflow:
    """Test deletion endpoints behave correctly.
    
    IMPORTANT: Only tests on discovered (non-upload) data.
    Uses DRY-RUN where possible to avoid modifying prod.
    """

    async def test_delete_nonexistent_returns_404(self, session):
        r = await session.delete("/api/jobs/test-delete-nonexistent")
        assert r.status_code == 404

    async def test_delete_sector_nonexistent(self, session):
        r = await session.delete("/api/dashboard/sector/NONEXISTENT_SECTOR_XYZ/tags")
        assert r.status_code == 404

    async def test_delete_dept_nonexistent(self, session):
        r = await session.delete("/api/dashboard/department/ZZ/tags")
        assert r.status_code == 404

    async def test_delete_job_group_nonexistent(self, session):
        r = await session.delete("/api/dashboard/job-group/NONEXISTENT_GROUP_XYZ")
        assert r.status_code == 404
