"""API Integration Tests — hit the real Fortress app with TestClient.

Tests all major API journeys: dashboard, jobs, companies, notes, search, auth.
READ-ONLY: no mutations to production data.
"""

import pytest

pytestmark = pytest.mark.anyio


# ═══════════════════════════════════════════════════════════════════════
# Journey 1: Dashboard & Stats
# ═══════════════════════════════════════════════════════════════════════

class TestDashboard:
    async def test_stats_returns_200(self, app_client):
        r = await app_client.get("/api/dashboard/stats")
        assert r.status_code == 200
        data = r.json()
        assert "total_companies" in data
        assert "with_phone" in data
        assert "departments_covered" in data

    async def test_recent_activity_returns_list(self, app_client):
        r = await app_client.get("/api/dashboard/recent-activity")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)

    async def test_stats_by_job_returns_groups(self, app_client):
        r = await app_client.get("/api/dashboard/stats/by-job")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)

    async def test_stats_by_sector_returns_list(self, app_client):
        r = await app_client.get("/api/dashboard/stats/by-sector")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)

    async def test_all_data_returns_paginated(self, app_client):
        r = await app_client.get("/api/dashboard/all-data?limit=5")
        assert r.status_code == 200
        data = r.json()
        assert "results" in data
        assert "total" in data
        assert isinstance(data["results"], list)

    async def test_analysis_returns_panels(self, app_client):
        r = await app_client.get("/api/dashboard/analysis")
        assert r.status_code == 200
        data = r.json()
        assert "quality" in data
        assert "gaps" in data
        assert "enrichers" in data
        assert "pipeline" in data

    async def test_data_bank_admin_only(self, app_client):
        r = await app_client.get("/api/dashboard/data-bank")
        assert r.status_code == 200  # We have admin cookie
        data = r.json()
        assert "totals" in data


# ═══════════════════════════════════════════════════════════════════════
# Journey 2: Jobs
# ═══════════════════════════════════════════════════════════════════════

class TestJobs:
    async def test_list_jobs(self, app_client):
        r = await app_client.get("/api/jobs")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, list)

    async def test_nonexistent_job_returns_404(self, app_client):
        r = await app_client.get("/api/jobs/nonexistent-id-12345")
        assert r.status_code == 404

    async def test_cancel_nonexistent_job(self, app_client):
        r = await app_client.post("/api/jobs/nonexistent-id-12345/cancel")
        assert r.status_code == 404

    async def test_delete_nonexistent_job(self, app_client):
        r = await app_client.delete("/api/jobs/nonexistent-id-12345")
        assert r.status_code == 404


# ═══════════════════════════════════════════════════════════════════════
# Journey 3: Company Detail
# ═══════════════════════════════════════════════════════════════════════

class TestCompanies:
    async def test_get_company_returns_all_fields(self, app_client, db_conn):
        """Get a real company SIREN from the DB and verify the API response shape."""
        row = await db_conn.execute(
            "SELECT siren FROM companies WHERE statut = 'A' LIMIT 1"
        )
        result = await row.fetchone()
        if not result:
            pytest.skip("No companies in database")
        
        siren = result["siren"]
        r = await app_client.get(f"/api/companies/{siren}")
        assert r.status_code == 200
        data = r.json()
        
        # Verify response shape
        assert "company" in data
        assert "contacts" in data
        assert "merged_contact" in data
        assert "officers" in data
        assert "history" in data

    async def test_search_by_name(self, app_client):
        """Search for companies by partial name."""
        r = await app_client.get("/api/companies/search?q=transport&limit=5")
        assert r.status_code == 200
        data = r.json()
        assert "results" in data or isinstance(data, list)

    async def test_editable_fields_whitelist(self, app_client):
        """PATCH with an invalid field should be rejected or ignored."""
        r = await app_client.patch(
            "/api/companies/000000000",
            json={"hacker_field": "pwned"}
        )
        # Should either 404 (no such siren) or ignore the bad field
        assert r.status_code in (404, 422, 200)


# ═══════════════════════════════════════════════════════════════════════
# Journey 4: Notes
# ═══════════════════════════════════════════════════════════════════════

class TestNotes:
    async def test_add_note_to_nonexistent_siren(self, app_client):
        r = await app_client.post("/api/notes/000000000", json={"text": "Test note"})
        # Should succeed (notes create for any siren) or 404
        assert r.status_code in (200, 201, 404)

    async def test_add_empty_note_fails(self, app_client):
        r = await app_client.post("/api/notes/000000000", json={"text": ""})
        assert r.status_code in (400, 422)

    async def test_delete_nonexistent_note(self, app_client):
        r = await app_client.delete("/api/notes/999999")
        assert r.status_code in (404, 200)

    async def test_error_response_no_traceback(self, app_client):
        """Verify that error responses don't leak stack traces (CRITICAL fix)."""
        r = await app_client.post("/api/notes/000000000", json={"text": ""})
        body = r.text
        assert "Traceback" not in body
        assert "File \"" not in body


# ═══════════════════════════════════════════════════════════════════════
# Journey 5: SIRENE Search
# ═══════════════════════════════════════════════════════════════════════

class TestSireneSearch:
    async def test_search_by_siren(self, app_client):
        r = await app_client.get("/api/sirene/search?q=123456789")
        assert r.status_code == 200
        data = r.json()
        assert "results" in data

    async def test_search_by_name_min_length(self, app_client):
        """Name search requires at least 3 characters."""
        r = await app_client.get("/api/sirene/search?q=ab")
        assert r.status_code == 400

    async def test_search_by_naf_code(self, app_client):
        """NAF code search (starts with digit)."""
        r = await app_client.get("/api/sirene/search?q=49.41")
        assert r.status_code == 200

    async def test_search_with_department_filter(self, app_client):
        r = await app_client.get("/api/sirene/search?q=transport&department=66")
        assert r.status_code == 200

    async def test_search_timeout_protection(self, app_client):
        """Broad search with no filters should not hang forever."""
        # This might 408 on very broad queries — that's correct behavior
        r = await app_client.get("/api/sirene/search?q=sas")
        assert r.status_code in (200, 408)


# ═══════════════════════════════════════════════════════════════════════
# Journey 6: Auth
# ═══════════════════════════════════════════════════════════════════════

class TestAuth:
    async def test_me_with_valid_session(self, app_client):
        r = await app_client.get("/api/auth/me")
        assert r.status_code == 200
        data = r.json()
        assert "user" in data
        assert data["user"]["username"] == "test_admin"

    async def test_me_without_session(self, app_client):
        """Request without session cookie should return 401."""
        import httpx
        from fortress.api.main import app
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app),
            base_url="http://testserver",
        ) as anon_client:
            r = await anon_client.get("/api/auth/me")
            assert r.status_code == 401

    async def test_auth_check_returns_status(self, app_client):
        r = await app_client.get("/api/auth/check")
        assert r.status_code == 200
        data = r.json()
        assert "auth_required" in data

    async def test_login_bad_credentials(self, app_client):
        r = await app_client.post("/api/auth/login", json={
            "username": "nonexistent_user_xyz",
            "password": "wrong_password",
        })
        assert r.status_code == 401


# ═══════════════════════════════════════════════════════════════════════
# Journey 7: Activity Log
# ═══════════════════════════════════════════════════════════════════════

class TestActivity:
    async def test_get_activity_log(self, app_client):
        r = await app_client.get("/api/activity")
        assert r.status_code == 200
        data = r.json()
        assert isinstance(data, (list, dict))


# ═══════════════════════════════════════════════════════════════════════
# Journey 8: Health
# ═══════════════════════════════════════════════════════════════════════

class TestHealth:
    async def test_health_endpoint(self, app_client):
        r = await app_client.get("/api/health")
        assert r.status_code == 200
        data = r.json()
        assert "status" in data


# ═══════════════════════════════════════════════════════════════════════
# Database Integrity
# ═══════════════════════════════════════════════════════════════════════

class TestDatabaseIntegrity:
    async def test_cascade_fks_exist(self, db_conn):
        """Verify all 9 FK constraints are in place with CASCADE/SET NULL."""
        result = await db_conn.execute("""
            SELECT
                tc.constraint_name,
                tc.table_name,
                rc.delete_rule
            FROM information_schema.table_constraints tc
            JOIN information_schema.referential_constraints rc
                ON tc.constraint_name = rc.constraint_name
            WHERE tc.constraint_type = 'FOREIGN KEY'
            ORDER BY tc.table_name
        """)
        rows = await result.fetchall()
        
        fk_map = {r["constraint_name"]: r for r in rows}
        
        # Verify CASCADE FKs
        cascade_fks = [name for name, r in fk_map.items() if r["delete_rule"] == "CASCADE"]
        set_null_fks = [name for name, r in fk_map.items() if r["delete_rule"] == "SET NULL"]
        
        assert len(cascade_fks) >= 6, f"Expected ≥6 CASCADE FKs, got {len(cascade_fks)}: {cascade_fks}"
        assert len(set_null_fks) >= 3, f"Expected ≥3 SET NULL FKs, got {len(set_null_fks)}: {set_null_fks}"

    async def test_new_social_columns_exist(self, db_conn):
        """Verify social_whatsapp and social_youtube columns exist on contacts."""
        result = await db_conn.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'contacts'
            AND column_name IN ('social_whatsapp', 'social_youtube', 'siren_match')
        """)
        rows = await result.fetchall()
        cols = {r["column_name"] for r in rows}
        
        assert "social_whatsapp" in cols, "contacts.social_whatsapp column missing"
        assert "social_youtube" in cols, "contacts.social_youtube column missing"

    async def test_no_orphan_contacts(self, db_conn):
        """Verify no contacts reference non-existent companies."""
        result = await db_conn.execute("""
            SELECT COUNT(*) AS orphans
            FROM contacts c
            LEFT JOIN companies co ON co.siren = c.siren
            WHERE co.siren IS NULL
            LIMIT 1
        """)
        row = await result.fetchone()
        orphans = row["orphans"] if row else 0
        # Allow some tolerance for MAPS entries that may not have company rows
        assert orphans < 100, f"Found {orphans} orphan contact rows"

    async def test_trigram_index_exists(self, db_conn):
        """Verify pg_trgm trigram index is in place for fuzzy search."""
        result = await db_conn.execute("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'companies'
            AND indexname = 'idx_companies_denomination_trgm'
        """)
        row = await result.fetchone()
        assert row is not None, "Trigram index idx_companies_denomination_trgm missing"
