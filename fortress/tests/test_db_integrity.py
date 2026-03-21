"""Database Integrity Tests — Direct queries against Neon DB.

Verifies FK constraints, schema correctness, orphan detection, indexes.
"""

import os
import pytest
import psycopg
from psycopg.rows import dict_row

NEON_URL = os.environ.get(
    "DATABASE_URL",
    "postgresql://neondb_owner:npg_1bgBYwTSa5UP@ep-noisy-tree-agzjuw4w-pooler.c-2.eu-central-1.aws.neon.tech/neondb?sslmode=require",
)

pytestmark = pytest.mark.anyio


@pytest.fixture(scope="module")
async def db():
    conn = await psycopg.AsyncConnection.connect(NEON_URL, row_factory=dict_row)
    yield conn
    await conn.close()


class TestForeignKeys:
    async def test_cascade_fks_exist(self, db):
        """Verify CASCADE FK constraints are in place."""
        result = await db.execute("""
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

        cascade = [r for r in rows if r["delete_rule"] == "CASCADE"]
        set_null = [r for r in rows if r["delete_rule"] == "SET NULL"]

        print(f"\n  CASCADE FKs ({len(cascade)}):")
        for r in cascade:
            print(f"    {r['constraint_name']} on {r['table_name']}")
        print(f"  SET NULL FKs ({len(set_null)}):")
        for r in set_null:
            print(f"    {r['constraint_name']} on {r['table_name']}")

        assert len(cascade) >= 6, f"Expected ≥6 CASCADE FKs, got {len(cascade)}"
        assert len(set_null) >= 3, f"Expected ≥3 SET NULL FKs, got {len(set_null)}"


class TestSchema:
    async def test_social_columns_exist(self, db):
        """Verify new social columns exist."""
        result = await db.execute("""
            SELECT column_name FROM information_schema.columns
            WHERE table_name = 'contacts'
            AND column_name IN ('social_whatsapp', 'social_youtube', 'siren_match')
        """)
        rows = await result.fetchall()
        cols = {r["column_name"] for r in rows}
        assert "social_whatsapp" in cols
        assert "social_youtube" in cols

    async def test_trigram_index(self, db):
        """Verify pg_trgm index exists for fuzzy search."""
        result = await db.execute("""
            SELECT indexname FROM pg_indexes
            WHERE tablename = 'companies'
            AND indexname = 'idx_companies_denomination_trgm'
        """)
        row = await result.fetchone()
        assert row is not None, "Trigram index missing"

    async def test_siren_match_column(self, db):
        """Verify siren_match column exists on contacts (your latest change)."""
        result = await db.execute("""
            SELECT column_name, data_type FROM information_schema.columns
            WHERE table_name = 'contacts' AND column_name = 'siren_match'
        """)
        row = await result.fetchone()
        assert row is not None, "contacts.siren_match column missing"


class TestDataIntegrity:
    async def test_no_orphan_contacts(self, db):
        """Check for contacts referencing non-existent companies."""
        result = await db.execute("""
            SELECT COUNT(*) AS orphans
            FROM contacts c
            LEFT JOIN companies co ON co.siren = c.siren
            WHERE co.siren IS NULL
        """)
        row = await result.fetchone()
        orphans = row["orphans"]
        print(f"\n  Orphan contacts: {orphans}")
        # Allow some — MAPS entities might not have company rows
        assert orphans < 100, f"Too many orphan contacts: {orphans}"

    async def test_no_orphan_notes(self, db):
        """Check for notes referencing non-existent companies."""
        result = await db.execute("""
            SELECT COUNT(*) AS orphans
            FROM company_notes n
            LEFT JOIN companies co ON co.siren = n.siren
            WHERE co.siren IS NULL
        """)
        row = await result.fetchone()
        orphans = row["orphans"]
        print(f"\n  Orphan notes: {orphans}")
        assert orphans < 10, f"Too many orphan notes: {orphans}"

    async def test_no_orphan_batch_tags(self, db):
        """Check for batch_tags referencing non-existent companies."""
        result = await db.execute("""
            SELECT COUNT(*) AS orphans
            FROM batch_tags bt
            LEFT JOIN companies co ON co.siren = bt.siren
            WHERE co.siren IS NULL
        """)
        row = await result.fetchone()
        orphans = row["orphans"]
        print(f"\n  Orphan batch_tags: {orphans}")
        assert orphans < 50, f"Too many orphan batch_tags: {orphans}"


class TestDataQuality:
    async def test_enrichment_coverage(self, db):
        """Report enrichment coverage stats."""
        result = await db.execute("""
            WITH tagged AS (
                SELECT DISTINCT siren FROM batch_tags
            ),
            enriched AS (
                SELECT
                    t.siren,
                    MAX(ct.phone) AS phone,
                    MAX(ct.email) AS email,
                    MAX(ct.website) AS website
                FROM tagged t
                LEFT JOIN contacts ct ON ct.siren = t.siren
                GROUP BY t.siren
            )
            SELECT
                COUNT(*) AS total,
                COUNT(*) FILTER (WHERE phone IS NOT NULL) AS with_phone,
                COUNT(*) FILTER (WHERE email IS NOT NULL) AS with_email,
                COUNT(*) FILTER (WHERE website IS NOT NULL) AS with_website,
                COUNT(*) FILTER (WHERE phone IS NULL AND email IS NULL AND website IS NULL) AS missing_all
            FROM enriched
        """)
        row = await result.fetchone()
        total = row["total"] or 0
        if total == 0:
            pytest.skip("No enriched data")

        phone_pct = round(100 * row["with_phone"] / total)
        email_pct = round(100 * row["with_email"] / total)
        web_pct = round(100 * row["with_website"] / total)
        missing_pct = round(100 * row["missing_all"] / total)

        print(f"\n  Enrichment coverage ({total} companies):")
        print(f"    📞 Phone: {phone_pct}%")
        print(f"    ✉️  Email: {email_pct}%")
        print(f"    🌐 Website: {web_pct}%")
        print(f"    ❌ Missing all: {missing_pct}%")

        # At least 10% should have some contact data
        assert missing_pct < 90, f"Too many companies with no contact data: {missing_pct}%"
