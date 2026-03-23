"""Integration tests for deduplicator — requires fortress_test PostgreSQL DB.

Run with:
  TEST_DB_URL=postgresql://fortress:fortress_dev@localhost/fortress_test \
  pytest tests/test_module_d/test_deduplicator.py -v

Functions confirmed present in deduplicator.py:
  - upsert_company(conn, company: Company) -> None
  - upsert_contact(conn, contact: Contact) -> None
  - upsert_officer(conn, officer: Officer) -> None
  - tag_query(conn, siren: str, query_name: str) -> None
  - bulk_tag_query(conn, sirens: list[str], query_name: str) -> None
  - log_audit(conn, *, query_id, siren, action, result, ...) -> None

All functions take an open psycopg3 connection (not a pool).
Transactions are managed by the caller.

psycopg3 pattern used throughout:
  async with pool.connection() as conn:
      async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
          await cur.execute("SELECT ...", (param,))
          row = await cur.fetchone()
"""
from __future__ import annotations

from datetime import date, datetime, timezone

import psycopg.rows
import pytest
import pytest_asyncio

from fortress.models import (
    Company,
    CompanyStatus,
    Contact,
    ContactSource,
    EmailType,
    Officer,
)
from fortress.processing.dedup import (
    bulk_tag_query,
    log_audit,
    tag_query,
    upsert_company,
    upsert_contact,
    upsert_officer,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_company(siren: str = "123456789", denomination: str = "Dupont SARL") -> Company:
    """Minimal valid Company for testing."""
    return Company(
        siren=siren,
        siret_siege=f"{siren}00012",
        denomination=denomination,
        naf_code="01.21Z",
        naf_libelle="Viticulture",
        forme_juridique="SARL",
        adresse="14 Chemin des Vignes",
        code_postal="66300",
        ville="Thuir",
        departement="66",
        region="Occitanie",
        statut=CompanyStatus.ACTIVE,
        date_creation=date(2008, 4, 15),
    )


def _make_contact(siren: str = "123456789") -> Contact:
    """Minimal valid Contact for testing."""
    return Contact(
        siren=siren,
        phone="0468532109",
        email="contact@dupont.fr",
        email_type=EmailType.FOUND,
        website="https://dupont.fr",
        source=ContactSource.WEBSITE_CRAWL,
        social_linkedin="https://linkedin.com/company/dupont",
        collected_at=datetime.now(tz=timezone.utc),
    )


def _make_officer(siren: str = "123456789") -> Officer:
    """Minimal valid Officer for testing."""
    return Officer(
        siren=siren,
        nom="Dupont",
        prenom="Marie",
        role="Gérante",
        source=ContactSource.INPI,
        collected_at=datetime.now(tz=timezone.utc),
    )


# ---------------------------------------------------------------------------
# upsert_company
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_company_insert(test_pool):
    """New company is inserted successfully."""
    company = _make_company(siren="900000001")

    async with test_pool.connection() as conn:
        await upsert_company(conn, company)

        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(
                "SELECT siren, denomination, naf_code, ville FROM companies WHERE siren = %s",
                (company.siren,),
            )
            row = await cur.fetchone()

        # Clean up
        await conn.execute("DELETE FROM companies WHERE siren = %s", (company.siren,))

    assert row is not None
    assert row["siren"] == "900000001"
    assert row["denomination"] == "Dupont SARL"
    assert row["naf_code"] == "01.21Z"
    assert row["ville"] == "Thuir"


@pytest.mark.asyncio
async def test_upsert_company_update_on_conflict(test_pool):
    """Existing SIREN updates mutable fields on re-upsert."""
    company = _make_company(siren="900000002", denomination="Original Name SARL")

    async with test_pool.connection() as conn:
        # First insert
        await upsert_company(conn, company)

        # Update denomination and re-upsert
        updated = company.model_copy(update={"denomination": "Updated Name SARL"})
        await upsert_company(conn, updated)

        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(
                "SELECT denomination FROM companies WHERE siren = %s",
                (company.siren,),
            )
            row = await cur.fetchone()

        # Clean up
        await conn.execute("DELETE FROM companies WHERE siren = %s", (company.siren,))

    assert row is not None
    assert row["denomination"] == "Updated Name SARL"


@pytest.mark.asyncio
async def test_upsert_company_idempotent(test_pool):
    """Upserting the same company twice does not create a duplicate row."""
    company = _make_company(siren="900000003")

    async with test_pool.connection() as conn:
        await upsert_company(conn, company)
        await upsert_company(conn, company)

        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(
                "SELECT COUNT(*) AS cnt FROM companies WHERE siren = %s",
                (company.siren,),
            )
            row = await cur.fetchone()

        # Clean up
        await conn.execute("DELETE FROM companies WHERE siren = %s", (company.siren,))

    assert row["cnt"] == 1


# ---------------------------------------------------------------------------
# upsert_contact
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_contact_insert(test_pool):
    """New contact is inserted successfully."""
    company = _make_company(siren="900000010")
    contact = _make_contact(siren="900000010")

    async with test_pool.connection() as conn:
        await upsert_company(conn, company)
        await upsert_contact(conn, contact)

        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(
                "SELECT siren, phone, email, source FROM contacts WHERE siren = %s AND source = %s",
                (contact.siren, contact.source.value),
            )
            row = await cur.fetchone()

        # Clean up (contacts referencing company — delete contacts first)
        await conn.execute("DELETE FROM contacts WHERE siren = %s", (contact.siren,))
        await conn.execute("DELETE FROM companies WHERE siren = %s", (company.siren,))

    assert row is not None
    assert row["phone"] == "0468532109"
    assert row["email"] == "contact@dupont.fr"
    assert row["source"] == "website_crawl"


@pytest.mark.asyncio
async def test_upsert_contact_update_on_conflict(test_pool):
    """Re-upserting with a new email (COALESCE) updates the row."""
    company = _make_company(siren="900000011")
    contact_no_email = Contact(
        siren="900000011",
        phone="0468532109",
        email=None,
        source=ContactSource.WEBSITE_CRAWL,
    )
    contact_with_email = Contact(
        siren="900000011",
        phone=None,
        email="contact@dupont.fr",
        source=ContactSource.WEBSITE_CRAWL,
        collected_at=datetime.now(tz=timezone.utc),
    )

    async with test_pool.connection() as conn:
        await upsert_company(conn, company)
        # First insert — no email
        await upsert_contact(conn, contact_no_email)
        # Second upsert — adds email via COALESCE
        await upsert_contact(conn, contact_with_email)

        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(
                "SELECT phone, email FROM contacts WHERE siren = %s AND source = %s",
                ("900000011", "website_crawl"),
            )
            row = await cur.fetchone()

        # Clean up
        await conn.execute("DELETE FROM contacts WHERE siren = %s", ("900000011",))
        await conn.execute("DELETE FROM companies WHERE siren = %s", ("900000011",))

    assert row is not None
    # COALESCE: original phone preserved, new email added
    assert row["phone"] == "0468532109"
    assert row["email"] == "contact@dupont.fr"


# ---------------------------------------------------------------------------
# upsert_officer
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_upsert_officer_insert(test_pool):
    """New officer is inserted successfully."""
    company = _make_company(siren="900000020")
    officer = _make_officer(siren="900000020")

    async with test_pool.connection() as conn:
        await upsert_company(conn, company)
        await upsert_officer(conn, officer)

        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(
                "SELECT siren, nom, prenom, role FROM officers WHERE siren = %s AND nom = %s",
                (officer.siren, officer.nom),
            )
            row = await cur.fetchone()

        # Clean up
        await conn.execute("DELETE FROM officers WHERE siren = %s", (officer.siren,))
        await conn.execute("DELETE FROM companies WHERE siren = %s", (company.siren,))

    assert row is not None
    assert row["nom"] == "Dupont"
    assert row["prenom"] == "Marie"
    assert row["role"] == "Gérante"


@pytest.mark.asyncio
async def test_upsert_officer_do_nothing_on_duplicate(test_pool):
    """Duplicate officer (same siren+nom+prenom) is silently skipped."""
    company = _make_company(siren="900000021")
    officer = _make_officer(siren="900000021")

    async with test_pool.connection() as conn:
        await upsert_company(conn, company)
        # Insert twice — second should be ON CONFLICT DO NOTHING
        await upsert_officer(conn, officer)
        await upsert_officer(conn, officer)

        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(
                "SELECT COUNT(*) AS cnt FROM officers WHERE siren = %s AND nom = %s AND prenom = %s",
                (officer.siren, officer.nom, officer.prenom),
            )
            row = await cur.fetchone()

        # Clean up
        await conn.execute("DELETE FROM officers WHERE siren = %s", (officer.siren,))
        await conn.execute("DELETE FROM companies WHERE siren = %s", (company.siren,))

    assert row["cnt"] == 1


# ---------------------------------------------------------------------------
# tag_query
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_tag_query_inserts_row(test_pool):
    """tag_query() inserts a (siren, query_name) row into query_tags."""
    company = _make_company(siren="900000030")

    async with test_pool.connection() as conn:
        await upsert_company(conn, company)
        await tag_query(conn, "900000030", "AGRICULTURE_66")

        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(
                "SELECT siren, query_name FROM query_tags WHERE siren = %s AND query_name = %s",
                ("900000030", "AGRICULTURE_66"),
            )
            row = await cur.fetchone()

        # Clean up
        await conn.execute("DELETE FROM query_tags WHERE siren = %s", ("900000030",))
        await conn.execute("DELETE FROM companies WHERE siren = %s", ("900000030",))

    assert row is not None
    assert row["siren"] == "900000030"
    assert row["query_name"] == "AGRICULTURE_66"


@pytest.mark.asyncio
async def test_tag_query_idempotent(test_pool):
    """Tagging the same company+query twice does not create a duplicate row."""
    company = _make_company(siren="900000031")

    async with test_pool.connection() as conn:
        await upsert_company(conn, company)
        await tag_query(conn, "900000031", "AGRICULTURE_66")
        await tag_query(conn, "900000031", "AGRICULTURE_66")

        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(
                "SELECT COUNT(*) AS cnt FROM query_tags WHERE siren = %s AND query_name = %s",
                ("900000031", "AGRICULTURE_66"),
            )
            row = await cur.fetchone()

        # Clean up
        await conn.execute("DELETE FROM query_tags WHERE siren = %s", ("900000031",))
        await conn.execute("DELETE FROM companies WHERE siren = %s", ("900000031",))

    assert row["cnt"] == 1


# ---------------------------------------------------------------------------
# bulk_tag_query
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_bulk_tag_query_inserts_multiple_rows(test_pool):
    """bulk_tag_query() tags multiple companies in a single batch."""
    sirens = ["900000040", "900000041", "900000042"]

    async with test_pool.connection() as conn:
        for siren in sirens:
            await upsert_company(conn, _make_company(siren=siren))

        await bulk_tag_query(conn, sirens, "VITICULTURE_66")

        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(
                "SELECT COUNT(*) AS cnt FROM query_tags WHERE query_name = %s AND siren = ANY(%s)",
                ("VITICULTURE_66", sirens),
            )
            row = await cur.fetchone()

        # Clean up
        for siren in sirens:
            await conn.execute("DELETE FROM query_tags WHERE siren = %s", (siren,))
            await conn.execute("DELETE FROM companies WHERE siren = %s", (siren,))

    assert row["cnt"] == 3


@pytest.mark.asyncio
async def test_bulk_tag_query_empty_list_is_noop(test_pool):
    """bulk_tag_query() with empty list does nothing and does not raise."""
    async with test_pool.connection() as conn:
        # Should not raise
        await bulk_tag_query(conn, [], "VITICULTURE_66")


# ---------------------------------------------------------------------------
# log_audit
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_log_audit_inserts_row(test_pool):
    """log_audit() inserts a row into scrape_audit."""
    async with test_pool.connection() as conn:
        await log_audit(
            conn,
            query_id="AGRICULTURE_66",
            siren="900000050",
            action="web_search",
            result="success",
            source_url="https://www.google.fr/search?q=dupont",
            duration_ms=450,
        )

        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(
                "SELECT query_id, siren, action, result, duration_ms "
                "FROM scrape_audit WHERE siren = %s AND query_id = %s ORDER BY id DESC LIMIT 1",
                ("900000050", "AGRICULTURE_66"),
            )
            row = await cur.fetchone()

        # Clean up
        await conn.execute(
            "DELETE FROM scrape_audit WHERE siren = %s AND query_id = %s",
            ("900000050", "AGRICULTURE_66"),
        )

    assert row is not None
    assert row["action"] == "web_search"
    assert row["result"] == "success"
    assert row["duration_ms"] == 450


@pytest.mark.asyncio
async def test_log_audit_null_optional_fields(test_pool):
    """log_audit() works with source_url and duration_ms omitted (None)."""
    async with test_pool.connection() as conn:
        await log_audit(
            conn,
            query_id="AGRICULTURE_66",
            siren="900000051",
            action="inpi_lookup",
            result="fail",
        )

        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute(
                "SELECT source_url, duration_ms FROM scrape_audit "
                "WHERE siren = %s ORDER BY id DESC LIMIT 1",
                ("900000051",),
            )
            row = await cur.fetchone()

        # Clean up
        await conn.execute(
            "DELETE FROM scrape_audit WHERE siren = %s", ("900000051",)
        )

    assert row is not None
    assert row["source_url"] is None
    assert row["duration_ms"] is None
