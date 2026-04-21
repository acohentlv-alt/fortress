"""Integration tests for _copy_sirene_reference_data (Frankenstein fix, Apr 22).

Verifies that code_postal and ville now use COALESCE (Maps location preserved)
while legal fields (naf_code, forme_juridique, etc.) are still always overwritten.

Requires fortress_test PostgreSQL DB.
"""
from __future__ import annotations

from datetime import date

import psycopg.rows
import pytest

from fortress.discovery import _copy_sirene_reference_data
from fortress.models import Company, CompanyStatus
from fortress.processing.dedup import upsert_company


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_MAPS_SIREN = "MAPS98001"
_REAL_SIREN = "980000001"  # A non-existent SIREN used only within fortress_test


def _make_maps_company(**overrides) -> Company:
    """Minimal MAPS entity for testing."""
    defaults = dict(
        siren=_MAPS_SIREN,
        denomination="Le Bistrot du Marché",
        enseigne="Le Bistrot du Marché",
        adresse="3 Pl. du Marché, 13001 Marseille, France",
        code_postal="13001",
        ville="Marseille",
        departement="13",
        statut=CompanyStatus.ACTIVE,
        workspace_id=174,
    )
    defaults.update(overrides)
    return Company(**defaults)


async def _insert_sirene_row(conn, cp: str, ville: str) -> None:
    """Insert a fake SIRENE row directly (bypassing upsert_company protection)."""
    await conn.execute(
        """
        INSERT INTO companies (
            siren, siret_siege, denomination, naf_code, naf_libelle,
            forme_juridique, code_postal, ville, statut, date_creation
        )
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (siren) DO UPDATE SET
            naf_code        = EXCLUDED.naf_code,
            naf_libelle     = EXCLUDED.naf_libelle,
            forme_juridique = EXCLUDED.forme_juridique,
            code_postal     = EXCLUDED.code_postal,
            ville           = EXCLUDED.ville,
            date_creation   = EXCLUDED.date_creation
        """,
        (
            _REAL_SIREN,
            f"{_REAL_SIREN}00012",
            "Societe Siège SA",
            "56.10A",
            "Restauration traditionnelle",
            "SA",
            cp,
            ville,
            "A",
            date(2005, 1, 10),
        ),
    )


async def _cleanup(conn) -> None:
    """Remove test rows."""
    await conn.execute(
        "DELETE FROM companies WHERE siren IN (%s, %s)",
        (_MAPS_SIREN, _REAL_SIREN),
    )


async def _read_company(conn, siren: str) -> dict:
    async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
        await cur.execute(
            """SELECT siren, code_postal, ville, naf_code, naf_libelle,
                      forme_juridique, siret_siege, date_creation, tranche_effectif
               FROM companies WHERE siren = %s""",
            (siren,),
        )
        return await cur.fetchone()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_copy_preserves_maps_cp_ville_when_present(test_pool):
    """Maps CP/ville must survive when already populated (Frankenstein fix core)."""
    async with test_pool.connection() as conn:
        # MAPS entity: storefront at 13001 Marseille
        await upsert_company(conn, _make_maps_company(code_postal="13001", ville="Marseille"))
        # SIRENE siège: HQ at 13008 (different arrondissement)
        await _insert_sirene_row(conn, cp="13008", ville="Marseille 8e arr")

        await _copy_sirene_reference_data(conn, _MAPS_SIREN, _REAL_SIREN)

        row = await _read_company(conn, _MAPS_SIREN)
        await _cleanup(conn)

    assert row is not None
    # Maps-derived location preserved — not overwritten by SIRENE siège CP
    assert row["code_postal"] == "13001"
    assert row["ville"] == "Marseille"


@pytest.mark.asyncio
async def test_copy_fills_cp_ville_when_maps_null(test_pool):
    """When MAPS entity has NULL cp/ville, SIRENE values should fill them in."""
    async with test_pool.connection() as conn:
        # MAPS entity with no location data (e.g. personne physique, no address on Maps)
        await upsert_company(conn, _make_maps_company(code_postal=None, ville=None))
        # SIRENE siège provides the location
        await _insert_sirene_row(conn, cp="92200", ville="Neuilly-sur-Seine")

        await _copy_sirene_reference_data(conn, _MAPS_SIREN, _REAL_SIREN)

        row = await _read_company(conn, _MAPS_SIREN)
        await _cleanup(conn)

    assert row is not None
    # SIRENE filled in the missing location
    assert row["code_postal"] == "92200"
    assert row["ville"] == "Neuilly-sur-Seine"


@pytest.mark.asyncio
async def test_copy_still_overwrites_naf_forme_juridique(test_pool):
    """Legal identity fields must ALWAYS be overwritten from SIRENE regardless of MAPS values."""
    async with test_pool.connection() as conn:
        # MAPS entity with some stale legal info
        maps_co = _make_maps_company(naf_code="99.99Z", forme_juridique="EURL")
        await upsert_company(conn, maps_co)
        # SIRENE provides the authoritative legal info
        await _insert_sirene_row(conn, cp="13001", ville="Marseille")

        await _copy_sirene_reference_data(conn, _MAPS_SIREN, _REAL_SIREN)

        row = await _read_company(conn, _MAPS_SIREN)
        await _cleanup(conn)

    assert row is not None
    # Legal fields overwritten from SIRENE (56.10A, not 99.99Z)
    assert row["naf_code"] == "56.10A"
    assert row["naf_libelle"] == "Restauration traditionnelle"
    # forme_juridique overwritten from SIRENE (SA, not EURL)
    assert row["forme_juridique"] == "SA"
    # siret_siege and date_creation overwritten
    assert row["siret_siege"] == f"{_REAL_SIREN}00012"
    assert row["date_creation"] == date(2005, 1, 10)


@pytest.mark.asyncio
async def test_copy_returns_cleanly_when_target_siren_missing(test_pool):
    """When the target SIREN doesn't exist, helper must return silently (no crash)."""
    async with test_pool.connection() as conn:
        await upsert_company(conn, _make_maps_company())

        # Call with a non-existent SIREN — should not raise
        await _copy_sirene_reference_data(conn, _MAPS_SIREN, "000000000")

        row = await _read_company(conn, _MAPS_SIREN)
        await _cleanup(conn)

    # MAPS row untouched (helper returned early)
    assert row is not None
    assert row["naf_code"] is None  # not overwritten since SIRENE row missing
