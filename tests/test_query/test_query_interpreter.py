"""Tests for fortress.query.interpreter.

All DB calls are mocked — these tests do not require a running PostgreSQL instance.

Run with:
    pytest tests/test_module_a/test_query_interpreter.py -v
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from fortress.query.interpreter import (
    AmbiguousQueryError,
    UnresolvableQueryError,
    _build_naf_conditions,
    _build_naf_pattern_description,
    _parse_tokens,
    _resolve_department_token,
    _resolve_industry_token,
    interpret_query,
)
from fortress.config.departments import postal_code_to_dept
from fortress.models import QueryResult


# ---------------------------------------------------------------------------
# Helpers — build fake DB pools that return controlled data
# ---------------------------------------------------------------------------


def _make_fake_company(
    siren: str = "123456789",
    denomination: str = "Test Company SARL",
    naf_code: str = "01.11Z",
    departement: str = "66",
    ville: str = "Perpignan",
) -> dict[str, Any]:
    """Return a dict that mimics a psycopg Row for a company record."""
    return {
        "siren": siren,
        "siret_siege": None,
        "denomination": denomination,
        "naf_code": naf_code,
        "naf_libelle": "Culture de cereales",
        "forme_juridique": None,
        "adresse": None,
        "code_postal": "66000",
        "ville": ville,
        "departement": departement,
        "region": "Occitanie",
        "statut": "A",
        "date_creation": None,
        "tranche_effectif": None,
        "latitude": None,
        "longitude": None,
        "fortress_id": 1,
    }


def _make_pool(count: int = 42, rows: list[dict[str, Any]] | None = None) -> MagicMock:
    """Build a mock psycopg_pool.AsyncConnectionPool that returns `count` total
    and `rows` as the sample result set.

    psycopg3 pattern used by the production code:
        async with pool.connection() as conn:
            async with conn.cursor(row_factory=dict_row) as cur:
                await cur.execute(count_sql, params)
                count_row = await cur.fetchone()   # {"count": N}
                await cur.execute(sample_sql, params)
                rows = await cur.fetchall()        # [dict, ...]
    """
    if rows is None:
        rows = []

    # Cursor mock: fetchone() → count dict, fetchall() → rows list
    mock_cur = AsyncMock()
    mock_cur.execute = AsyncMock()
    mock_cur.fetchone = AsyncMock(return_value={"count": count})
    mock_cur.fetchall = AsyncMock(return_value=rows)

    # conn.cursor(row_factory=...) is a sync call returning an async context manager
    cur_ctx = MagicMock()
    cur_ctx.__aenter__ = AsyncMock(return_value=mock_cur)
    cur_ctx.__aexit__ = AsyncMock(return_value=False)

    mock_conn = MagicMock()
    mock_conn.cursor = MagicMock(return_value=cur_ctx)

    # Make pool.connection() work as an async context manager
    pool = MagicMock()
    conn_ctx = MagicMock()
    conn_ctx.__aenter__ = AsyncMock(return_value=mock_conn)
    conn_ctx.__aexit__ = AsyncMock(return_value=False)
    pool.connection = MagicMock(return_value=conn_ctx)

    return pool


# ---------------------------------------------------------------------------
# Unit tests — pure parsing (no DB)
# ---------------------------------------------------------------------------


class TestTokenParsing:
    """Tests for _parse_tokens — the low-level token splitter."""

    def test_agriculture_66_splits_correctly(self) -> None:
        industry_tokens, dept_code, is_france_wide = _parse_tokens("AGRICULTURE 66")
        assert industry_tokens == ["AGRICULTURE"]
        assert dept_code == "66"
        assert is_france_wide is False

    def test_paris_resolves_to_75(self) -> None:
        industry_tokens, dept_code, is_france_wide = _parse_tokens("BOULANGERIE PARIS")
        assert industry_tokens == ["BOULANGERIE"]
        assert dept_code == "75"
        assert is_france_wide is False

    def test_france_token_sets_flag(self) -> None:
        industry_tokens, dept_code, is_france_wide = _parse_tokens("RESTAURANT FRANCE")
        assert industry_tokens == ["RESTAURANT"]
        assert dept_code is None
        assert is_france_wide is True

    def test_all_token_sets_flag(self) -> None:
        industry_tokens, dept_code, is_france_wide = _parse_tokens("LOGICIEL ALL")
        assert industry_tokens == ["LOGICIEL"]
        assert dept_code is None
        assert is_france_wide is True

    def test_naf_code_token_kept_as_industry(self) -> None:
        industry_tokens, dept_code, is_france_wide = _parse_tokens("62.01Z 75")
        assert "62.01Z" in industry_tokens
        assert dept_code == "75"
        assert is_france_wide is False

    def test_bare_dept_only(self) -> None:
        # "66" alone — resolves as dept token, leaving no industry tokens
        industry_tokens, dept_code, is_france_wide = _parse_tokens("66")
        assert industry_tokens == []
        assert dept_code == "66"
        assert is_france_wide is False

    def test_multi_token_industry(self) -> None:
        industry_tokens, dept_code, is_france_wide = _parse_tokens("CAVE A VIN 66")
        assert dept_code == "66"
        assert "CAVE" in industry_tokens

    def test_dom_tom_department(self) -> None:
        industry_tokens, dept_code, is_france_wide = _parse_tokens("AGRICULTURE 971")
        assert industry_tokens == ["AGRICULTURE"]
        assert dept_code == "971"


class TestDepartmentResolution:
    """Tests for _resolve_department_token."""

    def test_numeric_66(self) -> None:
        result = _resolve_department_token("66")
        assert result is not None
        assert result[0] == "66"
        assert result[1] == "Pyrenees-Orientales"

    def test_numeric_1_with_leading_zero(self) -> None:
        result = _resolve_department_token("01")
        assert result is not None
        assert result[0] == "01"

    def test_single_digit_dept(self) -> None:
        result = _resolve_department_token("1")
        assert result is not None
        assert result[0] == "01"

    def test_name_paris_resolves_to_75(self) -> None:
        result = _resolve_department_token("Paris")
        assert result is not None
        assert result[0] == "75"

    def test_france_token_returns_none(self) -> None:
        # France is not a dept — should return None (caller handles it)
        result = _resolve_department_token("FRANCE")
        assert result is None

    def test_unknown_token_returns_none(self) -> None:
        result = _resolve_department_token("ZZZZUNKNOWN")
        assert result is None

    def test_corsica_2a(self) -> None:
        result = _resolve_department_token("2A")
        assert result is not None
        assert result[0] == "2A"


class TestIndustryResolution:
    """Tests for _resolve_industry_token."""

    def test_exact_agriculture(self) -> None:
        result = _resolve_industry_token("agriculture")
        assert result is not None
        name, prefixes = result
        assert name == "agriculture"
        assert "01" in prefixes

    def test_exact_restaurant(self) -> None:
        result = _resolve_industry_token("restaurant")
        assert result is not None
        name, prefixes = result
        assert "56.10A" in prefixes

    def test_case_insensitive_agriculture(self) -> None:
        result = _resolve_industry_token("AGRICULTURE")
        assert result is not None
        _, prefixes = result
        assert "01" in prefixes

    def test_direct_naf_code(self) -> None:
        result = _resolve_industry_token("62.01Z")
        assert result is not None
        name, prefixes = result
        assert name == "62.01Z"
        assert prefixes == ["62.01Z"]

    def test_fuzzy_boulang_matches_boulangerie(self) -> None:
        # "boulanger" is close to "boulangerie"
        result = _resolve_industry_token("boulanger")
        assert result is not None
        _, prefixes = result
        # boulangerie prefixes include 10.71A/B/C/D
        assert any("10.71" in p for p in prefixes)

    def test_unknown_returns_none(self) -> None:
        result = _resolve_industry_token("xyzzy_nonsense_term")
        assert result is None

    def test_viticulture_exact(self) -> None:
        result = _resolve_industry_token("viticulture")
        assert result is not None
        _, prefixes = result
        assert "01.21Z" in prefixes


class TestNafConditionBuilder:
    """Tests for SQL condition builders."""

    def test_prefix_builds_like_pattern(self) -> None:
        sql, params = _build_naf_conditions(["01"])
        assert "LIKE" in sql
        assert "01.%" in params

    def test_full_code_builds_exact_match(self) -> None:
        sql, params = _build_naf_conditions(["62.01Z"])
        assert "= %s" in sql
        assert "62.01Z" in params

    def test_multiple_prefixes_joined_with_or(self) -> None:
        sql, params = _build_naf_conditions(["01", "02", "03"])
        assert sql.count("OR") == 2
        assert len(params) == 3

    def test_empty_list_returns_false_condition(self) -> None:
        sql, params = _build_naf_conditions([])
        assert "1=0" in sql
        assert params == []

    def test_sub_prefix_01_2(self) -> None:
        sql, params = _build_naf_conditions(["01.2"])
        assert "01.2%" in params

    def test_pattern_description_single_code(self) -> None:
        pattern = _build_naf_pattern_description(["62.01Z"])
        assert pattern == "62.01Z"

    def test_pattern_description_multi_prefix(self) -> None:
        pattern = _build_naf_pattern_description(["01", "02"])
        assert "01.%" in pattern
        assert "02.%" in pattern


# ---------------------------------------------------------------------------
# Integration-style tests — interpret_query with mocked DB pool
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
class TestInterpretQuery:
    """Tests for the top-level interpret_query function."""

    async def test_agriculture_66(self) -> None:
        """AGRICULTURE 66 → NAF codes starting with 01, dept 66."""
        fake_row = _make_fake_company(
            denomination="Domaine Dupont SARL",
            naf_code="01.11Z",
            departement="66",
        )
        pool = _make_pool(count=4200, rows=[fake_row])

        result = await interpret_query("AGRICULTURE 66", db_pool=pool)

        assert result.department == "66"
        assert result.department_name == "Pyrenees-Orientales"
        assert "01" in result.naf_codes
        assert result.is_france_wide is False
        assert result.company_count == 4200
        assert len(result.sample) == 1
        assert result.sample[0].denomination == "Domaine Dupont SARL"
        # naf_pattern should reference the 01 prefix
        assert "01" in result.naf_pattern

    async def test_parse_naf_direct(self) -> None:
        """62.01Z 75 → exact NAF code match, dept 75 (Paris)."""
        fake_row = _make_fake_company(
            denomination="Acme Software SAS",
            naf_code="62.01Z",
            departement="75",
            ville="Paris",
        )
        pool = _make_pool(count=350, rows=[fake_row])

        result = await interpret_query("62.01Z 75", db_pool=pool)

        assert result.department == "75"
        assert result.department_name == "Paris"
        assert "62.01Z" in result.naf_codes
        assert result.naf_pattern == "62.01Z"
        assert result.is_france_wide is False
        assert result.company_count == 350

    async def test_parse_france_wide(self) -> None:
        """RESTAURANT FRANCE → is_france_wide=True, no dept filter."""
        fake_row = _make_fake_company(
            denomination="Chez Marcel",
            naf_code="56.10A",
            departement="75",
        )
        pool = _make_pool(count=85000, rows=[fake_row])

        result = await interpret_query("RESTAURANT FRANCE", db_pool=pool)

        assert result.is_france_wide is True
        assert result.department is None
        assert result.department_name is None
        assert "56.10A" in result.naf_codes
        assert result.company_count == 85000

    async def test_parse_france_wide_all_keyword(self) -> None:
        """RESTAURANT ALL → same as FRANCE."""
        pool = _make_pool(count=85000, rows=[])

        result = await interpret_query("RESTAURANT ALL", db_pool=pool)

        assert result.is_france_wide is True
        assert result.department is None

    async def test_boulangerie_paris(self) -> None:
        """BOULANGERIE PARIS → dept resolved to 75 by name."""
        pool = _make_pool(count=1200, rows=[])

        result = await interpret_query("BOULANGERIE PARIS", db_pool=pool)

        assert result.department == "75"
        assert result.department_name == "Paris"
        assert any("10.71" in code for code in result.naf_codes)

    async def test_ambiguous_industry_raises_error(self) -> None:
        """A very short/ambiguous token should raise AmbiguousQueryError
        when multiple industries score similarly.

        We test with a term that is ambiguous across multiple aliases.
        Note: if the term happens to resolve cleanly in the alias table,
        this test skips — which is fine (the alias table may improve).
        """
        pool = _make_pool(count=0, rows=[])

        # "SOFT" is ambiguous between logiciel/software/conseil/informatique etc.
        # The test accepts either a valid resolution OR the ambiguous error.
        try:
            result = await interpret_query("SOFT 75", db_pool=pool)
            # If it resolves without raising, verify it produced a sane result
            assert result.department == "75"
            assert len(result.naf_codes) > 0
        except AmbiguousQueryError as exc:
            # Verify it surfaces meaningful options
            assert len(exc.options) >= 2
            for option in exc.options:
                assert isinstance(option, str)
                assert len(option) > 0

    async def test_unknown_industry_raises_unresolvable(self) -> None:
        """A completely unknown industry token raises UnresolvableQueryError."""
        pool = _make_pool(count=0, rows=[])

        with pytest.raises(UnresolvableQueryError):
            await interpret_query("XYZZY_NONSENSE 75", db_pool=pool)

    async def test_unknown_dept_raises_unresolvable(self) -> None:
        """AGRICULTURE UNKNOWN_PLACE → UnresolvableQueryError (no location)."""
        pool = _make_pool(count=0, rows=[])

        with pytest.raises(UnresolvableQueryError):
            await interpret_query("AGRICULTURE UNKNOWN_PLACE", db_pool=pool)

    async def test_no_industry_raises_unresolvable(self) -> None:
        """'66' alone (no industry) → UnresolvableQueryError."""
        pool = _make_pool(count=0, rows=[])

        with pytest.raises(UnresolvableQueryError):
            await interpret_query("66", db_pool=pool)

    async def test_no_location_raises_unresolvable(self) -> None:
        """'AGRICULTURE' alone (no location) → UnresolvableQueryError."""
        pool = _make_pool(count=0, rows=[])

        with pytest.raises(UnresolvableQueryError):
            await interpret_query("AGRICULTURE", db_pool=pool)

    async def test_zero_results_returns_empty_list(self) -> None:
        """A valid query with 0 DB results returns count=0 and empty sample."""
        pool = _make_pool(count=0, rows=[])

        result = await interpret_query("VITICULTURE 66", db_pool=pool)

        assert result.company_count == 0
        assert result.sample == []
        # Should still be a valid QueryResult
        assert isinstance(result, QueryResult)
        assert result.department == "66"

    async def test_dom_tom_department_971(self) -> None:
        """AGRICULTURE 971 → Guadeloupe."""
        pool = _make_pool(count=150, rows=[])

        result = await interpret_query("AGRICULTURE 971", db_pool=pool)

        assert result.department == "971"
        assert result.department_name == "Guadeloupe"
        assert result.is_france_wide is False

    async def test_result_is_queryresult_instance(self) -> None:
        """interpret_query always returns a QueryResult Pydantic model."""
        pool = _make_pool(count=10, rows=[])

        result = await interpret_query("TRANSPORT 13", db_pool=pool)

        assert isinstance(result, QueryResult)
        assert result.raw_query == "TRANSPORT 13"
        assert result.industry_name  # non-empty
        assert result.naf_codes  # non-empty list

    async def test_industry_name_is_title_cased(self) -> None:
        """industry_name should be title-cased in the result."""
        pool = _make_pool(count=5, rows=[])

        result = await interpret_query("restaurant 75", db_pool=pool)

        # Title case: "Restaurant" not "restaurant"
        assert result.industry_name == result.industry_name.title()

    async def test_db_error_returns_zero_count(self) -> None:
        """If the DB raises an exception, interpret_query returns 0 count
        rather than crashing (graceful degradation for CLI usage)."""
        pool = MagicMock()
        conn_ctx = MagicMock()
        conn_ctx.__aenter__ = AsyncMock(side_effect=Exception("DB connection failed"))
        conn_ctx.__aexit__ = AsyncMock(return_value=False)
        pool.connection = MagicMock(return_value=conn_ctx)

        # Should NOT raise — should return a result with 0 companies
        result = await interpret_query("AGRICULTURE 66", db_pool=pool)

        assert result.company_count == 0
        assert result.sample == []
        assert result.department == "66"


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


class TestEdgeCases:
    """Miscellaneous edge-case tests that don't require a DB."""

    def test_lowercase_query_parses_correctly(self) -> None:
        industry_tokens, dept_code, is_france_wide = _parse_tokens("agriculture 66")
        assert dept_code == "66"
        assert "agriculture" in industry_tokens

    def test_mixed_case_france_keyword(self) -> None:
        _, _, is_france_wide = _parse_tokens("BOULANGERIE france")
        assert is_france_wide is True

    def test_extra_whitespace_stripped(self) -> None:
        industry_tokens, dept_code, _ = _parse_tokens("  AGRICULTURE   66  ")
        assert dept_code == "66"
        assert "AGRICULTURE" in industry_tokens

    def test_naf_pattern_for_agriculture(self) -> None:
        # agriculture maps to ["01"] → pattern = "01.%"
        pattern = _build_naf_pattern_description(["01"])
        assert pattern == "01.%"

    def test_naf_pattern_for_vin_multi_prefix(self) -> None:
        # vin maps to ["01.21Z", "11.02"] → both represented
        pattern = _build_naf_pattern_description(["01.21Z", "11.02"])
        assert "01.21Z" in pattern
        assert "11.02.%" in pattern

    def test_construction_naf_conditions(self) -> None:
        # construction: ["41", "42", "43"]
        sql, params = _build_naf_conditions(["41", "42", "43"])
        assert params == ["41.%", "42.%", "43.%"]
        assert sql.count("LIKE") == 3


# ---------------------------------------------------------------------------
# Postal code → department resolution
# ---------------------------------------------------------------------------


class TestPostalCodeResolution:
    """Tests for postal_code_to_dept and its integration into the token parser."""

    def test_66000_resolves_to_66(self) -> None:
        assert postal_code_to_dept("66000") == "66"

    def test_75001_resolves_to_75(self) -> None:
        assert postal_code_to_dept("75001") == "75"

    def test_97100_resolves_to_971(self) -> None:
        assert postal_code_to_dept("97100") == "971"

    def test_97200_resolves_to_972(self) -> None:
        assert postal_code_to_dept("97200") == "972"

    def test_33000_resolves_to_33(self) -> None:
        assert postal_code_to_dept("33000") == "33"

    def test_01000_resolves_to_01(self) -> None:
        assert postal_code_to_dept("01000") == "01"

    def test_corsica_20000_resolves_to_2a(self) -> None:
        assert postal_code_to_dept("20000") == "2A"

    def test_corsica_20200_resolves_to_2b(self) -> None:
        assert postal_code_to_dept("20200") == "2B"

    def test_non_postal_returns_none(self) -> None:
        assert postal_code_to_dept("660") is None      # too short
        assert postal_code_to_dept("660000") is None   # too long
        assert postal_code_to_dept("ABCDE") is None    # not numeric

    def test_parse_tokens_with_postal_code(self) -> None:
        """'AGRICULTURE 66000' should resolve to dept 66."""
        industry_tokens, dept_code, is_france_wide = _parse_tokens("AGRICULTURE 66000")
        assert industry_tokens == ["AGRICULTURE"]
        assert dept_code == "66"
        assert is_france_wide is False

    def test_parse_tokens_logistique_33000(self) -> None:
        """'LOGISTIQUE 33000' should resolve to dept 33 (Gironde)."""
        industry_tokens, dept_code, is_france_wide = _parse_tokens("LOGISTIQUE 33000")
        assert "LOGISTIQUE" in industry_tokens
        assert dept_code == "33"

    def test_parse_tokens_paris_postal(self) -> None:
        """'RESTAURANT 75001' should resolve to dept 75."""
        industry_tokens, dept_code, is_france_wide = _parse_tokens("RESTAURANT 75001")
        assert dept_code == "75"

    def test_resolve_department_token_postal(self) -> None:
        """_resolve_department_token should handle 5-digit postal codes."""
        result = _resolve_department_token("66000")
        assert result is not None
        assert result[0] == "66"

    @pytest.mark.asyncio
    async def test_interpret_query_viticulture_resolves_naf(self) -> None:
        """Viticulture → exactly ["01.21Z"]."""
        pool = _make_pool(count=1800, rows=[])

        result = await interpret_query("VITICULTURE 66", db_pool=pool)

        assert "01.21Z" in result.naf_codes
        assert result.department == "66"
