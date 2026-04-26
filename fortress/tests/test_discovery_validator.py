"""Tests for _validate_inpi_step0_hit — A1.1 substring-pair loosen (Apr 22).
Also tests _is_franchise_live — Phase 2 live franchise-leak check (Apr 26).
"""

from unittest.mock import AsyncMock, MagicMock

from fortress.discovery import _validate_inpi_step0_hit, _normalize_name, _INDUSTRY_WORDS, _is_franchise_live


def _mt(name: str) -> list[str]:
    """Compute meaningful_terms as the pipeline does.

    Filters tokens through _INDUSTRY_WORDS and the >= 3 char minimum.
    Falls back to all >= 3-char tokens if none survive the industry filter.
    """
    normalized = _normalize_name(name)
    search_terms = normalized.split()
    mt = [t for t in search_terms if len(t) >= 3 and t not in _INDUSTRY_WORDS]
    return mt if mt else [t for t in search_terms if len(t) >= 3]


def test_pic_mie_loosen_accepts():
    """A1.1 core case: 'Pic & Mie' should match SIRENE 'PICMIE' via substring path."""
    assert _validate_inpi_step0_hit(
        maps_cp="69002", departement="69",
        meaningful_terms=_mt("Pic & Mie"),
        local_denom="PICMIE", local_enseigne="",
        local_cp="69002", local_dept="69",
    ) is True


def test_whole_token_still_accepts():
    """Path 1 (whole-token overlap) must still work after A1.1 change."""
    assert _validate_inpi_step0_hit(
        maps_cp="69002", departement="69",
        meaningful_terms=_mt("BOULANGERIE DU DOME"),
        local_denom="BOULANGERIE DU DOME", local_enseigne="",
        local_cp="69002", local_dept="69",
    ) is True


def test_generic_pair_rejected_bel_art():
    """'Bel Art' ⊂ 'BELARTISTE' — coverage 6/10 = 0.6 < 0.9 -> reject."""
    assert _validate_inpi_step0_hit(
        maps_cp="75001", departement="75",
        meaningful_terms=_mt("Bel Art"),
        local_denom="BELARTISTE", local_enseigne="",
        local_cp="75001", local_dept="75",
    ) is False


def test_generic_pair_rejected_rue_mer():
    """'Rue Mer' ⊂ 'RUEMERVEILLE' — coverage 6/12 = 0.5 < 0.9 -> reject."""
    assert _validate_inpi_step0_hit(
        maps_cp="75001", departement="75",
        meaningful_terms=_mt("Rue Mer"),
        local_denom="RUEMERVEILLE", local_enseigne="",
        local_cp="75001", local_dept="75",
    ) is False


def test_long_sirene_substring_rejected():
    """'SOHO LOFT' ⊂ 'SOHOLOFTIMMOBILIER' — coverage 8/18 = 0.44 < 0.9 -> reject."""
    assert _validate_inpi_step0_hit(
        maps_cp="75001", departement="75",
        meaningful_terms=_mt("SOHO LOFT"),
        local_denom="SOHOLOFTIMMOBILIER", local_enseigne="",
        local_cp="75001", local_dept="75",
    ) is False


def test_dept_check_still_rejects():
    """A1.1 substring loosen must NOT bypass the dept/postal check."""
    # Pic & Mie in Lyon (69002) vs SIRENE in Paris (69002 != 75001)
    assert _validate_inpi_step0_hit(
        maps_cp="75001", departement="75",
        meaningful_terms=_mt("Pic & Mie"),
        local_denom="PICMIE", local_enseigne="",
        local_cp="69002", local_dept="69",
    ) is False


def test_chic_choc_loosen_accepts():
    """'Chic Choc' should match SIRENE 'CHICCHOC' via substring path."""
    assert _validate_inpi_step0_hit(
        maps_cp="69001", departement="69",
        meaningful_terms=_mt("Chic Choc"),
        local_denom="CHICCHOC", local_enseigne="",
        local_cp="69001", local_dept="69",
    ) is True


# ── _is_franchise_live — Phase 2 live franchise-leak check (Apr 26) ──────────
# Three candidate scenarios:
#   A) SIREN already confirmed for a MAPS entity at a DIFFERENT CP → reject
#   B) SIREN only confirmed at the SAME CP → do not reject (same-business dedup)
#   C) No prior confirmed MAPS for this SIREN → do not reject (first occurrence)


def _make_conn(count: int) -> MagicMock:
    """Build a minimal async conn mock that returns `count` from COUNT(*)."""
    cursor = AsyncMock()
    cursor.fetchone = AsyncMock(return_value=(count,))
    conn = AsyncMock()
    conn.execute = AsyncMock(return_value=cursor)
    return conn


async def test_franchise_live_rejects_different_cp():
    """Scenario A: 3 confirmed MAPS entities at different CPs → should reject."""
    conn = _make_conn(3)
    should_reject, count = await _is_franchise_live(conn, "533670709", "34000")
    assert should_reject is True
    assert count == 3


async def test_franchise_live_passes_same_cp():
    """Scenario B: SIREN confirmed only at same CP → do not reject (dedup case)."""
    # SQL filters code_postal != current_maps_cp, so same-CP rows are excluded.
    # If DB returns 0 (no conflicts at different CP), we should NOT reject.
    conn = _make_conn(0)
    should_reject, count = await _is_franchise_live(conn, "533670709", "66140")
    assert should_reject is False
    assert count == 0


async def test_franchise_live_passes_no_prior_confirmed():
    """Scenario C: No previous confirmed MAPS for this SIREN → first occurrence, do not reject."""
    conn = _make_conn(0)
    should_reject, count = await _is_franchise_live(conn, "999999999", "75001")
    assert should_reject is False
    assert count == 0


async def test_franchise_live_passes_when_no_cp():
    """When current_maps_cp is None we cannot compare → do not reject (safe default)."""
    conn = AsyncMock()  # should never be called
    should_reject, count = await _is_franchise_live(conn, "533670709", None)
    assert should_reject is False
    assert count == 0
    conn.execute.assert_not_called()


async def test_franchise_live_handles_db_error():
    """If DB query fails, return (False, 0) — never crash the pipeline."""
    conn = AsyncMock()
    conn.execute = AsyncMock(side_effect=Exception("DB timeout"))
    should_reject, count = await _is_franchise_live(conn, "533670709", "34000")
    assert should_reject is False
    assert count == 0
