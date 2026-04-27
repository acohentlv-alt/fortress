"""Unit tests for Phase 1 multi-candidate helpers.

Tests _geo_proximity_top_n and _gather_alternatives.

Cases:
  1. _geo_proximity_top_n returns [] when bbox is empty
  2. _geo_proximity_top_n excludes exclude_siren from output
  3. _geo_proximity_top_n returns top-k sorted by name score descending
  4. _geo_proximity_top_n returns [] when maps_lat is None
  5. _geo_proximity_top_n returns [] when maps_lng is None
  6. _gather_alternatives dedupes when trigram and geo return same SIREN
  7. _gather_alternatives caps at k even when both sources return >k
  8. _gather_alternatives returns trigram-only when maps_lat/maps_lng are None
  9. _gather_alternatives returns [] on internal exception
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from fortress.discovery import _geo_proximity_top_n, _gather_alternatives


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_conn_stub(rows):
    """Async connection stub that returns `rows` from fetchall()."""
    cur_stub = MagicMock()
    cur_stub.fetchall = AsyncMock(return_value=rows)
    conn_stub = MagicMock()
    conn_stub.execute = AsyncMock(return_value=cur_stub)
    return conn_stub


def _geo_row(siren, denom="ENTREPRISE A", enseigne="", adresse="1 rue test",
             ville="Paris", naf="47.11F"):
    """Build a fake DB row matching the 6-column SELECT in _geo_proximity_top_n:
    cg.siren, co.denomination, co.enseigne, co.adresse, co.ville, co.naf_code
    """
    return (siren, denom, enseigne, adresse, ville, naf)


# ---------------------------------------------------------------------------
# 1. _geo_proximity_top_n — empty bbox → []
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_geo_top_n_empty_bbox():
    """When no SIRENE rows are in the bounding box, return []."""
    conn = _make_conn_stub([])
    result = await _geo_proximity_top_n(conn, "Camping du Soleil", 46.0, 3.0)
    assert result == []


# ---------------------------------------------------------------------------
# 2. _geo_proximity_top_n — exclude_siren is excluded
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_geo_top_n_excludes_exclude_siren():
    """The exclude_siren argument must not appear in the output."""
    rows = [
        _geo_row("111111111", denom="CAMPING DU SOLEIL"),
        _geo_row("222222222", denom="CAMPING DES PINS"),
    ]
    conn = _make_conn_stub(rows)
    result = await _geo_proximity_top_n(
        conn, "Camping du Soleil", 46.0, 3.0, exclude_siren="111111111"
    )
    sirens = [r["siren"] for r in result]
    assert "111111111" not in sirens, "exclude_siren must not appear in output"
    assert "222222222" in sirens


# ---------------------------------------------------------------------------
# 3. _geo_proximity_top_n — top-k sorted by name score descending
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_geo_top_n_sorted_by_score_descending():
    """Results must be sorted by name score descending and capped at k."""
    rows = [
        _geo_row("111111111", denom="BOULANGERIE MARTIN"),        # low score
        _geo_row("222222222", denom="CAMPING DU SOLEIL"),         # high score
        _geo_row("333333333", denom="CAMPING DU SOLEIL BORDEAUX"),  # medium score
        _geo_row("444444444", denom="PLOMBERIE DUPONT"),          # very low score
    ]
    conn = _make_conn_stub(rows)
    result = await _geo_proximity_top_n(conn, "Camping du Soleil", 46.0, 3.0, k=3)
    assert len(result) == 3, f"Expected 3 results (k=3), got {len(result)}"
    # Scores must be non-increasing
    scores = [r["score"] for r in result]
    assert scores == sorted(scores, reverse=True), f"Scores not sorted descending: {scores}"
    # Best match should be first
    assert result[0]["siren"] == "222222222", (
        f"Expected '222222222' (best name match) first, got '{result[0]['siren']}'"
    )


# ---------------------------------------------------------------------------
# 4. _geo_proximity_top_n — maps_lat is None → []
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_geo_top_n_returns_empty_when_lat_none():
    """Return [] immediately when maps_lat is None (no DB call)."""
    conn = _make_conn_stub([])  # should not be called
    result = await _geo_proximity_top_n(conn, "Camping", None, 3.0)
    assert result == []
    conn.execute.assert_not_called()


# ---------------------------------------------------------------------------
# 5. _geo_proximity_top_n — maps_lng is None → []
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_geo_top_n_returns_empty_when_lng_none():
    """Return [] immediately when maps_lng is None (no DB call)."""
    conn = _make_conn_stub([])
    result = await _geo_proximity_top_n(conn, "Camping", 46.0, None)
    assert result == []
    conn.execute.assert_not_called()


# ---------------------------------------------------------------------------
# 6. _gather_alternatives — deduplication across trigram + geo
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_gather_alternatives_deduplicates():
    """When trigram and geo return the same SIREN, it should appear only once."""
    shared_siren = "999999999"
    trigram_result = [
        {"siren": shared_siren, "denomination": "CAMPING DU SOLEIL",
         "enseigne": "", "adresse": "1 rue", "ville": "Paris",
         "naf_code": "55.30Z", "method": "trigram_pool", "score": 0.9},
    ]
    geo_result = [
        {"siren": shared_siren, "score": 0.88, "method": "geo_proximity_alt",
         "denomination": "CAMPING DU SOLEIL", "enseigne": "",
         "adresse": "1 rue", "ville": "Paris", "naf_code": "55.30Z"},
    ]
    with (
        patch("fortress.discovery._fetch_trigram_candidates", return_value=trigram_result),
        patch("fortress.discovery._geo_proximity_top_n", return_value=geo_result),
    ):
        conn = MagicMock()
        result = await _gather_alternatives(
            conn, "Camping du Soleil", 46.0, 3.0, "33",
            exclude_siren="111111111", k=3,
        )
    sirens = [r["siren"] for r in result]
    assert sirens.count(shared_siren) == 1, (
        f"SIREN {shared_siren} appeared {sirens.count(shared_siren)} times, expected 1"
    )


# ---------------------------------------------------------------------------
# 7. _gather_alternatives — caps at k even when both sources return >k
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_gather_alternatives_caps_at_k():
    """Total results must not exceed k even when trigram+geo return more."""
    trigram_result = [
        {"siren": f"{i:09d}", "denomination": f"COMPANY {i}",
         "enseigne": "", "adresse": "", "ville": "", "naf_code": None,
         "method": "trigram_pool", "score": 0.9 - i * 0.01}
        for i in range(1, 5)  # 4 trigram results
    ]
    geo_result = [
        {"siren": f"{i:09d}", "score": 0.8, "method": "geo_proximity_alt",
         "denomination": f"COMPANY {i}", "enseigne": "",
         "adresse": "", "ville": "", "naf_code": None}
        for i in range(10, 14)  # 4 different geo results
    ]
    with (
        patch("fortress.discovery._fetch_trigram_candidates", return_value=trigram_result),
        patch("fortress.discovery._geo_proximity_top_n", return_value=geo_result),
    ):
        conn = MagicMock()
        result = await _gather_alternatives(
            conn, "Company", 46.0, 3.0, "33",
            exclude_siren="999999999", k=3,
        )
    assert len(result) <= 3, f"Expected at most 3 results (k=3), got {len(result)}"


# ---------------------------------------------------------------------------
# 8. _gather_alternatives — trigram only when lat/lng are None
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_gather_alternatives_trigram_only_when_no_coords():
    """When maps_lat/maps_lng are None, only trigram is used (geo skipped)."""
    trigram_result = [
        {"siren": "111111111", "denomination": "CAMPING DU SOLEIL",
         "enseigne": "", "adresse": "", "ville": "", "naf_code": None,
         "method": "trigram_pool", "score": 0.9},
    ]
    with (
        patch("fortress.discovery._fetch_trigram_candidates", return_value=trigram_result) as mock_trigram,
        patch("fortress.discovery._geo_proximity_top_n") as mock_geo,
    ):
        conn = MagicMock()
        result = await _gather_alternatives(
            conn, "Camping du Soleil", None, None, "33",
            exclude_siren="999999999", k=3,
        )
    mock_trigram.assert_called_once()
    mock_geo.assert_not_called()
    assert len(result) == 1
    assert result[0]["siren"] == "111111111"


# ---------------------------------------------------------------------------
# 9. _gather_alternatives — returns [] on internal exception
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_gather_alternatives_returns_empty_on_exception():
    """If trigram helper raises, _gather_alternatives must return [] (never crash)."""
    with patch(
        "fortress.discovery._fetch_trigram_candidates",
        side_effect=RuntimeError("DB unavailable"),
    ):
        conn = MagicMock()
        result = await _gather_alternatives(
            conn, "Camping du Soleil", 46.0, 3.0, "33",
            exclude_siren="111111111", k=3,
        )
    assert result == [], f"Expected [] on exception, got {result}"
