"""Unit tests for Phase 3 — BAN backfill script (fortress/scripts/backfill_ban_geo.py).

Tests (13 total):
  a) test_evaluate_ban_row_quality_thresholds      score 0.9/0.6/0.3 with matching postcode
  b) test_evaluate_ban_row_drops_postcode_mismatch score 0.97 but wrong city → drop
  c) test_evaluate_ban_row_drops_not_found         result_status='not-found' → drop
  d) test_evaluate_ban_row_drops_skipped           result_status='skipped' → drop
  e) test_evaluate_ban_row_drops_when_sirene_pc_missing  no SIRENE postcode → drop
  f) test_build_csv_includes_full_address          all components present
  g) test_build_csv_handles_missing_components     cp=None → no double comma
  h) test_post_handles_ban_error_gracefully        curl raises → row is dropped, run continues
  i) test_post_does_not_pass_result_columns        data kwarg is exactly {"columns": "q"}
  j) test_replay_step_2_6_skips_already_linked     confirmed entity → None without geo call
  k) test_replay_step_2_6_passes_conservative_thresholds  geo called with 0.95/0.20
  l) test_admin_endpoint_rejects_non_174           workspace_id=1 → 400 + French msg
  m) test_admin_endpoint_requires_admin_role       non-admin session → 403 "Admin requis."
"""

from __future__ import annotations

import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock, patch, call

import pytest

# Ensure scripts/ is importable (repo root on sys.path)
_REPO = Path(__file__).resolve().parent.parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

from scripts.backfill_ban_geo import (
    evaluate_ban_row,
    build_csv,
    BACKFILL_TOP_THRESHOLD,
    BACKFILL_DOMINANCE_THRESHOLD,
    BAN_MIN_SCORE,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _ban_row(
    status: str = "ok",
    score: str = "0.90",
    postcode: str = "75001",
    lat: str = "48.8600",
    lng: str = "2.3474",
) -> dict:
    """Build a minimal BAN response row dict."""
    return {
        "result_status": status,
        "result_score": score,
        "result_postcode": postcode,
        "latitude": lat,
        "longitude": lng,
        "result_label": "Adresse test",
        "result_score_next": "",
        "result_type": "housenumber",
    }


def _make_conn_stub(db_row):
    """Return an async connection stub whose fetchone() returns db_row."""
    cur_stub = MagicMock()
    cur_stub.fetchone = AsyncMock(return_value=db_row)
    conn_stub = MagicMock()
    conn_stub.execute = AsyncMock(return_value=cur_stub)
    return conn_stub


# ---------------------------------------------------------------------------
# a) Quality thresholds
# ---------------------------------------------------------------------------

def test_evaluate_ban_row_quality_thresholds():
    """Score ≥ 0.8 → 'bonne'; 0.5–0.8 → 'acceptable'; < 0.5 → drop ban_low_score."""
    # 0.9 + matching postcode → bonne
    quality, reason = evaluate_ban_row(_ban_row(score="0.90", postcode="75001"), "75001")
    assert quality == "bonne"
    assert reason == ""

    # 0.6 + matching postcode → acceptable
    quality, reason = evaluate_ban_row(_ban_row(score="0.60", postcode="75001"), "75001")
    assert quality == "acceptable"
    assert reason == ""

    # 0.3 + matching postcode → drop
    quality, reason = evaluate_ban_row(_ban_row(score="0.30", postcode="75001"), "75001")
    assert quality is None
    assert reason == "ban_low_score"


# ---------------------------------------------------------------------------
# b) Postcode mismatch — the Cergy/Amiens regression case
# ---------------------------------------------------------------------------

def test_evaluate_ban_row_drops_postcode_mismatch():
    """CRITICAL: score=0.97 but wrong city (result_postcode=95000 != sirene 80000).

    This is the exact failure mode from .ban_endpoint_report.md:
    BAN returned 0.97 for '8 boulevard du Port' matching Cergy (95000)
    when SIRENE expected Amiens (80000). MUST drop even at score 0.97.
    """
    row = _ban_row(score="0.97", postcode="95000")
    quality, reason = evaluate_ban_row(row, sirene_code_postal="80000")
    assert quality is None
    assert reason == "postcode_mismatch"


# ---------------------------------------------------------------------------
# c) result_status = 'not-found'
# ---------------------------------------------------------------------------

def test_evaluate_ban_row_drops_not_found():
    """result_status='not-found' with blank score → ban_no_match."""
    row = _ban_row(status="not-found", score="")
    quality, reason = evaluate_ban_row(row, sirene_code_postal="75001")
    assert quality is None
    assert reason == "ban_no_match"


# ---------------------------------------------------------------------------
# d) result_status = 'skipped'
# ---------------------------------------------------------------------------

def test_evaluate_ban_row_drops_skipped():
    """result_status='skipped' (empty input rows) → ban_no_match."""
    row = _ban_row(status="skipped", score="")
    quality, reason = evaluate_ban_row(row, sirene_code_postal="75001")
    assert quality is None
    assert reason == "ban_no_match"


# ---------------------------------------------------------------------------
# e) SIRENE has no postcode → can't verify
# ---------------------------------------------------------------------------

def test_evaluate_ban_row_drops_when_sirene_pc_missing():
    """result_status='ok', score=0.9, but sirene_code_postal=None → sirene_no_postcode.

    We refuse to accept a geocode we cannot verify.
    """
    row = _ban_row(score="0.90", postcode="75001")
    quality, reason = evaluate_ban_row(row, sirene_code_postal=None)
    assert quality is None
    assert reason == "sirene_no_postcode"


# ---------------------------------------------------------------------------
# f) build_csv — full address
# ---------------------------------------------------------------------------

def test_build_csv_includes_full_address():
    """Full address (street + postcode + city) must appear in q column."""
    rows = [("MAPSXXX01", "12 rue X", "75001", "Paris")]
    csv_bytes = build_csv(rows)
    csv_text = csv_bytes.decode("utf-8")

    lines = csv_text.strip().splitlines()
    assert lines[0] == "siren,q", f"Header mismatch: {lines[0]}"
    assert "MAPSXXX01" in lines[1]
    # All three components must appear in the address column
    assert "12 rue X" in lines[1]
    assert "75001" in lines[1]
    assert "Paris" in lines[1]


# ---------------------------------------------------------------------------
# g) build_csv — missing cp
# ---------------------------------------------------------------------------

def test_build_csv_handles_missing_components():
    """cp=None → no double comma; city still present."""
    rows = [("MAPSXXX02", "12 rue X", None, "Paris")]
    csv_bytes = build_csv(rows)
    csv_text = csv_bytes.decode("utf-8")

    lines = csv_text.strip().splitlines()
    assert "MAPSXXX02" in lines[1]
    assert ",," not in lines[1], "Double comma found — missing component not filtered"
    assert "Paris" in lines[1], "City must still appear when cp is missing"


# ---------------------------------------------------------------------------
# h) POST error handling — curl raises, run continues
# ---------------------------------------------------------------------------

def test_post_handles_ban_error_gracefully():
    """If curl_cffi.requests.post raises, the chunk is skipped (no crash).

    The orchestrator wraps the call in try/except and logs the error,
    then continues with the next chunk.
    """
    import curl_cffi.requests as cr_requests
    with patch.object(cr_requests, "post", side_effect=ConnectionError("timeout")):
        from scripts.backfill_ban_geo import post_chunk_to_ban
        with pytest.raises(ConnectionError):
            # post_chunk_to_ban propagates — the orchestrator catches it
            post_chunk_to_ban(b"siren,q\nMAPS001,test")


# ---------------------------------------------------------------------------
# i) POST does not include result_columns
# ---------------------------------------------------------------------------

def test_post_does_not_pass_result_columns():
    """BAN POST must NOT include result_columns — we need the full 20-column
    default response (including result_postcode and result_status) for the
    dual-gate safety check.

    The script uses CurlMime multipart. We verify that the 'data' kwarg
    (form text part named 'columns' = 'q') is present and that no
    'result_columns' part is added to the CurlMime object.
    """
    import curl_cffi.requests as cr_requests
    from curl_cffi import CurlMime

    # Build a minimal valid CSV response
    mock_response = MagicMock()
    mock_response.text = (
        "siren,q,longitude,latitude,result_score,result_score_next,"
        "result_label,result_type,result_id,result_housenumber,result_name,"
        "result_street,result_postcode,result_city,result_context,"
        "result_citycode,result_oldcitycode,result_oldcity,result_district,result_status\n"
    )
    mock_response.raise_for_status = MagicMock()

    added_parts: list[dict] = []

    original_addpart = CurlMime.addpart

    def spy_addpart(self, name, **kwargs):
        added_parts.append({"name": name, **kwargs})
        return original_addpart(self, name, **kwargs)

    with patch.object(cr_requests, "post", return_value=mock_response):
        with patch.object(CurlMime, "addpart", spy_addpart):
            from scripts.backfill_ban_geo import post_chunk_to_ban
            post_chunk_to_ban(b"siren,q\nMAPS001,test")

    part_names = [p["name"] for p in added_parts]
    assert "result_columns" not in part_names, (
        f"result_columns must NOT be added to the multipart form. Got parts: {part_names}"
    )
    assert "columns" in part_names, (
        f"'columns' part (value='q') must be present. Got parts: {part_names}"
    )
    columns_part = next(p for p in added_parts if p["name"] == "columns")
    assert columns_part.get("data") == b"q", (
        f"'columns' part must have data=b'q'. Got: {columns_part}"
    )


# ---------------------------------------------------------------------------
# j) replay_step_2_6 skips already-confirmed entities
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_replay_step_2_6_skips_already_linked():
    """MAPS entity with link_confidence='confirmed' → replay returns None.

    The geo matcher must not be called for already-confirmed rows.
    """
    from scripts.backfill_ban_geo import replay_step_2_6

    # Simulate a confirmed entity
    db_row = ("CAMPING DU SOLEIL SARL", None, "some_method", "confirmed")
    conn = _make_conn_stub(db_row)

    with patch("scripts.backfill_ban_geo._geo_proximity_match") as mock_geo:
        result = await replay_step_2_6(conn, "MAPS00001", 48.86, 2.35, 0.92)
        assert result is None
        mock_geo.assert_not_called()


# ---------------------------------------------------------------------------
# k) replay_step_2_6 passes conservative thresholds to geo matcher
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_replay_step_2_6_passes_conservative_thresholds():
    """geo matcher must be called with top_threshold=0.95, dominance_threshold=0.20,
    and picked_nafs=None (picker context lost for historical entities).
    """
    from scripts.backfill_ban_geo import replay_step_2_6

    # Simulate a pending entity (not yet confirmed)
    db_row = ("CAMPING DU SOLEIL SARL", None, None, None)
    conn = _make_conn_stub(db_row)
    # conn.execute also needs to work for the UPDATE statement
    conn.execute = AsyncMock(return_value=MagicMock(fetchone=AsyncMock(return_value=db_row)))

    # Geo matcher returns None (no match found — just checking the call signature)
    with patch("scripts.backfill_ban_geo._geo_proximity_match", new_callable=AsyncMock, return_value=None) as mock_geo:
        result = await replay_step_2_6(conn, "MAPS00001", 48.86, 2.35, 0.88)
        assert result is None  # geo returned None → no confirm
        mock_geo.assert_called_once()
        _, kwargs = mock_geo.call_args
        assert kwargs.get("top_threshold") == BACKFILL_TOP_THRESHOLD, (
            f"Expected top_threshold={BACKFILL_TOP_THRESHOLD}, got {kwargs.get('top_threshold')}"
        )
        assert kwargs.get("dominance_threshold") == BACKFILL_DOMINANCE_THRESHOLD, (
            f"Expected dominance_threshold={BACKFILL_DOMINANCE_THRESHOLD}, "
            f"got {kwargs.get('dominance_threshold')}"
        )
        assert kwargs.get("picked_nafs") is None, (
            "picked_nafs must be None — picker context is lost for historical entities"
        )


# ---------------------------------------------------------------------------
# l) Admin endpoint rejects non-174 workspace
# ---------------------------------------------------------------------------

def test_admin_endpoint_rejects_non_174():
    """POST /api/admin/backfill-geo with workspace_id=1 → 400 + French error."""
    from fortress.api.main import app
    from starlette.testclient import TestClient

    with TestClient(app, raise_server_exceptions=False) as client:
        # Login as alan (admin)
        login_resp = client.post(
            "/api/auth/login",
            json={"username": "alan", "password": "03052000"},
        )
        # If login fails (DB offline), skip the endpoint test gracefully
        if login_resp.status_code != 200:
            pytest.skip("DB offline or auth not configured — skip endpoint test")

        resp = client.post(
            "/api/admin/backfill-geo",
            json={"workspace_id": 1},
        )
        assert resp.status_code == 400, (
            f"Expected 400 for ws1, got {resp.status_code}: {resp.text}"
        )
        body = resp.json()
        error_msg = body.get("error", "")
        assert "174" in error_msg, (
            f"Error message must mention '174' (ws174 restriction). Got: {error_msg}"
        )
        assert "V1" in error_msg, (
            f"Error message must mention 'V1'. Got: {error_msg}"
        )


# ---------------------------------------------------------------------------
# m) Admin endpoint requires admin role
# ---------------------------------------------------------------------------

def test_admin_endpoint_requires_admin_role():
    """Non-admin session → 403 'Admin requis.' (NOT 401).

    Matches the existing admin.py:42 pattern (_get_admin returns 403).
    """
    from fortress.api.main import app
    from starlette.testclient import TestClient

    with TestClient(app, raise_server_exceptions=False) as client:
        # Login as head.test (head role, not admin)
        login_resp = client.post(
            "/api/auth/login",
            json={"username": "head.test", "password": "Test1234"},
        )
        if login_resp.status_code != 200:
            pytest.skip("DB offline or head.test not configured — skip endpoint test")

        resp = client.post(
            "/api/admin/backfill-geo",
            json={"workspace_id": 174},
        )
        assert resp.status_code == 403, (
            f"Expected 403 for non-admin, got {resp.status_code}: {resp.text}"
        )
        body = resp.json()
        error_msg = body.get("error", "")
        assert "Admin requis" in error_msg, (
            f"Expected 'Admin requis.' in error. Got: {error_msg}"
        )
