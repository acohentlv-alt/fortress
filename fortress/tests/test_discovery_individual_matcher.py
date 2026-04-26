"""Unit tests for TOP PRIORITY 2 — Individual cat_jur 1000 matcher (agriculture pass 2).

Rewrites the prior 8 tests from the broken `if not rows:` trigger.
These 12 tests reflect the corrected trigger: pass 2 fires when pass 1 has rows
but no Band A candidate (top_sim < 0.90), OR pass 1 is empty — both gated by
agriculture-only check (every NAF prefix starts with '01.').

Tests:
  T1  test_indiv_threshold_exactly_085_accepts
  T2  test_indiv_threshold_0849_rejects
  T3  test_pass2_skipped_when_pass1_band_a                    (regression guard)
  T4  test_pass2_fires_when_pass1_below_band_a                (central structural fix)
  T5  test_pass2_skipped_when_picker_not_agriculture
  T6  test_pass2_skipped_when_division_whitelist_nonagri
  T7  test_pass2_handles_empty_pass1
  T8  test_pass2_returns_none_when_all_enseignes_null
  T9  test_pass2_meta_includes_pass_marker_and_pass1_diagnostics
  T10 test_pass2_does_not_emit_band_b_marker
  T11 test_pass2_scores_by_enseigne_only_not_greatest
  T12 test_leak_prevention_pass2_never_fires_outside_agriculture (parametrized)
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock, call

import pytest

from fortress.discovery import _cp_name_disamb_match


# ---------------------------------------------------------------------------
# Fixture helpers
# ---------------------------------------------------------------------------

def _make_row(
    siren: str = "123456789",
    denomination: str = "JEAN DUPONT",
    enseigne: str = "Domaine de la Foret",
    naf_code: str = "01.24Z",
    forme_juridique: str = "1000",
    code_postal: str = "51530",
    ville: str = "MARDEUIL",
    adresse: str = "1 RUE DES CHAMPS",
    sim: float = 0.92,
) -> tuple:
    """Return a 19-element tuple matching the SELECT column order in _cp_name_disamb_match.

    Column order:
      0  siren
      1  siret_siege
      2  denomination
      3  enseigne
      4  naf_code
      5  naf_libelle
      6  forme_juridique
      7  adresse
      8  code_postal
      9  ville
      10 departement
      11 region
      12 statut
      13 date_creation
      14 tranche_effectif
      15 latitude
      16 longitude
      17 fortress_id
      18 sim  (last computed column)
    """
    return (
        siren,          # 0
        None,           # 1 siret_siege
        denomination,   # 2
        enseigne,       # 3
        naf_code,       # 4
        None,           # 5 naf_libelle
        forme_juridique, # 6
        adresse,        # 7
        code_postal,    # 8
        ville,          # 9
        None,           # 10 departement
        None,           # 11 region
        "A",            # 12 statut
        None,           # 13 date_creation
        None,           # 14 tranche_effectif
        None,           # 15 latitude
        None,           # 16 longitude
        None,           # 17 fortress_id
        sim,            # 18 sim (computed)
    )


def _make_conn(pass1_rows: list[tuple], pass2_rows: list[tuple]) -> MagicMock:
    """Build a mock conn whose execute() returns cursors sequentially.

    First call to conn.execute(...) → cursor with pass1_rows.
    Second call to conn.execute(...) → cursor with pass2_rows.
    """
    cur1 = AsyncMock()
    cur1.fetchall = AsyncMock(return_value=pass1_rows)

    cur2 = AsyncMock()
    cur2.fetchall = AsyncMock(return_value=pass2_rows)

    conn = MagicMock()
    conn.execute = AsyncMock(side_effect=[cur1, cur2])
    return conn


def _make_conn_single(rows: list[tuple]) -> MagicMock:
    """Build a mock conn whose execute() returns only one cursor (pass 1 only)."""
    cur1 = AsyncMock()
    cur1.fetchall = AsyncMock(return_value=rows)

    conn = MagicMock()
    conn.execute = AsyncMock(return_value=cur1)
    return conn


# ---------------------------------------------------------------------------
# T1 — threshold exactly 0.85 accepts
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_indiv_threshold_exactly_085_accepts():
    """Pass 1 returns rows with top_sim=0.20 (below Band A).
    Pass 2 fires. Pass 2's first row has sim_enseigne=0.85 exactly.
    Helper must return method='cp_name_disamb_indiv'.
    """
    pass1_row = _make_row(sim=0.20, forme_juridique="1000")
    pass2_row = _make_row(sim=0.85, forme_juridique="1000")

    conn = _make_conn(pass1_rows=[pass1_row], pass2_rows=[pass2_row])

    result = await _cp_name_disamb_match(
        conn=conn,
        maps_name="Domaine de la Foret",
        maps_cp="51530",
        picked_nafs=["01.24Z"],
        naf_division_whitelist=None,
    )

    assert result is not None, "Expected a result at threshold 0.85"
    assert result["method"] == "cp_name_disamb_indiv"
    assert result["score"] == 0.85


# ---------------------------------------------------------------------------
# T2 — threshold 0.849 rejects
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_indiv_threshold_0849_rejects():
    """Pass 1 returns rows with top_sim=0.20 (below Band A).
    Pass 2 fires. Pass 2's first row has sim_enseigne=0.849.
    Pass 2 falls through. Pass 1 no_band path → helper returns None.
    """
    pass1_row = _make_row(sim=0.20, forme_juridique="1000")
    pass2_row = _make_row(sim=0.849, forme_juridique="1000")

    conn = _make_conn(pass1_rows=[pass1_row], pass2_rows=[pass2_row])

    result = await _cp_name_disamb_match(
        conn=conn,
        maps_name="Domaine de la Foret",
        maps_cp="51530",
        picked_nafs=["01.24Z"],
        naf_division_whitelist=None,
    )

    assert result is None, "Expected None when top2_sim=0.849 (below 0.85)"


# ---------------------------------------------------------------------------
# T3 — pass 2 skipped when pass 1 hits Band A (regression guard)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pass2_skipped_when_pass1_band_a():
    """Pass 1 returns rows with top_sim=0.95 (Band A hit).
    Pass 2 SQL must NOT fire (conn.execute called exactly ONCE).
    Helper returns method='cp_name_disamb' with the Band A candidate.

    THIS IS THE LOAD-BEARING REGRESSION GUARD: pass 1's positive-Band-A
    path must never invoke pass 2.
    """
    pass1_row = _make_row(sim=0.95, denomination="EARL DOMAINE FORET", forme_juridique="6599")

    cur1 = AsyncMock()
    cur1.fetchall = AsyncMock(return_value=[pass1_row])

    conn = MagicMock()
    conn.execute = AsyncMock(return_value=cur1)

    result = await _cp_name_disamb_match(
        conn=conn,
        maps_name="Domaine de la Foret",
        maps_cp="51530",
        picked_nafs=["01.24Z"],
        naf_division_whitelist=None,
    )

    assert result is not None
    assert result["method"] == "cp_name_disamb", (
        f"Expected cp_name_disamb (pass 1 Band A), got {result['method']}"
    )
    # conn.execute must be called exactly ONCE — no pass 2 SQL issued
    assert conn.execute.call_count == 1, (
        f"Pass 2 must not fire when pass 1 hits Band A. "
        f"conn.execute was called {conn.execute.call_count} times."
    )


# ---------------------------------------------------------------------------
# T4 — pass 2 fires when pass 1 is below Band A (central structural fix test)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pass2_fires_when_pass1_below_band_a():
    """Pass 1 returns rows with top_sim=0.40 (below 0.90).
    Pass 2 fires (conn.execute called TWICE).
    Pass 2's first row has sim_enseigne=0.92 → helper returns
    method='cp_name_disamb_indiv'.

    This is the central test demonstrating the bug fix: the old trigger
    `if not rows:` could never fire here because pass 1 has rows. The new
    trigger `if not pass1_has_band_a` correctly fires.
    """
    pass1_row = _make_row(sim=0.40, forme_juridique="6599")  # EARL, not 1000
    pass2_row = _make_row(sim=0.92, forme_juridique="1000", enseigne="Domaine de la Foret")

    conn = _make_conn(pass1_rows=[pass1_row], pass2_rows=[pass2_row])

    result = await _cp_name_disamb_match(
        conn=conn,
        maps_name="Domaine de la Foret",
        maps_cp="51530",
        picked_nafs=["01.24Z"],
        naf_division_whitelist=None,
    )

    assert result is not None, "Expected a result when pass 2 finds sim_enseigne=0.92"
    assert result["method"] == "cp_name_disamb_indiv", (
        f"Expected cp_name_disamb_indiv (pass 2), got {result['method']}"
    )
    # conn.execute must be called TWICE (pass 1 + pass 2)
    assert conn.execute.call_count == 2, (
        f"Pass 2 must fire when pass 1 has rows but top_sim < Band A. "
        f"conn.execute was called {conn.execute.call_count} times."
    )


# ---------------------------------------------------------------------------
# T5 — pass 2 skipped when picker is not agriculture
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pass2_skipped_when_picker_not_agriculture():
    """picked_nafs=['56.10A'] (restauration). Pass 1 returns rows with top_sim=0.30
    (below Band A). Pass 2 must NOT fire — agriculture gate rejects.
    Helper returns None via pass 1's no_band path.
    """
    pass1_row = _make_row(sim=0.30, naf_code="56.10A", forme_juridique="5499")

    cur1 = AsyncMock()
    cur1.fetchall = AsyncMock(return_value=[pass1_row])

    conn = MagicMock()
    conn.execute = AsyncMock(return_value=cur1)

    result = await _cp_name_disamb_match(
        conn=conn,
        maps_name="Le Petit Bistrot",
        maps_cp="51000",
        picked_nafs=["56.10A"],
        naf_division_whitelist=None,
    )

    assert result is None, "Expected None for non-agriculture picker"
    assert conn.execute.call_count == 1, (
        f"Pass 2 must not fire on non-agriculture NAF. "
        f"conn.execute called {conn.execute.call_count} times."
    )


# ---------------------------------------------------------------------------
# T6 — pass 2 skipped when division whitelist is non-agriculture
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pass2_skipped_when_division_whitelist_nonagri():
    """picked_nafs=['I'] (section letter), naf_division_whitelist=['56.10', '56.21'].
    Pass 2 must NOT fire (whitelist entries don't all start with '01.').
    """
    pass1_row = _make_row(sim=0.30, naf_code="56.10A", forme_juridique="5499")

    cur1 = AsyncMock()
    cur1.fetchall = AsyncMock(return_value=[pass1_row])

    conn = MagicMock()
    conn.execute = AsyncMock(return_value=cur1)

    result = await _cp_name_disamb_match(
        conn=conn,
        maps_name="Le Petit Bistrot",
        maps_cp="51000",
        picked_nafs=["I"],
        naf_division_whitelist=["56.10", "56.21"],
    )

    assert result is None
    assert conn.execute.call_count == 1, (
        f"Pass 2 must not fire when whitelist is non-agriculture. "
        f"conn.execute called {conn.execute.call_count} times."
    )


# ---------------------------------------------------------------------------
# T7 — pass 2 handles empty pass 1 (empty-pass-1 regression)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pass2_handles_empty_pass1():
    """Pass 1 returns []. picked_nafs=['01.24Z']. Pass 2 fires (gate passes;
    empty pass 1 is included in the trigger — pass1_has_band_a is False).
    Pass 2's first row has sim_enseigne=0.91 → helper returns
    method='cp_name_disamb_indiv'.

    Regression test: this was the ONLY case the prior exec's trigger handled.
    We keep it to ensure the new trigger still covers the empty case.
    """
    pass2_row = _make_row(sim=0.91, forme_juridique="1000", enseigne="Domaine de la Foret")

    conn = _make_conn(pass1_rows=[], pass2_rows=[pass2_row])

    result = await _cp_name_disamb_match(
        conn=conn,
        maps_name="Domaine de la Foret",
        maps_cp="51530",
        picked_nafs=["01.24Z"],
        naf_division_whitelist=None,
    )

    assert result is not None, "Expected a result when pass 2 finds sim_enseigne=0.91"
    assert result["method"] == "cp_name_disamb_indiv"
    assert conn.execute.call_count == 2


# ---------------------------------------------------------------------------
# T8 — pass 2 returns None when all enseignes null (edge case)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pass2_returns_none_when_all_enseignes_null():
    """Pass 1 returns rows with top_sim=0.30. Pass 2 fires but its cursor
    returns [] (no code-1000 rows with non-empty enseigne exist at this CP+NAF).
    Helper falls through to pass 1's no_band path → returns None.

    The prior brief missed this edge case: pass 2 might find no candidates
    and pass 1 still has non-Band-A rows; we must not raise, and must not
    return a partial pass-2 dict.
    """
    pass1_row = _make_row(sim=0.30, forme_juridique="6599")

    conn = _make_conn(pass1_rows=[pass1_row], pass2_rows=[])

    result = await _cp_name_disamb_match(
        conn=conn,
        maps_name="Domaine Inconnu",
        maps_cp="51530",
        picked_nafs=["01.24Z"],
        naf_division_whitelist=None,
    )

    assert result is None, "Expected None when pass 2 finds no code-1000 rows"


# ---------------------------------------------------------------------------
# T9 — pass 2 meta includes pass marker and pass 1 diagnostics
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pass2_meta_includes_pass_marker_and_pass1_diagnostics():
    """When pass 2 confirms, returned dict has cp_name_disamb_meta.pass == 2,
    forme_juridique_filter == '1000', pass1_pool_size (int >= 0),
    and pass1_top_sim (float).
    """
    pass1_row = _make_row(sim=0.20, forme_juridique="6599")
    pass2_row = _make_row(sim=0.91, forme_juridique="1000", enseigne="Domaine de la Foret")

    conn = _make_conn(pass1_rows=[pass1_row], pass2_rows=[pass2_row])

    result = await _cp_name_disamb_match(
        conn=conn,
        maps_name="Domaine de la Foret",
        maps_cp="51530",
        picked_nafs=["01.24Z"],
        naf_division_whitelist=None,
    )

    assert result is not None
    assert result["method"] == "cp_name_disamb_indiv"
    meta = result.get("cp_name_disamb_meta", {})

    assert meta.get("pass") == 2, f"Expected pass=2 in meta, got {meta}"
    assert meta.get("forme_juridique_filter") == "1000", (
        f"Expected forme_juridique_filter='1000' in meta, got {meta}"
    )
    assert isinstance(meta.get("pass1_pool_size"), int), (
        f"Expected pass1_pool_size as int in meta, got {meta}"
    )
    assert isinstance(meta.get("pass1_top_sim"), float), (
        f"Expected pass1_top_sim as float in meta, got {meta}"
    )
    # pass1_pool_size should equal 1 (one row in pass 1)
    assert meta["pass1_pool_size"] == 1
    assert meta["pass1_top_sim"] == 0.2


# ---------------------------------------------------------------------------
# T10 — pass 2 does NOT emit band_b marker
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pass2_does_not_emit_band_b_marker():
    """When pass 2 confirms, returned dict must NOT contain key 'cp_name_disamb_band'.

    If present, the auto-confirm gate at discovery.py line 2743 would treat it as
    Band B and force pending — guard against future drift.
    """
    pass1_row = _make_row(sim=0.20, forme_juridique="6599")
    pass2_row = _make_row(sim=0.91, forme_juridique="1000", enseigne="Domaine de la Foret")

    conn = _make_conn(pass1_rows=[pass1_row], pass2_rows=[pass2_row])

    result = await _cp_name_disamb_match(
        conn=conn,
        maps_name="Domaine de la Foret",
        maps_cp="51530",
        picked_nafs=["01.24Z"],
        naf_division_whitelist=None,
    )

    assert result is not None
    assert "cp_name_disamb_band" not in result, (
        "Pass 2 result must NOT contain cp_name_disamb_band key — "
        "it would incorrectly route to Band B forced-pending logic."
    )


# ---------------------------------------------------------------------------
# T11 — pass 2 scores by enseigne only, NOT GREATEST
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_pass2_scores_by_enseigne_only_not_greatest():
    """Inspect the SQL that pass 2 issues. Assert:
    - SQL contains `similarity(COALESCE(enseigne,'')` exactly once
    - SQL does NOT contain `GREATEST(`
    - Parameter list passed to pass 2 has length 1 + 1 + len(naf_prefixes)
      (one maps_name for the similarity, one maps_cp, one per NAF prefix)
      NOT 2 + 1 + ... (which would indicate GREATEST was kept by mistake)
    """
    pass1_row = _make_row(sim=0.20, forme_juridique="6599")
    pass2_row = _make_row(sim=0.91, forme_juridique="1000", enseigne="Domaine de la Foret")

    conn = _make_conn(pass1_rows=[pass1_row], pass2_rows=[pass2_row])

    picked_nafs = ["01.24Z"]

    await _cp_name_disamb_match(
        conn=conn,
        maps_name="Domaine de la Foret",
        maps_cp="51530",
        picked_nafs=picked_nafs,
        naf_division_whitelist=None,
    )

    assert conn.execute.call_count == 2, "Expected pass 1 + pass 2 calls"

    # Inspect the second call (pass 2)
    pass2_call = conn.execute.call_args_list[1]
    pass2_sql = pass2_call[0][0]   # first positional arg = SQL string
    pass2_params = pass2_call[0][1] # second positional arg = params list

    # SQL checks
    assert "similarity(COALESCE(enseigne,'')," in pass2_sql.replace(" ", "").replace("\n", ""), (
        f"Pass 2 SQL must use similarity(COALESCE(enseigne,''), ...). Got:\n{pass2_sql}"
    )
    assert "GREATEST(" not in pass2_sql, (
        f"Pass 2 SQL must NOT use GREATEST(). Got:\n{pass2_sql}"
    )

    # Parameter count: 1 (maps_name) + 1 (maps_cp) + len(naf_prefixes)
    expected_param_count = 1 + 1 + len(picked_nafs)
    assert len(pass2_params) == expected_param_count, (
        f"Pass 2 params should have {expected_param_count} entries "
        f"(1 maps_name + 1 maps_cp + {len(picked_nafs)} NAF prefix), "
        f"got {len(pass2_params)}: {pass2_params}"
    )


# ---------------------------------------------------------------------------
# T12 — leak prevention: pass 2 never fires outside agriculture (parametrized)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
@pytest.mark.parametrize("picker", [
    ["56.10A"],
    ["I"],
    ["10.71B"],
    ["86.10Z"],
    ["56.30Z"],
])
async def test_leak_prevention_pass2_never_fires_outside_agriculture(picker):
    """Belt-and-braces regression guard for the agriculture gate.

    For each non-agriculture picker, pass 1 returns rows with top_sim=0.20.
    Assert pass 2 SQL never executes (conn.execute called exactly ONCE per
    parametrization).
    """
    pass1_row = _make_row(sim=0.20, naf_code=picker[0], forme_juridique="5499")

    cur1 = AsyncMock()
    cur1.fetchall = AsyncMock(return_value=[pass1_row])

    conn = MagicMock()
    conn.execute = AsyncMock(return_value=cur1)

    result = await _cp_name_disamb_match(
        conn=conn,
        maps_name="Some Business",
        maps_cp="75001",
        picked_nafs=picker,
        naf_division_whitelist=None,
    )

    assert result is None, (
        f"Expected None for non-agriculture picker {picker} with top_sim=0.20"
    )
    assert conn.execute.call_count == 1, (
        f"Pass 2 must NOT fire for picker={picker}. "
        f"conn.execute called {conn.execute.call_count} times."
    )
