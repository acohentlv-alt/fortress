"""Unit tests for the chain/franchise detector.

Tests:
  1.  test_multi_token_phrase — "Franck Provost Coiffure Paris 11" -> coiffure hit
  2.  test_single_token_with_sector — "Paul Boulangerie" -> boulangerie hit
  3.  test_single_token_without_sector_rejected — "Paul Gautier Auto" -> None
  4.  test_unknown_brand — "Coiffure Marie Dupont" -> None
  5.  test_alias_with_accent — "Marie Blachère" and "MARIE BLACHERE" -> same hit
  6.  test_empty_or_short_input — "", "x", single space -> None
  7.  test_mcdonalds_variants — "McDo Nation", "McDonald's Bastille" -> both match
  8.  test_longer_canonical_wins — "Jacques Dessange Paris" -> "jacques dessange" (not "dessange")
  9.  test_chain_inside_longer_phrase — "Pizza Hut Express Rivoli" -> pizza hut hit
 10.  test_picker_zero_rows_returns_none
 11.  test_picker_single_row_returns_it
 12.  test_picker_ambiguous_without_branded_enseigne_returns_none
 13.  test_picker_ambiguous_resolves_with_branded_enseigne
 14.  test_chain_section_match_auto_confirm_logic — _naf_section_matches("96.02A", ["96.02B"]) == True
"""

from __future__ import annotations

from unittest.mock import AsyncMock, MagicMock

import pytest

from fortress.matching.chains import ChainHit, match_chain, find_chain_siret, CHAIN_MAP
from fortress.discovery import _naf_section_matches


# ---------------------------------------------------------------------------
# 1. Multi-token phrase detection
# ---------------------------------------------------------------------------

def test_multi_token_phrase():
    """'Franck Provost Coiffure Paris 11' must yield chain='franck provost', sector='coiffure'."""
    hit = match_chain("Franck Provost Coiffure Paris 11")
    assert hit is not None, "Expected a ChainHit for Franck Provost"
    assert hit.chain_name == "franck provost"
    assert hit.sector == "coiffure"
    assert "96.02A" in hit.nafs


# ---------------------------------------------------------------------------
# 2. Single-token brand WITH sector keyword
# ---------------------------------------------------------------------------

def test_single_token_with_sector():
    """'Paul Boulangerie' contains the brand token 'paul' plus sector keyword 'boulangerie'."""
    hit = match_chain("Paul Boulangerie")
    assert hit is not None, "Expected a ChainHit for Paul Boulangerie"
    assert hit.chain_name == "paul"
    assert hit.sector == "boulangerie"


# ---------------------------------------------------------------------------
# 3. Single-token brand WITHOUT a required sector keyword — must be rejected
# ---------------------------------------------------------------------------

def test_single_token_without_sector_rejected():
    """'Paul Gautier Auto' has the token 'paul' but no boulangerie sector keyword -> None."""
    hit = match_chain("Paul Gautier Auto")
    assert hit is None, "Single-token 'paul' with no sector keyword must not match"


# ---------------------------------------------------------------------------
# 4. Unknown brand — should not match
# ---------------------------------------------------------------------------

def test_unknown_brand():
    """'Coiffure Marie Dupont' is not a chain entry -> None."""
    hit = match_chain("Coiffure Marie Dupont")
    assert hit is None


# ---------------------------------------------------------------------------
# 5. Alias with accent and case insensitivity
# ---------------------------------------------------------------------------

def test_alias_with_accent():
    """Both 'Marie Blachère' (accented) and 'MARIE BLACHERE' (caps, unaccented) must map
    to the same ChainHit (chain_name='marie blachere')."""
    hit_accented = match_chain("Marie Blachère")
    hit_plain = match_chain("MARIE BLACHERE")

    assert hit_accented is not None, "Accented alias must match"
    assert hit_plain is not None, "Unaccented uppercase must match"
    assert hit_accented.chain_name == hit_plain.chain_name == "marie blachere"
    assert hit_accented.sector == "boulangerie"


# ---------------------------------------------------------------------------
# 6. Empty or very short input
# ---------------------------------------------------------------------------

def test_empty_or_short_input():
    """Empty string, single character, and whitespace-only must all return None."""
    assert match_chain("") is None
    assert match_chain("x") is None
    assert match_chain(" ") is None


# ---------------------------------------------------------------------------
# 7. McDonald's variants
# ---------------------------------------------------------------------------

def test_mcdonalds_variants():
    """Both 'McDo Nation' and 'McDonald s Bastille' (apostrophe stripped) must match."""
    hit_mcdo = match_chain("McDo Nation")
    assert hit_mcdo is not None, "McDo alias must match"
    assert hit_mcdo.chain_name == "mcdonalds"
    assert hit_mcdo.sector == "restauration_rapide"

    # McDonald's -> apostrophe stripped by _normalize_name -> "mcdonald s"
    hit_full = match_chain("McDonald's Bastille")
    assert hit_full is not None, "McDonald's with apostrophe must match"
    assert hit_full.chain_name == "mcdonalds"


# ---------------------------------------------------------------------------
# 8. Longer canonical wins over shorter one
# ---------------------------------------------------------------------------

def test_longer_canonical_wins():
    """'Jacques Dessange Paris' must match 'jacques dessange' (4-token canonical),
    not the shorter 'dessange' entry — longest match wins."""
    hit = match_chain("Jacques Dessange Paris")
    assert hit is not None
    assert hit.chain_name == "jacques dessange", (
        f"Expected 'jacques dessange', got '{hit.chain_name}'"
    )


# ---------------------------------------------------------------------------
# 9. Chain brand inside a longer phrase
# ---------------------------------------------------------------------------

def test_chain_inside_longer_phrase():
    """'Pizza Hut Express Rivoli' contains the phrase 'pizza hut' -> pizza hut hit."""
    hit = match_chain("Pizza Hut Express Rivoli")
    assert hit is not None
    assert hit.chain_name == "pizza hut"
    assert hit.sector == "restauration_rapide"


# ---------------------------------------------------------------------------
# Helper: build a fake DB row matching the SELECT column order in find_chain_siret
# columns: siren, denomination, enseigne, adresse, ville, code_postal, naf_code
# ---------------------------------------------------------------------------

def _make_row(siren="123456789", denomination="SARL FRANCK PROVOST", enseigne="Franck Provost",
              adresse="1 rue de la Paix", ville="Paris", cp="75001", naf="96.02A"):
    return (siren, denomination, enseigne, adresse, ville, cp, naf)


def _make_conn_stub(rows):
    """Return an async connection stub that returns `rows` from fetchall()."""
    cur_stub = MagicMock()
    cur_stub.fetchall = AsyncMock(return_value=rows)

    conn_stub = MagicMock()
    conn_stub.execute = AsyncMock(return_value=cur_stub)
    return conn_stub


# ---------------------------------------------------------------------------
# 10. Picker: zero rows -> None
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_picker_zero_rows_returns_none():
    """When no SIRENE rows match, find_chain_siret returns None."""
    conn = _make_conn_stub([])
    chain_hit = ChainHit("franck provost", frozenset({"96.02A", "96.02B"}), "coiffure", 0.95)
    result = await find_chain_siret(conn, chain_hit, "75001")
    assert result is None


# ---------------------------------------------------------------------------
# 11. Picker: single row -> return it
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_picker_single_row_returns_it():
    """When exactly one SIRENE row matches, it is returned directly."""
    row = _make_row()
    conn = _make_conn_stub([row])
    chain_hit = ChainHit("franck provost", frozenset({"96.02A", "96.02B"}), "coiffure", 0.95)
    result = await find_chain_siret(conn, chain_hit, "75001")
    assert result is not None
    assert result["siren"] == "123456789"
    assert result["method"] == "chain"


# ---------------------------------------------------------------------------
# 12. Picker: multiple rows, none with branded enseigne -> None (ambiguous)
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_picker_ambiguous_without_branded_enseigne_returns_none():
    """When multiple rows exist and none have the chain name in their enseigne/denomination,
    find_chain_siret returns None (ambiguous — safer than guessing)."""
    rows = [
        _make_row("111111111", "SARL JMC COIFFURE", "Salon de coiffure", "1 rue A", "Lyon", "69001"),
        _make_row("222222222", "EURL COIFFURE DU PORT", "Le Salon", "2 rue B", "Lyon", "69001"),
    ]
    conn = _make_conn_stub(rows)
    chain_hit = ChainHit("franck provost", frozenset({"96.02A", "96.02B"}), "coiffure", 0.95)
    result = await find_chain_siret(conn, chain_hit, "69001")
    assert result is None, "Ambiguous rows without branded enseigne must return None"


# ---------------------------------------------------------------------------
# 13. Picker: multiple rows, exactly one has branded enseigne -> return that one
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_picker_ambiguous_resolves_with_branded_enseigne():
    """When multiple rows exist and exactly one has the chain canonical tokens in
    enseigne/denomination, that row is returned."""
    rows = [
        _make_row("111111111", "SARL JMC COIFFURE", "Franck Provost", "1 rue A", "Lyon", "69001"),
        _make_row("222222222", "EURL COIFFURE DU PORT", "Le Salon", "2 rue B", "Lyon", "69001"),
    ]
    conn = _make_conn_stub(rows)
    chain_hit = ChainHit("franck provost", frozenset({"96.02A", "96.02B"}), "coiffure", 0.95)
    result = await find_chain_siret(conn, chain_hit, "69001")
    assert result is not None, "Branded enseigne row must be selected"
    assert result["siren"] == "111111111"
    assert result["method"] == "chain"


# ---------------------------------------------------------------------------
# 14. Auto-confirm integration: _naf_section_matches validates section-letter guard
# ---------------------------------------------------------------------------

def test_chain_section_match_auto_confirm_logic():
    """The chain post-else override uses _naf_section_matches to guard auto-confirm.
    Coiffure NAFs 96.02A and 96.02B both live in INSEE section S (other personal services).
    This confirms the section-letter check will pass for a chain match on coiffure.
    """
    # Both coiffure NAFs share section S
    assert _naf_section_matches("96.02A", ["96.02B"]) is True, (
        "96.02A and 96.02B must share the same INSEE section letter"
    )
    # Cross-section must not match (boulangerie 10.71C vs food retail 47.24Z)
    assert _naf_section_matches("10.71C", ["47.24Z"]) is False, (
        "10.71C (section C manufacturing) and 47.24Z (section G retail) must differ"
    )
    # Restauration NAFs share section I
    assert _naf_section_matches("56.10C", ["56.10A"]) is True, (
        "56.10C and 56.10A must share section I (restauration)"
    )


# ---------------------------------------------------------------------------
# v2 — new-sector smoke tests (camping, hotel, fitness)
# ---------------------------------------------------------------------------

def test_v2_camping_chain_detected():
    """The camping sector (added v2 after franchise HQ-leak evidence) should
    detect Siblu/Capfun/Huttopia — the exact chains that HQ-leaked at Step 1."""
    for name in ("Camping Siblu Les Landes", "Capfun Mouans-Sartoux", "Huttopia Saint-Genis"):
        hit = match_chain(name)
        assert hit is not None, f"Expected ChainHit for '{name}'"
        assert hit.sector == "camping"


def test_v2_hotel_ibis_respects_sector_gate():
    """Ibis is a single-token brand — must require a hotel sector token to avoid
    matching 'ibis' in non-hotel contexts (e.g. animal-themed business names)."""
    # Positive: hotel sector token present
    hit = match_chain("Hôtel Ibis Paris 11")
    assert hit is not None, "Hotel context should resolve Ibis"
    assert hit.sector == "hotellerie"
    # Negative: no hotel sector token
    assert match_chain("Restaurant Ibis Bar") is None, (
        "Ibis without hotel sector token should not match"
    )


def test_v2_fitness_chain_detected():
    """Fitness sector (v2) resolves Basic Fit + Fitness Park with expanded NAFs."""
    hit_bf = match_chain("Basic Fit Lyon 69003")
    assert hit_bf is not None and hit_bf.sector == "fitness"
    hit_fp = match_chain("Fitness Park Nice")
    assert hit_fp is not None and hit_fp.sector == "fitness"


def test_v2_chain_map_size_and_sectors():
    """Lock expected sector coverage at v2.1 landing: 11 sectors, ≥125 entries."""
    sectors = {e.sector for e in CHAIN_MAP}
    expected = {"boulangerie", "coiffure", "restauration_rapide", "restauration_trad",
                "optique", "camping", "hotellerie", "fitness", "ehpad", "caviste",
                "arboriculture"}
    assert sectors == expected, f"Sector mismatch: got {sectors}"
    assert len(CHAIN_MAP) >= 125, f"CHAIN_MAP should have ≥125 entries, got {len(CHAIN_MAP)}"


def test_v21_ehpad_chain_detected():
    """EHPAD sector (v2.1 — Cindy's workload) resolves Korian/Emeis/DomusVi."""
    for name in ("Korian Les Jardins", "Emeis Bordeaux", "DomusVi Nice"):
        hit = match_chain(name)
        assert hit is not None, f"Expected ChainHit for '{name}'"
        assert hit.sector == "ehpad"


def test_v21_caviste_nicolas_respects_sector_gate():
    """Nicolas is a common first name — must require wine sector token to avoid
    matching 'Nicolas Dupont Plomberie' or similar."""
    # Positive: wine sector token present
    hit = match_chain("Cave Nicolas Marseille")
    assert hit is not None and hit.sector == "caviste"
    # Negative: plumber named Nicolas is NOT the wine chain
    assert match_chain("Nicolas Dupont Plomberie") is None


def test_v21_v_and_b_multi_token():
    """V and B is multi-token: must resolve across variants."""
    for name in ("V and B Vitré", "V&B Rennes", "vins et bieres Lyon"):
        hit = match_chain(name)
        assert hit is not None, f"V and B should resolve for '{name}'"
        assert hit.sector == "caviste"


def test_v21_arboriculture_phrase_match():
    """Arboriculture multi-token brands (Fruits Rouges, Vergers du Sud) match via phrase.
    Low-ceiling sector but the few that surface should resolve."""
    hit = match_chain("Fruits Rouges Monflanquin")
    assert hit is not None and hit.sector == "arboriculture"
    hit2 = match_chain("Vergers du Sud Tarascon")
    assert hit2 is not None and hit2.sector == "arboriculture"


def test_v21_arboriculture_generic_name_gated():
    """Terrena is generic-sounding — must require a fruit/coop sector token."""
    # Positive: fruit context
    assert match_chain("Terrena Coop Pommes") is not None
    # Negative: no sector token
    assert match_chain("Terrena Dupont Garage") is None
