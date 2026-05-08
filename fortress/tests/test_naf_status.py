"""Unit tests for _compute_naf_status strict mode toggle and Step 2.7
establishment-enseigne disambiguation helpers.

Tests verify that the strict=True parameter:
  - disables clique/sibling expansion (SECTOR_EXPANSIONS)
  - preserves strict prefix matching
  - preserves section-letter division_whitelist matching

Non-strict (default) behaviour is also tested to confirm no regression.

Step 2.7 tests (Brief 01) verify _disambiguate_etab_rows and _etab_display_enseigne:
  - camping municipal pattern: head denomination is commune, enseigne_etablissement
    is the camping brand — disambiguation must succeed via establishment-level fields.
  - single-row returned enseigne prefers establishment-level over head-SIREN c.enseigne.
  - zero-overlap on ALL fields falls through unchanged (returns None).
"""

from fortress.discovery import _compute_naf_status, _disambiguate_etab_rows, _etab_display_enseigne


def test_strict_skips_clique():
    """Strict mode: 55.10Z (hotel) should NOT match picker 55.30Z (camping) via clique."""
    # In non-strict mode, SECTOR_EXPANSIONS contains the hotel clique so this would be 'verified'.
    # In strict mode, only strict prefix match is allowed — 55.10Z does NOT start with 55.30Z.
    assert _compute_naf_status("55.10Z", ["55.30Z"], None, strict=True) == "mismatch"


def test_strict_strict_prefix_still_works():
    """Strict mode: exact prefix match is always valid."""
    # 55.30Z starts with 55.30Z — should be 'verified' even in strict mode.
    assert _compute_naf_status("55.30Z", ["55.30Z"], None, strict=True) == "verified"


def test_strict_section_letter_still_works():
    """Strict mode: division_whitelist (section letter) check is preserved."""
    # Section letter "I" → division whitelist [55, 56] → matched 55.30Z starts with "55".
    assert _compute_naf_status("55.30Z", ["I"], ["55", "56"], strict=True) == "verified"


def test_non_strict_unchanged_via_clique():
    """Non-strict (default): hotel clique allows 55.10Z to match camping picker 55.30Z."""
    # Non-strict: 55.30Z (picker) and 55.10Z (matched) verify via SECTOR_EXPANSIONS hotel clique.
    assert _compute_naf_status("55.10Z", ["55.30Z"], None, strict=False) == "verified"
    # Default arg (no strict param) also works identically.
    assert _compute_naf_status("55.10Z", ["55.30Z"], None) == "verified"


def test_strict_sibling_under_leaf_picker():
    """Strict mode: 10.71D in 10.71C clique → mismatch (boulangerie)."""
    assert _compute_naf_status("10.71D", ["10.71C"], None, strict=True) == "mismatch"


def test_strict_cross_sector_clique():
    """Strict mode: 47.24Z in 10.71C cross-sector clique → mismatch."""
    assert _compute_naf_status("47.24Z", ["10.71C"], None, strict=True) == "mismatch"


def test_strict_intra_sector_restau_clique():
    """Strict mode: 56.10A in 56.10C restauration clique → mismatch."""
    assert _compute_naf_status("56.10A", ["56.10C"], None, strict=True) == "mismatch"


def test_strict_arboriculture_clique():
    """Strict mode: 01.24Z in 01.13Z arboriculture clique → mismatch (Cindy's 53% sector regression guard)."""
    assert _compute_naf_status("01.24Z", ["01.13Z"], None, strict=True) == "mismatch"


# ── Step 2.7 establishment-enseigne disambiguation tests (Brief 01 camping) ──
# Row column layout for _disambiguate_etab_rows and _etab_display_enseigne:
#   r[0]  e.siret
#   r[1]  e.siren
#   r[2]  e.naf_etablissement
#   r[3]  e.code_postal_etab
#   r[4]  c.denomination         ← head SIREN legal name
#   r[5]  c.enseigne             ← head SIREN trade name
#   r[6]  c.adresse
#   r[7]  c.ville
#   r[8]  c.statut
#   r[9]  e.enseigne_etablissement  ← preferred display name (NEW)
#   r[10] e.denomination_usuelle   ← second choice (NEW)

def _etab_row(siret, siren, naf="55.30Z", cp="64390",
              denomination="", enseigne="", adresse="1 rue test",
              ville="Sauveterre-de-Bearn", statut="A",
              enseigne_etablissement=None, denomination_usuelle=None):
    """Build a fake 11-column Step 2.7 establishment row (post-Brief-01 SELECT)."""
    return (siret, siren, naf, cp, denomination, enseigne, adresse, ville,
            statut, enseigne_etablissement, denomination_usuelle)


def test_step27_camping_municipal_disambiguates_via_etab_enseigne():
    """Camping municipal pattern: head SIREN denomination is the commune name,
    but e.enseigne_etablissement contains the camping brand. Disambiguation
    must succeed via establishment-level fields.

    Pre-fix: token overlap computed from c.denomination + c.enseigne only.
    Row 1 head denomination = "COMMUNE DE SAUVETERRE DE BEARN" → 0 overlap with "Camping du Gave".
    Row 2 head denomination = "TOMESA" → 0 overlap.
    Both score 0 → falls through → unmatched.

    Post-fix: token overlap also draws from e.enseigne_etablissement.
    Row 1 enseigne_etablissement = "CAMPING MUNICIPAL DU GAVE" → overlap = {camping, gave} = 2.
    Row 2 enseigne_etablissement = None → overlap = 0.
    Row 1 wins as dominant → returns siret_address_naf candidate for row 1.
    """
    rows = [
        _etab_row(
            siret="21640513400026", siren="216405134",
            denomination="COMMUNE DE SAUVETERRE DE BEARN", enseigne="",
            enseigne_etablissement="CAMPING MUNICIPAL DU GAVE",
        ),
        _etab_row(
            siret="83384194300016", siren="833841943",
            denomination="TOMESA", enseigne="",
            enseigne_etablissement=None,
        ),
    ]
    result = _disambiguate_etab_rows("Camping du Gave", rows)
    assert result is not None, "Disambiguation should succeed via enseigne_etablissement"
    assert result["siren"] == "216405134", (
        f"Expected winner siren 216405134 (the commune), got {result['siren']}"
    )
    assert result["method"] == "siret_address_naf"
    # Returned enseigne should be the establishment-level enseigne, not the commune name
    assert result["enseigne"] == "CAMPING MUNICIPAL DU GAVE", (
        f"Expected enseigne 'CAMPING MUNICIPAL DU GAVE', got '{result['enseigne']}'"
    )


def test_step27_single_row_returns_etab_enseigne_when_present():
    """_etab_display_enseigne: when e.enseigne_etablissement is present, it must be
    preferred over the head-SIREN c.enseigne (r[5]).
    """
    row = _etab_row(
        siret="21640513400026", siren="216405134",
        denomination="COMMUNE DE SAUVETERRE DE BEARN",
        enseigne="",  # head SIREN c.enseigne is empty
        enseigne_etablissement="CAMPING MUNICIPAL DU GAVE",
        denomination_usuelle=None,
    )
    result = _etab_display_enseigne(row)
    assert result == "CAMPING MUNICIPAL DU GAVE", (
        f"Expected 'CAMPING MUNICIPAL DU GAVE', got '{result}'"
    )


def test_step27_disamb_no_overlap_falls_through_unchanged():
    """Regression guard: when neither head-SIREN nor establishment-level fields
    overlap with maps_name tokens across ALL rows, _disambiguate_etab_rows must
    return None so Step 2.7 falls through to Step 5.

    This verifies the no-false-positive property: the fix never creates a winner
    from zero-overlap data.
    """
    rows = [
        _etab_row(
            siret="11122233300001", siren="111222333",
            denomination="ASSOCIATION DES PECHES", enseigne="",
            enseigne_etablissement="SOCIETE SPORTIVE PISCICOLE",
        ),
        _etab_row(
            siret="44455566600001", siren="444555666",
            denomination="SYNDICAT INTERCOMMUNAL", enseigne="",
            enseigne_etablissement="BASE DE LOISIRS NAUTIQUES",
        ),
    ]
    # maps_name "Camping du Lac" → tokens: {camping, lac} (after _normalize_name)
    # Row 1: tokens from "ASSOCIATION DES PECHES SOCIETE SPORTIVE PISCICOLE" → no camping/lac
    # Row 2: tokens from "SYNDICAT INTERCOMMUNAL BASE DE LOISIRS NAUTIQUES" → no camping/lac
    result = _disambiguate_etab_rows("Camping du Lac", rows)
    assert result is None, (
        f"Expected None (fall-through) when no rows overlap with maps_name, got {result}"
    )
