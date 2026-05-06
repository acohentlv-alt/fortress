"""Unit tests for _compute_naf_status strict mode toggle.

Tests verify that the strict=True parameter:
  - disables clique/sibling expansion (SECTOR_EXPANSIONS)
  - preserves strict prefix matching
  - preserves section-letter division_whitelist matching

Non-strict (default) behaviour is also tested to confirm no regression.
"""

from fortress.discovery import _compute_naf_status


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
