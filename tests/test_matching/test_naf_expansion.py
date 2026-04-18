"""Unit tests for Option C — per-sector NAF expansion (Apr 18)."""
from fortress.discovery import _compute_naf_status
from fortress.config.naf_sector_expansion import SECTOR_EXPANSIONS


def test_strict_prefix_still_verified():
    # Picker = 10.71C, matched SIREN = 10.71C → strict prefix match
    assert _compute_naf_status("10.71C", "10.71C", None) == "verified"
    # Longer prefix still matches (defensive — unlikely in real data)
    assert _compute_naf_status("10.71CAB", "10.71C", None) == "verified"


def test_sibling_in_expansion_verified():
    # 10.71C picker, SIREN = 10.71D pâtisserie → expansion path
    assert "10.71D" in SECTOR_EXPANSIONS["10.71C"]
    assert _compute_naf_status("10.71D", "10.71C", None) == "verified"
    # 10.71C picker, SIREN = 47.24Z retail bakery → expansion path (Alan-approved cross-section)
    assert "47.24Z" in SECTOR_EXPANSIONS["10.71C"]
    assert _compute_naf_status("47.24Z", "10.71C", None) == "verified"
    # 49.41A picker (fret interurbain), SIREN = 52.29B affrètement → expansion path
    assert "52.29B" in SECTOR_EXPANSIONS["49.41A"]
    assert _compute_naf_status("52.29B", "49.41A", None) == "verified"


def test_cross_sector_mismatch_critical_guardrail():
    """Option B regression guard. These are the exclusions we MUST protect."""
    # Boulangerie picker must NOT auto-confirm restaurant or supermarket
    assert _compute_naf_status("56.10C", "10.71C", None) == "mismatch"
    assert _compute_naf_status("47.11D", "10.71C", None) == "mismatch"
    assert _compute_naf_status("47.11F", "10.71C", None) == "mismatch"
    # Épicerie must NOT auto-confirm supermarché/hyper
    assert _compute_naf_status("47.11D", "47.11B", None) == "mismatch"
    assert _compute_naf_status("47.11F", "47.11B", None) == "mismatch"
    # Plomberie must NOT auto-confirm électricité
    assert _compute_naf_status("43.21A", "43.22A", None) == "mismatch"
    # Nettoyage courant must NOT auto-confirm paysagisme (espaces verts)
    assert _compute_naf_status("81.30Z", "81.21Z", None) == "mismatch"
    # Restauration rapide must NOT auto-confirm resto traditionnel (different business model)
    assert _compute_naf_status("56.10A", "56.10C", None) == "mismatch"


def test_section_whitelist_still_wins():
    # Picker = section letter "I", whitelist = ["55","56"], matched = 55.30Z → verified
    assert _compute_naf_status("55.30Z", "I", ["55", "56"]) == "verified"
    # Same whitelist, matched NAF outside whitelist → mismatch
    assert _compute_naf_status("10.71C", "I", ["55", "56"]) == "mismatch"


def test_unmapped_picker_falls_through():
    # Picker 99.99Z is NOT in SECTOR_EXPANSIONS. Strict prefix behavior only.
    assert "99.99Z" not in SECTOR_EXPANSIONS
    # Self-match still verified via strict prefix
    assert _compute_naf_status("99.99Z", "99.99Z", None) == "verified"
    # Non-matching, non-expanded → mismatch
    assert _compute_naf_status("10.71C", "99.99Z", None) == "mismatch"


def test_none_matched_naf_is_mismatch():
    # If SIRENE didn't provide a NAF, status must be mismatch (safe)
    assert _compute_naf_status(None, "10.71C", None) == "mismatch"


def test_seed_map_has_45_keys():
    """Regression: accidental edits to the map that drop/add entries should trip."""
    assert len(SECTOR_EXPANSIONS) == 45


def test_seed_map_all_values_are_frozenset():
    assert all(isinstance(v, frozenset) for v in SECTOR_EXPANSIONS.values())
    assert all(isinstance(k, str) for k in SECTOR_EXPANSIONS.keys())


def test_asymmetric_reverses_closed():
    """Each of the 8 new reverse-keys must verify against siblings in their cluster."""

    # 43.21B reverse (électricité voie publique ↔ bâtiment)
    assert _compute_naf_status("43.21A", "43.21B", None) == "verified"
    assert _compute_naf_status("43.21B", "43.21A", None) == "verified"

    # 43.32C reverse (menuiserie agencement ↔ pose)
    assert _compute_naf_status("43.32A", "43.32C", None) == "verified"
    assert _compute_naf_status("43.32C", "43.32A", None) == "verified"

    # 10.13A reverse (transformation viande ↔ charcuterie)
    assert _compute_naf_status("10.13B", "10.13A", None) == "verified"
    assert _compute_naf_status("10.13A", "10.13B", None) == "verified"

    # 10.71D reverse (cuisson pain ↔ boulangerie traditionnelle)
    # Regression: ANTONE Artisan Boulanger (boulangerie 33000, Apr 18)
    # Picker 10.71D, SIRENE 10.71C → must now be verified.
    assert _compute_naf_status("10.71C", "10.71D", None) == "verified"
    assert _compute_naf_status("10.71D", "10.71C", None) == "verified"

    # fret cluster — 49.41C reverse
    assert _compute_naf_status("49.41A", "49.41C", None) == "verified"
    assert _compute_naf_status("49.41C", "49.41A", None) == "verified"

    # fret cluster — 49.42Z reverse
    assert _compute_naf_status("49.41A", "49.42Z", None) == "verified"
    assert _compute_naf_status("49.42Z", "49.41B", None) == "verified"

    # fret cluster — 52.29A reverse
    assert _compute_naf_status("49.41B", "52.29A", None) == "verified"
    assert _compute_naf_status("52.29A", "52.29B", None) == "verified"

    # fret cluster — 52.29B reverse
    assert _compute_naf_status("49.41C", "52.29B", None) == "verified"
    assert _compute_naf_status("52.29B", "49.42Z", None) == "verified"

    # 47.24Z stays singleton — picking retail bread must NOT verify fabrication NAF
    assert _compute_naf_status("10.71C", "47.24Z", None) == "mismatch"
    assert _compute_naf_status("10.71D", "47.24Z", None) == "mismatch"
