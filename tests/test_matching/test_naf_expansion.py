"""Unit tests for Option C — per-sector NAF expansion (Apr 18) + multi-NAF picker (Apr 19)."""
from fortress.discovery import _compute_naf_status
from fortress.config.naf_sector_expansion import (
    SECTOR_EXPANSIONS,
    same_sector_group,
    all_same_sector_group,
)


def test_strict_prefix_still_verified():
    # Picker = 10.71C, matched SIREN = 10.71C → strict prefix match
    assert _compute_naf_status("10.71C", ["10.71C"], None) == "verified"
    # Longer prefix still matches (defensive — unlikely in real data)
    assert _compute_naf_status("10.71CAB", ["10.71C"], None) == "verified"


def test_sibling_in_expansion_verified():
    # 10.71C picker, SIREN = 10.71D pâtisserie → expansion path
    assert "10.71D" in SECTOR_EXPANSIONS["10.71C"]
    assert _compute_naf_status("10.71D", ["10.71C"], None) == "verified"
    # 10.71C picker, SIREN = 47.24Z retail bakery → expansion path (Alan-approved cross-section)
    assert "47.24Z" in SECTOR_EXPANSIONS["10.71C"]
    assert _compute_naf_status("47.24Z", ["10.71C"], None) == "verified"
    # 49.41A picker (fret interurbain), SIREN = 52.29B affrètement → expansion path
    assert "52.29B" in SECTOR_EXPANSIONS["49.41A"]
    assert _compute_naf_status("52.29B", ["49.41A"], None) == "verified"


def test_cross_sector_mismatch_critical_guardrail():
    """Option B regression guard. These are the exclusions we MUST protect."""
    # Boulangerie picker must NOT auto-confirm restaurant or supermarket
    assert _compute_naf_status("56.10C", ["10.71C"], None) == "mismatch"
    assert _compute_naf_status("47.11D", ["10.71C"], None) == "mismatch"
    assert _compute_naf_status("47.11F", ["10.71C"], None) == "mismatch"
    # Épicerie must NOT auto-confirm supermarché/hyper
    assert _compute_naf_status("47.11D", ["47.11B"], None) == "mismatch"
    assert _compute_naf_status("47.11F", ["47.11B"], None) == "mismatch"
    # Plomberie must NOT auto-confirm électricité
    assert _compute_naf_status("43.21A", ["43.22A"], None) == "mismatch"
    # Nettoyage courant must NOT auto-confirm paysagisme (espaces verts)
    assert _compute_naf_status("81.30Z", ["81.21Z"], None) == "mismatch"
    # Restauration rapide must NOT auto-confirm resto traditionnel — NOW REVERSED: they are cliqued
    # This test is updated: 56.10A IS now in 56.10C's expansion (restauration clique Apr 19)
    assert _compute_naf_status("56.10A", ["56.10C"], None) == "verified"


def test_section_whitelist_still_wins():
    # Picker = section letter "I", whitelist = ["55","56"], matched = 55.30Z → verified
    assert _compute_naf_status("55.30Z", ["I"], ["55", "56"]) == "verified"
    # Same whitelist, matched NAF outside whitelist → mismatch
    assert _compute_naf_status("10.71C", ["I"], ["55", "56"]) == "mismatch"


def test_unmapped_picker_falls_through():
    # Picker 99.99Z is NOT in SECTOR_EXPANSIONS. Strict prefix behavior only.
    assert "99.99Z" not in SECTOR_EXPANSIONS
    # Self-match still verified via strict prefix
    assert _compute_naf_status("99.99Z", ["99.99Z"], None) == "verified"
    # Non-matching, non-expanded → mismatch
    assert _compute_naf_status("10.71C", ["99.99Z"], None) == "mismatch"


def test_none_matched_naf_is_mismatch():
    # If SIRENE didn't provide a NAF, status must be mismatch (safe)
    assert _compute_naf_status(None, ["10.71C"], None) == "mismatch"


def test_seed_map_key_count():
    """Regression: accidental edits to the map that drop/add entries should trip.

    History:
      - 45 original
      - +2 (Apr 19) for hôtellerie clique — 55.20Z and 55.90Z added as new keys
        (55.10Z and 55.30Z were already keys); restauration 56.10A/56.10C/56.21Z
        already keys, no net change.
      - +4 (Apr 26) for horticulture clique — 01.30Z, 01.19Z, 46.22Z, 47.76Z
        all new keys (Pépinières de Vair Sur Loire regression).
    """
    assert len(SECTOR_EXPANSIONS) == 51


def test_seed_map_all_values_are_frozenset():
    assert all(isinstance(v, frozenset) for v in SECTOR_EXPANSIONS.values())
    assert all(isinstance(k, str) for k in SECTOR_EXPANSIONS.keys())


def test_asymmetric_reverses_closed():
    """Each of the 8 new reverse-keys must verify against siblings in their cluster."""

    # 43.21B reverse (électricité voie publique ↔ bâtiment)
    assert _compute_naf_status("43.21A", ["43.21B"], None) == "verified"
    assert _compute_naf_status("43.21B", ["43.21A"], None) == "verified"

    # 43.32C reverse (menuiserie agencement ↔ pose)
    assert _compute_naf_status("43.32A", ["43.32C"], None) == "verified"
    assert _compute_naf_status("43.32C", ["43.32A"], None) == "verified"

    # 10.13A reverse (transformation viande ↔ charcuterie)
    assert _compute_naf_status("10.13B", ["10.13A"], None) == "verified"
    assert _compute_naf_status("10.13A", ["10.13B"], None) == "verified"

    # 10.71D reverse (cuisson pain ↔ boulangerie traditionnelle)
    # Regression: ANTONE Artisan Boulanger (boulangerie 33000, Apr 18)
    # Picker 10.71D, SIRENE 10.71C → must now be verified.
    assert _compute_naf_status("10.71C", ["10.71D"], None) == "verified"
    assert _compute_naf_status("10.71D", ["10.71C"], None) == "verified"

    # fret cluster — 49.41C reverse
    assert _compute_naf_status("49.41A", ["49.41C"], None) == "verified"
    assert _compute_naf_status("49.41C", ["49.41A"], None) == "verified"

    # fret cluster — 49.42Z reverse
    assert _compute_naf_status("49.41A", ["49.42Z"], None) == "verified"
    assert _compute_naf_status("49.42Z", ["49.41B"], None) == "verified"

    # fret cluster — 52.29A reverse
    assert _compute_naf_status("49.41B", ["52.29A"], None) == "verified"
    assert _compute_naf_status("52.29A", ["52.29B"], None) == "verified"

    # fret cluster — 52.29B reverse
    assert _compute_naf_status("49.41C", ["52.29B"], None) == "verified"
    assert _compute_naf_status("52.29B", ["49.42Z"], None) == "verified"

    # 47.24Z stays singleton — picking retail bread must NOT verify fabrication NAF
    assert _compute_naf_status("10.71C", ["47.24Z"], None) == "mismatch"
    assert _compute_naf_status("10.71D", ["47.24Z"], None) == "mismatch"


# ── Multi-picker semantics ────────────────────────────────────────

def test_multi_picker_any_match_verifies():
    # Picker list [10.71C, 10.71D], matched 47.24Z → verified via 10.71C expansion
    assert _compute_naf_status("47.24Z", ["10.71C", "10.71D"], None) == "verified"
    # Picker list [10.71C, 10.71D], matched 56.10A → mismatch (not in either expansion)
    assert _compute_naf_status("56.10A", ["10.71C", "10.71D"], None) == "mismatch"


def test_multi_picker_empty_list_is_mismatch():
    # Defensive: empty picker should never auto-verify anything
    assert _compute_naf_status("10.71C", [], None) == "mismatch"


def test_compute_naf_status_does_not_enforce_same_sector():
    # The status function itself does NOT enforce same-sector-group constraint.
    # That check belongs to the backend validator in batch.py. This test documents
    # the intentional separation: feeding _compute_naf_status a cross-sector pair
    # like ['10.71D', '56.10A'] still verifies a 47.24Z match (via 10.71D's expansion).
    # This is by design — keeps the function single-purpose and unit-testable.
    assert _compute_naf_status("47.24Z", ["10.71D", "56.10A"], None) == "verified"


def test_same_sector_group_identity():
    assert same_sector_group("10.71C", "10.71C") is True
    assert same_sector_group("99.99Z", "99.99Z") is True  # Self-match even for non-keys


def test_same_sector_group_expansion_pair():
    # Both ways, even for asymmetric 47.24Z (only in 10.71C and 10.71D's expansion, not a key)
    assert same_sector_group("10.71C", "10.71D") is True
    assert same_sector_group("10.71D", "10.71C") is True
    assert same_sector_group("10.71C", "47.24Z") is True
    assert same_sector_group("47.24Z", "10.71C") is True


def test_same_sector_group_cross_sector_rejected():
    # Bakery vs restaurant — the regression this whole task guards against
    assert same_sector_group("10.71C", "56.10A") is False
    # Bakery vs supermarket
    assert same_sector_group("10.71C", "47.11F") is False
    # Électricité vs plomberie
    assert same_sector_group("43.21A", "43.22A") is False


def test_same_sector_group_isolated_codes():
    # Two codes neither of which is a key (and don't appear in each other's expansion)
    assert same_sector_group("99.99Z", "99.98A") is False
    # 47.24Z (non-key) alone with another non-key
    assert same_sector_group("47.24Z", "99.99Z") is False


def test_all_same_sector_group_empty_and_single():
    assert all_same_sector_group([]) is True
    assert all_same_sector_group(["10.71C"]) is True


def test_all_same_sector_group_fret_cluster():
    # Full fret cluster — all 6 codes should be co-group
    fret = ["49.41A", "49.41B", "49.41C", "49.42Z", "52.29A", "52.29B"]
    assert all_same_sector_group(fret) is True


def test_all_same_sector_group_mixed_rejects():
    # Mixing bakery + restaurant fails
    assert all_same_sector_group(["10.71C", "56.10A"]) is False
    # One odd-one-out in an otherwise valid list
    assert all_same_sector_group(["10.71C", "10.71D", "47.11F"]) is False


# ── New cliques added in 1b: hôtellerie + restauration ────────────

def test_hotellerie_clique_full():
    hot = ["55.10Z", "55.20Z", "55.30Z", "55.90Z"]
    assert all_same_sector_group(hot) is True
    # Spot-check pairs
    assert same_sector_group("55.10Z", "55.30Z") is True  # hôtel ↔ camping
    assert same_sector_group("55.20Z", "55.90Z") is True  # courte durée ↔ autres


def test_restauration_clique_full():
    resto = ["56.10A", "56.10C", "56.21Z"]
    assert all_same_sector_group(resto) is True
    assert same_sector_group("56.10A", "56.21Z") is True  # traditionnel ↔ traiteur
    assert same_sector_group("56.10C", "56.21Z") is True  # rapide ↔ traiteur
    # Cross-clique still rejected: hôtel vs restaurant
    assert same_sector_group("55.10Z", "56.10A") is False


def test_horticulture_clique_full():
    """Apr 26: Pépinières de Vair Sur Loire (44150) regression — picker 01.30Z
    + SIRENE 46.22Z held pending despite enseigne+address agree and 50m geo
    distance, because the matcher's NAF gate didn't know production ↔ wholesale
    of plants is the same filière. Clique fixes that.
    """
    horti = ["01.30Z", "01.19Z", "46.22Z", "47.76Z"]
    assert all_same_sector_group(horti) is True
    # Spot-check the regression case directly
    assert same_sector_group("01.30Z", "46.22Z") is True  # production ↔ wholesale (the regression)
    assert same_sector_group("46.22Z", "47.76Z") is True  # wholesale ↔ retail
    assert same_sector_group("01.19Z", "01.30Z") is True  # autres cultures ↔ pépinière
    # Documented exclusions stay rejected
    assert same_sector_group("01.30Z", "01.13Z") is False  # vs maraîchage (légumes)
    assert same_sector_group("01.30Z", "81.30Z") is False  # vs paysagisme (service)
    assert same_sector_group("46.22Z", "46.21Z") is False  # vs céréales/aliments bétail


def test_platrerie_remains_singleton():
    # Apr 19 decision: keep 43.31Z isolated. Document via test so future
    # accidental clique additions break the build.
    assert SECTOR_EXPANSIONS["43.31Z"] == frozenset({"43.31Z"})
    assert same_sector_group("43.31Z", "43.34Z") is False  # vs peinture
    assert same_sector_group("43.31Z", "43.33Z") is False  # vs carrelage


# ── Clique-property guard ─────────────────────────────────────────

def test_sector_expansions_are_cliques():
    """Every key K in SECTOR_EXPANSIONS must form a mutual clique with the codes
    in its expansion: for every C in SECTOR_EXPANSIONS[K], either C is itself a
    key whose expansion contains K, OR (C, K) is a documented one-way inclusion.

    Currently only one one-way inclusion exists: 47.24Z appears in 10.71C and
    10.71D expansions but is NOT a key (per Alan's "singleton one-way" decision,
    file line 107).

    If a future edit breaks the clique property without adding to the allow-list
    below, this test fails.
    """
    ONE_WAY_INCLUSIONS = {"47.24Z"}  # codes intentionally non-key, included from elsewhere
    for key, expansion in SECTOR_EXPANSIONS.items():
        # Self-membership invariant
        assert key in expansion, f"{key} missing from its own expansion"
        for sibling in expansion:
            if sibling == key:
                continue
            if sibling in ONE_WAY_INCLUSIONS:
                continue  # Documented one-way inclusion, exempt
            sibling_expansion = SECTOR_EXPANSIONS.get(sibling)
            assert sibling_expansion is not None, (
                f"{sibling} appears in {key}'s expansion but is not itself a key, "
                f"and is not in ONE_WAY_INCLUSIONS allow-list. "
                f"Either add {sibling} as a key or document it as a one-way inclusion."
            )
            assert key in sibling_expansion, (
                f"Clique broken: {sibling} appears in {key}'s expansion, "
                f"but {key} does NOT appear in {sibling}'s expansion. "
                f"Make this a mutual relationship or document as one-way."
            )
