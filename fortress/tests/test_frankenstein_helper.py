"""Tests for _is_frankenstein_parent_siren helper (Agent C Phase 1 signature, Apr 22)."""

from fortress.discovery import _is_frankenstein_parent_siren


def test_choufimafi_frankenstein():
    """Canonical case: OSG TWO (shell denom) / CHOUFIMAFI (real enseigne)."""
    assert _is_frankenstein_parent_siren("Choufimafi", "OSG TWO", "CHOUFIMAFI") is True


def test_noucademie_frankenstein():
    """Canonical case: MERCIERE YG (shell) / NOUCADEMIE (enseigne)."""
    assert _is_frankenstein_parent_siren("Noucadémie", "MERCIERE YG", "NOUCADEMIE") is True


def test_david_louder_frankenstein():
    """Canonical case: DL LA CIOTAT (shell) / DAVID LOUDER (enseigne)."""
    assert _is_frankenstein_parent_siren(
        "DAVID LOUDER Marseille Davso", "DL LA CIOTAT", "DAVID LOUDER"
    ) is True


def test_fournil_jarret_frankenstein():
    """Canonical case: SAS LA BLANCARDE (shell) / FOURNIL JARRET (enseigne)."""
    assert _is_frankenstein_parent_siren(
        "Le Fournil du Jarret", "SAS LA BLANCARDE", "FOURNIL JARRET"
    ) is True


def test_legitimate_single_site_not_frankenstein():
    """Normal company (denom == enseigne == maps_name) must NOT trigger."""
    assert _is_frankenstein_parent_siren(
        "BOULANGERIE DU DOME", "BOULANGERIE DU DOME", "BOULANGERIE DU DOME"
    ) is False


def test_null_enseigne_returns_false():
    """Documented residual risk — HAXO / Maison César fall here (NULL enseigne)."""
    assert _is_frankenstein_parent_siren("Les coiffeurs HAXO", "SAS HAXO 21", None) is False


def test_empty_denom_with_matching_enseigne():
    """Edge: denom null/empty but enseigne matches → denom overlap is 0 (< 0.3), enseigne 1.0 (>= 0.5) → True."""
    assert _is_frankenstein_parent_siren("PICMIE", None, "PICMIE") is True


def test_empty_enseigne_string_returns_false():
    """Empty string enseigne (no meaningful tokens) must return False."""
    assert _is_frankenstein_parent_siren("Some Shop", "SOME SHOP SAS", "") is False


def test_high_denom_overlap_not_frankenstein():
    """Denom overlaps well with maps_name — not a Frankenstein case."""
    assert _is_frankenstein_parent_siren(
        "Fournil de Saint-Mitre", "LE FOURNIL DE SAINT MITRE", "LE FOURNIL DE SAINT MITRE"
    ) is False


def test_empty_maps_name_returns_false():
    """Empty maps_name guard — must return False safely."""
    assert _is_frankenstein_parent_siren("", "OSG TWO", "CHOUFIMAFI") is False
