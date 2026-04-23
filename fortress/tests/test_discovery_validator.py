"""Tests for _validate_inpi_step0_hit — A1.1 substring-pair loosen (Apr 22)."""

from fortress.discovery import _validate_inpi_step0_hit, _normalize_name, _INDUSTRY_WORDS


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
