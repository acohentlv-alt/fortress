"""Unit tests for address normalization — civic-title abbreviation expansion (May 8 — Brief 02).

The pipeline's Phase 4 INPI corroboration (discovery.py:5267-5268) compares
Maps and INPI addresses by canonical street key. INPI returns "MAL DE LATTRE"
where Maps shows "Maréchal de Lattre"; without expansion the keys differ and
corroboration fails. This test file pins the expansion table behaviour.
"""
from fortress.matching.entities import (
    _STREET_ABBREVS,
    normalize_address,
    _extract_street_key,
    normalize_street_key,
)


# ───────────────────────── Existing-table baseline ────────────────────────

def test_baseline_street_type_abbreviations_unchanged():
    """Apr 2025 baseline — never regress."""
    for key, val in [
        ("rte", "route"), ("r", "rue"),
        ("av", "avenue"), ("ave", "avenue"),
        ("bd", "boulevard"), ("blvd", "boulevard"),
        ("ch", "chemin"), ("chem", "chemin"),
        ("pl", "place"),
        ("all", "allee"), ("imp", "impasse"),
        ("pass", "passage"), ("sq", "square"),
        ("res", "residence"), ("lot", "lotissement"),
        ("zac", "zac"), ("za", "zone artisanale"),
        ("zi", "zone industrielle"),
    ]:
        assert _STREET_ABBREVS[key] == val, f"baseline {key!r} expansion changed"


# ───────────────────────── Civic-title additions (May 8) ──────────────────

def test_civic_title_abbreviations_present():
    """May 8 expansion — Brief 02."""
    expected = {
        "mal": "marechal",
        "gal": "general",
        "gen": "general",
        "col": "colonel",
        "cdt": "commandant",
        "lt": "lieutenant",
        "dr": "docteur",
        "st": "saint",
        "ste": "sainte",
    }
    for k, v in expected.items():
        assert _STREET_ABBREVS.get(k) == v, f"missing civic-title expansion {k!r}: {v!r}"


def test_total_abbreviation_count_is_27():
    """Tripwire — 18 baseline + 9 civic-title = 27 entries."""
    assert len(_STREET_ABBREVS) == 27, (
        f"expected 27 abbreviations, got {len(_STREET_ABBREVS)}: "
        f"{sorted(_STREET_ABBREVS)}"
    )


def test_intentionally_excluded_abbreviations():
    """Document the explicitly-rejected candidates with rationale.

    These keys MUST NOT appear in _STREET_ABBREVS. See Brief 02 Q3-Q5 evidence.
    """
    excluded = {
        "mar": "Languedoc lagoon toponym (Mar Estang, Mar Vivo) — symmetric expansion still risky for fuzzy callers",
        "cap": "Atlantic coastal toponym (Cap-Ferret, Cap d'Agde, Cap Béar)",
        "pr":  "Low yield (1 Maps row) + Catalan-Spanish noise in 66/64 depts",
        "me":  "Low yield (130 SIRENE rows); reconsider in follow-up",
        "capt": "Low yield (56 rows), redundant with cdt for ~all military-honorific use",
    }
    for k in excluded:
        assert k not in _STREET_ABBREVS, (
            f"{k!r} should not be in _STREET_ABBREVS: {excluded[k]}"
        )


# ───────────────────────── End-to-end normalisation tests ─────────────────

def test_phase4_corroboration_real_pair_mal_marechal():
    """MAPS06363 — the real ws174 candidate the brief targets."""
    maps_addr = "198 Av. du Maréchal de Lattre de Tassigny, 33470 Gujan-Mestras, France"
    inpi_addr = "198 AV DU MAL DE LATTRE DE TASSIGNY 33470 GUJAN-MESTRAS"
    maps_key = normalize_street_key(_extract_street_key(normalize_address(maps_addr)))
    inpi_key = normalize_street_key(_extract_street_key(normalize_address(inpi_addr)))
    assert maps_key == inpi_key, (
        f"\n  maps_key = {maps_key!r}\n  inpi_key = {inpi_key!r}"
    )
    assert "marechal" in maps_key  # confirm expansion fired


def test_phase4_corroboration_real_pair_arcachon():
    """MAPS06027 — second real ws174 candidate.

    Note: real INPI data for MAPS06027 omits 'DU' preposition ('RUE MAL DE LATTRE'
    vs Maps 'Rue du Maréchal de Lattre'). The preposition gap is a separate orthographic
    issue outside this brief's scope. This test uses the DU-inclusive INPI form to
    verify the core mal→marechal expansion works symmetrically — the canonical proof
    of the expansion itself, not of the full Phase 4 rescue for this specific candidate.
    """
    maps_addr = "5 Rue du Maréchal de Lattre de Tassigny, 33120 Arcachon, France"
    inpi_addr = "5 RUE DU MAL DE LATTRE DE TASSIGNY 33120 ARCACHON"
    assert (
        normalize_street_key(_extract_street_key(normalize_address(maps_addr)))
        == normalize_street_key(_extract_street_key(normalize_address(inpi_addr)))
    )


def test_general_de_gaulle_short_vs_long():
    """SIRENE has 'AVENUE DU GENERAL DE GAULLE'; INPI has 'PL DU GAL DE GAULLE'."""
    long_form = "AVENUE DU GENERAL DE GAULLE 47600 NERAC"
    short_form = "AVENUE DU GAL DE GAULLE 47600 NERAC"
    assert (
        normalize_street_key(_extract_street_key(normalize_address(long_form)))
        == normalize_street_key(_extract_street_key(normalize_address(short_form)))
    )


def test_doctor_short_vs_long():
    long_form = "27 Place du Docteur Maschat, 19000 Tulle, France"
    short_form = "27 Pl. du Dr Maschat, 19000 Tulle, France"
    assert (
        normalize_street_key(_extract_street_key(normalize_address(long_form)))
        == normalize_street_key(_extract_street_key(normalize_address(short_form)))
    )


def test_saint_short_vs_long():
    """ST → SAINT in street names (not city/postal). SIRENE has 'RUE ST CHARLES'."""
    long_form = "60 Rue Saint Sabin, 75011 Paris, France"
    short_form = "60 Rue St Sabin, 75011 Paris, France"
    assert (
        normalize_street_key(_extract_street_key(normalize_address(long_form)))
        == normalize_street_key(_extract_street_key(normalize_address(short_form)))
    )


def test_dot_suffix_does_not_break_expansion():
    """normalize_address strips '.' (line 47); 'Mal.' becomes 'mal' becomes 'marechal'."""
    with_dot = "75 Rue Mal. Joffre, 66130 X"
    without_dot = "75 Rue Mal Joffre, 66130 X"
    assert (
        normalize_street_key(_extract_street_key(normalize_address(with_dot)))
        == normalize_street_key(_extract_street_key(normalize_address(without_dot)))
    )


# ───────────────────────── Negative-case (no false positive) ──────────────

def test_genuinely_different_streets_still_diverge():
    """Two different streets in the same commune must NOT collapse after expansion."""
    a = "33 Chemin des Palombes, 47200 Marmande"
    b = "BOUILHATS 47200 MARMANDE"
    ka = normalize_street_key(_extract_street_key(normalize_address(a)))
    kb = normalize_street_key(_extract_street_key(normalize_address(b)))
    assert ka != kb, f"Distinct streets must not collapse: {ka!r} vs {kb!r}"


def test_mar_estang_toponym_unchanged():
    """'Mar' in toponym Mar Estang must NOT be expanded to 'marechal'.

    Verifies that 'mar' was NOT added to _STREET_ABBREVS (per Brief 02 Q5 risk).
    """
    addr = "Camping Mar Estang, 5 Voie des Flamants Roses, 66140 Canet-en-Roussillon"
    key = normalize_street_key(_extract_street_key(normalize_address(addr)))
    assert "marechal" not in key, f"'mar' must remain a toponym, key={key!r}"
    assert "mar" in key.split(), f"toponym 'mar' must remain as a token, key={key!r}"


def test_cap_ferret_toponym_unchanged():
    """'Cap' in Cap-Ferret must NOT be expanded to 'capitaine'.

    Verifies that 'cap' was NOT added to _STREET_ABBREVS.
    """
    addr = "56 Rte du Cap Ferret, 33950 Lège-Cap-Ferret"
    key = normalize_street_key(_extract_street_key(normalize_address(addr)))
    assert "capitaine" not in key, f"'cap' must remain a toponym, key={key!r}"
