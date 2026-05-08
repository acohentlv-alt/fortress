"""Tests for the Association/Fondation prefix handler (Brief 05).

Covers:
  - _normalize_name (discovery.py:376-403) extending _LEGAL with assoc, fondation, fond
  - _ASSOC_PREFIX_RE (discovery.py:988) extending the regex alternation

Brief 05 follows Brief 04's pattern. These tests are scope-isolated to assoc/fond
recognition; Brief 04's test file (test_legal_form_normalization.py) covers the
broader _LEGAL set.
"""
from fortress.discovery import _normalize_name, _ASSOC_PREFIX_RE


# ── _normalize_name: positive cases (new tokens strip in leading position) ─

def test_assoc_leading():
    assert _normalize_name("ASSOC SPORT CULT GARE USSEL") == "sport cult gare ussel"


def test_fondation_leading():
    assert _normalize_name("FONDATION DIACONESSES DE REUILLY") == "diaconesses de reuilly"


def test_fond_leading():
    # 'fond' is rare standalone (223 active SIRENs) but valid — strip it.
    assert _normalize_name("FOND DE DOTATION FOO") == "de dotation foo"


def test_assoc_position_independent():
    # Token-set lookup is position-agnostic.
    assert _normalize_name("Mid ASSOC Position Foo") == "mid position foo"


# ── _normalize_name: negative cases (false-positive guards) ────────────────

def test_fonderie_not_stripped():
    """'fond' is a substring of 'fonderie' but tokenization separates them.
    'fonderie' is its own token; the set lookup 'fonderie' in _LEGAL is False."""
    assert _normalize_name("FONDERIE LEMER") == "fonderie lemer"


def test_fonds_not_stripped():
    """'fonds' (endowment fund, cat-jur 9301) is NOT 'fond'. Different tokens."""
    assert _normalize_name("FONDS DE DOTATION") == "fonds de dotation"


def test_associu_not_stripped():
    """'associu' (Corsican) is NOT 'assoc'. Different tokens."""
    assert _normalize_name("ASSOCIU ANDRE LUCIANI") == "associu andre luciani"


def test_curcuma_still_not_stripped():
    """Brief 04 false-positive guard sanity: 'curcuma' has 'cuma' substring,
    but token-based lookup keeps it intact. Brief 05 doesn't regress this."""
    assert _normalize_name("Restaurant Curcuma") == "restaurant curcuma"


def test_pharmacie_fondaudege_not_stripped():
    """Real Maps entity (MAPS02747): 'fondaudege' has 'fond' substring,
    but is a single concatenated token — set lookup misses."""
    assert _normalize_name("Pharmacie Fondaudege") == "pharmacie fondaudege"


# ── _normalize_name: case-insensitivity (function lowercases first) ────────

def test_lowercase_assoc():
    assert _normalize_name("assoc dupont") == "dupont"


def test_mixed_case_fondation():
    assert _normalize_name("Fondation Diaconesses") == "diaconesses"


# ── _ASSOC_PREFIX_RE: positive matches (storefront fallback fires) ─────────

def test_regex_matches_association():
    assert _ASSOC_PREFIX_RE.match("Association X - Y") is not None


def test_regex_matches_assoc():
    """Brief 05 NEW: 'Assoc' prefix without trailing 'iation' should match."""
    m = _ASSOC_PREFIX_RE.match("Assoc Sport Cult - Branche Locale")
    assert m is not None
    assert m.group(0).strip().lower() == "assoc"


def test_regex_matches_fondation():
    assert _ASSOC_PREFIX_RE.match("Fondation X - Y") is not None


def test_regex_matches_fond():
    """Brief 05 NEW: 'Fond' prefix (rare standalone) should match."""
    m = _ASSOC_PREFIX_RE.match("Fond X - Y")
    assert m is not None
    assert m.group(0).strip().lower() == "fond"


def test_regex_matches_asso():
    assert _ASSOC_PREFIX_RE.match("Asso X - Y") is not None


# ── _ASSOC_PREFIX_RE: negative matches (false-positive guards) ─────────────

def test_regex_rejects_fonderie():
    """'Fonderie X - Y' must NOT trigger the storefront fallback."""
    assert _ASSOC_PREFIX_RE.match("Fonderie Lemer - Site Annexe") is None


def test_regex_rejects_fonds():
    """'Fonds de Dotation' must NOT trigger — endowment fund (9301), not fondation (9300)."""
    assert _ASSOC_PREFIX_RE.match("Fonds de Dotation - Site Foo") is None


def test_regex_rejects_associu():
    """Corsican 'Associu' must NOT trigger."""
    assert _ASSOC_PREFIX_RE.match("Associu Andre Luciani - Branche") is None


def test_regex_rejects_associationd():
    """Concatenated word (no separator after 'association') must NOT trigger."""
    assert _ASSOC_PREFIX_RE.match("AssociationDIdees - Site") is None


# ── Regex ordering: longest-first alternation ──────────────────────────────

def test_regex_captures_longest_alternative():
    """When 'Association X' matches, it should capture 'Association' (the longest
    valid alternative), not 'Assoc' (the shorter one). Without longest-first
    ordering OR \\b word boundary, the regex would capture 'Assoc' and miss the
    full prefix."""
    m = _ASSOC_PREFIX_RE.match("Association des Pelotaris - Branche")
    assert m is not None
    captured = m.group(0).strip().lower()
    assert captured == "association", f"Expected 'association', got {captured!r}"


# ── Tripwire ──────────────────────────────────────────────────────────────

def test_brief05_tokens_in_legal():
    """Pin Brief 05's _LEGAL membership additions. If a future edit removes
    one of these, the corresponding probe assertion fires.

    Probe pattern: f'prefix {token} suffix' -> after _normalize_name, the token
    must be absent from the result tokens.
    """
    brief05_tokens = {"assoc", "fondation", "fond"}
    for token in brief05_tokens:
        probe = f"prefix {token} suffix"
        out = _normalize_name(probe)
        assert token not in out.split(), \
            f"Brief 05 token {token!r} should be stripped by _normalize_name"
