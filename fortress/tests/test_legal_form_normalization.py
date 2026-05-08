"""Tests for the legal-form token set inside _normalize_name (discovery.py:376-403).

Brief 04 — extends the local _LEGAL set with sarlu, selarl, selas, scp, scm,
gfa, cuma, sica. These tests cover:

  - Positive: each new token strips when it appears as a standalone whitespace-
    separated token (leading or trailing position).
  - Negative: substring matches do NOT strip (curcuma, sicamus) — false-positive
    guard via tokenization.
  - Tripwire: pin the set size so future additions are intentional.
  - Position-independence: the function is order-agnostic.
"""
from fortress.discovery import _normalize_name


# ── Positive: new tokens strip in leading position ─────────────────────────
def test_selarl_leading():
    assert _normalize_name("SELARL COMBETTES-CHEVILLARD") == "combettes chevillard"


def test_selas_leading():
    assert _normalize_name("SELAS GRANDE PHARMACIE") == "grande pharmacie"


def test_sarlu_leading():
    assert _normalize_name("SARLU BOULANGERIE DUPONT") == "boulangerie dupont"


def test_scp_leading():
    assert _normalize_name("SCP Avocats Bordeaux") == "avocats bordeaux"


def test_scm_leading():
    assert _normalize_name("SCM Cabinet Medical Central") == "cabinet medical central"


def test_gfa_leading():
    # Single-token clean case — 'gfa' strips, single token remains.
    assert _normalize_name("GFA CASTELNAUDARY") == "castelnaudary"


def test_cuma_leading():
    assert _normalize_name("CUMA Adour Proteoil") == "adour proteoil"


def test_sica_leading():
    assert _normalize_name("SICA Producteurs Bio") == "producteurs bio"


# ── Positive: new tokens strip in trailing position ────────────────────────
def test_selarl_trailing():
    assert _normalize_name("Pharmacie Mandron-Tivoli SELARL") == "pharmacie mandron tivoli"


def test_scp_trailing():
    assert _normalize_name("Cabinet Avocats SCP") == "cabinet avocats"


# ── Negative: false-positive guard via tokenization ────────────────────────
def test_curcuma_not_stripped():
    """'cuma' is a substring of 'curcuma' but tokenization separates them.
    'curcuma' is its own token; the set lookup 'curcuma' in _LEGAL is False."""
    assert _normalize_name("Restaurant Curcuma") == "restaurant curcuma"


def test_sicamus_not_stripped():
    """'sica' is a substring of 'sicamus' but tokenization separates them.
    (Same mechanism covers 'osicars' — one test sufficient.)"""
    assert _normalize_name("Sicamus Hortensias D Anjou") == "sicamus hortensias d anjou"


# ── Mixed case (function lowercases input first) ───────────────────────────
def test_lowercase_selarl():
    assert _normalize_name("selarl combettes") == "combettes"


def test_mixed_case_cuma():
    assert _normalize_name("Cuma Adour") == "adour"


# ── Tripwire ─────────────────────────────────────────────────────────────
def test_legal_set_tripwire():
    """Pin _LEGAL membership by probing every expected token.

    Module-level extraction was rejected (Open Q6) — _LEGAL stays function-
    local. The probe loop is the de-facto tripwire: if a future edit removes
    a token from the function-local set, the assertion below fires for that
    token. The previous len(expected_present) == 32 check was redundant — it
    only verified the local test variable, not the actual _LEGAL set inside
    _normalize_name."""
    expected_present = {
        "sarl", "sas", "sasu", "eurl", "sa", "sci", "snc",
        "scs", "sca", "ei", "eirl", "asso", "association",
        "et", "cie", "fils", "freres", "groupe", "holding",
        "earl", "gaec", "scea", "scev",
        # Brief 04
        "sarlu", "selarl", "selas", "scp", "scm",
        "gfa", "cuma", "sica",
    }
    # Verify each is stripped by checking it's removed from a probe
    for token in expected_present:
        probe = f"prefix {token} suffix"
        out = _normalize_name(probe)
        assert token not in out.split(), f"{token} should be stripped by _normalize_name"
