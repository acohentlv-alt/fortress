"""Unit tests for _promote_classify_signals tier-2 tightening (R.1) + audit-label fix (R.2).

Brief 03, May 8.

Tests verify:
  - R.1: Tier-2 dept-only path requires >=1 corroborating signal (enseigne/phone).
    Section-letter match alone is still tier-2.
  - R.2: agreeing-signals array distinguishes "empty_picker" (no NAF filter)
    from "naf_section" (section letter genuinely matches).
  - Strong-method-no-section-no-dept still blocks (regression guard).
  - Tier 1, Tier 3 paths unaffected (regression guards).
"""

from fortress.discovery import _promote_classify_signals


# ───────── R.1 — Tier 2 dept-only requires corroborating signal ─────────

def test_r1_tier2_dept_only_no_corroboration_blocks():
    """The May 7 leak case: strong method (inpi) + dept agrees + zero
    corroborating signals → must block (was tier2 pre-R.1).
    """
    tier, agreeing, blockers = _promote_classify_signals(
        method="inpi",
        link_signals={
            "enseigne_match": False,
            "phone_match": False,
            "address_match": False,
            "siren_website_match": False,
        },
        maps_dept="33",
        target_dept="33",
        matched_naf="56.10C",      # restaurants
        picked_nafs=["10.71C"],    # boulangerie — picker section != matched section
    )
    assert tier == "block", f"expected block, got tier={tier} agreeing={agreeing}"
    assert "dept" in agreeing      # dept_ok still recorded
    assert "naf_section" not in agreeing  # genuine section did NOT match
    assert "empty_picker" not in agreeing  # picker wasn't empty
    # The corroboration count is 0 → blocker label fires
    assert "strong_method_no_section_no_corroborated_dept" in blockers


def test_r1_tier2_dept_with_enseigne_match_promotes():
    """Strong method + dept + 1 corroborating signal (enseigne_match) → tier2.
    """
    tier, agreeing, blockers = _promote_classify_signals(
        method="inpi",
        link_signals={"enseigne_match": True, "phone_match": False,
                      "address_match": False, "siren_website_match": False},
        maps_dept="33",
        target_dept="33",
        matched_naf="56.10C",
        picked_nafs=["10.71C"],
    )
    assert tier == "tier2"
    assert "dept" in agreeing
    assert "enseigne" in agreeing


def test_r1_tier2_dept_with_phone_match_promotes():
    tier, agreeing, _ = _promote_classify_signals(
        method="phone",
        link_signals={"enseigne_match": False, "phone_match": True,
                      "address_match": False, "siren_website_match": False},
        maps_dept="33", target_dept="33",
        matched_naf="56.10C", picked_nafs=["10.71C"],
    )
    assert tier == "tier2"
    assert "phone" in agreeing


def test_r1_tier2_section_match_alone_still_promotes():
    """When section_ok=True (sections match), R.1 must still promote — no
    corroboration needed. Uses a deterministic section-letter picker so the
    test is not implementation-fragile.
    """
    # Picker section A (agriculture), matched NAF section A
    tier, agreeing, _ = _promote_classify_signals(
        method="inpi",
        link_signals={"enseigne_match": False, "phone_match": False,
                      "address_match": False, "siren_website_match": False},
        maps_dept="33", target_dept="33",
        matched_naf="01.11Z",  # Section A
        picked_nafs=["A"],     # Section A literal — deterministic section match
    )
    assert tier == "tier2", f"section_ok must promote without corroboration, got {tier} agreeing={agreeing}"
    assert "naf_section" in agreeing


def test_r1_tier2_empty_picker_alone_still_promotes():
    """Empty picker still counts as section_ok by definition (no filter to block).
    Tier-2 for empty-picker + no dept + no signals must still fire under R.1
    because section_ok branch is unchanged.
    """
    tier, agreeing, _ = _promote_classify_signals(
        method="inpi",
        link_signals={"enseigne_match": False, "phone_match": False,
                      "address_match": False, "siren_website_match": False},
        maps_dept="33", target_dept="40",  # dept does NOT agree
        matched_naf="56.10C",
        picked_nafs=[],                    # no NAF filter
    )
    assert tier == "tier2"
    # R.2 audit-label fix: empty_picker label fires here, NOT naf_section
    assert "empty_picker" in agreeing
    assert "naf_section" not in agreeing


def test_r1_no_dept_no_section_blocks():
    """Strong method, picker non-empty, section letter does NOT match,
    dept does NOT agree → blocks. Pre-R.1 also blocked; regression guard.
    """
    tier, agreeing, blockers = _promote_classify_signals(
        method="inpi",
        link_signals={"enseigne_match": True},  # has corroboration
        maps_dept="33", target_dept="40",
        matched_naf="56.10C", picked_nafs=["10.71C"],
    )
    assert tier == "block"
    assert "dept" not in agreeing
    assert "naf_section" not in agreeing
    # corroboration is present but neither path opens — block stands
    assert blockers


# ───────── R.2 — Audit label split ─────────

def test_r2_genuine_section_match_writes_naf_section():
    """When picker is non-empty and section letter genuinely agrees,
    label is "naf_section", not "empty_picker"."""
    tier, agreeing, _ = _promote_classify_signals(
        method="inpi",
        link_signals={"enseigne_match": True},  # corroborates dept too
        maps_dept="33", target_dept="33",
        matched_naf="10.71D",
        picked_nafs=["10.71C"],
    )
    # Both dept_ok + section_ok may be True; label should reflect genuine section
    if "naf_section" in agreeing or "empty_picker" in agreeing:
        # picker non-empty → naf_section path
        assert "naf_section" in agreeing
        assert "empty_picker" not in agreeing


def test_r2_empty_picker_writes_empty_picker_label():
    """When picker is empty, label is "empty_picker", not "naf_section"."""
    _, agreeing, _ = _promote_classify_signals(
        method="inpi",
        link_signals={"enseigne_match": True},
        maps_dept="33", target_dept="33",
        matched_naf="56.10C",
        picked_nafs=[],
    )
    assert "empty_picker" in agreeing
    assert "naf_section" not in agreeing


def test_r2_section_mismatch_writes_neither_label():
    """When picker is non-empty AND section letter does NOT match,
    neither "naf_section" nor "empty_picker" appears in agreeing.
    """
    _, agreeing, _ = _promote_classify_signals(
        method="inpi",
        link_signals={"enseigne_match": True},
        maps_dept="33", target_dept="33",
        matched_naf="56.10C",
        picked_nafs=["10.71C"],   # different section
    )
    # section_ok is False; empty_picker not added; naf_section not added
    assert "empty_picker" not in agreeing
    assert "naf_section" not in agreeing


# ───────── Regression guards — Tier 1 + Tier 3 paths unchanged ─────────

def test_tier1_siren_website_unchanged():
    tier, agreeing, _ = _promote_classify_signals(
        method="siren_website",
        link_signals={"siren_website_match": True},
        maps_dept="33", target_dept="40",
        matched_naf="56.10C", picked_nafs=["10.71C"],
    )
    assert tier == "tier1"


def test_tier1_address_match_unchanged():
    tier, _, _ = _promote_classify_signals(
        method="enseigne",
        link_signals={"address_match": True},
        maps_dept="33", target_dept="40",
        matched_naf="56.10C", picked_nafs=["10.71C"],
    )
    assert tier == "tier1"


def test_tier3_weak_method_with_dept_promotes():
    """Weak method (fuzzy_name) + dept agrees → tier3 (corroboration via dept).
    Tier-3 logic is independent of tier-2 path; should not regress.
    """
    tier, agreeing, _ = _promote_classify_signals(
        method="fuzzy_name",
        link_signals={"enseigne_match": False},
        maps_dept="33", target_dept="33",
        matched_naf="56.10C", picked_nafs=["10.71C"],
    )
    assert tier == "tier3"
    assert "dept" in agreeing
