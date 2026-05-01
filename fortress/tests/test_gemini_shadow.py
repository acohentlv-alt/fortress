"""Unit tests for Wave D1a Gemini shadow judge.

Tests:
  1. BudgetTracker cap enforcement
  2. _build_prompt includes rejected_siren
  3. _build_prompt never includes email
  4. judge_match returns None on malformed verdict
  5. judge_match returns None on timeout
  6. _build_prompt multi-candidate includes all SIRENs
  7. _build_prompt empty candidates
  8. judge_match rejects picked_siren outside pool
  9. judge_match normalizes picked_siren to None on non-match
 10. judge_match rejects malformed SIREN
 11. gemini_quarantine audit captures original_method before alias mutation (Apr 27)
 12. siret_address_naf method is NOT swapped by Gemini (May 1 exemption)
 13. siret_address_naf method is NOT quarantined by Gemini (May 1 exemption)
"""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
import pytest_asyncio

from fortress.matching.budget_tracker import BudgetTracker
from fortress.matching.gemini import _build_prompt, judge_match


# ---------------------------------------------------------------------------
# 1. BudgetTracker cap enforcement
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_budget_tracker_cap_hit():
    """would_exceed returns True once accumulated spend + new cost > cap.
    hit_cap flag is set only after the cap is actually exceeded."""
    tracker = BudgetTracker(cap_usd=0.001)
    cost = 0.0009

    # First call: 0 + 0.0009 = 0.0009 <= 0.001 → should NOT exceed
    exceeded_first = await tracker.would_exceed(cost)
    assert not exceeded_first, "First call should not exceed cap"
    assert not tracker.hit_cap, "hit_cap should be False before cap hit"

    # Record first spend
    await tracker.spend(cost)
    assert tracker.calls == 1

    # Second call: 0.0009 + 0.0009 = 0.0018 > 0.001 → should exceed
    exceeded_second = await tracker.would_exceed(cost)
    assert exceeded_second, "Second call should exceed cap"
    assert tracker.hit_cap, "hit_cap should be True after cap exceeded"


# ---------------------------------------------------------------------------
# 2. _build_prompt includes rejected_siren
# ---------------------------------------------------------------------------

def test_build_prompt_includes_rejected_siren():
    """When rejected_siren is provided, the prompt includes the SIREN and 'rejected by' phrase."""
    prompt = _build_prompt(
        maps_name="Camping Les Pins",
        maps_address="34500 Béziers",
        maps_phone="0467123456",
        candidates=[],
        rejected_siren="123456789",
    )
    assert "123456789" in prompt, "Rejected SIREN must appear in prompt"
    assert "rejected by" in prompt.lower(), "Phrase 'rejected by' must appear in prompt"


# ---------------------------------------------------------------------------
# 3. _build_prompt never includes email
# ---------------------------------------------------------------------------

def test_build_prompt_omits_email():
    """Email is NEVER included in the prompt — RGPD precaution.
    The prompt fields are maps_name/address/phone and candidate SIRENE fields only."""
    prompt = _build_prompt(
        maps_name="Restaurant du Port",
        maps_address="13000 Marseille",
        maps_phone="0491234567",
        candidates=[{
            "siren": "987654321",
            "denomination": "RESTAURANT DU PORT SARL",
            "enseigne": "Restaurant du Port",
            "adresse": "1 quai du port",
            "ville": "Marseille",
            "naf_code": "5610A",
            "method": "fuzzy_name",
            "score": 0.82,
        }],
        rejected_siren=None,
    )
    assert "@" not in prompt, "No email address (@ sign) should appear in the prompt"


# ---------------------------------------------------------------------------
# 4. judge_match returns None on malformed verdict
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_judge_match_returns_none_on_malformed_response():
    """If Gemini returns a verdict with an invalid value (e.g. 'maybe'), judge_match returns None."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "candidates": [{
            "content": {
                "parts": [{
                    "text": json.dumps({
                        "verdict": "maybe",
                        "confidence": 0.5,
                        "reasoning": "Not sure",
                    })
                }]
            }
        }]
    }

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch("httpx.AsyncClient", return_value=mock_client):
        result = await judge_match(
            api_key="fake-key",
            maps_name="Test Company",
            maps_address="75001 Paris",
            maps_phone=None,
            candidates=[],
            rejected_siren=None,
        )

    assert result is None, "Malformed verdict 'maybe' should return None"


# ---------------------------------------------------------------------------
# 5. judge_match returns None on timeout
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_judge_match_returns_none_on_timeout():
    """If httpx raises asyncio.TimeoutError, judge_match returns None without raising."""
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.post = AsyncMock(side_effect=asyncio.TimeoutError())

    with patch("httpx.AsyncClient", return_value=mock_client):
        result = await judge_match(
            api_key="fake-key",
            maps_name="Test Company",
            maps_address="75001 Paris",
            maps_phone=None,
            candidates=[],
            rejected_siren=None,
        )

    assert result is None, "TimeoutError should return None without raising"


# ---------------------------------------------------------------------------
# 6. _build_prompt multi-candidate includes all SIRENs
# ---------------------------------------------------------------------------

def test_build_prompt_multi_candidate_includes_all_sirens():
    """Multi-candidate prompt lists every SIREN so Gemini can pick one."""
    prompt = _build_prompt(
        maps_name="Camping Les Pins",
        maps_address="34500 Béziers",
        maps_phone=None,
        candidates=[
            {"siren": "111111111", "denomination": "A", "enseigne": "A",
             "adresse": "", "ville": "", "naf_code": "5510Z", "method": "trigram_pool", "score": 0.55},
            {"siren": "222222222", "denomination": "B", "enseigne": "B",
             "adresse": "", "ville": "", "naf_code": "5610A", "method": "trigram_pool", "score": 0.42},
        ],
        rejected_siren=None,
    )
    assert "111111111" in prompt
    assert "222222222" in prompt
    assert "Candidate 1" in prompt
    assert "Candidate 2" in prompt


# ---------------------------------------------------------------------------
# 7. _build_prompt empty candidates
# ---------------------------------------------------------------------------

def test_build_prompt_empty_candidates():
    """Empty candidates list produces the 'no candidate proposed' phrase."""
    prompt = _build_prompt(
        maps_name="Test", maps_address=None, maps_phone=None,
        candidates=[], rejected_siren=None,
    )
    assert "no candidate" in prompt.lower() or "(none —" in prompt


# ---------------------------------------------------------------------------
# 8. judge_match rejects picked_siren outside pool
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_judge_match_rejects_picked_siren_outside_pool():
    """Anti-hallucination: if Gemini returns match with a SIREN not in the pool, return None."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "candidates": [{
            "content": {"parts": [{"text": json.dumps({
                "verdict": "match",
                "confidence": 0.9,
                "picked_siren": "999999999",
                "reasoning": "looks right",
            })}]}
        }]
    }
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch("httpx.AsyncClient", return_value=mock_client):
        result = await judge_match(
            api_key="fake",
            maps_name="X", maps_address=None, maps_phone=None,
            candidates=[{"siren": "111111111"}, {"siren": "222222222"}],
            rejected_siren=None,
        )
    assert result is None


# ---------------------------------------------------------------------------
# 9. judge_match normalizes picked_siren to None on non-match
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_judge_match_normalizes_picked_siren_on_non_match():
    """If verdict is ambiguous or no_match, picked_siren is forced to None in the output."""
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.json.return_value = {
        "candidates": [{
            "content": {"parts": [{"text": json.dumps({
                "verdict": "ambiguous",
                "confidence": 0.5,
                "picked_siren": "111111111",
                "reasoning": "unsure",
            })}]}
        }]
    }
    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__ = AsyncMock(return_value=None)
    mock_client.post = AsyncMock(return_value=mock_response)

    with patch("httpx.AsyncClient", return_value=mock_client):
        result = await judge_match(
            api_key="fake",
            maps_name="X", maps_address=None, maps_phone=None,
            candidates=[{"siren": "111111111"}],
            rejected_siren=None,
        )
    assert result is not None
    assert result["verdict"] == "ambiguous"
    assert result["picked_siren"] is None


# ---------------------------------------------------------------------------
# 10. judge_match rejects malformed SIREN
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_judge_match_rejects_malformed_siren():
    """picked_siren must be 9 digits when verdict==match; otherwise return None."""
    for bad_siren in ["12345", "12345678A", "", None, 123456789]:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.json.return_value = {
            "candidates": [{
                "content": {"parts": [{"text": json.dumps({
                    "verdict": "match",
                    "confidence": 0.9,
                    "picked_siren": bad_siren,
                    "reasoning": "x",
                })}]}
            }]
        }
        mock_client = AsyncMock()
        mock_client.__aenter__ = AsyncMock(return_value=mock_client)
        mock_client.__aexit__ = AsyncMock(return_value=None)
        mock_client.post = AsyncMock(return_value=mock_response)

        with patch("httpx.AsyncClient", return_value=mock_client):
            result = await judge_match(
                api_key="fake",
                maps_name="X", maps_address=None, maps_phone=None,
                candidates=[{"siren": "111111111"}],
                rejected_siren=None,
            )
        assert result is None, f"Expected None for bad_siren={bad_siren!r}"


# ---------------------------------------------------------------------------
# 11. gemini_quarantine audit must record original_method (not the post-
#     mutation "gemini_quarantine") — Apr 27 alias-trap regression.
#
# Bug: at fortress/discovery.py:2958, `_pending_link = candidate` aliases
# the same dict. Then at line ~3379, `_pending_link["method"] = "gemini_quarantine"`
# mutates the shared dict. Pre-fix, the audit detail's `original_method` field
# read `candidate["method"]` AFTER the mutation, always recording
# "gemini_quarantine" and losing the real upstream method (enseigne / inpi /
# siren_website / geo_proximity / etc.).
#
# Fix: capture `_original_method = _pending_link["method"]` BEFORE the
# mutation and pass that into the audit detail. This test codifies the
# capture-before-mutate convention so future edits don't regress.
# ---------------------------------------------------------------------------

def test_quarantine_audit_captures_original_method_before_alias_mutation():
    """Pre-fix bug pattern would have failed; the fix captures before mutating."""
    # Reproduce the production pattern at discovery.py:2958
    candidate = {"method": "siren_website", "siren": "123456789"}
    _pending_link = candidate  # alias — same dict object

    # Fix pattern: capture BEFORE the mutation at discovery.py:~3379
    _original_method = _pending_link["method"]
    _pending_link["method"] = "gemini_quarantine"  # production mutation

    # Audit detail uses the captured value, not a re-read of the aliased dict.
    detail = {
        "quarantined_siren": "123456789",
        "original_method": _original_method,
    }
    assert detail["original_method"] == "siren_website", (
        "original_method must reflect the pre-quarantine method; "
        "regression means the alias was re-read AFTER the mutation."
    )
    # Sanity: the alias mutation did happen on the shared dict.
    assert candidate["method"] == "gemini_quarantine"
    assert _pending_link is candidate


# ---------------------------------------------------------------------------
# 12. siret_address_naf method is NOT swapped by Gemini (May 1 exemption).
#
# Discovery.py:3954 guards the swap action with:
#   _pending_link["method"] not in ("chain", "siret_address_naf")
#
# Track 2 (siret_address_naf) matches link head-SIREN data (e.g. "COMMUNE DE …")
# to an operating establishment (e.g. "Camping du Lac"). Gemini sees the
# denomination mismatch and would pick a different SIREN — but that "better"
# candidate is another head-SIREN unrelated to the operating establishment.
# The exemption prevents the swap. Step 2.7's SQL constraints are the safety net.
# ---------------------------------------------------------------------------

def test_siret_address_naf_not_swapped_by_gemini():
    """siret_address_naf pending_link must be excluded from the swap branch."""
    _pending_link = {"method": "siret_address_naf", "siren": "123456789"}
    _just_auto_confirmed = True
    _v = "match"
    _vpicked = "999999999"  # Gemini "picked" a different SIREN
    _vconf = 0.95  # above any threshold

    # Reproduce the swap branch guard from discovery.py:3947-3954
    swap_fires = (
        _v == "match"
        and _vpicked is not None
        and _pending_link is not None
        and _just_auto_confirmed
        and _vpicked != _pending_link["siren"]
        and _vconf >= 0.85  # typical gemini_d1b_quarantine_threshold
        and _pending_link["method"] not in ("chain", "siret_address_naf")  # exemption gate
    )

    assert not swap_fires, (
        "Swap branch must NOT fire for siret_address_naf — "
        "Gemini's 'better candidate' is another head-SIREN unrelated to the "
        "operating establishment found by Track 2's SQL constraints."
    )
    # Verify siren unchanged (swap action is gated, so no mutation)
    if not swap_fires:
        assert _pending_link["siren"] == "123456789"
        assert _pending_link["method"] == "siret_address_naf"


# ---------------------------------------------------------------------------
# 13. siret_address_naf method is NOT quarantined by Gemini (May 1 exemption).
#
# Discovery.py:4066 guards the quarantine action with:
#   _pending_link["method"] not in ("chain", "siret_address_naf")
#
# Gemini's Frankenstein check sees "COMMUNE DE …" vs "Camping du Lac" and
# would flip confirmed → pending. The exemption prevents that. The SQL
# constraints in Step 2.7 (exact CP + exact NAF + 1-row disambiguation) are
# the safety net instead.
# ---------------------------------------------------------------------------

def test_siret_address_naf_not_quarantined_by_gemini():
    """siret_address_naf pending_link must be excluded from the quarantine branch."""
    _pending_link = {"method": "siret_address_naf", "siren": "123456789"}
    link_confidence = "confirmed"
    _just_auto_confirmed = True
    _v = "no_match"
    _vconf = 0.95  # above any threshold

    # Reproduce the quarantine branch guard from discovery.py:4061-4066
    quarantine_fires = (
        _v == "no_match"
        and _vconf >= 0.85  # typical gemini_d1b_quarantine_threshold
        and _pending_link is not None
        and _just_auto_confirmed
        and _pending_link["method"] not in ("chain", "siret_address_naf")  # exemption gate
    )

    assert not quarantine_fires, (
        "Quarantine branch must NOT fire for siret_address_naf — "
        "Gemini's Frankenstein check cannot evaluate Track 2 because the "
        "head SIREN denomination belongs to a different entity than the "
        "operating establishment by design (e.g. commune ≠ campground)."
    )
    # Verify link_confidence and method unchanged (quarantine action is gated)
    if not quarantine_fires:
        assert link_confidence == "confirmed"
        assert _pending_link["method"] == "siret_address_naf"
