"""Gemini shadow judge — asks Google Gemini to verify MAPS → SIRENE matches.

D1a (shadow-only): the verdict is LOGGED to batch_log and does NOT influence
linking, auto-confirm, or any routing decision. This file exists purely to
generate ground-truth data for D1b.

Model: gemini-3.1-flash-lite-preview (preview model, April 2026).
Multi-candidate mode (Patch B, April 21): judge_match now accepts a LIST of
candidates (0..N) and may return a picked_siren pointing at one of them.
Cost constant is a conservative upper bound matching the multi-candidate prompt
size; single-candidate calls actually cost ~$0.00053 USD.
"""

from __future__ import annotations

import asyncio
import json
from typing import Any

import structlog

log = structlog.get_logger(__name__)

_MODEL_NAME = "gemini-3.1-flash-lite-preview"  # single-constant swap point — see R12
_ENDPOINT_BASE = "https://generativelanguage.googleapis.com/v1beta/models"
_TIMEOUT_SEC = 6.0
# Conservative upper bound matching multi-candidate prompt (~900 tokens in + 300 out).
# Single-candidate calls actually cost ~$0.00053; using one constant for simplicity.
_COST_PER_CALL_USD = 0.0009  # 900 × $0.25/M + 300 × $1.50/M


class _SkipGemini(Exception):
    """Raised inside judge_match to short-circuit the call cleanly without logging an error.
    Used when a race makes the call unnecessary (e.g. eligibility changed mid-flight).
    """
    pass


async def judge_match(
    *,
    api_key: str,
    maps_name: str,
    maps_address: str | None,
    maps_phone: str | None,
    candidates: list[dict],  # 0..N candidates; empty list => "no candidate" case
    rejected_siren: str | None,
    fallback_model: str | None = None,
) -> dict | None:
    """Ask Gemini whether any of the candidates is the same business as the Maps entity.

    Returns one of:
        {"verdict": "match",    "confidence": float, "picked_siren": "9-digit SIREN", "reasoning": str}
        {"verdict": "no_match", "confidence": float, "picked_siren": None,            "reasoning": str}
        {"verdict": "ambiguous","confidence": float, "picked_siren": None,            "reasoning": str}
    Or None on timeout, network error, JSON parse failure, or any exception.

    The verdict is purely informational in D1a — the caller MUST NOT use it
    to modify linking state.
    """
    try:
        prompt = _build_prompt(
            maps_name=maps_name,
            maps_address=maps_address,
            maps_phone=maps_phone,
            candidates=candidates,
            rejected_siren=rejected_siren,
        )
    except _SkipGemini:
        return None

    body = {
        "contents": [{"role": "user", "parts": [{"text": prompt}]}],
        "generationConfig": {
            "responseMimeType": "application/json",
            "maxOutputTokens": 2000,
            "thinkingConfig": {"thinkingBudget": 0},
        },
    }

    models_to_try = [_MODEL_NAME]
    if fallback_model:
        models_to_try.append(fallback_model)

    for model in models_to_try:
        url = f"{_ENDPOINT_BASE}/{model}:generateContent?key={api_key}"
        try:
            import httpx
            async with httpx.AsyncClient(timeout=_TIMEOUT_SEC) as hx:
                resp = await hx.post(url, json=body)
                if resp.status_code != 200:
                    log.warning("gemini.http_error", model=model,
                                status=resp.status_code,
                                body=resp.text[:200])
                    continue
                data = resp.json()
            text = (
                data.get("candidates", [{}])[0]
                    .get("content", {})
                    .get("parts", [{}])[0]
                    .get("text", "")
            )
            verdict = json.loads(text)
            if verdict.get("verdict") not in {"match", "no_match", "ambiguous"}:
                log.warning("gemini.bad_verdict", verdict=verdict)
                return None
            conf = verdict.get("confidence")
            if not isinstance(conf, (int, float)) or not (0 <= conf <= 1):
                log.warning("gemini.bad_confidence", verdict=verdict)
                return None
            picked = verdict.get("picked_siren")
            if verdict["verdict"] == "match":
                if not (isinstance(picked, str) and len(picked) == 9 and picked.isdigit()):
                    log.warning("gemini.bad_picked_siren", verdict=verdict)
                    return None
                candidate_sirens = {c.get("siren") for c in candidates}
                if picked not in candidate_sirens:
                    log.warning("gemini.picked_siren_not_in_pool",
                                picked=picked, pool=list(candidate_sirens))
                    return None
            else:
                verdict["picked_siren"] = None
            return verdict
        except asyncio.TimeoutError:
            log.warning("gemini.timeout", model=model)
            continue
        except Exception as exc:
            log.warning("gemini.error", model=model, error=str(exc))
            continue

    return None


def _build_prompt(
    *,
    maps_name: str,
    maps_address: str | None,
    maps_phone: str | None,
    candidates: list[dict],
    rejected_siren: str | None,
) -> str:
    """Build a compact prompt with the Maps entity and candidate SIRENE evidence.

    IMPORTANT: email is NEVER included in the prompt (RGPD precaution).

    Multi-candidate mode: `candidates` is a list of 0..N dicts; when empty,
    the prompt says "no candidate proposed" and Gemini can only return no_match
    or ambiguous. When non-empty, Gemini picks by SIREN.
    """
    maps_block = {
        "name": maps_name,
        "address": maps_address or "(none)",
        "phone": maps_phone or "(none)",
    }
    if not candidates:
        cand_section = "CANDIDATES:\n(none — the matcher returned no candidate and the trigram pool was empty)"
    else:
        cand_lines = []
        for i, c in enumerate(candidates, 1):
            cand_lines.append(
                f"Candidate {i}:\n"
                + json.dumps({
                    "siren": c.get("siren"),
                    "denomination": c.get("denomination"),
                    "enseigne": c.get("enseigne"),
                    "adresse": c.get("adresse"),
                    "ville": c.get("ville"),
                    "naf_code": c.get("naf_code"),
                    "match_method": c.get("method"),
                    "match_score": c.get("score"),
                }, ensure_ascii=False, indent=2)
            )
        cand_section = "CANDIDATES:\n" + "\n\n".join(cand_lines)

    rej_block = (
        f"The INPI primary step also proposed SIREN {rejected_siren} "
        f"but it was rejected by the department/name overlap validator. "
        f"Treat this as additional negative signal — Gemini may or may not agree."
        if rejected_siren else "No INPI near-miss rejection on file."
    )
    return (
        "You are a French business-matching judge. Decide whether any of the "
        "SIRENE candidates below is the same business as the Google Maps entity.\n\n"
        f"MAPS ENTITY:\n{json.dumps(maps_block, ensure_ascii=False, indent=2)}\n\n"
        f"{cand_section}\n\n"
        f"REJECTED NEAR-MISS CONTEXT:\n{rej_block}\n\n"
        "Respond with a JSON object and NOTHING else:\n"
        '{"verdict": "match"|"no_match"|"ambiguous", '
        '"confidence": 0.0-1.0, '
        '"picked_siren": "<9-digit SIREN from candidates> or null", '
        '"reasoning": "<≤ 200 chars, French or English, plain text>"}'
    )
