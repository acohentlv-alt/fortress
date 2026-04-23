"""One-shot retrofit of Gemini D1b quarantine on historical mismatch-confirmed rows.

Target: MAPS entities where naf_status='mismatch' AND link_confidence='confirmed'
        AND siren starts with 'MAPS' AND was confirmed before Gemini D1b shipped
        (2026-04-23 12:00 UTC). These rows slipped past the NAF gate via Phase A
        signals (phone+address+enseigne) before D1b's quarantine path existed.

Behaviour mirrors the live quarantine branch in discovery.py:
  - Skip if _is_frankenstein_parent_siren matches (legitimate parent-SIREN pattern)
  - Call gemini judge_match with the single currently-linked candidate
  - If verdict=no_match @ conf >= threshold: quarantine (flip to pending, roll
    back SIRENE-derived fields, keep linked_siren so the row appears in the
    pending queue for manual review).

Dry run by default; pass --apply to actually write. Always logs an audit row
per retrofitted SIREN with action='d1b_retrofit_quarantined' or a kept-variant
so the decision trail is reconstructible.

Usage:
    python3 -m scripts.retrofit_d1b              # dry run
    python3 -m scripts.retrofit_d1b --apply      # commit changes
    python3 -m scripts.retrofit_d1b --ws 1       # only Cindy's workspace
    python3 -m scripts.retrofit_d1b --limit 5    # test on first 5 rows
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

# Allow running from repo root with `python3 -m scripts.retrofit_d1b`
# or direct `python3 scripts/retrofit_d1b.py`.
_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import psycopg
from psycopg.rows import dict_row

from fortress.config.settings import settings
from fortress.discovery import _is_frankenstein_parent_siren
from fortress.matching import gemini as gemini_judge

_QUARANTINE_THRESHOLD = 0.85  # matches settings.gemini_d1b_quarantine_threshold default
_D1B_SHIP_TS = "2026-04-23 12:00+00"  # 8d0dc65 went to main

# ANSI colours for terminal readability
_RED = "\033[31m"
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_CYAN = "\033[36m"
_DIM = "\033[2m"
_BOLD = "\033[1m"
_RESET = "\033[0m"


async def fetch_candidates(conn, workspace_id: int | None, limit: int | None) -> list[dict]:
    """Return the retrofit pool: mismatch+confirmed MAPS rows pre-D1b."""
    ws_clause = "AND co.workspace_id = %s" if workspace_id is not None else ""
    ws_args: tuple = (workspace_id,) if workspace_id is not None else ()
    lim_clause = f"LIMIT {int(limit)}" if limit else ""
    sql = f"""
        SELECT
            co.siren                    AS maps_siren,
            co.denomination             AS maps_name,
            co.adresse                  AS maps_address,
            co.workspace_id,
            co.link_method,
            co.linked_siren             AS target_siren,
            co.updated_at,
            s.denomination              AS sirene_denom,
            s.enseigne                  AS sirene_enseigne,
            s.adresse                   AS sirene_address,
            s.naf_code                  AS sirene_naf,
            s.naf_libelle               AS sirene_naflbl,
            s.code_postal               AS sirene_cp,
            s.ville                     AS sirene_ville,
            ct.phone                    AS maps_phone
        FROM companies co
        JOIN companies s ON s.siren = co.linked_siren
        LEFT JOIN LATERAL (
            SELECT phone FROM contacts
            WHERE siren = co.siren AND source = 'google_maps'
            ORDER BY collected_at DESC NULLS LAST LIMIT 1
        ) ct ON TRUE
        WHERE co.naf_status = 'mismatch'
          AND co.link_confidence = 'confirmed'
          AND co.siren LIKE 'MAPS%%'
          AND co.updated_at < %s
          {ws_clause}
        ORDER BY co.workspace_id, co.updated_at DESC
        {lim_clause}
    """
    async with conn.cursor(row_factory=dict_row) as cur:
        await cur.execute(sql, (_D1B_SHIP_TS, *ws_args))
        return list(await cur.fetchall())


async def evaluate_row(row: dict, api_key: str) -> dict:
    """Returns {'action': 'quarantine'|'keep_frankenstein'|'keep_gemini_match'|
                           'keep_gemini_ambiguous'|'keep_low_conf'|'keep_error',
               'gemini_verdict': dict|None, 'reasoning': str}."""
    maps_name = row["maps_name"] or ""
    target_siren = row["target_siren"]

    # Step 1 — Frankenstein helper. Same gate as live quarantine path.
    if _is_frankenstein_parent_siren(maps_name, row.get("sirene_denom"), row.get("sirene_enseigne")):
        return {
            "action": "keep_frankenstein",
            "gemini_verdict": None,
            "reasoning": "Frankenstein signature matched (sirene_denom unrelated, enseigne matches maps_name)",
        }

    # Step 2 — ask Gemini with single-candidate pool (mirrors how discovery.py calls it).
    candidate = {
        "siren": target_siren,
        "denomination": row.get("sirene_denom"),
        "enseigne": row.get("sirene_enseigne"),
        "address": row.get("sirene_address"),
        "naf_code": row.get("sirene_naf"),
        "naf_libelle": row.get("sirene_naflbl"),
        "code_postal": row.get("sirene_cp"),
        "ville": row.get("sirene_ville"),
    }
    try:
        verdict = await gemini_judge.judge_match(
            api_key=api_key,
            maps_name=maps_name,
            maps_address=row.get("maps_address"),
            maps_phone=row.get("maps_phone"),
            candidates=[candidate],
            rejected_siren=None,
        )
    except Exception as exc:
        return {"action": "keep_error", "gemini_verdict": None, "reasoning": f"gemini exception: {exc}"}

    if verdict is None:
        return {"action": "keep_error", "gemini_verdict": None, "reasoning": "gemini returned None (timeout / parse error)"}

    v = verdict.get("verdict")
    conf = float(verdict.get("confidence") or 0.0)

    if v == "no_match" and conf >= _QUARANTINE_THRESHOLD:
        return {"action": "quarantine", "gemini_verdict": verdict,
                "reasoning": verdict.get("reasoning", "")}
    if v == "match":
        return {"action": "keep_gemini_match", "gemini_verdict": verdict,
                "reasoning": verdict.get("reasoning", "")}
    if v == "ambiguous":
        return {"action": "keep_gemini_ambiguous", "gemini_verdict": verdict,
                "reasoning": verdict.get("reasoning", "")}
    # no_match but below threshold
    return {"action": "keep_low_conf", "gemini_verdict": verdict,
            "reasoning": f"no_match conf={conf:.2f} below {_QUARANTINE_THRESHOLD}"}


async def apply_quarantine(conn, maps_siren: str, target_siren: str, verdict: dict, workspace_id: int | None) -> None:
    """Mirror of discovery.py gemini_quarantine branch."""
    async with conn.cursor() as cur:
        await cur.execute(
            """UPDATE companies
                  SET link_confidence = 'pending',
                      link_method     = 'gemini_quarantine',
                      siret_siege     = NULL,
                      naf_code        = NULL,
                      naf_libelle     = NULL,
                      forme_juridique = NULL,
                      date_creation   = NULL
                WHERE siren = %s""",
            (maps_siren,),
        )
        detail = json.dumps({
            "retrofit": True,
            "quarantined_siren": target_siren,
            "gemini_confidence": verdict.get("confidence"),
            "gemini_reasoning": (verdict.get("reasoning") or "")[:200],
        }, ensure_ascii=False)
        await cur.execute(
            """INSERT INTO batch_log (batch_id, siren, action, result, detail, workspace_id, timestamp)
               VALUES ('d1b_retrofit', %s, 'd1b_retrofit_quarantined', 'success', %s, %s, NOW())""",
            (maps_siren, detail, workspace_id),
        )


async def log_keep_decision(conn, maps_siren: str, decision: dict, workspace_id: int | None) -> None:
    """Audit rows we saw but did NOT quarantine — useful for reviewing false negatives later."""
    detail = json.dumps({
        "retrofit": True,
        "reason": decision["action"],
        "gemini_confidence": (decision.get("gemini_verdict") or {}).get("confidence"),
        "gemini_reasoning": (decision.get("reasoning") or "")[:200],
    }, ensure_ascii=False)
    async with conn.cursor() as cur:
        await cur.execute(
            """INSERT INTO batch_log (batch_id, siren, action, result, detail, workspace_id, timestamp)
               VALUES ('d1b_retrofit', %s, 'd1b_retrofit_kept', 'skipped', %s, %s, NOW())""",
            (maps_siren, detail, workspace_id),
        )


def colour_for_action(action: str) -> str:
    return {
        "quarantine":            _RED,
        "keep_frankenstein":     _CYAN,
        "keep_gemini_match":     _GREEN,
        "keep_gemini_ambiguous": _YELLOW,
        "keep_low_conf":         _YELLOW,
        "keep_error":            _DIM,
    }.get(action, _RESET)


async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--apply", action="store_true", help="Actually write changes (default: dry run)")
    ap.add_argument("--ws", type=int, default=None, help="Scope to one workspace (1 = Cindy, 174 = testing)")
    ap.add_argument("--limit", type=int, default=None, help="Cap the number of rows processed")
    args = ap.parse_args()

    if not settings.gemini_api_key:
        print(f"{_RED}GEMINI_API_KEY not configured. Check .env.{_RESET}")
        sys.exit(1)

    db_url = os.environ.get("DATABASE_URL")
    if not db_url:
        print(f"{_RED}DATABASE_URL not set.{_RESET}")
        sys.exit(1)

    mode = f"{_GREEN}APPLY{_RESET}" if args.apply else f"{_YELLOW}DRY RUN{_RESET}"
    ws_label = f"ws={args.ws}" if args.ws is not None else "all workspaces"
    print(f"{_BOLD}Gemini D1b retrofit — {mode}{_RESET}   ({ws_label}" + (f", limit={args.limit}" if args.limit else "") + ")")
    print(f"Threshold: no_match @ confidence >= {_QUARANTINE_THRESHOLD}")
    print()

    async with await psycopg.AsyncConnection.connect(db_url) as conn:
        rows = await fetch_candidates(conn, args.ws, args.limit)
        print(f"Candidates: {_BOLD}{len(rows)}{_RESET}")
        if not rows:
            return

        summary: dict[str, int] = {}
        for i, row in enumerate(rows, 1):
            maps_siren = row["maps_siren"]
            target = row["target_siren"]
            ws_id = row.get("workspace_id")
            ws_short = {1: "cindy", 174: "test"}.get(ws_id, f"ws{ws_id}")
            maps_name_trim = (row["maps_name"] or "")[:32]
            sirene_name_trim = (row["sirene_denom"] or "")[:28]
            print(f"{_DIM}[{i:2}/{len(rows)}]{_RESET} {ws_short:5} {maps_siren:10} {maps_name_trim:32} → {sirene_name_trim:28} (NAF {row.get('sirene_naf') or '—'})")

            decision = await evaluate_row(row, settings.gemini_api_key)
            action = decision["action"]
            summary[action] = summary.get(action, 0) + 1
            col = colour_for_action(action)
            reason = (decision.get("reasoning") or "")[:110]
            print(f"      {col}→ {action}{_RESET}  {_DIM}{reason}{_RESET}")

            if args.apply:
                if action == "quarantine":
                    await apply_quarantine(conn, maps_siren, target, decision["gemini_verdict"], ws_id)
                else:
                    await log_keep_decision(conn, maps_siren, decision, ws_id)

        if args.apply:
            await conn.commit()
            print(f"\n{_GREEN}Committed.{_RESET}")
        else:
            print(f"\n{_YELLOW}Dry run — no changes written. Re-run with --apply to commit.{_RESET}")

        print(f"\n{_BOLD}Summary:{_RESET}")
        for action, n in sorted(summary.items(), key=lambda x: -x[1]):
            col = colour_for_action(action)
            print(f"  {col}{action:26}{_RESET} {n}")


if __name__ == "__main__":
    asyncio.run(main())
