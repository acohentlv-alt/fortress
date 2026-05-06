"""Phase 3 retroactive promotion — manual dry-run / apply script.

Identifies past pending+gemini_shadow_yes rows and applies Phase 2's
tiered promotion gate retroactively.

USAGE (dry-run, safe to run anytime):
    python3 -m fortress.scripts.phase3_retroactive_promote --workspaces 174 --days 30

USAGE (apply — writes to DB):
    python3 -m fortress.scripts.phase3_retroactive_promote --workspaces 174 --days 30 --apply

SAFETY RULES:
  - NEVER operates on workspace 1 (Cindy) unless --allow-cindy is explicitly passed.
  - Default: dry-run only. --apply required for actual DB writes.
  - Reads settings.gemini_promote_workspace_ids; refuses any workspace not in that list
    unless --force-workspace is used.
  - Each promoted row gets action='auto_linked_gemini_promoted_retroactive' with
    detail.phase3_retroactive=true and detail.original_batch_id set.

DO NOT auto-run. Run manually after Phase 2 ships + ≥1 ws174 batch completes cleanly.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
import sys
from datetime import timezone, datetime

logging.basicConfig(level=logging.INFO, format="%(levelname)s: %(message)s")
log = logging.getLogger(__name__)


async def _run(
    *,
    workspaces: list[int],
    days: int,
    apply: bool,
    allow_cindy: bool,
    db_url: str,
) -> None:
    import psycopg  # type: ignore

    from fortress.config.settings import settings
    from fortress.discovery import (
        _promote_classify_signals,
        _gemini_reasoning_admits_close,
        _surname_ambiguity_count,
        _verify_signals,
        _compute_naf_status,
        _copy_sirene_reference_data,
    )

    allowed_by_settings = settings.gemini_promote_workspace_ids

    for ws_id in workspaces:
        if ws_id == 1 and not allow_cindy:
            log.error("Workspace 1 (Cindy) is protected. Pass --allow-cindy to override. Skipping ws1.")
            sys.exit(1)
        if ws_id not in allowed_by_settings:
            log.error(
                "Workspace %d is not in settings.gemini_promote_workspace_ids (%s). "
                "Set GEMINI_PROMOTE_WORKSPACE_IDS or use --force-workspace.",
                ws_id, allowed_by_settings,
            )
            sys.exit(1)

    conn = await psycopg.AsyncConnection.connect(db_url)

    try:
        # Fetch candidates: pending + gemini_shadow_yes + confidence >= 0.9
        cur = await conn.execute(
            """
            SELECT DISTINCT ON (co.siren)
                   co.siren, co.denomination, co.linked_siren, co.link_method,
                   co.link_signals, co.naf_code, co.code_postal, co.workspace_id,
                   bd.batch_id, bd.strict_naf,
                   (bl.detail::jsonb->>'picked_siren') AS gemini_siren,
                   (bl.detail::jsonb->>'confidence')::float AS gemini_conf,
                   (bl.detail::jsonb->>'reasoning') AS gemini_reasoning
              FROM companies co
              JOIN batch_tags bt ON bt.siren = co.siren
              JOIN batch_data bd ON bd.batch_id = bt.batch_id
              JOIN batch_log bl ON bl.siren = co.siren AND bl.batch_id = bd.batch_id
             WHERE bd.workspace_id = ANY(%s)
               AND bd.status = 'completed'
               AND bd.strict_naf = false
               AND bd.created_at::date >= CURRENT_DATE - INTERVAL '%s days'
               AND bl.action = 'gemini_shadow_yes'
               AND co.link_confidence = 'pending'
               AND (bl.detail::jsonb->>'picked_siren') IS NOT NULL
               AND (bl.detail::jsonb->>'confidence')::float >= %s
             ORDER BY co.siren, bd.created_at DESC
            """,
            (workspaces, days, settings.gemini_promote_min_confidence),
        )
        rows = await cur.fetchall()
        log.info("Found %d candidate pending+shadow_yes rows (last %d days, workspaces=%s).",
                 len(rows), days, workspaces)

        promote_count = 0
        hold_count = 0
        error_count = 0

        for row in rows:
            (siren, denomination, linked_siren, link_method,
             link_signals_raw, naf_code, code_postal, ws_id,
             batch_id, strict_naf, gemini_siren, gemini_conf,
             gemini_reasoning) = row

            if strict_naf:
                log.debug("SKIP %s — strict_naf batch, not eligible for promotion.", siren)
                continue

            try:
                # Fetch target SIRENE row
                prom_cur = await conn.execute(
                    """SELECT siren, denomination, enseigne, adresse, ville,
                              naf_code, code_postal, departement
                         FROM companies
                        WHERE siren = %s AND statut = 'A' AND siren NOT LIKE 'MAPS%%'
                        LIMIT 1""",
                    (gemini_siren,),
                )
                prom_row = await prom_cur.fetchone()
                if not prom_row:
                    log.warning("SKIP %s — gemini_siren=%s not found / inactive.", siren, gemini_siren)
                    hold_count += 1
                    continue

                prom_naf = prom_row[5] or ""
                prom_cp = prom_row[6] or ""
                prom_dept = prom_row[7] or ""
                maps_dept = (code_postal[:2] if code_postal else prom_dept or "")

                # Re-verify signals
                prom_signals, _ = await _verify_signals(
                    conn, gemini_siren,
                    denomination or "",  # maps_name
                    None,  # maps_phone (not available retroactively)
                    None,  # maps_address
                    None,  # extracted_siren
                )

                # Classify tier
                tier, agreeing, blockers = _promote_classify_signals(
                    method=link_method or "fuzzy_name",
                    link_signals=prom_signals,
                    maps_dept=maps_dept,
                    target_dept=prom_dept,
                    matched_naf=prom_naf,
                    picked_nafs=[],  # picker not available retroactively — treat as empty
                )

                close_admitted = _gemini_reasoning_admits_close(gemini_reasoning)
                close_address_only = close_admitted and agreeing == ["address_exact"]

                surname_count = await _surname_ambiguity_count(conn, prom_row[1] or "", prom_cp)
                surname_ambiguous = surname_count >= 2

                decision = "promote"
                decision_reason: list[str] = []
                if tier == "block":
                    decision = "hold_pending"
                    decision_reason.append(f"tier_classify={tier}")
                    decision_reason.extend(blockers)
                elif close_address_only:
                    decision = "hold_pending"
                    decision_reason.append("close_not_same_address")
                elif surname_ambiguous:
                    decision = "hold_pending"
                    decision_reason.append(f"surname_ambiguous_count={surname_count}")

                log.info(
                    "[%s] %s → %s | siren=%s gemini=%s tier=%s conf=%.2f signals=%s blockers=%s",
                    "PROMOTE" if decision == "promote" else "HOLD",
                    siren, denomination or "?",
                    linked_siren, gemini_siren, tier,
                    gemini_conf, agreeing, decision_reason,
                )

                if decision == "promote":
                    promote_count += 1
                    if apply:
                        # Snapshot + write
                        snap_cur = await conn.execute(
                            """SELECT siret_siege, naf_code, naf_libelle,
                                      forme_juridique, date_creation,
                                      tranche_effectif, naf_status, strict_match
                                 FROM companies WHERE siren = %s""",
                            (siren,),
                        )
                        snap_row = await snap_cur.fetchone()
                        snapshot = {
                            "siret_siege": snap_row[0] if snap_row else None,
                            "naf_code": snap_row[1] if snap_row else None,
                            "naf_libelle": snap_row[2] if snap_row else None,
                            "forme_juridique": snap_row[3] if snap_row else None,
                            "date_creation": snap_row[4].isoformat() if snap_row and snap_row[4] else None,
                            "tranche_effectif": snap_row[5] if snap_row else None,
                            "naf_status": snap_row[6] if snap_row else None,
                            "strict_match": snap_row[7] if snap_row else None,
                        }
                        prom_link_signals = dict(prom_signals) if prom_signals else {}
                        prom_link_signals["gemini_promotion"] = {
                            "tier": tier,
                            "confidence": gemini_conf,
                            "signals_used": agreeing,
                            "picked_siren_changed": (linked_siren is not None and gemini_siren != linked_siren),
                            "original_cascade_siren": linked_siren,
                            "original_method": link_method,
                            "gemini_picked_siren": gemini_siren,
                            "reasoning": (gemini_reasoning or "")[:200],
                            "snapshot": snapshot,
                        }
                        async with conn.transaction():
                            await conn.execute(
                                """UPDATE companies
                                      SET siret_siege = NULL, naf_code = NULL,
                                          naf_libelle = NULL, forme_juridique = NULL,
                                          date_creation = NULL, tranche_effectif = NULL,
                                          naf_status = NULL
                                    WHERE siren = %s""",
                                (siren,),
                            )
                            await _copy_sirene_reference_data(conn, siren, gemini_siren)
                            prom_strict_match = True  # no picker available retroactively
                            await conn.execute(
                                """UPDATE companies
                                      SET linked_siren = %s,
                                          link_confidence = 'confirmed',
                                          link_method = 'gemini_judge',
                                          link_signals = %s,
                                          naf_status = NULL,
                                          strict_match = %s,
                                          rescued_by = 'gemini_promoted'
                                    WHERE siren = %s""",
                                (gemini_siren,
                                 json.dumps(prom_link_signals),
                                 prom_strict_match,
                                 siren),
                            )
                            await conn.execute(
                                """INSERT INTO batch_log
                                       (batch_id, siren, action, result, detail, workspace_id, created_at)
                                   VALUES (%s, %s, %s, %s, %s, %s, NOW())""",
                                (
                                    batch_id, siren,
                                    "auto_linked_gemini_promoted_retroactive",
                                    "success",
                                    json.dumps({
                                        "tier": tier,
                                        "gemini_confidence": gemini_conf,
                                        "signals_used": agreeing,
                                        "gemini_picked_siren": gemini_siren,
                                        "original_cascade_siren": linked_siren,
                                        "original_method": link_method,
                                        "original_batch_id": str(batch_id),
                                        "phase3_retroactive": True,
                                    }, ensure_ascii=False),
                                    ws_id,
                                ),
                            )
                        log.info("  → Applied promotion for %s.", siren)
                else:
                    hold_count += 1

            except Exception as exc:
                log.error("ERROR processing %s: %s", siren, exc)
                error_count += 1

        log.info(
            "Done. promote=%d hold=%d errors=%d (apply=%s)",
            promote_count, hold_count, error_count, apply,
        )
        if not apply and promote_count > 0:
            log.info("Dry-run: rerun with --apply to write %d promotions.", promote_count)

    finally:
        await conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Phase 3 retroactive Gemini promotion gate (dry-run by default)."
    )
    parser.add_argument("--workspaces", type=int, nargs="+", required=True,
                        help="Workspace IDs to process (e.g. 174). NEVER include 1 without --allow-cindy.")
    parser.add_argument("--days", type=int, default=30,
                        help="How many days back to look for pending+shadow_yes rows (default 30).")
    parser.add_argument("--apply", action="store_true",
                        help="Write promotions to DB (default: dry-run only).")
    parser.add_argument("--allow-cindy", action="store_true",
                        help="DANGEROUS: allow workspace 1 (Cindy). Requires explicit Alan auth.")
    parser.add_argument("--force-workspace", type=int, nargs="*", default=None,
                        help="Override settings allowlist check for listed workspace IDs.")
    args = parser.parse_args()

    from fortress.config.settings import settings

    workspaces = args.workspaces

    # Validate workspace allowlist (unless --force-workspace overrides)
    allowed = set(settings.gemini_promote_workspace_ids)
    if args.force_workspace:
        allowed |= set(args.force_workspace)

    for ws_id in workspaces:
        if ws_id == 1 and not args.allow_cindy:
            log.error("Workspace 1 is protected. Pass --allow-cindy to override.")
            sys.exit(1)
        if ws_id not in allowed:
            log.error(
                "Workspace %d not in settings.gemini_promote_workspace_ids %s. "
                "Add it to GEMINI_PROMOTE_WORKSPACE_IDS env or use --force-workspace %d.",
                ws_id, sorted(allowed), ws_id,
            )
            sys.exit(1)

    db_url = settings.db_url
    asyncio.run(_run(
        workspaces=workspaces,
        days=args.days,
        apply=args.apply,
        allow_cindy=args.allow_cindy,
        db_url=db_url,
    ))


if __name__ == "__main__":
    main()
