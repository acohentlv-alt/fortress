"""BAN-geocoded backfill for ws174 historical MAPS entities.

Idempotent. Re-runs Step 2.6 of the matcher on backfilled rows
and auto-confirms with conservative thresholds (0.95 / 0.20).

NEVER calls _compute_naf_status. The picker context is lost
historically, so naf_status is set to NULL directly on accept,
and link_signals carries geo_proximity_backfill:true.

Usage:
    python3 -m scripts.backfill_ban_geo                       # ws174, all rows
    python3 -m scripts.backfill_ban_geo --limit 50            # cap at 50 rows (testing)
    python3 -m scripts.backfill_ban_geo --workspace 174       # explicit (only 174 allowed)
    python3 -m scripts.backfill_ban_geo --dry-run             # geocode + log, no DB writes
"""

from __future__ import annotations

import argparse
import asyncio
import csv
import io
import json
import logging
import os
import sys
import time
from collections import defaultdict
from datetime import date
from pathlib import Path

# Allow running from repo root with `python3 -m scripts.backfill_ban_geo`
# or direct `python3 scripts/backfill_ban_geo.py`.
_REPO = Path(__file__).resolve().parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import psycopg
from dotenv import load_dotenv

# Load .env before importing fortress modules (settings reads env at import time)
load_dotenv(str(_REPO / ".env"))

from fortress.config.settings import settings
from fortress.discovery import (
    _geo_proximity_match,
    _copy_sirene_reference_data,
)
from fortress.processing.dedup import log_audit

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BAN_CSV_URL = "https://api-adresse.data.gouv.fr/search/csv/"
BAN_CHUNK = 5000        # BAN limits ~50MB / 50 req/s
BAN_RATE_DELAY_S = 1    # polite delay between chunks (seconds)
BAN_MIN_SCORE = 0.5     # below this — DROP, do not insert

# Conservative thresholds for backfill auto-confirm
# (vs fresh-batch 0.85 / 0.15 — picker context lost, so tighten).
BACKFILL_TOP_THRESHOLD = 0.95
BACKFILL_DOMINANCE_THRESHOLD = 0.20

# Synthetic batch_id ties all audit rows for this run together.
# TEXT column has no length cap, so this 30-char string is safe.
BACKFILL_BATCH_ID = f"BACKFILL_BAN_GEO_{date.today().isoformat()}"

# ANSI colours for terminal readability
_GREEN = "\033[32m"
_YELLOW = "\033[33m"
_RED = "\033[31m"
_DIM = "\033[2m"
_BOLD = "\033[1m"
_RESET = "\033[0m"

log = logging.getLogger("fortress.backfill_ban_geo")

# ---------------------------------------------------------------------------
# Step 1 — Enumerate targets
# ---------------------------------------------------------------------------

async def enumerate_targets(conn, workspace_id: int = 174, limit: int | None = None) -> list[tuple]:
    """ws174 MAPS entities with non-empty address and no companies_geom row.

    Returns list of (siren, adresse, code_postal, ville) tuples.
    Does NOT filter by link_confidence — we geocode all of them. The
    Step 2.6 replay function skips already-confirmed rows.
    """
    lim_clause = f"LIMIT {int(limit)}" if limit else ""
    cur = await conn.execute(
        f"""SELECT co.siren, co.adresse, co.code_postal, co.ville
            FROM companies co
            LEFT JOIN companies_geom cg ON cg.siren = co.siren
            WHERE co.workspace_id = %s
              AND co.siren LIKE 'MAPS%%'
              AND co.adresse IS NOT NULL
              AND co.adresse <> ''
              AND cg.siren IS NULL
            ORDER BY co.siren {lim_clause}""",
        (workspace_id,),
    )
    return list(await cur.fetchall())


# ---------------------------------------------------------------------------
# Step 2 — Build CSV for BAN
# ---------------------------------------------------------------------------

def build_csv(rows: list[tuple]) -> bytes:
    """Build a CSV with siren + q columns.

    q MUST contain the FULL address (street + postcode + city) to avoid
    the postcode-mismatch false-positive documented in .ban_endpoint_report.md.
    Missing components are skipped (no double commas).
    """
    buf = io.StringIO()
    w = csv.writer(buf)
    w.writerow(["siren", "q"])
    for siren, addr, cp, ville in rows:
        full = ", ".join(p for p in (addr, cp, ville) if p)
        w.writerow([siren, full])
    return buf.getvalue().encode("utf-8")


# ---------------------------------------------------------------------------
# Step 3 — POST to BAN
# ---------------------------------------------------------------------------

def post_chunk_to_ban(csv_bytes: bytes) -> list[dict]:
    """POST to BAN /search/csv/. Synchronous — curl_cffi has no async API.

    Uses curl_cffi's CurlMime for multipart form-data upload (curl_cffi
    does not accept the `files=` kwarg — it requires `multipart=CurlMime()`).

    DO NOT pass result_columns — we need the FULL default response
    (20 cols) including result_postcode and result_status for the
    dual-gate safety check below.

    data kwarg is exactly {"columns": "q"} conceptually — in CurlMime
    form that is a plain text part named "columns" with value "q".
    """
    import curl_cffi.requests as cr_requests
    from curl_cffi import CurlMime

    mp = CurlMime()
    mp.addpart("data", data=csv_bytes, filename="addresses.csv", content_type="text/csv")
    mp.addpart("columns", data=b"q")

    resp = cr_requests.post(
        BAN_CSV_URL,
        multipart=mp,
        timeout=120,
    )
    resp.raise_for_status()
    return list(csv.DictReader(io.StringIO(resp.text)))


# ---------------------------------------------------------------------------
# Step 4 — Evaluate BAN response row (dual-gate safety)
# ---------------------------------------------------------------------------

def evaluate_ban_row(
    result: dict,
    sirene_code_postal: str | None,
) -> tuple[str | None, str]:
    """Apply the locked dual-gate to a single BAN response row.

    Returns (quality, drop_reason). Exactly one of the two is non-empty:
      - (quality, "")  → INSERT with this geocode_quality
      - (None, reason) → DROP, log audit with this reason

    Reasons (stable strings — used in audit detail and QA queries):
      - "ban_no_match"        result_status != 'ok' or blank score
      - "postcode_mismatch"   BAN city != SIRENE city (CRITICAL safety gate)
      - "ban_low_score"       score < 0.5
      - "sirene_no_postcode"  SIRENE has no postcode to verify against

    CRITICAL: The postcode-verification gate (GATE 2) runs BEFORE the
    score threshold because BAN can return score=0.97 for the wrong city
    when input lacks the city component. See .ban_endpoint_report.md.
    """
    # GATE 1 — status filter: drop non-ok and blank-score rows
    status = (result.get("result_status") or "").strip()
    score_str = (result.get("result_score") or "").strip()
    if status != "ok" or not score_str:
        return None, "ban_no_match"

    # GATE 2 — postcode verification (CRITICAL).
    # Must run BEFORE score threshold — BAN can score 0.97 for the wrong
    # city when the input address lacks the city component.
    sirene_pc = (sirene_code_postal or "").strip()
    if not sirene_pc:
        return None, "sirene_no_postcode"
    result_pc = (result.get("result_postcode") or "").strip()
    if result_pc != sirene_pc:
        return None, "postcode_mismatch"

    # GATE 3 — quality threshold
    try:
        score = float(score_str)
    except ValueError:
        return None, "ban_no_match"
    if score >= 0.8:
        return "bonne", ""
    if score >= BAN_MIN_SCORE:
        return "acceptable", ""
    return None, "ban_low_score"


# ---------------------------------------------------------------------------
# Step 5 — Replay Step 2.6 for a geocoded entity
# ---------------------------------------------------------------------------

async def replay_step_2_6(
    conn,
    siren: str,
    lat: float,
    lng: float,
    ban_score: float,
    dry_run: bool = False,
) -> dict | None:
    """Re-run Step 2.6 (geo proximity) with conservative thresholds.

    Skips entities that are already confirmed or already geo-matched.
    Returns the candidate dict on a new match, None otherwise.

    BLOCKER 1 fix: NEVER calls _compute_naf_status. Sets naf_status=NULL
    directly on accept. The picker context is lost historically.
    """
    cur = await conn.execute(
        """SELECT denomination, enseigne, link_method, link_confidence
           FROM companies WHERE siren = %s""",
        (siren,),
    )
    co = await cur.fetchone()
    if co is None:
        return None

    denom, enseigne, link_method, link_conf = co

    # Skip already-confirmed rows (any method) — don't downgrade or re-confirm
    if link_conf == "confirmed":
        return None
    # Skip rows already matched via geo_proximity (from a prior run)
    if link_method == "geo_proximity":
        return None

    maps_name = (enseigne or denom or "").strip()
    if not maps_name:
        return None

    # CRITICAL: picked_nafs=None (no filter mode) — picker context lost.
    # Conservative thresholds require Phase 2 signature exposing the kwargs.
    cand = await _geo_proximity_match(
        conn,
        maps_name,
        lat,
        lng,
        picked_nafs=None,
        naf_division_whitelist=None,
        top_threshold=BACKFILL_TOP_THRESHOLD,
        dominance_threshold=BACKFILL_DOMINANCE_THRESHOLD,
    )

    if cand is None:
        return None

    # Candidate found — build link_signals payload (BLOCKER 1 fix)
    top_score = cand.get("geo_proximity_top_score", cand.get("score", 0.0))
    dominance = top_score - cand.get("geo_proximity_2nd_score", 0.0)
    link_signals = json.dumps({
        "geo_proximity_backfill": True,
        "top_score": round(top_score, 4),
        "dominance": round(dominance, 4),
        "ban_score": round(ban_score, 4),
        "thresholds": {"top": BACKFILL_TOP_THRESHOLD, "dominance": BACKFILL_DOMINANCE_THRESHOLD},
    }, ensure_ascii=False)

    if not dry_run:
        # Write MAPS entity update — naf_status=NULL (BLOCKER 1 fix, bypass _compute_naf_status)
        await conn.execute(
            """UPDATE companies
               SET    linked_siren    = %s,
                      link_method     = 'geo_proximity',
                      link_confidence = 'confirmed',
                      naf_status      = NULL,
                      link_signals    = %s::jsonb
               WHERE  siren = %s""",
            (cand["siren"], link_signals, siren),
        )

        # Copy SIRENE reference data into the MAPS entity
        await _copy_sirene_reference_data(conn, maps_siren=siren, target_siren=cand["siren"])

        # Audit log
        await log_audit(
            conn,
            batch_id=BACKFILL_BATCH_ID,
            siren=siren,
            action="auto_linked_geo_proximity_backfill",
            result="success",
            detail=(
                f"target={cand['siren']} "
                f"top={top_score:.3f} "
                f"dom={dominance:.3f} "
                f"ban={ban_score:.3f}"
            ),
            workspace_id=174,
        )

    return cand


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------

async def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument(
        "--workspace", type=int, default=174,
        help="Workspace ID to backfill (must be 174 in V1)",
    )
    ap.add_argument(
        "--limit", type=int, default=None,
        help="Cap the number of MAPS entities processed (for testing)",
    )
    ap.add_argument(
        "--dry-run", action="store_true",
        help="Geocode + log telemetry but make no DB writes",
    )
    args = ap.parse_args()

    # Hard-gate: ws174 ONLY in V1
    if args.workspace != 174:
        print(
            f"{_RED}ERREUR: Backfill restreint au workspace 174 dans la V1. "
            f"Le retrofit ws1 (Cindy) sera lancé séparément.{_RESET}"
        )
        sys.exit(1)

    db_url = settings.db_url
    if not db_url:
        print(f"{_RED}DATABASE_URL non configuré. Vérifiez le fichier .env.{_RESET}")
        sys.exit(1)

    mode = f"{_YELLOW}DRY RUN{_RESET}" if args.dry_run else f"{_GREEN}APPLY{_RESET}"
    limit_label = f", limit={args.limit}" if args.limit else ""
    print(
        f"{_BOLD}BAN Geo Backfill — {mode}{_RESET}  "
        f"(workspace={args.workspace}{limit_label})"
    )
    print(f"batch_id: {BACKFILL_BATCH_ID}")
    print(
        f"thresholds: top={BACKFILL_TOP_THRESHOLD}, "
        f"dominance={BACKFILL_DOMINANCE_THRESHOLD}"
    )
    print()

    # Telemetry counters
    targets = 0
    geocoded = 0
    confirmed = 0
    drop_counts: dict[str, int] = defaultdict(int)
    quality_counts: dict[str, int] = defaultdict(int)

    async with await psycopg.AsyncConnection.connect(db_url) as conn:
        # --- Enumerate targets ---
        rows = await enumerate_targets(conn, workspace_id=args.workspace, limit=args.limit)
        targets = len(rows)
        print(f"Cibles ws174 sans geom: {_BOLD}{targets}{_RESET}")
        if not rows:
            print("Rien à traiter — idempotency OK.")
            return

        # Build lookup: siren → (code_postal,) for postcode gate
        sirene_lookup: dict[str, str | None] = {
            siren: cp for siren, _addr, cp, _ville in rows
        }

        # --- Process in chunks ---
        chunks = [rows[i : i + BAN_CHUNK] for i in range(0, len(rows), BAN_CHUNK)]
        log.info("backfill.ban_request_count n=%s", len(chunks))
        print(f"Chunks BAN: {len(chunks)} (taille max {BAN_CHUNK})")

        for chunk_idx, chunk in enumerate(chunks, 1):
            print(f"\n{_DIM}[Chunk {chunk_idx}/{len(chunks)}] — {len(chunk)} adresses{_RESET}")

            # Build CSV and POST to BAN (synchronous curl_cffi call)
            csv_bytes = build_csv(chunk)
            try:
                results = await asyncio.to_thread(post_chunk_to_ban, csv_bytes)
            except Exception as exc:
                log.error("backfill.ban_chunk_error chunk=%s error=%s", chunk_idx, exc)
                print(f"{_RED}Erreur BAN chunk {chunk_idx}: {exc}{_RESET}")
                # Continue with next chunk — don't abort the whole run
                continue

            # Build a map: siren → result row (BAN echoes input columns)
            # BAN echoes ALL non-q input columns unchanged. We keyed by `siren`.
            result_by_siren: dict[str, dict] = {}
            for r in results:
                s = (r.get("siren") or "").strip()
                if s:
                    result_by_siren[s] = r

            # --- Per-row processing ---
            inserted_sirens: list[tuple] = []  # (siren, lat, lng, ban_score)

            for siren, addr, cp, ville in chunk:
                result = result_by_siren.get(siren)
                if result is None:
                    # BAN didn't echo this row back — treat as no match
                    drop_counts["ban_no_match"] += 1
                    if not args.dry_run:
                        await log_audit(
                            conn,
                            batch_id=BACKFILL_BATCH_ID,
                            siren=siren,
                            action="ban_backfill_dropped",
                            result="dropped",
                            detail=(
                                f"reason=ban_no_match "
                                f"expected_pc={sirene_lookup.get(siren) or '-'} "
                                f"got_pc=- score=-"
                            ),
                            workspace_id=174,
                        )
                    continue

                sirene_pc = sirene_lookup.get(siren)
                quality, drop_reason = evaluate_ban_row(result, sirene_pc)

                if drop_reason:
                    drop_counts[drop_reason] += 1
                    pc_got = (result.get("result_postcode") or "-").strip()
                    score_str = (result.get("result_score") or "-").strip()
                    detail_str = (
                        f"reason={drop_reason} "
                        f"expected_pc={sirene_pc or '-'} "
                        f"got_pc={pc_got} "
                        f"score={score_str}"
                    )
                    if not args.dry_run:
                        await log_audit(
                            conn,
                            batch_id=BACKFILL_BATCH_ID,
                            siren=siren,
                            action="ban_backfill_dropped",
                            result="dropped",
                            detail=detail_str,
                            workspace_id=174,
                        )
                    continue

                # Passed all three gates — extract coords
                # BAN CSV returns longitude BEFORE latitude (EPSG xy convention).
                # Always parse by NAME, never by position.
                try:
                    lng_val = float(result["longitude"])
                    lat_val = float(result["latitude"])
                    ban_score_val = float((result.get("result_score") or "0").strip())
                except (KeyError, ValueError) as exc:
                    log.warning("backfill.bad_coords siren=%s error=%s", siren, exc)
                    drop_counts["ban_no_match"] += 1
                    continue

                quality_counts[quality] += 1  # type: ignore[index]
                geocoded += 1

                if not args.dry_run:
                    # INSERT — ON CONFLICT DO NOTHING: never overwrite Phase 1
                    # maps_panel or Phase 2 sirene_geo rows.
                    await conn.execute(
                        """INSERT INTO companies_geom
                               (siren, lat, lng, source, geocode_quality)
                           VALUES (%s, %s, %s, 'ban_backfill', %s)
                           ON CONFLICT (siren) DO NOTHING""",
                        (siren, lat_val, lng_val, quality),
                    )

                inserted_sirens.append((siren, lat_val, lng_val, ban_score_val))

            # Commit after each chunk to preserve work progressively
            if not args.dry_run:
                await conn.commit()

            # --- Step 2.6 replay for geocoded entities in this chunk ---
            for siren, lat_val, lng_val, ban_score_val in inserted_sirens:
                try:
                    cand = await replay_step_2_6(
                        conn, siren, lat_val, lng_val, ban_score_val,
                        dry_run=args.dry_run,
                    )
                    if cand is not None:
                        confirmed += 1
                        target_siren = cand["siren"]
                        top_score = cand.get("geo_proximity_top_score", cand.get("score", 0.0))
                        print(
                            f"  {_GREEN}+confirm{_RESET} "
                            f"{siren} → {target_siren} "
                            f"(top={top_score:.3f})"
                        )
                except Exception as exc:
                    log.error(
                        "backfill.replay_error siren=%s error=%s", siren, exc
                    )

            # Commit after Step 2.6 replay for this chunk
            if not args.dry_run:
                await conn.commit()

            # Polite delay between chunks (BAN rate limit: 50 req/s)
            if chunk_idx < len(chunks):
                time.sleep(BAN_RATE_DELAY_S)

    # --- Final telemetry ---
    log.info("backfill.ban_score_distribution %s", dict(quality_counts))
    log.info("backfill.dropped reason=ban_no_match n=%s", drop_counts["ban_no_match"])
    log.info("backfill.dropped reason=postcode_mismatch n=%s", drop_counts["postcode_mismatch"])
    log.info("backfill.dropped reason=ban_low_score n=%s", drop_counts["ban_low_score"])
    log.info("backfill.dropped reason=sirene_no_postcode n=%s", drop_counts["sirene_no_postcode"])
    log.info("backfill.step_2_6_recoveries n=%s", confirmed)
    log.info(
        "backfill.completed targets=%s inserted=%s confirmed=%s",
        targets, geocoded, confirmed,
    )

    match_rate = (geocoded / targets * 100) if targets > 0 else 0.0

    print(f"\n{_BOLD}{'='*60}{_RESET}")
    print(f"{'DRY RUN — ' if args.dry_run else ''}Backfill terminé.")
    print(f"  Cibles traitées  : {targets}")
    print(f"  Géocodées (BAN)  : {geocoded}  ({match_rate:.1f}%)")
    print(f"  Confirmées (2.6) : {confirmed}")
    print(f"  Abandonnées      :")
    for reason, n in sorted(drop_counts.items(), key=lambda x: -x[1]):
        print(f"    {reason:22s}: {n}")
    print(f"  Qualité BAN      : {dict(quality_counts)}")
    print(f"{_BOLD}{'='*60}{_RESET}")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    asyncio.run(main())
