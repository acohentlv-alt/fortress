"""Batch execution API — create and launch scrape jobs.

Provides POST /api/batch/run to:
  1. Insert a batch_data row in the database
  2. Spawn the fortress.runner subprocess in the background
  3. Return the batch_id so the frontend can monitor progress
"""

import asyncio
import json
import os
import re
import subprocess
import sys
from pathlib import Path

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from fortress.api.db import fetch_all, fetch_one, get_conn
from fortress.api.routes.activity import log_activity

router = APIRouter(prefix="/api/batch", tags=["batch"])


class BatchRunRequest(BaseModel):
    """JSON body for POST /api/batch/run."""
    sector: str = Field(..., min_length=1, description="Sector name, e.g. 'agriculture'")
    department: str = Field(..., min_length=1, max_length=10, description="Department code (e.g. '66') or 'FR'/'ALL' for France-wide")
    mode: str = Field("discovery", description="Mode: discovery or enrichment")
    city: str | None = Field(None, description="Optional city filter")
    naf_codes: list[str] | None = Field(
        None,
        description="Optional list of NAF filters (1-10). Each can be a section letter (A-U), 2-digit division (e.g. 55), or 5-char code (e.g. 55.30Z). All codes must belong to the same sector group (see SECTOR_EXPANSIONS). Section letters must be picked alone.",
    )
    # Legacy alias — single code posted by older clients. Normalized into naf_codes at handler entry.
    naf_code: str | None = Field(None, description="DEPRECATED — use naf_codes. Accepted for backward compatibility.")
    strategy: str = Field("sirene", description="Discovery strategy: 'sirene' (DB-first) or 'maps' (Maps-first)")
    search_queries: list[str] | None = Field(None, description="Maps-first: list of search terms")
    time_cap_per_query_min: int | None = Field(None, ge=1, le=240, description="Per-query time budget in minutes; null = no cap. JS sends null when 'Illimité' pill is selected.")



def _build_batch_id(sector: str, dept: str, workspace_id: int | None = None) -> str:
    """Generate a unique batch_id like TRANSPORT_66_W1_BATCH_003 (workspace) or TRANSPORT_66_BATCH_003 (admin)."""
    base = f"{sector.upper().replace(' ', '_')}_{dept}"
    if workspace_id is not None:
        base = f"{base}_W{workspace_id}"
    return base


@router.post("/run", status_code=202)
async def run_batch(body: BatchRunRequest, request: Request):
    """Create a scrape job and launch the runner subprocess.

    Returns 202 with the batch_id for monitoring.
    """
    sector = body.sector.strip().lower()
    dept = body.department.strip()
    # Use the user's original search query as the display name if available,
    # so "camping perpignan" stays as-is instead of becoming "camping 66"
    if body.search_queries and body.search_queries[0]:
        batch_name = body.search_queries[0].strip()
    else:
        batch_name = f"{sector} {dept}"

    # Resolve workspace_id early so it can be used in base_id construction
    session_user = getattr(request.state, 'user', None)
    user_id = session_user.id if session_user else None
    workspace_id = session_user.workspace_id if session_user else None

    # Build base_id for batch numbering (embeds _W{workspace_id} when not admin)
    base_id = _build_batch_id(sector, dept, workspace_id)

    # Defined here so the except handler below can safely reference it even
    # if an exception fires before the SELECT/INSERT block assigns it.
    batch_id = None

    # Normalize picker input: accept legacy single `naf_code` or new `naf_codes` list.
    # New code always writes `naf_codes` into filters_json (list), even for single pick.
    picked_codes: list[str] = []
    if body.naf_codes:
        picked_codes = [c.strip() for c in body.naf_codes if c and c.strip()]
    elif body.naf_code and body.naf_code.strip():
        picked_codes = [body.naf_code.strip()]

    # Cap (10)
    if len(picked_codes) > 10:
        return JSONResponse(
            status_code=400,
            content={"error": "Trop de codes NAF sélectionnés (maximum 10)."},
        )

    # Same-sector-group validation (skipped for section letters and single pick)
    if len(picked_codes) > 1:
        # Section letters (A-U, single alpha char) must stand alone
        if any(len(c) == 1 and c.isalpha() for c in picked_codes):
            return JSONResponse(
                status_code=400,
                content={"error": "Un filtre par section (A-U) doit être utilisé seul. Combinez uniquement des codes NAF détaillés."},
            )
        from fortress.config.naf_sector_expansion import all_same_sector_group
        if not all_same_sector_group(picked_codes):
            return JSONResponse(
                status_code=400,
                content={"error": "Ces codes NAF ne sont pas de la même catégorie. Seuls les codes appartenant au même groupe sectoriel peuvent être combinés."},
            )

    filters_dict = {}
    if picked_codes:
        filters_dict["naf_codes"] = picked_codes
    if body.department:
        filters_dict["department"] = body.department.strip()
    filters_json = json.dumps(filters_dict) if filters_dict else None

    # Build search_queries JSON for Maps-first mode
    search_queries_json = None
    if body.strategy == "maps" and body.search_queries:
        search_queries_json = json.dumps(body.search_queries)

    # Determine batch number + insert in ONE atomic connection
    # to prevent TOCTOU race where two requests get the same number.
    try:
        async with get_conn() as conn:
            # Lock existing rows for this base_id to prevent concurrent duplicates.
            # The LIKE pattern already includes _W{workspace_id} when set, so it is
            # naturally workspace-scoped. We add an explicit workspace filter for
            # correctness (handles NULL for admin).
            if workspace_id is not None:
                rows = await conn.execute(
                    "SELECT batch_id FROM batch_data WHERE batch_id LIKE %s AND workspace_id = %s ORDER BY batch_id DESC FOR UPDATE",
                    (f"{base_id}%", workspace_id),
                )
            else:
                rows = await conn.execute(
                    "SELECT batch_id FROM batch_data WHERE batch_id LIKE %s AND workspace_id IS NULL ORDER BY batch_id DESC FOR UPDATE",
                    (f"{base_id}%",),
                )
            existing = await rows.fetchall()

            if not existing:
                batch_number = 1
                batch_id = f"{base_id}_BATCH_001"
            else:
                max_num = 0
                for row in existing:
                    qid = row[0] if isinstance(row, tuple) else row["batch_id"]
                    match = re.search(r"BATCH_(\d+)$", qid)
                    if match:
                        max_num = max(max_num, int(match.group(1)))
                if max_num == 0:
                    batch_number = 1
                    batch_id = f"{base_id}_BATCH_001"
                else:
                    batch_number = max_num + 1
                    batch_id = f"{base_id}_BATCH_{batch_number:03d}"

            # Calculate batch_offset for discovery mode — scoped to this workspace
            # so offset counts only prior batches from the same workspace.
            batch_offset = 0
            if body.mode == "discovery":
                if workspace_id is not None:
                    count_row = await (await conn.execute(
                        "SELECT SUM(COALESCE(batch_size, 0)) AS total FROM batch_data WHERE UPPER(batch_name) = %s AND status != 'deleted' AND workspace_id = %s",
                        (batch_name.upper(), workspace_id),
                    )).fetchone()
                else:
                    count_row = await (await conn.execute(
                        "SELECT SUM(COALESCE(batch_size, 0)) AS total FROM batch_data WHERE UPPER(batch_name) = %s AND status != 'deleted' AND workspace_id IS NULL",
                        (batch_name.upper(),),
                    )).fetchone()
                if count_row and count_row[0]:
                    batch_offset = count_row[0]

            from fortress.config.settings import settings as _settings
            worker_id = _settings.effective_worker_id

            # ── Same-workspace batch guard ─────────────────────────────────────
            if workspace_id is not None:
                guard_cur = await conn.execute(
                    """SELECT 1 FROM batch_data
                       WHERE workspace_id = %s
                       AND status IN ('queued', 'in_progress')
                       AND updated_at > NOW() - INTERVAL '15 minutes'
                       LIMIT 1""",
                    (workspace_id,),
                )
            else:
                guard_cur = await conn.execute(
                    """SELECT 1 FROM batch_data
                       WHERE workspace_id IS NULL
                       AND status IN ('queued', 'in_progress')
                       AND updated_at > NOW() - INTERVAL '15 minutes'
                       LIMIT 1""",
                )
            blocking = await guard_cur.fetchone()
            if blocking:
                return JSONResponse(
                    status_code=409,
                    content={"error": "Un batch est déjà en cours dans cet espace de travail. Veuillez attendre qu'il se termine."}
                )

            await conn.execute(
                """INSERT INTO batch_data
                   (batch_id, batch_name, status, batch_number, batch_offset, total_companies, batch_size, filters_json, user_id, worker_id, strategy, search_queries, workspace_id, time_cap_per_query_min)
                   VALUES (%s, %s, 'queued', %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (batch_id, batch_name, batch_number, batch_offset, 2000, 2000, filters_json, user_id, worker_id, body.strategy, search_queries_json, workspace_id, body.time_cap_per_query_min),
            )
            await conn.commit()
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={
                "error": f"Database insert failed: {exc}",
                **({"batch_id": batch_id} if batch_id else {}),
            },
        )

    # Spawn the runner as a detached subprocess with stderr logging
    fortress_root = Path(__file__).resolve().parent.parent.parent  # fortress/
    log_dir = fortress_root / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{batch_id}.log"

    runner_module = "fortress.discovery"

    runner_cmd = [
        sys.executable, "-m", runner_module, batch_id,
    ]

    # Sandbox workaround: if launcher exists, use it to bypass .env stat()
    launcher = Path("/tmp/fortress_launcher.py")
    if launcher.exists():
        runner_cmd = [
            sys.executable, str(launcher), "runner", batch_id,
        ]


    try:
        # Create log file (for future use), but let output flow to Render console
        log_fd = os.open(str(log_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        os.close(log_fd)  # Just create the file for now

        process = subprocess.Popen(
            runner_cmd,
            cwd=str(fortress_root.parent),  # Must be PARENT of fortress/ so `-m fortress.runner` resolves
            stdout=None,      # Inherit parent stdout → visible in Render Logs (structlog)
            stderr=None,      # Inherit parent stderr → visible in Render Logs (warnings/errors)
            close_fds=False,  # Let child inherit the fd
            start_new_session=True,  # Detach from parent process
        )
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to spawn runner: {exc}", "batch_id": batch_id},
        )

    # Log activity
    user = getattr(request.state, 'user', None)
    await log_activity(
        user_id=getattr(user, 'id', None) if user else None,
        username=getattr(user, 'username', 'system') if user else 'system',
        action='batch_launched',
        target_type='batch',
        target_id=batch_id,
        details=f"Recherche {sector} {dept} — jusqu'à 2000 entités",
    )

    return {
        "batch_id": batch_id,
        "batch_name": batch_name,
        "batch_number": batch_number,
        "batch_offset": batch_offset,
        "max_entities": 2000,
        "mode": body.mode,
        "pid": process.pid,
        "status": "launched",
        "message": f"Batch {batch_id} launched (PID {process.pid}). Monitor at /api/jobs/{batch_id}",
    }


@router.get("/naf-codes")
async def list_naf_codes(request: Request):
    """Return sections + divisions + full codes for the NAF picker.

    Auth required (session middleware). Same static data for all workspaces.
    Divisions inherit their parent section's label (no separate division dict exists).
    """
    from fortress.config.naf_codes import NAF_CODES, NAF_SECTIONS, NAF_DIVISION_TO_SECTION

    # Division labels fall back to parent section label (N4 decision)
    divisions = []
    for div, section in NAF_DIVISION_TO_SECTION.items():
        section_label = NAF_SECTIONS.get(section, "")
        divisions.append({"code": div, "label": f"{div} — {section_label}"})

    from fortress.config.naf_sector_expansion import SECTOR_EXPANSIONS
    from fortress.config.sector_query_variants import SECTOR_QUERY_VARIANTS

    return {
        "sections": [{"code": code, "label": f"{code} — {label}"} for code, label in NAF_SECTIONS.items()],
        "divisions": sorted(divisions, key=lambda d: d["code"]),
        "codes": [{"code": code, "label": f"{code} — {label}"} for code, label in NAF_CODES.items()],
        "sector_expansions": {k: sorted(list(v)) for k, v in SECTOR_EXPANSIONS.items()},
        "sector_query_variants": SECTOR_QUERY_VARIANTS,
    }


