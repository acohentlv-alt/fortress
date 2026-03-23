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
    size: int = Field(20, ge=1, le=500, description="Number of entities to collect (SIRENE mode)")
    mode: str = Field("discovery", description="Mode: discovery or enrichment")
    city: str | None = Field(None, description="Optional city filter")
    naf_code: str | None = Field(None, description="Optional exact NAF code, e.g. '49.41A'")
    strategy: str = Field("sirene", description="Discovery strategy: 'sirene' (DB-first) or 'maps' (Maps-first)")
    search_queries: list[str] | None = Field(None, description="Maps-first: list of search terms")




def _build_batch_id(sector: str, dept: str) -> str:
    """Generate a unique batch_id like TRANSPORT_66_BATCH_003."""
    base = f"{sector.upper().replace(' ', '_')}_{dept}"
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

    # Build base_id for batch numbering
    base_id = _build_batch_id(sector, dept)

    # Build filters_json from optional fields
    filters_dict = {}
    if body.naf_code:
        filters_dict["naf_code"] = body.naf_code.strip()
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
            # Lock existing rows for this base_id to prevent concurrent duplicates
            rows = await conn.execute(
                "SELECT batch_id FROM batch_data WHERE batch_id LIKE %s ORDER BY batch_id DESC FOR UPDATE",
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

            # Calculate batch_offset for discovery mode
            batch_offset = 0
            if body.mode == "discovery":
                count_row = await (await conn.execute(
                    "SELECT SUM(COALESCE(batch_size, 0)) AS total FROM batch_data WHERE UPPER(batch_name) = %s AND status != 'deleted'",
                    (batch_name.upper(),),
                )).fetchone()
                if count_row and count_row[0]:
                    batch_offset = count_row[0]

            # Get user_id from authenticated session (if present)
            user_id = getattr(request.state, 'user', None)
            user_id = user_id.id if user_id else None

            from fortress.config.settings import settings as _settings
            worker_id = _settings.effective_worker_id

            await conn.execute(
                """INSERT INTO batch_data
                   (batch_id, batch_name, status, batch_number, batch_offset, total_companies, batch_size, filters_json, user_id, worker_id, strategy, search_queries)
                   VALUES (%s, %s, 'queued', %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (batch_id, batch_name, batch_number, batch_offset, body.size, body.size, filters_json, user_id, worker_id, body.strategy, search_queries_json),
            )
            await conn.commit()
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"error": f"Database insert failed: {exc}", "batch_id": batch_id},
        )

    # Spawn the runner as a detached subprocess with stderr logging
    fortress_root = Path(__file__).resolve().parent.parent.parent  # fortress/
    log_dir = fortress_root / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{batch_id}.log"

    # Choose the correct runner based on strategy
    if body.strategy == "maps":
        runner_module = "fortress.maps_discovery_runner"
    else:
        runner_module = "fortress.runner"

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
        details=f"Recherche {sector} {dept} — {body.size} entreprises",
    )

    return {
        "batch_id": batch_id,
        "batch_name": batch_name,
        "batch_number": batch_number,
        "batch_offset": batch_offset,
        "size": body.size,
        "mode": body.mode,
        "pid": process.pid,
        "status": "launched",
        "message": f"Batch {batch_id} launched (PID {process.pid}). Monitor at /api/jobs/{batch_id}",
    }



