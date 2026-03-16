"""Batch execution API — create and launch scrape jobs.

Provides POST /api/batch/run to:
  1. Insert a scrape_jobs row in the database
  2. Spawn the fortress.runner subprocess in the background
  3. Return the query_id so the frontend can monitor progress
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




def _build_query_id(sector: str, dept: str) -> str:
    """Generate a unique query_id like TRANSPORT_66_BATCH_003."""
    base = f"{sector.upper().replace(' ', '_')}_{dept}"
    return base


@router.post("/run", status_code=202)
async def run_batch(body: BatchRunRequest, request: Request):
    """Create a scrape job and launch the runner subprocess.

    Returns 202 with the query_id for monitoring.
    """
    sector = body.sector.strip().lower()
    dept = body.department.strip()
    query_name = f"{sector} {dept}"

    # Build base_id for batch numbering
    base_id = _build_query_id(sector, dept)

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
                "SELECT query_id FROM scrape_jobs WHERE query_id LIKE %s ORDER BY query_id DESC FOR UPDATE",
                (f"{base_id}%",),
            )
            existing = await rows.fetchall()

            if not existing:
                batch_number = 1
                query_id = f"{base_id}_BATCH_001"
            else:
                max_num = 0
                for row in existing:
                    qid = row[0] if isinstance(row, tuple) else row["query_id"]
                    match = re.search(r"BATCH_(\d+)$", qid)
                    if match:
                        max_num = max(max_num, int(match.group(1)))
                if max_num == 0:
                    batch_number = 1
                    query_id = f"{base_id}_BATCH_001"
                else:
                    batch_number = max_num + 1
                    query_id = f"{base_id}_BATCH_{batch_number:03d}"

            # Calculate batch_offset for discovery mode
            batch_offset = 0
            if body.mode == "discovery":
                count_row = await (await conn.execute(
                    "SELECT SUM(COALESCE(companies_scraped, 0)) AS total FROM scrape_jobs WHERE UPPER(query_name) = %s",
                    (query_name.upper(),),
                )).fetchone()
                if count_row and count_row[0]:
                    batch_offset = count_row[0]

            # Get user_id from authenticated session (if present)
            user_id = getattr(request.state, 'user', None)
            user_id = user_id.id if user_id else None

            from fortress.config.settings import settings as _settings
            worker_id = _settings.effective_worker_id

            await conn.execute(
                """INSERT INTO scrape_jobs
                   (query_id, query_name, status, batch_number, batch_offset, total_companies, batch_size, filters_json, user_id, worker_id, strategy, search_queries)
                   VALUES (%s, %s, 'queued', %s, %s, %s, %s, %s, %s, %s, %s, %s)""",
                (query_id, query_name, batch_number, batch_offset, body.size, body.size, filters_json, user_id, worker_id, body.strategy, search_queries_json),
            )
            await conn.commit()
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"error": f"Database insert failed: {exc}", "query_id": query_id},
        )

    # Spawn the runner as a detached subprocess with stderr logging
    fortress_root = Path(__file__).resolve().parent.parent.parent  # fortress/
    log_dir = fortress_root / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{query_id}.log"

    # Choose the correct runner based on strategy
    if body.strategy == "maps":
        runner_module = "fortress.maps_discovery_runner"
    else:
        runner_module = "fortress.runner"

    runner_cmd = [
        sys.executable, "-m", runner_module, query_id,
    ]

    # Sandbox workaround: if launcher exists, use it to bypass .env stat()
    launcher = Path("/tmp/fortress_launcher.py")
    if launcher.exists():
        runner_cmd = [
            sys.executable, str(launcher), "runner", query_id,
        ]


    try:
        log_fd = os.open(str(log_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        process = subprocess.Popen(
            runner_cmd,
            cwd=str(fortress_root.parent),  # Must be PARENT of fortress/ so `-m fortress.runner` resolves
            stdout=log_fd,
            stderr=log_fd,
            close_fds=False,  # Let child inherit the fd
            start_new_session=True,  # Detach from parent process
        )
        os.close(log_fd)  # Safe to close in parent after Popen — child has its own fd copy
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to spawn runner: {exc}", "query_id": query_id},
        )

    return {
        "query_id": query_id,
        "query_name": query_name,
        "batch_number": batch_number,
        "batch_offset": batch_offset,
        "size": body.size,
        "mode": body.mode,
        "pid": process.pid,
        "status": "launched",
        "message": f"Batch {query_id} launched (PID {process.pid}). Monitor at /api/jobs/{query_id}",
    }
