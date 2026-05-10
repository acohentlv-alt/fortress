"""Jobs API routes — job-based views, delete, cancel, and company listing."""

import json as _json

from fastapi import APIRouter, HTTPException, Query, Request
from fastapi.responses import JSONResponse

from fortress.api.db import fetch_all, fetch_one, get_conn
from fortress.api.routes.activity import log_activity
from fortress.api.routes.dashboard import _invalidate_cache
from fortress.api.sql_helpers import merged_contacts_cte
router = APIRouter(prefix="/api/jobs", tags=["jobs"])


async def _load_batch_filter_context(
    batch_id: str,
    ws_filter: str = "",
    ws_params: tuple = (),
    dept_override: str = "",
):
    """Load filters_json + strict_naf from batch_data and derive SQL filter snippets.

    Returns (strict_filter_sql, dept_filter_sql, dept_param_tuple, strict_naf_bool).
    Raises HTTPException(404) if batch not found.

    Added Brief 06 (2026-05-09): used by /summary, /quality, /companies to apply
    the same strict + dept filter that GET /jobs/{id} applies to hero tiles.

    2026-05-10 hotfix: dept_override="ALL" suppresses the dept filter entirely
    so the "Voir tout (mode multi-départements)" cross-dept expander on the Job
    page can show entities outside the batch's stored dept filter.
    """
    # NOTE: alias `batch_data AS sj` because callers pass ws_filter containing
    # `AND sj.workspace_id = %s` (their own SELECTs use `sj` as the alias).
    # Without the alias here, psycopg raises "missing FROM-clause entry for sj".
    bd_row = await fetch_one(
        f"SELECT sj.filters_json, COALESCE(sj.strict_naf, FALSE) AS strict_naf "
        f"FROM batch_data sj WHERE sj.batch_id = %s {ws_filter}",
        (batch_id,) + ws_params,
    )
    if not bd_row:
        raise HTTPException(status_code=404, detail="Batch introuvable ou accès refusé")

    filters_json_raw = bd_row.get("filters_json")
    strict_naf = bd_row.get("strict_naf") or False

    try:
        filters = _json.loads(filters_json_raw) if isinstance(filters_json_raw, str) else (filters_json_raw or {})
    except Exception:
        filters = {}

    dept_filter_value = (filters.get("department") or "").strip()
    if dept_filter_value in ("FR", "ALL"):
        dept_filter_value = ""

    # 2026-05-10 hotfix: dept_override="ALL" suppresses the stored dept filter,
    # so the cross-dept "Voir tout" expander can show all-departments view.
    if (dept_override or "").strip().upper() == "ALL":
        dept_filter_value = ""

    # Leading space ensures correct concatenation when callers do
    # `where_extra = ... + _co_strict_filter + _dept_filter` (avoids "trueAND" syntax error).
    _dept_filter = " AND co.departement = %s" if dept_filter_value else ""
    _dept_param: tuple = (dept_filter_value,) if dept_filter_value else ()
    _strict_filter = " AND co.strict_match = true" if strict_naf else ""

    return _strict_filter, _dept_filter, _dept_param, bool(strict_naf)


@router.get("/timing-baseline")
async def get_timing_baseline(request: Request):
    """Return average time per query from recent completed batches, for the duration estimator.

    Workspace-scoped. Falls back to 18 min/query if fewer than 3 samples.
    """
    user = getattr(request.state, "user", None)
    workspace_id = user.workspace_id if user else None

    row = await fetch_one(
        """
        SELECT
            AVG(EXTRACT(EPOCH FROM (updated_at - created_at)) / 60.0 /
                GREATEST(1, jsonb_array_length(search_queries))) AS avg_min_per_query,
            COUNT(*) AS sample_size
        FROM batch_data
        WHERE workspace_id = %s
          AND status = 'completed'
          AND search_queries IS NOT NULL
          AND total_companies > 5
          AND created_at > NOW() - INTERVAL '14 days'
        """,
        (workspace_id,),
    )

    sample_size = int(row["sample_size"] or 0) if row else 0
    if sample_size < 3:
        return {"avg_min_per_query": 18.0, "sample_size": 0, "fallback": True}

    avg = float(row["avg_min_per_query"] or 18.0)
    return {"avg_min_per_query": avg, "sample_size": sample_size, "fallback": False}


@router.get("")
async def list_jobs(request: Request):
    """All jobs with status and progress — scoped by workspace."""
    user = getattr(request.state, "user", None)

    if user and not user.is_admin:
        ws_filter = "AND sj.workspace_id = %s"
        ws_params: list = [user.workspace_id]
    else:
        ws_filter = ""
        ws_params = []

    base_query = f"""
        SELECT
            sj.batch_id, sj.batch_name,
            sj.status AS status,
            sj.total_companies, sj.companies_scraped, sj.companies_failed,
            sj.triage_black, COALESCE(sj.triage_blue, 0) AS triage_blue, sj.triage_green, sj.triage_yellow, sj.triage_red,
            sj.wave_current, sj.wave_total,
            sj.batch_number, sj.created_at, sj.updated_at,
            COALESCE(sj.batch_size, sj.total_companies) AS batch_size,
            COALESCE(sj.replaced_count, 0) AS replaced_count,
            COALESCE(sj.companies_qualified, 0) AS companies_qualified,
            sj.filters_json,
            UPPER(SPLIT_PART(sj.batch_name, ' ', 1)) AS sector,
            sj.user_id,
            sj.worker_id,
            sj.mode,
            sj.workspace_id,
            sj.shortfall_reason,
            sj.current_query,
            COALESCE((
                SELECT COUNT(DISTINCT bt.siren)
                FROM batch_tags bt
                JOIN companies co ON co.siren = bt.siren
                WHERE bt.batch_id = sj.batch_id
                  AND (NOT COALESCE(sj.strict_naf, FALSE) OR co.strict_match = true)
            ), 0) AS batch_unique_companies
        FROM batch_data sj
        WHERE sj.status != 'deleted'
        {ws_filter}
    """

    rows = await fetch_all(
        base_query + " ORDER BY sj.updated_at DESC",
        tuple(ws_params) if ws_params else None,
    )

    # Parse exhaustive flag from filters_json (backend-computed — single source of truth)
    # Also compute exhaustive_default: True when batch was created under Apr-17+
    # exhaustive-default regime (batch_size hardcoded to 2000). Frontend uses this
    # to hide the now-meaningless "Mode exhaustif" legacy badge.
    import json as _json
    for r in rows:
        fj = r.get("filters_json")
        if fj:
            try:
                parsed = _json.loads(fj) if isinstance(fj, str) else fj
                r["exhaustive"] = bool(parsed.get("exhaustive", False))
            except Exception:
                r["exhaustive"] = False
        else:
            r["exhaustive"] = False
        r["exhaustive_default"] = bool((r.get("batch_size") or 0) >= 2000)

    # Watchdog: auto-resolve orphaned batches (in_progress but idle >10 min)
    # completed if scraped >= batch_size, interrupted if scraped < batch_size
    import datetime as _dt
    orphaned_rows = [
        r for r in rows
        if r.get("status") == "in_progress"
        and r.get("updated_at")
        and (_dt.datetime.now(_dt.timezone.utc) - r["updated_at"]).total_seconds() > 600
    ]
    if orphaned_rows:
        try:
            async with get_conn() as conn:
                for r in orphaned_rows:
                    bid = r["batch_id"]
                    scraped = r.get("companies_scraped") or 0
                    batch_size = r.get("batch_size") or 0
                    if scraped >= batch_size and batch_size > 0:
                        new_status = "completed"
                        reason = None
                    else:
                        new_status = "interrupted"
                        reason = (
                            f"Le processus pipeline s'est arrêté de manière inattendue "
                            f"après {scraped} entreprises sur {batch_size} demandées. "
                            f"Relancez le batch pour continuer."
                        )
                    await conn.execute(
                        """UPDATE batch_data SET status = %s,
                           shortfall_reason = COALESCE(shortfall_reason, %s),
                           updated_at = NOW()
                           WHERE batch_id = %s AND status = 'in_progress'""",
                        (new_status, reason, bid),
                    )
                await conn.commit()
            # Update in-memory rows
            orphaned_ids = {r["batch_id"] for r in orphaned_rows}
            for r in rows:
                if r["batch_id"] in orphaned_ids:
                    scraped = r.get("companies_scraped") or 0
                    batch_size = r.get("batch_size") or 0
                    if scraped >= batch_size and batch_size > 0:
                        r["status"] = "completed"
                    else:
                        r["status"] = "interrupted"
        except Exception:
            pass  # Non-fatal

    return rows


@router.delete("/{batch_id}")
async def delete_job(batch_id: str, request: Request):
    """Soft-delete a batch. Removes batch_tags but preserves company/contact data.
    Admin can delete any batch in any workspace. Head and regular users can only
    delete batches in their own workspace.
    """
    user = getattr(request.state, 'user', None)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentification requise"})

    # Determine workspace scope for this user
    if user.is_admin:
        ws_scope = ""
        ws_params: tuple = ()
    else:
        ws_scope = "AND workspace_id = %s"
        ws_params = (user.workspace_id,)

    async with get_conn() as conn:
        row = await (await conn.execute(
            f"""SELECT status, batch_name, workspace_id,
                      EXTRACT(EPOCH FROM (NOW() - updated_at)) AS idle_seconds
               FROM batch_data WHERE batch_id = %s {ws_scope}""",
            (batch_id,) + ws_params,
        )).fetchone()

        if not row:
            # Could be not found OR belongs to a different workspace
            return JSONResponse(status_code=404, content={"error": "Job introuvable ou accès refusé"})

        # ── Cross-workspace typed-confirmation gate ──────────────────────────
        # Admin's workspace_id is None; non-admin's is the batch owner's id.
        # Guard fires only when an admin targets a batch whose workspace_id
        # is non-NULL AND differs from admin's own (always None for admin).
        # Mental model: NULL workspace_id == admin's own batch == simple confirm;
        # any non-NULL workspace_id == cross-workspace == typed confirm.
        target_ws_id = row[2]
        target_batch_name = row[1]
        if (
            user.is_admin
            and target_ws_id is not None
            and target_ws_id != user.workspace_id  # admin.workspace_id is None
        ):
            try:
                body = await request.json()
            except Exception:
                body = {}
            confirm_name = (body.get("confirm_name") or "").strip() if isinstance(body, dict) else ""
            if confirm_name != (target_batch_name or "").strip():
                return JSONResponse(
                    status_code=422,
                    content={
                        "error": "Confirmation requise — saisissez le nom exact du batch.",
                        "expected_name": target_batch_name,
                    },
                )

        raw_status = row[0]
        idle_seconds = row[3] or 0
        is_stale = idle_seconds > 600
        if raw_status == "in_progress" and not is_stale:
            return JSONResponse(status_code=409, content={"error": "Arrêtez le batch d'abord"})

        batch_name = row[1]

        # ── Step 1: Find all MAPS sirens in this batch ───────────────
        maps_rows = await (await conn.execute(
            "SELECT DISTINCT siren FROM batch_log WHERE batch_id = %s AND siren LIKE 'MAPS%%'",
            (batch_id,),
        )).fetchall()
        maps_sirens_in_batch = {r[0] for r in maps_rows} if maps_rows else set()

        # ── Step 2: Find MAPS sirens shared with OTHER active batches ─
        orphan_sirens: set = set()
        if maps_sirens_in_batch:
            placeholders = ",".join(["%s"] * len(maps_sirens_in_batch))
            shared_rows = await (await conn.execute(
                f"""SELECT DISTINCT siren FROM batch_log
                    WHERE siren IN ({placeholders})
                    AND batch_id != %s
                    AND batch_id IN (
                        SELECT batch_id FROM batch_data WHERE status != 'deleted'
                    )""",
                tuple(maps_sirens_in_batch) + (batch_id,),
            )).fetchall()
            shared_sirens = {r[0] for r in shared_rows} if shared_rows else set()
            orphan_sirens = maps_sirens_in_batch - shared_sirens
        else:
            shared_sirens = set()

        # ── Step 3: Soft-delete batch_data ───────────────────────────
        await conn.execute(
            f"UPDATE batch_data SET status = 'deleted', updated_at = NOW() WHERE batch_id = %s {ws_scope}",
            (batch_id,) + ws_params,
        )

        # ── Step 4: Delete batch_tags for this batch ──────────────────
        deleted_tags_result = await conn.execute(
            f"DELETE FROM batch_tags WHERE batch_id = %s {ws_scope}",
            (batch_id,) + ws_params,
        )
        deleted_tags = deleted_tags_result.rowcount or 0

        # ── Step 5: Delete batch_log for this batch ───────────────────
        await conn.execute("DELETE FROM batch_log WHERE batch_id = %s", (batch_id,))

        # ── Step 6: Delete enrichment_log for this batch ─────────────
        await conn.execute(
            "DELETE FROM enrichment_log WHERE batch_id = %s",
            (batch_id,),
        )

        # ── Step 7: Delete orphan MAPS entities ───────────────────────
        deleted_companies = 0
        if orphan_sirens:
            # Extra safety: only process sirens that actually start with MAPS
            safe_orphans = [s for s in orphan_sirens if s.startswith('MAPS')]
            if safe_orphans:
                siren_placeholders = ",".join(["%s"] * len(safe_orphans))
                siren_tuple = tuple(safe_orphans)
                await conn.execute(
                    f"DELETE FROM company_notes WHERE siren IN ({siren_placeholders})",
                    siren_tuple,
                )
                await conn.execute(
                    f"DELETE FROM officers WHERE siren IN ({siren_placeholders})",
                    siren_tuple,
                )
                await conn.execute(
                    f"DELETE FROM contacts WHERE siren IN ({siren_placeholders})",
                    siren_tuple,
                )
                await conn.execute(
                    f"DELETE FROM batch_tags WHERE siren IN ({siren_placeholders})",
                    siren_tuple,
                )
                del_result = await conn.execute(
                    f"DELETE FROM companies WHERE siren IN ({siren_placeholders})",
                    siren_tuple,
                )
                deleted_companies = del_result.rowcount or 0

        await conn.commit()

    _invalidate_cache()

    maps_sirens_found = len(maps_sirens_in_batch)
    shared_sirens_kept = len(shared_sirens)

    await log_activity(
        user_id=getattr(user, 'id', None),
        username=getattr(user, 'username', 'unknown'),
        action='delete_job',
        target_type='job',
        target_id=batch_id,
        details=(
            f"Suppression du batch {batch_name or batch_id}: "
            f"{deleted_tags} tags supprimés, "
            f"{deleted_companies} entités MAPS supprimées "
            f"({shared_sirens_kept} partagées conservées)"
        ),
    )

    return {
        "deleted": True,
        "batch_id": batch_id,
        "batch_name": batch_name,
        "deleted_tags": deleted_tags,
        "deleted_companies": deleted_companies,
        "maps_sirens_found": maps_sirens_found,
        "shared_sirens_kept": shared_sirens_kept,
    }


@router.post("/{batch_id}/cancel")
async def cancel_job(batch_id: str, request: Request):
    """Request graceful cancellation of a running batch. All authenticated users."""
    user = getattr(request.state, 'user', None)
    if not user:
        return JSONResponse(status_code=403, content={"error": "Accès refusé"})

    if user.is_admin:
        ws_scope = ""
        ws_params = ()
    else:
        ws_scope = "AND workspace_id = %s"
        ws_params = (user.workspace_id,)

    async with get_conn() as conn:
        row = await (await conn.execute(
            f"SELECT status, batch_name, workspace_id FROM batch_data WHERE batch_id = %s {ws_scope}",
            (batch_id,) + ws_params,
        )).fetchone()

        if not row:
            return JSONResponse(status_code=404, content={"error": "Job introuvable ou accès refusé"})

        # ── Cross-workspace typed-confirmation gate ──────────────────────────
        # Mirror of delete_job (jobs.py:158-183): admin's workspace_id is None;
        # any non-NULL target ws is "cross-workspace" → require typed name.
        target_status = row[0]
        target_batch_name = row[1]
        target_ws_id = row[2]
        if (
            user.is_admin
            and target_ws_id is not None
            and target_ws_id != user.workspace_id  # admin.workspace_id is None
        ):
            try:
                body = await request.json()
            except Exception:
                body = {}
            confirm_name = (body.get("confirm_name") or "").strip() if isinstance(body, dict) else ""
            if confirm_name != (target_batch_name or "").strip():
                return JSONResponse(
                    status_code=422,
                    content={
                        "error": "Confirmation requise — saisissez le nom exact du batch.",
                        "expected_name": target_batch_name,
                    },
                )

        if target_status not in ("in_progress", "queued", "triage", "new"):
            return JSONResponse(status_code=409, content={
                "error": "Le batch n'est pas en cours",
                "current_status": target_status,
            })

        await conn.execute(
            f"UPDATE batch_data SET cancel_requested = TRUE, status = 'cancelled', updated_at = NOW() WHERE batch_id = %s {ws_scope}",
            (batch_id,) + ws_params,
        )
        await conn.commit()

    await log_activity(
        user_id=getattr(user, 'id', None),
        username=getattr(user, 'username', 'admin'),
        action='cancel_job',
        target_type='job',
        target_id=batch_id,
        details=f"Annulation du batch {batch_id}",
    )

    return {"cancelled": True, "batch_id": batch_id}


@router.post("/{batch_id}/resume")
async def resume_job(batch_id: str, request: Request):
    """Resume an interrupted batch from its last completed query checkpoint."""
    import subprocess
    import sys
    import os
    from pathlib import Path

    user = getattr(request.state, 'user', None)
    if not user:
        return JSONResponse(status_code=403, content={"error": "Accès refusé"})

    # Admin can resume any workspace's batch (per CLAUDE.md: "admin sees all
    # workspaces, nothing restricted"). Head restricted to their own workspace.
    # The 15-min lock guard below still uses the batch's actual workspace_id,
    # so cross-workspace resume can't bypass concurrency protection.
    if user.is_admin:
        ws_scope = ""
        ws_params: tuple = ()
    else:
        ws_scope = "AND workspace_id = %s"
        ws_params = (user.workspace_id,)

    async with get_conn() as conn:
        # SELECT FOR UPDATE locks the row so two simultaneous clicks can't both pass
        row = await (await conn.execute(
            f"""SELECT status, workspace_id, batch_name, batch_size, companies_scraped
                FROM batch_data
                WHERE batch_id = %s {ws_scope}
                FOR UPDATE""",
            (batch_id,) + ws_params,
        )).fetchone()

        if not row:
            return JSONResponse(status_code=404, content={"error": "Job introuvable ou accès refusé"})

        current_status = row[0]
        row_workspace_id = row[1]
        batch_name = row[2]
        batch_size_row = row[3] or 0
        companies_scraped_row = row[4] or 0

        # ── Cross-workspace typed-confirmation gate ──────────────────────────
        # Mirror of delete_job (jobs.py:158-183) and cancel_job (this PR):
        # admin's workspace_id is None; any non-NULL target ws is "cross-
        # workspace" → require typed name. The SELECT FOR UPDATE row lock
        # above is released by psycopg on JSONResponse return (rollback).
        if (
            user.is_admin
            and row_workspace_id is not None
            and row_workspace_id != user.workspace_id  # admin.workspace_id is None
        ):
            try:
                body = await request.json()
            except Exception:
                body = {}
            confirm_name = (body.get("confirm_name") or "").strip() if isinstance(body, dict) else ""
            if confirm_name != (batch_name or "").strip():
                return JSONResponse(
                    status_code=422,
                    content={
                        "error": "Confirmation requise — saisissez le nom exact du batch.",
                        "expected_name": batch_name,
                    },
                )

        # Size-cap guard: a batch that already reached its target has nothing
        # left to resume even if the watchdog marked it interrupted. Return
        # 409 with a clear French message so direct endpoint hits (stale
        # frontend caches) surface the state instead of silently restarting.
        if batch_size_row > 0 and companies_scraped_row >= batch_size_row:
            return JSONResponse(
                status_code=409,
                content={
                    "error": "Ce batch a déjà atteint sa taille cible — aucune requête à reprendre",
                    "companies_scraped": companies_scraped_row,
                    "batch_size": batch_size_row,
                },
            )

        # Only 'interrupted' is resumable — never 'failed', 'completed', or anything else
        if current_status != 'interrupted':
            return JSONResponse(
                status_code=409,
                content={
                    "error": "Ce batch ne peut pas être repris",
                    "current_status": current_status,
                },
            )

        # Same-workspace batch guard (15 min, excluding self).
        # Test workspaces bypass to allow parallel QA cycles.
        from fortress.config.settings import settings as _settings_jobs
        _skip_resume_guard = (
            row_workspace_id is not None
            and row_workspace_id in _settings_jobs.test_workspace_ids
        )

        if not _skip_resume_guard:
            if row_workspace_id is not None:
                guard_cur = await conn.execute(
                    """SELECT 1 FROM batch_data
                       WHERE workspace_id = %s
                         AND status IN ('queued', 'in_progress')
                         AND batch_id != %s
                         AND updated_at > NOW() - INTERVAL '15 minutes'
                       LIMIT 1""",
                    (row_workspace_id, batch_id),
                )
            else:
                guard_cur = await conn.execute(
                    """SELECT 1 FROM batch_data
                       WHERE workspace_id IS NULL
                         AND status IN ('queued', 'in_progress')
                         AND batch_id != %s
                         AND updated_at > NOW() - INTERVAL '15 minutes'
                       LIMIT 1""",
                    (batch_id,),
                )
            blocking = await guard_cur.fetchone()
            if blocking:
                return JSONResponse(
                    status_code=409,
                    content={"error": "Un batch est déjà en cours dans cet espace de travail. Veuillez attendre qu'il se termine."},
                )

        await conn.execute(
            """UPDATE batch_data
               SET status = 'queued',
                   cancel_requested = FALSE,
                   shortfall_reason = NULL,
                   resume_attempt_count = 0,
                   updated_at = NOW()
               WHERE batch_id = %s""",
            (batch_id,),
        )
        await conn.commit()

    # Spawn the runner subprocess — same pattern as batch.py run_batch
    fortress_root = Path(__file__).resolve().parent.parent.parent.parent
    log_dir = fortress_root / "fortress" / "data" / "logs"
    log_dir.mkdir(parents=True, exist_ok=True)
    log_path = log_dir / f"{batch_id}.log"

    runner_cmd = [sys.executable, "-m", "fortress.discovery", batch_id]
    launcher = Path("/tmp/fortress_launcher.py")
    if launcher.exists():
        runner_cmd = [sys.executable, str(launcher), "runner", batch_id]

    try:
        log_fd = os.open(str(log_path), os.O_WRONLY | os.O_CREAT | os.O_TRUNC)
        os.close(log_fd)
        process = subprocess.Popen(
            runner_cmd,
            cwd=str(fortress_root),
            stdout=None,
            stderr=None,
            close_fds=False,
            start_new_session=True,
        )
    except Exception as exc:
        return JSONResponse(
            status_code=500,
            content={"error": f"Failed to spawn runner: {exc}", "batch_id": batch_id},
        )

    await log_activity(
        user_id=getattr(user, 'id', None),
        username=getattr(user, 'username', 'unknown'),
        action='resume_job',
        target_type='job',
        target_id=batch_id,
        details=f"Reprise du batch {batch_name or batch_id}",
    )

    return {
        "resumed": True,
        "batch_id": batch_id,
        "pid": process.pid,
        "status": "queued",
    }


@router.get("/{batch_id}")
async def get_job(batch_id: str, request: Request, dept: str = Query("", description="2026-05-10 dept override; 'ALL' suppresses the batch's stored dept filter so cross-dept entities are visible")):
    """Single job detail with progress info."""
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        ws_filter = "AND sj.workspace_id = %s"
        ws_params: tuple = (user.workspace_id,)
    else:
        ws_filter = ""
        ws_params = ()

    job = await fetch_one(f"""
        SELECT
            sj.batch_id, sj.batch_name,
            sj.status AS status,
            sj.total_companies, sj.companies_scraped, sj.companies_failed,
            sj.triage_black, COALESCE(sj.triage_blue, 0) AS triage_blue, sj.triage_green, sj.triage_yellow, sj.triage_red,
            sj.wave_current, sj.wave_total,
            sj.batch_number, sj.batch_offset, sj.filters_json, sj.search_queries,
            sj.created_at, sj.updated_at,
            COALESCE(sj.batch_size, sj.total_companies) AS batch_size,
            COALESCE(sj.replaced_count, 0) AS replaced_count,
            COALESCE(sj.companies_qualified, 0) AS companies_qualified,
            sj.mode,
            sj.workspace_id,
            sj.shortfall_reason,
            sj.current_query,
            COALESCE(sj.strict_naf, FALSE) AS strict_naf,
            sj.timing_breakdown,
            sj.time_cap_per_query_min,
            sj.time_cap_total_min,
            sj.entity_cap_confirmed,
            EXTRACT(EPOCH FROM (NOW() - sj.updated_at)) AS idle_seconds,
            EXTRACT(EPOCH FROM (
                (SELECT MAX(timestamp) FROM batch_log WHERE batch_id = sj.batch_id) - sj.created_at
            ))::int AS duration_sec
        FROM batch_data sj
        WHERE sj.batch_id = %s {ws_filter}
    """, (batch_id,) + ws_params)
    if not job:
        return JSONResponse(status_code=404, content={"error": "Job not found"})

    # Parse exhaustive flag from filters_json
    fj = job.get("filters_json")
    if fj:
        try:
            parsed = _json.loads(fj) if isinstance(fj, str) else fj
            job["exhaustive"] = bool(parsed.get("exhaustive", False))
        except Exception:
            job["exhaustive"] = False
    else:
        job["exhaustive"] = False
    # Exhaustive-default regime (Apr 17+): batch_size hardcoded to 2000.
    # Used by the frontend to hide the legacy "Mode exhaustif" badge.
    job["exhaustive_default"] = bool((job.get("batch_size") or 0) >= 2000)

    # Parse picked NAFs from batch_data.filters_json — same logic as discovery.py:1601-1613.
    # Accepts new key `naf_codes` (list) or legacy `naf_code` (scalar).
    filters_raw = job.get("filters_json")
    picked_nafs: list[str] = []
    _job_dept = ""  # Department filter for tile queries (added 2026-05-07); always in scope
    if filters_raw:
        try:
            filters = _json.loads(filters_raw) if isinstance(filters_raw, str) else filters_raw
            raw_list = filters.get("naf_codes")
            if isinstance(raw_list, list):
                picked_nafs = [str(c).strip() for c in raw_list if c and str(c).strip()]
            else:
                legacy = (filters.get("naf_code") or "").strip()
                if legacy:
                    picked_nafs = [legacy]
            # Department filter for tile queries (added 2026-05-07).
            # Aligns dashboard tile counts with export filters.
            _job_dept = (filters.get("department") or "").strip() if filters else ""
            if _job_dept and _job_dept in ("FR", "ALL"):
                _job_dept = ""
        except Exception:
            pass
    job["picked_nafs"] = picked_nafs

    # Watchdog: if batch is in_progress but no update in 10+ minutes, resolve status.
    # completed if scraped >= batch_size, interrupted if scraped < batch_size.
    idle = job.get("idle_seconds") or 0
    if job["status"] == "in_progress" and idle > 600:
        scraped = job.get("companies_scraped") or 0
        batch_size = job.get("batch_size") or 0
        if scraped >= batch_size and batch_size > 0:
            new_status = "completed"
            shortfall = None
        else:
            new_status = "interrupted"
            shortfall = (
                f"Le processus pipeline s'est arrêté de manière inattendue "
                f"après {scraped} entreprises sur {batch_size} demandées. "
                f"Relancez le batch pour continuer."
            )
        try:
            async with get_conn() as conn:
                await conn.execute(
                    """UPDATE batch_data SET status = %s,
                       shortfall_reason = COALESCE(shortfall_reason, %s),
                       updated_at = NOW()
                       WHERE batch_id = %s AND status = 'in_progress'""",
                    (new_status, shortfall, batch_id),
                )
                await conn.commit()
            job = {**job, "status": new_status, "shortfall_reason": shortfall}
        except Exception:
            pass  # Non-fatal — just show stale data

    # Remove internal field from response
    job = {k: v for k, v in job.items() if k != "idle_seconds"}

    # Strict-mode filter: when batch was launched with strict_naf=true, all stat queries
    # must filter to co.strict_match=true so counts/tiles reflect the visible entities only.
    _strict_filter = "AND co.strict_match = true" if job.get("strict_naf") else ""

    # Department filter for tile queries (added 2026-05-07).
    # Aligns tile counts with export filters when batch has a department scope.
    _dept_filter_sql = "AND co.departement = %s" if _job_dept else ""
    _dept_params: tuple = (_job_dept,) if _job_dept else ()

    # Also get departments this job touches.
    # Note: f-string required to apply _strict_filter; use batch_id (not batch_name) so
    # the filter scopes to this specific batch — the existing query keyed on batch_name
    # which would inflate department counts across same-name batches.
    depts = await fetch_all(f"""
        SELECT DISTINCT co.departement, COUNT(DISTINCT co.siren) AS company_count
        FROM batch_tags qt
        JOIN companies co ON co.siren = qt.siren
        WHERE qt.batch_id = %s AND co.departement IS NOT NULL
        {_strict_filter}
        {_dept_filter_sql}
        GROUP BY co.departement
        ORDER BY co.departement
    """, (batch_id, *_dept_params))

    pending_row = await fetch_one(f"""
        SELECT COUNT(DISTINCT co.siren) AS pending_links
        FROM batch_tags bt
        JOIN companies co ON co.siren = bt.siren
        WHERE bt.batch_id = %s AND co.link_confidence = 'pending'
        {_strict_filter}
        {_dept_filter_sql}
    """, (batch_id, *_dept_params))
    pending_links = (pending_row or {}).get("pending_links", 0)

    # Link stats — the two rates that map to the 99% north star goal:
    #   (1) auto-confirm rate = confirmed / total_found
    #   (2) NAF accuracy rate = naf_verified / total_found  (hidden when no NAF filter was set)
    # Plus a state breakdown + per-method breakdown for the [▸ détail] disclosure.
    link_rows = await fetch_all(f"""
        SELECT
            CASE
                WHEN co.siren LIKE 'MAPS%%' AND co.linked_siren IS NULL THEN 'unlinked'
                WHEN co.link_confidence = 'pending' THEN 'pending'
                ELSE 'confirmed'
            END AS state,
            -- Split gemini_judge into two surfaces: regular judge vs Phase 2 promotion.
            -- Phase 2 promotion sets link_method='gemini_judge' AND rescued_by='gemini_promoted'.
            CASE
                WHEN COALESCE(co.link_method, 'native_sirene') = 'gemini_judge'
                     AND co.rescued_by = 'gemini_promoted' THEN 'gemini_promoted'
                ELSE COALESCE(co.link_method, 'native_sirene')
            END AS method,
            COUNT(DISTINCT co.siren) AS n
        FROM batch_tags bt
        JOIN companies co ON co.siren = bt.siren
        WHERE bt.batch_id = %s
        {_strict_filter}
        {_dept_filter_sql}
        GROUP BY state, method
    """, (batch_id, *_dept_params))
    link_stats = {
        "confirmed": 0, "pending": 0, "unlinked": 0,
        "total": 0, "naf_verified": 0, "naf_evaluated": 0,
        "by_method": {},
    }
    for row in link_rows:
        state = row["state"]
        method = row["method"]
        n = int(row["n"] or 0)
        link_stats[state] = link_stats.get(state, 0) + n
        link_stats["total"] += n
        if state == "confirmed" and method != "native_sirene":
            link_stats["by_method"][method] = link_stats["by_method"].get(method, 0) + n

    # NAF accuracy breakdown — how the matched SIRENs align with the demanded NAF code.
    #   naf_verified  = strict prefix OR section-letter OR curated-sibling match (CLAUDE.md line 275)
    #   naf_mismatch  = confirmed despite NAF being out-of-family (Phase A signals, chain, Gemini rescue)
    #   naf_evaluated = verified + mismatch  — non-zero means a NAF filter was applied
    # The "exact / related" split inside naf_verified comes from the batch_log.action:
    #   auto_linked_verified  = strict prefix OR section letter (treated as "exact or sector-same")
    #   auto_linked_expanded  = curated sibling family (treated as "related")
    naf_row = await fetch_one(f"""
        SELECT
            COUNT(DISTINCT co.siren) FILTER (WHERE co.naf_status = 'verified') AS verified,
            COUNT(DISTINCT co.siren) FILTER (WHERE co.naf_status = 'mismatch') AS mismatch,
            COUNT(DISTINCT co.siren) FILTER (
                WHERE co.naf_status = 'mismatch' AND co.link_confidence = 'confirmed'
            ) AS mismatch_confirmed,
            COUNT(DISTINCT co.siren) FILTER (
                WHERE co.naf_status = 'mismatch' AND co.link_confidence = 'pending'
            ) AS mismatch_pending,
            COUNT(DISTINCT co.siren) FILTER (
                WHERE co.naf_status = 'verified' AND co.link_confidence = 'pending'
            ) AS verified_pending,
            COUNT(DISTINCT co.siren) FILTER (WHERE co.naf_status IN ('verified', 'mismatch')) AS evaluated,
            COUNT(DISTINCT co.siren) FILTER (
                WHERE co.naf_status = 'verified'
                  AND (co.link_confidence IS NULL OR co.link_confidence != 'pending')
                  AND NOT (co.siren LIKE 'MAPS%%' AND co.linked_siren IS NULL)
            ) AS verified_clickable,
            COUNT(DISTINCT co.siren) FILTER (
                WHERE co.naf_status = 'mismatch'
                  AND (co.link_confidence IS NULL OR co.link_confidence != 'pending')
            ) AS mismatch_clickable,
            COUNT(DISTINCT co.siren) FILTER (
                WHERE co.link_confidence = 'pending'
            ) AS pending_clickable,
            COUNT(DISTINCT co.siren) FILTER (
                WHERE co.siren LIKE 'MAPS%%' AND co.linked_siren IS NULL
            ) AS unlinked_clickable,
            -- Brief 06 (2026-05-09): total strict-confirmed across ALL depts (ignores _dept_filter).
            -- Scalar subquery so the WHERE dept restriction doesn't affect this count.
            -- Used to compute cross_dept_count = total_strict_all_depts - confirmed_in_dept.
            (SELECT COUNT(DISTINCT bt2.siren)
             FROM batch_tags bt2
             JOIN companies co2 ON co2.siren = bt2.siren
             WHERE bt2.batch_id = %s
               AND co2.link_confidence = 'confirmed'
               AND co2.strict_match = true
            ) AS total_strict_all_depts
        FROM batch_tags bt
        JOIN companies co ON co.siren = bt.siren
        WHERE bt.batch_id = %s
        {_strict_filter}
        {_dept_filter_sql}
    """, (batch_id, batch_id, *_dept_params))
    if naf_row:
        link_stats["naf_verified"] = int(naf_row.get("verified") or 0)
        link_stats["naf_mismatch"] = int(naf_row.get("mismatch") or 0)
        link_stats["naf_mismatch_confirmed"] = int(naf_row.get("mismatch_confirmed") or 0)
        link_stats["naf_mismatch_pending"] = int(naf_row.get("mismatch_pending") or 0)
        link_stats["naf_verified_pending"] = int(naf_row.get("verified_pending") or 0)
        link_stats["naf_evaluated"] = int(naf_row.get("evaluated") or 0)
        # BUG 2 FIX — clickable counts (legend + bar segment use these so legend == click result)
        link_stats["naf_verified_clickable"] = int(naf_row.get("verified_clickable") or 0)
        link_stats["naf_mismatch_clickable"] = int(naf_row.get("mismatch_clickable") or 0)
        link_stats["pending_clickable"] = int(naf_row.get("pending_clickable") or 0)
        link_stats["unlinked_clickable"] = int(naf_row.get("unlinked_clickable") or 0)
        # Brief 06 (2026-05-09): cross-dept expander count (A2=a).
        # total_strict_all_depts is a scalar subquery that ignores the dept WHERE filter.
        # confirmed_in_dept = link_stats["confirmed"] which is already dept-filtered.
        total_strict_all_depts = int(naf_row.get("total_strict_all_depts") or 0)
        confirmed_in_dept = int(link_stats.get("confirmed") or 0)
        link_stats["cross_dept_count"] = max(0, total_strict_all_depts - confirmed_in_dept)
    else:
        link_stats["cross_dept_count"] = 0

    # Split naf_verified into EXACT (strict prefix / section / method-keyed) vs RELATED (curated sibling).
    #
    # Bucket simplification (accepted Apr 29):
    # Method-keyed actions (chain, inpi_agree, mentions_legales, cp_name_disamb,
    # individual_match, geo_proximity, gemini_swap, gemini_rescue) are bucketed
    # as EXACT regardless of whether the SIRENE NAF aligns via strict prefix or
    # curated sibling. This simplification preserves today's behavior on
    # auto_linked_geo_proximity (which had the same property pre-v3) and accepts
    # a small over-count of EXACT on sibling-aligned method-keyed matches.
    # Refinement deferred — re-query _compute_naf_status semantics if ever needed.
    # Only auto_linked_expanded is explicitly in RELATED.
    #
    # Single-row dedupe by SIREN: handles the case where one SIREN has overlapping
    # audit rows (e.g., initial auto_linked_geo_proximity → later auto_linked_gemini_swap).
    # EXACT takes precedence: if a SIREN appears in any EXACT action, it counts
    # as EXACT only — never also as RELATED. Prevents the disclosure summing > naf_verified.
    naf_split = await fetch_one(f"""
        WITH exact_sirens AS (
            SELECT DISTINCT bl.siren
            FROM batch_log bl
            JOIN companies co ON co.siren = bl.siren
            WHERE bl.batch_id = %s
              AND co.naf_status = 'verified'
              {_strict_filter}
              {_dept_filter_sql}
              AND bl.action IN (
                  'auto_linked_verified',
                  'auto_linked_geo_proximity',
                  'auto_linked_inpi_agree',
                  'auto_linked_mentions_legales',
                  'auto_linked_chain',
                  'auto_linked_cp_name_disamb',
                  'auto_linked_individual_match',
                  'auto_linked_gemini_swap',
                  'auto_linked_gemini_rescue',
                  'auto_linked_gemini_promoted',
                  'auto_linked_inpi_validated',
                  'auto_linked_siret_address_naf',
                  'auto_linked_strong_no_filter',
                  'auto_linked_municipal',
                  'auto_linked_mismatch_accepted'
              )
        ),
        related_sirens AS (
            SELECT DISTINCT bl.siren
            FROM batch_log bl
            JOIN companies co ON co.siren = bl.siren
            WHERE bl.batch_id = %s
              AND co.naf_status = 'verified'
              {_strict_filter}
              {_dept_filter_sql}
              AND bl.action = 'auto_linked_expanded'
              AND bl.siren NOT IN (SELECT siren FROM exact_sirens)
        )
        SELECT
            (SELECT COUNT(*) FROM exact_sirens)   AS exact_count,
            (SELECT COUNT(*) FROM related_sirens) AS related_count
    """, (batch_id, *_dept_params, batch_id, *_dept_params))
    link_stats["naf_exact"] = int((naf_split or {}).get("exact_count") or 0)
    link_stats["naf_related"] = int((naf_split or {}).get("related_count") or 0)

    # Per-NAF-code breakdown for the scoreboard legend — counts confirmed matches
    # grouped by the ACTUAL NAF code stored on the matched entity.
    # Confirmed = NOT unlinked (MAPS with no linked_siren) AND NOT pending.
    # NULL-safe: native SIRENE matches have link_confidence IS NULL.
    from fortress.config.naf_codes import get_naf_label  # noqa: E402
    naf_per_code = await fetch_all(f"""
        SELECT co.naf_code, COUNT(DISTINCT co.siren) AS n
        FROM batch_tags bt
        JOIN companies co ON co.siren = bt.siren
        WHERE bt.batch_id = %s
          AND co.naf_code IS NOT NULL
          AND NOT (co.siren LIKE 'MAPS%%' AND co.linked_siren IS NULL)
          AND (co.link_confidence IS NULL OR co.link_confidence != 'pending')
          {_strict_filter}
          {_dept_filter_sql}
        GROUP BY co.naf_code
        ORDER BY n DESC
    """, (batch_id, *_dept_params))

    link_stats["by_naf"] = [
        {
            "code": row["naf_code"],
            "label": get_naf_label(row["naf_code"]) or row["naf_code"],
            "count": int(row["n"] or 0),
            "is_picked": row["naf_code"] in picked_nafs,
        }
        for row in (naf_per_code or [])
    ]

    # ── Queue info (only when batch is waiting) ──
    if job.get("status") == "queued":
        # Find the blocking batch (any in_progress batch)
        blocking = await fetch_one(
            """SELECT batch_id, batch_name, companies_scraped, batch_size,
                      current_query, created_at, updated_at,
                      EXTRACT(EPOCH FROM (NOW() - created_at))/60 as running_min
               FROM batch_data
               WHERE status = 'in_progress'
               ORDER BY created_at ASC LIMIT 1"""
        )

        # Count batches queued before this one
        queued_ahead = await fetch_one(
            "SELECT COUNT(*) as cnt FROM batch_data WHERE status = 'queued' AND created_at < %s AND batch_id != %s",
            (job["created_at"], batch_id),
        )
        position = (queued_ahead["cnt"] if queued_ahead else 0) + 1

        queue_info = {"position": position, "blocking_batch": None, "estimated_wait_minutes": None}

        if blocking:
            scraped = blocking.get("companies_scraped") or 0
            target = blocking.get("batch_size") or 50
            running_min = blocking.get("running_min") or 0

            queue_info["blocking_batch"] = {
                "batch_id": blocking["batch_id"],
                "batch_name": blocking.get("batch_name", ""),
                "progress": scraped,
                "target": target,
                "current_query": blocking.get("current_query"),
                "exhaustive_default": bool((target or 0) >= 2000),
            }

            # Estimate remaining time
            if scraped > 0 and running_min > 0:
                rate = running_min / scraped  # minutes per entity
                remaining = (target - scraped) * rate
                queue_info["estimated_wait_minutes"] = round(remaining)

        job["queue_info"] = queue_info

    return {**job, "departments": depts, "pending_links": pending_links, "link_stats": link_stats}


@router.get("/{batch_id}/summary")
async def get_job_summary(batch_id: str, request: Request, dept: str = Query("", description="2026-05-10 dept override; 'ALL' = cross-dept view")):
    """Batch summary — why results were low, triage breakdown, shortfall reason."""
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        ws_filter = "AND sj.workspace_id = %s"
        ws_params: tuple = (user.workspace_id,)
    else:
        ws_filter = ""
        ws_params = ()

    job = await fetch_one(
        f"""SELECT batch_size, total_companies, companies_scraped, companies_qualified,
                   companies_failed, triage_black, triage_green, triage_yellow, triage_red,
                   shortfall_reason,
                   COALESCE(strict_naf, FALSE) AS strict_naf
            FROM batch_data sj
            WHERE sj.batch_id = %s {ws_filter}""",
        (batch_id,) + ws_params,
    )
    if not job:
        return JSONResponse(status_code=404, content={"error": "Job introuvable ou accès refusé"})

    # Brief 06 (2026-05-09): load dept filter + strict filter consistently with GET /jobs/{id}.
    # Previously this endpoint did NOT load filters_json, causing result_breakdown to be
    # cross-dept (whole batch) while the hero tile was dept-filtered. Bug fixed here.
    # 2026-05-10: dept_override allows ?dept=ALL cross-dept view via Voir tout link.
    _strict_filter, _dept_filter, _dept_param, _strict_naf = await _load_batch_filter_context(
        batch_id, ws_filter, ws_params, dept_override=dept
    )

    # Aggregate batch_log by action + result
    log_rows = await fetch_all(
        "SELECT action, result, COUNT(*) AS cnt FROM batch_log WHERE batch_id = %s GROUP BY action, result",
        (batch_id,),
    )

    # Build lookup: (action, result) -> count
    log_counts: dict = {}
    for row in (log_rows or []):
        log_counts[(row["action"], row["result"])] = row["cnt"]

    def _log(action, result):
        return log_counts.get((action, result), 0)

    # Result breakdown — single source of truth for "x/y discovered, z strict, w pending, n unmatched"
    # Brief 06 (2026-05-09): apply strict + dept filter so these match the hero tile scope.
    # Fix: confirmed_rescued drops {_strict_filter} from its FILTER clause to avoid the
    # self-contradiction "strict_match=false AND strict_match=true" that always returned 0.
    # Semantic: in strict mode, confirmed_rescued=0 by design (no rescue paths run);
    # in wide mode, it counts genuinely rescued entities (strict_match=false).
    breakdown_rows = await fetch_one(
        f"""
        SELECT
          COUNT(DISTINCT bt.siren) FILTER (WHERE TRUE {_strict_filter}) AS discovered,
          COUNT(DISTINCT bt.siren) FILTER (WHERE co.link_confidence = 'confirmed' {_strict_filter}) AS confirmed,
          COUNT(DISTINCT bt.siren) FILTER (WHERE co.link_confidence = 'confirmed' AND co.strict_match = true) AS confirmed_strict,
          COUNT(DISTINCT bt.siren) FILTER (WHERE co.link_confidence = 'confirmed' AND co.strict_match = false) AS confirmed_rescued,
          COUNT(DISTINCT bt.siren) FILTER (WHERE co.link_confidence = 'pending' {_strict_filter}) AS pending_review,
          COUNT(DISTINCT bt.siren) FILTER (WHERE co.siren LIKE 'MAPS%%' AND co.link_confidence IS NULL {_strict_filter}) AS unmatched_maps
        FROM batch_tags bt
        JOIN companies co ON co.siren = bt.siren
        WHERE bt.batch_id = %s {_dept_filter}
        """,
        (batch_id,) + _dept_param,
    )

    # System health — narrow set of TRUE-failure categories.
    # Excluded by design: a2_no_mentions_page (expected: many small businesses
    # have no mentions-légales page), website_crawl no_data (successful crawl, no useful
    # data), gemini_*_skipped/quarantine (intentional rejections, not failures).
    HEALTH_CATEGORIES = {
        "website_unreachable": [("website_crawl", "fail")],
        "maps_no_data": [("maps_lookup", "no_data"), ("maps_lookup", "no_match"), ("maps_lookup", "not_found")],
        "a2_extract_anomaly": [("a2_extract_returned_none", "fail")],
        "entity_error": [("entity_error", "fail")],
    }

    system_health: dict = {"total_errors": 0, "categories": {}}
    for cat_key, action_results in HEALTH_CATEGORIES.items():
        cat_count = sum(_log(a, r) for a, r in action_results)
        system_health["total_errors"] += cat_count
        if cat_count > 0:
            # a2_* actions log siren='A2PENDING' placeholder; extract maps_name from detail.
            # psycopg3 doesn't support row-value `(col1, col2) IN %s` — expand to OR clauses.
            or_clauses = " OR ".join(["(action = %s AND result = %s)"] * len(action_results))
            flat_params = [p for ar in action_results for p in ar]
            sample_rows = await fetch_all(
                f"""SELECT DISTINCT
                     CASE
                       WHEN siren LIKE 'A2%%' OR siren IS NULL THEN
                         NULLIF(split_part(split_part(detail, 'maps_name=', 2), ' | ', 1), '')
                       ELSE siren
                     END AS sample_label
                   FROM batch_log
                   WHERE batch_id = %s AND ({or_clauses})
                   LIMIT 3""",
                (batch_id, *flat_params),
            )
            samples = [r["sample_label"] for r in (sample_rows or []) if r.get("sample_label")]
            system_health["categories"][cat_key] = {
                "count": cat_count,
                "samples": samples,
            }

    # Brief 08: sample anomalous SIRENs for the feedback CTA pre-filled context
    sample_rows = await fetch_all(
        """SELECT DISTINCT siren
           FROM batch_log
           WHERE batch_id = %s
             AND result IN ('fail', 'no_data', 'no_match', 'not_found')
             AND siren IS NOT NULL
             AND siren NOT LIKE 'A2%%'
           LIMIT 5""",
        (batch_id,),
    )
    sample_anomaly_sirens = [r["siren"] for r in (sample_rows or [])]

    target = job.get("batch_size") or job.get("total_companies") or 0
    found = job.get("companies_scraped") or 0
    # Brief 06 (2026-05-09) — Change 2 (C1=b): count entities matching the search criteria
    # (strict + dept filtered) so the chip scope matches the hero tile.
    # In strict mode: count strict_match=true entities in the dept-filtered scope.
    # In wide mode: use companies_qualified (legacy counter, no dept filter — acceptable
    # since wide mode does not apply dept filtering to the confirmed count either).
    if _strict_naf:
        _qrow = await fetch_one(
            f"""SELECT COUNT(DISTINCT bt.siren) AS n
               FROM batch_tags bt
               JOIN companies co ON co.siren = bt.siren
               WHERE bt.batch_id = %s AND co.strict_match = true {_dept_filter}""",
            (batch_id,) + _dept_param,
        )
        qualified = (_qrow or {}).get("n") or 0
    else:
        qualified = job.get("companies_qualified") or 0
    failed = job.get("companies_failed") or 0
    black = job.get("triage_black") or 0
    green = job.get("triage_green") or 0
    yellow = job.get("triage_yellow") or 0
    red = job.get("triage_red") or 0

    # website crawl failures: action=website_crawl, result=fail (or error/timeout)
    website_crawl_failed = (
        _log("website_crawl", "fail")
        + _log("website_crawl", "error")
        + _log("website_crawl", "timeout")
    )

    # no sirene match: action=maps_lookup, result=no_data or no_match
    no_sirene_match = (
        _log("maps_lookup", "no_data")
        + _log("maps_lookup", "no_match")
        + _log("maps_lookup", "not_found")
    )

    return {
        "target": target,
        "found": found,
        "matching_search_count": qualified,
        "failed": failed,
        "triage": {
            "black": black,
            "green": green,
            "yellow": yellow,
            "red": red,
        },
        "breakdown": {
            "already_enriched": green,
            "blacklisted": black,
            "new_processed": yellow + red,
            "website_crawl_failed": website_crawl_failed,
            "no_sirene_match": no_sirene_match,
        },
        "shortfall_reason": job.get("shortfall_reason"),
        "exhaustive_default": bool((job.get("batch_size") or 0) >= 2000),
        "result_breakdown": {
            "discovered": (breakdown_rows or {}).get("discovered") or 0,
            "confirmed": (breakdown_rows or {}).get("confirmed") or 0,
            "confirmed_strict": (breakdown_rows or {}).get("confirmed_strict") or 0,
            "confirmed_rescued": (breakdown_rows or {}).get("confirmed_rescued") or 0,
            "pending_review": (breakdown_rows or {}).get("pending_review") or 0,
            "unmatched_maps": (breakdown_rows or {}).get("unmatched_maps") or 0,
            "strict_naf_active": bool(job.get("strict_naf")),
        },
        "system_health": system_health,
        "sample_anomaly_sirens": sample_anomaly_sirens,
    }


@router.get("/{batch_id}/companies")
async def get_job_companies(
    batch_id: str,
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    search: str = Query("", description="Filter by name, city, or SIREN"),
    sort: str = Query("completude", description="Sort by: completude | name | date"),
    state_filter: str = Query("", description="Filter: naf_confirmed | naf_sibling | pending | unlinked"),
    search_query: str = Query("", description="Filter to entities found by exactly this Maps query"),
    dept: str = Query("", description="2026-05-10 dept override; 'ALL' = cross-dept view"),
):
    """Paginated companies for a job with merged contact data."""
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        ws_filter = "AND sj.workspace_id = %s"
        ws_params: tuple = (user.workspace_id,)
    else:
        ws_filter = ""
        ws_params = ()

    # Get the batch_name + strict_naf from the job, scoped to user's workspace
    job = await fetch_one(
        f"SELECT batch_name, COALESCE(strict_naf, FALSE) AS strict_naf FROM batch_data sj WHERE sj.batch_id = %s {ws_filter}",
        (batch_id,) + ws_params,
    )
    if not job:
        return JSONResponse(status_code=404, content={"error": "Batch introuvable."})

    batch_name = job["batch_name"]
    _co_strict_filter = "AND co.strict_match = true" if job.get("strict_naf") else ""
    qid = batch_id  # Use batch_id for batch-scoped data
    offset = (page - 1) * page_size

    # Brief 06 (2026-05-09) — Change 5: load dept filter so list scope matches hero tile.
    # Previously this endpoint did NOT load filters_json; list showed all depts even when
    # the hero tile was dept-filtered.
    _strict_filter_c, _dept_filter_c, _dept_param_c, _strict_naf_c = await _load_batch_filter_context(
        batch_id, ws_filter, ws_params, dept_override=dept
    )

    # Build WHERE clause for search filter
    where_extra = ""
    search_params: list = []
    if search:
        where_extra = """
            AND (co.denomination ILIKE %s
                 OR co.ville ILIKE %s
                 OR co.siren LIKE %s)
        """
        like = f"%{search}%"
        search_params = [like, like, like]

    # Scoreboard legend filter — derived from link_confidence + linked_siren pattern.
    # Mirrors state classification in link_stats SQL at jobs.py:573-586.
    # NULL-safe: native SIRENE matches have link_confidence IS NULL.
    filter_clause = ""
    if state_filter == "naf_confirmed":
        filter_clause = " AND co.naf_status = 'verified' AND (co.link_confidence IS NULL OR co.link_confidence != 'pending') AND NOT (co.siren LIKE 'MAPS%%' AND co.linked_siren IS NULL)"
    elif state_filter == "naf_sibling":
        filter_clause = " AND co.naf_status = 'mismatch' AND (co.link_confidence IS NULL OR co.link_confidence != 'pending')"
    elif state_filter == "pending":
        filter_clause = " AND co.link_confidence = 'pending'"
    elif state_filter == "unlinked":
        filter_clause = " AND co.siren LIKE 'MAPS%%' AND co.linked_siren IS NULL"
    # Empty/unknown → no extra clause (preserves existing behavior)

    # E4.A drill-down — filter to SIRENs whose batch_log row recorded this exact search_query.
    # batch_log.search_query is populated by log_audit() at processing/dedup.py:282 from
    # _current_search_query in discovery.py.
    sq_clause = ""
    sq_params: list = []
    if search_query:
        sq_clause = """
            AND co.siren IN (
                SELECT DISTINCT siren FROM batch_log
                WHERE batch_id = %s AND search_query = %s
            )
        """
        sq_params = [qid, search_query]

    # Brief 06 (2026-05-09): append dept filter so companies list is scoped to the
    # same department as the hero tile (via filters_json.department).
    where_extra = where_extra + filter_clause + sq_clause + _co_strict_filter + _dept_filter_c

    # Determine sort clause
    sort_clause = {
        "name": "co.denomination ASC",
        "date": "co.date_creation DESC NULLS LAST",
    }.get(sort, "completude DESC")

    # Count total — scoped to this specific batch via batch_tags
    # (batch_log includes A2 candidate-lookup audit rows under candidate
    # SIRENs which would inflate the count; batch_tags = real Maps results.)
    count_row = await fetch_one(f"""
        SELECT COUNT(DISTINCT co.siren) AS total
        FROM batch_tags sa
        JOIN companies co ON co.siren = sa.siren
        WHERE sa.batch_id = %s {where_extra}
    """, tuple([qid] + search_params + sq_params + list(_dept_param_c)))
    total = (count_row or {}).get("total", 0)

    # Fetch companies with merged contact per SIREN — scoped to this batch.
    # Uses merged_contacts CTE (ARRAY_AGG with source priority) to pick the
    # best value per field across all contact rows for a given SIREN.
    # Item C2 (May 4): search_queries CTE adds per-card "Trouvé via" chips.
    rows = await fetch_all(f"""
        WITH batch_sirens AS (
            SELECT DISTINCT siren FROM batch_tags WHERE batch_id = %s
        ),
        {merged_contacts_cte('SELECT siren FROM batch_sirens')},
        search_queries AS (
            -- Item C2 (May 4) -- distinct search queries that found each siren.
            -- maps_lookup fires for every Maps result with search_query populated.
            -- JOIN via batch_tags ensures only real primary matches (not A2 candidate
            -- audit rows logged under candidate SIRENs as siren='A2PENDING').
            SELECT bl.siren, ARRAY_AGG(DISTINCT bl.search_query) AS queries
            FROM batch_log bl
            JOIN batch_tags bt ON bt.batch_id = bl.batch_id AND bt.siren = bl.siren
            WHERE bl.batch_id = %s
              AND bl.search_query IS NOT NULL
              AND bl.action IN (
                  'maps_lookup',
                  'auto_linked_verified',
                  'auto_linked_expanded',
                  'auto_linked_mismatch_accepted',
                  'auto_linked_strong_no_filter',
                  'auto_linked_inpi_agree',
                  'auto_linked_chain',
                  'auto_linked_municipal',
                  'auto_linked_mentions_legales',
                  'auto_linked_cp_name_disamb',
                  'auto_linked_individual_match',
                  'auto_linked_geo_proximity',
                  'auto_linked_gemini_rescue',
                  'auto_linked_gemini_swap',
                  'auto_linked_siret_address_naf',
                  'auto_linked_inpi_validated'
              )
            GROUP BY bl.siren
        )
        SELECT
            co.siren, co.denomination, co.naf_code, co.naf_libelle,
            co.forme_juridique, co.adresse, co.code_postal, co.ville,
            co.departement, co.region, co.statut, co.date_creation,
            co.tranche_effectif, co.fortress_id,
            co.linked_siren, co.link_confidence, co.link_method,
            mc.phone, mc.email, mc.email_type, mc.website,
            mc.social_linkedin, mc.social_facebook, mc.social_twitter,
            mc.rating, mc.review_count, mc.maps_url, mc.contact_source,
            mc.phone_source, mc.email_source, mc.website_source,
            sq.queries AS search_queries,
            CASE WHEN mc.phone IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN mc.email IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN mc.website IS NOT NULL THEN 1 ELSE 0 END AS completude
        FROM batch_sirens bs
        JOIN companies co ON co.siren = bs.siren
        LEFT JOIN merged_contacts mc ON mc.siren = co.siren
        LEFT JOIN search_queries sq ON sq.siren = co.siren
        WHERE 1=1 {where_extra}
        ORDER BY {sort_clause}
        LIMIT %s OFFSET %s
    """, tuple([qid, qid] + search_params + sq_params + list(_dept_param_c) + [page_size, offset]))

    return {"companies": rows, "total": total, "page": page, "page_size": page_size}


@router.get("/{batch_id}/quality")
async def get_job_quality(batch_id: str, request: Request, dept: str = Query("", description="2026-05-10 dept override; 'ALL' = cross-dept view")):
    """Data quality breakdown scoped to THIS specific batch only.

    Uses batch_tags (one row per actual Maps result, keyed by batch_id).
    batch_log would also include A2 candidate-lookup audit rows under
    each candidate's real SIREN — those would inflate stats with rows
    that aren't real Maps results.

    Brief 06 (2026-05-09) — Change 3 (A3=a): apply strict + dept filter so gauges
    match the hero tile scope. Previously gauges showed whole-batch percentages
    while hero tiles showed strict + dept.
    """
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        ws_filter = "AND sj.workspace_id = %s"
        ws_params: tuple = (user.workspace_id,)
    else:
        ws_filter = ""
        ws_params = ()

    job = await fetch_one(
        f"SELECT batch_id FROM batch_data sj WHERE sj.batch_id = %s {ws_filter}",
        (batch_id,) + ws_params,
    )
    if not job:
        return JSONResponse(status_code=404, content={"error": "Job not found"})

    # Load dept + strict filter (same logic as GET /jobs/{id} hero tiles).
    _strict_filter_q, _dept_filter_q, _dept_param_q, _strict_naf_q = await _load_batch_filter_context(
        batch_id, ws_filter, ws_params, dept_override=dept
    )

    stats = await fetch_one(f"""
        WITH batch_sirens AS (
            SELECT DISTINCT bt.siren FROM batch_tags bt
            JOIN companies co ON co.siren = bt.siren
            WHERE bt.batch_id = %s {_strict_filter_q} {_dept_filter_q}
        ),
        {merged_contacts_cte('SELECT siren FROM batch_sirens')}
        SELECT
            COUNT(DISTINCT co.siren) AS total,
            COUNT(DISTINCT CASE WHEN mc.phone IS NOT NULL THEN co.siren END) AS with_phone,
            COUNT(DISTINCT CASE WHEN mc.email IS NOT NULL THEN co.siren END) AS with_email,
            COUNT(DISTINCT CASE WHEN mc.website IS NOT NULL THEN co.siren END) AS with_website,
            COUNT(DISTINCT CASE WHEN (mc.social_linkedin IS NOT NULL OR mc.social_facebook IS NOT NULL) THEN co.siren END) AS with_social
        FROM batch_sirens sa
        JOIN companies co ON co.siren = sa.siren
        LEFT JOIN merged_contacts mc ON mc.siren = co.siren
    """, (batch_id,) + _dept_param_q)

    if not stats or not stats["total"]:
        return {"total": 0, "phone_pct": 0, "email_pct": 0, "website_pct": 0, "siret_pct": 0}

    total = stats["total"]
    # Source breakdown from batch_log (whole-batch — not dept-filtered, measures pipeline steps)
    sources_raw = await fetch_all("""
        SELECT action,
               COUNT(*) FILTER (WHERE result = 'success') AS success,
               COUNT(*) FILTER (WHERE result != 'success') AS other,
               COUNT(*) AS total
        FROM batch_log
        WHERE batch_id = %s
        GROUP BY action
    """, (batch_id,))
    sources = {}
    for s in (sources_raw or []):
        sources[s["action"]] = {
            "success": s["success"],
            "total": s["total"],
            "rate": round(100 * s["success"] / s["total"]) if s["total"] else 0,
        }

    # Count companies with officers and/or financial data (INPI results).
    # For Maps Discovery batches, officers/financials live on the linked real SIREN,
    # not on the MAPS entity — so we follow linked_siren via COALESCE.
    # Apply strict + dept filter consistent with hero tiles.
    inpi_stat = await fetch_one(f"""
        WITH bs AS (
            SELECT DISTINCT bt.siren FROM batch_tags bt
            JOIN companies co ON co.siren = bt.siren
            WHERE bt.batch_id = %s {_strict_filter_q} {_dept_filter_q}
        )
        SELECT
            COUNT(DISTINCT CASE WHEN o.siren IS NOT NULL THEN bs.siren END) AS with_officers,
            COUNT(DISTINCT CASE WHEN real.chiffre_affaires IS NOT NULL THEN bs.siren END) AS with_financials
        FROM bs
        JOIN companies co ON co.siren = bs.siren
        LEFT JOIN companies real ON real.siren = co.linked_siren
        LEFT JOIN officers o ON o.siren = COALESCE(co.linked_siren, co.siren)
    """, (batch_id,) + _dept_param_q)
    with_officers = (inpi_stat or {}).get("with_officers", 0)
    with_financials = (inpi_stat or {}).get("with_financials", 0)

    return {
        "total": total,
        "with_phone": stats["with_phone"],
        "with_email": stats["with_email"],
        "with_website": stats["with_website"],
        "with_social": stats["with_social"],
        "with_officers": with_officers,
        "with_financials": with_financials,
        "phone_pct": round(100 * stats["with_phone"] / total),
        "email_pct": round(100 * stats["with_email"] / total),
        "website_pct": round(100 * stats["with_website"] / total),
        "social_pct": round(100 * stats["with_social"] / total),
        "officers_pct": round(100 * with_officers / total),
        "financials_pct": round(100 * with_financials / total),
        "sources": sources,
    }


@router.get("/{batch_id}/queries")
async def get_job_queries(batch_id: str, request: Request):
    """Return query execution history split into three categories:
    done (queries_json), running (current_query + live count), queued (planned but not started)."""
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        ws_filter = "AND sj.workspace_id = %s"
        ws_params: tuple = (user.workspace_id,)
    else:
        ws_filter = ""
        ws_params = ()

    job = await fetch_one(
        f"""SELECT queries_json, current_query, current_widening_json,
                   time_cap_per_query_min, time_cap_total_min,
                   search_queries, status
            FROM batch_data sj WHERE sj.batch_id = %s {ws_filter}""",
        (batch_id,) + ws_params,
    )
    if not job:
        return JSONResponse(status_code=404, content={"error": "Job introuvable"})

    import json as _json
    queries = job.get("queries_json") or []
    if isinstance(queries, str):
        queries = _json.loads(queries)

    # Planned list (jsonb array of strings written by batch.py:204-209 during INSERT)
    planned = job.get("search_queries") or []
    if isinstance(planned, str):
        try:
            planned = _json.loads(planned)
        except Exception:
            planned = []

    # Done queries: distinct primary names from queries_json (is_expansion=False rows)
    done_set = {
        q.get("query") for q in queries
        if isinstance(q, dict) and not q.get("is_expansion") and q.get("query")
    }

    current_query = job.get("current_query")
    status = job.get("status")
    is_running = status in ("in_progress", "queued", "triage")

    # Live count for running query — only when batch is active and has a current_query
    running_payload = None
    if is_running and current_query and current_query not in done_set:
        cnt_row = await fetch_one(
            """SELECT COUNT(DISTINCT siren) AS n
               FROM batch_log
               WHERE batch_id = %s AND search_query = %s
                 AND siren NOT LIKE 'FILTERED_%%'
                 AND siren NOT LIKE 'DEDUP_%%'
                 AND siren NOT LIKE 'WIDEN_%%'""",
            (batch_id, current_query),
        )
        running_payload = {
            "query": current_query,
            "live_count": int((cnt_row or {}).get("n") or 0),
            "is_expansion": False,
            "primary_query": None,
            "widening_type": None,
            "value": None,
        }

        # If a widening is in flight, override expansion fields from the
        # authoritative current_widening_json column. No heuristic, no race.
        wid_blob = job.get("current_widening_json")
        if wid_blob:
            if isinstance(wid_blob, str):
                try:
                    wid_blob = _json.loads(wid_blob)
                except Exception:
                    wid_blob = None
            if isinstance(wid_blob, dict):
                running_payload["is_expansion"] = True
                running_payload["primary_query"] = wid_blob.get("primary_query")
                running_payload["widening_type"] = wid_blob.get("widening_type")
                running_payload["value"] = wid_blob.get("value")

    # Queued: planned minus done minus running (preserve original order)
    queued: list[str] = []
    for q in planned:
        if not isinstance(q, str) or not q.strip():
            continue
        if q in done_set:
            continue
        if running_payload and q == running_payload["query"]:
            continue
        queued.append(q)

    return {
        "queries": queries,
        "current_query": current_query,
        "time_cap_min": job.get("time_cap_per_query_min"),
        "time_cap_total_min": job.get("time_cap_total_min"),
        "running": running_payload,
        "queued": queued,
    }
