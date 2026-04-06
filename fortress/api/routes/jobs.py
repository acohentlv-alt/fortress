"""Jobs API routes — job-based views, delete, cancel, and company listing."""

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from fortress.api.db import fetch_all, fetch_one, get_conn
from fortress.api.routes.activity import log_activity
from fortress.api.routes.dashboard import _invalidate_cache
from fortress.api.sql_helpers import merged_contacts_cte

router = APIRouter(prefix="/api/jobs", tags=["jobs"])


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
            sj.shortfall_reason
        FROM batch_data sj
        WHERE sj.status != 'deleted'
        {ws_filter}
    """

    rows = await fetch_all(
        base_query + " ORDER BY sj.updated_at DESC",
        tuple(ws_params) if ws_params else None,
    )

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
    Admin can only delete NULL-workspace batches. Head can only delete their workspace batches.
    Regular users get 403.
    """
    user = getattr(request.state, 'user', None)
    if not user:
        return JSONResponse(status_code=401, content={"error": "Authentification requise"})

    # Regular users cannot delete
    if user.role not in ('admin', 'head'):
        return JSONResponse(status_code=403, content={"error": "Accès refusé"})

    # Determine workspace scope for this user
    if user.is_admin:
        ws_scope = "AND workspace_id IS NULL"
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
    """Request graceful cancellation of a running batch. Admin or head."""
    user = getattr(request.state, 'user', None)
    if not user or user.role not in ('admin', 'head'):
        return JSONResponse(status_code=403, content={"error": "Accès refusé"})

    if user.is_admin:
        ws_scope = "AND workspace_id IS NULL"
        ws_params = ()
    else:
        ws_scope = "AND workspace_id = %s"
        ws_params = (user.workspace_id,)

    async with get_conn() as conn:
        row = await (await conn.execute(
            f"SELECT status FROM batch_data WHERE batch_id = %s {ws_scope}",
            (batch_id,) + ws_params,
        )).fetchone()

        if not row:
            return JSONResponse(status_code=404, content={"error": "Job introuvable ou accès refusé"})
        if row[0] not in ("in_progress", "queued", "triage", "new"):
            return JSONResponse(status_code=409, content={
                "error": "Le batch n'est pas en cours",
                "current_status": row[0],
            })

        await conn.execute(
            f"UPDATE batch_data SET cancel_requested = TRUE, updated_at = NOW() WHERE batch_id = %s {ws_scope}",
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



@router.get("/{batch_id}")
async def get_job(batch_id: str, request: Request):
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
            sj.batch_number, sj.batch_offset, sj.filters_json,
            sj.created_at, sj.updated_at,
            COALESCE(sj.batch_size, sj.total_companies) AS batch_size,
            COALESCE(sj.replaced_count, 0) AS replaced_count,
            COALESCE(sj.companies_qualified, 0) AS companies_qualified,
            sj.mode,
            sj.shortfall_reason,
            EXTRACT(EPOCH FROM (NOW() - sj.updated_at)) AS idle_seconds
        FROM batch_data sj
        WHERE sj.batch_id = %s {ws_filter}
    """, (batch_id,) + ws_params)
    if not job:
        return JSONResponse(status_code=404, content={"error": "Job not found"})

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

    # Also get departments this job touches
    depts = await fetch_all("""
        SELECT DISTINCT co.departement, COUNT(DISTINCT co.siren) AS company_count
        FROM batch_tags qt
        JOIN companies co ON co.siren = qt.siren
        WHERE qt.batch_name = %s AND co.departement IS NOT NULL
        GROUP BY co.departement
        ORDER BY co.departement
    """, (job["batch_name"],))

    pending_row = await fetch_one("""
        SELECT COUNT(DISTINCT co.siren) AS pending_links
        FROM batch_log bl
        JOIN companies co ON co.siren = bl.siren
        WHERE bl.batch_id = %s AND co.link_confidence = 'pending'
    """, (batch_id,))
    pending_links = (pending_row or {}).get("pending_links", 0)

    return {**job, "departments": depts, "pending_links": pending_links}


@router.get("/{batch_id}/summary")
async def get_job_summary(batch_id: str, request: Request):
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
                   shortfall_reason
            FROM batch_data sj
            WHERE sj.batch_id = %s {ws_filter}""",
        (batch_id,) + ws_params,
    )
    if not job:
        return JSONResponse(status_code=404, content={"error": "Job introuvable ou accès refusé"})

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

    target = job.get("batch_size") or job.get("total_companies") or 0
    found = job.get("companies_scraped") or 0
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
        "qualified": qualified,
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
    }


@router.get("/{batch_id}/companies")
async def get_job_companies(
    batch_id: str,
    request: Request,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    search: str = Query("", description="Filter by name, city, or SIREN"),
    sort: str = Query("completude", description="Sort by: completude | name | date"),
):
    """Paginated companies for a job with merged contact data."""
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        ws_filter = "AND sj.workspace_id = %s"
        ws_params: tuple = (user.workspace_id,)
    else:
        ws_filter = ""
        ws_params = ()

    # Get the batch_name from the job, scoped to user's workspace
    job = await fetch_one(
        f"SELECT batch_name FROM batch_data sj WHERE sj.batch_id = %s {ws_filter}",
        (batch_id,) + ws_params,
    )
    if not job:
        return JSONResponse(status_code=404, content={"error": "Batch introuvable."})

    batch_name = job["batch_name"]
    qid = batch_id  # Use batch_id for batch-scoped data
    offset = (page - 1) * page_size

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

    # Determine sort clause
    sort_clause = {
        "name": "co.denomination ASC",
        "date": "co.date_creation DESC NULLS LAST",
    }.get(sort, "completude DESC")

    # Count total — scoped to this specific batch via batch_log
    count_row = await fetch_one(f"""
        SELECT COUNT(DISTINCT co.siren) AS total
        FROM batch_log sa
        JOIN companies co ON co.siren = sa.siren
        WHERE sa.batch_id = %s {where_extra}
    """, tuple([qid] + search_params))
    total = (count_row or {}).get("total", 0)

    # Fetch companies with merged contact per SIREN — scoped to this batch.
    # Uses merged_contacts CTE (ARRAY_AGG with source priority) to pick the
    # best value per field across all contact rows for a given SIREN.
    params_fetch = search_params + [page_size, offset]
    rows = await fetch_all(f"""
        WITH audit_query AS (
            SELECT DISTINCT ON (siren) siren, search_query
            FROM batch_log
            WHERE batch_id = %s
            ORDER BY siren, timestamp ASC
        ),
        {merged_contacts_cte('SELECT siren FROM audit_query')}
        SELECT
            co.siren, co.denomination, co.naf_code, co.naf_libelle,
            co.forme_juridique, co.adresse, co.code_postal, co.ville,
            co.departement, co.region, co.statut, co.date_creation,
            co.tranche_effectif, co.fortress_id,
            co.linked_siren, co.link_confidence, co.link_method,
            mc.phone, mc.email, mc.email_type, mc.website,
            mc.social_linkedin, mc.social_facebook, mc.social_twitter,
            mc.rating, mc.review_count, mc.maps_url, mc.contact_source,
            CASE WHEN mc.phone IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN mc.email IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN mc.website IS NOT NULL THEN 1 ELSE 0 END AS completude,
            aq.search_query
        FROM audit_query aq
        JOIN companies co ON co.siren = aq.siren
        LEFT JOIN merged_contacts mc ON mc.siren = co.siren
        WHERE 1=1 {where_extra}
        ORDER BY {sort_clause}
        LIMIT %s OFFSET %s
    """, tuple([qid] + params_fetch))

    return {"companies": rows, "total": total, "page": page, "page_size": page_size}


@router.get("/{batch_id}/quality")
async def get_job_quality(batch_id: str, request: Request):
    """Data quality breakdown scoped to THIS specific batch only.

    Uses batch_log (which has batch_id per company) instead of batch_tags
    (which shares batch_name across batches) so each batch shows its own stats.
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

    stats = await fetch_one(f"""
        WITH batch_sirens AS (
            SELECT DISTINCT siren FROM batch_log WHERE batch_id = %s
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
    """, (batch_id,))

    if not stats or not stats["total"]:
        return {"total": 0, "phone_pct": 0, "email_pct": 0, "website_pct": 0, "siret_pct": 0}

    total = stats["total"]
    # Source breakdown from batch_log
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
    inpi_stat = await fetch_one("""
        WITH bs AS (SELECT DISTINCT siren FROM batch_log WHERE batch_id = %s)
        SELECT
            COUNT(DISTINCT CASE WHEN o.siren IS NOT NULL THEN bs.siren END) AS with_officers,
            COUNT(DISTINCT CASE WHEN real.chiffre_affaires IS NOT NULL THEN bs.siren END) AS with_financials
        FROM bs
        JOIN companies co ON co.siren = bs.siren
        LEFT JOIN companies real ON real.siren = co.linked_siren
        LEFT JOIN officers o ON o.siren = COALESCE(co.linked_siren, co.siren)
    """, (batch_id,))
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
