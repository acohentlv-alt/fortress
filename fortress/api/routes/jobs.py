"""Jobs API routes — job-based views, delete, cancel, and company listing."""

from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from fortress.api.db import fetch_all, fetch_one, get_conn
from fortress.api.routes.activity import log_activity

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
            sj.workspace_id
        FROM batch_data sj
        WHERE sj.status != 'deleted'
        {ws_filter}
    """

    rows = await fetch_all(
        base_query + " ORDER BY sj.updated_at DESC",
        tuple(ws_params) if ws_params else None,
    )

    # Watchdog: auto-complete orphaned batches (in_progress but idle >10 min)
    orphaned = [
        r["batch_id"] for r in rows
        if r.get("status") == "in_progress"
        and r.get("updated_at")
        and (__import__("datetime").datetime.now(
            __import__("datetime").timezone.utc
        ) - r["updated_at"]).total_seconds() > 600
    ]
    if orphaned:
        try:
            async with get_conn() as conn:
                for bid in orphaned:
                    await conn.execute(
                        """UPDATE batch_data SET status = 'completed',
                           shortfall_reason = COALESCE(shortfall_reason,
                               'Le processus pipeline s''est arrêté. Relancez le batch pour continuer.'),
                           updated_at = NOW()
                           WHERE batch_id = %s AND status = 'in_progress'""",
                        (bid,),
                    )
                await conn.commit()
            # Update in-memory rows
            for r in rows:
                if r["batch_id"] in orphaned:
                    r["status"] = "completed"
        except Exception:
            pass  # Non-fatal

    return rows


@router.delete("/{batch_id}")
async def delete_job(batch_id: str, request: Request):
    """Soft-delete a batch. Removes batch_tags but preserves company/contact data. Admin only."""
    user = getattr(request.state, 'user', None)
    if not user or user.role != 'admin':
        return JSONResponse(status_code=403, content={"error": "Admin uniquement"})

    async with get_conn() as conn:
        row = await (await conn.execute(
            """SELECT status, batch_name, companies_scraped, batch_size, updated_at,
                      EXTRACT(EPOCH FROM (NOW() - updated_at)) AS idle_seconds,
                      COALESCE(companies_qualified, 0) AS companies_qualified
               FROM batch_data WHERE batch_id = %s""",
            (batch_id,),
        )).fetchone()

        if not row:
            return JSONResponse(status_code=404, content={"error": "Job introuvable"})
        
        raw_status = row[0]
        idle_seconds = row[5] or 0
        is_stale = idle_seconds > 600
        if raw_status == "in_progress" and not is_stale:
            return JSONResponse(status_code=409, content={"error": "Arrêtez le batch d'abord"})

        batch_name = row[1]

        # ── Hard delete: contacts, audit, tags, job ──────────────────
        # 1. Get all SIRENs that belong to THIS batch (via batch_log)
        batch_sirens = await (await conn.execute(
            "SELECT DISTINCT siren FROM batch_log WHERE batch_id = %s",
            (batch_id,),
        )).fetchall()
        siren_list = [r[0] for r in batch_sirens] if batch_sirens else []

        deleted_contacts = 0
        if siren_list:
            # 2. Delete contacts for these SIRENs (only source='website_crawl' or 'google_maps')
            #    Keep 'upload' source contacts (user-uploaded data)
            result = await conn.execute(
                "DELETE FROM contacts WHERE siren = ANY(%s) AND source NOT IN ('upload', 'client_upload')",
                (siren_list,),
            )
            deleted_contacts = result.rowcount

        # 3. Delete batch_log rows for this batch
        result = await conn.execute(
            "DELETE FROM batch_log WHERE batch_id = %s",
            (batch_id,),
        )
        deleted_audit = result.rowcount

        # 4. Delete enrichment_log rows for this batch
        await conn.execute(
            "DELETE FROM enrichment_log WHERE batch_id = %s",
            (batch_id,),
        )

        # 5. Delete batch_tags for these SIRENs + this batch_name
        if siren_list:
            await conn.execute(
                "DELETE FROM batch_tags WHERE batch_name = %s AND siren = ANY(%s)",
                (batch_name, siren_list),
            )

        # 6. Hard delete the job row itself
        await conn.execute(
            "DELETE FROM batch_data WHERE batch_id = %s",
            (batch_id,),
        )
        await conn.commit()

    await log_activity(
        user_id=getattr(user, 'id', None),
        username=getattr(user, 'username', 'admin'),
        action='delete_job',
        target_type='job',
        target_id=batch_id,
        details=f"Suppression complète du batch {batch_name or batch_id}: {deleted_contacts} contacts, {deleted_audit} audit, {len(siren_list)} entreprises",
    )

    return {
        "deleted": True,
        "batch_id": batch_id,
        "deleted_contacts": deleted_contacts,
        "deleted_audit": deleted_audit,
        "sirens_affected": len(siren_list),
    }


@router.post("/{batch_id}/cancel")
async def cancel_job(batch_id: str, request: Request):
    """Request graceful cancellation of a running batch. Admin only."""
    user = getattr(request.state, 'user', None)
    if not user or user.role != 'admin':
        return JSONResponse(status_code=403, content={"error": "Admin uniquement"})
    async with get_conn() as conn:
        row = await (await conn.execute(
            "SELECT status FROM batch_data WHERE batch_id = %s",
            (batch_id,),
        )).fetchone()

        if not row:
            return JSONResponse(status_code=404, content={"error": "Job introuvable"})
        if row[0] not in ("in_progress", "queued", "triage", "new"):
            return JSONResponse(status_code=409, content={
                "error": "Le batch n'est pas en cours",
                "current_status": row[0],
            })

        await conn.execute(
            "UPDATE batch_data SET cancel_requested = TRUE, updated_at = NOW() WHERE batch_id = %s",
            (batch_id,),
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
async def get_job(batch_id: str):
    """Single job detail with progress info."""
    job = await fetch_one("""
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
        WHERE sj.batch_id = %s
    """, (batch_id,))
    if not job:
        return JSONResponse(status_code=404, content={"error": "Job not found"})

    # Watchdog: if batch is in_progress but no update in 10+ minutes, mark as completed.
    # The worker process likely crashed (Render restart, OOM, etc.).
    idle = job.get("idle_seconds") or 0
    if job["status"] == "in_progress" and idle > 600:
        scraped = job.get("companies_scraped") or 0
        batch_size = job.get("batch_size") or 0
        shortfall = (
            f"Le processus pipeline s'est arrêté après {scraped} entreprises "
            f"(objectif : {batch_size}). Relancez le batch pour continuer."
        ) if scraped < batch_size else None
        try:
            async with get_conn() as conn:
                await conn.execute(
                    """UPDATE batch_data SET status = 'completed',
                       shortfall_reason = COALESCE(shortfall_reason, %s),
                       updated_at = NOW()
                       WHERE batch_id = %s AND status = 'in_progress'""",
                    (shortfall, batch_id),
                )
                await conn.commit()
            job = {**job, "status": "completed", "shortfall_reason": shortfall}
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


@router.get("/{batch_id}/companies")
async def get_job_companies(
    batch_id: str,
    page: int = Query(1, ge=1),
    page_size: int = Query(20, ge=1, le=100),
    search: str = Query("", description="Filter by name, city, or SIREN"),
    sort: str = Query("completude", description="Sort by: completude | name | date"),
):
    """Paginated companies for a job with merged contact data."""
    # Get the batch_name from the job
    job = await fetch_one(
        "SELECT batch_name FROM batch_data WHERE batch_id = %s", (batch_id,)
    )
    if not job:
        return {"companies": [], "total": 0, "page": page}

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

    # Fetch companies with best contact per SIREN — scoped to this batch.
    # Uses DISTINCT ON instead of LATERAL JOIN to avoid O(N) nested sorts.
    # DISTINCT ON (siren) picks one row per siren, ordered by completeness.
    params_fetch = search_params + [page_size, offset]
    rows = await fetch_all(f"""
        WITH audit_query AS (
            SELECT DISTINCT ON (siren) siren, search_query
            FROM batch_log
            WHERE batch_id = %s
            ORDER BY siren, timestamp ASC
        ),
        best_contact AS (
            SELECT DISTINCT ON (c2.siren)
                c2.siren,
                c2.phone, c2.email, c2.email_type, c2.website,
                c2.social_linkedin, c2.social_facebook, c2.social_twitter,
                c2.rating, c2.review_count, c2.maps_url, c2.source AS contact_source
            FROM contacts c2
            WHERE c2.siren IN (SELECT siren FROM audit_query)
            ORDER BY c2.siren,
                (CASE WHEN c2.phone IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN c2.email IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN c2.website IS NOT NULL THEN 1 ELSE 0 END) DESC
        )
        SELECT
            co.siren, co.denomination, co.naf_code, co.naf_libelle,
            co.forme_juridique, co.adresse, co.code_postal, co.ville,
            co.departement, co.region, co.statut, co.date_creation,
            co.tranche_effectif, co.fortress_id,
            co.linked_siren, co.link_confidence, co.link_method,
            bc.phone, bc.email, bc.email_type, bc.website,
            bc.social_linkedin, bc.social_facebook, bc.social_twitter,
            bc.rating, bc.review_count, bc.maps_url, bc.contact_source,
            CASE WHEN bc.phone IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN bc.email IS NOT NULL THEN 1 ELSE 0 END +
            CASE WHEN bc.website IS NOT NULL THEN 1 ELSE 0 END AS completude,
            aq.search_query
        FROM audit_query aq
        JOIN companies co ON co.siren = aq.siren
        LEFT JOIN best_contact bc ON bc.siren = co.siren
        WHERE 1=1 {where_extra}
        ORDER BY {sort_clause}
        LIMIT %s OFFSET %s
    """, tuple([qid] + params_fetch))

    return {"companies": rows, "total": total, "page": page, "page_size": page_size}


@router.get("/{batch_id}/quality")
async def get_job_quality(batch_id: str):
    """Data quality breakdown scoped to THIS specific batch only.

    Uses batch_log (which has batch_id per company) instead of batch_tags
    (which shares batch_name across batches) so each batch shows its own stats.
    """
    job = await fetch_one(
        "SELECT batch_id FROM batch_data WHERE batch_id = %s", (batch_id,)
    )
    if not job:
        return JSONResponse(status_code=404, content={"error": "Job not found"})

    stats = await fetch_one("""
        WITH batch_sirens AS (
            SELECT DISTINCT siren FROM batch_log WHERE batch_id = %s
        ),
        best_contact AS (
            SELECT DISTINCT ON (c.siren)
                c.siren, c.phone, c.email, c.website,
                c.social_linkedin, c.social_facebook
            FROM contacts c
            WHERE c.siren IN (SELECT siren FROM batch_sirens)
            ORDER BY c.siren,
                (CASE WHEN c.phone IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN c.email IS NOT NULL THEN 1 ELSE 0 END +
                 CASE WHEN c.website IS NOT NULL THEN 1 ELSE 0 END) DESC
        )
        SELECT
            COUNT(DISTINCT co.siren) AS total,
            COUNT(DISTINCT CASE WHEN bc.phone IS NOT NULL THEN co.siren END) AS with_phone,
            COUNT(DISTINCT CASE WHEN bc.email IS NOT NULL THEN co.siren END) AS with_email,
            COUNT(DISTINCT CASE WHEN bc.website IS NOT NULL THEN co.siren END) AS with_website,
            COUNT(DISTINCT CASE WHEN (bc.social_linkedin IS NOT NULL OR bc.social_facebook IS NOT NULL) THEN co.siren END) AS with_social
        FROM batch_sirens sa
        JOIN companies co ON co.siren = sa.siren
        LEFT JOIN best_contact bc ON bc.siren = co.siren
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
