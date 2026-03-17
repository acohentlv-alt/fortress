"""Dashboard API routes — global stats and recent activity.

Admin: sees ALL enriched data (the "data bank" view).
Regular users: stats scoped to their own jobs only.
"""

from fastapi import APIRouter, Request

from fortress.api.db import fetch_all, fetch_one

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/stats")
async def get_stats(request: Request):
    """Dashboard statistics.

    Admin: stats from ALL query_tags (the data bank).
    User: stats scoped to their own jobs' query_names.
    """
    user = getattr(request.state, 'user', None)
    is_admin = user and user.role == 'admin'

    # Build a WHERE clause on query_tags for user scoping
    if is_admin:
        scope_clause = ""
        scope_params: tuple = ()
        jobs_scope = ""
    else:
        user_id = user.id if user else -1
        scope_clause = "WHERE qt.query_name IN (SELECT query_name FROM scrape_jobs WHERE user_id = %s)"
        scope_params = (user_id,)
        jobs_scope = f"WHERE user_id = {user_id}" if user else ""

    stats = await fetch_one(f"""
        WITH tagged AS (
            SELECT DISTINCT qt.siren
            FROM query_tags qt
            {scope_clause}
        ),
        enriched AS (
            SELECT
                t.siren,
                co.departement,
                MAX(ct.phone)   AS phone,
                MAX(ct.email)   AS email,
                MAX(ct.website) AS website
            FROM tagged t
            JOIN companies co ON co.siren = t.siren
            LEFT JOIN contacts ct ON ct.siren = t.siren
            GROUP BY t.siren, co.departement
        )
        SELECT
            COUNT(*)                                           AS total_companies,
            COUNT(*) FILTER (WHERE phone IS NOT NULL)          AS with_phone,
            COUNT(*) FILTER (WHERE email IS NOT NULL)          AS with_email,
            COUNT(*) FILTER (WHERE website IS NOT NULL)        AS with_website,
            COUNT(DISTINCT departement)
                FILTER (WHERE departement IS NOT NULL)         AS departments_covered,
            (SELECT COUNT(*) FROM scrape_jobs {jobs_scope})    AS total_jobs,
            (SELECT COUNT(*) FROM scrape_jobs
             WHERE status = 'completed' {'AND user_id = ' + str(user_id) if not is_admin else ''}) AS completed_jobs,
            (SELECT COUNT(*) FROM scrape_jobs
             WHERE status IN ('in_progress', 'queued', 'triage') AND EXTRACT(EPOCH FROM (NOW() - updated_at)) <= 180 {'AND user_id = ' + str(user_id) if not is_admin else ''}) AS running_jobs
        FROM enriched
    """, scope_params)
    return stats or {}


@router.get("/recent-activity")
async def get_recent_activity(request: Request):
    """Last 10 job updates (user-scoped)."""
    user = getattr(request.state, 'user', None)
    is_admin = user and user.role == 'admin'

    if is_admin:
        rows = await fetch_all("""
            SELECT query_id, query_name, 
                   CASE 
                       WHEN status IN ('in_progress', 'queued', 'triage') AND EXTRACT(EPOCH FROM (NOW() - updated_at)) > 180 THEN 'failed'
                       ELSE status 
                   END AS status,
                   total_companies, companies_scraped, companies_failed,
                   wave_current, wave_total,
                   triage_black, triage_green, triage_yellow, triage_red,
                   created_at, updated_at, worker_id
            FROM scrape_jobs
            ORDER BY updated_at DESC
            LIMIT 10
        """)
    else:
        user_id = user.id if user else -1
        rows = await fetch_all("""
            SELECT query_id, query_name, 
                   CASE 
                       WHEN status IN ('in_progress', 'queued', 'triage') AND EXTRACT(EPOCH FROM (NOW() - updated_at)) > 180 THEN 'failed'
                       ELSE status 
                   END AS status,
                   total_companies, companies_scraped, companies_failed,
                   wave_current, wave_total,
                   triage_black, triage_green, triage_yellow, triage_red,
                   created_at, updated_at
            FROM scrape_jobs
            WHERE user_id = %s
            ORDER BY updated_at DESC
            LIMIT 10
        """, (user_id,))
    return rows


# ---------------------------------------------------------------------------
# Action 3: By-job stats with UPPER() normalization + nested batches
# ---------------------------------------------------------------------------

@router.get("/stats/by-job")
async def get_stats_by_job(request: Request):
    """Job-level stats aggregated by normalized query_name.

    Admin: all jobs. User: own jobs only.
    """
    user = getattr(request.state, 'user', None)
    is_admin = user and user.role == 'admin'
    user_filter = "" if is_admin else f"WHERE sj.user_id = {user.id if user else -1}"

    groups = await fetch_all(f"""
        SELECT
            UPPER(sj.query_name) AS query_name,
            COUNT(*) AS batch_count,
            SUM(COALESCE(sj.companies_scraped, 0)) AS total_scraped,
            SUM(COALESCE(sj.companies_failed, 0)) AS total_failed,
            SUM(COALESCE(sj.triage_green, 0)) AS total_green,
            SUM(COALESCE(sj.triage_yellow, 0)) AS total_yellow,
            SUM(COALESCE(sj.triage_red, 0)) AS total_red,
            SUM(COALESCE(sj.triage_black, 0)) AS total_black,
            MAX(sj.updated_at) AS last_updated
        FROM scrape_jobs sj
        {user_filter}
        GROUP BY UPPER(sj.query_name)
        ORDER BY MAX(sj.updated_at) DESC
    """)

    all_batches = await fetch_all(f"""
        SELECT
            UPPER(sj.query_name) AS group_key,
            sj.query_id, sj.query_name, 
            CASE 
                WHEN sj.status IN ('in_progress', 'queued', 'triage') AND EXTRACT(EPOCH FROM (NOW() - sj.updated_at)) > 180 THEN 'failed'
                ELSE sj.status 
            END AS status,
            sj.batch_number, sj.companies_scraped, sj.companies_failed,
            sj.total_companies, sj.wave_current, sj.wave_total,
            sj.triage_green, sj.triage_yellow, sj.triage_red, sj.triage_black,
            sj.created_at, sj.updated_at
        FROM scrape_jobs sj
        {user_filter}
        ORDER BY sj.created_at DESC
    """)

    batch_map: dict[str, list[dict]] = {}
    for b in all_batches:
        key = b.pop("group_key")
        batch_map.setdefault(key, []).append(b)

    result = []
    for g in groups:
        g["batches"] = batch_map.get(g["query_name"], [])
        result.append(g)

    return result


# ---------------------------------------------------------------------------
# Admin-only: Data Bank global stats
# ---------------------------------------------------------------------------

@router.get("/data-bank")
async def get_data_bank(request: Request):
    """Global enrichment stats — admin only ('the data bank').

    Shows all enriched data across all users/workers.
    """
    user = getattr(request.state, 'user', None)
    if not user or user.role != 'admin':
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=403, content={"error": "Admin uniquement"})

    # Global totals
    totals = await fetch_one("""
        SELECT
            COUNT(DISTINCT qt.siren) AS total_enriched,
            COUNT(DISTINCT ct.siren) FILTER (WHERE ct.phone IS NOT NULL) AS with_phone,
            COUNT(DISTINCT ct.siren) FILTER (WHERE ct.email IS NOT NULL) AS with_email,
            COUNT(DISTINCT ct.siren) FILTER (WHERE ct.website IS NOT NULL) AS with_website,
            COUNT(DISTINCT u.id) AS total_users,
            COUNT(DISTINCT sj.query_id) AS total_batches
        FROM query_tags qt
        LEFT JOIN contacts ct ON ct.siren = qt.siren
        LEFT JOIN scrape_jobs sj ON UPPER(sj.query_name) = UPPER(qt.query_name)
        LEFT JOIN users u ON u.id = sj.user_id
    """)

    # Top 10 sectors
    top_sectors = await fetch_all("""
        SELECT
            UPPER(SPLIT_PART(query_name, ' ', 1)) AS sector,
            COUNT(DISTINCT siren) AS companies
        FROM query_tags
        GROUP BY sector
        ORDER BY companies DESC
        LIMIT 10
    """)

    # Top 10 departments
    top_depts = await fetch_all("""
        SELECT
            co.departement,
            COUNT(DISTINCT qt.siren) AS companies
        FROM query_tags qt
        JOIN companies co ON co.siren = qt.siren
        WHERE co.departement IS NOT NULL
        GROUP BY co.departement
        ORDER BY companies DESC
        LIMIT 10
    """)

    # Active workers (batches per worker_id in last 7 days)
    workers = await fetch_all("""
        SELECT
            COALESCE(worker_id, 'inconnu') AS worker,
            COUNT(*) AS batches,
            MAX(updated_at) AS last_active
        FROM scrape_jobs
        WHERE updated_at > NOW() - INTERVAL '7 days'
        GROUP BY worker_id
        ORDER BY last_active DESC
    """)

    return {
        "totals": totals or {},
        "top_sectors": top_sectors,
        "top_departments": top_depts,
        "workers": workers,
    }


# ---------------------------------------------------------------------------
# By-Sector Stats — accurate unique SIREN counts from query_tags
# ---------------------------------------------------------------------------

@router.get("/stats/by-sector")
async def get_stats_by_sector(request: Request):
    """Sector-level stats using unique SIRENs from query_tags.

    Admin: all data. User: scoped to their own jobs.
    Returns unique company counts per sector so totals match the dashboard.
    """
    user = getattr(request.state, 'user', None)
    is_admin = user and user.role == 'admin'

    if is_admin:
        scope_clause = ""
        scope_params: tuple = ()
    else:
        user_id = user.id if user else -1
        scope_clause = "WHERE qt.query_name IN (SELECT query_name FROM scrape_jobs WHERE user_id = %s)"
        scope_params = (user_id,)

    rows = await fetch_all(f"""
        SELECT
            UPPER(SPLIT_PART(qt.query_name, ' ', 1)) AS sector,
            COUNT(DISTINCT qt.siren) AS companies,
            COUNT(DISTINCT CASE WHEN ct.phone IS NOT NULL THEN qt.siren END) AS with_phone,
            COUNT(DISTINCT CASE WHEN ct.email IS NOT NULL THEN qt.siren END) AS with_email,
            COUNT(DISTINCT CASE WHEN ct.website IS NOT NULL THEN qt.siren END) AS with_website,
            COUNT(DISTINCT SPLIT_PART(qt.query_name, ' ', 2)) AS dept_count,
            (SELECT COUNT(DISTINCT sj2.query_id)
             FROM scrape_jobs sj2
             WHERE UPPER(SPLIT_PART(sj2.query_name, ' ', 1)) = UPPER(SPLIT_PART(qt.query_name, ' ', 1))
               AND sj2.status != 'deleted'
            ) AS batch_count,
            (SELECT BOOL_OR(sj3.status = 'in_progress')
             FROM scrape_jobs sj3
             WHERE UPPER(SPLIT_PART(sj3.query_name, ' ', 1)) = UPPER(SPLIT_PART(qt.query_name, ' ', 1))
            ) AS has_running,
            ARRAY_AGG(DISTINCT SPLIT_PART(qt.query_name, ' ', 2)) FILTER (WHERE SPLIT_PART(qt.query_name, ' ', 2) != '') AS departments
        FROM query_tags qt
        LEFT JOIN contacts ct ON ct.siren = qt.siren
        {scope_clause}
        GROUP BY sector
        ORDER BY companies DESC
    """, scope_params)
    return rows


# ---------------------------------------------------------------------------
# Delete endpoints — remove tags (never delete company/contact data)
# ---------------------------------------------------------------------------

@router.delete("/sector/{sector_name}/tags")
async def delete_sector_tags(sector_name: str, request: Request):
    """Remove all query_tags for a sector. Soft-deletes associated jobs."""
    from fastapi.responses import JSONResponse
    from fortress.api.db import get_conn

    async with get_conn() as conn:
        # Find all query_names matching this sector
        qnames = await conn.execute(
            "SELECT DISTINCT query_name FROM query_tags WHERE UPPER(SPLIT_PART(query_name, ' ', 1)) = UPPER(%s)",
            (sector_name,),
        )
        names = [r[0] for r in await qnames.fetchall()]

        if not names:
            return JSONResponse(status_code=404, content={"error": "Secteur introuvable"})

        # Delete tags
        deleted = await conn.execute(
            "DELETE FROM query_tags WHERE UPPER(SPLIT_PART(query_name, ' ', 1)) = UPPER(%s)",
            (sector_name,),
        )
        tag_count = deleted.rowcount or 0

        # Soft-delete jobs
        for name in names:
            await conn.execute(
                "UPDATE scrape_jobs SET status = 'deleted', updated_at = NOW() WHERE query_name = %s AND status != 'deleted'",
                (name,),
            )
        await conn.commit()

    return {"deleted": True, "sector": sector_name, "tags_removed": tag_count, "jobs_affected": len(names)}


@router.delete("/department/{dept}/tags")
async def delete_department_tags(dept: str, request: Request):
    """Remove all query_tags for companies in a specific department."""
    from fastapi.responses import JSONResponse
    from fortress.api.db import get_conn

    async with get_conn() as conn:
        deleted = await conn.execute("""
            DELETE FROM query_tags
            WHERE siren IN (
                SELECT siren FROM companies WHERE departement = %s
            )
        """, (dept,))
        tag_count = deleted.rowcount or 0
        await conn.commit()

    if tag_count == 0:
        return JSONResponse(status_code=404, content={"error": "Aucun tag trouvé pour ce département"})

    return {"deleted": True, "department": dept, "tags_removed": tag_count}


@router.delete("/job-group/{query_name}")
async def delete_job_group(query_name: str, request: Request):
    """Soft-delete all batches with this query_name (case-insensitive) and remove their tags."""
    from fastapi.responses import JSONResponse
    from fortress.api.db import get_conn

    async with get_conn() as conn:
        # Soft-delete all matching jobs
        jobs_result = await conn.execute(
            "UPDATE scrape_jobs SET status = 'deleted', updated_at = NOW() WHERE UPPER(query_name) = UPPER(%s) AND status != 'deleted' RETURNING query_id",
            (query_name,),
        )
        job_ids = [r[0] for r in await jobs_result.fetchall()]

        if not job_ids:
            return JSONResponse(status_code=404, content={"error": "Groupe de jobs introuvable"})

        # Remove query_tags for this group
        tag_result = await conn.execute(
            "DELETE FROM query_tags WHERE UPPER(query_name) = UPPER(%s)",
            (query_name,),
        )
        tag_count = tag_result.rowcount or 0
        await conn.commit()

    return {"deleted": True, "query_name": query_name, "jobs_deleted": len(job_ids), "tags_removed": tag_count}
