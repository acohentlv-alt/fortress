"""Dashboard API routes — global stats and recent activity.

Admin: sees ALL enriched data (the "data bank" view).
Regular users: stats scoped to their own jobs only.
"""

from fastapi import APIRouter, Request

from fortress.api.db import fetch_all, fetch_one

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/stats")
async def get_stats(request: Request):
    """Dashboard statistics — shared workspace."""
    scope_clause = ""
    scope_params: tuple = ()
    jobs_scope = ""

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
             WHERE status = 'completed') AS completed_jobs,
            (SELECT COUNT(*) FROM scrape_jobs
             WHERE status IN ('in_progress', 'queued', 'triage') AND EXTRACT(EPOCH FROM (NOW() - updated_at)) <= 180) AS running_jobs
        FROM enriched
    """, scope_params)
    return stats or {}


@router.get("/recent-activity")
async def get_recent_activity(request: Request):
    """Last 10 job updates — shared workspace."""
    rows = await fetch_all("""
        SELECT query_id, query_name,
               CASE
                   WHEN status IN ('in_progress', 'queued', 'triage') AND EXTRACT(EPOCH FROM (NOW() - updated_at)) > 180
                        AND COALESCE(companies_qualified, 0) >= COALESCE(batch_size, total_companies, 1)
                        THEN 'completed'
                   WHEN status IN ('in_progress', 'queued', 'triage') AND EXTRACT(EPOCH FROM (NOW() - updated_at)) > 180
                        THEN 'failed'
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
    return rows


# ---------------------------------------------------------------------------
# Action 3: By-job stats with UPPER() normalization + nested batches
# ---------------------------------------------------------------------------

@router.get("/stats/by-job")
async def get_stats_by_job(request: Request):
    """Job-level stats — shared workspace."""
    user_filter = ""

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
        WHERE sj.status != 'deleted'
        {user_filter}
        GROUP BY UPPER(sj.query_name)
        ORDER BY MAX(sj.updated_at) DESC
    """)

    all_batches = await fetch_all(f"""
        SELECT
            UPPER(sj.query_name) AS group_key,
            sj.query_id, sj.query_name, 
            CASE 
                WHEN sj.status IN ('in_progress', 'queued', 'triage') AND EXTRACT(EPOCH FROM (NOW() - sj.updated_at)) > 180
                     AND COALESCE(sj.companies_qualified, 0) >= COALESCE(sj.batch_size, sj.total_companies, 1)
                     THEN 'completed'
                WHEN sj.status IN ('in_progress', 'queued', 'triage') AND EXTRACT(EPOCH FROM (NOW() - sj.updated_at)) > 180
                     THEN 'failed'
                ELSE sj.status 
            END AS status,
            sj.batch_number, sj.companies_scraped, sj.companies_failed,
            sj.total_companies, sj.wave_current, sj.wave_total,
            sj.triage_green, sj.triage_yellow, sj.triage_red, sj.triage_black,
            sj.created_at, sj.updated_at
        FROM scrape_jobs sj
        WHERE sj.status != 'deleted'
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
# Data Analysis — focused analytics: quality, gaps, enrichers, pipeline
# ---------------------------------------------------------------------------

@router.get("/analysis")
async def get_analysis(request: Request):
    """Data analysis dashboard — 4 focused panels.

    Returns: quality, gaps, enrichers, pipeline.
    """
    scope_clause = ""
    scope_params: tuple = ()
    jobs_where = "WHERE sj.status != 'deleted'"

    # ── 1. Quality scores ────────────────────────────────────────
    quality = await fetch_one(f"""
        WITH tagged AS (
            SELECT DISTINCT qt.siren FROM query_tags qt {scope_clause}
        ),
        enriched AS (
            SELECT
                t.siren,
                MAX(ct.phone)   AS phone,
                MAX(ct.email)   AS email,
                MAX(ct.website) AS website,
                MAX(ct.social_linkedin) AS linkedin,
                MAX(ct.social_facebook) AS facebook
            FROM tagged t
            LEFT JOIN contacts ct ON ct.siren = t.siren
            GROUP BY t.siren
        )
        SELECT
            COUNT(*)                                          AS total,
            COUNT(*) FILTER (WHERE phone IS NOT NULL)         AS with_phone,
            COUNT(*) FILTER (WHERE email IS NOT NULL)         AS with_email,
            COUNT(*) FILTER (WHERE website IS NOT NULL)       AS with_website,
            COUNT(*) FILTER (WHERE linkedin IS NOT NULL OR facebook IS NOT NULL) AS with_social
        FROM enriched
    """, scope_params)

    q = quality or {}
    total = q.get("total", 0) or 0
    phone_pct = round(100 * (q.get("with_phone", 0) or 0) / max(total, 1))
    email_pct = round(100 * (q.get("with_email", 0) or 0) / max(total, 1))
    web_pct = round(100 * (q.get("with_website", 0) or 0) / max(total, 1))
    social_pct = round(100 * (q.get("with_social", 0) or 0) / max(total, 1))
    overall = round((phone_pct + email_pct + web_pct) / 3)

    quality_data = {
        "total": total,
        "with_phone": q.get("with_phone", 0) or 0,
        "with_email": q.get("with_email", 0) or 0,
        "with_website": q.get("with_website", 0) or 0,
        "with_social": q.get("with_social", 0) or 0,
        "phone_pct": phone_pct,
        "email_pct": email_pct,
        "website_pct": web_pct,
        "social_pct": social_pct,
        "overall_score": overall,
    }

    # ── 2. Data gaps — what's missing ────────────────────────────
    gaps = await fetch_one(f"""
        WITH tagged AS (
            SELECT DISTINCT qt.siren FROM query_tags qt {scope_clause}
        ),
        enriched AS (
            SELECT
                t.siren,
                MAX(ct.phone)   AS phone,
                MAX(ct.email)   AS email,
                MAX(ct.website) AS website
            FROM tagged t
            LEFT JOIN contacts ct ON ct.siren = t.siren
            GROUP BY t.siren
        )
        SELECT
            COUNT(*)                                                              AS total,
            COUNT(*) FILTER (WHERE phone IS NULL)                                 AS missing_phone,
            COUNT(*) FILTER (WHERE email IS NULL)                                 AS missing_email,
            COUNT(*) FILTER (WHERE website IS NULL)                               AS missing_website,
            COUNT(*) FILTER (WHERE phone IS NULL AND email IS NULL AND website IS NULL) AS missing_all,
            COUNT(*) FILTER (WHERE phone IS NOT NULL AND email IS NOT NULL AND website IS NOT NULL) AS complete
        FROM enriched
    """, scope_params)

    gaps_data = gaps or {}

    # ── 3. Enricher health — simplified ──────────────────────────
    enrichers = {}

    # All-time stats from scrape_audit
    audit_stats = await fetch_all("""
        SELECT
            action,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE result = 'success') AS success,
            ROUND(AVG(duration_ms) FILTER (WHERE result = 'success')) AS avg_time_ms,
            MAX(created_at) AS last_run
        FROM scrape_audit
        WHERE action IN ('maps_lookup', 'website_crawl')
        GROUP BY action
    """)

    # Last 24h stats from scrape_audit
    audit_24h = await fetch_all("""
        SELECT
            action,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE result = 'success') AS success
        FROM scrape_audit
        WHERE action IN ('maps_lookup', 'website_crawl')
          AND created_at >= NOW() - INTERVAL '24 hours'
        GROUP BY action
    """)

    audit_map = {r["action"]: r for r in (audit_stats or [])}
    audit_24h_map = {r["action"]: r for r in (audit_24h or [])}

    # Maps enricher
    maps_all = audit_map.get("maps_lookup", {})
    maps_24h = audit_24h_map.get("maps_lookup", {})
    maps_total = maps_all.get("total", 0) or 0
    maps_success = maps_all.get("success", 0) or 0
    enrichers["maps"] = {
        "total": maps_total,
        "success": maps_success,
        "rate": round(100 * maps_success / max(maps_total, 1)),
        "avg_time_s": round((maps_all.get("avg_time_ms") or 0) / 1000, 1),
        "last_run": str(maps_all.get("last_run") or ""),
        "last_24h_total": maps_24h.get("total", 0) or 0,
        "last_24h_success": maps_24h.get("success", 0) or 0,
    }

    # Crawl enricher
    crawl_all = audit_map.get("website_crawl", {})
    crawl_24h = audit_24h_map.get("website_crawl", {})
    crawl_total = crawl_all.get("total", 0) or 0
    crawl_success = crawl_all.get("success", 0) or 0
    enrichers["crawl"] = {
        "total": crawl_total,
        "success": crawl_success,
        "rate": round(100 * crawl_success / max(crawl_total, 1)),
        "avg_time_s": round((crawl_all.get("avg_time_ms") or 0) / 1000, 1),
        "last_run": str(crawl_all.get("last_run") or ""),
        "last_24h_total": crawl_24h.get("total", 0) or 0,
        "last_24h_success": crawl_24h.get("success", 0) or 0,
    }

    # Outcomes from enrichment_log
    outcomes = await fetch_all("""
        SELECT outcome, COUNT(*) AS count
        FROM enrichment_log
        GROUP BY outcome
        ORDER BY count DESC
    """)
    enrichers["outcomes"] = {r["outcome"]: r["count"] for r in (outcomes or [])}

    # ── 4. Pipeline health ───────────────────────────────────────
    pipeline_counts = await fetch_one(f"""
        SELECT
            COUNT(*) FILTER (WHERE status = 'completed')        AS completed_total,
            COUNT(*) FILTER (WHERE status = 'failed')           AS failed_total,
            COUNT(*) FILTER (WHERE status IN ('in_progress', 'queued', 'triage')
                            AND EXTRACT(EPOCH FROM (NOW() - updated_at)) <= 180)
                                                                AS running_now,
            COUNT(*) FILTER (WHERE status = 'completed'
                            AND created_at >= NOW() - INTERVAL '7 days')
                                                                AS completed_7d,
            COUNT(*) FILTER (WHERE status = 'failed'
                            AND created_at >= NOW() - INTERVAL '7 days')
                                                                AS failed_7d,
            SUM(COALESCE(companies_qualified, 0))               AS total_qualified,
            SUM(COALESCE(replaced_count, 0))                    AS total_replaced
        FROM scrape_jobs
        WHERE status != 'deleted'
    """)

    # Weekly quality trend (last 12 weeks)
    weekly_trend = await fetch_all(f"""
        WITH weekly_jobs AS (
            SELECT
                DATE_TRUNC('week', sj.created_at) AS week,
                sj.query_name,
                COUNT(DISTINCT qt.siren) AS companies,
                COUNT(DISTINCT CASE WHEN ct.phone IS NOT NULL THEN qt.siren END) AS with_phone,
                COUNT(DISTINCT CASE WHEN ct.email IS NOT NULL THEN qt.siren END) AS with_email,
                COUNT(DISTINCT CASE WHEN ct.website IS NOT NULL THEN qt.siren END) AS with_website
            FROM scrape_jobs sj
            JOIN query_tags qt ON UPPER(qt.query_name) = UPPER(sj.query_name)
            LEFT JOIN contacts ct ON ct.siren = qt.siren
            WHERE sj.status = 'completed'
              AND sj.created_at >= NOW() - INTERVAL '12 weeks'
            GROUP BY week, sj.query_name
        )
        SELECT
            TO_CHAR(week, 'YYYY-"W"IW') AS week,
            SUM(companies) AS companies,
            ROUND(AVG(
                CASE WHEN companies > 0 THEN
                    (100.0 * with_phone / companies +
                     100.0 * with_email / companies +
                     100.0 * with_website / companies) / 3
                ELSE 0 END
            )) AS avg_quality
        FROM weekly_jobs
        GROUP BY week
        ORDER BY week ASC
    """)

    # Last 5 completed/failed jobs (compact)
    recent_jobs = await fetch_all(f"""
        SELECT
            sj.query_id, sj.query_name,
            CASE
                WHEN sj.status IN ('in_progress', 'queued', 'triage')
                     AND EXTRACT(EPOCH FROM (NOW() - sj.updated_at)) > 180
                     AND COALESCE(sj.companies_qualified, 0) >= COALESCE(sj.batch_size, sj.total_companies, 1)
                     THEN 'completed'
                WHEN sj.status IN ('in_progress', 'queued', 'triage')
                     AND EXTRACT(EPOCH FROM (NOW() - sj.updated_at)) > 180
                     THEN 'failed'
                ELSE sj.status
            END AS status,
            sj.companies_scraped,
            COALESCE(sj.batch_size, sj.total_companies) AS batch_size,
            sj.created_at
        FROM scrape_jobs sj
        {jobs_where}
        ORDER BY sj.created_at DESC
        LIMIT 5
    """)

    pipeline_data = {
        **(pipeline_counts or {}),
        "weekly_trend": weekly_trend or [],
        "recent_jobs": recent_jobs or [],
    }

    return {
        "quality": quality_data,
        "gaps": gaps_data,
        "enrichers": enrichers,
        "pipeline": pipeline_data,
    }


# ---------------------------------------------------------------------------
# All Data — browse all enriched entities with contact info
# ---------------------------------------------------------------------------

@router.get("/all-data")
async def get_all_data(
    request: Request,
    q: str = "",
    department: str = "",
    limit: int = 50,
    offset: int = 0,
):
    """Browse all enriched companies with their best contact data.

    Admin: all enriched data. User: scoped to own jobs.
    Supports text search (name/SIREN) and department filter.
    """
    # Shared workspace — no user scoping
    scope_clause = ""
    scope_params: list = []

    # Build search filter
    search_clause = ""
    search_params: list = []
    if q.strip():
        clean = q.strip()
        digits = clean.replace(" ", "")
        if digits.isdigit() and len(digits) == 9:
            search_clause = "AND co.siren = %s"
            search_params = [digits]
        else:
            search_clause = "AND UPPER(co.denomination) LIKE UPPER(%s)"
            search_params = [f"%{clean}%"]

    dept_clause = ""
    dept_params: list = []
    if department.strip():
        dept_clause = "AND co.departement = %s"
        dept_params = [department.strip()]

    all_params = tuple(scope_params + search_params + dept_params + [limit, offset])

    # Count total for pagination
    count_params = tuple(scope_params + search_params + dept_params)
    total_row = await fetch_one(f"""
        SELECT COUNT(DISTINCT qt.siren) AS total
        FROM query_tags qt
        JOIN companies co ON co.siren = qt.siren
        WHERE 1=1 {scope_clause} {search_clause} {dept_clause}
    """, count_params)
    total = (total_row or {}).get("total", 0) or 0

    rows = await fetch_all(f"""
        SELECT DISTINCT ON (co.siren)
            co.siren, co.denomination, co.naf_code, co.naf_libelle,
            co.forme_juridique, co.ville, co.departement,
            ct.phone, ct.email, ct.website,
            qt.query_name, qt.tagged_at
        FROM query_tags qt
        JOIN companies co ON co.siren = qt.siren
        LEFT JOIN LATERAL (
            SELECT phone, email, website FROM contacts c2
            WHERE c2.siren = co.siren
            ORDER BY (CASE WHEN c2.phone IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN c2.email IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN c2.website IS NOT NULL THEN 1 ELSE 0 END) DESC
            LIMIT 1
        ) ct ON true
        WHERE 1=1 {scope_clause} {search_clause} {dept_clause}
        ORDER BY co.siren, qt.tagged_at DESC
        LIMIT %s OFFSET %s
    """, all_params)

    return {"results": rows, "total": total, "offset": offset, "limit": limit}


# ---------------------------------------------------------------------------
# By-Sector Stats — accurate unique SIREN counts from query_tags
# ---------------------------------------------------------------------------

@router.get("/stats/by-sector")
async def get_stats_by_sector(request: Request):
    """Sector-level stats using unique SIRENs from query_tags.

    Admin: all data. User: scoped to their own jobs.
    Returns unique company counts per sector so totals match the dashboard.
    """
    # Shared workspace — no user scoping
    scope_clause = ""
    scope_params: tuple = ()

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
    """Remove all query_tags for a sector. Soft-deletes associated jobs. Admin only."""
    from fastapi.responses import JSONResponse
    from fortress.api.db import get_conn

    user = getattr(request.state, 'user', None)
    if not user or user.role != 'admin':
        return JSONResponse(status_code=403, content={"error": "Admin uniquement"})

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
    """Remove all query_tags for companies in a specific department. Admin only."""
    from fastapi.responses import JSONResponse
    from fortress.api.db import get_conn

    user = getattr(request.state, 'user', None)
    if not user or user.role != 'admin':
        return JSONResponse(status_code=403, content={"error": "Admin uniquement"})

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
    """Soft-delete all batches with this query_name (case-insensitive) and remove their tags. Admin only."""
    from fastapi.responses import JSONResponse
    from fortress.api.db import get_conn

    user = getattr(request.state, 'user', None)
    if not user or user.role != 'admin':
        return JSONResponse(status_code=403, content={"error": "Admin uniquement"})

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
