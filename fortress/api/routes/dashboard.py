"""Dashboard API routes — global stats and recent activity.

Admin: sees ALL enriched data (the "data bank" view).
Regular users: stats scoped to their own jobs only.
"""

import asyncio
import time
from fastapi import APIRouter, Request

from fortress.api.db import fetch_all, fetch_one
from fortress.api.sql_helpers import merged_contacts_cte

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])

# ── Simple in-process cache ─────────────────────────────────────────────────
_cache: dict = {}


def _cached(key: str, ttl_seconds: int):
    """Return cached value if fresh, else None."""
    entry = _cache.get(key)
    if entry and time.time() - entry["ts"] < ttl_seconds:
        return entry["data"]
    return None


def _set_cache(key: str, data):
    _cache[key] = {"data": data, "ts": time.time()}


def _invalidate_cache():
    _cache.clear()


@router.get("/stats")
async def get_stats(request: Request):
    """Dashboard statistics — scoped by workspace."""
    user = getattr(request.state, "user", None)

    if user and not user.is_admin:
        wid = user.workspace_id
        cache_key = f"stats_ws_{wid}"
        tags_clause = "WHERE qt.workspace_id = %s"
        jobs_clause = "AND workspace_id = %s"
        stats_params: tuple = (wid, wid, wid, wid)
    else:
        cache_key = "stats_admin"
        tags_clause = ""
        jobs_clause = ""
        stats_params = ()

    cached = _cached(cache_key, 60)
    if cached is not None:
        return cached

    stats = await fetch_one(f"""
        WITH tagged AS (
            SELECT DISTINCT qt.siren FROM batch_tags qt {tags_clause}
        ),
        {merged_contacts_cte('SELECT siren FROM tagged')}
        SELECT
            COUNT(*)                                                    AS total_companies,
            COUNT(*) FILTER (WHERE cs.phone IS NOT NULL)                AS with_phone,
            COUNT(*) FILTER (WHERE cs.email IS NOT NULL)                AS with_email,
            COUNT(*) FILTER (WHERE cs.website IS NOT NULL)              AS with_website,
            COUNT(DISTINCT co.departement)
                FILTER (WHERE co.departement IS NOT NULL)               AS departments_covered,
            (SELECT COUNT(*) FROM batch_data WHERE status != 'deleted' {jobs_clause})   AS total_jobs,
            (SELECT COUNT(*) FROM batch_data
             WHERE status IN ('completed', 'interrupted') {jobs_clause})  AS completed_jobs,
            (SELECT COUNT(*) FROM batch_data
             WHERE status IN ('in_progress', 'queued', 'triage')
               {jobs_clause})                                           AS running_jobs
        FROM tagged t
        JOIN companies co ON co.siren = t.siren
        LEFT JOIN merged_contacts cs ON cs.siren = t.siren
    """, stats_params if stats_params else None)
    result = stats or {}
    _set_cache(cache_key, result)
    return result


@router.get("/recent-activity")
async def get_recent_activity(request: Request):
    """Last 10 job updates — scoped by workspace."""
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        ws_filter = "AND workspace_id = %s"
        ws_params: tuple = (user.workspace_id,)
    else:
        ws_filter = ""
        ws_params = ()

    rows = await fetch_all(f"""
        SELECT batch_id, batch_name,
               status,
               total_companies, companies_scraped, companies_failed,
               wave_current, wave_total,
               triage_black, triage_green, triage_yellow, triage_red,
               created_at, updated_at, worker_id
        FROM batch_data
        WHERE status != 'deleted' {ws_filter}
        ORDER BY updated_at DESC
        LIMIT 10
    """, ws_params if ws_params else None)
    return rows


# ---------------------------------------------------------------------------
# Action 3: By-job stats with UPPER() normalization + nested batches
# ---------------------------------------------------------------------------

@router.get("/stats/by-job")
async def get_stats_by_job(request: Request):
    """Job-level stats — scoped by workspace."""
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        ws_filter = "AND sj.workspace_id = %s"
        ws_params: tuple = (user.workspace_id,)
        tag_filter = "WHERE workspace_id = %s"
        tag_params: tuple = (user.workspace_id,)
    else:
        ws_filter = ""
        ws_params = ()
        tag_filter = ""
        tag_params = ()

    groups = await fetch_all(f"""
        WITH tag_counts AS (
            SELECT UPPER(batch_name) AS batch_key, COUNT(DISTINCT siren) AS unique_companies
            FROM batch_tags
            {tag_filter}
            GROUP BY UPPER(batch_name)
        )
        SELECT
            UPPER(sj.batch_name) AS batch_key,
            MAX(sj.batch_name) AS display_name,
            COUNT(*) AS batch_count,
            SUM(COALESCE(sj.companies_scraped, 0)) AS total_scraped,
            SUM(COALESCE(sj.companies_failed, 0)) AS total_failed,
            SUM(COALESCE(sj.triage_green, 0)) AS total_green,
            SUM(COALESCE(sj.triage_yellow, 0)) AS total_yellow,
            SUM(COALESCE(sj.triage_red, 0)) AS total_red,
            SUM(COALESCE(sj.triage_black, 0)) AS total_black,
            MAX(sj.updated_at) AS last_updated,
            COALESCE(MAX(tc.unique_companies), 0) AS unique_companies
        FROM batch_data sj
        LEFT JOIN tag_counts tc ON tc.batch_key = UPPER(sj.batch_name)
        WHERE sj.status != 'deleted'
        {ws_filter}
        GROUP BY UPPER(sj.batch_name)
        ORDER BY MAX(sj.updated_at) DESC
    """, (tag_params + ws_params) if (tag_params or ws_params) else None)

    all_batches = await fetch_all(f"""
        SELECT
            UPPER(sj.batch_name) AS group_key,
            sj.batch_id, sj.batch_name,
            sj.status AS status,
            sj.batch_number, sj.companies_scraped, sj.companies_failed,
            sj.total_companies, sj.wave_current, sj.wave_total,
            sj.triage_green, sj.triage_yellow, sj.triage_red, sj.triage_black,
            sj.created_at, sj.updated_at
        FROM batch_data sj
        WHERE sj.status != 'deleted'
        {ws_filter}
        ORDER BY sj.created_at DESC
        LIMIT 200
    """, ws_params if ws_params else None)

    batch_map: dict[str, list[dict]] = {}
    for b in all_batches:
        key = b.pop("group_key")
        batch_map.setdefault(key, []).append(b)

    result = []
    for g in groups:
        batch_key = g.pop("batch_key")
        g["batch_name"] = batch_key
        g["batches"] = batch_map.get(batch_key, [])
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
    totals = await fetch_one(f"""
        WITH {merged_contacts_cte('SELECT DISTINCT siren FROM batch_tags')}
        SELECT
            COUNT(DISTINCT qt.siren) AS total_enriched,
            COUNT(DISTINCT ct.siren) FILTER (WHERE ct.phone IS NOT NULL) AS with_phone,
            COUNT(DISTINCT ct.siren) FILTER (WHERE ct.email IS NOT NULL) AS with_email,
            COUNT(DISTINCT ct.siren) FILTER (WHERE ct.website IS NOT NULL) AS with_website,
            COUNT(DISTINCT u.id) AS total_users,
            COUNT(DISTINCT sj.batch_id) AS total_batches
        FROM batch_tags qt
        LEFT JOIN merged_contacts ct ON ct.siren = qt.siren
        LEFT JOIN batch_data sj ON UPPER(sj.batch_name) = UPPER(qt.batch_name)
        LEFT JOIN users u ON u.id = sj.user_id
    """)

    # Top 10 sectors
    top_sectors = await fetch_all("""
        SELECT
            UPPER(SPLIT_PART(batch_name, ' ', 1)) AS sector,
            COUNT(DISTINCT siren) AS companies
        FROM batch_tags
        GROUP BY sector
        ORDER BY companies DESC
        LIMIT 10
    """)

    # Top 10 departments
    top_depts = await fetch_all("""
        SELECT
            co.departement,
            COUNT(DISTINCT qt.siren) AS companies
        FROM batch_tags qt
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
        FROM batch_data
        WHERE updated_at > NOW() - INTERVAL '7 days'
        GROUP BY worker_id
        ORDER BY last_active DESC
    """)

    # Per-workspace breakdown for admin dedup view
    by_workspace = await fetch_all(f"""
        WITH {merged_contacts_cte('SELECT DISTINCT siren FROM batch_tags')}
        SELECT
            w.name AS workspace_name,
            COUNT(DISTINCT qt.siren) AS total_companies,
            COUNT(DISTINCT ct.siren) FILTER (WHERE ct.phone IS NOT NULL) AS with_phone,
            COUNT(DISTINCT ct.siren) FILTER (WHERE ct.email IS NOT NULL) AS with_email
        FROM workspaces w
        LEFT JOIN batch_tags qt ON qt.workspace_id = w.id
        LEFT JOIN merged_contacts ct ON ct.siren = qt.siren
        GROUP BY w.id, w.name
        ORDER BY total_companies DESC
    """)

    return {
        "totals": totals or {},
        "top_sectors": top_sectors,
        "top_departments": top_depts,
        "workers": workers,
        "by_workspace": by_workspace or [],
    }


# ---------------------------------------------------------------------------
# Data Analysis — focused analytics: quality, gaps, enrichers, pipeline
# ---------------------------------------------------------------------------

@router.get("/analysis")
async def get_analysis(request: Request):
    """Data analysis dashboard — 4 focused panels.

    Returns: quality, gaps, enrichers, pipeline.
    Queries run in parallel groups using asyncio.gather for speed.
    """
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        wid = user.workspace_id
        cache_key = f"analysis_ws_{wid}"
        scope_clause = "WHERE qt.workspace_id = %s"
        scope_params: tuple = (wid,)
        batch_ws = "AND workspace_id = %s"
        batch_ws_params: tuple = (wid,)
        tag_ws = "AND qt.workspace_id = %s"
        tag_ws_params: tuple = (wid,)
    else:
        cache_key = "analysis_admin"
        scope_clause = ""
        scope_params = ()
        batch_ws = ""
        batch_ws_params = ()
        tag_ws = ""
        tag_ws_params = ()

    cached = _cached(cache_key, 120)
    if cached is not None:
        return cached

    # Semaphore: limit to 4 concurrent DB connections from this endpoint
    sem = asyncio.Semaphore(4)

    async def _fetch_one_sem(sql, params=None):
        async with sem:
            return await fetch_one(sql, params)

    async def _fetch_all_sem(sql, params=None):
        async with sem:
            return await fetch_all(sql, params)

    # ── Group 1: Merged quality + gaps query (one CTE instead of two) ──
    async def _group_quality_gaps():
        return await _fetch_one_sem(f"""
            WITH tagged AS (
                SELECT DISTINCT qt.siren FROM batch_tags qt {scope_clause}
            ),
            {merged_contacts_cte('SELECT siren FROM tagged')},
            enriched AS (
                SELECT
                    t.siren,
                    mc.phone,
                    mc.email,
                    mc.website,
                    mc.social_linkedin  AS linkedin,
                    mc.social_facebook  AS facebook
                FROM tagged t
                LEFT JOIN merged_contacts mc ON mc.siren = t.siren
            )
            SELECT
                COUNT(*)                                                              AS total,
                COUNT(*) FILTER (WHERE phone IS NOT NULL)                             AS with_phone,
                COUNT(*) FILTER (WHERE email IS NOT NULL)                             AS with_email,
                COUNT(*) FILTER (WHERE website IS NOT NULL)                           AS with_website,
                COUNT(*) FILTER (WHERE linkedin IS NOT NULL OR facebook IS NOT NULL)  AS with_social,
                COUNT(*) FILTER (WHERE phone IS NULL)                                 AS missing_phone,
                COUNT(*) FILTER (WHERE email IS NULL)                                 AS missing_email,
                COUNT(*) FILTER (WHERE website IS NULL)                               AS missing_website,
                COUNT(*) FILTER (WHERE phone IS NULL AND email IS NULL AND website IS NULL) AS missing_all,
                COUNT(*) FILTER (WHERE phone IS NOT NULL AND email IS NOT NULL AND website IS NOT NULL) AS complete
            FROM enriched
        """, scope_params)

    # ── Group 2: Enricher stats (all-time + 24h) + outcomes ────────────
    async def _group_enrichers():
        audit_stats, audit_24h, outcomes = await asyncio.gather(
            _fetch_all_sem(f"""
                SELECT
                    bl.action,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE bl.result = 'success') AS success,
                    ROUND(AVG(bl.duration_ms) FILTER (WHERE bl.result = 'success')) AS avg_time_ms,
                    MAX(bl.timestamp) AS last_run
                FROM batch_log bl JOIN batch_data bd ON bd.batch_id = bl.batch_id AND bd.status != 'deleted'
                WHERE bl.action IN ('maps_lookup', 'website_crawl') {batch_ws.replace('workspace_id', 'bl.workspace_id')}
                GROUP BY bl.action
            """, batch_ws_params if batch_ws_params else None),
            _fetch_all_sem(f"""
                SELECT
                    bl.action,
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE bl.result = 'success') AS success
                FROM batch_log bl JOIN batch_data bd ON bd.batch_id = bl.batch_id AND bd.status != 'deleted'
                WHERE bl.action IN ('maps_lookup', 'website_crawl')
                  AND bl.timestamp >= NOW() - INTERVAL '24 hours' {batch_ws.replace('workspace_id', 'bl.workspace_id')}
                GROUP BY bl.action
            """, batch_ws_params if batch_ws_params else None),
            _fetch_all_sem(f"""
                SELECT outcome, COUNT(*) AS count
                FROM enrichment_log el
                {'WHERE el.siren IN (SELECT qt.siren FROM batch_tags qt WHERE qt.workspace_id = %s)' if scope_params else ''}
                GROUP BY outcome
                ORDER BY count DESC
            """, scope_params if scope_params else None),
        )
        return audit_stats, audit_24h, outcomes

    # ── Group 3: Pipeline counts + weekly trend + recent jobs ──────────
    async def _group_pipeline():
        pipeline_counts, weekly_trend, recent_jobs = await asyncio.gather(
            _fetch_one_sem(f"""
                SELECT
                    COUNT(*) FILTER (WHERE status IN ('completed', 'interrupted'))  AS completed_total,
                    COUNT(*) FILTER (WHERE status = 'failed')           AS failed_total,
                    COUNT(*) FILTER (WHERE status IN ('in_progress', 'queued', 'triage'))
                                                                        AS running_now,
                    COUNT(*) FILTER (WHERE status IN ('completed', 'interrupted')
                                    AND created_at >= NOW() - INTERVAL '7 days')
                                                                        AS completed_7d,
                    COUNT(*) FILTER (WHERE status = 'failed'
                                    AND created_at >= NOW() - INTERVAL '7 days')
                                                                        AS failed_7d,
                    SUM(COALESCE(companies_qualified, 0))               AS total_qualified,
                    SUM(COALESCE(replaced_count, 0))                    AS total_replaced
                FROM batch_data
                WHERE status != 'deleted' {batch_ws}
            """, batch_ws_params if batch_ws_params else None),
            _fetch_all_sem(f"""
                WITH {merged_contacts_cte(f"""
                    SELECT DISTINCT qt.siren FROM batch_tags qt
                    JOIN batch_data sj ON UPPER(qt.batch_name) = UPPER(sj.batch_name)
                    WHERE sj.status IN ('completed', 'interrupted')
                      AND sj.created_at >= NOW() - INTERVAL '12 weeks'
                      {batch_ws.replace('workspace_id', 'sj.workspace_id')} {tag_ws}
                """)},
                weekly_jobs AS (
                    SELECT
                        DATE_TRUNC('week', sj.created_at) AS week,
                        sj.batch_name,
                        COUNT(DISTINCT qt.siren) AS companies,
                        COUNT(DISTINCT CASE WHEN mc.phone IS NOT NULL THEN qt.siren END) AS with_phone,
                        COUNT(DISTINCT CASE WHEN mc.email IS NOT NULL THEN qt.siren END) AS with_email,
                        COUNT(DISTINCT CASE WHEN mc.website IS NOT NULL THEN qt.siren END) AS with_website
                    FROM batch_data sj
                    JOIN batch_tags qt ON UPPER(qt.batch_name) = UPPER(sj.batch_name)
                    LEFT JOIN merged_contacts mc ON mc.siren = qt.siren
                    WHERE sj.status IN ('completed', 'interrupted')
                      AND sj.created_at >= NOW() - INTERVAL '12 weeks' {batch_ws.replace('workspace_id', 'sj.workspace_id')} {tag_ws}
                    GROUP BY week, sj.batch_name
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
            """, (batch_ws_params + tag_ws_params) * 2 if (batch_ws_params or tag_ws_params) else None),
            _fetch_all_sem(f"""
                SELECT
                    sj.batch_id, sj.batch_name,
                    sj.status AS status,
                    sj.companies_scraped,
                    COALESCE(sj.batch_size, sj.total_companies) AS batch_size,
                    sj.created_at
                FROM batch_data sj
                WHERE sj.status != 'deleted' {batch_ws.replace('workspace_id', 'sj.workspace_id')}
                ORDER BY sj.created_at DESC
                LIMIT 5
            """, batch_ws_params if batch_ws_params else None),
        )
        return pipeline_counts, weekly_trend, recent_jobs

    # ── Group 4: Top searches + recent searches + week comparison ──────
    async def _group_searches():
        top_searches, recent_searches, week_row = await asyncio.gather(
            _fetch_all_sem(f"""
                WITH {merged_contacts_cte('SELECT DISTINCT siren FROM batch_tags' + (' WHERE workspace_id = %s' if tag_ws_params else ''))}
                SELECT sj.batch_name, COUNT(*) AS company_count,
                    ROUND(100.0 * COUNT(CASE WHEN mc.phone IS NOT NULL THEN 1 END) / NULLIF(COUNT(*), 0)) AS phone_rate,
                    ROUND(100.0 * COUNT(CASE WHEN mc.email IS NOT NULL THEN 1 END) / NULLIF(COUNT(*), 0)) AS email_rate
                FROM batch_tags bt
                JOIN batch_data sj ON sj.batch_id = bt.batch_id
                JOIN companies co ON co.siren = bt.siren
                LEFT JOIN merged_contacts mc ON mc.siren = bt.siren
                WHERE 1=1 {'AND bt.workspace_id = %s' if tag_ws_params else ''}
                GROUP BY sj.batch_name
                ORDER BY company_count DESC
                LIMIT 5
            """, (tag_ws_params + tag_ws_params) if tag_ws_params else None),
            _fetch_all_sem(f"""
                SELECT DISTINCT sj.batch_name, sj.created_at
                FROM batch_data sj
                WHERE sj.created_at >= NOW() - INTERVAL '7 days'
                  AND sj.status != 'deleted' {batch_ws.replace('workspace_id', 'sj.workspace_id')}
                ORDER BY sj.created_at DESC
                LIMIT 10
            """, batch_ws_params if batch_ws_params else None),
            _fetch_one_sem(f"""
                SELECT
                    COUNT(CASE WHEN bt.tagged_at >= NOW() - INTERVAL '7 days' THEN 1 END) AS this_week_companies,
                    COUNT(CASE WHEN bt.tagged_at >= NOW() - INTERVAL '14 days'
                                AND bt.tagged_at < NOW() - INTERVAL '7 days' THEN 1 END) AS last_week_companies,
                    COUNT(DISTINCT CASE WHEN sj.created_at >= NOW() - INTERVAL '7 days' THEN sj.batch_id END) AS this_week_batches,
                    COUNT(DISTINCT CASE WHEN sj.created_at >= NOW() - INTERVAL '14 days'
                                          AND sj.created_at < NOW() - INTERVAL '7 days' THEN sj.batch_id END) AS last_week_batches
                FROM batch_tags bt
                JOIN batch_data sj ON sj.batch_id = bt.batch_id
                WHERE 1=1 {'AND bt.workspace_id = %s' if tag_ws_params else ''}
            """, tag_ws_params if tag_ws_params else None),
        )
        return top_searches, recent_searches, week_row

    # ── Run all 4 groups in parallel ────────────────────────────────────
    (
        quality_gaps_row,
        (audit_stats, audit_24h, outcomes),
        (pipeline_counts, weekly_trend, recent_jobs),
        (top_searches, recent_searches, week_row),
    ) = await asyncio.gather(
        _group_quality_gaps(),
        _group_enrichers(),
        _group_pipeline(),
        _group_searches(),
    )

    # ── Build quality_data from merged row ───────────────────────────────
    q = quality_gaps_row or {}
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

    gaps_data = {
        "total": total,
        "missing_phone": q.get("missing_phone", 0) or 0,
        "missing_email": q.get("missing_email", 0) or 0,
        "missing_website": q.get("missing_website", 0) or 0,
        "missing_all": q.get("missing_all", 0) or 0,
        "complete": q.get("complete", 0) or 0,
    }

    # ── Build enrichers from audit rows ──────────────────────────────────
    enrichers: dict = {}
    audit_map = {r["action"]: r for r in (audit_stats or [])}
    audit_24h_map = {r["action"]: r for r in (audit_24h or [])}

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
    enrichers["outcomes"] = {r["outcome"]: r["count"] for r in (outcomes or [])}

    # ── Build pipeline_data ──────────────────────────────────────────────
    pipeline_data = {
        **(pipeline_counts or {}),
        "weekly_trend": weekly_trend or [],
        "recent_jobs": recent_jobs or [],
    }

    # ── Build week_comparison ────────────────────────────────────────────
    wr = week_row or {}
    week_comparison = {
        "this_week": {
            "companies": wr.get("this_week_companies", 0) or 0,
            "batches": wr.get("this_week_batches", 0) or 0,
        },
        "last_week": {
            "companies": wr.get("last_week_companies", 0) or 0,
            "batches": wr.get("last_week_batches", 0) or 0,
        },
    }

    result = {
        "quality": quality_data,
        "gaps": gaps_data,
        "enrichers": enrichers,
        "pipeline": pipeline_data,
        "week_comparison": week_comparison,
        "top_searches": top_searches or [],
        "recent_searches": recent_searches or [],
    }
    _set_cache(cache_key, result)
    return result


# ---------------------------------------------------------------------------
# Per-batch Pipeline Success — full user-journey analysis
# ---------------------------------------------------------------------------

@router.get("/batch-analysis")
async def get_batch_analysis(request: Request):
    """Pipeline success per batch — shows the FULL user journey for each batch.

    For enrichment batches (e.g. "camping 33"):
        Step 1: SIRENE → How many companies were available
        Step 2: Maps   → How many Maps found data for
        Step 3: Data   → Phone / Email / Website hit rates

    For import/upload batches (e.g. "Import: Base_Transport.xlsx"):
        Step 1: Import  → How many rows were ingested
        Step 2: SIRENE  → How many matched to a SIREN
        Step 3: Data    → Phone / Email / Website hit rates

    System batches (SYNC_CRAWL, ENTITY_MERGE, MANUAL_EDIT) are excluded.
    Uses 3 bulk queries instead of N×3 per-batch queries.
    """
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        wid = user.workspace_id
        bd_ws = "AND bd.workspace_id = %s"
        bd_ws_params: tuple = (wid,)
        qt_ws = "AND qt.workspace_id = %s"
        qt_ws_params: tuple = (wid,)
    else:
        bd_ws = ""
        bd_ws_params = ()
        qt_ws = ""
        qt_ws_params = ()

    # ── Get last 10 user-facing batches from batch_data ─────────────────
    batches = await fetch_all(f"""
        SELECT
            bd.batch_id,
            bd.batch_name,
            bd.status,
            bd.total_companies,
            bd.batch_size,
            bd.companies_scraped,
            bd.companies_qualified,
            bd.companies_failed,
            bd.replaced_count,
            bd.triage_green,
            bd.triage_yellow,
            bd.triage_red,
            bd.triage_black,
            bd.created_at
        FROM batch_data bd
        WHERE bd.status != 'deleted'
          AND bd.batch_name IS NOT NULL
          AND bd.batch_name != '' {bd_ws}
        ORDER BY bd.created_at DESC
        LIMIT 10
    """, bd_ws_params if bd_ws_params else None)

    if not batches:
        return {"batches": []}

    batch_ids = [b["batch_id"] for b in batches]
    batch_names = [b["batch_name"] or "" for b in batches]
    batch_names_upper = [n.upper() for n in batch_names]

    # ── Bulk query 1: direct actions per batch_id ────────────────────────
    bulk_actions_rows = await fetch_all("""
        SELECT batch_id, action,
               COUNT(*) AS total,
               COUNT(*) FILTER (WHERE result = 'success') AS success
        FROM batch_log
        WHERE batch_id = ANY(%s)
        GROUP BY batch_id, action
    """, (batch_ids,))

    # Shape: { batch_id -> { action -> {total, success} } }
    bulk_actions: dict = {}
    for row in (bulk_actions_rows or []):
        bid = row["batch_id"]
        if bid not in bulk_actions:
            bulk_actions[bid] = {}
        bulk_actions[bid][row["action"]] = {"total": row["total"], "success": row["success"]}

    # ── Bulk query 2: async actions per batch_name (upper) ───────────────
    bulk_async_rows = await fetch_all(f"""
        SELECT UPPER(qt.batch_name) AS batch_name_upper,
               bl.action,
               COUNT(DISTINCT qt.siren) AS total,
               COUNT(DISTINCT qt.siren) FILTER (WHERE bl.result = 'success' OR bl.result = 'filtered') AS success
        FROM batch_log bl
        JOIN batch_tags qt ON bl.siren = qt.siren
        WHERE UPPER(qt.batch_name) = ANY(%s) {qt_ws}
        GROUP BY UPPER(qt.batch_name), bl.action
    """, (batch_names_upper,) + qt_ws_params)

    # Shape: { batch_name_upper -> { action -> {total, success} } }
    bulk_async: dict = {}
    for row in (bulk_async_rows or []):
        bnu = row["batch_name_upper"]
        if bnu not in bulk_async:
            bulk_async[bnu] = {}
        bulk_async[bnu][row["action"]] = {"total": row["total"], "success": row["success"]}

    # ── Bulk query 3: hit rates per batch_name (upper) ───────────────────
    bulk_hits_rows = await fetch_all(f"""
        WITH {merged_contacts_cte('SELECT DISTINCT siren FROM batch_tags WHERE UPPER(batch_name) = ANY(%s)' + (' AND workspace_id = %s' if qt_ws_params else ''))}
        SELECT UPPER(qt.batch_name) AS batch_name_upper,
               COUNT(DISTINCT qt.siren) AS tagged,
               COUNT(DISTINCT qt.siren) FILTER (WHERE mc.phone IS NOT NULL) AS with_phone,
               COUNT(DISTINCT qt.siren) FILTER (WHERE mc.email IS NOT NULL) AS with_email,
               COUNT(DISTINCT qt.siren) FILTER (WHERE mc.website IS NOT NULL) AS with_website
        FROM batch_tags qt
        LEFT JOIN merged_contacts mc ON mc.siren = qt.siren
        WHERE UPPER(qt.batch_name) = ANY(%s) {qt_ws}
        GROUP BY UPPER(qt.batch_name)
    """, (batch_names_upper,) + qt_ws_params + (batch_names_upper,) + qt_ws_params)

    # Shape: { batch_name_upper -> {tagged, with_phone, with_email, with_website} }
    bulk_hits: dict = {row["batch_name_upper"]: row for row in (bulk_hits_rows or [])}

    # ── Build results from pre-fetched maps ─────────────────────────────
    results = []
    for b in batches:
        batch_id = b["batch_id"]
        batch_name = b["batch_name"] or ""
        batch_name_upper = batch_name.upper()
        is_upload = batch_name.startswith("Import:")
        is_manual_enrich = batch_id.startswith("MANUAL_ENRICH_")
        total = b["total_companies"] or 0
        scraped = b["companies_scraped"] or 0
        reused = total - scraped if total >= scraped else 0

        action_map = bulk_actions.get(batch_id, {})
        async_map = bulk_async.get(batch_name_upper, {})
        h = bulk_hits.get(batch_name_upper, {})

        tagged = h.get("tagged", 0) or 0
        with_phone = h.get("with_phone", 0) or 0
        with_email = h.get("with_email", 0) or 0
        with_website = h.get("with_website", 0) or 0

        phone_pct = round(100 * with_phone / max(tagged, 1))
        email_pct = round(100 * with_email / max(tagged, 1))
        web_pct = round(100 * with_website / max(tagged, 1))

        # ── Build pipeline steps ───────────────────────────────────
        steps = []
        summary_parts = []

        if is_upload:
            # Upload pipeline: Import → SIRENE → Données
            upload_action = action_map.get("upload", {})
            sirene_action = action_map.get("sirene", {})
            upload_success = upload_action.get("success", 0) or 0
            upload_total = upload_action.get("total", 0) or total
            sirene_success = sirene_action.get("success", 0) or 0

            steps.append({
                "label": "📥 Import",
                "detail": f"{upload_success} lignes importées",
                "value": upload_success,
                "total": upload_total,
                "pct": round(100 * upload_success / max(upload_total, 1)),
            })
            steps.append({
                "label": "🔍 Match SIRENE",
                "detail": f"{sirene_success} matchés",
                "value": sirene_success,
                "total": upload_success,
                "pct": round(100 * sirene_success / max(upload_success, 1)),
            })
            steps.append({
                "label": "📊 Données",
                "detail": f"📞 {phone_pct}%  ✉️ {email_pct}%  🌐 {web_pct}%",
                "value": with_phone + with_email + with_website,
                "total": tagged * 3,
                "pct": round((phone_pct + email_pct + web_pct) / 3),
            })

            summary_parts.append(f"{upload_success} lignes importées avec succès")
            if sirene_success > 0:
                summary_parts.append(f"{sirene_success} entreprises parfaitement matchées avec SIRENE ({round(100 * sirene_success / max(upload_success, 1))}%)")
            else:
                summary_parts.append(f"Erreur ou en attente du matching SIRENE")
            summary_parts.append(f"Résultat: {phone_pct}% avec téléphone, {email_pct}% avec email")

        elif is_manual_enrich:
            # Manual Enrich pipeline: Crawl → Dirigeants → Finances → Données
            crawl_t = async_map.get("website_crawl", {}).get("total", 0)
            crawl_s = async_map.get("website_crawl", {}).get("success", 0)

            off_t = async_map.get("officers_found", {}).get("total", 0)
            off_s = async_map.get("officers_found", {}).get("success", 0)

            fin_t = async_map.get("financial_data", {}).get("total", 0)
            fin_s = async_map.get("financial_data", {}).get("success", 0)

            steps.append({
                "label": "🕸️ Crawl",
                "detail": f"{crawl_s}/{crawl_t} sites" if crawl_t > 0 else "En attente...",
                "value": crawl_s,
                "total": crawl_t,
                "pct": round(100 * crawl_s / max(crawl_t, 1)) if crawl_t > 0 else 0,
            })
            steps.append({
                "label": "👔 Dirigeants",
                "detail": f"{off_s}/{off_t} trouvés" if off_t > 0 else "En attente...",
                "value": off_s,
                "total": off_t,
                "pct": round(100 * off_s / max(off_t, 1)) if off_t > 0 else 0,
            })
            steps.append({
                "label": "💶 Finances",
                "detail": f"{fin_s}/{fin_t} bilans" if fin_t > 0 else "En attente...",
                "value": fin_s,
                "total": fin_t,
                "pct": round(100 * fin_s / max(fin_t, 1)) if fin_t > 0 else 0,
            })
            steps.append({
                "label": "📊 Données",
                "detail": f"📞 {phone_pct}%  ✉️ {email_pct}%  🌐 {web_pct}%",
                "value": with_phone + with_email + with_website,
                "total": tagged * 3,
                "pct": round((phone_pct + email_pct + web_pct) / 3),
            })

            summary_parts.append(f"Action manuelle sur {total} entreprise(s)")
            if crawl_t > 0:
                summary_parts.append(f"Crawler a analysé {crawl_s} cibles")
            if off_s > 0 or fin_s > 0:
                summary_parts.append(f"{off_s} dirigeants et {fin_s} liasses financières extraits")

        else:
            # Discovery pipeline: SIRENE → Maps → Crawl → Dirigeants → Finances → Données
            maps_action = action_map.get("maps_lookup", {})
            maps_total = maps_action.get("total", 0) or scraped
            maps_success = maps_action.get("success", 0) or 0

            crawl_t = async_map.get("website_crawl", {}).get("total", 0)
            crawl_s = async_map.get("website_crawl", {}).get("success", 0)

            off_t = async_map.get("officers_found", {}).get("total", 0)
            off_s = async_map.get("officers_found", {}).get("success", 0)

            fin_t = async_map.get("financial_data", {}).get("total", 0)
            fin_s = async_map.get("financial_data", {}).get("success", 0)

            steps.append({
                "label": "📋 SIRENE",
                "detail": f"{total} entreprises trouvées",
                "value": total,
                "total": b["batch_size"] or total,
                "pct": round(100 * total / max(b["batch_size"] or total, 1)),
            })
            steps.append({
                "label": "🗺️ Maps",
                "detail": f"{maps_success}/{maps_total} traitées",
                "value": maps_success,
                "total": maps_total,
                "pct": round(100 * maps_success / max(maps_total, 1)),
            })
            steps.append({
                "label": "🕸️ Crawl",
                "detail": f"{crawl_s}/{crawl_t} sites" if crawl_t > 0 else "En attente...",
                "value": crawl_s,
                "total": crawl_t,
                "pct": round(100 * crawl_s / max(crawl_t, 1)) if crawl_t > 0 else 0,
            })
            steps.append({
                "label": "👔 Dirigeants",
                "detail": f"{off_s}/{off_t} trouvés" if off_t > 0 else "En attente...",
                "value": off_s,
                "total": off_t,
                "pct": round(100 * off_s / max(off_t, 1)) if off_t > 0 else 0,
            })
            steps.append({
                "label": "💶 Finances",
                "detail": f"{fin_s}/{fin_t} bilans" if fin_t > 0 else "En attente...",
                "value": fin_s,
                "total": fin_t,
                "pct": round(100 * fin_s / max(fin_t, 1)) if fin_t > 0 else 0,
            })
            steps.append({
                "label": "📊 Données",
                "detail": f"📞 {phone_pct}%  ✉️ {email_pct}%  🌐 {web_pct}%",
                "value": with_phone + with_email + with_website,
                "total": tagged * 3,
                "pct": round((phone_pct + email_pct + web_pct) / 3),
            })

            summary_parts.append(f"{total} entreprises extraites de SIRENE")
            if reused > 0:
                summary_parts.append(f"({reused} déjà en base de données, économie de requêtes)")

            if maps_total > 0:
                summary_parts.append(f"Google Maps a trouvé {maps_success} correspondances")

            if crawl_t > 0:
                summary_parts.append(f"Le Crawler a analysé {crawl_s} sites web")
            if off_s > 0 or fin_s > 0:
                summary_parts.append(f"Enrichissement profond: {off_s} dirigeants et {fin_s} données financières récupérés")

        summary = ". ".join(summary_parts) + "." if summary_parts else "Aucune donnée."

        results.append({
            "batch_id": batch_id,
            "batch_name": batch_name,
            "created_at": str(b["created_at"]) if b["created_at"] else None,
            "status": b["status"],
            "is_upload": is_upload,
            "steps": steps,
            "hit_rates": {
                "total": tagged,
                "with_phone": with_phone, "phone_pct": phone_pct,
                "with_email": with_email, "email_pct": email_pct,
                "with_website": with_website, "website_pct": web_pct,
            },
            "summary": summary,
        })

    return {"batches": results}


# ---------------------------------------------------------------------------
# All Data — browse all enriched entities with contact info
# ---------------------------------------------------------------------------

@router.get("/all-data")
async def get_all_data(
    request: Request,
    q: str = "",
    department: str = "",
    naf_code: str = "",
    limit: int = 50,
    offset: int = 0,
):
    """Browse all enriched companies with their best contact data.

    Searches companies that have at least one contact row (enriched).
    Supports text search (name/SIREN), department filter, and NAF code filter.
    """
    # Build search filters
    where_parts: list[str] = []
    params: list = []

    if q.strip():
        clean = q.strip()
        digits = clean.replace(" ", "")
        if digits.isdigit() and len(digits) == 9:
            where_parts.append("co.siren = %s")
            params.append(digits)
        else:
            where_parts.append("UPPER(co.denomination) LIKE UPPER(%s)")
            params.append(f"%{clean}%")

    if department.strip():
        where_parts.append("co.departement = %s")
        params.append(department.strip())

    if naf_code.strip():
        where_parts.append("co.naf_code ILIKE %s")
        params.append(f"{naf_code.strip()}%")

    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        where_parts.append("co.siren IN (SELECT qt.siren FROM batch_tags qt WHERE qt.workspace_id = %s)")
        params.append(user.workspace_id)

    where_clause = (" AND ".join(where_parts)) if where_parts else "TRUE"

    # Count total enriched companies matching filters
    count_params = tuple(params)
    total_row = await fetch_one(f"""
        SELECT COUNT(DISTINCT co.siren) AS total
        FROM companies co
        WHERE EXISTS (SELECT 1 FROM contacts ct WHERE ct.siren = co.siren)
          AND {where_clause}
    """, count_params)
    total = (total_row or {}).get("total", 0) or 0

    all_params = tuple(params + [limit, offset])
    # Performance note: merged_contacts CTE uses ARRAY_AGG with source priority
    # to pick the best value per field across all contact rows. The subquery
    # scopes it to enriched companies only (those with at least one contact).
    rows = await fetch_all(f"""
        WITH {merged_contacts_cte('SELECT DISTINCT siren FROM contacts')}
        SELECT
            co.siren, co.denomination, co.naf_code, co.naf_libelle,
            co.forme_juridique, co.ville, co.departement,
            ct.phone, ct.email, ct.website,
            qt.batch_name
        FROM companies co
        LEFT JOIN merged_contacts ct ON ct.siren = co.siren
        LEFT JOIN LATERAL (
            SELECT batch_name FROM batch_tags bt
            WHERE bt.siren = co.siren
            ORDER BY bt.tagged_at DESC
            LIMIT 1
        ) qt ON true
        WHERE EXISTS (SELECT 1 FROM contacts ct2 WHERE ct2.siren = co.siren)
          AND {where_clause}
        ORDER BY co.denomination
        LIMIT %s OFFSET %s
    """, all_params)

    return {"results": rows, "total": total, "offset": offset, "limit": limit}


# ---------------------------------------------------------------------------
# By-Sector Stats — accurate unique SIREN counts from batch_tags
# ---------------------------------------------------------------------------

@router.get("/stats/by-sector")
async def get_stats_by_sector(request: Request):
    """Sector-level stats using unique SIRENs from batch_tags.
    
    Uses a CTE to pre-split batch_name, avoiding SQL grouping violations
    when calculating batch counts and running status in subqueries.
    """
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        wid = user.workspace_id
        scope_clause = "WHERE qt.workspace_id = %s"
        scope_params: tuple = (wid,)
        bd_ws = "AND sj2.workspace_id = %s"
        bd_ws2 = "AND sj3.workspace_id = %s"
        extra_params: tuple = (wid, wid)
    else:
        scope_clause = ""
        scope_params = ()
        bd_ws = ""
        bd_ws2 = ""
        extra_params = ()

    rows = await fetch_all(f"""
        WITH {merged_contacts_cte('SELECT DISTINCT qt2.siren FROM batch_tags qt2' + (' WHERE qt2.workspace_id = %s' if scope_params else ''))},
        sector_data AS (
            SELECT
                qt.siren,
                UPPER(SPLIT_PART(qt.batch_name, ' ', 1)) AS normalized_sector,
                SPLIT_PART(qt.batch_name, ' ', 2) AS normalized_dept,
                mc.phone, mc.email, mc.website
            FROM batch_tags qt
            LEFT JOIN merged_contacts mc ON mc.siren = qt.siren
            {scope_clause}
        )
        SELECT
            normalized_sector AS sector,
            COUNT(DISTINCT siren) AS companies,
            COUNT(DISTINCT CASE WHEN phone IS NOT NULL THEN siren END) AS with_phone,
            COUNT(DISTINCT CASE WHEN email IS NOT NULL THEN siren END) AS with_email,
            COUNT(DISTINCT CASE WHEN website IS NOT NULL THEN siren END) AS with_website,
            COUNT(DISTINCT normalized_dept) FILTER (WHERE normalized_dept != '') AS dept_count,
            (SELECT COUNT(DISTINCT sj2.batch_id)
             FROM batch_data sj2
             WHERE UPPER(SPLIT_PART(sj2.batch_name, ' ', 1)) = normalized_sector
               AND sj2.status != 'deleted' {bd_ws}
            ) AS batch_count,
            (SELECT BOOL_OR(sj3.status = 'in_progress')
             FROM batch_data sj3
             WHERE UPPER(SPLIT_PART(sj3.batch_name, ' ', 1)) = normalized_sector {bd_ws2}
            ) AS has_running,
            ARRAY_AGG(DISTINCT normalized_dept) FILTER (WHERE normalized_dept != '') AS departments
        FROM sector_data
        GROUP BY normalized_sector
        ORDER BY companies DESC
    """, scope_params + scope_params + extra_params if (scope_params or extra_params) else None)
    return rows


# ---------------------------------------------------------------------------
# Pending Links — companies with link_confidence = 'pending'
# ---------------------------------------------------------------------------

@router.get("/pending-links")
async def get_pending_links(request: Request):
    """Return all companies with link_confidence = 'pending', workspace-scoped.

    These are fuzzy-name matches waiting for user confirmation.
    """
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        ws_filter = "AND co.workspace_id = %s"
        ws_params: tuple = (user.workspace_id,)
    else:
        ws_filter = ""
        ws_params = ()

    rows = await fetch_all(f"""
        WITH {merged_contacts_cte("SELECT DISTINCT siren FROM companies WHERE link_confidence = 'pending'")}
        SELECT DISTINCT ON (co.siren)
            co.siren,
            co.denomination,
            co.linked_siren AS suggested_siren,
            target.denomination AS suggested_name,
            target.ville AS suggested_ville,
            target.adresse AS suggested_address,
            target.naf_code AS suggested_naf,
            target.naf_libelle AS suggested_naf_libelle,
            co.link_method,
            co.departement,
            co.ville,
            ct.phone,
            ct.address AS maps_address,
            bt.batch_name
        FROM companies co
        LEFT JOIN companies target ON target.siren = co.linked_siren
        LEFT JOIN merged_contacts ct ON ct.siren = co.siren
        LEFT JOIN LATERAL (
            SELECT batch_name FROM batch_tags WHERE siren = co.siren
            ORDER BY tagged_at DESC LIMIT 1
        ) bt ON true
        WHERE co.link_confidence = 'pending'
          {ws_filter}
        ORDER BY co.siren
    """, ws_params if ws_params else None)

    return {"count": len(rows), "results": rows}


# ---------------------------------------------------------------------------
# Delete endpoints — remove tags (never delete company/contact data)
# ---------------------------------------------------------------------------

@router.delete("/sector/{sector_name}/tags")
async def delete_sector_tags(sector_name: str, request: Request):
    """Remove all batch_tags for a sector. Soft-deletes associated jobs.
    Admin: only affects NULL-workspace data. Head: only affects their workspace.
    Regular users get 403.
    """
    from fastapi.responses import JSONResponse
    from fortress.api.db import get_conn

    user = getattr(request.state, 'user', None)
    if not user or user.role not in ('admin', 'head'):
        return JSONResponse(status_code=403, content={"error": "Accès refusé"})

    if user.is_admin:
        ws_scope = "AND workspace_id IS NULL"
        ws_params: tuple = ()
    else:
        ws_scope = "AND workspace_id = %s"
        ws_params = (user.workspace_id,)

    async with get_conn() as conn:
        # Find all batch_names matching this sector (scoped)
        qnames = await conn.execute(
            f"SELECT DISTINCT batch_name FROM batch_tags WHERE UPPER(SPLIT_PART(batch_name, ' ', 1)) = UPPER(%s) {ws_scope}",
            (sector_name,) + ws_params,
        )
        names = [r[0] for r in await qnames.fetchall()]

        if not names:
            return JSONResponse(status_code=404, content={"error": "Secteur introuvable"})

        # Delete tags (scoped)
        deleted = await conn.execute(
            f"DELETE FROM batch_tags WHERE UPPER(SPLIT_PART(batch_name, ' ', 1)) = UPPER(%s) {ws_scope}",
            (sector_name,) + ws_params,
        )
        tag_count = deleted.rowcount or 0

        # Soft-delete jobs and collect their batch_ids for batch_log cleanup
        sector_batch_ids: list = []
        for name in names:
            bids_result = await conn.execute(
                f"UPDATE batch_data SET status = 'deleted', updated_at = NOW() WHERE batch_name = %s AND status != 'deleted' {ws_scope} RETURNING batch_id",
                (name,) + ws_params,
            )
            for r in await bids_result.fetchall():
                sector_batch_ids.append(r[0])

        # Remove batch_log for deleted batches
        for bid in sector_batch_ids:
            await conn.execute("DELETE FROM batch_log WHERE batch_id = %s", (bid,))

        await conn.commit()

    _invalidate_cache()
    return {"deleted": True, "sector": sector_name, "tags_removed": tag_count, "jobs_affected": len(names)}


@router.delete("/department/{dept}/tags")
async def delete_department_tags(dept: str, request: Request):
    """Remove all batch_tags for companies in a specific department.
    Admin: only affects NULL-workspace tags. Head: only affects their workspace.
    Regular users get 403.
    """
    from fastapi.responses import JSONResponse
    from fortress.api.db import get_conn

    user = getattr(request.state, 'user', None)
    if not user or user.role not in ('admin', 'head'):
        return JSONResponse(status_code=403, content={"error": "Accès refusé"})

    if user.is_admin:
        ws_scope = "AND bt.workspace_id IS NULL"
        ws_params: tuple = ()
    else:
        ws_scope = "AND bt.workspace_id = %s"
        ws_params = (user.workspace_id,)

    async with get_conn() as conn:
        deleted = await conn.execute(f"""
            DELETE FROM batch_tags bt
            WHERE bt.siren IN (
                SELECT siren FROM companies WHERE departement = %s
            ) {ws_scope}
        """, (dept,) + ws_params)
        tag_count = deleted.rowcount or 0
        await conn.commit()

    if tag_count == 0:
        return JSONResponse(status_code=404, content={"error": "Aucun tag trouvé pour ce département"})

    _invalidate_cache()
    return {"deleted": True, "department": dept, "tags_removed": tag_count}


@router.delete("/job-group/{batch_name}")
async def delete_job_group(batch_name: str, request: Request):
    """Soft-delete all batches with this batch_name (case-insensitive) and remove their tags.
    Admin: only affects NULL-workspace batches. Head: only affects their workspace.
    Regular users get 403.
    """
    from fastapi.responses import JSONResponse
    from fortress.api.db import get_conn

    user = getattr(request.state, 'user', None)
    if not user or user.role not in ('admin', 'head'):
        return JSONResponse(status_code=403, content={"error": "Accès refusé"})

    if user.is_admin:
        ws_scope = "AND workspace_id IS NULL"
        ws_params: tuple = ()
    else:
        ws_scope = "AND workspace_id = %s"
        ws_params = (user.workspace_id,)

    async with get_conn() as conn:
        # Soft-delete all matching jobs (scoped)
        jobs_result = await conn.execute(
            f"UPDATE batch_data SET status = 'deleted', updated_at = NOW() WHERE UPPER(batch_name) = UPPER(%s) AND status != 'deleted' {ws_scope} RETURNING batch_id",
            (batch_name,) + ws_params,
        )
        job_ids = [r[0] for r in await jobs_result.fetchall()]

        if not job_ids:
            return JSONResponse(status_code=404, content={"error": "Groupe de jobs introuvable ou accès refusé"})

        # Remove batch_tags for this group (scoped)
        tag_result = await conn.execute(
            f"DELETE FROM batch_tags WHERE UPPER(batch_name) = UPPER(%s) {ws_scope}",
            (batch_name,) + ws_params,
        )
        tag_count = tag_result.rowcount or 0

        # Remove batch_log for deleted batches
        for bid in job_ids:
            await conn.execute("DELETE FROM batch_log WHERE batch_id = %s", (bid,))

        await conn.commit()

    _invalidate_cache()
    return {"deleted": True, "batch_name": batch_name, "jobs_deleted": len(job_ids), "tags_removed": tag_count}
