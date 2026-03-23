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
            FROM batch_tags qt
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
            (SELECT COUNT(*) FROM batch_data {jobs_scope})    AS total_jobs,
            (SELECT COUNT(*) FROM batch_data
             WHERE status = 'completed') AS completed_jobs,
            (SELECT COUNT(*) FROM batch_data
             WHERE status IN ('in_progress', 'queued', 'triage') AND EXTRACT(EPOCH FROM (NOW() - updated_at)) <= 180) AS running_jobs
        FROM enriched
    """, scope_params)
    return stats or {}


@router.get("/recent-activity")
async def get_recent_activity(request: Request):
    """Last 10 job updates — shared workspace."""
    rows = await fetch_all("""
        SELECT batch_id, batch_name,
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
        FROM batch_data
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
        WITH tag_counts AS (
            SELECT UPPER(batch_name) AS batch_key, COUNT(DISTINCT siren) AS unique_companies
            FROM batch_tags
            GROUP BY UPPER(batch_name)
        )
        SELECT
            UPPER(sj.batch_name) AS batch_name,
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
        {user_filter}
        GROUP BY UPPER(sj.batch_name)
        ORDER BY MAX(sj.updated_at) DESC
    """)

    all_batches = await fetch_all(f"""
        SELECT
            UPPER(sj.batch_name) AS group_key,
            sj.batch_id, sj.batch_name, 
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
        FROM batch_data sj
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
        g["batches"] = batch_map.get(g["batch_name"], [])
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
            COUNT(DISTINCT sj.batch_id) AS total_batches
        FROM batch_tags qt
        LEFT JOIN contacts ct ON ct.siren = qt.siren
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
            SELECT DISTINCT qt.siren FROM batch_tags qt {scope_clause}
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
            SELECT DISTINCT qt.siren FROM batch_tags qt {scope_clause}
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

    # All-time stats from batch_log
    audit_stats = await fetch_all("""
        SELECT
            action,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE result = 'success') AS success,
            ROUND(AVG(duration_ms) FILTER (WHERE result = 'success')) AS avg_time_ms,
            MAX(timestamp) AS last_run
        FROM batch_log
        WHERE action IN ('maps_lookup', 'website_crawl')
        GROUP BY action
    """)

    # Last 24h stats from batch_log
    audit_24h = await fetch_all("""
        SELECT
            action,
            COUNT(*) AS total,
            COUNT(*) FILTER (WHERE result = 'success') AS success
        FROM batch_log
        WHERE action IN ('maps_lookup', 'website_crawl')
          AND timestamp >= NOW() - INTERVAL '24 hours'
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
        FROM batch_data
        WHERE status != 'deleted'
    """)

    # Weekly quality trend (last 12 weeks)
    weekly_trend = await fetch_all(f"""
        WITH weekly_jobs AS (
            SELECT
                DATE_TRUNC('week', sj.created_at) AS week,
                sj.batch_name,
                COUNT(DISTINCT qt.siren) AS companies,
                COUNT(DISTINCT CASE WHEN ct.phone IS NOT NULL THEN qt.siren END) AS with_phone,
                COUNT(DISTINCT CASE WHEN ct.email IS NOT NULL THEN qt.siren END) AS with_email,
                COUNT(DISTINCT CASE WHEN ct.website IS NOT NULL THEN qt.siren END) AS with_website
            FROM batch_data sj
            JOIN batch_tags qt ON UPPER(qt.batch_name) = UPPER(sj.batch_name)
            LEFT JOIN contacts ct ON ct.siren = qt.siren
            WHERE sj.status = 'completed'
              AND sj.created_at >= NOW() - INTERVAL '12 weeks'
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
    """)

    # Last 5 completed/failed jobs (compact)
    recent_jobs = await fetch_all(f"""
        SELECT
            sj.batch_id, sj.batch_name,
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
        FROM batch_data sj
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
    """
    # ── Get last 10 user-facing batches from batch_data ─────────────────
    batches = await fetch_all("""
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
          AND bd.batch_name != ''
        ORDER BY bd.created_at DESC
        LIMIT 10
    """)

    if not batches:
        return {"batches": []}

    results = []
    for b in batches:
        batch_id = b["batch_id"]
        batch_name = b["batch_name"] or ""
        is_upload = batch_name.startswith("Import:")
        is_manual_enrich = batch_id.startswith("MANUAL_ENRICH_")
        total = b["total_companies"] or 0
        scraped = b["companies_scraped"] or 0
        reused = total - scraped if total >= scraped else 0

        # ── 1. Direct synchronous actions (Upload, Maps) ───────────
        actions = await fetch_all("""
            SELECT action, COUNT(*) AS total,
                   COUNT(*) FILTER (WHERE result = 'success') AS success
            FROM batch_log
            WHERE batch_id = %s
            GROUP BY action
        """, (batch_id,))
        action_map = {a["action"]: a for a in (actions or [])}

        # ── 2. Async actions across the company lifecycle ───────────
        # Finding all actions that ever happened to companies in this batch
        async_actions = await fetch_all("""
            SELECT bl.action, COUNT(DISTINCT qt.siren) AS total,
                   COUNT(DISTINCT qt.siren) FILTER (WHERE bl.result = 'success' OR bl.result = 'filtered') AS success
            FROM batch_log bl
            JOIN batch_tags qt ON bl.siren = qt.siren
            WHERE UPPER(qt.batch_name) = UPPER(%s)
            GROUP BY bl.action
        """, (batch_name,))
        async_map = {a["action"]: a for a in (async_actions or [])}

        # ── 3. Final Hit rates from contacts ───────────────────────
        hit = await fetch_one("""
            SELECT
                COUNT(DISTINCT qt.siren) AS tagged,
                COUNT(DISTINCT qt.siren) FILTER (WHERE ct.phone IS NOT NULL) AS with_phone,
                COUNT(DISTINCT qt.siren) FILTER (WHERE ct.email IS NOT NULL) AS with_email,
                COUNT(DISTINCT qt.siren) FILTER (WHERE ct.website IS NOT NULL) AS with_website
            FROM batch_tags qt
            LEFT JOIN contacts ct ON ct.siren = qt.siren
            WHERE UPPER(qt.batch_name) = UPPER(%s)
        """, (batch_name,))
        h = hit or {}
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
            # Maps total is scraped, but we found `total` companies in SIRENE.
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
        FROM batch_tags qt
        JOIN companies co ON co.siren = qt.siren
        WHERE 1=1 {scope_clause} {search_clause} {dept_clause}
    """, count_params)
    total = (total_row or {}).get("total", 0) or 0

    rows = await fetch_all(f"""
        SELECT DISTINCT ON (co.siren)
            co.siren, co.denomination, co.naf_code, co.naf_libelle,
            co.forme_juridique, co.ville, co.departement,
            ct.phone, ct.email, ct.website,
            qt.batch_name, qt.tagged_at
        FROM batch_tags qt
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
# By-Sector Stats — accurate unique SIREN counts from batch_tags
# ---------------------------------------------------------------------------

@router.get("/stats/by-sector")
async def get_stats_by_sector(request: Request):
    """Sector-level stats using unique SIRENs from batch_tags.
    
    Uses a CTE to pre-split batch_name, avoiding SQL grouping violations
    when calculating batch counts and running status in subqueries.
    """
    # Shared workspace — no user scoping
    scope_clause = ""
    scope_params: tuple = ()

    rows = await fetch_all(f"""
        WITH sector_data AS (
            SELECT 
                qt.siren,
                UPPER(SPLIT_PART(qt.batch_name, ' ', 1)) AS normalized_sector,
                SPLIT_PART(qt.batch_name, ' ', 2) AS normalized_dept,
                ct.phone, ct.email, ct.website
            FROM batch_tags qt
            LEFT JOIN contacts ct ON ct.siren = qt.siren
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
               AND sj2.status != 'deleted'
            ) AS batch_count,
            (SELECT BOOL_OR(sj3.status = 'in_progress')
             FROM batch_data sj3
             WHERE UPPER(SPLIT_PART(sj3.batch_name, ' ', 1)) = normalized_sector
            ) AS has_running,
            ARRAY_AGG(DISTINCT normalized_dept) FILTER (WHERE normalized_dept != '') AS departments
        FROM sector_data
        GROUP BY normalized_sector
        ORDER BY companies DESC
    """, scope_params)
    return rows


# ---------------------------------------------------------------------------
# Delete endpoints — remove tags (never delete company/contact data)
# ---------------------------------------------------------------------------

@router.delete("/sector/{sector_name}/tags")
async def delete_sector_tags(sector_name: str, request: Request):
    """Remove all batch_tags for a sector. Soft-deletes associated jobs. Admin only."""
    from fastapi.responses import JSONResponse
    from fortress.api.db import get_conn

    user = getattr(request.state, 'user', None)
    if not user or user.role != 'admin':
        return JSONResponse(status_code=403, content={"error": "Admin uniquement"})

    async with get_conn() as conn:
        # Find all batch_names matching this sector
        qnames = await conn.execute(
            "SELECT DISTINCT batch_name FROM batch_tags WHERE UPPER(SPLIT_PART(batch_name, ' ', 1)) = UPPER(%s)",
            (sector_name,),
        )
        names = [r[0] for r in await qnames.fetchall()]

        if not names:
            return JSONResponse(status_code=404, content={"error": "Secteur introuvable"})

        # Delete tags
        deleted = await conn.execute(
            "DELETE FROM batch_tags WHERE UPPER(SPLIT_PART(batch_name, ' ', 1)) = UPPER(%s)",
            (sector_name,),
        )
        tag_count = deleted.rowcount or 0

        # Soft-delete jobs
        for name in names:
            await conn.execute(
                "UPDATE batch_data SET status = 'deleted', updated_at = NOW() WHERE batch_name = %s AND status != 'deleted'",
                (name,),
            )
        await conn.commit()

    return {"deleted": True, "sector": sector_name, "tags_removed": tag_count, "jobs_affected": len(names)}


@router.delete("/department/{dept}/tags")
async def delete_department_tags(dept: str, request: Request):
    """Remove all batch_tags for companies in a specific department. Admin only."""
    from fastapi.responses import JSONResponse
    from fortress.api.db import get_conn

    user = getattr(request.state, 'user', None)
    if not user or user.role != 'admin':
        return JSONResponse(status_code=403, content={"error": "Admin uniquement"})

    async with get_conn() as conn:
        deleted = await conn.execute("""
            DELETE FROM batch_tags
            WHERE siren IN (
                SELECT siren FROM companies WHERE departement = %s
            )
        """, (dept,))
        tag_count = deleted.rowcount or 0
        await conn.commit()

    if tag_count == 0:
        return JSONResponse(status_code=404, content={"error": "Aucun tag trouvé pour ce département"})

    return {"deleted": True, "department": dept, "tags_removed": tag_count}


@router.delete("/job-group/{batch_name}")
async def delete_job_group(batch_name: str, request: Request):
    """Soft-delete all batches with this batch_name (case-insensitive) and remove their tags. Admin only."""
    from fastapi.responses import JSONResponse
    from fortress.api.db import get_conn

    user = getattr(request.state, 'user', None)
    if not user or user.role != 'admin':
        return JSONResponse(status_code=403, content={"error": "Admin uniquement"})

    async with get_conn() as conn:
        # Soft-delete all matching jobs
        jobs_result = await conn.execute(
            "UPDATE batch_data SET status = 'deleted', updated_at = NOW() WHERE UPPER(batch_name) = UPPER(%s) AND status != 'deleted' RETURNING batch_id",
            (batch_name,),
        )
        job_ids = [r[0] for r in await jobs_result.fetchall()]

        if not job_ids:
            return JSONResponse(status_code=404, content={"error": "Groupe de jobs introuvable"})

        # Remove batch_tags for this group
        tag_result = await conn.execute(
            "DELETE FROM batch_tags WHERE UPPER(batch_name) = UPPER(%s)",
            (batch_name,),
        )
        tag_count = tag_result.rowcount or 0
        await conn.commit()

    return {"deleted": True, "batch_name": batch_name, "jobs_deleted": len(job_ids), "tags_removed": tag_count}
