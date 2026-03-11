"""Dashboard API routes — global stats and recent activity.

Stats are scoped to companies we actually scraped (via query_tags),
not the full 16M+ sirene import table.
"""

from fastapi import APIRouter

from fortress.api.db import fetch_all, fetch_one

router = APIRouter(prefix="/api/dashboard", tags=["dashboard"])


@router.get("/stats")
async def get_stats():
    """Global dashboard statistics (scraped companies only).

    Uses a single-pass CTE instead of 7 correlated subqueries to
    avoid O(7×N) table scans on every dashboard poll.
    """
    stats = await fetch_one("""
        WITH tagged AS (
            SELECT DISTINCT qt.siren
            FROM query_tags qt
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
            (SELECT COUNT(*) FROM scrape_jobs)                 AS total_jobs,
            (SELECT COUNT(*) FROM scrape_jobs
             WHERE status = 'completed')                      AS completed_jobs,
            (SELECT COUNT(*) FROM scrape_jobs
             WHERE status = 'in_progress')                    AS running_jobs
        FROM enriched
    """)
    return stats or {}


@router.get("/recent-activity")
async def get_recent_activity():
    """Last 10 job updates."""
    rows = await fetch_all("""
        SELECT query_id, query_name, status,
               total_companies, companies_scraped, companies_failed,
               wave_current, wave_total,
               triage_black, triage_green, triage_yellow, triage_red,
               created_at, updated_at
        FROM scrape_jobs
        ORDER BY updated_at DESC
        LIMIT 10
    """)
    return rows


# ---------------------------------------------------------------------------
# Action 3: By-job stats with UPPER() normalization + nested batches
# ---------------------------------------------------------------------------

@router.get("/stats/by-job")
async def get_stats_by_job():
    """Job-level stats aggregated by normalized query_name.

    Uses UPPER(query_name) to merge case-variant duplicates
    (e.g. 'agriculture 66' and 'AGRICULTURE 66') into one row.

    Returns a nested `batches` array within each group to prevent
    the frontend timeline UI from breaking on flat payloads.
    """
    # Step 1: Aggregated summary per normalized query_name
    groups = await fetch_all("""
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
        GROUP BY UPPER(sj.query_name)
        ORDER BY MAX(sj.updated_at) DESC
    """)

    # Step 2: Fetch individual batches for nesting
    all_batches = await fetch_all("""
        SELECT
            UPPER(sj.query_name) AS group_key,
            sj.query_id, sj.query_name, sj.status,
            sj.batch_number, sj.companies_scraped, sj.companies_failed,
            sj.total_companies, sj.wave_current, sj.wave_total,
            sj.triage_green, sj.triage_yellow, sj.triage_red, sj.triage_black,
            sj.created_at, sj.updated_at
        FROM scrape_jobs sj
        ORDER BY sj.created_at DESC
    """)

    # Index batches by normalized group key
    batch_map: dict[str, list[dict]] = {}
    for b in all_batches:
        key = b.pop("group_key")
        batch_map.setdefault(key, []).append(b)

    # Step 3: Merge groups + nested batches
    result = []
    for g in groups:
        g["batches"] = batch_map.get(g["query_name"], [])
        result.append(g)

    return result
