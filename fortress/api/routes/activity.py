"""Activity Log API — admin-only audit trail of user actions.

Insert-only table tracking: batch launches, uploads, deletions, exports, etc.
Auto-creates the table on first use if it doesn't exist.
"""

from __future__ import annotations

import logging
from fastapi import APIRouter, Query, Request
from fastapi.responses import JSONResponse

from fortress.api.db import fetch_all, fetch_one, get_conn

router = APIRouter(prefix="/api/activity", tags=["activity"])
logger = logging.getLogger("fortress.api.activity")

_TABLE_ENSURED = False


async def _ensure_table():
    """Create activity_log table if it doesn't exist (runs once)."""
    global _TABLE_ENSURED
    if _TABLE_ENSURED:
        return
    try:
        async with get_conn() as conn:
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS activity_log (
                    id          SERIAL PRIMARY KEY,
                    user_id     INTEGER REFERENCES users(id),
                    username    VARCHAR(100),
                    action      VARCHAR(50) NOT NULL,
                    target_type VARCHAR(50),
                    target_id   TEXT,
                    details     TEXT,
                    created_at  TIMESTAMPTZ DEFAULT NOW()
                )
            """)
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_activity_log_time ON activity_log (created_at DESC)"
            )
            await conn.execute(
                "CREATE INDEX IF NOT EXISTS idx_activity_log_user ON activity_log (user_id, created_at DESC)"
            )
            await conn.commit()
        _TABLE_ENSURED = True
        logger.info("✅ activity_log table ensured")
    except Exception as e:
        logger.warning("Could not ensure activity_log table: %s", e)


async def log_activity(
    user_id: int | None,
    username: str,
    action: str,
    target_type: str = None,
    target_id: str = None,
    details: str = None,
):
    """Insert an activity log entry. Fire-and-forget — never crashes the caller."""
    try:
        await _ensure_table()
        async with get_conn() as conn:
            await conn.execute("""
                INSERT INTO activity_log (user_id, username, action, target_type, target_id, details)
                VALUES (%s, %s, %s, %s, %s, %s)
            """, (user_id, username, action, target_type, target_id, details))
            await conn.commit()
    except Exception as e:
        logger.warning("Activity log insert failed (non-fatal): %s", e)


@router.get("")
async def get_activity(
    request: Request,
    period: str = Query("week", description="day, week, or month"),
    action_type: str = Query("all", description="all, notes, batches"),
    limit: int = Query(50, ge=1, le=200),
    offset: int = Query(0, ge=0),
):
    """Return recent activity log entries. Admin-only."""
    user = getattr(request.state, "user", None)
    if not user or getattr(user, "role", None) != "admin":
        return JSONResponse(status_code=403, content={"error": "Accès réservé aux administrateurs."})

    await _ensure_table()

    # Map period to interval
    interval_map = {
        "day": "1 day",
        "week": "7 days",
        "month": "30 days",
        "all": "10 years",
    }
    interval = interval_map.get(period, "7 days")

    action_filter = ""
    if action_type == "notes":
        action_filter = "AND action IN ('note_added', 'note_deleted')"
    elif action_type == "batches":
        action_filter = "AND action IN ('batch_launched', 'batch_completed', 'batch_failed', 'upload', 'export', 'cancel_job', 'delete_job')"

    try:
        rows = await fetch_all(f"""
            SELECT id, user_id, username, action, target_type, target_id, details, created_at
            FROM activity_log
            WHERE created_at >= NOW() - %s::interval
            {action_filter}
            ORDER BY created_at DESC
            LIMIT %s OFFSET %s
        """, (interval, limit, offset))

        count_row = await fetch_one(f"""
            SELECT COUNT(*) AS total FROM activity_log
            WHERE created_at >= NOW() - %s::interval
            {action_filter}
        """, (interval,))
        total = (count_row or {}).get("total", 0)

        return {
            "entries": rows or [],
            "total": total,
            "period": period,
        }
    except RuntimeError as exc:
        return JSONResponse(status_code=503, content={"error": str(exc)})
