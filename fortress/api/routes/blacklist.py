"""Blacklist management API routes.

Allows users to add/remove SIRENs from the blacklist so they are
automatically skipped in future batch runs.
"""

from __future__ import annotations

import logging

from fastapi import APIRouter, Request
from fastapi.responses import JSONResponse

from fortress.api.db import fetch_all, get_conn

logger = logging.getLogger("fortress.api.blacklist")

router = APIRouter(tags=["blacklist"])


@router.get("")
async def list_blacklist(request: Request, search: str = ""):
    """List all blacklisted SIRENs with company names.

    Returns: [{siren, reason, added_by, added_at, denomination}]
    Supports ?search= to filter by SIREN or company name.
    """
    if search:
        rows = await fetch_all(
            """
            SELECT b.siren, b.reason, b.added_by, b.added_at,
                   COALESCE(c.denomination, '') AS denomination
            FROM blacklisted_sirens b
            LEFT JOIN companies c ON c.siren = b.siren
            WHERE b.siren ILIKE %s OR c.denomination ILIKE %s
            ORDER BY b.added_at DESC
            """,
            (f"%{search}%", f"%{search}%"),
        )
    else:
        rows = await fetch_all(
            """
            SELECT b.siren, b.reason, b.added_by, b.added_at,
                   COALESCE(c.denomination, '') AS denomination
            FROM blacklisted_sirens b
            LEFT JOIN companies c ON c.siren = b.siren
            ORDER BY b.added_at DESC
            """
        )

    return [
        {
            "siren": r["siren"],
            "reason": r["reason"] or "",
            "added_by": r["added_by"] or "",
            "added_at": r["added_at"].isoformat() if r["added_at"] else None,
            "denomination": r["denomination"],
        }
        for r in rows
    ]


@router.post("")
async def add_to_blacklist(request: Request):
    """Add a SIREN to the blacklist.

    Body: {siren: string, reason: string}
    """
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Corps de requête invalide."})

    siren = (body.get("siren") or "").strip()
    reason = (body.get("reason") or "").strip()

    if not siren:
        return JSONResponse(status_code=400, content={"error": "SIREN requis."})

    if not (siren.isdigit() and len(siren) == 9) and not siren.startswith("MAPS"):
        return JSONResponse(status_code=400, content={
            "error": "SIREN invalide. Saisissez un SIREN à 9 chiffres ou un identifiant MAPS."
        })

    user = request.state.user
    added_by = user.username if user else "unknown"

    async with get_conn() as conn:
        await conn.execute(
            """
            INSERT INTO blacklisted_sirens (siren, reason, added_by)
            VALUES (%s, %s, %s)
            ON CONFLICT (siren) DO UPDATE SET reason = EXCLUDED.reason, added_by = EXCLUDED.added_by
            """,
            (siren, reason, added_by),
        )
        await conn.commit()

    logger.info("blacklist.add", siren=siren, added_by=added_by)
    return {"status": "ok", "siren": siren}


@router.delete("/{siren}")
async def remove_from_blacklist(siren: str, request: Request):
    """Remove a SIREN from the blacklist."""
    async with get_conn() as conn:
        await conn.execute(
            "DELETE FROM blacklisted_sirens WHERE siren = %s",
            (siren,),
        )
        await conn.commit()

    logger.info("blacklist.remove", siren=siren)
    return {"status": "ok"}
