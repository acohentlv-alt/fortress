"""Contacts List API — flat view of all enriched contacts + officers.

Returns a paginated, searchable list of all contacts across all enriched
companies. Joins contacts + officers via LATERAL to pick the "best" record
per company. Only queries companies linked via query_tags (enriched data),
never the full 14.7M SIRENE table.
"""

from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from fortress.api.db import fetch_all, fetch_one

router = APIRouter(prefix="/api/contacts", tags=["contacts_list"])


@router.get("/list")
async def list_contacts(
    q: str = Query(None, description="Search by name, SIREN, phone, or email"),
    department: str = Query(None, description="Filter by department code"),
    limit: int = Query(50, ge=1, le=100),
    offset: int = Query(0, ge=0),
):
    """Return a flat, paginated list of all enriched contacts + officers."""

    where_parts: list[str] = []
    params: list = []

    if q:
        clean_q = q.strip()
        where_parts.append("""(
            co.denomination ILIKE %s
            OR co.siren = %s
            OR ct.phone ILIKE %s
            OR ct.email ILIKE %s
            OR o.nom ILIKE %s
            OR o.prenom ILIKE %s
            OR o.ligne_directe ILIKE %s
            OR o.email_direct ILIKE %s
        )""")
        like = f"%{clean_q}%"
        params.extend([like, clean_q, like, like, like, like, like, like])

    if department:
        where_parts.append("co.departement = %s")
        params.append(department.strip())

    where_clause = " AND ".join(where_parts) if where_parts else "TRUE"

    try:
        # Count total (for pagination)
        count_row = await fetch_one(f"""
            SELECT COUNT(DISTINCT co.siren) AS total
            FROM query_tags qt
            JOIN companies co ON co.siren = qt.siren
            LEFT JOIN LATERAL (
                SELECT * FROM contacts c WHERE c.siren = co.siren
                ORDER BY (c.phone IS NOT NULL)::int DESC
                LIMIT 1
            ) ct ON true
            LEFT JOIN LATERAL (
                SELECT * FROM officers off2 WHERE off2.siren = co.siren
                ORDER BY (off2.ligne_directe IS NOT NULL)::int DESC
                LIMIT 1
            ) o ON true
            WHERE {where_clause}
        """, tuple(params) if params else None)
        total = (count_row or {}).get("total", 0)

        # Fetch page
        page_params = list(params) + [limit, offset]
        rows = await fetch_all(f"""
            SELECT DISTINCT ON (co.siren)
                co.siren, co.denomination, co.departement, co.naf_code,
                co.ville, co.naf_libelle,
                ct.phone, ct.email, ct.website, ct.source AS contact_source,
                ct.social_linkedin, ct.rating, ct.review_count,
                o.nom AS dirigeant_nom, o.prenom AS dirigeant_prenom,
                o.role AS dirigeant_role, o.email_direct, o.ligne_directe
            FROM query_tags qt
            JOIN companies co ON co.siren = qt.siren
            LEFT JOIN LATERAL (
                SELECT * FROM contacts c WHERE c.siren = co.siren
                ORDER BY (c.phone IS NOT NULL)::int DESC
                LIMIT 1
            ) ct ON true
            LEFT JOIN LATERAL (
                SELECT * FROM officers off2 WHERE off2.siren = co.siren
                ORDER BY (off2.ligne_directe IS NOT NULL)::int DESC
                LIMIT 1
            ) o ON true
            WHERE {where_clause}
            ORDER BY co.siren, co.denomination
            LIMIT %s OFFSET %s
        """, tuple(page_params))

        return {
            "results": rows or [],
            "total": total,
            "limit": limit,
            "offset": offset,
        }

    except RuntimeError as exc:
        return JSONResponse(status_code=503, content={"error": str(exc)})
    except Exception as exc:
        error_str = str(exc)
        if "statement timeout" in error_str.lower():
            return JSONResponse(
                status_code=408,
                content={"error": "Recherche trop large — affinez vos filtres"},
            )
        return JSONResponse(
            status_code=500,
            content={"error": f"Erreur: {error_str}"},
        )
