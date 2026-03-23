"""Contacts List API — flat view of all enriched contacts + officers.

Returns a paginated, searchable list of all contacts across all enriched
companies. Joins contacts + officers via LATERAL to pick the "best" record
per company. Only queries companies linked via batch_tags (enriched data),
never the full 14.7M SIRENE table.

Performance notes (for 3K contacts/month growth):
  - LATERAL JOINs use idx_contacts_siren and idx_officers_siren
  - Smart count: exact for <100, approximate for larger result sets
  - Count uses LIMIT 1001 cap to avoid full scans
"""

from __future__ import annotations

from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from fortress.api.db import fetch_all, fetch_one

router = APIRouter(prefix="/api/contacts", tags=["contacts_list"])

# Maximum rows to count exactly (above this, we approximate)
_COUNT_CAP = 10001


def _format_count(exact_count: int) -> dict:
    """Return a smart count for the frontend.

    Rules (per user spec):
      - Under 100: exact number ("87")
      - 100-999: rounded to nearest 100 ("200+", "500+")
      - 1000-9999: rounded to nearest 500 ("1.5K+", "3K+", "5.5K+")
      - 10000+: "10K+"
    """
    if exact_count < 100:
        return {"total": exact_count, "display": str(exact_count), "exact": True}
    if exact_count >= _COUNT_CAP:
        return {"total": _COUNT_CAP, "display": "10K+", "exact": False}
    if exact_count < 1000:
        rounded = (exact_count // 100) * 100
        return {"total": exact_count, "display": f"{rounded}+", "exact": False}
    # 1000-9999: round to nearest 500
    rounded = (exact_count // 500) * 500
    thousands = rounded / 1000
    if thousands == int(thousands):
        return {"total": exact_count, "display": f"{int(thousands)}K+", "exact": False}
    return {"total": exact_count, "display": f"{thousands:.1f}K+", "exact": False}


@router.get("")
@router.get("/list")
async def list_contacts(
    q: str = Query(None, description="Search by name, SIREN, phone, or email"),
    department: str = Query(None, description="Filter by department code"),
    naf_code: str = Query(None, description="Filter by NAF code prefix"),
    limit: int = Query(50, ge=1, le=250),
    offset: int = Query(0, ge=0),
):
    """Return a flat, paginated list of all enriched contacts + officers."""

    where_parts: list[str] = []
    params: list = []

    if q:
        clean_q = q.strip()
        # Smart search: digits = SIREN prefix, letters = name prefix / ILIKE
        clean_digits = clean_q.replace(" ", "")
        if clean_digits.isdigit():
            # SIREN prefix search
            where_parts.append("co.siren LIKE %s")
            params.append(f"{clean_digits}%")
        elif len(clean_q) <= 2:
            # Short text: prefix on denomination
            where_parts.append("co.denomination LIKE %s")
            params.append(f"{clean_q.upper()}%")
        else:
            # Longer text: search across multiple fields
            where_parts.append("""(
                co.denomination ILIKE %s
                OR co.siren = %s
                OR ct.phone ILIKE %s
                OR ct.email ILIKE %s
                OR o.nom ILIKE %s
                OR o.prenom ILIKE %s
            )""")
            like = f"%{clean_q}%"
            params.extend([like, clean_q, like, like, like, like])

    if department:
        where_parts.append("co.departement = %s")
        params.append(department.strip())

    if naf_code:
        where_parts.append("co.naf_code LIKE %s")
        params.append(f"{naf_code.strip().upper()}%")

    where_clause = " AND ".join(where_parts) if where_parts else "TRUE"

    # Shared FROM + JOIN clause (used by both count and page queries)
    from_clause = """
        FROM batch_tags qt
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
    """

    try:
        # Smart count — cap at 1001 to avoid full scan
        count_params = list(params) + [_COUNT_CAP]
        count_row = await fetch_one(f"""
            SELECT COUNT(*) AS total FROM (
                SELECT DISTINCT co.siren
                {from_clause}
                WHERE {where_clause}
                LIMIT %s
            ) sub
        """, tuple(count_params) if count_params else None)
        raw_count = (count_row or {}).get("total", 0)
        count_info = _format_count(raw_count)

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
            {from_clause}
            WHERE {where_clause}
            ORDER BY co.siren, co.denomination
            LIMIT %s OFFSET %s
        """, tuple(page_params))

        return {
            "results": rows or [],
            "total": count_info["total"],
            "total_display": count_info["display"],
            "total_exact": count_info["exact"],
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

