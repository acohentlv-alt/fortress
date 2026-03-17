"""SIRENE search API — queries the full 14.7M companies table.

This endpoint is used by the Base SIRENE page to search ALL French companies,
not just the ones that have been scraped/enriched. No query_tags join.

Performance safeguards:
  - Minimum 3 characters for name search (prevent full table scan)
  - Maximum 50 results per page
  - 5-second SQL statement timeout
  - Uses existing indexes: PK(siren), idx_companies_dept_naf_statut, idx_companies_naf_statut
"""

import psycopg
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from fortress.api.db import get_conn

router = APIRouter(prefix="/api/sirene", tags=["sirene"])


@router.get("/search")
async def search_sirene(
    q: str = Query(..., min_length=1, description="Search by name, SIREN, or NAF code"),
    limit: int = Query(50, ge=1, le=50),
    offset: int = Query(0, ge=0),
    department: str = Query(None, description="Filter by department code (e.g. 66, 31)"),
    naf_code: str = Query(None, description="Filter by NAF code prefix (e.g. 49, 49.41)"),
    statut: str = Query("A", description="Company status: A=active, F=fermée"),
):
    """Search the raw SIRENE database (14.7M companies).

    Returns SIRENE data only — no enriched fields (phone, email, website).
    For enriched data, use /api/companies/search instead.
    """
    clean_q = q.strip()

    # Classify query type
    clean_digits = clean_q.replace(" ", "")
    is_siren = clean_digits.isdigit() and len(clean_digits) == 9
    is_naf = len(clean_q) >= 2 and clean_q[0].isdigit()

    if not is_siren and not is_naf and len(clean_q) < 3:
        return JSONResponse(
            status_code=400,
            content={"error": "Minimum 3 caractères pour une recherche par nom"},
        )

    try:
        async with get_conn() as conn:
            # Set statement timeout (5 seconds) to protect against slow queries
            await conn.execute("SET LOCAL statement_timeout = '5000ms'")

            if is_siren:
                # Exact SIREN lookup — uses PK index (instant)
                async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    await cur.execute("""
                        SELECT siren, denomination, enseigne, naf_code, naf_libelle,
                               forme_juridique, tranche_effectif,
                               adresse, ville, code_postal, departement, statut
                        FROM companies
                        WHERE siren = %s
                    """, (clean_digits,))
                    results = await cur.fetchall()
            else:
                # Name / NAF search with optional filters
                where_parts = []
                params: list = []

                if is_naf:
                    # NAF code search — uses idx_companies_naf_statut
                    where_parts.append("naf_code ILIKE %s")
                    params.append(f"{clean_q}%")
                else:
                    # Name search — UPPER + LIKE
                    where_parts.append("UPPER(denomination) LIKE UPPER(%s)")
                    params.append(f"%{clean_q}%")

                # Optional filters
                if department:
                    where_parts.append("departement = %s")
                    params.append(department.strip())

                if naf_code:
                    where_parts.append("naf_code ILIKE %s")
                    params.append(f"{naf_code.strip()}%")

                if statut:
                    where_parts.append("statut = %s")
                    params.append(statut.strip())

                where_clause = " AND ".join(where_parts)
                params.extend([limit, offset])

                async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    await cur.execute(f"""
                        SELECT siren, denomination, enseigne, naf_code, naf_libelle,
                               forme_juridique, tranche_effectif,
                               adresse, ville, code_postal, departement, statut
                        FROM companies
                        WHERE {where_clause}
                        ORDER BY denomination
                        LIMIT %s OFFSET %s
                    """, tuple(params))
                    results = await cur.fetchall()

            return {"results": results, "count": len(results), "offset": offset, "limit": limit}

    except RuntimeError as exc:
        # Database offline
        return JSONResponse(status_code=503, content={"error": str(exc)})
    except Exception as exc:
        error_str = str(exc)
        if "statement timeout" in error_str.lower():
            return JSONResponse(
                status_code=408,
                content={"error": "Recherche trop large — affinez avec un département ou un code NAF"},
            )
        return JSONResponse(
            status_code=500,
            content={"error": f"Erreur de recherche: {error_str}"},
        )
