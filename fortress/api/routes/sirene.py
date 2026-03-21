"""SIRENE search API — queries the full 14.7M companies table.

This endpoint is used by the Base SIRENE page to search ALL French companies,
not just the ones that have been scraped/enriched. No batch_tags join.

Performance safeguards:
  - Minimum 3 characters for name search (prevent full table scan)
  - Maximum 50 results per page
  - 5-second SQL statement timeout
  - Uses existing indexes: PK(siren), idx_companies_dept_naf_statut,
    idx_companies_naf_statut, idx_companies_denomination_trgm (GIN trigram)
"""

import psycopg
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from fortress.api.db import get_conn

router = APIRouter(prefix="/api/sirene", tags=["sirene"])


@router.get("/search")
async def search_sirene(
    q: str = Query(None, description="Search by name, SIREN, or NAF code"),
    limit: int = Query(50, ge=1, le=50),
    offset: int = Query(0, ge=0),
    department: str = Query(None, description="Filter by department code (e.g. 66, 31)"),
    naf_code: str = Query(None, description="Filter by NAF code prefix (e.g. 49, 49.41)"),
    statut: str = Query("A", description="Company status: A=active, F=fermée"),
):
    """Search the raw SIRENE database (14.7M companies).

    Returns SIRENE data only — no enriched fields (phone, email, website).
    For enriched data, use /api/companies/search instead.

    Supports:
      - Exact SIREN lookup (9 digits)
      - Fuzzy name search via trigram similarity (GIN index)
      - NAF code prefix search
      - Department / NAF / status filters (work without a text query)
    """
    clean_q = (q or "").strip()
    has_filters = bool(department or naf_code)

    # Nothing to search — need either a query or at least one filter
    if not clean_q and not has_filters:
        return JSONResponse(
            status_code=400,
            content={"error": "Saisissez un terme de recherche ou sélectionnez un filtre"},
        )

    # Classify query type
    clean_digits = clean_q.replace(" ", "")
    is_siren = clean_digits.isdigit() and len(clean_digits) == 9
    is_naf = len(clean_q) >= 2 and clean_q[0].isdigit() and not is_siren

    if clean_q and not is_siren and not is_naf and len(clean_q) < 3 and not has_filters:
        return JSONResponse(
            status_code=400,
            content={"error": "Minimum 3 caractères pour une recherche par nom"},
        )

    try:
        async with get_conn() as conn:
            # Set statement timeout (10 seconds) for large SIRENE queries
            await conn.execute("SET LOCAL statement_timeout = '10000ms'")

            if is_siren:
                # Exact SIREN lookup — uses PK index (instant)
                async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    await cur.execute("""
                        SELECT siren, denomination, enseigne, naf_code, naf_libelle,
                               forme_juridique, tranche_effectif,
                               adresse, ville, code_postal, departement, statut,
                               EXISTS(SELECT 1 FROM contacts ct WHERE ct.siren = companies.siren) AS is_enriched
                        FROM companies
                        WHERE siren = %s
                    """, (clean_digits,))
                    results = await cur.fetchall()
                return {"results": results, "total": len(results), "count": len(results), "offset": offset, "limit": limit}
            else:
                # Build WHERE clause from query + filters
                where_parts: list[str] = []
                params: list = []
                similarity_q = None  # track for ORDER BY

                if clean_q:
                    if is_naf:
                        # NAF code search — uses idx_companies_naf_statut
                        where_parts.append("naf_code ILIKE %s")
                        params.append(f"{clean_q}%")
                    else:
                        # Hybrid fuzzy search: trigram similarity + substring match
                        # Uses idx_companies_denomination_trgm (GIN index)
                        # Threshold 0.15 is permissive for "cev" → "CEVA"
                        # Use similarity() function to avoid % placeholder escaping issues
                        await conn.execute("SET LOCAL pg_trgm.similarity_threshold = 0.15")
                        where_parts.append("(similarity(denomination, %s) > 0.15 OR denomination ILIKE %s)")
                        params.append(clean_q)
                        params.append(f"%{clean_q}%")
                        similarity_q = clean_q

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

                where_clause = " AND ".join(where_parts) if where_parts else "TRUE"

                # ── COUNT query — get total matching rows ──
                async with conn.cursor() as cur:
                    await cur.execute(
                        f"SELECT COUNT(*) FROM companies WHERE {where_clause}",
                        tuple(params),
                    )
                    total = (await cur.fetchone())[0]

                # ── Data query — fetch page ──
                if similarity_q:
                    order_clause = "ORDER BY similarity(denomination, %s) DESC"
                    data_params = list(params) + [similarity_q, limit, offset]
                else:
                    order_clause = "ORDER BY denomination"
                    data_params = list(params) + [limit, offset]

                async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    await cur.execute(f"""
                        SELECT siren, denomination, enseigne, naf_code, naf_libelle,
                               forme_juridique, tranche_effectif,
                               adresse, ville, code_postal, departement, statut,
                               EXISTS(SELECT 1 FROM contacts ct WHERE ct.siren = companies.siren) AS is_enriched
                        FROM companies
                        WHERE {where_clause}
                        {order_clause}
                        LIMIT %s OFFSET %s
                    """, tuple(data_params))
                    results = await cur.fetchall()

                return {"results": results, "total": total, "count": len(results), "offset": offset, "limit": limit}

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

