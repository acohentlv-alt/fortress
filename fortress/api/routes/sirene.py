"""SIRENE search API — queries the full 14.7M companies table.

This endpoint is used by the Base SIRENE page to search ALL French companies,
not just the ones that have been scraped/enriched. No batch_tags join.

Search logic:
  - Text input is ALWAYS treated as a name/SIREN prefix search
  - Filters (dept, NAF, statut) ALWAYS apply on top of the text search
  - 1 character works fine when filters narrow the result set
  - Exact 9-digit SIREN → PK lookup (instant)
  - Short text (1-2 chars) → prefix LIKE on denomination + SIREN prefix
  - Longer text (3+) → trigram similarity for fuzzy matching
  - No text + filters only → filter search (fast with B-tree indexes)

Performance:
  - No COUNT(*) — uses LIMIT+1 trick to detect "has more"
  - 10-second statement timeout as safety net
"""

import psycopg
from fastapi import APIRouter, Query
from fastapi.responses import JSONResponse

from fortress.api.db import get_conn

router = APIRouter(prefix="/api/sirene", tags=["sirene"])

_SELECT_COLS = """
    siren, denomination, enseigne, naf_code, naf_libelle,
    forme_juridique, tranche_effectif,
    adresse, ville, code_postal, departement, statut
"""


@router.get("/search")
async def search_sirene(
    q: str = Query(None, description="Search by name or SIREN"),
    limit: int = Query(50, ge=1, le=50),
    offset: int = Query(0, ge=0),
    department: str = Query(None, description="Filter by department code (e.g. 66, 31)"),
    naf_code: str = Query(None, description="Filter by NAF code prefix (e.g. 49, 49.41)"),
    statut: str = Query("A", description="Company status: A=active, F=fermée"),
):
    """Search the raw SIRENE database (14.7M companies).

    The text input searches by company name or SIREN number.
    Filters (department, NAF code) always apply on top.
    """
    clean_q = (q or "").strip()
    has_filters = bool(department or naf_code)

    if not clean_q and not has_filters:
        return JSONResponse(
            status_code=400,
            content={"error": "Saisissez un terme de recherche ou sélectionnez un filtre"},
        )

    # Detect exact SIREN (9 digits)
    clean_digits = clean_q.replace(" ", "")
    is_exact_siren = clean_digits.isdigit() and len(clean_digits) == 9

    try:
        async with get_conn() as conn:
            await conn.execute("SET LOCAL statement_timeout = '5000ms'")

            if is_exact_siren:
                # Exact SIREN lookup — PK index (instant)
                async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                    await cur.execute(f"""
                        SELECT {_SELECT_COLS}
                        FROM companies WHERE siren = %s
                    """, (clean_digits,))
                    results = await cur.fetchall()
                return {
                    "results": results, "total": len(results), "has_more": False,
                    "count": len(results), "offset": offset, "limit": limit,
                }

            # ── Build WHERE from text + filters ──────────────────
            where_parts: list[str] = []
            params: list = []
            use_similarity_order = False

            if clean_q:
                is_digit_prefix = clean_digits.isdigit() and len(clean_digits) < 9

                if is_digit_prefix:
                    # Digits → SIREN prefix search (e.g. "3" → SIRENs starting with 3)
                    where_parts.append("siren LIKE %s")
                    params.append(f"{clean_digits}%")
                elif len(clean_q) <= 2:
                    # 1-2 characters → simple prefix on denomination (fast LIKE)
                    where_parts.append("denomination LIKE %s")
                    params.append(f"{clean_q.upper()}%")
                else:
                    # 3+ characters → ILIKE substring search.
                    # pg_trgm's GIN index handles LIKE/ILIKE directly (no % operator needed).
                    # Server-side execution: ~8ms without sort, ~22ms with similarity sort.
                    # Works uniformly with or without department filter — the planner
                    # BitmapAnds the trigram index with idx_companies_dept automatically.
                    where_parts.append("denomination ILIKE %s")
                    params.append(f"%{clean_q}%")
                    use_similarity_order = True

            # Filters always apply
            if department:
                where_parts.append("departement = %s")
                params.append(department.strip())

            if naf_code:
                where_parts.append("naf_code LIKE %s")
                params.append(f"{naf_code.strip().upper()}%")

            if statut:
                where_parts.append("statut = %s")
                params.append(statut.strip())

            where_clause = " AND ".join(where_parts) if where_parts else "TRUE"

            # ── Fetch page (LIMIT+1 to detect more) ──────────────
            fetch_limit = limit + 1
            if use_similarity_order:
                order_clause = "ORDER BY similarity(denomination, %s) DESC"
                data_params = list(params) + [clean_q, fetch_limit, offset]
            elif not clean_q and has_filters:
                # Filter-only (no text): no explicit sort — use natural index order.
                # Sorting by name or SIREN forces a costly sort on large result sets (e.g. Paris).
                order_clause = ""
                data_params = list(params) + [fetch_limit, offset]
            else:
                order_clause = "ORDER BY denomination"
                data_params = list(params) + [fetch_limit, offset]

            async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
                await cur.execute(f"""
                    SELECT {_SELECT_COLS}
                    FROM companies
                    WHERE {where_clause}
                    {order_clause}
                    LIMIT %s OFFSET %s
                """, tuple(data_params))
                results = await cur.fetchall()

            has_more = len(results) > limit
            if has_more:
                results = results[:limit]

            total = (offset + limit + 1) if has_more else (offset + len(results))

            return {
                "results": results, "total": total, "has_more": has_more,
                "count": len(results), "offset": offset, "limit": limit,
            }

    except RuntimeError as exc:
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
