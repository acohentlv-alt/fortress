"""Export API routes — CSV, XLSX, and JSONL downloads."""

import csv
import decimal
import io
import json


class _DecimalEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, decimal.Decimal):
            return float(obj)
        return super().default(obj)

from fastapi import APIRouter, Request
from fastapi.responses import StreamingResponse
from pydantic import BaseModel

from fortress.api.db import fetch_all, fetch_one
from fortress.api.sql_helpers import merged_contacts_cte

router = APIRouter(prefix="/api/export", tags=["export"])

# Map SIRENE numeric codes to human-readable legal form labels
_FORME_JURIDIQUE_LABELS = {
    '1000': 'Entrepreneur individuel', '5306': 'EURL', '5307': 'SA',
    '5370': 'SAS', '5498': 'EURL', '5499': 'SARL',
    '5505': 'SA', '5510': 'SAS', '5515': 'SNC',
    '5520': 'SCS', '5522': 'SCA', '5525': 'SARL unipersonnelle',
    '5530': 'SELASU', '5532': 'SELAS', '5560': 'SCI', '5599': 'SA',
    '5710': 'SAS', '5720': 'SASU', '9220': 'Association loi 1901',
    '9221': 'Association déclarée', '6316': 'SCOP', '6317': 'SCOP',
}


def _fmt(col_key: str, value) -> str:
    """Format a cell value for export. Converts forme_juridique codes to labels."""
    if not value:
        return ""
    if col_key == "forme_juridique":
        return _FORME_JURIDIQUE_LABELS.get(str(value), str(value))
    return str(value)

_CSV_COLUMNS = [
    ("Nom", "denomination"),
    ("SIREN", "effective_siren"),
    ("SIRET", "siret_siege"),
    ("NAF", "naf_code"),
    ("Activité", "naf_libelle"),
    ("Forme juridique", "forme_juridique"),
    ("Adresse", "adresse"),
    ("Code postal", "code_postal"),
    ("Ville", "ville"),
    ("Département", "departement"),
    ("Statut", "statut"),
    ("Date création", "date_creation"),
    ("Effectif", "tranche_effectif"),
    ("Téléphone", "phone"),
    ("Email", "email"),
    ("Site web", "website"),
    ("Adresse Maps", "address"),
    ("Google Maps URL", "maps_url"),
    ("LinkedIn", "social_linkedin"),
    ("Facebook", "social_facebook"),
    ("Twitter", "social_twitter"),
    ("Note Google", "rating"),
    ("Avis Google", "review_count"),
    ("Notes", "notes"),
]

# SELECT columns used by every export query. Computes the REAL SIREN:
# - Pure SIRENE entities: use co.siren directly
# - MAPS entities with confirmed link: use linked_siren (real SIREN)
# - MAPS entities without confirmed link: NULL (avoids exporting MAPS00001 as "SIREN")
_EXPORT_SELECT = """
    CASE
        WHEN co.siren NOT LIKE 'MAPS%%' THEN co.siren
        WHEN co.link_confidence = 'confirmed' THEN co.linked_siren
        ELSE NULL
    END AS effective_siren,
    co.siret_siege, co.denomination,
    co.naf_code, co.naf_libelle, co.forme_juridique,
    co.adresse, co.code_postal, co.ville,
    co.departement, co.statut, co.date_creation,
    co.tranche_effectif,
    mc.phone, mc.email, mc.website,
    mc.address, mc.maps_url,
    mc.social_linkedin, mc.social_facebook, mc.social_twitter,
    mc.rating, mc.review_count,
    cn.notes
"""

# JOINs used by every export query
_EXPORT_JOINS = """
    LEFT JOIN merged_contacts mc ON mc.siren = co.siren
    LEFT JOIN (
        SELECT siren, STRING_AGG(text, ' | ' ORDER BY created_at DESC) AS notes
        FROM company_notes
        GROUP BY siren
    ) cn ON cn.siren = co.siren
"""


async def _fetch_export_data(batch_id: str) -> list[dict]:
    """Fetch all companies + contacts for a specific batch.

    Scoped via batch_log (per-batch batch_id), not batch_tags (shared
    batch_name), so each batch export contains only its own companies.
    """
    job = await fetch_one(
        "SELECT batch_id FROM batch_data WHERE batch_id = %s", (batch_id,)
    )
    if not job:
        return []

    return await fetch_all(f"""
        WITH {merged_contacts_cte('SELECT DISTINCT siren FROM batch_log WHERE batch_id = %s')}
        SELECT {_EXPORT_SELECT}
        FROM (SELECT DISTINCT siren FROM batch_log WHERE batch_id = %s) sa
        JOIN companies co ON co.siren = sa.siren
        {_EXPORT_JOINS}
        WHERE (co.siren NOT LIKE 'MAPS%%' OR co.link_confidence = 'confirmed')
          AND (co.naf_status IS DISTINCT FROM 'mismatch' OR co.link_method IN ('chain', 'gemini_judge'))
        ORDER BY co.denomination
    """, (batch_id, batch_id))


# ── MASTER EXPORT (must be declared BEFORE parameterised routes) ──


@router.get("/master/csv")
async def export_master_csv(request: Request):
    """Download all SCRAPED companies across all queries as CSV.

    Admin only. Scoped to batch_tags (only companies we've actually processed),
    not the full 14.7M SIRENE table.
    """
    user = getattr(request.state, 'user', None)
    if not user or user.role != 'admin':
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=403, content={"error": "Admin uniquement"})
    rows = await fetch_all(f"""
        WITH {merged_contacts_cte('SELECT DISTINCT siren FROM batch_tags')}
        SELECT {_EXPORT_SELECT}
        FROM (SELECT DISTINCT siren FROM batch_tags) qt
        JOIN companies co ON co.siren = qt.siren
        {_EXPORT_JOINS}
        WHERE (co.siren NOT LIKE 'MAPS%%' OR co.link_confidence = 'confirmed')
          AND (co.naf_status IS DISTINCT FROM 'mismatch' OR co.link_method IN ('chain', 'gemini_judge'))
        ORDER BY co.denomination
    """)
    if not rows:
        return StreamingResponse(
            io.BytesIO(b"No data"),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=fortress_master.csv"},
        )

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";")
    writer.writerow([col[0] for col in _CSV_COLUMNS])
    for row in rows:
        writer.writerow([
            _fmt(col[1], row.get(col[1])) for col in _CSV_COLUMNS
        ])

    content = buf.getvalue().encode("utf-8-sig")
    return StreamingResponse(
        io.BytesIO(content),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=fortress_master.csv"},
    )


@router.get("/master/xlsx")
async def export_master_xlsx(request: Request):
    """Download all SCRAPED companies across all queries as XLSX. Admin only."""
    user = getattr(request.state, 'user', None)
    if not user or user.role != 'admin':
        from fastapi.responses import JSONResponse
        return JSONResponse(status_code=403, content={"error": "Admin uniquement"})
    rows = await fetch_all(f"""
        WITH {merged_contacts_cte('SELECT DISTINCT siren FROM batch_tags')}
        SELECT {_EXPORT_SELECT}
        FROM (SELECT DISTINCT siren FROM batch_tags) qt
        JOIN companies co ON co.siren = qt.siren
        {_EXPORT_JOINS}
        WHERE (co.siren NOT LIKE 'MAPS%%' OR co.link_confidence = 'confirmed')
          AND (co.naf_status IS DISTINCT FROM 'mismatch' OR co.link_method IN ('chain', 'gemini_judge'))
        ORDER BY co.denomination
    """)
    return _to_xlsx(rows, "fortress_master.xlsx")


# ── FILTERED CONTACTS EXPORT (must be declared BEFORE {batch_id} routes) ──


@router.get("/contacts/csv")
async def export_contacts_filtered_csv(
    request: Request,
    q: str = None,
    department: str = None,
    naf_code: str = None,
):
    """Export contacts matching the current filters as CSV.

    Uses the same filter logic as /api/contacts/list. Returns ALL matching
    contacts (capped at 10,000), not just the currently loaded page.
    """
    user = getattr(request.state, "user", None)

    where_parts: list[str] = []
    params: list = []

    # Workspace filter — admin sees all, others scoped to their workspace
    if user and not user.is_admin:
        where_parts.append("qt.workspace_id = %s")
        params.append(user.workspace_id)

    if q:
        clean_q = q.strip()
        clean_digits = clean_q.replace(" ", "")
        if clean_digits.isdigit():
            where_parts.append("(co.siren LIKE %s)")
            params.append(f"{clean_digits}%")
        elif clean_q.upper().startswith("MAPS"):
            where_parts.append("(co.siren ILIKE %s)")
            params.append(f"{clean_q}%")
        elif len(clean_q) <= 2:
            where_parts.append("co.denomination LIKE %s")
            params.append(f"{clean_q.upper()}%")
        else:
            # Multi-field search — must match contacts_list.py exactly
            where_parts.append("""(
                co.denomination ILIKE %s
                OR co.enseigne ILIKE %s
                OR co.siren ILIKE %s
                OR co.ville ILIKE %s
                OR co.naf_code ILIKE %s
                OR co.departement = %s
                OR mc.phone ILIKE %s
                OR mc.email ILIKE %s
                OR mc.website ILIKE %s
                OR o.nom ILIKE %s
                OR o.prenom ILIKE %s
            )""")
            like = f"%{clean_q}%"
            dept_q = clean_q.strip()
            params.extend([like, like, like, like, like, dept_q, like, like, like, like, like])

    if department:
        where_parts.append("co.departement = %s")
        params.append(department.strip())

    if naf_code:
        where_parts.append("co.naf_code LIKE %s")
        params.append(f"{naf_code.strip().upper()}%")

    # Exclude pending MAPS rows — they have no confirmed SIRENE data and are not export-ready
    where_parts.append("(co.siren NOT LIKE 'MAPS%%' OR co.link_confidence = 'confirmed')")
    # Exclude NAF-mismatch rows — except chain and gemini_judge matches which are included (high-confidence)
    where_parts.append("(co.naf_status IS DISTINCT FROM 'mismatch' OR co.link_method IN ('chain', 'gemini_judge'))")

    where_clause = " AND ".join(where_parts) if where_parts else "TRUE"

    # Cap at 10k rows — safety net, prevents massive exports.
    # DISTINCT ON uses COALESCE(linked_siren, co.siren) as the dedup key:
    # - MAPS entities linked to a real SIREN (confirmed OR pending) collapse into one row
    # - Unmatched MAPS entities (linked_siren IS NULL) stay as distinct rows (via siren)
    # - Pure SIRENE entities dedup by their own siren
    rows = await fetch_all(f"""
        WITH {merged_contacts_cte('SELECT DISTINCT qt2.siren FROM batch_tags qt2')}
        SELECT DISTINCT ON (COALESCE(co.linked_siren, co.siren)) {_EXPORT_SELECT}
        FROM batch_tags qt
        JOIN companies co ON co.siren = qt.siren
        LEFT JOIN merged_contacts mc ON mc.siren = co.siren
        LEFT JOIN LATERAL (
            SELECT * FROM officers off2 WHERE off2.siren = co.siren
            ORDER BY (off2.ligne_directe IS NOT NULL)::int DESC
            LIMIT 1
        ) o ON true
        LEFT JOIN (
            SELECT siren, STRING_AGG(text, ' | ' ORDER BY created_at DESC) AS notes
            FROM company_notes
            GROUP BY siren
        ) cn ON cn.siren = co.siren
        WHERE {where_clause}
        ORDER BY COALESCE(co.linked_siren, co.siren), co.denomination
        LIMIT 10000
    """, tuple(params) if params else None)

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";")
    writer.writerow([col[0] for col in _CSV_COLUMNS])
    for row in (rows or []):
        writer.writerow([_fmt(col[1], row.get(col[1])) for col in _CSV_COLUMNS])

    content = buf.getvalue().encode("utf-8-sig")

    # Build descriptive filename from filters
    filename_parts = ["contacts"]
    if department:
        filename_parts.append(f"dept{department}")
    if naf_code:
        filename_parts.append(f"naf{naf_code}")
    if q:
        filename_parts.append(q.strip()[:20].replace(" ", "_"))
    filename = "_".join(filename_parts) + ".csv"

    return StreamingResponse(
        io.BytesIO(content),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


# ── PER-QUERY EXPORTS ────────────────────────────────────────────


@router.get("/{batch_id}/csv")
async def export_csv(batch_id: str, request: Request):
    """Download all companies for a query as CSV."""
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        batch = await fetch_one(
            "SELECT workspace_id FROM batch_data WHERE batch_id = %s", (batch_id,)
        )
        if not batch or batch["workspace_id"] != user.workspace_id:
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=403, content={"error": "Accès refusé."})
    rows = await _fetch_export_data(batch_id)
    if not rows:
        return StreamingResponse(
            io.BytesIO(b"No data"),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={batch_id}.csv"},
        )

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";")
    writer.writerow([col[0] for col in _CSV_COLUMNS])
    for row in rows:
        writer.writerow([
            _fmt(col[1], row.get(col[1])) for col in _CSV_COLUMNS
        ])

    content = buf.getvalue().encode("utf-8-sig")  # BOM for Excel
    return StreamingResponse(
        io.BytesIO(content),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={batch_id}.csv"},
    )


@router.get("/{batch_id}/jsonl")
async def export_jsonl(batch_id: str, request: Request):
    """Download all companies for a query as JSONL."""
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        batch = await fetch_one(
            "SELECT workspace_id FROM batch_data WHERE batch_id = %s", (batch_id,)
        )
        if not batch or batch["workspace_id"] != user.workspace_id:
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=403, content={"error": "Accès refusé."})
    rows = await _fetch_export_data(batch_id)
    lines = []
    for row in rows:
        # Convert date objects to strings
        clean = {}
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                clean[k] = v.isoformat()
            else:
                clean[k] = v
        lines.append(json.dumps(clean, cls=_DecimalEncoder, ensure_ascii=False))

    content = "\n".join(lines).encode("utf-8")
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/jsonl",
        headers={"Content-Disposition": f"attachment; filename={batch_id}.jsonl"},
    )


@router.get("/{batch_id}/xlsx")
async def export_xlsx(batch_id: str, request: Request):
    """Download all companies for a query as XLSX."""
    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        batch = await fetch_one(
            "SELECT workspace_id FROM batch_data WHERE batch_id = %s", (batch_id,)
        )
        if not batch or batch["workspace_id"] != user.workspace_id:
            from fastapi.responses import JSONResponse
            return JSONResponse(status_code=403, content={"error": "Accès refusé."})
    rows = await _fetch_export_data(batch_id)
    return _to_xlsx(rows, f"{batch_id}.xlsx")


# ── BULK EXPORT ──────────────────────────────────────────────────


class BulkExportRequest(BaseModel):
    sirens: list[str]


@router.post("/bulk/csv")
async def export_bulk_csv(body: BulkExportRequest, request: Request):
    """Export selected SIRENs as CSV (from dashboard bulk selection)."""
    if not body.sirens:
        return StreamingResponse(
            io.BytesIO(b"No data"),
            media_type="text/csv",
            headers={"Content-Disposition": "attachment; filename=fortress_selection.csv"},
        )

    user = getattr(request.state, "user", None)
    if user and not user.is_admin:
        # Scope: only export SIRENs that belong to the user's workspace via batch_tags
        rows = await fetch_all(f"""
            WITH workspace_sirens AS (
                SELECT DISTINCT bt.siren
                FROM batch_tags bt
                JOIN batch_data bd ON bd.batch_id = bt.batch_id
                WHERE bd.workspace_id = %s AND bt.siren = ANY(%s)
            ),
            {merged_contacts_cte('SELECT siren FROM workspace_sirens')}
            SELECT {_EXPORT_SELECT}
            FROM workspace_sirens ws
            JOIN companies co ON co.siren = ws.siren
            {_EXPORT_JOINS}
            WHERE (co.siren NOT LIKE 'MAPS%%' OR co.link_confidence = 'confirmed')
              AND (co.naf_status IS DISTINCT FROM 'mismatch' OR co.link_method IN ('chain', 'gemini_judge'))
            ORDER BY co.denomination
        """, (user.workspace_id, body.sirens))
    else:
        rows = await fetch_all(f"""
            WITH {merged_contacts_cte('SELECT UNNEST(%s::text[])')}
            SELECT {_EXPORT_SELECT}
            FROM companies co
            {_EXPORT_JOINS}
            WHERE co.siren = ANY(%s)
              AND (co.siren NOT LIKE 'MAPS%%' OR co.link_confidence = 'confirmed')
              AND (co.naf_status IS DISTINCT FROM 'mismatch' OR co.link_method IN ('chain', 'gemini_judge'))
            ORDER BY co.denomination
        """, (body.sirens, body.sirens))

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";")
    writer.writerow([col[0] for col in _CSV_COLUMNS])
    for row in (rows or []):
        writer.writerow([_fmt(col[1], row.get(col[1])) for col in _CSV_COLUMNS])

    content = buf.getvalue().encode("utf-8-sig")
    return StreamingResponse(
        io.BytesIO(content),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=fortress_selection.csv"},
    )


# ── XLSX helper ──────────────────────────────────────────────────


def _to_xlsx(rows: list[dict] | None, filename: str) -> StreamingResponse:
    """Convert rows to an XLSX file and return as StreamingResponse."""
    from openpyxl import Workbook
    from openpyxl.styles import Font, PatternFill, Alignment

    wb = Workbook()
    ws = wb.active
    ws.title = "Export"

    # Header row
    header_font = Font(bold=True, color="FFFFFF")
    header_fill = PatternFill(start_color="1a1a3e", end_color="1a1a3e", fill_type="solid")
    for col_idx, (label, _) in enumerate(_CSV_COLUMNS, 1):
        cell = ws.cell(row=1, column=col_idx, value=label)
        cell.font = header_font
        cell.fill = header_fill
        cell.alignment = Alignment(horizontal="center")

    # Data rows
    for row_idx, row in enumerate(rows or [], 2):
        for col_idx, (_, key) in enumerate(_CSV_COLUMNS, 1):
            val = row.get(key)
            if hasattr(val, "isoformat"):
                val = val.isoformat()
            ws.cell(row=row_idx, column=col_idx, value=_fmt(key, val))

    # Auto-width (approximate)
    for col_idx, (label, _) in enumerate(_CSV_COLUMNS, 1):
        ws.column_dimensions[ws.cell(row=1, column=col_idx).column_letter].width = max(len(label) + 4, 12)

    buf = io.BytesIO()
    wb.save(buf)
    buf.seek(0)

    return StreamingResponse(
        buf,
        media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )
