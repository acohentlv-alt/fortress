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
    ("SIREN", "siren"),
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
    ("Source", "contact_source"),
    ("Source tel", "phone_source"),
    ("Source email", "email_source"),
    ("Source site", "website_source"),
    ("Notes", "notes"),
]


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
        SELECT
            co.siren, co.siret_siege, co.denomination,
            co.naf_code, co.naf_libelle, co.forme_juridique,
            co.adresse, co.code_postal, co.ville,
            co.departement, co.statut, co.date_creation,
            co.tranche_effectif,
            mc.phone, mc.email, mc.website,
            mc.address, mc.maps_url,
            mc.social_linkedin, mc.social_facebook, mc.social_twitter,
            mc.rating, mc.review_count, mc.contact_source,
            mc.phone_source, mc.email_source, mc.website_source,
            cn.notes
        FROM (SELECT DISTINCT siren FROM batch_log WHERE batch_id = %s) sa
        JOIN companies co ON co.siren = sa.siren
        LEFT JOIN merged_contacts mc ON mc.siren = co.siren
        LEFT JOIN (
            SELECT siren, STRING_AGG(text, ' | ' ORDER BY created_at DESC) AS notes
            FROM company_notes
            GROUP BY siren
        ) cn ON cn.siren = co.siren
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
        SELECT
            co.siren, co.siret_siege, co.denomination,
            co.naf_code, co.naf_libelle, co.forme_juridique,
            co.adresse, co.code_postal, co.ville,
            co.departement, co.statut, co.date_creation,
            co.tranche_effectif,
            mc.phone, mc.email, mc.website,
            mc.address, mc.maps_url,
            mc.social_linkedin, mc.social_facebook, mc.social_twitter,
            mc.rating, mc.review_count, mc.contact_source,
            mc.phone_source, mc.email_source, mc.website_source,
            cn.notes
        FROM (SELECT DISTINCT siren FROM batch_tags) qt
        JOIN companies co ON co.siren = qt.siren
        LEFT JOIN merged_contacts mc ON mc.siren = co.siren
        LEFT JOIN (
            SELECT siren, STRING_AGG(text, ' | ' ORDER BY created_at DESC) AS notes
            FROM company_notes
            GROUP BY siren
        ) cn ON cn.siren = co.siren
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
        SELECT
            co.siren, co.siret_siege, co.denomination,
            co.naf_code, co.naf_libelle, co.forme_juridique,
            co.adresse, co.code_postal, co.ville,
            co.departement, co.statut, co.date_creation,
            co.tranche_effectif,
            mc.phone, mc.email, mc.website,
            mc.address, mc.maps_url,
            mc.social_linkedin, mc.social_facebook, mc.social_twitter,
            mc.rating, mc.review_count, mc.contact_source,
            mc.phone_source, mc.email_source, mc.website_source,
            cn.notes
        FROM (SELECT DISTINCT siren FROM batch_tags) qt
        JOIN companies co ON co.siren = qt.siren
        LEFT JOIN merged_contacts mc ON mc.siren = co.siren
        LEFT JOIN (
            SELECT siren, STRING_AGG(text, ' | ' ORDER BY created_at DESC) AS notes
            FROM company_notes
            GROUP BY siren
        ) cn ON cn.siren = co.siren
        ORDER BY co.denomination
    """)
    return _to_xlsx(rows, "fortress_master.xlsx")


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
            SELECT
                co.siren, co.siret_siege, co.denomination,
                co.naf_code, co.naf_libelle, co.forme_juridique,
                co.adresse, co.code_postal, co.ville,
                co.departement, co.statut, co.date_creation,
                co.tranche_effectif,
                mc.phone, mc.email, mc.website,
                mc.address, mc.maps_url,
                mc.social_linkedin, mc.social_facebook, mc.social_twitter,
                mc.rating, mc.review_count, mc.contact_source,
                mc.phone_source, mc.email_source, mc.website_source,
                cn.notes
            FROM workspace_sirens ws
            JOIN companies co ON co.siren = ws.siren
            LEFT JOIN merged_contacts mc ON mc.siren = co.siren
            LEFT JOIN (
                SELECT siren, STRING_AGG(text, ' | ' ORDER BY created_at DESC) AS notes
                FROM company_notes
                GROUP BY siren
            ) cn ON cn.siren = co.siren
            ORDER BY co.denomination
        """, (user.workspace_id, body.sirens))
    else:
        rows = await fetch_all(f"""
            WITH {merged_contacts_cte('SELECT UNNEST(%s::text[])')}
            SELECT
                co.siren, co.siret_siege, co.denomination,
                co.naf_code, co.naf_libelle, co.forme_juridique,
                co.adresse, co.code_postal, co.ville,
                co.departement, co.statut, co.date_creation,
                co.tranche_effectif,
                mc.phone, mc.email, mc.website,
                mc.address, mc.maps_url,
                mc.social_linkedin, mc.social_facebook, mc.social_twitter,
                mc.rating, mc.review_count, mc.contact_source,
                mc.phone_source, mc.email_source, mc.website_source,
                cn.notes
            FROM companies co
            LEFT JOIN merged_contacts mc ON mc.siren = co.siren
            LEFT JOIN (
                SELECT siren, STRING_AGG(text, ' | ' ORDER BY created_at DESC) AS notes
                FROM company_notes
                GROUP BY siren
            ) cn ON cn.siren = co.siren
            WHERE co.siren = ANY(%s)
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
