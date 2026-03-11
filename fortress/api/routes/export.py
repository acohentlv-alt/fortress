"""Export API routes — CSV and JSONL downloads."""

import csv
import io
import json

from fastapi import APIRouter
from fastapi.responses import StreamingResponse

from fortress.api.db import fetch_all, fetch_one

router = APIRouter(prefix="/api/export", tags=["export"])

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
]


async def _fetch_export_data(query_id: str) -> list[dict]:
    """Fetch all companies + contacts for a specific batch.

    Scoped via scrape_audit (per-batch query_id), not query_tags (shared
    query_name), so each batch export contains only its own companies.
    """
    job = await fetch_one(
        "SELECT query_id FROM scrape_jobs WHERE query_id = %s", (query_id,)
    )
    if not job:
        return []

    return await fetch_all("""
        SELECT
            co.siren, co.siret_siege, co.denomination,
            co.naf_code, co.naf_libelle, co.forme_juridique,
            co.adresse, co.code_postal, co.ville,
            co.departement, co.statut, co.date_creation,
            co.tranche_effectif,
            ct.phone, ct.email, ct.website,
            ct.address, ct.maps_url,
            ct.social_linkedin, ct.social_facebook, ct.social_twitter,
            ct.rating, ct.review_count, ct.source AS contact_source
        FROM (SELECT DISTINCT siren FROM scrape_audit WHERE query_id = %s) sa
        JOIN companies co ON co.siren = sa.siren
        LEFT JOIN LATERAL (
            SELECT * FROM contacts c2
            WHERE c2.siren = co.siren
            ORDER BY (CASE WHEN c2.phone IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN c2.email IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN c2.website IS NOT NULL THEN 1 ELSE 0 END) DESC
            LIMIT 1
        ) ct ON true
        ORDER BY co.denomination
    """, (query_id,))


# ── MASTER EXPORT (must be declared BEFORE parameterised routes) ──


@router.get("/master/csv")
async def export_master_csv():
    """Download all SCRAPED companies across all queries as CSV.

    Scoped to query_tags (only companies we've actually processed),
    not the full 14.7M SIRENE table.
    """
    rows = await fetch_all("""
        SELECT
            co.siren, co.siret_siege, co.denomination,
            co.naf_code, co.naf_libelle, co.forme_juridique,
            co.adresse, co.code_postal, co.ville,
            co.departement, co.statut, co.date_creation,
            co.tranche_effectif,
            ct.phone, ct.email, ct.website,
            ct.address, ct.maps_url,
            ct.social_linkedin, ct.social_facebook, ct.social_twitter,
            ct.rating, ct.review_count, ct.source AS contact_source
        FROM (SELECT DISTINCT siren FROM query_tags) qt
        JOIN companies co ON co.siren = qt.siren
        LEFT JOIN LATERAL (
            SELECT * FROM contacts c2
            WHERE c2.siren = co.siren
            ORDER BY (CASE WHEN c2.phone IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN c2.email IS NOT NULL THEN 1 ELSE 0 END +
                      CASE WHEN c2.website IS NOT NULL THEN 1 ELSE 0 END) DESC
            LIMIT 1
        ) ct ON true
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
            str(row.get(col[1]) or "") for col in _CSV_COLUMNS
        ])

    content = buf.getvalue().encode("utf-8-sig")
    return StreamingResponse(
        io.BytesIO(content),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": "attachment; filename=fortress_master.csv"},
    )


# ── PER-QUERY EXPORTS ────────────────────────────────────────────


@router.get("/{query_id}/csv")
async def export_csv(query_id: str):
    """Download all companies for a query as CSV."""
    rows = await _fetch_export_data(query_id)
    if not rows:
        return StreamingResponse(
            io.BytesIO(b"No data"),
            media_type="text/csv",
            headers={"Content-Disposition": f"attachment; filename={query_id}.csv"},
        )

    buf = io.StringIO()
    writer = csv.writer(buf, delimiter=";")
    writer.writerow([col[0] for col in _CSV_COLUMNS])
    for row in rows:
        writer.writerow([
            str(row.get(col[1]) or "") for col in _CSV_COLUMNS
        ])

    content = buf.getvalue().encode("utf-8-sig")  # BOM for Excel
    return StreamingResponse(
        io.BytesIO(content),
        media_type="text/csv; charset=utf-8",
        headers={"Content-Disposition": f"attachment; filename={query_id}.csv"},
    )


@router.get("/{query_id}/jsonl")
async def export_jsonl(query_id: str):
    """Download all companies for a query as JSONL."""
    rows = await _fetch_export_data(query_id)
    lines = []
    for row in rows:
        # Convert date objects to strings
        clean = {}
        for k, v in row.items():
            if hasattr(v, "isoformat"):
                clean[k] = v.isoformat()
            else:
                clean[k] = v
        lines.append(json.dumps(clean, ensure_ascii=False))

    content = "\n".join(lines).encode("utf-8")
    return StreamingResponse(
        io.BytesIO(content),
        media_type="application/jsonl",
        headers={"Content-Disposition": f"attachment; filename={query_id}.jsonl"},
    )
