"""Client API routes — CSV upload for dedup (BLUE triage).

Allows the client to upload their existing CRM database (CSV with SIREN column).
Uploaded SIRENs are stored in client_sirens and used by triage to skip
companies the client already owns (BLUE classification).
"""

from __future__ import annotations

import csv
import io
import re
from datetime import datetime, timezone

from fastapi import APIRouter, File, UploadFile
from fastapi.responses import JSONResponse

from fortress.api.db import fetch_all, fetch_one, get_conn

router = APIRouter(prefix="/api/client", tags=["client"])

# Match a 9-digit SIREN (may appear with spaces or dashes)
_SIREN_RE = re.compile(r"^\d{9}$")


def _clean_siren(raw: str) -> str | None:
    """Extract a valid 9-digit SIREN from a raw cell value."""
    cleaned = re.sub(r"[\s\-.]", "", raw.strip())
    if _SIREN_RE.match(cleaned):
        return cleaned
    return None


@router.post("/upload")
async def upload_client_csv(file: UploadFile = File(...)):
    """Upload a CSV with a SIREN column. Extracts and stores all valid SIRENs.

    Accepts CSV with any column layout — looks for a column named 'siren',
    'SIREN', 'Siren', or the first column if no header match is found.

    Returns:
        JSON with inserted count, skipped count, and total rows processed.
    """
    # Read entire file (CSVs are small — client CRM is typically < 50K rows)
    content = await file.read()
    try:
        text = content.decode("utf-8-sig")  # Handle BOM from Excel
    except UnicodeDecodeError:
        try:
            text = content.decode("latin-1")
        except UnicodeDecodeError:
            return JSONResponse(
                status_code=400,
                content={"error": "Encodage non reconnu. Utilisez UTF-8 ou Latin-1."},
            )

    # Parse CSV
    reader = csv.reader(io.StringIO(text), delimiter=",")
    rows = list(reader)
    if not rows:
        return JSONResponse(
            status_code=400,
            content={"error": "Fichier CSV vide."},
        )

    # Detect SIREN column
    header = rows[0]
    siren_col = None
    for i, col_name in enumerate(header):
        if col_name.strip().upper() == "SIREN":
            siren_col = i
            break

    # Also try semicolon delimiter if no SIREN column found
    if siren_col is None and ";" in text:
        reader = csv.reader(io.StringIO(text), delimiter=";")
        rows = list(reader)
        header = rows[0]
        for i, col_name in enumerate(header):
            if col_name.strip().upper() == "SIREN":
                siren_col = i
                break

    # Fall back to first column if header doesn't match
    if siren_col is None:
        siren_col = 0

    # Determine if first row is a header or data
    first_is_header = not _clean_siren(header[siren_col]) if header else True

    # Extract valid SIRENs
    sirens: list[str] = []
    seen: set[str] = set()
    data_rows = rows[1:] if first_is_header else rows
    for row in data_rows:
        if len(row) <= siren_col:
            continue
        s = _clean_siren(row[siren_col])
        if s and s not in seen:
            sirens.append(s)
            seen.add(s)

    if not sirens:
        return JSONResponse(
            status_code=400,
            content={"error": "Aucun SIREN valide trouvé dans le fichier."},
        )

    # Bulk upsert into client_sirens
    now = datetime.now(tz=timezone.utc)
    filename = file.filename or "upload"
    inserted = 0
    skipped = 0

    try:
        async with get_conn() as conn:
            # Ensure table exists (idempotent)
            await conn.execute("""
                CREATE TABLE IF NOT EXISTS client_sirens (
                    siren       VARCHAR(9)  PRIMARY KEY,
                    client_id   VARCHAR(50) NOT NULL DEFAULT 'default',
                    source_file TEXT,
                    uploaded_at TIMESTAMP   NOT NULL DEFAULT NOW()
                )
            """)

            for siren in sirens:
                result = await conn.execute(
                    """
                    INSERT INTO client_sirens (siren, client_id, source_file, uploaded_at)
                    VALUES (%s, 'default', %s, %s)
                    ON CONFLICT (siren) DO NOTHING
                    """,
                    (siren, filename, now),
                )
                if result.rowcount and result.rowcount > 0:
                    inserted += 1
                else:
                    skipped += 1
            await conn.commit()
    except RuntimeError as exc:
        return JSONResponse(status_code=503, content={"error": str(exc)})

    return {
        "status": "ok",
        "filename": filename,
        "total_rows": len(data_rows),
        "valid_sirens": len(sirens),
        "inserted": inserted,
        "already_existed": skipped,
    }


@router.get("/stats")
async def client_stats():
    """Return count of uploaded client SIRENs and recent uploads."""
    try:
        total = await fetch_one("""
            SELECT COUNT(*) AS count FROM client_sirens
        """)

        recent = await fetch_all("""
            SELECT source_file, COUNT(*) AS siren_count, MAX(uploaded_at) AS uploaded_at
            FROM client_sirens
            GROUP BY source_file
            ORDER BY MAX(uploaded_at) DESC
            LIMIT 10
        """)
    except RuntimeError as exc:
        return JSONResponse(status_code=503, content={"error": str(exc)})

    return {
        "total_sirens": total["count"] if total else 0,
        "uploads": recent or [],
    }


@router.delete("/clear")
async def clear_client_sirens():
    """Remove all uploaded client SIRENs (reset)."""
    try:
        async with get_conn() as conn:
            result = await conn.execute("DELETE FROM client_sirens")
            await conn.commit()
            deleted = result.rowcount or 0
    except RuntimeError as exc:
        return JSONResponse(status_code=503, content={"error": str(exc)})

    return {"status": "ok", "deleted": deleted}
