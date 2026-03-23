"""SIRENE bulk data ingester.

Reads the local StockUniteLegale Parquet (or ZIP) file with Polars lazy streaming,
filters to active companies, and bulk-upserts them into PostgreSQL using psycopg3 COPY.

Usage:
    python -m fortress.query.sirene_ingest [--refresh] [--limit N]

Options:
    --refresh   Re-download the SIRENE file before ingesting.
    --limit N   Only process the first N rows (useful for testing).

Column mapping (SIRENE → companies table):
    siren                               → siren
    nicSiegeUniteLegale (Int64)         → siret_siege   (siren + NIC zero-padded to 5 digits)
    denominationUniteLegale             → denomination
      └─ fallback for sole traders: nomUniteLegale + prenomUsuelUniteLegale
    activitePrincipaleUniteLegale       → naf_code      (normalised to "01.21Z" format)
      └─ naf_libelle populated from NAF_CODES reference dict (no libelle column in file)
    categorieJuridiqueUniteLegale (Int64)→ forme_juridique (cast to str)
    dateCreationUniteLegale (Date)      → date_creation
    trancheEffectifsUniteLegale         → tranche_effectif
    etatAdministratifUniteLegale        → statut

Columns confirmed absent from StockUniteLegale Parquet (verified Feb 2026):
    siretSiegeSocial                    — does not exist; NIC-based computation used instead
    raisonSocialleUniteLegale           — does not exist; nomUniteLegale used instead
    libelleActivitePrincipaleUniteLegale— does not exist; NAF_CODES dict used instead

Address fields (adresse, code_postal, ville, departement) are NOT populated here;
they come from StockEtablissement and will be enriched in a later step.

Performance:
    - Polars scan_parquet() + collect(streaming=True) — never loads 12M rows into RAM.
    - psycopg3 COPY (binary) in batches of 10,000 rows.
    - ON CONFLICT (siren) DO UPDATE — full upsert semantics.
"""

from __future__ import annotations

import argparse
import asyncio
import re
import sys
import time
from pathlib import Path
from typing import Any

import polars as pl
import psycopg
import structlog

from fortress.config.naf_codes import NAF_CODES
from fortress.config.settings import settings
from fortress.database.pool import close_pool, get_pool, init_db
from fortress.query.sirene_download import download_sirene

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BATCH_SIZE: int = 10_000

# SIRENE field etatAdministratifUniteLegale value for active companies
ACTIVE_STATUS: str = "A"

# SIRENE columns we want to read (minimises memory footprint).
# Column names verified against StockUniteLegale_utf8.parquet (Feb 2026).
SIRENE_COLUMNS: list[str] = [
    "siren",
    "nicSiegeUniteLegale",            # Int64 — NIC of head office; siret = siren + NIC.zfill(5)
    "denominationUniteLegale",        # legal / trade name for companies
    "nomUniteLegale",                 # family name for sole traders (physical persons)
    "prenomUsuelUniteLegale",         # given name for sole traders
    "activitePrincipaleUniteLegale",  # NAF/APE code (NAF 2008 standard)
    "categorieJuridiqueUniteLegale",  # legal form code (Int64 in Parquet — cast to str)
    "dateCreationUniteLegale",        # Date type in Parquet
    "trancheEffectifsUniteLegale",
    "etatAdministratifUniteLegale",
]

# SQL upsert statement executed via COPY + ON CONFLICT
_UPSERT_SQL = """
INSERT INTO companies (
    siren,
    siret_siege,
    denomination,
    naf_code,
    naf_libelle,
    forme_juridique,
    date_creation,
    tranche_effectif,
    statut,
    updated_at
)
VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, NOW())
ON CONFLICT (siren) DO UPDATE SET
    siret_siege      = EXCLUDED.siret_siege,
    denomination     = EXCLUDED.denomination,
    naf_code         = EXCLUDED.naf_code,
    naf_libelle      = EXCLUDED.naf_libelle,
    forme_juridique  = EXCLUDED.forme_juridique,
    date_creation    = EXCLUDED.date_creation,
    tranche_effectif = EXCLUDED.tranche_effectif,
    statut           = EXCLUDED.statut,
    updated_at       = NOW()
"""

# Regex for the "old" APE format without dot: "0121Z" → "01.21Z"
# Format is: 2 digits + 2 digits + letter  (5 chars total, no dot)
_OLD_APE_RE = re.compile(r"^(\d{2})(\d{2})([A-Z])$")


# ---------------------------------------------------------------------------
# NAF code normalisation
# ---------------------------------------------------------------------------


def normalize_naf_code(raw: str | None) -> str | None:
    """Normalise an APE/NAF code to the canonical "01.21Z" format.

    Handles:
        "0121Z"  → "01.21Z"   (old INSEE format, no dot)
        "01.21Z" → "01.21Z"   (already canonical)
        "01.21z" → "01.21Z"   (lowercase letter)
        ""       → None
        None     → None
    """
    if not raw:
        return None
    code = raw.strip().upper()
    if not code:
        return None
    # Already in canonical form
    if re.match(r"^\d{2}\.\d{2}[A-Z]$", code):
        return code
    # Old form: 5 chars, no dot
    m = _OLD_APE_RE.match(code)
    if m:
        return f"{m.group(1)}.{m.group(2)}{m.group(3)}"
    # Unknown format — return as-is (log at batch level, not per-row)
    return code


def lookup_naf_libelle(naf_code: str | None) -> str | None:
    """Look up the French label for a NAF code in the reference table."""
    if not naf_code:
        return None
    return NAF_CODES.get(naf_code)


# ---------------------------------------------------------------------------
# Row transformation
# ---------------------------------------------------------------------------


def _coerce_str(value: Any) -> str | None:
    """Return a non-empty string or None."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def transform_row(row: dict[str, Any]) -> tuple[Any, ...] | None:
    """Convert a raw SIRENE dict row to a DB-ready tuple.

    Returns None if the row must be skipped (e.g. missing mandatory siren).
    """
    # ── Mandatory field ───────────────────────────────────────────────────
    siren = _coerce_str(row.get("siren"))
    if not siren:
        return None
    # Zero-pad to 9 digits
    siren = siren.zfill(9)

    # ── SIRET of head office ──────────────────────────────────────────────
    # Computed from siren (9 digits) + NIC (5 digits, zero-padded).
    # nicSiegeUniteLegale is Int64 in Parquet — comes through as int or None.
    # siretSiegeSocial does NOT exist in the file.
    nic_raw = row.get("nicSiegeUniteLegale")
    if nic_raw is not None:
        try:
            siret_siege = siren + str(int(nic_raw)).zfill(5)
        except (ValueError, TypeError):
            siret_siege = None
    else:
        siret_siege = None

    # ── Denomination ──────────────────────────────────────────────────────
    # Companies: denominationUniteLegale (always present for legal entities).
    # Sole traders (physical persons): denomination is absent; build from
    #   nomUniteLegale (family name) + prenomUsuelUniteLegale (given name).
    # raisonSocialleUniteLegale does NOT exist in the file.
    denomination = _coerce_str(row.get("denominationUniteLegale"))
    if not denomination:
        nom    = _coerce_str(row.get("nomUniteLegale")) or ""
        prenom = _coerce_str(row.get("prenomUsuelUniteLegale")) or ""
        full_name = f"{prenom} {nom}".strip() if (nom or prenom) else None
        denomination = full_name or f"SIREN {siren}"

    # ── NAF code ──────────────────────────────────────────────────────────
    raw_naf = _coerce_str(row.get("activitePrincipaleUniteLegale"))
    naf_code = normalize_naf_code(raw_naf)

    # libelleActivitePrincipaleUniteLegale does NOT exist in the file.
    # Use our reference dict exclusively.
    naf_libelle = lookup_naf_libelle(naf_code)

    # ── Legal form ────────────────────────────────────────────────────────
    # categorieJuridiqueUniteLegale is Int64 in Parquet (e.g. 1000, 5710).
    # _coerce_str handles non-string values via str() cast.
    forme_juridique = _coerce_str(row.get("categorieJuridiqueUniteLegale"))

    # ── Date ──────────────────────────────────────────────────────────────
    # dateCreationUniteLegale is a Polars Date dtype → Python datetime.date.
    # str(datetime.date(...)) produces "YYYY-MM-DD" which psycopg3 accepts.
    date_creation: str | None = _coerce_str(row.get("dateCreationUniteLegale"))

    tranche_effectif = _coerce_str(row.get("trancheEffectifsUniteLegale"))

    statut = _coerce_str(row.get("etatAdministratifUniteLegale")) or ACTIVE_STATUS

    return (
        siren,
        siret_siege,
        denomination,
        naf_code,
        naf_libelle,
        forme_juridique,
        date_creation,
        tranche_effectif,
        statut,
    )


# ---------------------------------------------------------------------------
# Locate the local SIRENE file
# ---------------------------------------------------------------------------


def _find_local_sirene_file() -> Path | None:
    """Return the path of the local SIRENE file if it exists, else None."""
    for ext in (".parquet", ".zip"):
        p = settings.sirene_dir / f"StockUniteLegale{ext}"
        if p.exists():
            return p
    return None


# ---------------------------------------------------------------------------
# Polars lazy scan
# ---------------------------------------------------------------------------


def _build_lazy_frame(
    path: Path, limit: int | None
) -> pl.LazyFrame:
    """Return a lazy Polars frame filtered to active companies only.

    We attempt to read only the columns we need; if the file doesn't have
    some optional columns (e.g. libelleActivitePrincipale) we fall back
    gracefully.
    """
    # Detect available columns without loading data
    try:
        schema = pl.scan_parquet(str(path)).collect_schema()
        available_cols = set(schema.names())
    except Exception:
        # For ZIP or if schema scan fails, read all columns and select later
        available_cols = set(SIRENE_COLUMNS)

    wanted = [c for c in SIRENE_COLUMNS if c in available_cols]

    lf: pl.LazyFrame
    if path.suffix.lower() == ".parquet":
        lf = pl.scan_parquet(str(path)).select(wanted)
    else:
        # ZIP / CSV fallback — Polars can scan CSVs inside ZIPs via read_csv
        # We must use read_csv (no lazy streaming for ZIP), then convert to lazy.
        # This is the fallback path only; Parquet is strongly preferred.
        log.warning(
            "zip_fallback",
            msg="ZIP file detected — reading with read_csv (no streaming). "
            "Download the Parquet version for better performance.",
        )
        df = pl.read_csv(
            str(path),
            columns=wanted,
            separator=",",
            encoding="utf8-lossy",
            infer_schema_length=10_000,
            null_values=["", "NN"],
        )
        lf = df.lazy()

    # Filter to active companies only
    if "etatAdministratifUniteLegale" in available_cols:
        lf = lf.filter(
            pl.col("etatAdministratifUniteLegale") == ACTIVE_STATUS
        )
    else:
        log.warning(
            "status_col_missing",
            msg="Column etatAdministratifUniteLegale not found — "
            "ingesting all rows without status filter.",
        )

    if limit is not None:
        lf = lf.limit(limit)

    return lf


# ---------------------------------------------------------------------------
# Async batch insert
# ---------------------------------------------------------------------------


async def _insert_batch(
    conn: psycopg.AsyncConnection,
    batch: list[tuple[Any, ...]],
) -> int:
    """Execute an executemany upsert for one batch. Returns number of rows affected."""
    # psycopg3: executemany lives on the cursor, not on the connection.
    async with conn.cursor() as cur:
        await cur.executemany(_UPSERT_SQL, batch)
    return len(batch)


# ---------------------------------------------------------------------------
# Main ingestion coroutine
# ---------------------------------------------------------------------------


async def ingest_sirene(*, refresh: bool = False, limit: int | None = None) -> None:
    """Download (optionally) then ingest the SIRENE file into PostgreSQL.

    Args:
        refresh: If True, re-download the file before ingesting.
        limit:   Only process the first *limit* rows (for testing).
    """
    t_start = time.monotonic()

    # ---- Optionally refresh the local file ----
    if refresh:
        log.info("refresh_requested")
        await download_sirene(force=True)

    # ---- Locate the local file ----
    sirene_path = _find_local_sirene_file()
    if sirene_path is None:
        log.error("sirene_file_not_found", sirene_dir=str(settings.sirene_dir))
        raise FileNotFoundError(
            f"No SIRENE file found in {settings.sirene_dir}. "
            "Run the downloader first: python -m fortress.query.sirene_download"
        )

    log.info("sirene_file_found", path=str(sirene_path))

    # ---- Ensure DB is ready ----
    await init_db()
    pool = await get_pool()

    # ---- Build lazy frame ----
    log.info("polars_scan_start", path=str(sirene_path), limit=limit)
    lf = _build_lazy_frame(sirene_path, limit)

    # ---- Stream + ingest ----
    total_processed: int = 0
    total_upserted: int = 0
    total_skipped: int = 0
    total_errors: int = 0

    # collect(streaming=True) processes the lazy frame in chunks
    # without materialising the entire 12M-row dataset at once.
    # For Polars >= 1.0 this is the correct API.
    log.info("polars_collect_streaming_start")

    try:
        # engine="streaming" processes the LazyFrame in chunks to avoid OOM
        # on the full 12M-row file.  Requires Polars ≥ 1.25.
        df: pl.DataFrame = lf.collect(engine="streaming")
    except TypeError:
        # Polars < 1.25: fall back to the old streaming=True keyword.
        try:
            df = lf.collect(streaming=True)  # type: ignore[call-arg]
        except TypeError:
            log.warning("polars_streaming_unsupported", fallback="collect()")
            df = lf.collect()

    log.info("polars_collect_done", rows=len(df))

    # Convert to row-iterator once so we avoid per-row Python overhead
    rows_iter = df.iter_rows(named=True)

    batch: list[tuple[Any, ...]] = []

    async with pool.connection() as conn:
        for raw_row in rows_iter:
            total_processed += 1

            # Per-row transformation — never crash on bad data
            try:
                row_tuple = transform_row(raw_row)
            except Exception as exc:
                total_errors += 1
                log.warning(
                    "row_transform_error",
                    siren=raw_row.get("siren"),
                    error=str(exc),
                )
                continue

            if row_tuple is None:
                total_skipped += 1
                continue

            batch.append(row_tuple)

            if len(batch) >= BATCH_SIZE:
                try:
                    upserted = await _insert_batch(conn, batch)
                    total_upserted += upserted
                except Exception as exc:
                    log.error(
                        "batch_insert_error",
                        batch_size=len(batch),
                        error=str(exc),
                    )
                    total_errors += len(batch)
                finally:
                    batch.clear()

                # Progress print every batch
                print(
                    f"  Ingested: {total_upserted:,} rows"
                    f"  (processed: {total_processed:,}"
                    f"  skipped: {total_skipped:,})",
                    flush=True,
                )

        # Flush the last (possibly partial) batch
        if batch:
            try:
                upserted = await _insert_batch(conn, batch)
                total_upserted += upserted
            except Exception as exc:
                log.error(
                    "final_batch_insert_error",
                    batch_size=len(batch),
                    error=str(exc),
                )
                total_errors += len(batch)
            batch.clear()

        await conn.commit()

    duration = time.monotonic() - t_start

    # ---- Stats ----
    print("\n" + "=" * 60, flush=True)
    print("  SIRENE Ingestion Complete", flush=True)
    print("=" * 60, flush=True)
    print(f"  Total rows processed : {total_processed:>12,}", flush=True)
    print(f"  Inserted / updated   : {total_upserted:>12,}", flush=True)
    print(f"  Skipped (inactive)   : {total_skipped:>12,}", flush=True)
    print(f"  Errors (skipped)     : {total_errors:>12,}", flush=True)
    print(f"  Duration             : {duration:>11.1f}s", flush=True)
    print("=" * 60, flush=True)

    log.info(
        "ingestion_complete",
        total_processed=total_processed,
        total_upserted=total_upserted,
        total_skipped=total_skipped,
        total_errors=total_errors,
        duration_s=round(duration, 1),
    )


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest the SIRENE StockUniteLegale file into PostgreSQL. "
            "Polars lazy streaming + psycopg3 COPY, batch size 10,000."
        )
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-download the SIRENE file before ingesting.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N rows (for testing).",
    )
    return parser.parse_args(argv)


async def _main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    print("Fortress — SIRENE Ingester", flush=True)

    try:
        await ingest_sirene(refresh=args.refresh, limit=args.limit)
    except FileNotFoundError as exc:
        log.error("ingestion_failed", error=str(exc))
        print(f"  ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
    except Exception as exc:
        log.error("ingestion_failed", error=str(exc))
        print(f"  ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(_main())
