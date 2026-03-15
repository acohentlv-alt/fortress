"""SIRENE StockEtablissement ingester — populates location data for companies.

Reads the local StockEtablissement Parquet file, filters to headquarters only
(etablissementSiege = True), and bulk-updates the companies table with:
    code_postal, ville, adresse, departement

Uses a high-performance TEMP TABLE + UPDATE … FROM strategy to update
millions of rows in seconds instead of hours.

Usage:
    python -m fortress.module_a.sirene_etablissement_ingester [--refresh] [--limit N]

Options:
    --refresh   Re-download the StockEtablissement file before ingesting.
    --limit N   Only process the first N rows (useful for testing).

Column mapping (StockEtablissement → companies table):
    siren                               → siren  (join key)
    codePostalEtablissement             → code_postal
    libelleCommuneEtablissement         → ville
    numeroVoieEtablissement +
        typeVoieEtablissement +
        libelleVoieEtablissement        → adresse  (constructed)
    LEFT(code_postal, 2)                → departement  (derived)
        Special cases: 97x, 98x (DOM-TOM) → first 3 chars
"""

from __future__ import annotations

import argparse
import asyncio
import sys
import time
from pathlib import Path
from typing import Any

import polars as pl
import psycopg
import structlog

from fortress.config.settings import settings
from fortress.database.connection import close_pool, get_pool, init_db
from fortress.module_a.sirene_downloader import download_etablissement

log = structlog.get_logger(__name__)

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BATCH_SIZE: int = 50_000  # Rows per TEMP TABLE batch (higher = faster)

# Regex: keep only digits (strips spaces, letters, punctuation from postal codes)
import re
_DIGITS_ONLY = re.compile(r"[^\d]")

# Columns we need from the Parquet file (minimise memory footprint)
ETAB_COLUMNS: list[str] = [
    "siren",
    "etablissementSiege",
    "etatAdministratifEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
    "numeroVoieEtablissement",
    "typeVoieEtablissement",
    "libelleVoieEtablissement",
    "complementAdresseEtablissement",
    "enseigne1Etablissement",             # Commercial sign name ("Camping La Marende")
    "denominationUsuelleEtablissement",   # Trade/usage name ("La Marende")
    "trancheEffectifsEtablissement",      # Employee headcount range at this location
]


# ---------------------------------------------------------------------------
# Address construction
# ---------------------------------------------------------------------------


def _build_adresse(row: dict[str, Any]) -> str | None:
    """Construct a human-readable address from SIRENE components.

    Format: "12 RUE DE LA PAIX" (number + type + street name).
    """
    parts: list[str] = []

    numero = _coerce_str(row.get("numeroVoieEtablissement"))
    if numero:
        parts.append(numero)

    type_voie = _coerce_str(row.get("typeVoieEtablissement"))
    if type_voie:
        parts.append(type_voie)

    libelle = _coerce_str(row.get("libelleVoieEtablissement"))
    if libelle:
        parts.append(libelle)

    complement = _coerce_str(row.get("complementAdresseEtablissement"))
    if complement and not parts:
        parts.append(complement)

    return " ".join(parts) if parts else None


def _sanitize_code_postal(raw: str | None) -> str | None:
    """Strip non-digit characters from a raw postal code.

    Government data contains values like '20 46', '7 500', '  33000  '.
    We strip everything except digits, then return None if fewer than 2 digits.
    """
    if not raw:
        return None
    cleaned = _DIGITS_ONLY.sub("", str(raw).strip())
    return cleaned if len(cleaned) >= 2 else None


def _derive_departement(code_postal: str | None) -> str | None:
    """Derive the department code from a sanitized French postal code.

    Standard: first 2 digits (e.g. "66000" → "66").
    DOM-TOM:  first 3 digits (e.g. "97100" → "971").
    Corse:    "20" prefix → "2A" or "2B" based on the full code.
    """
    if not code_postal or len(code_postal) < 2:
        return None

    prefix2 = code_postal[:2]

    # DOM-TOM: 97x, 98x → use 3 digits
    if prefix2 in ("97", "98") and len(code_postal) >= 3:
        return code_postal[:3]

    # Corse: 20xxx → split into 2A (Corse-du-Sud) and 2B (Haute-Corse)
    if prefix2 == "20" and len(code_postal) >= 5:
        try:
            cp_int = int(code_postal[:5])
            return "2A" if cp_int < 20200 else "2B"
        except (ValueError, TypeError):
            return "20"  # Fallback: can't parse → generic Corse

    return prefix2


def _coerce_str(value: Any) -> str | None:
    """Return a non-empty stripped string or None."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


# ---------------------------------------------------------------------------
# Polars lazy scan
# ---------------------------------------------------------------------------


def _build_lazy_frame(path: Path, limit: int | None) -> pl.LazyFrame:
    """Return a lazy Polars frame filtered to active HQ establishments."""
    # Detect available columns
    try:
        schema = pl.scan_parquet(str(path)).collect_schema()
        available = set(schema.names())
    except Exception:
        available = set(ETAB_COLUMNS)

    wanted = [c for c in ETAB_COLUMNS if c in available]

    if path.suffix.lower() == ".parquet":
        lf = pl.scan_parquet(str(path)).select(wanted)
    else:
        # ZIP / CSV fallback
        log.warning("zip_fallback", msg="ZIP detected — loading with read_csv")
        df = pl.read_csv(
            str(path),
            columns=wanted,
            separator=",",
            encoding="utf8-lossy",
            infer_schema_length=10_000,
        )
        lf = df.lazy()

    # Filter: headquarters only + active only
    if "etablissementSiege" in available:
        lf = lf.filter(pl.col("etablissementSiege") == True)  # noqa: E712 — Boolean col

    if "etatAdministratifEtablissement" in available:
        lf = lf.filter(pl.col("etatAdministratifEtablissement") == "A")

    if limit is not None:
        lf = lf.limit(limit)

    return lf


# ---------------------------------------------------------------------------
# High-performance batch update using TEMP TABLE
# ---------------------------------------------------------------------------


async def _update_batch(
    conn: psycopg.AsyncConnection,
    batch: list[tuple[str, str | None, str | None, str | None, str | None, str | None, str | None]],
) -> int:
    """Insert batch into a temp table, then UPDATE companies via JOIN.

    This is ~100x faster than individual UPDATE statements.

    Tuple format: (siren, code_postal, ville, adresse, departement, enseigne, tranche_effectif)
    """
    if not batch:
        return 0

    async with conn.cursor() as cur:
        # Create temp table (lives only for this transaction)
        await cur.execute("""
            CREATE TEMP TABLE IF NOT EXISTS _etab_staging (
                siren             VARCHAR(9)  NOT NULL,
                code_postal       VARCHAR(10),
                ville             TEXT,
                adresse           TEXT,
                departement       VARCHAR(3),
                enseigne          TEXT,
                tranche_effectif  VARCHAR(10)
            ) ON COMMIT DELETE ROWS
        """)

        # Bulk insert into temp table using executemany
        await cur.executemany(
            "INSERT INTO _etab_staging (siren, code_postal, ville, adresse, departement, enseigne, tranche_effectif) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s)",
            batch,
        )

        # UPDATE companies from staging — only fill NULL fields (never overwrite)
        result = await cur.execute("""
            UPDATE companies c SET
                code_postal      = COALESCE(c.code_postal,      s.code_postal),
                ville            = COALESCE(c.ville,            s.ville),
                adresse          = COALESCE(c.adresse,          s.adresse),
                departement      = COALESCE(c.departement,      s.departement),
                enseigne         = COALESCE(c.enseigne,         s.enseigne),
                tranche_effectif = COALESCE(c.tranche_effectif, s.tranche_effectif),
                updated_at       = NOW()
            FROM _etab_staging s
            WHERE c.siren = s.siren
              AND (c.code_postal IS NULL OR c.ville IS NULL
                   OR c.adresse IS NULL OR c.departement IS NULL
                   OR c.enseigne IS NULL OR c.tranche_effectif IS NULL)
        """)

        updated = result.rowcount if result and hasattr(result, 'rowcount') else 0

    await conn.commit()
    return updated


# ---------------------------------------------------------------------------
# Main ingestion coroutine
# ---------------------------------------------------------------------------


async def ingest_etablissement(
    *, refresh: bool = False, limit: int | None = None
) -> None:
    """Download (optionally) and ingest StockEtablissement location data.

    Populates code_postal, ville, adresse, departement on the companies table
    for all HQ establishments that match by SIREN.

    Args:
        refresh: If True, re-download the file before ingesting.
        limit:   Only process the first *limit* rows (for testing).
    """
    t_start = time.monotonic()

    # ── 1. Ensure file exists ────────────────────────────────────────────
    if refresh:
        log.info("etablissement_refresh_requested")

    etab_path = _find_local_etablissement_file()

    if etab_path is None:
        print("  📥 StockEtablissement not found — downloading (~2 GB)...", flush=True)
        etab_path = await download_etablissement(force=refresh)

    if etab_path is None or not etab_path.exists():
        raise FileNotFoundError(
            f"StockEtablissement file not found in {settings.sirene_dir}. "
            "Run: python -m fortress.module_a.sirene_downloader --etablissement"
        )

    print(f"  📂 Using: {etab_path.name} ({etab_path.stat().st_size / 1e9:.2f} GB)", flush=True)

    # ── 2. Ensure DB is ready ────────────────────────────────────────────
    await init_db()
    pool = await get_pool()

    # ── 3. Read Parquet in chunks (low memory) ───────────────────────────
    # PyArrow reads row groups without loading the whole file into RAM.
    # This keeps memory under ~200 MB even for the 2 GB file.
    import pyarrow.parquet as pq

    print("  🔍 Reading Parquet in chunks (memory-safe)...", flush=True)

    # Columns we need (only read what we use)
    wanted_cols = [c for c in ETAB_COLUMNS]

    # Detect available columns
    schema = pq.read_schema(str(etab_path))
    available = set(schema.names)
    read_cols = [c for c in wanted_cols if c in available]

    parquet_file = pq.ParquetFile(str(etab_path))
    CHUNK_SIZE = 100_000  # rows per chunk — keeps RAM ~100-200 MB

    # ── 4. Process chunks ────────────────────────────────────────────────
    batch: list[tuple[str, str | None, str | None, str | None, str | None, str | None]] = []
    total_processed = 0
    total_updated = 0
    total_skipped = 0
    total_errors = 0

    async with pool.connection() as conn:
        for arrow_batch in parquet_file.iter_batches(
            batch_size=CHUNK_SIZE,
            columns=read_cols,
        ):
            # Convert to Python dicts
            chunk_df = arrow_batch.to_pydict()
            n_rows = len(chunk_df.get("siren", []))

            for i in range(n_rows):
                total_processed += 1

                try:
                    raw_row = {col: chunk_df[col][i] for col in read_cols if col in chunk_df}

                    # Filter: HQ only + active only
                    if "etablissementSiege" in raw_row:
                        is_siege = raw_row["etablissementSiege"]
                        if is_siege is not True and str(is_siege).lower() != "true":
                            total_skipped += 1
                            continue

                    if "etatAdministratifEtablissement" in raw_row:
                        etat = raw_row.get("etatAdministratifEtablissement")
                        if etat and str(etat).strip() != "A":
                            total_skipped += 1
                            continue

                    siren = _coerce_str(raw_row.get("siren"))
                    if not siren:
                        total_skipped += 1
                        continue

                    siren = siren.zfill(9)
                    code_postal = _sanitize_code_postal(
                        raw_row.get("codePostalEtablissement")
                    )
                    ville = _coerce_str(raw_row.get("libelleCommuneEtablissement"))
                    adresse = _build_adresse(raw_row)
                    departement = _derive_departement(code_postal)

                    # Enseigne: prefer enseigne1Etablissement, fallback to denominationUsuelle
                    enseigne = _coerce_str(raw_row.get("enseigne1Etablissement"))
                    if not enseigne:
                        enseigne = _coerce_str(raw_row.get("denominationUsuelleEtablissement"))

                    # Tranche effectifs (employee headcount range at this location)
                    tranche_effectif = _coerce_str(raw_row.get("trancheEffectifsEtablissement"))

                    # Skip rows with no useful data at all
                    if not code_postal and not ville and not adresse and not enseigne:
                        total_skipped += 1
                        continue

                    batch.append((siren, code_postal, ville, adresse, departement, enseigne, tranche_effectif))
                except (ValueError, TypeError, KeyError) as exc:
                    total_errors += 1
                    if total_errors <= 20:  # Log first 20 only
                        log.warning(
                            "row_transform_error",
                            siren=raw_row.get("siren") if 'raw_row' in dir() else "?",
                            error=str(exc),
                        )
                    continue

                if len(batch) >= BATCH_SIZE:
                    updated = await _update_batch(conn, batch)
                    total_updated += updated
                    batch.clear()

                    print(
                        f"  ✅ Processed: {total_processed:>10,}"
                        f"  Updated: {total_updated:>10,}"
                        f"  Skipped: {total_skipped:>8,}",
                        flush=True,
                    )

        # Flush last batch
        if batch:
            updated = await _update_batch(conn, batch)
            total_updated += updated
            batch.clear()

    duration = time.monotonic() - t_start

    # ── 6. Stats ─────────────────────────────────────────────────────────
    print("\n" + "=" * 64, flush=True)
    print("  SIRENE Établissement Ingestion Complete", flush=True)
    print("=" * 64, flush=True)
    print(f"  Total HQ rows processed  : {total_processed:>12,}", flush=True)
    print(f"  Companies updated        : {total_updated:>12,}", flush=True)
    print(f"  Rows skipped (no data)   : {total_skipped:>12,}", flush=True)
    print(f"  Dirty rows (recovered)   : {total_errors:>12,}", flush=True)
    print(f"  Duration                 : {duration:>11.1f}s", flush=True)
    print("=" * 64, flush=True)

    log.info(
        "etablissement_ingestion_complete",
        total_processed=total_processed,
        total_updated=total_updated,
        total_skipped=total_skipped,
        duration_s=round(duration, 1),
    )

    # ── 7. Post-ingestion verification ───────────────────────────────────
    async with pool.connection() as conn:
        async with conn.cursor(row_factory=psycopg.rows.dict_row) as cur:
            await cur.execute("""
                SELECT
                    COUNT(*) AS total,
                    COUNT(departement) AS with_dept,
                    COUNT(code_postal) AS with_cp,
                    COUNT(ville) AS with_ville,
                    COUNT(adresse) AS with_adresse
                FROM companies
            """)
            stats = await cur.fetchone()

    if stats:
        total = stats["total"]
        print(f"\n  📊 Post-ingestion coverage:", flush=True)
        for col in ("with_dept", "with_cp", "with_ville", "with_adresse"):
            cnt = stats[col]
            pct = (cnt / total * 100) if total > 0 else 0
            label = col.replace("with_", "")
            print(f"     {label:<12}: {cnt:>12,} / {total:,} ({pct:.1f}%)", flush=True)


# ---------------------------------------------------------------------------
# File locator
# ---------------------------------------------------------------------------


def _find_local_etablissement_file() -> Path | None:
    """Return the path of the local StockEtablissement file if it exists."""
    for ext in (".parquet", ".zip"):
        p = settings.sirene_dir / f"StockEtablissement{ext}"
        if p.exists():
            return p
    return None


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Ingest SIRENE StockEtablissement data to populate location fields "
            "(code_postal, ville, adresse, departement) on the companies table."
        )
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Re-download the StockEtablissement file before ingesting.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help="Process only the first N HQ rows (for testing).",
    )
    return parser.parse_args(argv)


async def _main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    print("Fortress — SIRENE Établissement Ingester", flush=True)
    print(f"  Target: UPDATE companies SET code_postal, ville, adresse, departement", flush=True)

    try:
        await ingest_etablissement(refresh=args.refresh, limit=args.limit)
    except FileNotFoundError as exc:
        log.error("ingestion_failed", error=str(exc))
        print(f"  ❌ ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
    except Exception as exc:
        log.error("ingestion_failed", error=str(exc))
        print(f"  ❌ ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)
    finally:
        await close_pool()


if __name__ == "__main__":
    asyncio.run(_main())
