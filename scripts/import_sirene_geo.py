"""Bulk-import INSEE geocoded SIRENE into companies_geom.

Run once per monthly INSEE refresh. Idempotent — UPSERT semantics,
never overwrites maps_panel or ban_backfill rows.

Source schema is locked against EXPECTED_SCHEMA below; an assertion at
the top of the run aborts on any drift in the monthly refresh.

Usage (from fortress/ working directory):
    python -m scripts.import_sirene_geo --parquet /tmp/sirene_geo.parquet

Or download + import in one step (omit --parquet to auto-download):
    python -m scripts.import_sirene_geo

The INSEE_GEO_URL env var overrides the default download URL.

Architecture (v5 -- psql copy via regular staging table):
  1. Read + filter the Parquet with polars streaming engine.
     Input: 35.3M SIRET rows. After filtering qualite_xy=33: ~35.3M rows.
  2. Write filtered CSV to a temp file on disk (tab-separated, no header).
  3. Create a regular (non-TEMP) staging table -- survives across Neon pooler
     connection rotations, unlike TEMP tables.
  4. Use psql client-side copy (single connection) to bulk-load CSV into
     the staging table -- ~13 min for 35M rows.
  5. Server-side DISTINCT ON (siren) + INSERT ON CONFLICT to merge staging
     into companies_geom.  The DISTINCT ON deduplicates by SIREN (9-digit
     company ID), keeping the establishment with the best geocode_quality
     (11=exact address < 12 < 21 < 22).  Yields ~24.5M unique SIRENs.
  6. DROP staging table, ANALYZE companies_geom.
  Uses the Neon pooler endpoint -- psql holds one connection for the entire
  copy stream, so PgBouncer does not rotate the backend mid-stream.
"""

from __future__ import annotations

import argparse
import os
import subprocess
import sys
import time
import urllib.request

import polars as pl

# ── Endpoints ─────────────────────────────────────────────────────────────────

# psql binary — homebrew installs several versions; prefer PG16
_PSQL_CANDIDATES = [
    "/opt/homebrew/Cellar/postgresql@16/16.13/bin/psql",
    "/opt/homebrew/Cellar/postgresql@15/15.17/bin/psql",
    "/opt/homebrew/Cellar/postgresql@18/18.3/bin/psql",
    "/usr/local/bin/psql",
    "/usr/bin/psql",
    "psql",  # fallback — works if psql is on $PATH
]

# Note on Neon pooler vs direct endpoint:
# The pooler (PgBouncer in transaction mode) is safe to use for psql \copy.
# \copy is a client-side meta-command — psql holds a single connection open
# and streams the entire file within it.  PgBouncer won't rotate the backend
# mid-stream.  The TEMP TABLE problem (attempted in v3) was caused by issuing
# CREATE TEMP and then COPY as separate psql invocations.  Here we use a
# regular (non-TEMP) staging table + a single subprocess per SQL call, so
# the pooler endpoint works fine throughout.
def _psql_url(database_url: str) -> str:
    """Return the DATABASE_URL suitable for psql.
    The pooler endpoint is perfectly usable; this is a no-op kept for clarity.
    """
    return database_url


# ── Download URL ──────────────────────────────────────────────────────────────

INSEE_GEO_URL = os.environ.get(
    "INSEE_GEO_URL",
    "https://object.files.data.gouv.fr/data-pipeline-open/"
    "siren/geoloc/"
    "GeolocalisationEtablissement_Sirene_pour_etudes_statistiques_utf8.parquet",
)

# Locked against the Apr 26 schema dump.
# Assert on every run — guards against silent INSEE column renames.
EXPECTED_SCHEMA = {
    "siret": pl.String,
    "x": pl.Float32,
    "y": pl.Float32,
    "qualite_xy": pl.String,
    "epsg": pl.String,
    "plg_qp24": pl.String,
    "plg_iris": pl.String,
    "plg_zus": pl.String,
    "plg_qp15": pl.String,
    "plg_qva": pl.String,
    "plg_code_commune": pl.String,
    "distance_precision": pl.Float32,
    "qualite_qp24": pl.String,
    "qualite_iris": pl.String,
    "qualite_zus": pl.String,
    "qualite_qp15": pl.String,
    "qualite_qva": pl.String,
    "y_latitude": pl.Float32,
    "x_longitude": pl.Float32,
}

_CSV_PATH = "/tmp/sirene_geo_filtered.csv"
_STAGING_TABLE = "_sirene_geo_staging"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_psql() -> str:
    """Return the path to a working psql binary."""
    for candidate in _PSQL_CANDIDATES:
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
        # Try via PATH for the bare "psql" fallback
        if candidate == "psql":
            import shutil
            found = shutil.which("psql")
            if found:
                return found
    raise RuntimeError(
        "psql not found.  Install PostgreSQL client tools:\n"
        "  brew install postgresql@16"
    )


def _run_psql(psql: str, db_url: str, sql: str) -> None:
    """Run a SQL statement via psql.  Raises on non-zero exit."""
    result = subprocess.run(
        [psql, db_url, "-c", sql],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"psql command failed (exit {result.returncode}):\n"
            f"  SQL: {sql[:200]}\n"
            f"  stderr: {result.stderr.strip()}\n"
            f"  stdout: {result.stdout.strip()}"
        )


def _run_psql_copy(psql: str, db_url: str, csv_path: str) -> None:
    r"""Run \copy staging ← csv_path via psql.

    psql's \copy is a client-side meta-command: it opens the local file
    and streams it over the single connection psql holds.  This avoids the
    Neon pooler's connection-rotation problem because the entire stream
    lands on one stable backend.
    """
    # Build the \copy meta-command string.
    # E'\t' is the tab delimiter; NULL '\N' matches polars null_value='\\N'.
    copy_cmd = (
        r"\copy " + _STAGING_TABLE +
        r" FROM '" + csv_path + r"'"
        r" WITH (FORMAT csv, DELIMITER E'\t', NULL '\N')"
    )
    result = subprocess.run(
        [psql, db_url, "-c", copy_cmd],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"psql \\copy failed (exit {result.returncode}):\n"
            f"  stderr: {result.stderr.strip()}\n"
            f"  stdout: {result.stdout.strip()}"
        )
    # psql prints "COPY N" to stdout on success
    print(f"  psql reported: {result.stdout.strip()}")


def download_parquet(dest_path: str) -> None:
    """Download the INSEE Parquet file to dest_path, showing progress."""
    print(f"Downloading INSEE geocoded SIRENE Parquet from:\n  {INSEE_GEO_URL}")
    print(f"  -> {dest_path}")
    start = time.time()

    def _progress(block_num: int, block_size: int, total_size: int) -> None:
        downloaded = block_num * block_size
        pct = min(100.0, 100.0 * downloaded / total_size) if total_size > 0 else 0.0
        mb = downloaded / 1_048_576
        total_mb = total_size / 1_048_576 if total_size > 0 else 0
        print(f"\r  {mb:.1f} MB / {total_mb:.1f} MB ({pct:.1f}%)", end="", flush=True)

    urllib.request.urlretrieve(INSEE_GEO_URL, dest_path, reporthook=_progress)
    elapsed = time.time() - start
    size_mb = os.path.getsize(dest_path) / 1_048_576
    print(f"\n  Downloaded {size_mb:.1f} MB in {elapsed:.1f}s")


# ── Main ──────────────────────────────────────────────────────────────────────

def main(parquet_path: str, dry_run: bool = False) -> None:
    t_total = time.time()

    # Locate psql early — fail fast before doing any heavy work.
    psql = _find_psql()
    print(f"Using psql: {psql}")

    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        sys.exit("DATABASE_URL environment variable is not set.")
    db_url = _psql_url(database_url)
    print(f"DB endpoint: {db_url.split('@')[1]}")  # hide credentials

    # ── Step 1: Schema drift guard ────────────────────────────────────────────
    print(f"\n[1/4] Checking Parquet schema: {parquet_path}")
    lf = pl.scan_parquet(parquet_path)
    actual_schema = dict(lf.collect_schema())
    if actual_schema != EXPECTED_SCHEMA:
        sys.exit(
            "INSEE Parquet schema drift detected — refusing to import.\n"
            f"Expected: {EXPECTED_SCHEMA}\n"
            f"Actual:   {actual_schema}\n"
            "Update EXPECTED_SCHEMA if INSEE has changed columns."
        )
    print("  Schema OK — 19 columns match EXPECTED_SCHEMA.")

    # ── Step 2: Read + filter Parquet ─────────────────────────────────────────
    # qualite_xy='33' means "randomly assigned within commune" — not useful.
    # qualite_xy 11/12/21/22 are all street-level or better — keep them.
    print("\n[2/4] Reading + filtering Parquet with polars streaming engine...")
    t_read = time.time()
    df = (
        lf.select([
            pl.col("siret").str.slice(0, 9).alias("siren"),
            pl.col("y_latitude").cast(pl.Float64).alias("lat"),
            pl.col("x_longitude").cast(pl.Float64).alias("lng"),
            pl.col("qualite_xy").alias("geocode_quality"),
        ])
        .filter(
            (pl.col("geocode_quality") != "33")
            & pl.col("lat").is_not_null()
            & pl.col("lng").is_not_null()
            # WGS84 paranoia bounds: covers métropole + all outre-mer territories
            & pl.col("lat").is_between(-22.0, 51.5)
            & pl.col("lng").is_between(-62.0, 56.0)
        )
        .collect(engine="streaming")
    )
    elapsed_read = time.time() - t_read
    total_rows = len(df)
    print(f"  {total_rows:,} rows after filtering ({elapsed_read:.1f}s)")
    print(f"  Dropped qualite_xy='33': ~{37_380_068 - total_rows:,} rows")

    if dry_run:
        print("\n[DRY RUN] Skipping CSV write and DB load. Exiting.")
        return

    # ── Step 2b: Write filtered CSV to disk ───────────────────────────────────
    # polars writes Rust-native at ~16M rows/s. The file is ~1.8 GB.
    # Tab-separated, no header.  null_value='\\N' matches PostgreSQL COPY NULL.
    print(f"\n  Writing {total_rows:,} rows to {_CSV_PATH}...")
    t_csv = time.time()
    df.write_csv(_CSV_PATH, separator="\t", include_header=False, null_value="\\N")
    elapsed_csv = time.time() - t_csv
    csv_size_gb = os.path.getsize(_CSV_PATH) / 1e9
    print(f"  CSV written: {csv_size_gb:.2f} GB in {elapsed_csv:.1f}s")
    del df  # free ~3 GB RAM before the DB phase

    # ── Step 3: Bulk-load via psql \copy ──────────────────────────────────────
    # Architecture:
    #   a) Drop any leftover staging table from a previous aborted run.
    #   b) Create a regular (non-TEMP) staging table — regular tables survive
    #      across Neon pooler connection rotations; TEMP tables do not.
    #   c) \copy loads the local CSV into staging over a single psql connection.
    #   d) INSERT … SELECT … ON CONFLICT merges staging → companies_geom.
    #      The WHERE companies_geom.source = 'sirene_geo' clause ensures we
    #      NEVER overwrite maps_panel or ban_backfill rows.
    #   e) DROP staging, ANALYZE target.
    print(f"\n[3/4] Bulk-loading {total_rows:,} rows via psql \\copy  [{time.strftime('%H:%M:%S')}]")

    # a) Clean up any leftover staging table
    print("  a) Dropping any leftover staging table...")
    _run_psql(psql, db_url, f"DROP TABLE IF EXISTS {_STAGING_TABLE};")

    # b) Create regular staging table (no index — faster COPY, index not needed for bulk merge)
    print("  b) Creating staging table...")
    _run_psql(
        psql, db_url,
        f"""CREATE TABLE {_STAGING_TABLE} (
            siren TEXT NOT NULL,
            lat DOUBLE PRECISION NOT NULL,
            lng DOUBLE PRECISION NOT NULL,
            geocode_quality TEXT NOT NULL
        );""",
    )

    # c) \copy CSV → staging (this is the slow step: ~3-5 min for 35M rows)
    print(f"  c) \\copy {_CSV_PATH} → {_STAGING_TABLE} ...  [{time.strftime('%H:%M:%S')}]")
    t_copy = time.time()
    _run_psql_copy(psql, db_url, _CSV_PATH)
    elapsed_copy = time.time() - t_copy
    rows_per_s = total_rows / elapsed_copy if elapsed_copy > 0 else 0
    print(f"  COPY done in {elapsed_copy:.0f}s ({rows_per_s:,.0f} rows/s)  [{time.strftime('%H:%M:%S')}]")

    # d) Server-side UPSERT merge staging → companies_geom.
    #
    # The parquet has one row per SIRET (establishment), but companies_geom
    # is keyed on SIREN (company, 9 digits).  A company can have many
    # establishments so the staging table has duplicate SIRENs.  PostgreSQL
    # rejects ON CONFLICT DO UPDATE when the same conflict key appears
    # multiple times in the source ("row affected a second time").
    #
    # Fix: wrap the SELECT in a DISTINCT ON (siren) ordered by geocode_quality
    # ASC so that per company we keep the establishment with the best
    # geolocation quality (11 = exact address is the lowest char, so ASC wins).
    print(f"  d) Merging staging → companies_geom (dedup by SIREN, ON CONFLICT UPSERT)...  [{time.strftime('%H:%M:%S')}]")
    t_upsert = time.time()
    _run_psql(
        psql, db_url,
        f"""INSERT INTO companies_geom
                (siren, lat, lng, source, geocode_quality)
            SELECT DISTINCT ON (siren)
                siren, lat, lng, 'sirene_geo', geocode_quality
            FROM {_STAGING_TABLE}
            ORDER BY siren, geocode_quality ASC
            ON CONFLICT (siren) DO UPDATE SET
                lat              = EXCLUDED.lat,
                lng              = EXCLUDED.lng,
                geocode_quality  = EXCLUDED.geocode_quality,
                updated_at       = NOW()
            WHERE companies_geom.source = 'sirene_geo';""",
    )
    elapsed_upsert = time.time() - t_upsert
    print(f"  UPSERT merge done in {elapsed_upsert:.0f}s  [{time.strftime('%H:%M:%S')}]")

    # e) Clean up staging table
    print(f"  e) Dropping staging table...")
    _run_psql(psql, db_url, f"DROP TABLE IF EXISTS {_STAGING_TABLE};")

    # ── Step 4: ANALYZE for fresh query planner stats ─────────────────────────
    print(f"\n[4/4] ANALYZE companies_geom (fresh planner stats)...  [{time.strftime('%H:%M:%S')}]")
    t_analyze = time.time()
    _run_psql(psql, db_url, "ANALYZE companies_geom;")
    print(f"  ANALYZE complete in {time.time() - t_analyze:.0f}s")

    # ── Final summary ─────────────────────────────────────────────────────────
    total_elapsed = time.time() - t_total
    print(
        f"\nDONE: loaded {total_rows:,} rows from {parquet_path} "
        f"into companies_geom in {total_elapsed:.0f}s "
        f"({total_elapsed/60:.1f} min)."
    )


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Bulk-import INSEE geocoded SIRENE into companies_geom."
    )
    parser.add_argument(
        "--parquet",
        metavar="PATH",
        default=None,
        help=(
            "Path to a pre-downloaded Parquet file. "
            "If omitted, the file is downloaded from INSEE_GEO_URL."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Validate schema + filter counts but do not write CSV or INSERT.",
    )
    return parser.parse_args()


if __name__ == "__main__":
    # Load .env if python-dotenv is available (local dev convenience)
    try:
        from dotenv import load_dotenv
        load_dotenv()
    except ImportError:
        pass

    args = _parse_args()

    if args.parquet is None:
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp.close()
        parquet_path = tmp.name
        try:
            download_parquet(parquet_path)
        except Exception as exc:
            print(f"Download failed: {exc}", file=sys.stderr)
            sys.exit(1)
    else:
        parquet_path = args.parquet

    main(parquet_path, dry_run=args.dry_run)
