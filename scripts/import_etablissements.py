"""Bulk-import INSEE StockEtablissement into the establishments table.

Run once per monthly INSEE refresh. Idempotent — atomic table swap, so
re-running with the same source always produces the same final state.

Source file: StockEtablissement_utf8.parquet (~2.1 GB)
Source URL:  https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockEtablissement_utf8.parquet

Usage (from the repo root):
    python -m scripts.import_etablissements              # download + import (~25 min)
    python -m scripts.import_etablissements --dry-run --limit 1000  # smoke-test only

Dry-run mode (--dry-run --limit N):
  Reads the first N rows directly from the remote URL via polars lazy scan
  (no full download needed — only fetches the relevant row groups). Validates
  parsing and column mapping; does NOT touch the production database.

Architecture (mirrors import_sirene_geo.py v5 approach):
  1. Download the Parquet file (~2.1 GB) unless --parquet is provided.
  2. Polars streaming filter: etat_administratif='A' (active only), keep the
     12 columns we need, concatenate address fields.
  3. Write filtered rows to /tmp/etabs.tsv (tab-separated, no header).
  4. Create regular staging table establishments_new (no indexes — faster COPY).
  5. psql \\copy establishments_new FROM /tmp/etabs.tsv.
  6. ANALYZE establishments_new.
  7. Add indexes to staging, then atomic swap:
       RENAME live → establishments_old
       RENAME establishments_new → live
       DROP establishments_old
  8. Log row count, distinct SIREN count, NAF distribution top 10.
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

_PSQL_CANDIDATES = [
    "/opt/homebrew/Cellar/postgresql@16/16.13/bin/psql",
    "/opt/homebrew/Cellar/postgresql@15/15.17/bin/psql",
    "/opt/homebrew/Cellar/postgresql@18/18.3/bin/psql",
    "/usr/local/bin/psql",
    "/usr/bin/psql",
    "psql",
]

# Current INSEE StockEtablissement Parquet (Apr 2026 edition).
# Override with INSEE_ETAB_URL env var if INSEE rotates the path.
INSEE_ETAB_URL = os.environ.get(
    "INSEE_ETAB_URL",
    "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/"
    "StockEtablissement_utf8.parquet",
)

_TSV_PATH = "/tmp/etabs.tsv"
_STAGING_TABLE = "establishments_new"
_OLD_TABLE = "establishments_old"
_LIVE_TABLE = "establishments"


# ── Helpers ───────────────────────────────────────────────────────────────────

def _find_psql() -> str:
    """Return path to a working psql binary."""
    for candidate in _PSQL_CANDIDATES:
        if os.path.isfile(candidate) and os.access(candidate, os.X_OK):
            return candidate
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
            f"  SQL: {sql[:300]}\n"
            f"  stderr: {result.stderr.strip()}\n"
            f"  stdout: {result.stdout.strip()}"
        )
    if result.stdout.strip():
        print(f"  psql: {result.stdout.strip()}")


def _run_psql_copy(psql: str, db_url: str, tsv_path: str) -> None:
    r"""Load tsv_path into _STAGING_TABLE via psql \\copy.

    psql's \\copy is a client-side meta-command — psql holds one connection
    and streams the entire file.  PgBouncer never rotates the backend
    mid-stream, so the Neon pooler endpoint is safe to use.
    """
    copy_cmd = (
        r"\copy " + _STAGING_TABLE +
        r" FROM '" + tsv_path + r"'"
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
    print(f"  psql reported: {result.stdout.strip()}")


def _run_psql_query(psql: str, db_url: str, sql: str) -> list[list[str]]:
    """Run a SELECT and return rows as list of string lists (tab-separated)."""
    result = subprocess.run(
        [psql, db_url, "--no-align", "--tuples-only", "-F", "\t", "-c", sql],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"psql query failed: {result.stderr.strip()}"
        )
    lines = [line for line in result.stdout.splitlines() if line.strip()]
    return [line.split("\t") for line in lines]


def download_parquet(dest_path: str) -> None:
    """Download the INSEE StockEtablissement Parquet to dest_path."""
    print(f"Downloading INSEE StockEtablissement Parquet from:\n  {INSEE_ETAB_URL}")
    print(f"  -> {dest_path}")
    start = time.time()

    def _progress(block_num: int, block_size: int, total_size: int) -> None:
        downloaded = block_num * block_size
        pct = min(100.0, 100.0 * downloaded / total_size) if total_size > 0 else 0.0
        mb = downloaded / 1_048_576
        total_mb = total_size / 1_048_576 if total_size > 0 else 0
        print(f"\r  {mb:.1f} MB / {total_mb:.1f} MB ({pct:.1f}%)", end="", flush=True)

    urllib.request.urlretrieve(INSEE_ETAB_URL, dest_path, reporthook=_progress)
    elapsed = time.time() - start
    size_mb = os.path.getsize(dest_path) / 1_048_576
    print(f"\n  Downloaded {size_mb:.1f} MB in {elapsed:.1f}s")


# ── Column mapping ────────────────────────────────────────────────────────────
# INSEE StockEtablissement column names we need (verified against the
# stocketablissement-311 layout description, Jan 2026 edition).
# We keep only active establishments (etatAdministratifEtablissement='A').

# These are the columns we read from the raw Parquet
_NEEDED_COLS = [
    "siret",
    "siren",
    "etablissementSiege",
    "etatAdministratifEtablissement",
    "enseigne1Etablissement",
    "denominationUsuelleEtablissement",
    "activitePrincipaleEtablissement",
    "codePostalEtablissement",
    "libelleCommuneEtablissement",
    # Address components — concatenated into adresse_etab
    "numeroVoieEtablissement",
    "typeVoieEtablissement",
    "libelleVoieEtablissement",
]


# ── Filter + shape ────────────────────────────────────────────────────────────

def _filter_and_shape(source: str, limit: int | None = None) -> pl.DataFrame:
    """Read, filter, and shape the Parquet into TSV-ready form.

    source can be a local file path or a remote https:// URL.
    When source is a URL, polars fetches only the required row groups
    (no full download needed for small limits).

    Returns a polars DataFrame with exactly the columns matching
    the establishments table column order (excluding auto-generated
    created_at / updated_at which default on INSERT).
    """
    print(f"\n[2] Reading + filtering Parquet: {source}")
    t_read = time.time()

    # Lazy scan — works with both local paths and https:// URLs
    lf = pl.scan_parquet(source)

    # Verify required columns are present
    schema_cols = set(lf.collect_schema().names())
    missing = [c for c in _NEEDED_COLS if c not in schema_cols]
    if missing:
        sys.exit(
            f"INSEE Parquet schema drift — missing columns: {missing}\n"
            f"Available columns: {sorted(schema_cols)}\n"
            "Update _NEEDED_COLS if INSEE renamed these fields."
        )

    # Select only the columns we care about
    lf = lf.select(_NEEDED_COLS)

    # Filter: active establishments only
    lf = lf.filter(pl.col("etatAdministratifEtablissement") == "A")

    if limit is not None:
        lf = lf.head(limit)

    # Collect (streaming engine reduces peak RAM for the full ~30M file)
    df = lf.collect(engine="streaming")

    elapsed = time.time() - t_read
    print(f"  {len(df):,} active establishments read in {elapsed:.1f}s")

    # Build adresse_etab by concatenating num + type + libelle (space-joined, nulls skipped)
    df = df.with_columns(
        pl.concat_str(
            [
                pl.when(pl.col("numeroVoieEtablissement").is_not_null())
                  .then(pl.col("numeroVoieEtablissement"))
                  .otherwise(pl.lit("")),
                pl.when(pl.col("typeVoieEtablissement").is_not_null())
                  .then(pl.col("typeVoieEtablissement"))
                  .otherwise(pl.lit("")),
                pl.when(pl.col("libelleVoieEtablissement").is_not_null())
                  .then(pl.col("libelleVoieEtablissement"))
                  .otherwise(pl.lit("")),
            ],
            separator=" ",
            ignore_nulls=True,
        )
        .str.strip_chars()
        .replace("", None)
        .alias("adresse_etab")
    )

    # Normalise etablissementSiege to boolean — the Parquet may store it as
    # a native bool (new format) or as a 'true'/'false' string (legacy CSV).
    if df["etablissementSiege"].dtype == pl.Boolean:
        df = df.with_columns(
            pl.col("etablissementSiege").alias("etablissement_siege")
        )
    else:
        df = df.with_columns(
            pl.col("etablissementSiege")
              .str.to_lowercase()
              .eq("true")
              .alias("etablissement_siege")
        )

    # Select and rename final columns (in table column order)
    df = df.select([
        pl.col("siret"),
        pl.col("siren"),
        pl.col("etablissement_siege"),
        pl.col("etatAdministratifEtablissement").alias("etat_administratif"),
        pl.col("enseigne1Etablissement").alias("enseigne_etablissement"),
        pl.col("denominationUsuelleEtablissement").alias("denomination_usuelle"),
        pl.col("activitePrincipaleEtablissement").alias("naf_etablissement"),
        pl.col("codePostalEtablissement").alias("code_postal_etab"),
        pl.col("adresse_etab"),
        pl.col("libelleCommuneEtablissement").alias("libelle_commune"),
    ])

    return df


# ── Main ──────────────────────────────────────────────────────────────────────

def main(source: str, dry_run: bool = False, limit: int | None = None) -> None:
    """Main entry point.

    source: local file path OR remote https:// URL.
    When dry_run=True and source is a URL, no full download happens — polars
    fetches only the row groups needed for the limit.
    """
    t_total = time.time()

    # For production runs we need psql; skip the check in dry-run URL mode.
    psql = None
    database_url = None
    if not dry_run:
        psql = _find_psql()
        print(f"Using psql: {psql}")
        database_url = os.environ.get("DATABASE_URL")
        if not database_url:
            sys.exit("DATABASE_URL environment variable is not set.")
        db_url = database_url
        print(f"DB endpoint: {db_url.split('@')[1] if '@' in db_url else '(hidden)'}")
    else:
        db_url = None

    # ── Step 1: Announce source ───────────────────────────────────────────────
    print(f"\n[1] Source: {source}")
    if source.startswith("https://") or source.startswith("http://"):
        print("  (URL mode — polars will fetch row groups on demand)")

    # ── Step 2: Read + filter Parquet ─────────────────────────────────────────
    df = _filter_and_shape(source, limit=limit)
    total_rows = len(df)
    print(f"  Shaped to {total_rows:,} rows x {len(df.columns)} columns")
    print(f"  Columns: {df.columns}")

    if dry_run:
        # Show a small sample and the NAF distribution
        print("\n[DRY RUN] Sample (first 5 rows):")
        print(df.head(5))
        naf_counts = (
            df.group_by("naf_etablissement")
            .agg(pl.len().alias("count"))
            .sort("count", descending=True)
            .head(10)
        )
        print("\nTop 10 NAF codes in sample:")
        print(naf_counts)
        print(
            f"\n[DRY RUN] Validation OK — {total_rows:,} rows parsed from "
            f"{source}. NOT touching the production database."
        )
        return

    # ── Step 3: Write filtered TSV to disk ────────────────────────────────────
    print(f"\n[3] Writing {total_rows:,} rows to {_TSV_PATH}...")
    t_csv = time.time()
    df.write_csv(_TSV_PATH, separator="\t", include_header=False, null_value="\\N")
    elapsed_csv = time.time() - t_csv
    tsv_size_gb = os.path.getsize(_TSV_PATH) / 1e9
    print(f"  TSV written: {tsv_size_gb:.2f} GB in {elapsed_csv:.1f}s")
    del df  # free RAM before DB phase

    # ── Step 4: Create staging table ──────────────────────────────────────────
    print(f"\n[4] Creating staging table {_STAGING_TABLE}...  [{time.strftime('%H:%M:%S')}]")
    # Drop any leftover from a previous aborted run
    _run_psql(psql, db_url, f"DROP TABLE IF EXISTS {_STAGING_TABLE};")
    _run_psql(psql, db_url, f"DROP TABLE IF EXISTS {_OLD_TABLE};")
    # Create staging without indexes — faster COPY, indexes added after swap
    _run_psql(
        psql, db_url,
        f"""CREATE TABLE {_STAGING_TABLE} (
            siret VARCHAR(14) NOT NULL,
            siren VARCHAR(9) NOT NULL,
            etablissement_siege BOOLEAN NOT NULL DEFAULT FALSE,
            etat_administratif VARCHAR(1) NOT NULL DEFAULT 'A',
            enseigne_etablissement TEXT,
            denomination_usuelle TEXT,
            naf_etablissement VARCHAR(10),
            code_postal_etab VARCHAR(10),
            adresse_etab TEXT,
            libelle_commune VARCHAR(100)
        );""",
    )

    # ── Step 5: psql \\copy staging <- TSV ───────────────────────────────────
    print(f"\n[5] \\copy {_TSV_PATH} -> {_STAGING_TABLE}...  [{time.strftime('%H:%M:%S')}]")
    t_copy = time.time()
    _run_psql_copy(psql, db_url, _TSV_PATH)
    elapsed_copy = time.time() - t_copy
    rows_per_s = total_rows / elapsed_copy if elapsed_copy > 0 else 0
    print(f"  COPY done in {elapsed_copy:.0f}s ({rows_per_s:,.0f} rows/s)  [{time.strftime('%H:%M:%S')}]")

    # ── Step 6: ANALYZE staging ───────────────────────────────────────────────
    print(f"\n[6] ANALYZE {_STAGING_TABLE}...  [{time.strftime('%H:%M:%S')}]")
    _run_psql(psql, db_url, f"ANALYZE {_STAGING_TABLE};")

    # ── Step 7: Add indexes + atomic swap ────────────────────────────────────
    # Add indexes to staging BEFORE swap so the live table arrives with indexes.
    print(f"\n[7] Adding indexes to {_STAGING_TABLE}...  [{time.strftime('%H:%M:%S')}]")
    _run_psql(
        psql, db_url,
        f"ALTER TABLE {_STAGING_TABLE} ADD PRIMARY KEY (siret);",
    )
    _run_psql(
        psql, db_url,
        f"CREATE INDEX idx_etab_new_siren ON {_STAGING_TABLE}(siren);",
    )
    _run_psql(
        psql, db_url,
        f"CREATE INDEX idx_etab_new_cp_naf ON {_STAGING_TABLE}(code_postal_etab, naf_etablissement);",
    )

    # Atomic rename swap — no downtime: rename live out, rename staging in.
    # The two RENAMEs are separate psql calls because ALTER TABLE ... RENAME
    # is DDL and Neon may not allow multiple DDL statements in a single -c.
    print(f"\n  Atomic swap: {_LIVE_TABLE} -> {_OLD_TABLE} -> {_STAGING_TABLE} -> {_LIVE_TABLE}")
    _run_psql(psql, db_url, f"ALTER TABLE {_LIVE_TABLE} RENAME TO {_OLD_TABLE};")
    _run_psql(psql, db_url, f"ALTER TABLE {_STAGING_TABLE} RENAME TO {_LIVE_TABLE};")
    _run_psql(psql, db_url, f"DROP TABLE IF EXISTS {_OLD_TABLE};")
    print(f"  Swap complete — {_OLD_TABLE} dropped.")

    # ── Step 8: Stats + summary ───────────────────────────────────────────────
    print(f"\n[8] Post-import stats...  [{time.strftime('%H:%M:%S')}]")

    count_rows = _run_psql_query(psql, db_url, f"SELECT COUNT(*) FROM {_LIVE_TABLE};")
    count_sirens = _run_psql_query(
        psql, db_url, f"SELECT COUNT(DISTINCT siren) FROM {_LIVE_TABLE};"
    )
    naf_dist = _run_psql_query(
        psql, db_url,
        f"""SELECT naf_etablissement, COUNT(*) AS n
              FROM {_LIVE_TABLE}
          GROUP BY 1
          ORDER BY 2 DESC
          LIMIT 10;""",
    )

    print(f"\n  Total rows:       {count_rows[0][0] if count_rows else '?':>12}")
    print(f"  Distinct SIRENs:  {count_sirens[0][0] if count_sirens else '?':>12}")
    print("\n  Top 10 NAF codes:")
    for row in naf_dist:
        naf = row[0] if row[0] else "(null)"
        n = row[1] if len(row) > 1 else "?"
        print(f"    {naf:10s}  {n:>10}")

    total_elapsed = time.time() - t_total
    print(
        f"\nDONE: {total_rows:,} rows imported from {source} "
        f"into {_LIVE_TABLE} in {total_elapsed:.0f}s ({total_elapsed/60:.1f} min)."
    )


# ── CLI ───────────────────────────────────────────────────────────────────────

def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description=(
            "Bulk-import INSEE StockEtablissement into the establishments table."
        )
    )
    parser.add_argument(
        "--parquet",
        metavar="PATH",
        default=None,
        help=(
            "Path to a pre-downloaded Parquet file. "
            "If omitted, downloads from INSEE_ETAB_URL."
        ),
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help=(
            "Parse and validate only — do NOT write to the database. "
            "Use with --limit to restrict how many rows are fetched."
        ),
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        metavar="N",
        help=(
            "Process only the first N rows. "
            "In dry-run mode, polars fetches only the necessary row groups "
            "from the remote URL — no full download needed."
        ),
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

    if args.parquet is not None:
        # Use pre-downloaded local file
        source = args.parquet
    elif args.dry_run:
        # Dry-run: use remote URL directly — polars fetches lazily, no full download
        print(f"[DRY RUN] Using remote URL directly (no full download).")
        source = INSEE_ETAB_URL
    else:
        # Production run: download full Parquet first
        import tempfile
        tmp = tempfile.NamedTemporaryFile(suffix=".parquet", delete=False)
        tmp.close()
        source = tmp.name
        try:
            download_parquet(source)
        except Exception as exc:
            print(f"Download failed: {exc}", file=sys.stderr)
            sys.exit(1)

    main(source, dry_run=args.dry_run, limit=args.limit)
