"""SIRENE bulk data downloader.

Downloads the latest StockUniteLegale Parquet (or ZIP fallback) from data.gouv.fr.

Usage:
    python -m fortress.query.sirene_download [--force]

Behaviour:
- Queries the data.gouv.fr dataset API to locate the latest Parquet file.
- Skips download if the local file is < 32 days old (unless --force).
- Verifies the download against the SHA-256 checksum advertised by the API.
- Shows download progress via a simple streaming counter.
- Saves to settings.SIRENE_DIR / "StockUniteLegale.parquet"
  (falls back to "StockUniteLegale.zip" if no Parquet is available).
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import NamedTuple

import httpx
import structlog

from fortress.config.settings import settings

log = structlog.get_logger(__name__)

# data.gouv.fr dataset ID for SIRENE StockUniteLegale
DATASET_API_URL = "https://www.data.gouv.fr/api/1/datasets/5b7ffc618b4c4169d30727e0/"

# Direct fallback URLs if the API returns nothing usable (verified Feb 2026)
FALLBACK_PARQUET_URL = "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockUniteLegale_utf8.parquet"
FALLBACK_ZIP_URL = "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockUniteLegale_utf8.zip"

# StockEtablissement — establishment-level data (~2.1 GB Parquet, ~2.8 GB ZIP)
# Contains address fields (code_postal, ville, adresse, lat/lon) per establishment.
# Required to populate companies.code_postal / ville / departement after UniteLegale ingestion.
FALLBACK_ETABLISSEMENT_PARQUET_URL = "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockEtablissement_utf8.parquet"
FALLBACK_ETABLISSEMENT_ZIP_URL = "https://object.files.data.gouv.fr/data-pipeline-open/siren/stock/StockEtablissement_utf8.zip"

# Maximum age before we consider the local file stale
MAX_FILE_AGE_DAYS: int = 32

# Chunk size used when streaming the download (8 MiB)
CHUNK_SIZE: int = 8 * 1024 * 1024


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------


class ResourceInfo(NamedTuple):
    """Metadata about a SIRENE resource on data.gouv.fr."""

    url: str
    filename: str
    checksum_sha256: str | None
    mime_type: str | None


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _is_parquet(resource: dict) -> bool:
    """Return True if the resource looks like a Parquet file."""
    url: str = resource.get("url", "")
    mime: str = resource.get("mime", "") or ""
    title: str = resource.get("title", "") or ""
    return (
        url.endswith(".parquet")
        or "parquet" in mime.lower()
        or "parquet" in title.lower()
        or "StockUniteLegale" in url
        and url.endswith(".parquet")
    )


def _is_zip(resource: dict) -> bool:
    """Return True if the resource looks like a ZIP file for StockUniteLegale."""
    url: str = resource.get("url", "")
    title: str = resource.get("title", "") or ""
    return (
        url.endswith(".zip")
        and "StockUniteLegale" in (url + title)
    )


def _extract_sha256(resource: dict) -> str | None:
    """Pull the SHA-256 checksum from a data.gouv.fr resource dict, if present."""
    checksum: dict | None = resource.get("checksum")
    if checksum and checksum.get("type", "").lower() == "sha256":
        return checksum.get("value")
    return None


def _file_is_recent(path: Path, max_age_days: int = MAX_FILE_AGE_DAYS) -> bool:
    """Return True if the file exists and its mtime is within max_age_days."""
    if not path.exists():
        return False
    age = datetime.now(timezone.utc) - datetime.fromtimestamp(
        path.stat().st_mtime, tz=timezone.utc
    )
    return age < timedelta(days=max_age_days)


def _normalize_filename(url: str) -> str:
    """Extract the bare filename from a URL."""
    return url.rstrip("/").split("/")[-1].split("?")[0]


def _verify_sha256(path: Path, expected: str) -> bool:
    """Compute SHA-256 of *path* and compare against *expected*."""
    log.info("checksum_verifying", path=str(path))
    sha = hashlib.sha256()
    with path.open("rb") as fh:
        for chunk in iter(lambda: fh.read(CHUNK_SIZE), b""):
            sha.update(chunk)
    actual = sha.hexdigest()
    if actual.lower() == expected.lower():
        log.info("checksum_ok", sha256=actual[:16] + "...")
        return True
    log.error(
        "checksum_mismatch",
        expected=expected[:16] + "...",
        actual=actual[:16] + "...",
    )
    return False


# ---------------------------------------------------------------------------
# Core async functions
# ---------------------------------------------------------------------------


async def fetch_dataset_resources(client: httpx.AsyncClient) -> list[ResourceInfo]:
    """Query the data.gouv.fr API and return a ranked list of SIRENE resources.

    Parquet files are returned first; ZIP files are returned as fallback.
    """
    log.info("api_query", url=DATASET_API_URL)
    resp = await client.get(DATASET_API_URL, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    resources: list[dict] = data.get("resources", [])
    parquets: list[ResourceInfo] = []
    zips: list[ResourceInfo] = []

    for r in resources:
        url: str = r.get("url", "")
        if not url:
            continue
        info = ResourceInfo(
            url=url,
            filename=_normalize_filename(url),
            checksum_sha256=_extract_sha256(r),
            mime_type=r.get("mime"),
        )
        if _is_parquet(r):
            parquets.append(info)
        elif _is_zip(r):
            zips.append(info)

    log.info(
        "api_resources_found",
        parquet_count=len(parquets),
        zip_count=len(zips),
    )
    return parquets + zips


async def download_file(
    client: httpx.AsyncClient,
    url: str,
    dest: Path,
) -> None:
    """Stream *url* to *dest*, printing progress to stdout."""
    log.info("download_start", url=url, dest=str(dest))
    dest.parent.mkdir(parents=True, exist_ok=True)

    tmp = dest.with_suffix(dest.suffix + ".tmp")
    bytes_downloaded: int = 0
    last_print_time: float = time.monotonic()

    async with client.stream("GET", url, timeout=None, follow_redirects=True) as resp:
        resp.raise_for_status()
        total_str: str = resp.headers.get("content-length", "?")
        total_bytes: int | None = int(total_str) if total_str != "?" else None

        with tmp.open("wb") as fh:
            async for chunk in resp.aiter_bytes(chunk_size=CHUNK_SIZE):
                fh.write(chunk)
                bytes_downloaded += len(chunk)

                now = time.monotonic()
                if now - last_print_time >= 5.0:
                    downloaded_mb = bytes_downloaded / 1_048_576
                    if total_bytes:
                        total_mb = total_bytes / 1_048_576
                        pct = bytes_downloaded / total_bytes * 100
                        print(
                            f"  Downloading ... {downloaded_mb:.0f} MB"
                            f" / {total_mb:.0f} MB  ({pct:.1f}%)",
                            flush=True,
                        )
                    else:
                        print(
                            f"  Downloading ... {downloaded_mb:.0f} MB",
                            flush=True,
                        )
                    last_print_time = now

    # Atomic rename
    tmp.replace(dest)
    log.info(
        "download_complete",
        dest=str(dest),
        size_mb=round(bytes_downloaded / 1_048_576, 1),
    )
    print(
        f"  Download complete: {dest.name}"
        f"  ({bytes_downloaded / 1_048_576:.1f} MB)",
        flush=True,
    )


async def download_sirene(*, force: bool = False) -> Path:
    """Main entry-point: download the latest SIRENE StockUniteLegale file.

    Returns the path to the downloaded (or already-existing) file.

    Args:
        force: If True, re-download even if the local file is recent.

    Raises:
        RuntimeError: If no usable resource is found and the fallback also fails.
    """
    sirene_dir: Path = settings.sirene_dir
    sirene_dir.mkdir(parents=True, exist_ok=True)

    # --- Early exit: skip ALL network calls if a recent file already exists ---
    # Check for both extensions so we don't hit the API unnecessarily.
    if not force:
        for _ext in (".parquet", ".zip"):
            _candidate = sirene_dir / f"StockUniteLegale{_ext}"
            if _file_is_recent(_candidate):
                log.info("download_skipped_recent", path=str(_candidate))
                return _candidate

    async with httpx.AsyncClient(
        headers={"User-Agent": "Fortress/0.1 (data.gouv.fr API client)"},
        timeout=httpx.Timeout(connect=15, read=600, write=60, pool=10),
    ) as client:
        # --- Discover latest resource via API ---
        try:
            resources = await fetch_dataset_resources(client)
        except Exception as exc:
            log.warning("api_query_failed", error=str(exc), fallback=True)
            resources = []

        # Determine which resource to use
        resource: ResourceInfo | None = resources[0] if resources else None

        if resource is None:
            log.warning("no_api_resource_found", using_fallback=FALLBACK_PARQUET_URL)
            resource = ResourceInfo(
                url=FALLBACK_PARQUET_URL,
                filename="StockUniteLegale_utf8.parquet",
                checksum_sha256=None,
                mime_type="application/parquet",
            )

        # Determine canonical local path based on the extension of the chosen resource
        ext = Path(resource.filename).suffix  # e.g. ".parquet" or ".zip"
        if not ext:
            ext = ".parquet" if "parquet" in resource.url.lower() else ".zip"
        local_path = sirene_dir / f"StockUniteLegale{ext}"

        # --- Skip check ---
        if not force and _file_is_recent(local_path):
            age_days = (
                datetime.now(timezone.utc)
                - datetime.fromtimestamp(local_path.stat().st_mtime, tz=timezone.utc)
            ).days
            log.info(
                "download_skipped_recent",
                path=str(local_path),
                age_days=age_days,
            )
            print(
                f"  Skipping download — {local_path.name} is only {age_days} days old."
                f"  Use --force to re-download.",
                flush=True,
            )
            return local_path

        # --- Download ---
        await download_file(client, resource.url, local_path)

        # --- Verify checksum ---
        if resource.checksum_sha256:
            ok = _verify_sha256(local_path, resource.checksum_sha256)
            if not ok:
                local_path.unlink(missing_ok=True)
                raise RuntimeError(
                    f"SHA-256 mismatch for {local_path.name}. "
                    "File deleted. Re-run to retry the download."
                )
        else:
            log.info("checksum_unavailable", path=str(local_path))

        return local_path


# ---------------------------------------------------------------------------
# StockEtablissement download
# ---------------------------------------------------------------------------


def _is_etablissement_resource(resource: dict) -> bool:
    """Return True if the resource is a StockEtablissement file (parquet or zip).

    Explicitly excludes StockEtablissementHistorique — historical snapshots that
    are ~10× larger and not needed for current address data.
    """
    url: str = resource.get("url", "")
    title: str = resource.get("title", "") or ""
    combined = url + title
    if "Historique" in combined or "historique" in combined:
        return False
    return "StockEtablissement" in combined


async def download_etablissement(*, force: bool = False) -> Path:
    """Download the latest SIRENE StockEtablissement file (~2.1 GB Parquet).

    Mirrors the behaviour of download_sirene() but targets the establishment
    file.  Saves to settings.sirene_dir / "StockEtablissement.parquet"
    (or .zip if the API only offers ZIP).

    Args:
        force: If True, re-download even if the local file is < 32 days old.

    Returns:
        Path to the downloaded (or already-existing) local file.
    """
    sirene_dir: Path = settings.sirene_dir
    sirene_dir.mkdir(parents=True, exist_ok=True)

    # --- Early exit: skip ALL network calls if a recent file already exists ---
    if not force:
        for _ext in (".parquet", ".zip"):
            _candidate = sirene_dir / f"StockEtablissement{_ext}"
            if _file_is_recent(_candidate):
                log.info("etablissement_download_skipped_recent", path=str(_candidate))
                return _candidate

    async with httpx.AsyncClient(
        headers={"User-Agent": "Fortress/0.1 (data.gouv.fr API client)"},
        timeout=httpx.Timeout(connect=15, read=600, write=60, pool=10),
    ) as client:
        # --- Discover latest resource via API ---
        try:
            all_resources = await fetch_dataset_resources(client)
            # Filter to StockEtablissement resources only, prefer Parquet
            etab = [r for r in all_resources if _is_etablissement_resource(r._asdict())]
            parquets = [r for r in etab if r.filename.lower().endswith(".parquet")]
            zips = [r for r in etab if r.filename.lower().endswith(".zip")]
            resources = parquets + zips
        except Exception as exc:
            log.warning(
                "etablissement_api_query_failed", error=str(exc), fallback=True
            )
            resources = []

        resource: ResourceInfo | None = resources[0] if resources else None

        if resource is None:
            log.warning(
                "no_etablissement_resource_found",
                using_fallback=FALLBACK_ETABLISSEMENT_PARQUET_URL,
            )
            resource = ResourceInfo(
                url=FALLBACK_ETABLISSEMENT_PARQUET_URL,
                filename="StockEtablissement_utf8.parquet",
                checksum_sha256=None,
                mime_type="application/parquet",
            )

        ext = Path(resource.filename).suffix
        if not ext:
            ext = ".parquet" if "parquet" in resource.url.lower() else ".zip"
        local_path = sirene_dir / f"StockEtablissement{ext}"

        # --- Skip if recent ---
        if not force and _file_is_recent(local_path):
            age_days = (
                datetime.now(timezone.utc)
                - datetime.fromtimestamp(local_path.stat().st_mtime, tz=timezone.utc)
            ).days
            log.info(
                "etablissement_download_skipped_recent",
                path=str(local_path),
                age_days=age_days,
            )
            print(
                f"  Skipping download — {local_path.name} is only {age_days} days old."
                f"  Use --force to re-download.",
                flush=True,
            )
            return local_path

        # --- Download ---
        await download_file(client, resource.url, local_path)

        # --- Verify checksum ---
        if resource.checksum_sha256:
            ok = _verify_sha256(local_path, resource.checksum_sha256)
            if not ok:
                local_path.unlink(missing_ok=True)
                raise RuntimeError(
                    f"SHA-256 mismatch for {local_path.name}. "
                    "File deleted. Re-run to retry the download."
                )
        else:
            log.info("checksum_unavailable", path=str(local_path))

        return local_path


# ---------------------------------------------------------------------------
# CLI entry-point
# ---------------------------------------------------------------------------


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download the SIRENE StockUniteLegale bulk file from data.gouv.fr."
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Force re-download even if the local file is < 32 days old.",
    )
    return parser.parse_args(argv)


async def _main(argv: list[str] | None = None) -> None:
    args = _parse_args(argv)
    print("Fortress — SIRENE Downloader", flush=True)
    print(f"  Target directory: {settings.sirene_dir}", flush=True)

    try:
        path = await download_sirene(force=args.force)
        print(f"  Ready: {path}", flush=True)
    except Exception as exc:
        log.error("download_failed", error=str(exc))
        print(f"  ERROR: {exc}", file=sys.stderr, flush=True)
        sys.exit(1)


if __name__ == "__main__":
    asyncio.run(_main())
