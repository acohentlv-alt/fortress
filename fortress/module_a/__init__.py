"""Module A — Data Lake: SIRENE ingestion, query interpreter, triage."""

from fortress.module_a.sirene_downloader import download_sirene
from fortress.module_a.sirene_ingester import ingest_sirene, normalize_naf_code

__all__ = [
    "download_sirene",
    "ingest_sirene",
    "normalize_naf_code",
]
