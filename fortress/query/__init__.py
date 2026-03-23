"""Query — SIRENE ingestion, query interpreter, triage."""

from fortress.query.sirene_download import download_sirene
from fortress.query.sirene_ingest import ingest_sirene, normalize_naf_code

__all__ = [
    "download_sirene",
    "ingest_sirene",
    "normalize_naf_code",
]
