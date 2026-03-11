# Column name flexibility for CSV imports
"""
Maps common French business column names to internal field names.
Used when importing client CSVs with varying column naming conventions.

Usage:
    from fortress.utils.column_mapper import map_columns
    mapped_df = map_columns(raw_dataframe)
"""

import unicodedata
import re
from typing import Any

import structlog

log = structlog.get_logger()

# ── Alias mapping ────────────────────────────────────────────────────────────
# Keys: lowercased, accent-stripped versions of common column names.
# Values: internal field names used by the Fortress pipeline.

COLUMN_ALIASES: dict[str, str] = {
    # SIREN
    "siren": "siren",
    "n siren": "siren",
    "no siren": "siren",
    "numero siren": "siren",
    "num siren": "siren",
    "n° siren": "siren",
    "numéro siren": "siren",
    "code siren": "siren",

    # SIRET
    "siret": "siret",
    "n siret": "siret",
    "numero siret": "siret",
    "siret siege": "siret",

    # Company name
    "denomination": "denomination",
    "dénomination": "denomination",
    "denomination sociale": "denomination",
    "raison sociale": "denomination",
    "nom": "denomination",
    "nom de l'entreprise": "denomination",
    "nom entreprise": "denomination",
    "name": "denomination",
    "company": "denomination",
    "company name": "denomination",
    "societe": "denomination",
    "société": "denomination",
    "nom commercial": "denomination",
    "enseigne": "denomination",
    "entite": "denomination",
    "nom de l'entite": "denomination",
    "nom entite": "denomination",
    "denominations": "denomination",

    # Phone
    "telephone": "phone",
    "téléphone": "phone",
    "tel": "phone",
    "tél": "phone",
    "phone": "phone",
    "mobile": "phone",
    "portable": "phone",
    "numero telephone": "phone",
    "n telephone": "phone",
    "numero de telephone": "phone",
    "tel fixe": "phone",
    "telephone fixe": "phone",
    "telephone mobile": "phone",

    # Email
    "email": "email",
    "e-mail": "email",
    "mail": "email",
    "courriel": "email",
    "adresse mail": "email",
    "adresse email": "email",
    "adresse e-mail": "email",
    "contact email": "email",
    "email contact": "email",

    # Website
    "website": "website",
    "site web": "website",
    "site internet": "website",
    "url": "website",
    "site": "website",
    "web": "website",
    "page web": "website",

    # Address
    "adresse": "adresse",
    "address": "adresse",
    "adresse postale": "adresse",
    "rue": "adresse",
    "adresse siege": "adresse",

    # City
    "ville": "ville",
    "city": "ville",
    "commune": "ville",
    "localite": "ville",
    "localité": "ville",

    # Postal code
    "code postal": "code_postal",
    "cp": "code_postal",
    "zip": "code_postal",
    "code_postal": "code_postal",
    "zipcode": "code_postal",
    "postal code": "code_postal",

    # Department
    "departement": "departement",
    "département": "departement",
    "dept": "departement",
    "dep": "departement",
    "code departement": "departement",

    # NAF / APE
    "naf": "naf_code",
    "code naf": "naf_code",
    "code ape": "naf_code",
    "ape": "naf_code",
    "naf_code": "naf_code",
    "activite": "naf_code",
    "activité": "naf_code",

    # Legal form
    "forme juridique": "forme_juridique",
    "statut juridique": "forme_juridique",
    "type societe": "forme_juridique",
    "forme": "forme_juridique",
}


def _normalize(text: str) -> str:
    """Lowercase, strip accents, collapse whitespace, remove special chars."""
    # Lowercase
    text = text.lower().strip()
    # Strip accents (keep the base character)
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    # Remove special chars except spaces and hyphens
    text = re.sub(r"[^\w\s-]", " ", text)
    # Collapse whitespace
    text = re.sub(r"\s+", " ", text).strip()
    return text


def map_columns(columns: list[str]) -> dict[str, str]:
    """Map raw CSV column names to internal Fortress field names.

    Args:
        columns: List of column names from the uploaded CSV.

    Returns:
        dict mapping original_column_name → internal_field_name
        for all recognized columns. Unrecognized columns are omitted.
    """
    mapping: dict[str, str] = {}
    used_fields: set[str] = set()  # Prevent duplicate mappings

    for col in columns:
        normalized = _normalize(col)

        # Direct match
        if normalized in COLUMN_ALIASES:
            field = COLUMN_ALIASES[normalized]
            if field not in used_fields:
                mapping[col] = field
                used_fields.add(field)
                continue

        # Try without hyphens/underscores
        collapsed = normalized.replace("-", " ").replace("_", " ")
        if collapsed in COLUMN_ALIASES:
            field = COLUMN_ALIASES[collapsed]
            if field not in used_fields:
                mapping[col] = field
                used_fields.add(field)

    # Log what was mapped
    log.info(
        "column_mapper.mapped",
        recognized=len(mapping),
        total=len(columns),
        mappings={k: v for k, v in mapping.items()},
        ignored=[c for c in columns if c not in mapping],
    )

    return mapping


def rename_dataframe_columns(df: Any, mapping: dict[str, str] | None = None) -> Any:
    """Rename a pandas DataFrame's columns using the alias mapping.

    Args:
        df: pandas DataFrame with raw column names.
        mapping: Optional pre-computed mapping. If None, computed from df.columns.

    Returns:
        DataFrame with renamed columns.
    """
    if mapping is None:
        mapping = map_columns(list(df.columns))
    return df.rename(columns=mapping)
