"""Smart column mapper — maps arbitrary CSV/XLSX column names to Fortress schema.

Uses a fuzzy alias dictionary to recognize French/English column headers
from various data providers (KOMPASS, CRMs, etc.) and map them to
the correct Fortress table and field.

Columns that don't match any alias are stored in companies.extra_data (JSONB).
"""

from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass, field


# ---------------------------------------------------------------------------
# Column mapping result
# ---------------------------------------------------------------------------

@dataclass
class MappedColumn:
    """Represents a single mapped column from an uploaded file."""
    source_name: str          # Original header from the file
    target_table: str         # 'companies' | 'contacts' | 'officers' | 'extra_data'
    target_field: str         # DB column name, or the original name if overflow
    confidence: float = 1.0   # 0-1, how confident the match is


@dataclass
class MappingResult:
    """Complete mapping result for an uploaded file."""
    columns: list[MappedColumn] = field(default_factory=list)
    siren_column: int | None = None     # Index of the SIREN column
    has_officer_data: bool = False       # File contains person data (prénom, nom)

    @property
    def recognized(self) -> list[MappedColumn]:
        return [c for c in self.columns if c.target_table != 'extra_data']

    @property
    def overflow(self) -> list[MappedColumn]:
        return [c for c in self.columns if c.target_table == 'extra_data']


# ---------------------------------------------------------------------------
# Alias dictionary — maps normalized patterns to (table, field)
# Order matters: first match wins. More specific patterns first.
# ---------------------------------------------------------------------------

# Each entry: (target_table, target_field, [list of normalized alias patterns])
ALIAS_REGISTRY: list[tuple[str, str, list[str]]] = [
    # ── SIREN / Registration ──
    ("companies", "siren", [
        "siren", "n siren", "numero siren", "siret siren",
        "numero denregistrement", "numero d enregistrement",
        "registration number", "id entreprise",
        "siret",  # SIRET → we extract first 9 digits
    ]),

    # ── Company identity ──
    ("companies", "denomination", [
        "nom de l entreprise", "nom de lentreprise", "denomination",
        "raison sociale", "societe", "entreprise", "company name",
        "nom entreprise", "denomination sociale",
    ]),
    ("companies", "enseigne", [
        "denomination commerciale", "enseigne", "sigle",
        "nom commercial", "trading name", "brand name",
    ]),
    ("companies", "naf_code", [
        "code naf 2008", "code naf", "naf 2008", "naf", "code ape", "ape",
        "naf code", "activite principale",
    ]),
    ("companies", "naf_libelle", [
        "libelle naf 2008", "libelle naf", "libelle activite",
        "activite", "naf libelle", "secteur activite",
        "secteur d activite",
    ]),
    ("companies", "forme_juridique", [
        "forme juridique", "statut juridique", "legal form",
        "type societe", "categorie juridique",
    ]),

    # ── Address ──
    ("companies", "adresse", [
        "rue", "adresse", "street", "address", "voie",
        "numero et rue", "adresse postale",
    ]),
    ("companies", "code_postal", [
        "code postal", "cp", "postal code", "zip",
        "code postal legal",
    ]),
    ("companies", "ville", [
        "ville", "city", "commune", "localite",
        "ville legale",
    ]),
    ("companies", "departement", [
        "departement", "dept", "department",
        "departement district", "district",
    ]),
    ("companies", "region", [
        "region",
    ]),

    # ── Workforce & Revenue ──
    ("companies", "tranche_effectif", [
        "effectif entreprise", "effectif a l adresse",
        "effectif", "headcount", "employees", "nb salaries",
        "tranche effectif", "taille entreprise",
    ]),
    ("companies", "effectif_exact", [
        "effectif exact", "effectif exact de l entreprise",
        "effectif exact a l adresse", "nombre exact de salaries",
    ]),
    ("companies", "chiffre_affaires", [
        "chiffre d affaires brut", "chiffre d affaires",
        "ca brut", "ca", "revenue", "turnover",
        "chiffre affaires",
    ]),
    ("companies", "annee_ca", [
        "annee du ca brut", "annee du ca", "annee ca",
        "year revenue",
    ]),
    ("companies", "tranche_ca", [
        "tranche du ca brut", "tranche du ca",
        "tranche ca", "revenue range",
    ]),
    ("companies", "date_fondation", [
        "date de fondation", "date fondation",
        "date creation", "founded",
    ]),
    # Note: "Année de fondation" (year only) is handled specially in ingestion
    ("companies", "type_etablissement", [
        "type d etablissement", "type etablissement",
        "establishment type",
    ]),

    # ── Contact fields ──
    ("contacts", "phone", [
        "numero de telephone", "telephone", "tel", "phone",
        "phone number", "tel fixe", "telephone fixe",
    ]),
    ("contacts", "email", [
        "email", "e mail", "courriel", "mail",
        "email entreprise", "certified email",
        "adresse email", "adresse e mail",
    ]),
    ("contacts", "website", [
        "site web", "website", "url", "site internet",
        "web", "site", "adresse web",
    ]),
    ("contacts", "social_linkedin", [
        "linkedin", "reseaux sociaux", "social",
        "social linkedin", "lien linkedin",
    ]),
    ("contacts", "social_facebook", [
        "facebook", "social facebook", "lien facebook",
    ]),
    ("contacts", "social_twitter", [
        "twitter", "x com", "social twitter",
    ]),

    # ── Officer (person) fields ──
    ("officers", "civilite", [
        "civilite", "title", "mr mrs",
    ]),
    ("officers", "prenom", [
        "prenom", "deuxieme prenom", "first name",
        "given name",
    ]),
    ("officers", "nom", [
        "nom", "last name", "surname", "family name",
        "nom de famille",
    ]),
    ("officers", "role", [
        "fonction", "function", "job title", "poste",
        "role", "libelle personnalise",
    ]),
    ("officers", "code_fonction", [
        "code fonction", "function code",
    ]),
    ("officers", "type_fonction", [
        "type de fonction", "function type",
        "type fonction",
    ]),
    ("officers", "email_direct", [
        "email direct dirigeant", "email direct",
        "direct email", "email personnel",
        "email responsable",
    ]),
    ("officers", "ligne_directe", [
        "ligne directe", "direct line", "direct phone",
        "telephone direct", "tel direct",
    ]),
]

# Columns to skip entirely (no value for the database)
SKIP_PATTERNS: set[str] = {
    "genre", "source de la donnee contact", "identifiant kompass",
    "company url",  # Kompass profile URL, not the company's website
    "phone additional info", "tps", "telephone preference service",
    "non mailing indicator", "fps", "fax preference service",
    "emps", "email preference service",
    "pays", "code pays", "etat",  # We know it's France
    "complement d adresse", "complement d adresse postale",
    "rue postale", "boite postale",
    "registration address", "pays d enregistrement",
    "rue d enregistrement", "complement d enregistrement",
    "boite d enregistrement",
    "adresse legale", "pays legal", "rue legale",
    "complement legal", "ville legale", "code postal legal",
    "recherche par texte",  # Kompass internal search text
}


# ---------------------------------------------------------------------------
# Text normalization
# ---------------------------------------------------------------------------

def _normalize(text: str) -> str:
    """Normalize a column header for fuzzy matching.

    Strips accents, lowercases, removes all punctuation/special chars,
    collapses whitespace.
    """
    if not text:
        return ""
    # Strip accents
    nfkd = unicodedata.normalize("NFKD", text)
    ascii_text = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Lowercase
    ascii_text = ascii_text.lower()
    # Remove special chars (keep letters, digits, spaces)
    ascii_text = re.sub(r"[^a-z0-9\s]", " ", ascii_text)
    # Collapse whitespace
    ascii_text = re.sub(r"\s+", " ", ascii_text).strip()
    return ascii_text


# ---------------------------------------------------------------------------
# SIREN normalization
# ---------------------------------------------------------------------------

_DIGITS_RE = re.compile(r"\d+")


def normalize_siren(raw: str) -> str | None:
    """Extract a valid 9-digit SIREN from various French registration formats.

    Handles:
    - SIREN (9 digits): '420916918' → '420916918'
    - SIRET (14 digits): '42091691800048' → '420916918'
    - SIRET with space: '420916918 00048' → '420916918'
    - SIREN with spaces: '385 018 254' → '385018254'
    - TVA intra: 'FR13 420916918' → '420916918'
    - TVA intra (no space): 'FR13420916918' → '420916918'
    """
    if not raw:
        return None
    raw = str(raw).strip()

    # Extract all digit groups
    digits = "".join(_DIGITS_RE.findall(raw))

    if not digits:
        return None

    # TVA intracommunautaire: FR + 2 check digits + 9 SIREN
    # After stripping FR prefix, we get 11 digits → last 9 = SIREN
    upper_raw = raw.upper().replace(" ", "")
    if upper_raw.startswith("FR") and len(digits) == 11:
        siren = digits[2:11]
        if len(siren) == 9:
            return siren

    # SIRET (14 digits) → first 9 = SIREN
    if len(digits) == 14:
        return digits[:9]

    # SIREN (9 digits)
    if len(digits) == 9:
        return digits

    # Longer than 14 → try first 9
    if len(digits) > 14:
        candidate = digits[:9]
        if len(candidate) == 9:
            return candidate

    # Shorter than 9 → invalid
    return None


# ---------------------------------------------------------------------------
# Main mapper function
# ---------------------------------------------------------------------------

def map_columns(headers: list[str]) -> MappingResult:
    """Map a list of column headers to Fortress schema fields.

    Returns a MappingResult with all columns classified.
    """
    result = MappingResult()
    used_targets: set[tuple[str, str]] = set()  # Prevent double-mapping

    for col_idx, header in enumerate(headers):
        if not header:
            result.columns.append(MappedColumn(
                source_name=header or f"Col_{col_idx}",
                target_table="extra_data",
                target_field=f"col_{col_idx}",
            ))
            continue

        norm = _normalize(header)

        # Skip useless columns
        if norm in SKIP_PATTERNS:
            result.columns.append(MappedColumn(
                source_name=header,
                target_table="skip",
                target_field="",
            ))
            continue

        # Try alias registry
        matched = False
        for target_table, target_field, aliases in ALIAS_REGISTRY:
            if (target_table, target_field) in used_targets:
                # Already mapped this field from an earlier column
                # Exception: officer fields CAN appear per-row (multiple officers)
                if target_table != "officers":
                    continue

            if norm in aliases or any(alias in norm for alias in aliases):
                result.columns.append(MappedColumn(
                    source_name=header,
                    target_table=target_table,
                    target_field=target_field,
                    confidence=1.0 if norm in aliases else 0.8,
                ))
                used_targets.add((target_table, target_field))

                if target_field == "siren":
                    result.siren_column = col_idx
                if target_table == "officers" and target_field in ("prenom", "nom"):
                    result.has_officer_data = True

                matched = True
                break

        if not matched:
            # Check if it's a well-known skip pattern (partial match)
            skip = False
            for skip_pat in SKIP_PATTERNS:
                if skip_pat in norm or norm in skip_pat:
                    skip = True
                    break

            if skip:
                result.columns.append(MappedColumn(
                    source_name=header,
                    target_table="skip",
                    target_field="",
                ))
            else:
                # Overflow → extra_data
                result.columns.append(MappedColumn(
                    source_name=header,
                    target_table="extra_data",
                    target_field=header.strip(),
                ))

    return result
""", "CodeMarkdownLanguage": "python", "Complexity": 8, "Description": "Core intelligence module for the Smart Upload Engine — maps arbitrary column names from any French data provider to Fortress fields, with SIREN/SIRET/TVA normalization", "EmptyFile": false, "IsArtifact": false, "Overwrite": false, "TargetFile": "/Users/alancohen/Downloads/Project Alan copy/fortress/fortress/api/column_mapper.py"
