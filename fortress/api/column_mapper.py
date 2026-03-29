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
# ORDER MATTERS: more-specific entries must come before generic ones.
# Officer fields before contacts to prevent "email direct dirigeant" → contacts.email.
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
        "code naf 2008", "code naf", "naf 2008", "code ape", "ape",
        "naf code", "activite principale",
    ]),
    ("companies", "naf_libelle", [
        "libelle naf 2008", "libelle naf", "libelle activite",
        "naf libelle", "secteur activite",
        "secteur d activite",
        "type d activite", "type activite",
    ]),
    # NOTE: companies.statut must be BEFORE forme_juridique so exact match on "statut"
    # hits here, not "statut juridique" → forme_juridique via substring match.
    ("companies", "statut", [
        "statut", "etat", "statut entreprise", "actif inactif",
    ]),
    ("companies", "forme_juridique", [
        "forme juridique", "statut juridique", "legal form",
        "type societe", "categorie juridique",
    ]),

    # ── Address ──
    ("companies", "adresse", [
        "rue", "adresse", "street", "address", "voie",
        "numero et rue",
    ]),
    ("companies", "code_postal", [
        "code postal", "cp", "postal code", "zip",
    ]),
    ("companies", "ville", [
        "ville", "city", "commune", "localite",
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
        "effectif exact de l entreprise", "effectif exact a l adresse",
        "effectif exact", "nombre exact de salaries",
    ]),
    ("companies", "chiffre_affaires", [
        "chiffre d affaires brut", "chiffre d affaires",
        "chiffre affaires",
    ]),
    ("companies", "annee_ca", [
        "annee du ca brut", "annee du ca", "annee ca",
    ]),
    ("companies", "tranche_ca", [
        "tranche du ca brut", "tranche du ca",
        "tranche ca",
    ]),
    ("companies", "date_fondation", [
        "date de fondation", "date fondation",
        "annee de fondation",
        "date creation", "founded",
        "annee de creation", "annee creation",
    ]),
    ("companies", "type_etablissement", [
        "type d etablissement", "type etablissement",
        "establishment type",
    ]),

    # ── Officer (person) fields — MUST come before contacts ──
    # So "email direct dirigeant" matches officers.email_direct, not contacts.email
    ("officers", "email_direct", [
        "email direct dirigeant", "email direct",
        "direct email", "email personnel",
        "email responsable",
    ]),
    ("officers", "ligne_directe", [
        "ligne directe", "direct line", "direct phone",
        "telephone direct", "tel direct",
    ]),
    ("officers", "code_fonction", [
        "code fonction", "function code",
    ]),
    ("officers", "type_fonction", [
        "type de fonction", "function type",
        "type fonction",
    ]),
    ("officers", "civilite", [
        "civilite", "title",
    ]),
    ("officers", "prenom", [
        "prenom", "deuxieme prenom", "first name",
        "given name",
    ]),
    ("officers", "nom", [
        "nom", "nom de famille", "last name", "surname", "family name",
    ]),
    ("officers", "role", [
        "fonction", "function", "job title", "poste",
        "libelle personnalise",
    ]),

    # ── Contact fields — AFTER officers ──
    ("contacts", "phone", [
        "numero de telephone", "telephone", "tel", "phone",
        "phone number", "tel fixe", "telephone fixe",
    ]),
    ("contacts", "email", [
        "email entreprise", "certified email",
        "adresse email", "adresse e mail",
        "e mail", "courriel", "email", "mail",
    ]),
    ("contacts", "website", [
        "site web", "website", "url", "site internet",
        "web", "adresse web",
    ]),
    ("contacts", "social_linkedin", [
        "reseaux sociaux", "social linkedin", "lien linkedin",
        "linkedin",
    ]),
    ("contacts", "social_facebook", [
        "facebook", "social facebook", "lien facebook",
    ]),
    ("contacts", "social_twitter", [
        "twitter", "x com", "social twitter",
    ]),
]

# Columns to skip entirely (no value for the database)
# These are EXACT normalized matches — no substring matching.
SKIP_PATTERNS: set[str] = {
    "genre", "source de la donnee contact", "identifiant kompass",
    "company url",  # Kompass profile URL, not the company's website
    "phone additional info", "tps", "telephone preference service",
    "non mailing indicator", "fps", "fax preference service",
    "emps", "email preference service",
    "pays", "code pays",  # We know it's France
    "complement d adresse", "complement d adresse postale",
    "rue postale", "boite postale", "adresse postale",
    "registration address", "pays d enregistrement",
    "rue d enregistrement", "complement d enregistrement",
    "boite d enregistrement",
    "adresse legale", "pays legal", "rue legale",
    "complement legal", "ville legale", "code postal legal",
    "recherche par texte",  # Kompass internal search text
    "fax",  # Obsolete, no fax field in schema
    "pays d export", "pays d import",  # Low-value for B2B leads
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
    used_targets: set[tuple[str, str]] = set()

    # Fields that can be mapped from multiple source columns
    # (take the first non-empty value during ingestion)
    allow_duplicates = {
        ("companies", "enseigne"),
        ("companies", "tranche_effectif"),
        ("companies", "effectif_exact"),
        ("companies", "date_fondation"),
    }

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

        # Two-pass matching: exact first, then longest-alias substring
        matched = False

        # Pass 1: exact match (normalized header == alias exactly)
        for target_table, target_field, aliases in ALIAS_REGISTRY:
            if (target_table, target_field) in used_targets:
                if target_table != "officers" and (target_table, target_field) not in allow_duplicates:
                    continue
            if norm in aliases:
                result.columns.append(MappedColumn(
                    source_name=header,
                    target_table=target_table,
                    target_field=target_field,
                    confidence=1.0,
                ))
                used_targets.add((target_table, target_field))
                if target_field == "siren":
                    result.siren_column = col_idx
                if target_table == "officers" and target_field in ("prenom", "nom"):
                    result.has_officer_data = True
                matched = True
                break

        # Pass 2: longest-alias substring match
        # ("email direct dirigeant" contains "email direct" which is longer
        #  than "email", so officers.email_direct wins over contacts.email)
        if not matched:
            best_match = None
            best_alias_len = 0
            for target_table, target_field, aliases in ALIAS_REGISTRY:
                if (target_table, target_field) in used_targets:
                    if target_table != "officers" and (target_table, target_field) not in allow_duplicates:
                        continue
                for alias in aliases:
                    if alias in norm and len(alias) > best_alias_len:
                        best_match = (target_table, target_field)
                        best_alias_len = len(alias)

            if best_match:
                target_table, target_field = best_match
                result.columns.append(MappedColumn(
                    source_name=header,
                    target_table=target_table,
                    target_field=target_field,
                    confidence=0.8,
                ))
                used_targets.add((target_table, target_field))
                if target_field == "siren":
                    result.siren_column = col_idx
                if target_table == "officers" and target_field in ("prenom", "nom"):
                    result.has_officer_data = True
                matched = True

        if not matched:
            # Overflow → extra_data
            result.columns.append(MappedColumn(
                source_name=header,
                target_table="extra_data",
                target_field=header.strip(),
            ))

    return result
