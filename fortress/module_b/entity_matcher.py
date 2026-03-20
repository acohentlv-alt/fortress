"""Entity matcher — links MAPS-discovered companies to real SIREN entries.

Three matching strategies, ranked by confidence:
  HIGH   — Same SIRET found on website (already extracted by crawler)
  HIGH   — Same normalized address + same département
  MEDIUM — Fuzzy denomination match + same département
"""
from __future__ import annotations

import re
import unicodedata
from dataclasses import dataclass


# ---------------------------------------------------------------------------
# Address normalisation
# ---------------------------------------------------------------------------

# Words to strip from addresses for comparison
_ADDR_NOISE = re.compile(
    r"\b(france|cedex|bp\s*\d+)\b", re.IGNORECASE
)
# Collapse whitespace
_WHITESPACE = re.compile(r"\s+")


def normalize_address(addr: str | None) -> str:
    """Normalize a French address to a canonical lowercase form.

    Strips accents, removes 'France', 'Cedex', 'BP XXXX', lowercases,
    and collapses whitespace.  The result is suitable for matching.

    Example:
        "16 Rue des Pins, 66470 Sainte-Marie-la-Mer, France"
        → "16 rue des pins 66470 sainte-marie-la-mer"
    """
    if not addr:
        return ""
    # Strip accents (é→e, ô→o, etc.)
    nfkd = unicodedata.normalize("NFKD", addr)
    text = "".join(c for c in nfkd if not unicodedata.combining(c))
    # Lowercase
    text = text.lower()
    # Remove noise words
    text = _ADDR_NOISE.sub("", text)
    # Remove punctuation (commas, dots, parentheses) but keep hyphens
    text = re.sub(r"[,.\(\)']", " ", text)
    # Collapse whitespace and strip
    text = _WHITESPACE.sub(" ", text).strip()
    return text


def _extract_street_key(normalized: str) -> str:
    """Extract the street number + street name from a normalized address.

    Strips postal code and city to focus on the physical location.
    Example: "16 rue des pins 66470 sainte-marie-la-mer" → "16 rue des pins"
    """
    # Find the first 5-digit sequence (postal code) and take everything before it
    m = re.search(r"\b\d{5}\b", normalized)
    if m:
        return normalized[: m.start()].strip()
    return normalized


# ---------------------------------------------------------------------------
# Denomination normalisation (for fuzzy matching)
# ---------------------------------------------------------------------------

# Common legal form prefixes/suffixes to strip
_LEGAL_FORMS = re.compile(
    r"\b(sas|sarl|sa|sasu|eurl|sci|scp|snc|eirl|auto[- ]?entrepreneur?|ei)\b",
    re.IGNORECASE,
)
# Single-letter initials (e.g., "S M" in "SAS L M MEDINA")
_INITIALS = re.compile(r"\b[A-Z]\b")


def normalize_denomination(name: str | None) -> str:
    """Normalize a company name for comparison.

    Strips accents, legal forms (SAS, SARL...), single-letter initials,
    and lowercases.
    """
    if not name:
        return ""
    nfkd = unicodedata.normalize("NFKD", name)
    text = "".join(c for c in nfkd if not unicodedata.combining(c))
    text = text.upper()
    text = _LEGAL_FORMS.sub("", text)
    text = _INITIALS.sub("", text)
    text = re.sub(r"[^\w\s]", " ", text)
    text = _WHITESPACE.sub(" ", text).strip().lower()
    return text


def _token_overlap(a: str, b: str) -> float:
    """Return Jaccard-like token overlap between two normalized strings."""
    tokens_a = set(a.split())
    tokens_b = set(b.split())
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)


# ---------------------------------------------------------------------------
# Match result
# ---------------------------------------------------------------------------

@dataclass
class SirenMatch:
    siren: str
    denomination: str
    confidence: str  # 'high' or 'medium'
    method: str  # 'address', 'siret_website', 'fuzzy_name'
    address: str | None = None
    ville: str | None = None


# ---------------------------------------------------------------------------
# Main matching function
# ---------------------------------------------------------------------------

async def find_matches(
    maps_siren: str,
    maps_addr: str | None,
    maps_name: str | None,
    departement: str | None,
    conn,
) -> list[SirenMatch]:
    """Find real SIREN entities matching a MAPS-discovered company.

    Runs three strategies in order:
      1. Address match (HIGH confidence)
      2. SIRET from website — checked via linked_siren if already set
      3. Fuzzy name match (MEDIUM confidence)

    Returns a list of matches sorted by confidence (high first).
    """
    matches: list[SirenMatch] = []
    seen_sirens: set[str] = set()

    if not departement:
        return matches

    # ── Strategy A: Address match ────────────────────────────────
    if maps_addr:
        norm_addr = normalize_address(maps_addr)
        street_key = _extract_street_key(norm_addr)

        if street_key and len(street_key) > 5:  # Need meaningful content
            # Extract street number for more precise matching
            num_match = re.match(r"(\d+)", street_key)
            street_num = num_match.group(1) if num_match else None

            # Query: same département, address contains the street key tokens
            # We search using the first significant words of the street
            search_pattern = f"%{street_key}%"
            rows = await (await conn.execute(
                """SELECT siren, denomination, adresse, ville
                   FROM companies
                   WHERE departement = %s
                     AND siren NOT LIKE 'MAPS%%'
                     AND LOWER(adresse) LIKE %s
                   LIMIT 10""",
                (departement, search_pattern),
            )).fetchall()

            for row in (rows or []):
                r_siren = row[0] if isinstance(row, tuple) else row["siren"]
                r_denom = row[1] if isinstance(row, tuple) else row["denomination"]
                r_addr = row[2] if isinstance(row, tuple) else row["adresse"]
                r_ville = row[3] if isinstance(row, tuple) else row["ville"]

                if r_siren in seen_sirens:
                    continue

                # Extra validation: if we have a street number, make sure it matches
                if street_num:
                    r_norm = normalize_address(r_addr)
                    r_num_match = re.match(r"(\d+)", r_norm)
                    if r_num_match and r_num_match.group(1) != street_num:
                        continue  # Different building number

                seen_sirens.add(r_siren)
                matches.append(SirenMatch(
                    siren=r_siren,
                    denomination=r_denom,
                    confidence="high",
                    method="address",
                    address=r_addr,
                    ville=r_ville,
                ))

    # ── Strategy C: Fuzzy name match ─────────────────────────────
    if maps_name and departement:
        norm_name = normalize_denomination(maps_name)
        # Extract the most significant tokens (skip very short ones)
        significant_tokens = [t for t in norm_name.split() if len(t) >= 4]

        if significant_tokens:
            # Search by the longest/most unique token
            best_token = max(significant_tokens, key=len)

            rows = await (await conn.execute(
                """SELECT siren, denomination, adresse, ville
                   FROM companies
                   WHERE departement = %s
                     AND siren NOT LIKE 'MAPS%%'
                     AND LOWER(denomination) LIKE %s
                   LIMIT 10""",
                (departement, f"%{best_token}%"),
            )).fetchall()

            for row in (rows or []):
                r_siren = row[0] if isinstance(row, tuple) else row["siren"]
                r_denom = row[1] if isinstance(row, tuple) else row["denomination"]
                r_addr = row[2] if isinstance(row, tuple) else row["adresse"]
                r_ville = row[3] if isinstance(row, tuple) else row["ville"]

                if r_siren in seen_sirens:
                    continue

                # Check token overlap
                r_norm = normalize_denomination(r_denom)
                overlap = _token_overlap(norm_name, r_norm)

                if overlap >= 0.3:  # At least 30% token overlap
                    seen_sirens.add(r_siren)
                    matches.append(SirenMatch(
                        siren=r_siren,
                        denomination=r_denom,
                        confidence="medium",
                        method="fuzzy_name",
                        address=r_addr,
                        ville=r_ville,
                    ))

    # Sort: high confidence first, then medium
    matches.sort(key=lambda m: 0 if m.confidence == "high" else 1)
    return matches
