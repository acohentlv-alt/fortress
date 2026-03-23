"""Seen Set — intra-query deduplication.

Tracks every company encountered during a single query run.
Filters duplicates BEFORE scraping, not after.

Why this matters:
  Module B generates ~150 micro-queries (one per postal code).
  A large farm near a ZIP code boundary appears in multiple results.
  Without the Seen Set, we would scrape the same company 3-5 times per query.

Identification strategy (three parallel sets):
  1. by_siren     — exact match, fastest. SIREN is the gold standard.
  2. by_phone     — catches unregistered businesses that share a phone.
  3. by_name_zip  — (normalised_name, postal_code) pair as fallback.

Persistence:
  Serialised to JSON after every wave → restored on resume.
  Stored at: data/checkpoints/{job_id}/seen_set.json
"""

from __future__ import annotations

import json
import re
import unicodedata
from pathlib import Path
from typing import Any


class SeenSet:
    """In-memory dedup filter for all companies encountered in a query run."""

    def __init__(self) -> None:
        self.by_siren: set[str] = set()
        self.by_phone: set[str] = set()
        self.by_name_zip: set[tuple[str, str]] = set()

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def is_seen(self, listing: dict[str, Any]) -> bool:
        """Return True if this company has already been processed.

        Checks all three identifier sets. Any match returns True immediately.
        """
        siren = listing.get("siren")
        if siren and siren in self.by_siren:
            return True

        phone = listing.get("phone")
        if phone and phone in self.by_phone:
            return True

        key = _name_zip_key(listing)
        if key != ("", "") and key in self.by_name_zip:
            return True

        return False

    def mark_seen(self, listing: dict[str, Any]) -> None:
        """Record this company so future duplicates are filtered out."""
        siren = listing.get("siren")
        if siren:
            self.by_siren.add(siren)

        phone = listing.get("phone")
        if phone:
            self.by_phone.add(phone)

        key = _name_zip_key(listing)
        if key != ("", ""):  # Skip empty keys — prevent false positives
            self.by_name_zip.add(key)

    def save(self, path: Path) -> None:
        """Serialise to JSON for checkpoint persistence."""
        path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "by_siren": sorted(self.by_siren),
            "by_phone": sorted(self.by_phone),
            "by_name_zip": [list(pair) for pair in sorted(self.by_name_zip)],
        }
        path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")

    @classmethod
    def load(cls, path: Path) -> "SeenSet":
        """Restore from a JSON checkpoint file.

        Returns an empty SeenSet if the file does not exist.
        """
        instance = cls()
        if not path.exists():
            return instance

        data = json.loads(path.read_text(encoding="utf-8"))
        instance.by_siren = set(data.get("by_siren", []))
        instance.by_phone = set(data.get("by_phone", []))
        instance.by_name_zip = {
            (pair[0], pair[1]) for pair in data.get("by_name_zip", [])
        }
        return instance

    # ------------------------------------------------------------------
    # Stats
    # ------------------------------------------------------------------

    @property
    def size(self) -> int:
        """Approximate number of unique companies tracked (by SIREN count)."""
        return len(self.by_siren)

    def __repr__(self) -> str:
        return (
            f"SeenSet("
            f"sirens={len(self.by_siren)}, "
            f"phones={len(self.by_phone)}, "
            f"name_zip={len(self.by_name_zip)})"
        )


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalise_name(name: str) -> str:
    """Lowercase, strip accents, remove punctuation — for fuzzy name matching."""
    nfkd = unicodedata.normalize("NFKD", name)
    ascii_name = "".join(c for c in nfkd if not unicodedata.combining(c))
    return re.sub(r"[^a-z0-9]", "", ascii_name.lower())


def _name_zip_key(listing: dict[str, Any]) -> tuple[str, str]:
    """Build the (normalised_name, postal_code) dedup key."""
    name = listing.get("denomination") or listing.get("name") or ""
    postal = listing.get("code_postal") or listing.get("postal_code") or ""
    return (_normalise_name(name), str(postal).strip())
