"""Unit tests for SeenSet — pure in-memory, no DB or HTTP mocking needed.

SeenSet tracks companies via three parallel identifier sets:
  1. by_siren     — exact SIREN match
  2. by_phone     — exact phone match
  3. by_name_zip  — (normalised_name, postal_code) pair

Persistence methods (actual names from seen_set.py):
  - seen.save(path: Path)        — serialise to JSON
  - SeenSet.load(path: Path)     — class method, deserialise from JSON

Name key fields (from _name_zip_key in seen_set.py):
  - "denomination" or "name"        — company name
  - "code_postal" or "postal_code"  — postal code
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from fortress.processing.seen_set import SeenSet


# ---------------------------------------------------------------------------
# is_seen / mark_seen — by_siren
# ---------------------------------------------------------------------------


def test_is_seen_by_siren():
    """Known SIREN returns True."""
    seen = SeenSet()
    seen.mark_seen({"siren": "123456789"})
    assert seen.is_seen({"siren": "123456789"})


def test_is_not_seen_for_unknown_siren():
    """Unknown SIREN returns False."""
    seen = SeenSet()
    assert not seen.is_seen({"siren": "999999999"})


def test_is_seen_siren_exact_match_only():
    """Different SIREN is not seen even if close."""
    seen = SeenSet()
    seen.mark_seen({"siren": "123456789"})
    assert not seen.is_seen({"siren": "123456788"})


# ---------------------------------------------------------------------------
# is_seen / mark_seen — by_phone
# ---------------------------------------------------------------------------


def test_is_seen_by_phone():
    """Same phone number with different SIREN still returns True."""
    seen = SeenSet()
    seen.mark_seen({"siren": "111111111", "phone": "0612345678"})
    # Different SIREN, same phone
    assert seen.is_seen({"siren": "999999999", "phone": "0612345678"})


def test_is_not_seen_different_phone():
    """Different phone is not seen."""
    seen = SeenSet()
    seen.mark_seen({"siren": "111111111", "phone": "0612345678"})
    assert not seen.is_seen({"phone": "0699999999"})


def test_is_seen_phone_without_siren():
    """Entry with no SIREN, only phone — matched by phone."""
    seen = SeenSet()
    seen.mark_seen({"phone": "0600000001"})
    assert seen.is_seen({"phone": "0600000001"})


# ---------------------------------------------------------------------------
# is_seen / mark_seen — by_name_zip
# ---------------------------------------------------------------------------


def test_is_seen_by_name_zip_using_name_and_postal_code():
    """(name, postal_code) fields used for name+ZIP key."""
    seen = SeenSet()
    seen.mark_seen({"name": "Dupont SARL", "postal_code": "66000"})
    assert seen.is_seen({"name": "Dupont SARL", "postal_code": "66000"})


def test_is_seen_by_name_zip_using_denomination_and_code_postal():
    """(denomination, code_postal) alternate field names also work."""
    seen = SeenSet()
    seen.mark_seen({"denomination": "Dupont SARL", "code_postal": "66000"})
    assert seen.is_seen({"denomination": "Dupont SARL", "code_postal": "66000"})


def test_is_seen_by_name_zip_cross_field_names():
    """denomination matches against name — normalised key is the same."""
    seen = SeenSet()
    seen.mark_seen({"denomination": "Dupont SARL", "code_postal": "66000"})
    # Same company, different field names for the lookup
    assert seen.is_seen({"name": "Dupont SARL", "postal_code": "66000"})


def test_is_seen_by_name_zip_different_siren_same_name():
    """Same name+ZIP with different SIREN returns True (duplicate)."""
    seen = SeenSet()
    seen.mark_seen(
        {"siren": "111111111", "name": "Dupont SARL", "postal_code": "66000"}
    )
    assert seen.is_seen({"name": "Dupont SARL", "postal_code": "66000"})


def test_is_not_seen_different_postal_code():
    """Same name but different postal code is not seen."""
    seen = SeenSet()
    seen.mark_seen({"name": "Dupont SARL", "postal_code": "66000"})
    assert not seen.is_seen({"name": "Dupont SARL", "postal_code": "66100"})


# ---------------------------------------------------------------------------
# Normalisation — accent stripping, case folding, punctuation removal
# ---------------------------------------------------------------------------


def test_normalise_name_accents():
    """Élodie normalises to elodie — accents stripped before matching."""
    seen = SeenSet()
    seen.mark_seen({"name": "Élodie Dupont SARL", "postal_code": "66000"})
    assert seen.is_seen({"name": "elodie dupont sarl", "postal_code": "66000"})


def test_normalise_name_uppercase():
    """UPPERCASE name matches lowercase."""
    seen = SeenSet()
    seen.mark_seen({"name": "DUPONT SARL", "postal_code": "75001"})
    assert seen.is_seen({"name": "dupont sarl", "postal_code": "75001"})


def test_normalise_name_punctuation():
    """Punctuation removed: 'Dupont & Fils' matches 'dupont fils'."""
    seen = SeenSet()
    seen.mark_seen({"name": "Dupont & Fils", "postal_code": "13000"})
    assert seen.is_seen({"name": "dupont fils", "postal_code": "13000"})


def test_normalise_name_cedilla():
    """Ç is normalised to c."""
    seen = SeenSet()
    seen.mark_seen({"name": "Garçon Boulangerie", "postal_code": "75000"})
    assert seen.is_seen({"name": "garcon boulangerie", "postal_code": "75000"})


# ---------------------------------------------------------------------------
# Round-trip: mark then check
# ---------------------------------------------------------------------------


def test_mark_seen_then_check_siren():
    """Round-trip via SIREN: mark then check returns True."""
    seen = SeenSet()
    entry = {"siren": "123456789", "phone": "0600000001", "name": "Test SAS", "postal_code": "75001"}
    assert not seen.is_seen(entry)
    seen.mark_seen(entry)
    assert seen.is_seen(entry)


def test_mark_seen_populates_all_three_sets():
    """mark_seen adds to by_siren, by_phone, and by_name_zip."""
    seen = SeenSet()
    seen.mark_seen({"siren": "123456789", "phone": "0600000001", "name": "Test", "postal_code": "66000"})
    assert "123456789" in seen.by_siren
    assert "0600000001" in seen.by_phone
    assert len(seen.by_name_zip) == 1


def test_mark_seen_partial_entry_no_siren():
    """mark_seen with no SIREN still records phone and name+zip."""
    seen = SeenSet()
    seen.mark_seen({"phone": "0600000001", "name": "Test SAS", "postal_code": "75001"})
    assert len(seen.by_siren) == 0
    assert "0600000001" in seen.by_phone


def test_mark_seen_partial_entry_no_phone():
    """mark_seen with no phone still records SIREN and name+zip."""
    seen = SeenSet()
    seen.mark_seen({"siren": "123456789", "name": "Test SAS", "postal_code": "75001"})
    assert "123456789" in seen.by_siren
    assert len(seen.by_phone) == 0


# ---------------------------------------------------------------------------
# size property
# ---------------------------------------------------------------------------


def test_size_reflects_siren_count():
    """size property returns number of SIRENs tracked."""
    seen = SeenSet()
    assert seen.size == 0
    seen.mark_seen({"siren": "111111111"})
    assert seen.size == 1
    seen.mark_seen({"siren": "222222222"})
    assert seen.size == 2


def test_size_does_not_count_phone_only_entries():
    """Entries without SIREN do not count toward size."""
    seen = SeenSet()
    seen.mark_seen({"phone": "0600000001"})
    assert seen.size == 0


# ---------------------------------------------------------------------------
# repr
# ---------------------------------------------------------------------------


def test_repr_contains_counts():
    """__repr__ includes siren, phone, and name_zip counts."""
    seen = SeenSet()
    seen.mark_seen({"siren": "123456789", "phone": "0600000001", "name": "Test", "postal_code": "66000"})
    r = repr(seen)
    assert "sirens=1" in r
    assert "phones=1" in r
    assert "name_zip=1" in r


# ---------------------------------------------------------------------------
# save() / load() — persistence round-trip
# ---------------------------------------------------------------------------


def test_save_creates_json_file(tmp_path):
    """save() creates a JSON file at the given path."""
    seen = SeenSet()
    seen.mark_seen({"siren": "123456789"})

    path = tmp_path / "seen_set.json"
    seen.save(path)

    assert path.exists()
    data = json.loads(path.read_text(encoding="utf-8"))
    assert "by_siren" in data
    assert "by_phone" in data
    assert "by_name_zip" in data


def test_save_and_load_siren_roundtrip(tmp_path):
    """SIREN survives save → load round-trip."""
    seen = SeenSet()
    seen.mark_seen({"siren": "123456789"})

    path = tmp_path / "seen_set.json"
    seen.save(path)

    seen2 = SeenSet.load(path)
    assert seen2.is_seen({"siren": "123456789"})


def test_save_and_load_phone_roundtrip(tmp_path):
    """Phone survives save → load round-trip."""
    seen = SeenSet()
    seen.mark_seen({"phone": "0612345678"})

    path = tmp_path / "seen_set.json"
    seen.save(path)

    seen2 = SeenSet.load(path)
    assert seen2.is_seen({"phone": "0612345678"})


def test_save_and_load_name_zip_roundtrip(tmp_path):
    """name+ZIP survives save → load round-trip."""
    seen = SeenSet()
    seen.mark_seen({"name": "Dupont SARL", "postal_code": "66000"})

    path = tmp_path / "seen_set.json"
    seen.save(path)

    seen2 = SeenSet.load(path)
    assert seen2.is_seen({"name": "Dupont SARL", "postal_code": "66000"})


def test_save_and_load_full_entry(tmp_path):
    """Full entry (SIREN + phone + name+ZIP) survives save → load."""
    seen = SeenSet()
    seen.mark_seen({"siren": "123456789", "phone": "0600000001", "name": "Test SAS", "postal_code": "75001"})

    path = tmp_path / "seen_set.json"
    seen.save(path)

    seen2 = SeenSet.load(path)
    assert seen2.is_seen({"siren": "123456789"})
    assert seen2.is_seen({"phone": "0600000001"})
    assert seen2.is_seen({"name": "Test SAS", "postal_code": "75001"})


def test_load_returns_empty_seen_set_when_file_missing(tmp_path):
    """load() with a non-existent file returns an empty SeenSet."""
    path = tmp_path / "nonexistent_seen_set.json"
    seen = SeenSet.load(path)

    assert seen.size == 0
    assert len(seen.by_phone) == 0
    assert len(seen.by_name_zip) == 0


def test_save_creates_parent_directories(tmp_path):
    """save() creates parent directories if they don't exist."""
    path = tmp_path / "deep" / "nested" / "seen_set.json"
    seen = SeenSet()
    seen.mark_seen({"siren": "123456789"})
    seen.save(path)

    assert path.exists()


def test_save_multiple_entries_preserved(tmp_path):
    """Multiple entries all survive save → load."""
    seen = SeenSet()
    seen.mark_seen({"siren": "111111111"})
    seen.mark_seen({"siren": "222222222"})
    seen.mark_seen({"phone": "0600000001"})
    seen.mark_seen({"phone": "0700000002"})

    path = tmp_path / "seen_set.json"
    seen.save(path)

    seen2 = SeenSet.load(path)
    assert seen2.is_seen({"siren": "111111111"})
    assert seen2.is_seen({"siren": "222222222"})
    assert seen2.is_seen({"phone": "0600000001"})
    assert seen2.is_seen({"phone": "0700000002"})
