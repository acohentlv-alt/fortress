"""Tests for export.py — to_csv_bytes, to_jsonl_bytes, to_txt_bytes.

All functions are pure (no I/O) — no fixtures or tmp_path needed.
"""
from __future__ import annotations

import csv
import io
import json

import pytest

from fortress.export.csv import to_csv_bytes, to_jsonl_bytes, to_txt_bytes

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_card(
    siren: str = "123456789",
    denomination: str = "SARL Dupont",
    phone: str | None = "0561123456",
    email: str | None = "contact@dupont.fr",
    website_url: str | None = "https://dupont.fr",
    rating: float | None = 4.2,
    completeness_pct: int = 80,
) -> dict:
    return {
        "card_index": 1,
        "fortress_id": "F-00001",
        "query_name": "LOGISTIQUE 31",
        "siren": siren,
        "denomination": denomination,
        "phone": phone,
        "email": email,
        "website_url": website_url,
        "social_linkedin": None,
        "rating": rating,
        "completeness_pct": completeness_pct,
    }


# ---------------------------------------------------------------------------
# to_csv_bytes
# ---------------------------------------------------------------------------


def test_to_csv_bytes_empty_returns_empty_bytes():
    assert to_csv_bytes([]) == b""


def test_to_csv_bytes_returns_bytes():
    result = to_csv_bytes([_make_card()])
    assert isinstance(result, bytes)


def test_to_csv_bytes_is_valid_csv():
    cards = [_make_card(siren="111111111"), _make_card(siren="222222222")]
    data = to_csv_bytes(cards).decode("utf-8")
    reader = csv.DictReader(io.StringIO(data))
    rows = list(reader)
    assert len(rows) == 2


def test_to_csv_bytes_headers_match_card_keys():
    card = _make_card()
    data = to_csv_bytes([card]).decode("utf-8")
    reader = csv.DictReader(io.StringIO(data))
    assert reader.fieldnames == list(card.keys())


def test_to_csv_bytes_field_values_correct():
    card = _make_card(siren="987654321", phone="0612345678")
    data = to_csv_bytes([card]).decode("utf-8")
    reader = csv.DictReader(io.StringIO(data))
    rows = list(reader)
    assert rows[0]["siren"] == "987654321"
    assert rows[0]["phone"] == "0612345678"


def test_to_csv_bytes_none_fields_handled():
    card = _make_card(phone=None, email=None)
    data = to_csv_bytes([card]).decode("utf-8")
    reader = csv.DictReader(io.StringIO(data))
    rows = list(reader)
    assert rows[0]["phone"] == ""  # CSV converts None to empty string


def test_to_csv_bytes_utf8_encoding():
    card = _make_card(denomination="Société Générale — Boulangerie")
    data = to_csv_bytes([card])
    assert "Société Générale" in data.decode("utf-8")


# ---------------------------------------------------------------------------
# to_jsonl_bytes
# ---------------------------------------------------------------------------


def test_to_jsonl_bytes_empty_returns_empty_bytes():
    assert to_jsonl_bytes([]) == b""


def test_to_jsonl_bytes_returns_bytes():
    result = to_jsonl_bytes([_make_card()])
    assert isinstance(result, bytes)


def test_to_jsonl_bytes_single_card_one_line():
    data = to_jsonl_bytes([_make_card()]).decode("utf-8")
    lines = [l for l in data.split("\n") if l.strip()]
    assert len(lines) == 1


def test_to_jsonl_bytes_multiple_cards_multiple_lines():
    cards = [_make_card(siren="111"), _make_card(siren="222"), _make_card(siren="333")]
    data = to_jsonl_bytes(cards).decode("utf-8")
    lines = [l for l in data.split("\n") if l.strip()]
    assert len(lines) == 3


def test_to_jsonl_bytes_each_line_is_valid_json():
    cards = [_make_card(siren="111"), _make_card(siren="222")]
    data = to_jsonl_bytes(cards).decode("utf-8")
    lines = [l for l in data.split("\n") if l.strip()]
    for line in lines:
        obj = json.loads(line)
        assert isinstance(obj, dict)


def test_to_jsonl_bytes_values_preserved():
    card = _make_card(siren="777777777", phone="0561999888")
    data = to_jsonl_bytes([card]).decode("utf-8")
    obj = json.loads(data.strip())
    assert obj["siren"] == "777777777"
    assert obj["phone"] == "0561999888"


def test_to_jsonl_bytes_preserves_french_chars():
    card = _make_card(denomination="Boulangerie Héritier & Fils")
    data = to_jsonl_bytes([card]).decode("utf-8")
    assert "Héritier" in data  # ensure_ascii=False keeps accents native


def test_to_jsonl_bytes_none_becomes_json_null():
    card = _make_card(phone=None)
    data = to_jsonl_bytes([card]).decode("utf-8")
    obj = json.loads(data.strip())
    assert obj["phone"] is None


# ---------------------------------------------------------------------------
# to_txt_bytes
# ---------------------------------------------------------------------------


def test_to_txt_bytes_empty_returns_empty_bytes():
    assert to_txt_bytes([]) == b""


def test_to_txt_bytes_returns_bytes():
    result = to_txt_bytes([_make_card()])
    assert isinstance(result, bytes)


def test_to_txt_bytes_contains_card_marker():
    data = to_txt_bytes([_make_card()]).decode("utf-8")
    assert "CARD" in data


def test_to_txt_bytes_contains_denomination():
    card = _make_card(denomination="TRANSPORT BIDULE SAS")
    data = to_txt_bytes([card]).decode("utf-8")
    assert "TRANSPORT BIDULE SAS" in data


def test_to_txt_bytes_multiple_cards_separated():
    cards = [_make_card(siren="111"), _make_card(siren="222")]
    data = to_txt_bytes(cards).decode("utf-8")
    # Both SIRENs should appear
    assert "111" in data
    assert "222" in data
    # Cards separated by blank line
    assert "\n\n" in data


def test_to_txt_bytes_utf8_encoding():
    card = _make_card(denomination="Boulangerie Héritier")
    data = to_txt_bytes([card])
    assert "Héritier" in data.decode("utf-8")
