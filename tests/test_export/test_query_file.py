"""Tests for query_file — append_wave, load_query_cards, export_query_csv, export_query_txt.

Uses tmp_path to avoid touching the real data directory.
"""
from __future__ import annotations

import csv
import io
import json
from pathlib import Path

import pytest

from fortress.export.queries import (
    append_wave,
    export_query_csv,
    export_query_txt,
    load_query_cards,
)

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

_QUERY_ID = "LOGISTIQUE_31_BATCH_001"


def _make_card(
    siren: str = "123456789",
    denomination: str = "SARL Dupont",
    phone: str | None = "0561123456",
    card_index: int = 1,
) -> dict:
    return {
        "card_index": card_index,
        "siren": siren,
        "denomination": denomination,
        "phone": phone,
        "email": "contact@dupont.fr",
        "website_url": "https://dupont.fr",
        "completeness_pct": 80,
    }


# ---------------------------------------------------------------------------
# append_wave
# ---------------------------------------------------------------------------


def test_append_wave_creates_file(tmp_path: Path):
    append_wave(_QUERY_ID, [_make_card()], base_dir=tmp_path)
    assert (tmp_path / f"{_QUERY_ID}.jsonl").exists()


def test_append_wave_writes_one_line_per_card(tmp_path: Path):
    cards = [_make_card("111", card_index=1), _make_card("222", card_index=2)]
    append_wave(_QUERY_ID, cards, base_dir=tmp_path)
    lines = (tmp_path / f"{_QUERY_ID}.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2


def test_append_wave_second_wave_appends(tmp_path: Path):
    append_wave(_QUERY_ID, [_make_card("111")], base_dir=tmp_path)
    append_wave(_QUERY_ID, [_make_card("222")], base_dir=tmp_path)
    cards = load_query_cards(_QUERY_ID, base_dir=tmp_path)
    assert len(cards) == 2


def test_append_wave_each_line_is_valid_json(tmp_path: Path):
    cards = [_make_card("111"), _make_card("222")]
    append_wave(_QUERY_ID, cards, base_dir=tmp_path)
    for line in (tmp_path / f"{_QUERY_ID}.jsonl").read_text().splitlines():
        obj = json.loads(line)
        assert isinstance(obj, dict)


def test_append_wave_empty_cards_is_noop(tmp_path: Path):
    append_wave(_QUERY_ID, [], base_dir=tmp_path)
    path = tmp_path / f"{_QUERY_ID}.jsonl"
    if path.exists():
        assert path.read_text() == ""


def test_append_wave_creates_parent_dirs(tmp_path: Path):
    nested = tmp_path / "deep" / "nested"
    append_wave(_QUERY_ID, [_make_card()], base_dir=nested)
    assert (nested / f"{_QUERY_ID}.jsonl").exists()


# ---------------------------------------------------------------------------
# load_query_cards
# ---------------------------------------------------------------------------


def test_load_query_cards_nonexistent_returns_empty(tmp_path: Path):
    result = load_query_cards("NONEXISTENT_QUERY", base_dir=tmp_path)
    assert result == []


def test_load_query_cards_reads_back_cards(tmp_path: Path):
    cards = [_make_card("111", card_index=1), _make_card("222", card_index=2)]
    append_wave(_QUERY_ID, cards, base_dir=tmp_path)
    loaded = load_query_cards(_QUERY_ID, base_dir=tmp_path)
    assert len(loaded) == 2
    assert loaded[0]["siren"] == "111"
    assert loaded[1]["siren"] == "222"


def test_load_query_cards_skips_malformed_lines(tmp_path: Path):
    path = tmp_path / f"{_QUERY_ID}.jsonl"
    path.write_text(
        json.dumps({"siren": "111"}) + "\n"
        + "MALFORMED\n"
        + json.dumps({"siren": "222"}) + "\n",
        encoding="utf-8",
    )
    loaded = load_query_cards(_QUERY_ID, base_dir=tmp_path)
    assert len(loaded) == 2


def test_load_query_cards_returns_dicts(tmp_path: Path):
    append_wave(_QUERY_ID, [_make_card()], base_dir=tmp_path)
    results = load_query_cards(_QUERY_ID, base_dir=tmp_path)
    assert all(isinstance(r, dict) for r in results)


def test_load_query_cards_multiple_waves_ordered(tmp_path: Path):
    # Simulate two waves — cards should come out in append order
    append_wave(_QUERY_ID, [_make_card("WAVE1_A"), _make_card("WAVE1_B")], base_dir=tmp_path)
    append_wave(_QUERY_ID, [_make_card("WAVE2_A")], base_dir=tmp_path)
    cards = load_query_cards(_QUERY_ID, base_dir=tmp_path)
    assert len(cards) == 3
    assert cards[0]["siren"] == "WAVE1_A"
    assert cards[2]["siren"] == "WAVE2_A"


# ---------------------------------------------------------------------------
# export_query_csv
# ---------------------------------------------------------------------------


def test_export_query_csv_creates_csv_file(tmp_path: Path):
    append_wave(_QUERY_ID, [_make_card()], base_dir=tmp_path)
    out = export_query_csv(_QUERY_ID, base_dir=tmp_path)
    assert out.exists()
    assert out.suffix == ".csv"


def test_export_query_csv_empty_query_creates_empty_file(tmp_path: Path):
    out = export_query_csv("EMPTY_QUERY", base_dir=tmp_path)
    assert out.exists()
    assert out.read_text(encoding="utf-8") == ""


def test_export_query_csv_valid_csv_structure(tmp_path: Path):
    cards = [_make_card("111", card_index=1), _make_card("222", card_index=2)]
    append_wave(_QUERY_ID, cards, base_dir=tmp_path)
    out = export_query_csv(_QUERY_ID, base_dir=tmp_path)
    data = out.read_text(encoding="utf-8")
    reader = csv.DictReader(io.StringIO(data))
    rows = list(reader)
    assert len(rows) == 2
    assert rows[0]["siren"] == "111"
    assert rows[1]["siren"] == "222"


def test_export_query_csv_headers_from_first_card(tmp_path: Path):
    card = _make_card()
    append_wave(_QUERY_ID, [card], base_dir=tmp_path)
    out = export_query_csv(_QUERY_ID, base_dir=tmp_path)
    data = out.read_text(encoding="utf-8")
    reader = csv.DictReader(io.StringIO(data))
    assert set(reader.fieldnames or []) == set(card.keys())


def test_export_query_csv_returns_path_to_csv(tmp_path: Path):
    append_wave(_QUERY_ID, [_make_card()], base_dir=tmp_path)
    out = export_query_csv(_QUERY_ID, base_dir=tmp_path)
    assert isinstance(out, Path)
    assert out.name.endswith(".csv")


# ---------------------------------------------------------------------------
# export_query_txt
# ---------------------------------------------------------------------------


def test_export_query_txt_creates_txt_file(tmp_path: Path):
    append_wave(_QUERY_ID, [_make_card()], base_dir=tmp_path)
    out = export_query_txt(_QUERY_ID, base_dir=tmp_path)
    assert out.exists()
    assert out.suffix == ".txt"


def test_export_query_txt_contains_card_content(tmp_path: Path):
    append_wave(_QUERY_ID, [_make_card(denomination="GEODIS SAS")], base_dir=tmp_path)
    out = export_query_txt(_QUERY_ID, base_dir=tmp_path)
    text = out.read_text(encoding="utf-8")
    assert "GEODIS SAS" in text
    assert "CARD" in text


def test_export_query_txt_empty_query_creates_empty_file(tmp_path: Path):
    out = export_query_txt("EMPTY_QUERY", base_dir=tmp_path)
    assert out.exists()
    assert out.read_text(encoding="utf-8") == ""


def test_export_query_txt_multiple_cards(tmp_path: Path):
    cards = [_make_card("111"), _make_card("222")]
    append_wave(_QUERY_ID, cards, base_dir=tmp_path)
    out = export_query_txt(_QUERY_ID, base_dir=tmp_path)
    text = out.read_text(encoding="utf-8")
    assert "111" in text
    assert "222" in text


def test_export_query_txt_returns_path(tmp_path: Path):
    append_wave(_QUERY_ID, [_make_card()], base_dir=tmp_path)
    out = export_query_txt(_QUERY_ID, base_dir=tmp_path)
    assert isinstance(out, Path)
    assert out.name.endswith(".txt")
