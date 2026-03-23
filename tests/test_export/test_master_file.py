"""Tests for master_file — append_records() and load_master().

Uses tmp_path to avoid touching the real data directory.
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from fortress.export.master_file import append_records, load_master

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_record(siren: str = "123456789", denomination: str = "SARL Dupont") -> dict:
    return {
        "siren": siren,
        "denomination": denomination,
        "phone": "0561123456",
        "email": "contact@dupont.fr",
    }


# ---------------------------------------------------------------------------
# append_records
# ---------------------------------------------------------------------------


def test_append_records_creates_file(tmp_path: Path):
    records = [_make_record()]
    append_records(records, base_dir=tmp_path)
    assert (tmp_path / "fortress_master.jsonl").exists()


def test_append_records_writes_one_line_per_record(tmp_path: Path):
    records = [_make_record("111"), _make_record("222"), _make_record("333")]
    append_records(records, base_dir=tmp_path)
    lines = (tmp_path / "fortress_master.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 3


def test_append_records_writes_valid_json(tmp_path: Path):
    record = _make_record(siren="987654321")
    append_records([record], base_dir=tmp_path)
    content = (tmp_path / "fortress_master.jsonl").read_text(encoding="utf-8")
    obj = json.loads(content.strip())
    assert obj["siren"] == "987654321"


def test_append_records_appends_on_second_call(tmp_path: Path):
    append_records([_make_record("111")], base_dir=tmp_path)
    append_records([_make_record("222")], base_dir=tmp_path)
    lines = (tmp_path / "fortress_master.jsonl").read_text(encoding="utf-8").splitlines()
    assert len(lines) == 2
    sirens = [json.loads(l)["siren"] for l in lines]
    assert "111" in sirens
    assert "222" in sirens


def test_append_records_empty_list_writes_no_records(tmp_path: Path):
    append_records([], base_dir=tmp_path)
    # File may be created but no records written — load returns empty
    assert list(load_master(base_dir=tmp_path)) == []


def test_append_records_preserves_french_chars(tmp_path: Path):
    record = {"siren": "123", "denomination": "Boulangerie Héritier & Fils"}
    append_records([record], base_dir=tmp_path)
    content = (tmp_path / "fortress_master.jsonl").read_text(encoding="utf-8")
    assert "Héritier" in content


def test_append_records_creates_parent_dirs(tmp_path: Path):
    nested = tmp_path / "a" / "b" / "c"
    append_records([_make_record()], base_dir=nested)
    assert (nested / "fortress_master.jsonl").exists()


# ---------------------------------------------------------------------------
# load_master
# ---------------------------------------------------------------------------


def test_load_master_nonexistent_file_returns_empty(tmp_path: Path):
    result = list(load_master(base_dir=tmp_path))
    assert result == []


def test_load_master_reads_back_records(tmp_path: Path):
    records = [_make_record("111"), _make_record("222")]
    append_records(records, base_dir=tmp_path)
    loaded = list(load_master(base_dir=tmp_path))
    assert len(loaded) == 2
    assert loaded[0]["siren"] == "111"
    assert loaded[1]["siren"] == "222"


def test_load_master_returns_dicts(tmp_path: Path):
    append_records([_make_record()], base_dir=tmp_path)
    results = list(load_master(base_dir=tmp_path))
    assert all(isinstance(r, dict) for r in results)


def test_load_master_skips_malformed_lines(tmp_path: Path):
    path = tmp_path / "fortress_master.jsonl"
    # Write one valid + one malformed line
    path.write_text(
        json.dumps({"siren": "111"}) + "\n"
        + "NOT_JSON\n"
        + json.dumps({"siren": "222"}) + "\n",
        encoding="utf-8",
    )
    results = list(load_master(base_dir=tmp_path))
    assert len(results) == 2
    sirens = [r["siren"] for r in results]
    assert "111" in sirens
    assert "222" in sirens


def test_load_master_is_generator(tmp_path: Path):
    append_records([_make_record()], base_dir=tmp_path)
    gen = load_master(base_dir=tmp_path)
    # Should be a generator (not a list)
    import types
    assert isinstance(gen, types.GeneratorType)


def test_load_master_skips_blank_lines(tmp_path: Path):
    path = tmp_path / "fortress_master.jsonl"
    path.write_text(
        json.dumps({"siren": "111"}) + "\n\n\n",
        encoding="utf-8",
    )
    results = list(load_master(base_dir=tmp_path))
    assert len(results) == 1


def test_load_master_roundtrip_french_chars(tmp_path: Path):
    record = {"siren": "999", "denomination": "Société Générale"}
    append_records([record], base_dir=tmp_path)
    results = list(load_master(base_dir=tmp_path))
    assert results[0]["denomination"] == "Société Générale"
