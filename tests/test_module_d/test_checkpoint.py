"""Tests for checkpoint — uses tmp_path, no real DB needed.

Functions confirmed present in checkpoint.py:
  - save(job_id, wave_num, wave_results, seen_set, *, job_state, base_dir) -> None
  - load(job_id, *, base_dir) -> tuple[dict | None, SeenSet]
  - load_wave_results(job_id, wave_num, *, base_dir) -> list[dict]
  - last_completed_wave(job_id, *, base_dir) -> int
  - checkpoint_exists(job_id, *, base_dir) -> bool

All writes are atomic: write to .tmp then rename.
Directory layout:
  base_dir/{job_id}/job_state.json
  base_dir/{job_id}/seen_set.json
  base_dir/{job_id}/wave_NNN_complete.jsonl
"""
from __future__ import annotations

import json
from pathlib import Path

import pytest

from fortress.module_d.checkpoint import (
    checkpoint_exists,
    last_completed_wave,
    load,
    load_wave_results,
    save,
)
from fortress.module_d.seen_set import SeenSet


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _make_job_state(wave: int = 1, status: str = "in_progress") -> dict:
    """Minimal job state dict for testing."""
    return {
        "query": "AGRICULTURE 66",
        "query_id": "AGRICULTURE_66",
        "wave_size": 50,
        "total_waves": 10,
        "wave_current": wave,
        "status": status,
    }


def _make_wave_results(n: int = 3) -> list[dict]:
    """Minimal list of company-like dicts for testing."""
    return [
        {"siren": f"10000000{i}", "denomination": f"Company {i}", "code_postal": "66000"}
        for i in range(n)
    ]


# ---------------------------------------------------------------------------
# checkpoint_exists
# ---------------------------------------------------------------------------


def test_checkpoint_exists_returns_false_for_fresh_job(tmp_path):
    """No checkpoint returns False."""
    assert checkpoint_exists("AGRICULTURE_66", base_dir=tmp_path) is False


def test_checkpoint_exists_returns_true_after_save(tmp_path):
    """checkpoint_exists returns True after save() is called."""
    seen = SeenSet()
    state = _make_job_state(wave=1)
    save("AGRICULTURE_66", 1, _make_wave_results(), seen, job_state=state, base_dir=tmp_path)

    assert checkpoint_exists("AGRICULTURE_66", base_dir=tmp_path) is True


# ---------------------------------------------------------------------------
# save — file creation
# ---------------------------------------------------------------------------


def test_save_creates_job_state_json(tmp_path):
    """job_state.json is created after save()."""
    seen = SeenSet()
    state = _make_job_state(wave=1)
    save("AGRICULTURE_66", 1, _make_wave_results(), seen, job_state=state, base_dir=tmp_path)

    job_state_path = tmp_path / "AGRICULTURE_66" / "job_state.json"
    assert job_state_path.exists()


def test_save_creates_wave_jsonl(tmp_path):
    """wave_001_complete.jsonl is created after save() with wave_num=1."""
    seen = SeenSet()
    state = _make_job_state(wave=1)
    save("AGRICULTURE_66", 1, _make_wave_results(), seen, job_state=state, base_dir=tmp_path)

    wave_path = tmp_path / "AGRICULTURE_66" / "wave_001_complete.jsonl"
    assert wave_path.exists()


def test_save_creates_seen_set_json(tmp_path):
    """seen_set.json is created after save()."""
    seen = SeenSet()
    seen.mark_seen({"siren": "123456789"})
    state = _make_job_state(wave=1)
    save("AGRICULTURE_66", 1, [], seen, job_state=state, base_dir=tmp_path)

    seen_path = tmp_path / "AGRICULTURE_66" / "seen_set.json"
    assert seen_path.exists()


def test_save_wave_jsonl_content(tmp_path):
    """Wave JSONL file contains one JSON object per line."""
    seen = SeenSet()
    results = _make_wave_results(n=3)
    state = _make_job_state(wave=1)
    save("AGRICULTURE_66", 1, results, seen, job_state=state, base_dir=tmp_path)

    wave_path = tmp_path / "AGRICULTURE_66" / "wave_001_complete.jsonl"
    lines = [l for l in wave_path.read_text(encoding="utf-8").splitlines() if l.strip()]
    assert len(lines) == 3
    # Each line must be valid JSON
    for line in lines:
        obj = json.loads(line)
        assert "siren" in obj


def test_save_job_state_json_content(tmp_path):
    """job_state.json contains wave_current and updated_at fields."""
    seen = SeenSet()
    state = _make_job_state(wave=3)
    save("AGRICULTURE_66", 3, [], seen, job_state=state, base_dir=tmp_path)

    state_path = tmp_path / "AGRICULTURE_66" / "job_state.json"
    data = json.loads(state_path.read_text(encoding="utf-8"))
    assert data["wave_current"] == 3
    assert "updated_at" in data


def test_save_wave_number_zero_padded(tmp_path):
    """Wave files use zero-padded 3-digit numbers: wave_042_complete.jsonl."""
    seen = SeenSet()
    state = _make_job_state(wave=42)
    save("AGRICULTURE_66", 42, [], seen, job_state=state, base_dir=tmp_path)

    wave_path = tmp_path / "AGRICULTURE_66" / "wave_042_complete.jsonl"
    assert wave_path.exists()


def test_save_creates_subdirectory_for_job(tmp_path):
    """save() creates the job's checkpoint directory automatically."""
    seen = SeenSet()
    state = _make_job_state()
    save("NEW_JOB_99", 1, [], seen, job_state=state, base_dir=tmp_path)

    assert (tmp_path / "NEW_JOB_99").is_dir()


def test_save_empty_wave_results(tmp_path):
    """save() works with an empty wave results list."""
    seen = SeenSet()
    state = _make_job_state(wave=1)
    # Should not raise
    save("AGRICULTURE_66", 1, [], seen, job_state=state, base_dir=tmp_path)

    wave_path = tmp_path / "AGRICULTURE_66" / "wave_001_complete.jsonl"
    assert wave_path.exists()


# ---------------------------------------------------------------------------
# load — state restoration
# ---------------------------------------------------------------------------


def test_load_returns_none_if_no_checkpoint(tmp_path):
    """Fresh job with no checkpoint returns (None, empty SeenSet)."""
    state, seen = load("FRESH_JOB", base_dir=tmp_path)

    assert state is None
    assert isinstance(seen, SeenSet)
    assert seen.size == 0


def test_load_restores_job_state(tmp_path):
    """load() returns the job state dict that was saved."""
    seen = SeenSet()
    original_state = _make_job_state(wave=5, status="in_progress")
    save("AGRICULTURE_66", 5, [], seen, job_state=original_state, base_dir=tmp_path)

    loaded_state, _ = load("AGRICULTURE_66", base_dir=tmp_path)

    assert loaded_state is not None
    assert loaded_state["query_id"] == "AGRICULTURE_66"
    assert loaded_state["wave_current"] == 5


def test_load_restores_seen_set(tmp_path):
    """load() returns the SeenSet that was saved."""
    seen = SeenSet()
    seen.mark_seen({"siren": "123456789"})
    seen.mark_seen({"phone": "0600000001"})
    state = _make_job_state(wave=1)
    save("AGRICULTURE_66", 1, [], seen, job_state=state, base_dir=tmp_path)

    _, loaded_seen = load("AGRICULTURE_66", base_dir=tmp_path)

    assert loaded_seen.is_seen({"siren": "123456789"})
    assert loaded_seen.is_seen({"phone": "0600000001"})


def test_load_after_multiple_saves_returns_latest_state(tmp_path):
    """After multiple save() calls, load() returns the most recent state."""
    seen = SeenSet()

    for wave in range(1, 4):
        state = _make_job_state(wave=wave)
        save("AGRICULTURE_66", wave, _make_wave_results(1), seen, job_state=state, base_dir=tmp_path)

    loaded_state, _ = load("AGRICULTURE_66", base_dir=tmp_path)
    assert loaded_state["wave_current"] == 3


# ---------------------------------------------------------------------------
# load_wave_results
# ---------------------------------------------------------------------------


def test_load_wave_results_returns_empty_for_missing_wave(tmp_path):
    """load_wave_results() returns [] for a wave that was never saved."""
    results = load_wave_results("AGRICULTURE_66", 99, base_dir=tmp_path)
    assert results == []


def test_load_wave_results_returns_saved_records(tmp_path):
    """load_wave_results() returns exactly the records that were saved."""
    seen = SeenSet()
    original_results = _make_wave_results(n=3)
    state = _make_job_state(wave=2)
    save("AGRICULTURE_66", 2, original_results, seen, job_state=state, base_dir=tmp_path)

    loaded = load_wave_results("AGRICULTURE_66", 2, base_dir=tmp_path)

    assert len(loaded) == 3
    sirens = {r["siren"] for r in loaded}
    expected_sirens = {r["siren"] for r in original_results}
    assert sirens == expected_sirens


def test_load_wave_results_correct_wave_number(tmp_path):
    """Wave 1 and Wave 2 results are stored separately and loaded independently."""
    seen = SeenSet()

    wave1_results = [{"siren": "111111111", "denomination": "Alpha"}]
    wave2_results = [{"siren": "222222222", "denomination": "Beta"}]

    save("AGRICULTURE_66", 1, wave1_results, seen, job_state=_make_job_state(wave=1), base_dir=tmp_path)
    save("AGRICULTURE_66", 2, wave2_results, seen, job_state=_make_job_state(wave=2), base_dir=tmp_path)

    loaded1 = load_wave_results("AGRICULTURE_66", 1, base_dir=tmp_path)
    loaded2 = load_wave_results("AGRICULTURE_66", 2, base_dir=tmp_path)

    assert loaded1[0]["siren"] == "111111111"
    assert loaded2[0]["siren"] == "222222222"


# ---------------------------------------------------------------------------
# last_completed_wave
# ---------------------------------------------------------------------------


def test_last_completed_wave_returns_zero_for_new_job(tmp_path):
    """Returns 0 if no wave files exist."""
    result = last_completed_wave("AGRICULTURE_66", base_dir=tmp_path)
    assert result == 0


def test_last_completed_wave_returns_highest_wave(tmp_path):
    """Returns the highest wave number that has a complete file."""
    seen = SeenSet()

    for wave in [1, 2, 3]:
        state = _make_job_state(wave=wave)
        save("AGRICULTURE_66", wave, [], seen, job_state=state, base_dir=tmp_path)

    result = last_completed_wave("AGRICULTURE_66", base_dir=tmp_path)
    assert result == 3


def test_last_completed_wave_single_wave(tmp_path):
    """Works correctly with a single completed wave."""
    seen = SeenSet()
    save("AGRICULTURE_66", 7, [], seen, job_state=_make_job_state(wave=7), base_dir=tmp_path)

    result = last_completed_wave("AGRICULTURE_66", base_dir=tmp_path)
    assert result == 7


def test_last_completed_wave_handles_missing_job_dir(tmp_path):
    """Returns 0 when the job directory does not exist at all."""
    result = last_completed_wave("NONEXISTENT_JOB", base_dir=tmp_path)
    assert result == 0
