"""Checkpoint — persist pipeline state to disk after every wave.

Guarantees zero data loss on crash. On restart, the pipeline resumes
from the next incomplete wave automatically.

Directory layout:
  data/checkpoints/{job_id}/
  ├── job_state.json              # progress, triage stats, status
  ├── seen_set.json               # intra-query dedup state
  ├── wave_001_complete.jsonl     # 50 scraped company records (wave 1)
  ├── wave_002_complete.jsonl     # wave 2
  └── ...

All writes are atomic: write to a .tmp file, then rename to final name.
This prevents partial writes leaving corrupted checkpoint files.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fortress.module_d.seen_set import SeenSet

# Root checkpoint directory (relative to project data/ folder).
# Overridable for tests via the `base_dir` parameter.
_DEFAULT_BASE_DIR = Path("data/checkpoints")


def save(
    job_id: str,
    wave_num: int,
    wave_results: list[dict[str, Any]],
    seen_set: SeenSet,
    *,
    job_state: dict[str, Any],
    base_dir: Path = _DEFAULT_BASE_DIR,
) -> None:
    """Save one completed wave plus updated job state.

    Args:
        job_id:        Query identifier (e.g. "AGRICULTURE_66").
        wave_num:      1-based wave number that just completed.
        wave_results:  List of company dicts scraped in this wave.
        seen_set:      Current seen set (serialised alongside wave data).
        job_state:     Full job state dict — will be written to job_state.json.
        base_dir:      Override checkpoint root (useful in tests).
    """
    ckpt_dir = base_dir / job_id
    ckpt_dir.mkdir(parents=True, exist_ok=True)

    # 1. Write wave JSONL — one company record per line
    wave_path = ckpt_dir / f"wave_{wave_num:03d}_complete.jsonl"
    _atomic_write_text(
        wave_path,
        "\n".join(json.dumps(r, ensure_ascii=False, default=str) for r in wave_results)
        + ("\n" if wave_results else ""),
    )

    # 2. Write seen set JSON
    seen_set.save(ckpt_dir / "seen_set.json")

    # 3. Update job state
    job_state["wave_current"] = wave_num
    job_state["updated_at"] = datetime.now(tz=timezone.utc).isoformat()
    _atomic_write_text(
        ckpt_dir / "job_state.json",
        json.dumps(job_state, ensure_ascii=False, indent=2, default=str),
    )


def load(
    job_id: str,
    *,
    base_dir: Path = _DEFAULT_BASE_DIR,
) -> tuple[dict[str, Any] | None, SeenSet]:
    """Load checkpoint state for a job.

    Returns:
        (job_state, seen_set) if a checkpoint exists.
        (None, empty SeenSet) if no checkpoint found.
    """
    ckpt_dir = base_dir / job_id
    state_path = ckpt_dir / "job_state.json"

    if not state_path.exists():
        return None, SeenSet()

    job_state = json.loads(state_path.read_text(encoding="utf-8"))
    seen_set = SeenSet.load(ckpt_dir / "seen_set.json")

    return job_state, seen_set


def load_wave_results(
    job_id: str,
    wave_num: int,
    *,
    base_dir: Path = _DEFAULT_BASE_DIR,
) -> list[dict[str, Any]]:
    """Load the results of a specific completed wave.

    Returns an empty list if the wave file does not exist.
    """
    path = base_dir / job_id / f"wave_{wave_num:03d}_complete.jsonl"
    if not path.exists():
        return []

    results = []
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if line:
            try:
                results.append(json.loads(line))
            except json.JSONDecodeError:
                continue
    return results


def last_completed_wave(
    job_id: str,
    *,
    base_dir: Path = _DEFAULT_BASE_DIR,
) -> int:
    """Return the highest wave number that has a complete checkpoint file.

    Returns 0 if no wave files exist (job not started or no checkpoint).
    """
    ckpt_dir = base_dir / job_id
    if not ckpt_dir.exists():
        return 0

    completed = [
        int(p.stem.split("_")[1])  # "wave_041_complete" → 41
        for p in ckpt_dir.glob("wave_*_complete.jsonl")
        if p.stem.split("_")[1].isdigit()
    ]
    return max(completed, default=0)


def checkpoint_exists(job_id: str, *, base_dir: Path = _DEFAULT_BASE_DIR) -> bool:
    """Return True if any checkpoint data exists for this job."""
    return (base_dir / job_id / "job_state.json").exists()


def clear(job_id: str, *, base_dir: Path = _DEFAULT_BASE_DIR) -> bool:
    """Remove all checkpoint files for a job.

    Returns True if a checkpoint directory was found and removed.
    Safe to call even if no checkpoint exists.
    """
    import shutil

    ckpt_dir = base_dir / job_id
    if ckpt_dir.exists():
        shutil.rmtree(ckpt_dir, ignore_errors=True)
        return True
    return False


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _atomic_write_text(path: Path, content: str) -> None:
    """Write content to path atomically: write .tmp, then rename."""
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(content, encoding="utf-8")
    tmp.replace(path)
