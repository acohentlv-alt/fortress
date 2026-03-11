"""Master JSONL file — cumulative export of all companies across all queries.

File location: data/outputs/fortress_master.jsonl
One JSON record per line. Grows forever; never rewritten from scratch.

PostgreSQL is the dedup engine — this file is an export/mirror, not the source
of truth. Appending here happens after the DB dedup pass has already committed
the canonical record.

Settings note:
    settings.outputs_dir resolves to data/outputs by default.
    The master file lives directly in outputs_dir.
"""

from __future__ import annotations

import json
from collections.abc import Generator
from pathlib import Path

from fortress.config.settings import settings


def _master_path(base_dir: Path | None) -> Path:
    """Return (and ensure parent exists for) the master JSONL file path.

    Default: settings.outputs_dir / "fortress_master.jsonl"
             (i.e. data/outputs/fortress_master.jsonl)
    """
    if base_dir is None:
        base_dir = Path(settings.outputs_dir)
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir / "fortress_master.jsonl"


def append_records(records: list[dict], base_dir: Path | None = None) -> None:
    """Append new company records to the master JSONL file.

    Only NEW records (first time a SIREN is seen) should be appended here.
    Updates to existing records are handled in PostgreSQL; the master file
    is an append-only export — it never has duplicates if callers honour
    the contract that only fresh inserts are passed.
    """
    path = _master_path(base_dir)
    with path.open("a", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, ensure_ascii=False, default=str))
            f.write("\n")


def load_master(base_dir: Path | None = None) -> Generator[dict, None, None]:
    """Stream all master records one by one (RAM-safe for large files).

    The file can grow to millions of records. This generator reads one line
    at a time so memory stays constant regardless of file size.

    Usage:
        for record in load_master():
            process(record)

    Malformed lines are silently skipped.
    """
    path = _master_path(base_dir)
    if not path.exists():
        return
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    yield json.loads(line)
                except json.JSONDecodeError:
                    pass  # skip any corrupted lines
