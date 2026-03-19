"""Per-query JSONL output file management.

Each query gets its own JSONL file at:
    data/outputs/queries/{batch_id}.jsonl

One JSON object per line. The file grows as waves complete.

Settings note:
    settings.outputs_dir resolves to data/outputs by default.
    The per-query subdirectory is outputs_dir / "queries".
"""

from __future__ import annotations

import csv
import json
from pathlib import Path

from fortress.config.settings import settings
from fortress.module_e.card_formatter import format_card_text


def _query_dir(base_dir: Path | None) -> Path:
    """Return (and create if needed) the per-query output directory.

    Default: settings.outputs_dir / "queries"  (i.e. data/outputs/queries)
    """
    if base_dir is None:
        base_dir = Path(settings.outputs_dir) / "queries"
    base_dir.mkdir(parents=True, exist_ok=True)
    return base_dir


def _query_path(batch_id: str, base_dir: Path | None = None) -> Path:
    """Return the JSONL path for a given batch_id."""
    return _query_dir(base_dir) / f"{batch_id}.jsonl"


def append_wave(batch_id: str, cards: list[dict], base_dir: Path | None = None) -> None:
    """Append cards to the query's JSONL file (one JSON per line, atomic append).

    Called after each wave completes. The file grows incrementally — it is never
    rewritten from scratch, so partial results survive a crash mid-query.
    """
    path = _query_path(batch_id, base_dir)
    with path.open("a", encoding="utf-8") as f:
        for card in cards:
            f.write(json.dumps(card, ensure_ascii=False, default=str))
            f.write("\n")


def load_query_cards(batch_id: str, base_dir: Path | None = None) -> list[dict]:
    """Load all cards from a query's JSONL file.

    Returns an empty list if the file does not exist yet.
    Malformed lines (rare, e.g. truncated by crash) are silently skipped.
    """
    path = _query_path(batch_id, base_dir)
    if not path.exists():
        return []
    cards: list[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    cards.append(json.loads(line))
                except json.JSONDecodeError:
                    pass  # skip malformed lines
    return cards


def export_query_csv(batch_id: str, base_dir: Path | None = None) -> Path:
    """Export all query cards to a CSV file alongside the JSONL file.

    Returns the path to the written CSV file.
    Produces an empty file if no cards exist yet (not an error — query may still
    be running).
    """
    cards = load_query_cards(batch_id, base_dir)
    out_path = _query_path(batch_id, base_dir).with_suffix(".csv")
    if not cards:
        out_path.write_text("", encoding="utf-8")
        return out_path

    fieldnames = list(cards[0].keys())
    with out_path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(cards)
    return out_path


def export_query_txt(batch_id: str, base_dir: Path | None = None) -> Path:
    """Export all query cards as formatted human-readable text cards.

    Each card is rendered by format_card_text(), separated by a blank line.
    Returns the path to the written .txt file.
    """
    cards = load_query_cards(batch_id, base_dir)
    out_path = _query_path(batch_id, base_dir).with_suffix(".txt")
    with out_path.open("w", encoding="utf-8") as f:
        for card in cards:
            f.write(format_card_text(card))
            f.write("\n\n")
    return out_path
