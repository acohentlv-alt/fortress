"""In-memory export helpers — produce bytes for Streamlit download_button.

These functions take a list of card dicts (as returned by format_card or
load_query_cards) and convert them to bytes suitable for st.download_button.

No file I/O happens here — everything is in-memory so Streamlit can stream
the result directly to the browser without touching the filesystem.
"""

from __future__ import annotations

import csv
import io
import json

from fortress.module_e.card_formatter import format_card_text


def to_csv_bytes(cards: list[dict]) -> bytes:
    """Convert a card list to UTF-8 CSV bytes.

    Column order follows the key order of the first card dict.
    Returns empty bytes if the list is empty (Streamlit handles that gracefully).
    """
    if not cards:
        return b""
    buf = io.StringIO()
    fieldnames = list(cards[0].keys())
    writer = csv.DictWriter(buf, fieldnames=fieldnames, extrasaction="ignore")
    writer.writeheader()
    writer.writerows(cards)
    return buf.getvalue().encode("utf-8")


def to_jsonl_bytes(cards: list[dict]) -> bytes:
    """Convert a card list to JSONL bytes (one JSON object per line).

    Uses ensure_ascii=False so French characters (accents, etc.) are preserved
    in their native form rather than being escaped.
    """
    lines = [json.dumps(card, ensure_ascii=False, default=str) for card in cards]
    return "\n".join(lines).encode("utf-8")


def to_txt_bytes(cards: list[dict]) -> bytes:
    """Convert a card list to formatted human-readable text card bytes.

    Each card is rendered by format_card_text(), separated by a blank line.
    Suitable for a plain-text download that the user can read directly.
    """
    parts = [format_card_text(card) for card in cards]
    return "\n\n".join(parts).encode("utf-8")
