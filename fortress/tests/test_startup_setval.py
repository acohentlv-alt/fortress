"""Unit tests for the lifespan maps_id_seq setval SQL rewrite.

Brief 06 rewrites MAX(CAST(SUBSTRING(siren FROM 5) AS INTEGER)) →
CAST(SUBSTRING(MAX(siren) FROM 5) AS INTEGER) so Postgres uses
companies_pkey via Index Only Scan Backward (~0.05 ms) instead of a
parallel seq scan over 14.7M rows (~11 s on cold cache).

All tests are pure-Python static checks + invariant proofs.
No live DB connection required — runs in ~100 ms.
"""

import re
from pathlib import Path


# Resolve the path to api/main.py relative to THIS file's location
# (fortress/tests/ → fortress/ → api/main.py)
_MAIN_PY = Path(__file__).parent.parent / "api" / "main.py"


def test_setval_sql_uses_max_siren_not_max_substring():
    """Fast path: api/main.py must contain MAX(siren) for the setval aggregate.

    Whitespace-robust regex so future indentation edits don't break the test
    as long as the SQL is still correct.

    Note: the negative assertion checks for the comma-form SUBSTRING(siren, 5)
    only — the fallback branch intentionally keeps SUBSTRING(siren FROM 5) (the
    FROM-form), which is textually distinct. This test catches an AI regression
    back to the comma-form (the most likely revert pattern) without false-
    flagging the intentional overflow-fallback branch.
    """
    source = _MAIN_PY.read_text()

    # Happy-path branch must use MAX(siren)
    assert re.search(r"MAX\s*\(\s*siren\s*\)", source), (
        "Expected MAX(siren) in api/main.py — fast-path index-friendly aggregate not found. "
        "Has Brief 06's Change A been applied?"
    )

    # Comma-form SUBSTRING(siren, 5) must NOT appear in SQL strings — that's the old
    # slow-path form. Strip Python comment lines before checking so the comment block
    # that documents "NOT MAX(SUBSTRING(siren, 5)::INTEGER)" doesn't false-positive.
    # The fallback branch uses SUBSTRING(siren FROM 5) (FROM-form), which is fine.
    non_comment_lines = "\n".join(
        line for line in source.splitlines()
        if not line.lstrip().startswith("#")
    )
    assert not re.search(r"SUBSTRING\s*\(\s*siren\s*,\s*5\s*\)", non_comment_lines), (
        "Old slow-path SUBSTRING(siren, 5) (comma-form) detected in non-comment code in "
        "api/main.py. This forces a parallel seq scan. "
        "Use SUBSTRING(siren FROM 5) or MAX(siren) instead."
    )


def test_setval_sql_has_overflow_guard():
    """api/main.py must contain both the overflow detection regex and the WARNING fallback."""
    source = _MAIN_PY.read_text()

    assert "'^MAPS[0-9]{6,}$'" in source, (
        "Overflow guard regex '^MAPS[0-9]{6,}$' not found in api/main.py. "
        "Change A should include a regex check for 6+ digit MAPS suffixes."
    )

    assert 'logger.warning' in source and 'MAPS ID overflow' in source, (
        "WARNING fallback for MAPS ID overflow not found in api/main.py. "
        "Change A should log a warning and fall back to legacy SQL when overflow is detected."
    )


def test_lex_max_equals_numeric_max_at_5_digit_padding():
    """For zero-padded 5-digit MAPS IDs, lexicographic max equals numeric max.

    This is the invariant that makes the Brief 06 SQL rewrite correct:
    MAX(siren) on zero-padded fixed-width identifiers returns the same row
    as MAX(SUBSTRING(siren, 5)::INTEGER) would have selected.
    """
    maps_ids = ["MAPS00001", "MAPS00100", "MAPS06919"]

    lex_max = max(maps_ids)
    assert lex_max == "MAPS06919", (
        f"Lexicographic max is {lex_max!r}, expected 'MAPS06919'. "
        "Zero-padded 5-digit lex order must equal numeric order."
    )

    numeric_max = int(lex_max[4:])
    assert numeric_max == 6919, (
        f"Numeric extraction from lex-max gave {numeric_max}, expected 6919."
    )

    # Cross-check: extracting the suffix from the lex-max equals the numeric max
    # of all suffixes — confirming the rewrite's core invariant.
    suffix_max = max(int(s[4:]) for s in maps_ids)
    assert numeric_max == suffix_max, (
        f"Numeric max of all suffixes is {suffix_max} but lex-max suffix is {numeric_max}. "
        "Invariant violated — lex-max and numeric-max diverge at 5-digit padding."
    )


def test_lex_max_diverges_at_6_digit_overflow():
    """Documents WHY the overflow tripwire is necessary.

    At 6+ digit suffixes, lexicographic order diverges from numeric order:
    'MAPS99999' sorts AFTER 'MAPS100000' lexicographically (because '9' > '1'),
    but numerically 100000 > 99999.

    This is the exact failure mode the overflow guard at startup prevents:
    if a 10-char MAPS row ever existed, MAX(siren) would return 'MAPS99999'
    (wrong) instead of 'MAPS100000' (correct).

    Note: this failure mode cannot occur with the current schema (companies.siren
    is varchar(9); a 10-char INSERT raises StringDataRightTruncation). The test
    documents the invariant for future maintainers.
    """
    maps_ids_with_overflow = ["MAPS06919", "MAPS99999", "MAPS100000"]

    lex_max = max(maps_ids_with_overflow)
    # Lexicographically: '9' > '1', so 'MAPS99999' > 'MAPS100000'
    assert lex_max == "MAPS99999", (
        f"Expected lex-max to be 'MAPS99999' (the divergence case), got {lex_max!r}. "
        "String comparison: '9' > '1', so 'MAPS99999' sorts after 'MAPS100000'."
    )

    numeric_max_suffix = max(int(s[4:]) for s in maps_ids_with_overflow)
    assert numeric_max_suffix == 100000, (
        f"Numeric max suffix is {numeric_max_suffix}, expected 100000."
    )

    # The divergence: lex-max picks 99999, numeric-max is 100000 — they differ.
    assert int(lex_max[4:]) != numeric_max_suffix, (
        "Expected lex-max suffix and numeric-max to diverge at 6-digit overflow, "
        "but they were equal — the test premise is wrong."
    )


def test_empty_maps_returns_zero_for_setval():
    """COALESCE-to-zero edge case: when no MAPS rows exist, setval starts at 1.

    The SQL wraps the aggregate in COALESCE(..., 0) + 1, so:
    - MAX(siren) returns NULL (no rows match LIKE 'MAPS%')
    - SUBSTRING(NULL, 5) returns NULL
    - CAST(NULL AS INTEGER) returns NULL
    - COALESCE(NULL, 0) returns 0
    - 0 + 1 = 1
    - setval('maps_id_seq', 1, false) → first nextval returns 1 → MAPS00001

    This matches the original legacy SQL behavior exactly (same COALESCE logic).
    """
    # Pure-Python simulation of the COALESCE logic
    def coalesce_logic(max_siren: str | None) -> int:
        """Simulate: COALESCE(CAST(SUBSTRING(MAX(siren) FROM 5) AS INTEGER), 0) + 1."""
        if max_siren is None:
            return 0 + 1  # COALESCE(NULL, 0) + 1
        suffix = max_siren[4:]  # SUBSTRING(max_siren FROM 5) — 1-based, takes chars 5..end
        return int(suffix) + 1

    # Empty MAPS table: MAX(siren) returns NULL
    result = coalesce_logic(None)
    assert result == 1, (
        f"Expected setval to start at 1 for empty MAPS table, got {result}. "
        "First MAPS row should be MAPS00001."
    )

    # Non-empty: verify the happy-path too
    result_non_empty = coalesce_logic("MAPS06919")
    assert result_non_empty == 6920, (
        f"Expected setval 6920 when max MAPS is MAPS06919, got {result_non_empty}."
    )
