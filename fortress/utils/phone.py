"""Canonical French phone normalisation utilities.

All phone comparison, normalisation, and SQL helpers go here.
Import these instead of writing local helpers.

Public API
----------
normalize_phone(raw)          → '0XXXXXXXXX' or '' (for Python comparison)
normalize_phone_e164(raw)     → '+33XXXXXXXXX' or '' (for storage in contacts)
phones_equivalent(a, b)       → bool — None-safe equality after normalisation
PHONE_NORMALIZE_SQL           → SQL expression template (substitute {col})

French phone formats handled
-----------------------------
  +33 (0)4 68 81 04 61   → 0468810461   (trunk-marker style, very common)
  +33612345678           → 0612345678
  +33 6 12 34 56 78      → 0612345678
  0033612345678          → 0612345678
  33612345678            → 0612345678   (11-digit without leading +/00)
  0612345678             → 0612345678   (already canonical)
  any other              → ''           (unrecognisable — skip comparison)
"""

from __future__ import annotations

import re

# Strip (0) trunk marker before digit stripping so "+33 (0)4..." doesn't
# produce an extra leading zero.
_TRUNK_MARKER_RE = re.compile(r"\(0\)")

# Keep only digits and the leading '+' (for +33 detection).
_NON_DIGIT_RE = re.compile(r"[^\d+]")


def normalize_phone(raw: str | None) -> str:
    """Return canonical 10-digit '0XXXXXXXXX' form, or '' if unrecognisable.

    This is the form used for Python-side comparison only.
    Do NOT store this directly — use normalize_phone_e164() for DB storage.

    Examples
    --------
    >>> normalize_phone('+33 (0)4 68 81 04 61')
    '0468810461'
    >>> normalize_phone('+33612345678')
    '0612345678'
    >>> normalize_phone('0033612345678')
    '0612345678'
    >>> normalize_phone('33612345678')
    '0612345678'
    >>> normalize_phone('0612345678')
    '0612345678'
    >>> normalize_phone(None)
    ''
    >>> normalize_phone('')
    ''
    """
    if not raw:
        return ""

    # Strip (0) trunk marker BEFORE removing non-digits so "+33 (0)4..." → "+334..."
    s = _TRUNK_MARKER_RE.sub("", raw)

    # Keep '+' and digits only
    s = _NON_DIGIT_RE.sub("", s)

    # +33XXXXXXXXX  (12 chars: +33 + 9 digits)
    if s.startswith("+33") and len(s) >= 12:
        return "0" + s[3:12]

    # 0033XXXXXXXXX  (13 chars: 0033 + 9 digits)
    if s.startswith("0033") and len(s) >= 13:
        return "0" + s[4:13]

    # 33XXXXXXXXX  (11 chars: 33 + 9 digits, no leading 0 or +)
    if s.startswith("33") and len(s) == 11:
        return "0" + s[2:]

    # Already 0XXXXXXXXX  (10 digits, starts with 0)
    if s.startswith("0") and len(s) == 10:
        return s

    return ""


def normalize_phone_e164(raw: str | None) -> str:
    """Return E.164 '+33XXXXXXXXX' form, or '' if unrecognisable.

    Use this for storing phone numbers in the contacts table
    (matching the format extract_phones() produces for website crawl results).

    Examples
    --------
    >>> normalize_phone_e164('+33 (0)4 68 81 04 61')
    '+33468810461'
    >>> normalize_phone_e164('0612345678')
    '+33612345678'
    """
    canonical = normalize_phone(raw)
    if not canonical:
        return ""
    # canonical is '0XXXXXXXXX' — convert to E.164
    return "+33" + canonical[1:]


def phones_equivalent(a: str | None, b: str | None) -> bool:
    """Return True if two phone strings refer to the same number.

    None-safe. Normalises both sides before comparing.

    Examples
    --------
    >>> phones_equivalent('0467658567', '+33467658567')
    True
    >>> phones_equivalent(None, '0612345678')
    False
    """
    na = normalize_phone(a)
    nb = normalize_phone(b)
    if not na or not nb:
        return False
    return na == nb


# ---------------------------------------------------------------------------
# SQL normalisation expression
# ---------------------------------------------------------------------------
# Use this in WHERE clauses to compare a DB column against a normalised value.
#
# Usage (psycopg / asyncpg):
#   query = f"SELECT ... WHERE ({PHONE_NORMALIZE_SQL.format(col='c.phone')}) = %s"
#   params = (normalize_phone(raw_phone),)
#
# The expression maps any stored phone format to '0XXXXXXXXX'.
# Five branches cover all canonical prefixes in priority order.
# Returns '' (empty string) for unrecognisable values — which will never
# equal a valid normalised phone, so unrecognisable rows are safely excluded.

PHONE_NORMALIZE_SQL = """\
CASE
    WHEN regexp_replace({col}, '[^0-9+]', '', 'g') ~ '^\\+33[0-9]{{9}}$'
        THEN '0' || substr(regexp_replace({col}, '[^0-9+]', '', 'g'), 4)
    WHEN regexp_replace(regexp_replace({col}, '\\(0\\)', '', 'g'), '[^0-9+]', '', 'g') ~ '^\\+33[0-9]{{9}}$'
        THEN '0' || substr(regexp_replace(regexp_replace({col}, '\\(0\\)', '', 'g'), '[^0-9+]', '', 'g'), 4)
    WHEN regexp_replace({col}, '[^0-9]', '', 'g') ~ '^0033[0-9]{{9}}$'
        THEN '0' || substr(regexp_replace({col}, '[^0-9]', '', 'g'), 5)
    WHEN regexp_replace({col}, '[^0-9]', '', 'g') ~ '^33[0-9]{{9}}$'
        THEN '0' || substr(regexp_replace({col}, '[^0-9]', '', 'g'), 3)
    WHEN regexp_replace({col}, '[^0-9]', '', 'g') ~ '^0[0-9]{{9}}$'
        THEN regexp_replace({col}, '[^0-9]', '', 'g')
    ELSE ''
END"""
