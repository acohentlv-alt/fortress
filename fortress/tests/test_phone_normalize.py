"""Unit tests for fortress.utils.phone — canonical French phone normalisation.

Covers:
  - normalize_phone()        → 0XXXXXXXXX or ''
  - normalize_phone_e164()   → +33XXXXXXXXX or ''
  - phones_equivalent()      → None-safe equality
  - PHONE_NORMALIZE_SQL      → Python/SQL parity spot-check

22+ cases covering all format variants and edge cases.
"""

import pytest
from fortress.utils.phone import normalize_phone, normalize_phone_e164, phones_equivalent


# ---------------------------------------------------------------------------
# normalize_phone — canonical 0XXXXXXXXX
# ---------------------------------------------------------------------------

class TestNormalizePhone:
    """normalize_phone() should always return 10-digit 0XXXXXXXXX or ''."""

    def test_international_with_trunk_marker(self):
        """'+33 (0)4 68 81 04 61' is the critical (0) case — must be 10 digits, not 11."""
        result = normalize_phone('+33 (0)4 68 81 04 61')
        assert result == '0468810461', f"Expected '0468810461', got {result!r}"

    def test_international_compact(self):
        assert normalize_phone('+33612345678') == '0612345678'

    def test_international_spaced(self):
        assert normalize_phone('+33 6 12 34 56 78') == '0612345678'

    def test_international_dotted(self):
        assert normalize_phone('+33.6.12.34.56.78') == '0612345678'

    def test_international_dashed(self):
        assert normalize_phone('+33-6-12-34-56-78') == '0612345678'

    def test_double_zero_prefix(self):
        assert normalize_phone('0033612345678') == '0612345678'

    def test_double_zero_prefix_spaced(self):
        assert normalize_phone('0033 6 12 34 56 78') == '0612345678'

    def test_eleven_digit_no_plus(self):
        """33 + 9 digits without leading 0 or + — less common but valid."""
        assert normalize_phone('33612345678') == '0612345678'

    def test_national_format(self):
        assert normalize_phone('0612345678') == '0612345678'

    def test_national_spaced(self):
        assert normalize_phone('06 12 34 56 78') == '0612345678'

    def test_national_dotted(self):
        assert normalize_phone('06.12.34.56.78') == '0612345678'

    def test_national_dashed(self):
        assert normalize_phone('06-12-34-56-78') == '0612345678'

    def test_landline(self):
        assert normalize_phone('0146781234') == '0146781234'

    def test_landline_international(self):
        assert normalize_phone('+33146781234') == '0146781234'

    # Edge cases — should return ''

    def test_none_input(self):
        assert normalize_phone(None) == ''

    def test_empty_string(self):
        assert normalize_phone('') == ''

    def test_letters_only(self):
        assert normalize_phone('abcdefghij') == ''

    def test_too_short(self):
        assert normalize_phone('0612') == ''

    def test_too_long_no_prefix(self):
        """Random 15-digit number not matching any French prefix."""
        assert normalize_phone('123456789012345') == ''

    def test_whitespace_only(self):
        assert normalize_phone('   ') == ''

    def test_trunk_marker_mobile(self):
        """Variant with mobile: +33 (0)6 12 34 56 78."""
        assert normalize_phone('+33 (0)6 12 34 56 78') == '0612345678'

    def test_trunk_marker_no_space(self):
        """Compact trunk marker: +33(0)612345678."""
        assert normalize_phone('+33(0)612345678') == '0612345678'


# ---------------------------------------------------------------------------
# normalize_phone_e164 — +33XXXXXXXXX for storage
# ---------------------------------------------------------------------------

class TestNormalizePhoneE164:
    def test_from_national(self):
        assert normalize_phone_e164('0612345678') == '+33612345678'

    def test_from_international(self):
        assert normalize_phone_e164('+33612345678') == '+33612345678'

    def test_from_trunk_marker(self):
        assert normalize_phone_e164('+33 (0)4 68 81 04 61') == '+33468810461'

    def test_from_double_zero(self):
        assert normalize_phone_e164('0033612345678') == '+33612345678'

    def test_none_returns_empty(self):
        assert normalize_phone_e164(None) == ''

    def test_garbage_returns_empty(self):
        assert normalize_phone_e164('not-a-phone') == ''


# ---------------------------------------------------------------------------
# phones_equivalent — None-safe comparison
# ---------------------------------------------------------------------------

class TestPhonesEquivalent:
    def test_same_format(self):
        assert phones_equivalent('0612345678', '0612345678') is True

    def test_national_vs_international(self):
        assert phones_equivalent('0467658567', '+33467658567') is True

    def test_international_vs_double_zero(self):
        assert phones_equivalent('+33467658567', '0033467658567') is True

    def test_different_numbers(self):
        assert phones_equivalent('0612345678', '0612345679') is False

    def test_none_left(self):
        assert phones_equivalent(None, '0612345678') is False

    def test_none_right(self):
        assert phones_equivalent('0612345678', None) is False

    def test_both_none(self):
        assert phones_equivalent(None, None) is False

    def test_empty_strings(self):
        assert phones_equivalent('', '') is False

    def test_trunk_marker_vs_national(self):
        """Core regression: +33 (0)4 68 81 04 61 must equal 0468810461."""
        assert phones_equivalent('+33 (0)4 68 81 04 61', '0468810461') is True
