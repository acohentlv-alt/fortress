"""Unit tests for per-query department resolution (mixed-dept batches)."""
from fortress.discovery import _parse_dept_hint_from_query


def test_parse_dept_hint_2_digit_code():
    assert _parse_dept_hint_from_query("arboriculture 47") == "47"
    assert _parse_dept_hint_from_query("viticulture 51") == "51"
    assert _parse_dept_hint_from_query("camping 66") == "66"


def test_parse_dept_hint_postal_code():
    assert _parse_dept_hint_from_query("camping 66000") == "66"
    assert _parse_dept_hint_from_query("transport 33000") == "33"


def test_parse_dept_hint_no_code():
    assert _parse_dept_hint_from_query("camping") is None
    assert _parse_dept_hint_from_query("restaurant Paris") is None


def test_effective_dept_falls_back_to_batch():
    query_hint = _parse_dept_hint_from_query("camping")
    batch_dept = "75"
    effective = query_hint or batch_dept
    assert effective == "75"


def test_effective_dept_overrides_batch():
    query_hint = _parse_dept_hint_from_query("arboriculture 47")
    batch_dept = "51"
    effective = query_hint or batch_dept
    assert effective == "47"
