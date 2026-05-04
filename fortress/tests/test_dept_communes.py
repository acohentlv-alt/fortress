"""Data integrity tests for the hardcoded dept_communes.py file."""

from fortress.config.dept_communes import DEPT_COMMUNES


def test_dept_codes_valid():
    valid_codes = {f"{i:02d}" for i in range(1, 96) if i != 20}
    valid_codes.update({"2A", "2B"})
    valid_codes.update({"971", "972", "973", "974", "975", "976", "977", "978", "986", "987", "988"})
    invalid = set(DEPT_COMMUNES.keys()) - valid_codes
    assert not invalid, f"Invalid dept codes leaked through: {invalid}"


def test_no_empty_communes():
    for dept, communes in DEPT_COMMUNES.items():
        assert len(communes) > 0, f"Dept {dept} has zero communes"


def test_communes_sorted_by_count_desc():
    for dept, communes in DEPT_COMMUNES.items():
        counts = [c[1] for c in communes]
        assert counts == sorted(counts, reverse=True), f"Dept {dept} not sorted desc"


def test_known_communes_present():
    bordeaux_33 = [c for c in DEPT_COMMUNES["33"] if c[0] == "BORDEAUX"]
    assert len(bordeaux_33) == 1
    assert bordeaux_33[0][1] > 50000

    paris_75 = [c for c in DEPT_COMMUNES["75"] if c[0] == "PARIS"]
    assert len(paris_75) == 1

    perpignan_66 = [c for c in DEPT_COMMUNES["66"] if c[0] == "PERPIGNAN"]
    assert len(perpignan_66) == 1


def test_no_sentinel_dept():
    assert "[N" not in DEPT_COMMUNES
    assert "99" not in DEPT_COMMUNES
    assert "970" not in DEPT_COMMUNES
