"""Unit tests for the input-query dedup helper in routes/batch.py."""

import pytest

from fortress.api.routes.batch import _canonicalize_query, _dedup_queries


class TestCanonicalize:
    def test_hyphen_vs_space(self):
        assert _canonicalize_query("BANYULS-SUR-MER") == _canonicalize_query("BANYULS SUR MER")

    def test_double_hyphen(self):
        assert _canonicalize_query("ESPIRA-DE-L'AGLY") == _canonicalize_query("ESPIRA DE L AGLY")

    def test_st_expansion(self):
        assert _canonicalize_query("ST PAUL DE FENOUILLET") == _canonicalize_query("SAINT-PAUL-DE-FENOUILLET")

    def test_ste_expansion(self):
        assert _canonicalize_query("STE MARIE") == _canonicalize_query("SAINTE MARIE")

    def test_case_insensitive(self):
        assert _canonicalize_query("perpignan") == _canonicalize_query("PERPIGNAN")

    def test_accent_strip(self):
        assert _canonicalize_query("Cérêt") == _canonicalize_query("CERET")

    def test_apostrophe_variants(self):
        # Straight apostrophe, curly right single quote, curly left single quote
        assert _canonicalize_query("L'AGLY") == _canonicalize_query("L’AGLY")
        assert _canonicalize_query("L'AGLY") == _canonicalize_query("L‘AGLY")

    def test_empty(self):
        assert _canonicalize_query("") == ""
        assert _canonicalize_query("   ") == ""


class TestDedup:
    def test_ilanit_real_examples(self):
        queries = [
            "CULTURE DE LA VIGNE BANYULS-SUR-MER",
            "CULTURE DE LA VIGNE BANYULS SUR MER",
            "CULTURE DE LA VIGNE ESPIRA-DE-L'AGLY",
            "CULTURE DE LA VIGNE ESPIRA DE L'AGLY",
            "CULTURE DE LA VIGNE ESPIRA DE L AGLY",
            "CULTURE DE LA VIGNE ST PAUL DE FENOUILLET",
            "CULTURE DE LA VIGNE SAINT-PAUL-DE-FENOUILLET",
        ]
        kept, removed = _dedup_queries(queries)
        assert len(kept) == 3
        assert removed == 4
        # First occurrence wins — preserves user's casing
        assert kept[0] == "CULTURE DE LA VIGNE BANYULS-SUR-MER"
        assert kept[1] == "CULTURE DE LA VIGNE ESPIRA-DE-L'AGLY"
        assert kept[2] == "CULTURE DE LA VIGNE ST PAUL DE FENOUILLET"

    def test_preserve_order(self):
        kept, _ = _dedup_queries(["B", "A", "C", "a"])
        assert kept == ["B", "A", "C"]

    def test_empty_inputs_dropped(self):
        kept, removed = _dedup_queries(["A", "", "  ", "A"])
        assert kept == ["A"]
        # The 2 empty strings + 1 duplicate = 3 removed
        assert removed == 3

    def test_no_duplicates(self):
        kept, removed = _dedup_queries(["A", "B", "C"])
        assert kept == ["A", "B", "C"]
        assert removed == 0

    def test_empty_list(self):
        assert _dedup_queries([]) == ([], 0)


class TestEdgeCases:
    def test_st_in_middle_of_word_not_expanded(self):
        # ST is whole-word only — should NOT expand inside "BIST" or "POST"
        assert _canonicalize_query("BIST") != _canonicalize_query("BISAINT")
        assert _canonicalize_query("POST OFFICE") != _canonicalize_query("POSAINT OFFICE")

    def test_multiple_whitespace_runs(self):
        assert _canonicalize_query("A   B    C") == "A B C"

    def test_hyphen_at_boundary(self):
        # Leading / trailing hyphens collapse with space-collapse + strip
        assert _canonicalize_query("-ABC-") == "ABC"
