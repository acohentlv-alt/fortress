from fortress.discovery import _name_match_score


def test_char_substring_rejected_alluma():
    assert _name_match_score("Alluma", "SAGEM ALLUMAGE") == 0.0


def test_char_substring_rejected_ajolia():
    assert _name_match_score("Jolia", "AJOLIA") == 0.0


def test_char_substring_rejected_dupontel():
    assert _name_match_score("Dupont", "DUPONTEL") == 0.0


def test_token_subset_roberto_stari():
    assert _name_match_score("Roberto Stari - Salon de Coiffure", "ROBERTO STARI") == 1.0


def test_token_subset_cafe_cosmos():
    assert _name_match_score("Café Cosmos", "COSMOS") == 1.0


def test_token_subset_boulangerie_dupont():
    assert _name_match_score("BOULANGERIE DUPONT", "DUPONT") == 1.0


def test_accent_normalized():
    assert _name_match_score("Café", "CAFE") == 1.0


def test_apostrophe_split():
    assert _name_match_score("L'Atelier", "ATELIER") == 1.0


def test_no_overlap():
    assert _name_match_score("Coiffure Olivier", "ENTREPRISE BLEU") == 0.0


def test_partial_overlap():
    score = _name_match_score("salon dupont", "salon martin")
    assert 0.4 < score < 0.6


def test_empty_string():
    assert _name_match_score("", "ANYTHING") == 0.0
    assert _name_match_score("ANYTHING", "") == 0.0


def test_all_legal_forms_stripped():
    assert _name_match_score("SARL", "SAS") == 0.0


def test_known_not_fixed_token_subset_common_surname():
    # Documents expected behavior — common surname matches any name containing it.
    # NAF gate catches unrelated businesses at a later stage.
    assert _name_match_score("GIBON", "CHARLOTTE GIBON") == 1.0
