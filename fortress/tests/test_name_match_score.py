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


from fortress.discovery import _validate_inpi_step0_hit


def test_step0_validation_paris_strict_postal_pass():
    assert _validate_inpi_step0_hit(
        maps_cp="75011", departement="75",
        meaningful_terms=["laia", "voltaire"],
        local_denom="Q & P VOLTAIRE", local_enseigne="LAIA",
        local_cp="75011", local_dept="75",
    ) is True


def test_step0_validation_paris_cross_arrondissement_rejected():
    assert _validate_inpi_step0_hit(
        maps_cp="75011", departement="75",
        meaningful_terms=["alluma"],
        local_denom="SAGEM ALLUMAGE", local_enseigne="",
        local_cp="75016", local_dept="75",
    ) is False


def test_step0_validation_non_dense_urban_dept_sufficient():
    assert _validate_inpi_step0_hit(
        maps_cp="66300", departement="66",
        meaningful_terms=["cedric", "busuttil"],
        local_denom="CEDRIC BUSUTTIL", local_enseigne="",
        local_cp="66310", local_dept="66",
    ) is True


def test_step0_validation_industry_word_only_overlap_rejected():
    assert _validate_inpi_step0_hit(
        maps_cp="69001", departement="69",
        meaningful_terms=["dupont"],
        local_denom="RESTAURANT XYZ", local_enseigne="",
        local_cp="69001", local_dept="69",
    ) is False


def test_step0_validation_good_overlap_accepts():
    assert _validate_inpi_step0_hit(
        maps_cp="69001", departement="69",
        meaningful_terms=["roberto", "stari"],
        local_denom="GESTION2COIFFURE", local_enseigne="ROBERTO STARI",
        local_cp="69003", local_dept="69",
    ) is True
