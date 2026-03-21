import pytest
from fortress.module_d.enricher import (
    _normalize_name, _names_match, _geo_matches, _best_phone, _best_email
)
from fortress.models import Company

def test_normalize_name():
    assert _normalize_name("H CONVOYAGE SARL") == ["h", "convoyage"]
    assert _normalize_name("BAILLOEUIL") == ["bailloeuil"]
    assert _normalize_name("A T N") == ["a", "t", "n"]
    assert _normalize_name("S.A.S. LUXURY") == ["luxury"]

def test_names_match():
    # Containment
    assert _names_match("Bailloeuil Perpignan", "BAILLOEUIL") is True
    # Acronym
    assert _names_match("ATN Transport", "A T N") is True
    # Overlap
    assert _names_match("Garage du Centre", "Garage Centre") is True
    # Single token guard
    assert _names_match("Restaurant Chez Taxi", "TAXI") is False
    assert _names_match("TAXI", "TAXI") is True

def test_geo_matches():
    mock_company = Company(siren="123", denomination="Test", adresse="...", code_postal="66000", departement="66", ville="Perpignan")
    
    # Exact postal
    assert _geo_matches("10 Rue de la Paix, 66000 Perpignan", mock_company) is True
    # Exact city
    assert _geo_matches("Perpignan, France", mock_company) is True
    # Dept match
    assert _geo_matches("66100 Perpignan", mock_company) is True
    # Mismatch
    assert _geo_matches("75001 Paris", mock_company) is False

def test_best_phone():
    # Priority: Geographic match (04 for 66) > Other landline > Mobile > VoIP
    phones = ["06 12 34 56 78", "04 68 12 34 56", "01 23 45 67 89", "09 87 65 43 21"]
    
    # Dept 66 -> 04
    assert _best_phone(phones, "123", departement="66") == "04 68 12 34 56"
    
    # No dept -> any landline
    assert _best_phone(["06 12 34 56 78", "01 23 45 67 89"], "123") == "01 23 45 67 89"

def test_best_email():
    emails = ["john@gmail.com", "contact@bailloeuil.fr", "info@bailloeuil.fr", "junk@spam.com"]
    website = "https://www.bailloeuil.fr"
    
    # Prefers contact@ with same domain
    assert _best_email(emails, website, "123") == "contact@bailloeuil.fr"
    
    # Fallback to info@
    assert _best_email(["info@bailloeuil.fr", "john@gmail.com"], website, "123") == "info@bailloeuil.fr"
    
    # Personal email with name match (BAILLOEUIL)
    assert _best_email(["bailloeuil@gmail.com", "random@gmail.com"], None, "123", "BAILLOEUIL") == "bailloeuil@gmail.com"
