"""Tests for card_formatter — format_card() and format_card_text().

No DB required — pure function tests using model objects.
"""
from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal

import pytest

from fortress.models import Company, CompanyStatus, Contact, ContactSource, Officer
from fortress.module_e.card_formatter import format_card, format_card_text

# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------


def _company(
    siren: str = "123456789",
    denomination: str = "SARL Dupont",
    ville: str = "Toulouse",
    departement: str = "31",
    fortress_id: int | None = None,
) -> Company:
    return Company(
        siren=siren,
        denomination=denomination,
        naf_code="62.01Z",
        naf_libelle="Programmation informatique",
        forme_juridique="SARL",
        adresse="14 Rue de la Paix",
        code_postal="31000",
        ville=ville,
        departement=departement,
        statut=CompanyStatus.ACTIVE,
        date_creation=date(2010, 6, 15),
        tranche_effectif="6-9",
        fortress_id=fortress_id,
    )


def _contact(
    siren: str = "123456789",
    phone: str | None = "0561123456",
    email: str | None = "contact@dupont.fr",
    website: str | None = "https://dupont.fr",
    social_linkedin: str | None = "linkedin.com/company/dupont",
    rating: Decimal | None = Decimal("4.2"),
) -> Contact:
    return Contact(
        siren=siren,
        phone=phone,
        email=email,
        website=website,
        social_linkedin=social_linkedin,
        rating=rating,
        source=ContactSource.WEBSITE_CRAWL,
        collected_at=datetime.now(tz=timezone.utc),
    )


def _officer(nom: str = "Dupont", prenom: str = "Marie", role: str = "Gérante") -> Officer:
    return Officer(
        siren="123456789",
        nom=nom,
        prenom=prenom,
        role=role,
        source=ContactSource.INPI,
    )


# ---------------------------------------------------------------------------
# format_card — structure and field mapping
# ---------------------------------------------------------------------------


def test_format_card_returns_dict():
    card = format_card(_company(), _contact(), [], "LOGISTIQUE 31", 1)
    assert isinstance(card, dict)


def test_format_card_siren_present():
    card = format_card(_company(siren="987654321"), _contact(siren="987654321"), [], "Q", 1)
    assert card["siren"] == "987654321"


def test_format_card_denomination():
    card = format_card(_company(denomination="TRANSPORT BIDULE SAS"), _contact(), [], "Q", 1)
    assert card["denomination"] == "TRANSPORT BIDULE SAS"


def test_format_card_contact_fields_populated():
    card = format_card(_company(), _contact(), [], "Q", 1)
    assert card["phone"] == "0561123456"
    assert card["email"] == "contact@dupont.fr"
    assert card["website_url"] == "https://dupont.fr"
    assert card["social_linkedin"] == "linkedin.com/company/dupont"
    assert card["rating"] == pytest.approx(4.2)


def test_format_card_contact_none_gives_null_fields():
    card = format_card(_company(), None, [], "Q", 1)
    assert card["phone"] is None
    assert card["email"] is None
    assert card["website_url"] is None
    assert card["social_linkedin"] is None
    assert card["rating"] is None


def test_format_card_completeness_full():
    # All 5 MVP fields present → 100%
    card = format_card(_company(), _contact(), [], "Q", 1)
    assert card["completeness_pct"] == 100


def test_format_card_completeness_zero():
    # No contact → 0%
    card = format_card(_company(), None, [], "Q", 1)
    assert card["completeness_pct"] == 0


def test_format_card_completeness_partial():
    # Only phone (1/5 MVP fields) → 20%
    c = _contact(email=None, website=None, social_linkedin=None, rating=None)
    card = format_card(_company(), c, [], "Q", 1)
    assert card["completeness_pct"] == 20


def test_format_card_officers_empty():
    card = format_card(_company(), _contact(), [], "Q", 1)
    assert card["officers"] == []


def test_format_card_officers_one():
    officers = [_officer(nom="Dupont", prenom="Marie", role="Gérante")]
    card = format_card(_company(), _contact(), officers, "Q", 1)
    assert len(card["officers"]) == 1
    assert card["officers"][0]["name"] == "Marie Dupont"
    assert card["officers"][0]["role"] == "Gérante"


def test_format_card_officers_multiple():
    officers = [
        _officer("Dupont", "Marie", "Gérante"),
        _officer("Martin", "Jean", "Associé"),
    ]
    card = format_card(_company(), _contact(), officers, "Q", 1)
    assert len(card["officers"]) == 2
    assert card["officers"][1]["name"] == "Jean Martin"


def test_format_card_officer_no_prenom():
    officers = [Officer(siren="123456789", nom="Dupont", source=ContactSource.INPI)]
    card = format_card(_company(), _contact(), officers, "Q", 1)
    assert card["officers"][0]["name"] == "Dupont"


def test_format_card_fortress_id_none():
    card = format_card(_company(fortress_id=None), _contact(), [], "Q", 1)
    assert card["fortress_id"] is None


def test_format_card_fortress_id_present():
    card = format_card(_company(fortress_id=42), _contact(), [], "Q", 1)
    assert card["fortress_id"] == "F-00042"


def test_format_card_fortress_id_large():
    card = format_card(_company(fortress_id=99999), _contact(), [], "Q", 1)
    assert card["fortress_id"] == "F-99999"


def test_format_card_card_index():
    card = format_card(_company(), _contact(), [], "Q", 7)
    assert card["card_index"] == 7


def test_format_card_query_name():
    card = format_card(_company(), _contact(), [], "AGRICULTURE 66", 1)
    assert card["query_name"] == "AGRICULTURE 66"


def test_format_card_rating_none_is_none():
    c = _contact(rating=None)
    card = format_card(_company(), c, [], "Q", 1)
    assert card["rating"] is None


def test_format_card_rating_decimal_to_float():
    c = _contact(rating=Decimal("3.7"))
    card = format_card(_company(), c, [], "Q", 1)
    assert card["rating"] == pytest.approx(3.7)
    assert isinstance(card["rating"], float)


def test_format_card_statut_string():
    card = format_card(_company(), _contact(), [], "Q", 1)
    assert isinstance(card["statut"], str)
    assert card["statut"] == "A"  # CompanyStatus.ACTIVE = "A"


# ---------------------------------------------------------------------------
# format_card_text — human-readable output
# ---------------------------------------------------------------------------


def _full_card() -> dict:
    officers = [_officer("Dupont", "Marie", "Gérante")]
    return format_card(_company(), _contact(), officers, "LOGISTIQUE 31", 1)


def test_format_card_text_contains_card_header():
    text = format_card_text(_full_card())
    assert "CARD #001" in text


def test_format_card_text_contains_company_name():
    text = format_card_text(_full_card())
    assert "SARL Dupont" in text


def test_format_card_text_contains_siren():
    text = format_card_text(_full_card())
    assert "123456789" in text


def test_format_card_text_contains_phone():
    text = format_card_text(_full_card())
    assert "0561123456" in text


def test_format_card_text_contains_email():
    text = format_card_text(_full_card())
    assert "contact@dupont.fr" in text


def test_format_card_text_contains_website():
    text = format_card_text(_full_card())
    assert "https://dupont.fr" in text


def test_format_card_text_contains_officer():
    text = format_card_text(_full_card())
    assert "Marie Dupont" in text


def test_format_card_text_missing_phone_shows_pending():
    c = _contact(phone=None)
    officers: list[Officer] = []
    card = format_card(_company(), c, officers, "Q", 1)
    text = format_card_text(card)
    assert "— (pending)" in text


def test_format_card_text_missing_officer_shows_pending():
    card = format_card(_company(), _contact(), [], "Q", 1)
    text = format_card_text(card)
    assert "— (pending)" in text


def test_format_card_text_separator_lines():
    text = format_card_text(_full_card())
    assert "═" in text
    assert "─" in text


def test_format_card_text_returns_string():
    text = format_card_text(_full_card())
    assert isinstance(text, str)
    assert len(text) > 100  # sanity: non-trivial output
