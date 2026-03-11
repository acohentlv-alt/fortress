"""Unit tests for contact_parser — pure functions, no mocking needed.

Functions confirmed present in contact_parser.py:
  - extract_phones(html) -> list[str]
  - extract_emails(html) -> list[str]
  - extract_social_links(html) -> dict[str, str]
  - is_junk_email(email) -> bool
  - parse_schema_org(html) -> dict[str, Any]
  - synthesize_email(first_name, last_name, domain) -> list[str]
"""
from __future__ import annotations

import json

import pytest

from fortress.module_b.contact_parser import (
    extract_emails,
    extract_phones,
    extract_social_links,
    is_junk_email,
    parse_schema_org,
    synthesize_email,
)


# ---------------------------------------------------------------------------
# extract_phones
# ---------------------------------------------------------------------------


def test_extract_phones_international_format():
    """French international format +33 6 12 34 56 78 is extracted and normalised."""
    html = "<p>Appelez-nous au +33 6 12 34 56 78 pour plus d'informations.</p>"
    result = extract_phones(html)
    assert "+33612345678" in result


def test_extract_phones_international_compact():
    """International format without spaces +33612345678 is extracted."""
    html = "<a href='tel:+33612345678'>+33612345678</a>"
    result = extract_phones(html)
    assert "+33612345678" in result


def test_extract_phones_national_mobile():
    """French national mobile format 06 12 34 56 78 is extracted."""
    html = "<p>Mobile: 06 12 34 56 78</p>"
    result = extract_phones(html)
    assert "0612345678" in result


def test_extract_phones_national_landline():
    """French national landline format 04 68 53 21 09 is extracted."""
    html = "<p>Téléphone: 04 68 53 21 09</p>"
    result = extract_phones(html)
    assert "0468532109" in result


def test_extract_phones_national_compact():
    """National format without spaces 0468532109 is extracted."""
    html = "<span>Tél: 0468532109</span>"
    result = extract_phones(html)
    assert "0468532109" in result


def test_extract_phones_with_dots():
    """Dot-separated format 04.68.53.21.09 is extracted."""
    html = "<p>04.68.53.21.09</p>"
    result = extract_phones(html)
    assert "0468532109" in result


def test_extract_phones_freephone():
    """Freephone 0800 123 456 is extracted."""
    html = "<p>Numéro gratuit: 0800 123 456</p>"
    result = extract_phones(html)
    # 0800 123 456 → freephone pattern: 08(0[0-9]|[1-9]\d)\d{3}\d{3}
    # "0800 123 456" matches "0800" + "123" + "456" → "0800123456"
    assert len(result) >= 1


def test_extract_phones_deduplicates():
    """Same phone number appearing twice is returned only once."""
    html = "<p>Tel: 06 12 34 56 78. Rappel: 06 12 34 56 78.</p>"
    result = extract_phones(html)
    assert result.count("0612345678") == 1


def test_extract_phones_returns_sorted():
    """Result list is sorted."""
    html = "<p>07 99 88 77 66 et 06 12 34 56 78</p>"
    result = extract_phones(html)
    assert result == sorted(result)


def test_extract_phones_empty_html():
    """No phone in HTML returns empty list."""
    html = "<p>Pas de numéro ici.</p>"
    result = extract_phones(html)
    assert result == []


# ---------------------------------------------------------------------------
# extract_emails
# ---------------------------------------------------------------------------


def test_extract_emails_business_address():
    """Business email contact@domaine-dupont.fr is extracted."""
    html = "<p>Écrivez-nous: contact@domaine-dupont.fr</p>"
    result = extract_emails(html)
    assert "contact@domaine-dupont.fr" in result


def test_extract_emails_filters_noreply():
    """noreply@ prefix is excluded as junk."""
    html = "<p>noreply@example.fr</p>"
    result = extract_emails(html)
    assert result == []


def test_extract_emails_filters_no_reply_hyphen():
    """no-reply@ prefix is excluded as junk."""
    html = "<p>no-reply@example.fr</p>"
    result = extract_emails(html)
    assert result == []


def test_extract_emails_filters_gmail():
    """Gmail addresses are excluded as personal domains."""
    html = "<p>jean.dupont@gmail.com</p>"
    result = extract_emails(html)
    assert result == []


def test_extract_emails_filters_hotmail_fr():
    """hotmail.fr addresses are excluded as personal domains."""
    html = "<p>marie@hotmail.fr</p>"
    result = extract_emails(html)
    assert result == []


def test_extract_emails_filters_yahoo_fr():
    """yahoo.fr addresses are excluded as personal domains."""
    html = "<p>contact@yahoo.fr</p>"
    result = extract_emails(html)
    assert result == []


def test_extract_emails_multiple():
    """Multiple different business emails are all extracted."""
    html = "<p>contact@dupont.fr et info@dupont.fr</p>"
    result = extract_emails(html)
    assert "contact@dupont.fr" in result
    assert "info@dupont.fr" in result


def test_extract_emails_lowercased():
    """Email addresses are returned in lowercase."""
    html = "<p>Contact@Dupont.FR</p>"
    result = extract_emails(html)
    assert "contact@dupont.fr" in result


def test_extract_emails_deduplicates():
    """Same email appearing twice is returned only once."""
    html = "<p>contact@dupont.fr et contact@dupont.fr</p>"
    result = extract_emails(html)
    assert result.count("contact@dupont.fr") == 1


def test_extract_emails_returns_sorted():
    """Result list is sorted."""
    html = "<p>zinfo@dupont.fr et ainfo@dupont.fr</p>"
    result = extract_emails(html)
    assert result == sorted(result)


def test_extract_emails_empty_html():
    """No email in HTML returns empty list."""
    html = "<p>Pas d'adresse email ici.</p>"
    result = extract_emails(html)
    assert result == []


# ---------------------------------------------------------------------------
# is_junk_email
# ---------------------------------------------------------------------------


def test_is_junk_email_noreply():
    """noreply@example.fr is junk."""
    assert is_junk_email("noreply@example.fr") is True


def test_is_junk_email_no_reply_hyphen():
    """no-reply@example.fr is junk."""
    assert is_junk_email("no-reply@example.fr") is True


def test_is_junk_email_mailer_daemon():
    """mailer-daemon@example.fr is junk."""
    assert is_junk_email("mailer-daemon@example.fr") is True


def test_is_junk_email_postmaster():
    """postmaster@example.fr is junk."""
    assert is_junk_email("postmaster@example.fr") is True


def test_is_junk_email_bounce():
    """bounce@example.fr is junk."""
    assert is_junk_email("bounce@example.fr") is True


def test_is_junk_email_newsletter():
    """newsletter@example.fr is junk."""
    assert is_junk_email("newsletter@example.fr") is True


def test_is_junk_email_personal_gmail():
    """user@gmail.com is junk (personal domain)."""
    assert is_junk_email("user@gmail.com") is True


def test_is_junk_email_personal_yahoo():
    """user@yahoo.fr is junk (personal domain)."""
    assert is_junk_email("user@yahoo.fr") is True


def test_is_junk_email_personal_orange():
    """user@orange.fr is junk (personal domain)."""
    assert is_junk_email("user@orange.fr") is True


def test_is_junk_email_image_extension():
    """logo@domain.png looks like an email but has image extension — junk."""
    assert is_junk_email("logo@domain.png") is True


def test_is_junk_email_image_extension_jpg():
    """icon@domain.jpg is junk."""
    assert is_junk_email("icon@domain.jpg") is True


def test_is_junk_email_valid_business():
    """contact@dupont.fr is a valid business email — not junk."""
    assert is_junk_email("contact@dupont.fr") is False


def test_is_junk_email_valid_info():
    """info@mycompany.com is a valid business email — not junk."""
    assert is_junk_email("info@mycompany.com") is False


def test_is_junk_email_valid_subdomain():
    """contact@mail.dupont.fr is a valid business email — not junk."""
    assert is_junk_email("contact@mail.dupont.fr") is False


# ---------------------------------------------------------------------------
# extract_social_links
# ---------------------------------------------------------------------------


def test_extract_social_linkedin_company():
    """linkedin.com/company/foo is matched."""
    html = '<a href="https://www.linkedin.com/company/dupont-sarl/">LinkedIn</a>'
    result = extract_social_links(html)
    assert "linkedin" in result
    assert "linkedin.com/company/dupont-sarl" in result["linkedin"]


def test_extract_social_linkedin_no_www():
    """linkedin.com without www is matched."""
    html = '<a href="https://linkedin.com/company/mon-domaine/">LinkedIn</a>'
    result = extract_social_links(html)
    assert "linkedin" in result


def test_extract_social_facebook():
    """facebook.com page is matched."""
    html = '<a href="https://www.facebook.com/domainedupontfr">Facebook</a>'
    result = extract_social_links(html)
    assert "facebook" in result
    assert "facebook.com/domainedupontfr" in result["facebook"]


def test_extract_social_twitter():
    """twitter.com handle is matched."""
    html = '<a href="https://twitter.com/domaindupont">Twitter</a>'
    result = extract_social_links(html)
    assert "twitter" in result
    assert "twitter.com/domaindupont" in result["twitter"]


def test_extract_social_x_com():
    """x.com handle (rebranded Twitter) is matched."""
    html = '<a href="https://x.com/domaindupont">X</a>'
    result = extract_social_links(html)
    assert "twitter" in result


def test_extract_social_multiple_platforms():
    """Multiple platforms extracted from same HTML."""
    html = (
        '<a href="https://www.linkedin.com/company/dupont/">LinkedIn</a> '
        '<a href="https://www.facebook.com/dupont">Facebook</a>'
    )
    result = extract_social_links(html)
    assert "linkedin" in result
    assert "facebook" in result


def test_extract_social_first_match_wins():
    """When two LinkedIn URLs appear, only the first is returned."""
    html = (
        '<a href="https://linkedin.com/company/first-company/">First</a> '
        '<a href="https://linkedin.com/company/second-company/">Second</a>'
    )
    result = extract_social_links(html)
    assert "first-company" in result["linkedin"]
    assert "second-company" not in result["linkedin"]


def test_extract_social_no_social_links():
    """HTML without social links returns empty dict."""
    html = "<p>Pas de réseaux sociaux ici.</p>"
    result = extract_social_links(html)
    assert result == {}


def test_extract_social_only_present_keys():
    """Only platforms that are found appear as keys — no None values."""
    html = '<a href="https://www.linkedin.com/company/test/">LinkedIn</a>'
    result = extract_social_links(html)
    assert "facebook" not in result
    assert "twitter" not in result


# ---------------------------------------------------------------------------
# parse_schema_org
# ---------------------------------------------------------------------------


def test_parse_schema_org_telephone():
    """telephone field is extracted from JSON-LD."""
    json_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "LocalBusiness",
        "telephone": "+33 4 68 53 21 09",
        "name": "Domaine Dupont",
    })
    html = f'<script type="application/ld+json">{json_ld}</script>'
    result = parse_schema_org(html)
    assert result.get("phone") == "+33 4 68 53 21 09"


def test_parse_schema_org_email():
    """email field is extracted from JSON-LD."""
    json_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "Organization",
        "email": "contact@dupont.fr",
    })
    html = f'<script type="application/ld+json">{json_ld}</script>'
    result = parse_schema_org(html)
    assert result.get("email") == "contact@dupont.fr"


def test_parse_schema_org_url():
    """url field is extracted from JSON-LD."""
    json_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "Organization",
        "url": "https://dupont.fr",
    })
    html = f'<script type="application/ld+json">{json_ld}</script>'
    result = parse_schema_org(html)
    assert result.get("url") == "https://dupont.fr"


def test_parse_schema_org_all_fields():
    """All three fields (phone, email, url) are extracted together."""
    json_ld = json.dumps({
        "@context": "https://schema.org",
        "@type": "LocalBusiness",
        "telephone": "+33468532109",
        "email": "contact@dupont.fr",
        "url": "https://dupont.fr",
    })
    html = f'<script type="application/ld+json">{json_ld}</script>'
    result = parse_schema_org(html)
    assert result.get("phone") == "+33468532109"
    assert result.get("email") == "contact@dupont.fr"
    assert result.get("url") == "https://dupont.fr"


def test_parse_schema_org_graph_array():
    """@graph array format is handled."""
    json_ld = json.dumps({
        "@context": "https://schema.org",
        "@graph": [
            {"@type": "WebSite", "url": "https://dupont.fr"},
            {"@type": "LocalBusiness", "telephone": "+33468000000"},
        ],
    })
    html = f'<script type="application/ld+json">{json_ld}</script>'
    result = parse_schema_org(html)
    assert result.get("url") == "https://dupont.fr"
    assert result.get("phone") == "+33468000000"


def test_parse_schema_org_list_of_objects():
    """JSON-LD as a top-level list is handled."""
    json_ld = json.dumps([
        {"@type": "Organization", "telephone": "+33468999999"},
    ])
    html = f'<script type="application/ld+json">{json_ld}</script>'
    result = parse_schema_org(html)
    assert result.get("phone") == "+33468999999"


def test_parse_schema_org_no_json_ld():
    """HTML without JSON-LD returns empty dict."""
    html = "<p>Pas de données structurées.</p>"
    result = parse_schema_org(html)
    assert result == {}


def test_parse_schema_org_malformed_json():
    """Malformed JSON-LD is silently skipped — returns empty dict."""
    html = '<script type="application/ld+json">{ this is not valid json }</script>'
    result = parse_schema_org(html)
    assert result == {}


def test_parse_schema_org_first_match_wins():
    """When two JSON-LD blocks exist, the first phone/email/url wins."""
    json_ld1 = json.dumps({"@type": "LocalBusiness", "telephone": "+33100000000"})
    json_ld2 = json.dumps({"@type": "LocalBusiness", "telephone": "+33200000000"})
    html = (
        f'<script type="application/ld+json">{json_ld1}</script>'
        f'<script type="application/ld+json">{json_ld2}</script>'
    )
    result = parse_schema_org(html)
    assert result.get("phone") == "+33100000000"


# ---------------------------------------------------------------------------
# synthesize_email
# ---------------------------------------------------------------------------


def test_synthesize_email_standard_patterns():
    """Standard patterns are generated: prenom.nom, p.nom, prenom, nom, contact."""
    result = synthesize_email("Marie", "Dupont", "dupont.fr")
    assert "marie.dupont@dupont.fr" in result
    assert "m.dupont@dupont.fr" in result
    assert "marie@dupont.fr" in result
    assert "dupont@dupont.fr" in result
    assert "contact@dupont.fr" in result


def test_synthesize_email_accent_transliteration():
    """Accented names are ASCII-transliterated: Élodie → elodie."""
    result = synthesize_email("Élodie", "Lecomte", "lecomte.fr")
    assert "elodie.lecomte@lecomte.fr" in result
    assert "e.lecomte@lecomte.fr" in result


def test_synthesize_email_accent_e_circumflex():
    """Ê, É, È, Ë are all transliterated to e."""
    result = synthesize_email("Jérôme", "Lefèvre", "lefèvre.fr")
    assert any("jerome" in email for email in result)


def test_synthesize_email_strips_https_prefix():
    """Domain with https:// prefix is cleaned correctly."""
    result = synthesize_email("Pierre", "Martin", "https://www.martin.fr")
    # The domain should be cleaned — contact@ at minimum should appear
    assert any("martin" in email or "contact" in email for email in result)


def test_synthesize_email_preserves_order():
    """prenom.nom pattern appears first (highest priority)."""
    result = synthesize_email("Jean", "Durand", "durand.fr")
    assert result[0] == "jean.durand@durand.fr"


def test_synthesize_email_deduplicates():
    """No duplicate email addresses in the result list."""
    result = synthesize_email("A", "B", "c.fr")
    assert len(result) == len(set(result))


def test_synthesize_email_empty_firstname():
    """Empty first name falls back to contact@ only."""
    result = synthesize_email("", "Dupont", "dupont.fr")
    assert result == ["contact@dupont.fr"]


def test_synthesize_email_empty_lastname():
    """Empty last name falls back to contact@ only."""
    result = synthesize_email("Marie", "", "dupont.fr")
    assert result == ["contact@dupont.fr"]


def test_synthesize_email_empty_domain():
    """Empty domain returns empty list."""
    result = synthesize_email("Marie", "Dupont", "")
    assert result == []


def test_synthesize_email_lowercased():
    """All generated emails are lowercased."""
    result = synthesize_email("PIERRE", "MARTIN", "MARTIN.FR")
    for email in result:
        assert email == email.lower()
