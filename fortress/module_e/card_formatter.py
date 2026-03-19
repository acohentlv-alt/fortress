"""Card formatter — renders Company + Contact + Officers into structured dicts
and human-readable text cards.

Text card format (from CLAUDE.md):
    ═══════════════════════════════════════════════════════════════
      CARD #001 | Fortress ID: F-00042 | Query: AGRICULTURE 66
    ═══════════════════════════════════════════════════════════════
      Company:        Domaine Viticole Dupont SARL
      SIREN:          123456789
      SIRET:          12345678900012
      NAF Code:       01.21Z (Viticulture)
      Status:         Active
      Established:    2008-04-15
    ───────────────────────────────────────────────────────────────
      Address:        14 Chemin des Vignes, 66300 Thuir
      Department:     66 — Pyrénées-Orientales
    ───────────────────────────────────────────────────────────────
      Officers:       Marie Dupont (Gérante)
                      Jean Dupont (Associé)
    ───────────────────────────────────────────────────────────────
      Phone:          +33 4 68 53 XX XX
      Website:        https://domaine-dupont.fr
      Email:          contact@domaine-dupont.fr
      LinkedIn:       linkedin.com/company/domaine-dupont
      Rating:         4.2 ★ (Google Maps)
    ───────────────────────────────────────────────────────────────
      Revenue:        500K-1M EUR
      Workforce:      6-9 employees
      Completeness:   89%
    ═══════════════════════════════════════════════════════════════

Completeness = filled MVP fields / total MVP fields * 100
MVP fields: phone, email, website (Contact.website), social_linkedin, rating

Model field notes (as of current models.py):
    - Contact.website   — URL field (NOT website_url)
    - Contact.email     — generic email
    - Contact.rating    — Decimal | None (Google Maps rating)
    - Officer.nom       — surname (always present)
    - Officer.prenom    — given name (optional)
    - Officer.role      — role / title (optional, NOT qualite)
    - Company.fortress_id — int | None (rendered as F-XXXXX)
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from fortress.models import Company, Contact, Officer

# MVP fields used for completeness calculation.
# These correspond to Contact model attribute names EXCEPT 'website'
# which maps to Contact.website (not website_url).
_MVP_FIELDS = ("phone", "email", "website", "social_linkedin", "rating")

_CARD_WIDTH = 63


def format_card(
    company: "Company",
    contact: "Contact | None",
    officers: "list[Officer]",
    batch_name: str,
    card_index: int,
) -> dict:
    """Return a card dict suitable for JSONL output.

    The dict contains all available fields. Missing fields are represented
    as None (not omitted) so downstream code can distinguish missing vs absent.

    The output dict uses the stable key 'website_url' regardless of the
    Contact model's internal attribute name ('website'), so the card API
    is consistent across the codebase.
    """
    # Gather contact fields — Contact.website is the URL field.
    phone = getattr(contact, "phone", None) if contact else None
    email = getattr(contact, "email", None) if contact else None
    website_url = getattr(contact, "website", None) if contact else None  # Contact.website → card 'website_url'
    social_linkedin = getattr(contact, "social_linkedin", None) if contact else None
    rating = getattr(contact, "rating", None) if contact else None

    # Completeness: count how many MVP fields have a truthy value.
    mvp_values = [phone, email, website_url, social_linkedin, rating]
    filled = sum(1 for v in mvp_values if v)
    completeness_pct = round(filled / len(_MVP_FIELDS) * 100)

    # Build officer list.
    # Officer model: nom (required str), prenom (optional), role (optional).
    # There is no nom_complet field — construct full name from prenom + nom.
    officer_list: list[dict] = []
    for o in officers:
        prenom = getattr(o, "prenom", None) or ""
        nom = getattr(o, "nom", "") or ""
        full_name = f"{prenom} {nom}".strip() if prenom else nom
        role = getattr(o, "role", None) or ""
        officer_list.append({"name": full_name, "role": role})

    # Format fortress_id as "F-XXXXX" string if present, else None.
    fortress_id_raw = getattr(company, "fortress_id", None)
    fortress_id = f"F-{fortress_id_raw:05d}" if fortress_id_raw is not None else None

    return {
        "card_index": card_index,
        "fortress_id": fortress_id,
        "batch_name": batch_name,
        # Company identity
        "siren": company.siren,
        "siret": getattr(company, "siret_siege", None),
        "denomination": company.denomination,
        "naf_code": company.naf_code,
        "naf_libelle": getattr(company, "naf_libelle", None),
        "forme_juridique": getattr(company, "forme_juridique", None),
        # statut is a CompanyStatus StrEnum — convert to plain string for JSON
        "statut": str(company.statut) if company.statut else None,
        "date_creation": str(company.date_creation) if getattr(company, "date_creation", None) else None,
        # Location
        "adresse": company.adresse,
        "code_postal": company.code_postal,
        "ville": company.ville,
        "departement": company.departement,
        "region": getattr(company, "region", None),
        # Officers
        "officers": officer_list,
        # Contact — note: card key is 'website_url', source field is Contact.website
        "phone": phone,
        "website_url": website_url,
        "email": email,
        "social_linkedin": social_linkedin,
        "rating": float(rating) if rating is not None else None,  # Decimal → float for JSON
        # Business info
        "tranche_effectif": getattr(company, "tranche_effectif", None),
        # Completeness score (0-100)
        "completeness_pct": completeness_pct,
    }


def format_card_text(card: dict) -> str:
    """Render a card dict as the human-readable text block format.

    Missing fields render as '— (pending)' rather than blank, per CLAUDE.md rules.
    """
    sep = "═" * _CARD_WIDTH
    thin = "─" * _CARD_WIDTH

    fortress_id = card.get("fortress_id") or "—"
    card_num = f"#{card.get('card_index', 0):03d}"
    query = card.get("batch_name", "—")

    naf_label = card.get("naf_code") or "—"
    if card.get("naf_libelle"):
        naf_label = f"{naf_label} ({card['naf_libelle']})"

    lines: list[str] = [
        sep,
        f"  CARD {card_num} | Fortress ID: {fortress_id} | Query: {query}",
        sep,
        f"  {'Company:':<16}{card.get('denomination') or '—'}",
        f"  {'SIREN:':<16}{card.get('siren') or '—'}",
        f"  {'SIRET:':<16}{card.get('siret') or '—'}",
        f"  {'NAF Code:':<16}{naf_label}",
        f"  {'Status:':<16}{card.get('statut') or '—'}",
        f"  {'Established:':<16}{card.get('date_creation') or '—'}",
        thin,
    ]

    # Address block
    addr_parts = [
        p for p in [card.get("adresse"), card.get("code_postal"), card.get("ville")]
        if p
    ]
    lines.append(f"  {'Address:':<16}{', '.join(addr_parts) or '—'}")
    dept = card.get("departement")
    lines.append(f"  {'Department:':<16}{dept or '—'}")
    lines.append(thin)

    # Officers block — first on same line as label, rest indented
    officers = card.get("officers") or []
    if officers:
        first = officers[0]
        role_suffix = f" ({first['role']})" if first.get("role") else ""
        lines.append(f"  {'Officers:':<16}{first['name']}{role_suffix}")
        for o in officers[1:]:
            role_suffix = f" ({o['role']})" if o.get("role") else ""
            lines.append(f"  {'':<16}{o['name']}{role_suffix}")
    else:
        lines.append(f"  {'Officers:':<16}— (pending)")
    lines.append(thin)

    # Contact block
    rating = card.get("rating")
    rating_str = f"{rating} ★ (Google Maps)" if rating is not None else "— (pending)"

    lines.append(f"  {'Phone:':<16}{card.get('phone') or '— (pending)'}")
    lines.append(f"  {'Website:':<16}{card.get('website_url') or '— (pending)'}")
    lines.append(f"  {'Email:':<16}{card.get('email') or '— (pending)'}")
    lines.append(f"  {'LinkedIn:':<16}{card.get('social_linkedin') or '— (pending)'}")
    lines.append(f"  {'Rating:':<16}{rating_str}")
    lines.append(thin)

    # Business info
    lines.append(f"  {'Workforce:':<16}{card.get('tranche_effectif') or '—'}")
    lines.append(f"  {'Completeness:':<16}{card.get('completeness_pct', 0)}%")
    lines.append(sep)

    return "\n".join(lines)
