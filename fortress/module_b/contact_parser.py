"""Contact parser — pure regex extraction from HTML strings.

No HTTP calls. No browser. Purely deterministic text processing.
Used by website_crawler.py to extract contacts from fetched HTML.

Handles:
  - French phone numbers (+33, 0X formats)
  - Email addresses (with junk filtering)
  - Social media links (LinkedIn, Facebook, Twitter/X)
  - Schema.org / JSON-LD structured data (telephone, email, url)
  - Officer email synthesis (prenom.nom@domain patterns)
"""

from __future__ import annotations

import json
import re
import unicodedata
from typing import Any

# ---------------------------------------------------------------------------
# Phone regexes — French formats
# ---------------------------------------------------------------------------

# Matches: +33 6 12 34 56 78 / +33612345678 / 06 12 34 56 78 / 0612345678
# Also: 0800 123 456, 3XXX short numbers (excluded — not useful)
_PHONE_PATTERNS: list[re.Pattern[str]] = [
    # International: +33 X XX XX XX XX (with optional separators)
    re.compile(r"\+33\s?[1-9](?:[\s.\-]?\d{2}){4}"),
    # National: 0X XX XX XX XX (land + mobile)
    re.compile(r"0[1-9](?:[\s.\-]?\d{2}){4}"),
    # Free-phone: 0800 / 0806 / 0809 XXX XXX
    re.compile(r"08(?:0[0-9]|[1-9]\d)[\s.\-]?\d{3}[\s.\-]?\d{3}"),
]

_PHONE_NORMALISE_RE = re.compile(r"[\s.\-]")

# ---------------------------------------------------------------------------
# Phone validation — shared with web_search.py (avoid circular import)
# ---------------------------------------------------------------------------

# Slightly broader than _PHONE_NORMALISE_RE: also strips parentheses.
_PHONE_DIGITS_RE = re.compile(r"[\s.\-()]")

# French phone prefixes that represent useful business numbers:
# 01-05 = geographic landlines, 06-07 = mobile, 09 = VoIP/Freebox
# 08XX (premium/shared-cost) intentionally excluded.
_VALID_FRENCH_PREFIXES: frozenset[str] = frozenset(
    {"01", "02", "03", "04", "05", "06", "07", "09"}
)

# Suspicious repeating or sequential patterns — almost certainly fake.
_FAKE_PHONE_RE = re.compile(
    r"^(?:0[1-9])(\d)\1{7}$|"   # 0X followed by same digit 8 times (e.g. 0611111111)
    r"^0[1-9]12345678$|"         # sequential 12345678 after any valid prefix
    r"^0[1-9]0{8}$",            # any valid prefix + 8 zeros (placeholder pattern)
)


def _is_valid_french_phone(phone: str) -> bool:
    """Return True if `phone` looks like a real French business phone number.

    Accepts: 01-07, 09 (landlines, mobile, VoIP) — 10 digits national or +33 variant.
    Rejects: 08XX (premium/shared-cost), sequential/repeating fakes, bad lengths.
    """
    digits = _PHONE_DIGITS_RE.sub("", phone)

    # Normalise +33 → 0
    if digits.startswith("+33") and len(digits) == 12:
        digits = "0" + digits[3:]

    if not digits.startswith("0") or len(digits) != 10:
        return False

    prefix = digits[:2]
    if prefix not in _VALID_FRENCH_PREFIXES:
        return False  # 08XX rejected

    if _FAKE_PHONE_RE.match(digits):
        return False

    return True


# ---------------------------------------------------------------------------
# Email regex
# ---------------------------------------------------------------------------

_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Image extensions that look like emails but aren't
_IMAGE_EXTENSIONS = frozenset(
    {"png", "jpg", "jpeg", "gif", "svg", "webp", "bmp", "ico"}
)

# Junk sender patterns (not useful as business contacts)
_JUNK_EMAIL_PREFIXES = frozenset(
    {
        "noreply",
        "no-reply",
        "no_reply",
        "donotreply",
        "do-not-reply",
        "mailer-daemon",
        "mailer_daemon",
        "postmaster",
        "bounce",
        "bounces",
        "spam",
        "abuse",
        "unsubscribe",
        "newsletter",
        "newsletters",
        "notifications",
        "notification",
        # Privacy / legal / GDPR contacts — found on legal pages, not business contacts
        "privacy",
        "rgpd",
        "dpo",
        "legal",
        "compliance",
        "security",
        "cnil",
        "webmaster",
        # "support", "info", "contact", "admin" intentionally omitted — common valid
        # French business contact prefixes (info@domain.fr, contact@domain.fr)
    }
)

# Infrastructure / cloud / analytics providers — email addresses from these
# domains are injected by scripts and banners, not real business contacts.
_JUNK_INFRASTRUCTURE_DOMAINS = frozenset(
    {
        # Cloud / hosting
        "amazon.com",
        "amazonaws.com",
        "aws.amazon.com",
        "google.com",
        "googleapis.com",
        "googletagmanager.com",
        "cloudflare.com",
        "microsoft.com",
        "azure.com",
        "outlook.com",
        "office365.com",
        "oracle.com",
        "digitalocean.com",
        "ovh.com",
        "ovhcloud.com",
        # Social media (not direct business emails)
        "facebook.com",
        "instagram.com",
        "linkedin.com",
        "twitter.com",
        "x.com",
        "youtube.com",
        "tiktok.com",
        # Tech infrastructure
        "w3.org",
        "schema.org",
        "sentry.io",
        "gravatar.com",
        "wordpress.com",
        "wp.com",
        "github.com",
        "gitlab.com",
        "jsdelivr.net",
        "cdnjs.cloudflare.com",
        # Analytics / CRM / marketing tools
        "hotjar.com",
        "hubspot.com",
        "hubspotusercontent.com",
        "mailchimp.com",
        "sendgrid.net",
        "sendgrid.com",
        "sendinblue.com",
        "brevo.com",
        "mailjet.com",
        # French government (not company emails)
        "impots.gouv.fr",
        "service-public.fr",
        # Local directory placeholders
        "etre-visible.local.fr",
        "pagesjaunes.fr",
    }
)

# Regex to detect UUID / hex tokens in the local part of an email address.
# e.g. a3f8b2c1d4e5@... — auto-generated, not a real contact.
_UUID_LOCAL_RE = re.compile(r"^[0-9a-f]{8,}$", re.IGNORECASE)

# Free/personal email providers — not useful as business contacts
_PERSONAL_DOMAINS = frozenset(
    {
        "gmail.com",
        "hotmail.com",
        "hotmail.fr",
        "yahoo.com",
        "yahoo.fr",
        "outlook.com",
        "outlook.fr",
        "live.com",
        "live.fr",
        "laposte.net",
        "orange.fr",
        "sfr.fr",
        "free.fr",
        "bbox.fr",
        "wanadoo.fr",
        "icloud.com",
        "me.com",
        "protonmail.com",
        "pm.me",
    }
)

# ---------------------------------------------------------------------------
# Social media URL patterns
# ---------------------------------------------------------------------------

_SOCIAL_PATTERNS: dict[str, re.Pattern[str]] = {
    "linkedin": re.compile(
        r"https?://(?:www\.)?linkedin\.com/company/[a-zA-Z0-9\-_%]+/?",
        re.IGNORECASE,
    ),
    "facebook": re.compile(
        r"https?://(?:www\.)?facebook\.com/[a-zA-Z0-9.\-_%/]+",
        re.IGNORECASE,
    ),
    "twitter": re.compile(
        r"https?://(?:www\.)?(?:twitter|x)\.com/[a-zA-Z0-9_]+",
        re.IGNORECASE,
    ),
}

# ---------------------------------------------------------------------------
# Schema.org / JSON-LD pattern
# ---------------------------------------------------------------------------

_JSON_LD_RE = re.compile(
    r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def extract_phones(html: str) -> list[str]:
    """Extract and normalise French phone numbers from HTML.

    Returns deduplicated list in E.164-like format (digits only, e.g. +33612345678).
    """
    found: set[str] = set()
    for pattern in _PHONE_PATTERNS:
        for match in pattern.finditer(html):
            raw = match.group(0)
            normalised = _normalise_phone(raw)
            if normalised:
                found.add(normalised)
    return sorted(found)


def extract_emails(html: str) -> list[str]:
    """Extract business emails from HTML, excluding junk and personal domains."""
    found: set[str] = set()
    for match in _EMAIL_RE.finditer(html):
        email = match.group(0).lower()
        if not is_junk_email(email):
            found.add(email)
    return sorted(found)


def extract_social_links(html: str) -> dict[str, str]:
    """Extract first matched social media URL per platform.

    Returns dict with keys 'linkedin', 'facebook', 'twitter' (only those found).
    """
    result: dict[str, str] = {}
    for platform, pattern in _SOCIAL_PATTERNS.items():
        match = pattern.search(html)
        if match:
            result[platform] = match.group(0)
    return result


def is_junk_email(email: str) -> bool:
    """Return True if the email is not useful as a business contact.

    Filters out:
    - Image file extensions (img@domain.png)
    - Common noreply / system sender prefixes (noreply, privacy, dpo, rgpd…)
    - Infrastructure / cloud provider domains (amazon.com, google.com, …)
    - Personal email providers (gmail, hotmail, etc.)
    - UUID / hex tokens in the local part (auto-generated addresses)
    - Obviously fake addresses (local == "email", total length > 80)
    """
    email = email.lower().strip()

    # Sanity check — must be a plausible length
    if len(email) > 80:
        return True

    # Check for image extension (false positives in HTML attribute values)
    parts = email.split(".")
    if parts[-1] in _IMAGE_EXTENSIONS:
        return True

    local, _, domain = email.partition("@")
    if not domain or not local:
        return True

    # Junk prefix
    if local in _JUNK_EMAIL_PREFIXES:
        return True

    # Obviously fake: local part is "email", "test", "example", etc.
    if local in {"email", "test", "example", "sample", "demo", "user", "placeholder"}:
        return True

    # UUID / hex auto-generated local part (e.g. a3f8b2c1d4e5@...)
    if _UUID_LOCAL_RE.match(local):
        return True

    # Infrastructure / cloud provider domains
    if domain in _JUNK_INFRASTRUCTURE_DOMAINS:
        return True
    # Also match subdomains of blocked providers (e.g. aws-eu-privacy@amazon.com checked above,
    # but also sub.amazon.com)
    for blocked in _JUNK_INFRASTRUCTURE_DOMAINS:
        if domain.endswith("." + blocked):
            return True

    # Personal provider
    if domain in _PERSONAL_DOMAINS:
        return True

    return False


def parse_schema_org(html: str) -> dict[str, Any]:
    """Extract telephone, email, and url from JSON-LD structured data.

    Returns a dict with any of: {"phone": ..., "email": ..., "url": ...}
    Returns empty dict if no JSON-LD found or parsing fails.
    """
    result: dict[str, Any] = {}
    for match in _JSON_LD_RE.finditer(html):
        try:
            data = json.loads(match.group(1))
        except (json.JSONDecodeError, ValueError):
            continue

        # Handle @graph arrays
        items: list[dict[str, Any]] = []
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("@graph", [data])

        for item in items:
            if not isinstance(item, dict):
                continue
            if "telephone" in item and "phone" not in result:
                result["phone"] = str(item["telephone"])
            if "email" in item and "email" not in result:
                result["email"] = str(item["email"])
            if "url" in item and "url" not in result:
                result["url"] = str(item["url"])

    return result


def synthesize_email(
    first_name: str,
    last_name: str,
    domain: str,
) -> list[str]:
    """Generate candidate executive email addresses from officer name + domain.

    Patterns (in priority order):
      prenom.nom@domain
      p.nom@domain
      prenom@domain
      nom@domain
      contact@domain  (generic fallback — always included)

    Names are ASCII-transliterated and lowercased.
    """

    def _ascii(s: str) -> str:
        """Lowercase + remove accents."""
        nfkd = unicodedata.normalize("NFKD", s)
        return "".join(c for c in nfkd if not unicodedata.combining(c)).lower()

    fn = _ascii(first_name.strip())
    ln = _ascii(last_name.strip())
    d = domain.lower().lstrip("htps:/").lstrip("www.").rstrip("/")

    if not fn or not ln or not d:
        return [f"contact@{d}"] if d else []

    candidates = [
        f"{fn}.{ln}@{d}",
        f"{fn[0]}.{ln}@{d}",
        f"{fn}@{d}",
        f"{ln}@{d}",
        f"contact@{d}",
    ]

    # Deduplicate while preserving order
    seen: set[str] = set()
    result: list[str] = []
    for c in candidates:
        if c not in seen:
            seen.add(c)
            result.append(c)
    return result


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------


def _normalise_phone(raw: str) -> str:
    """Convert a raw French phone match to a compact format.

    Returns E.164 for international (+33...), national 10-digit for others.
    """
    digits = _PHONE_NORMALISE_RE.sub("", raw)

    if digits.startswith("+33"):
        # +33 6 12 34 56 78 → +33612345678
        return digits

    if digits.startswith("0") and len(digits) == 10:
        return digits

    if digits.startswith("33") and len(digits) == 11:
        return "+" + digits

    # Fallback — return as-is
    return digits if len(digits) >= 9 else ""
