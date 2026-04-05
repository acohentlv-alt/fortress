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
from urllib.parse import urlparse

import structlog

log = structlog.get_logger("fortress.matching.contacts")

# ---------------------------------------------------------------------------
# Phone regexes — French formats
# ---------------------------------------------------------------------------

# Matches: +33 6 12 34 56 78 / +33612345678 / 06 12 34 56 78 / 0612345678
# Also: 0800 123 456, 3XXX short numbers (excluded — not useful)
_PHONE_PATTERNS: list[re.Pattern[str]] = [
    # International with (0): +33 (0)4.68.68.19.33 — very common on French corporate sites
    re.compile(r"\+33\s?\(0\)\s?[1-9](?:[\s.\-]?\d{2}){4}"),
    # International: +33 X XX XX XX XX (with optional separators)
    re.compile(r"\+33\s?[1-9](?:[\s.\-]?\d{2}){4}"),
    # National: 0X XX XX XX XX (land + mobile)
    re.compile(r"0[1-9](?:[\s.\-]?\d{2}){4}"),
    # Free-phone: 0800 / 0806 / 0809 XXX XXX
    re.compile(r"08(?:0[0-9]|[1-9]\d)[\s.\-]?\d{3}[\s.\-]?\d{3}"),
]

_PHONE_NORMALISE_RE = re.compile(r"[\s.\-()]")  # also strip parentheses from +33(0) format

# ---------------------------------------------------------------------------
# Phone validation
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

# Free/personal email providers — blocked by default, but allowed if the
# local part references the company name (many French SMBs use Gmail).
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

# Known web agency / tech provider domains — emails from these are never
# business contacts; they're injected in footers/code by site builders.
_WEB_AGENCY_DOMAINS = frozenset(
    {
        "geek-tonic.com",
        "webedia.fr",
        "webmaster.com",
        "wix.com",
        "squarespace.com",
        "jimdo.com",
        "ionos.fr",
        "1and1.fr",
        "amen.fr",
        "gandi.net",
        "lws.fr",
        "o2switch.fr",
        "planethoster.com",
        "infomaniak.com",
        "ex2.com",
        "rezo-actif.fr",
        "sitew.com",
        "e-monsite.com",
        "webnode.fr",
        "strikingly.com",
        "weebly.com",
        "duda.co",
        "elegantthemes.com",
        "developer.mozilla.org",
        "cookiebot.com",
        "axeptio.eu",
        "didomi.io",
        "osano.com",
        "onetrust.com",
        "trustcommander.net",
    }
)

# ---------------------------------------------------------------------------
# Social media URL patterns
# ---------------------------------------------------------------------------

_SOCIAL_PATTERNS: dict[str, re.Pattern[str]] = {
    "linkedin": re.compile(
        # Match both /company/ pages AND /in/ personal profiles
        r"https?://(?:www\.)?linkedin\.com/(?:company|in)/[a-zA-Z0-9\-_%]+/?",
        re.IGNORECASE,
    ),
    "facebook": re.compile(
        # Reject generic/utility Facebook URLs (profile.php, sharer.php, etc.)
        r"https?://(?:www\.)?facebook\.com/(?!profile\.php|sharer|share\.php|login|dialog|hashtag|watch|groups/|events/|marketplace|gaming|help)[a-zA-Z0-9.\-_%/]+",
        re.IGNORECASE,
    ),
    "twitter": re.compile(
        # Must be a real profile handle, NOT /intent, /share, /hashtag, /i, /search etc.
        r'https?://(?:www\.)?(?:twitter|x)\.com/(?!intent|share|hashtag|search|i/|home|explore|login|signup|privacy|tos)[a-zA-Z0-9_]{1,15}(?:/?)(?=["\'\s<>])',
        re.IGNORECASE,
    ),
    "instagram": re.compile(
        # Instagram company/brand profiles (exclude /p/, /reel/, /explore/, /accounts/)
        r"https?://(?:www\.)?instagram\.com/(?!p/|reel/|explore/|accounts/|about/|legal/)[a-zA-Z0-9_.]{1,30}/?",
        re.IGNORECASE,
    ),
    "tiktok": re.compile(
        r"https?://(?:www\.)?tiktok\.com/@[a-zA-Z0-9_.]{1,24}/?",
        re.IGNORECASE,
    ),
    "google_maps": re.compile(
        r"https?://(?:www\.)?google\.com/maps/place/[^\s\"'<>]+",
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
    """Extract business emails from HTML, excluding junk and personal domains.
    
    Also decodes JavaScript-obfuscated emails (ml/mi char-code pattern).
    """
    found: set[str] = set()
    # Standard regex extraction
    for match in _EMAIL_RE.finditer(html):
        email = match.group(0).lower()
        if not is_junk_email(email):
            found.add(email)
    # Decode JS-obfuscated emails (common WordPress email protection)
    for email in _decode_js_emails(html):
        if not is_junk_email(email):
            found.add(email)
    return sorted(found)


def _decode_js_emails(html: str) -> list[str]:
    """Decode emails hidden by JavaScript ml/mi char-code obfuscation.
    
    Pattern: var ml="...",mi="..."; o+= ml.charAt(mi.charCodeAt(j)-48)
    This is used by WordPress email protection plugins (e.g. Email Encoder Bundle).
    The decoded output is URL-encoded HTML, which may contain mailto: links.
    """
    decoded_emails: list[str] = []
    # Find all ml/mi pairs in the HTML
    for m in re.finditer(
        r'var\s+ml\s*=\s*"([^"]+)"\s*,\s*mi\s*=\s*"([^"]+)"',
        html,
    ):
        ml, mi = m.group(1), m.group(2)
        try:
            import urllib.parse
            o = ""
            for j in range(len(mi)):
                idx = ord(mi[j]) - 48
                if 0 <= idx < len(ml):
                    o += ml[idx]
            decoded = urllib.parse.unquote(o)
            # Extract email addresses from the decoded HTML
            for em in _EMAIL_RE.finditer(decoded):
                decoded_emails.append(em.group(0).lower())
        except Exception:
            continue
    return decoded_emails


def extract_social_links(html: str) -> dict[str, str]:
    """Extract first matched social media URL per platform.

    Returns dict with keys 'linkedin', 'facebook', 'twitter', 'instagram',
    'tiktok' (only those found).
    """
    result: dict[str, str] = {}
    for platform, pattern in _SOCIAL_PATTERNS.items():
        match = pattern.search(html)
        if match:
            result[platform] = match.group(0)
    return result


# ---------------------------------------------------------------------------
# SIRET / SIREN extraction
# ---------------------------------------------------------------------------

_SIRET_RE = re.compile(
    r"(?:SIRET|siret)\s*:?\s*(\d{3}\s?\d{3}\s?\d{3}\s?\d{5})",
)
_SIREN_RE = re.compile(
    r"(?:SIREN|siren)\s*:?\s*(\d{3}\s?\d{3}\s?\d{3})(?!\d)",
)


def extract_siret(html: str) -> str | None:
    """Extract SIREN (9 digits) from SIRET or SIREN mentions in HTML.

    If a 14-digit SIRET is found, returns the first 9 digits (the SIREN).
    Returns None if nothing found.
    """
    # Try SIRET first (more specific, 14 digits)
    m = _SIRET_RE.search(html)
    if m:
        digits = re.sub(r"\s", "", m.group(1))
        if len(digits) == 14:
            return digits[:9]  # SIREN = first 9 of SIRET

    # Fallback: look for explicit SIREN mention (9 digits)
    m = _SIREN_RE.search(html)
    if m:
        digits = re.sub(r"\s", "", m.group(1))
        if len(digits) == 9:
            return digits

    return None


# ---------------------------------------------------------------------------
# SIREN extraction from HTML — used by crawl.py
# ---------------------------------------------------------------------------

_SIREN_CONTEXT_RE = re.compile(
    r'(?:SIREN|RCS|immatricul|enregistr|n[°o]\s*d.immatricul)[^0-9]{0,40}(\d{3}[\s\u00a0]?\d{3}[\s\u00a0]?\d{3})',
    re.IGNORECASE,
)

# SIRET is 14 digits — first 9 are the SIREN
_SIRET_14_RE = re.compile(
    r'(?:SIRET)[^0-9]{0,20}(\d{3}[\s\u00a0]?\d{3}[\s\u00a0]?\d{3}[\s\u00a0]?\d{5})',
    re.IGNORECASE,
)

# Footer pattern: SIREN near end of page or inside <footer> tag
_FOOTER_SIREN_RE = re.compile(
    r'(?:SIREN|RCS|SIRET|N°\s*TVA)[^0-9]{0,30}(\d{3}[\s\u00a0\-]?\d{3}[\s\u00a0\-]?\d{3})',
    re.IGNORECASE,
)


def extract_siren_from_html(html: str) -> str | None:
    """Extract SIREN from an HTML page using 4 strategies.

    Used by crawl.py after fetching website pages.
    More thorough than extract_siret() — uses context patterns and footer analysis.
    """
    if not html:
        return None

    # Strategy 1: Contextual match anywhere (SIREN/RCS/SIRET + digits)
    match = _SIREN_CONTEXT_RE.search(html)
    if match:
        raw = match.group(1).replace(" ", "").replace("\u00a0", "").replace("-", "")
        if len(raw) == 9 and raw.isdigit() and raw != "000000000":
            return raw

    # Strategy 2: SIRET (14 digits) — first 9 are the SIREN
    siret_match = _SIRET_14_RE.search(html)
    if siret_match:
        raw = siret_match.group(1).replace(" ", "").replace("\u00a0", "").replace("-", "")
        if len(raw) == 14 and raw.isdigit():
            siren = raw[:9]
            if siren != "000000000":
                return siren

    # Strategy 3: Check the footer area (last 25% of page)
    footer_start = len(html) * 3 // 4
    footer_html = html[footer_start:]
    footer_match = _FOOTER_SIREN_RE.search(footer_html)
    if footer_match:
        raw = footer_match.group(1).replace(" ", "").replace("\u00a0", "").replace("-", "")
        if len(raw) == 9 and raw.isdigit() and raw != "000000000":
            return raw

    # Strategy 4: Check inside <footer> tag if present
    footer_tag = re.search(r'<footer[^>]*>(.*?)</footer>', html, re.DOTALL | re.IGNORECASE)
    if footer_tag:
        ft_match = _FOOTER_SIREN_RE.search(footer_tag.group(1))
        if ft_match:
            raw = ft_match.group(1).replace(" ", "").replace("\u00a0", "").replace("-", "")
            if len(raw) == 9 and raw.isdigit() and raw != "000000000":
                return raw

    return None


def is_junk_email(email: str) -> bool:
    """Return True if the email is not useful as a business contact.

    Filters out:
    - Image file extensions (img@domain.png)
    - Common noreply / system sender prefixes (noreply, privacy, dpo, rgpd…)
    - Infrastructure / cloud provider domains (amazon.com, google.com, …)
    - Web agency / site builder domains (geek-tonic.com, wix.com, …)
    - UUID / hex tokens in the local part (auto-generated addresses)
    - Obviously fake addresses (local == "email", total length > 80)
    NOTE: Personal domains (gmail.com etc.) are NO LONGER rejected here.
    They are handled by is_personal_email() with company-name context.
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
    # Also match subdomains of blocked providers (e.g. sub.amazon.com)
    for blocked in _JUNK_INFRASTRUCTURE_DOMAINS:
        if domain.endswith("." + blocked):
            return True

    # Web agency / site builder domains
    if domain in _WEB_AGENCY_DOMAINS:
        return True
    for blocked in _WEB_AGENCY_DOMAINS:
        if domain.endswith("." + blocked):
            return True

    # Personal provider — NOT rejected here anymore.
    # Use is_personal_email() for context-aware filtering.

    return False


def is_personal_email(email: str, company_name: str | None = None) -> bool:
    """Return True if this is a personal-domain email with no company relation.

    If company_name is provided, allows personal-domain emails whose local part
    contains words from the company name (e.g. leparadismedoc@gmail.com for
    'Camping Le Paradis du Médoc').

    If company_name is None, rejects all personal-domain emails (legacy behavior).
    """
    email = email.lower().strip()
    _, _, domain = email.partition("@")
    if domain not in _PERSONAL_DOMAINS:
        return False  # Not a personal domain at all

    # It IS a personal domain. Check if the local part references the company.
    if not company_name:
        return True  # No context → reject

    local = email.split("@")[0]
    # Normalize: remove accents, lowercase, strip separators
    local_clean = re.sub(r"[^a-z0-9]", "", unicodedata.normalize("NFKD", local))
    local_clean = "".join(c for c in local_clean if not unicodedata.combining(c))

    name_clean = re.sub(r"[^a-z0-9]", "", unicodedata.normalize("NFKD", company_name.lower()))
    name_clean = "".join(c for c in name_clean if not unicodedata.combining(c))

    # Extract meaningful words (>= 4 chars) from the company name
    name_words = [w for w in re.split(r"[^a-z0-9]+", company_name.lower()) if len(w) >= 4]
    name_words_clean = []
    for w in name_words:
        wc = re.sub(r"[^a-z0-9]", "", unicodedata.normalize("NFKD", w))
        wc = "".join(c for c in wc if not unicodedata.combining(c))
        if len(wc) >= 4:
            name_words_clean.append(wc)

    # Check if any significant company word appears in the local part
    for word in name_words_clean:
        if word in local_clean:
            return False  # Looks like a business Gmail → allow it

    # Also check if the local part is a substantial substring of the company name
    if len(local_clean) >= 5 and local_clean in name_clean:
        return False

    return True  # Generic personal email (e.g. jean.dupont@gmail.com)


def is_agency_email(email: str, company_website: str | None = None) -> bool:
    """Return True if this email likely belongs to a web agency, not the company.

    Detects domain mismatch: if the company website is transport-medina.com and
    the email is contact@anthedesign.fr, the domains don't match → it's the web
    developer's contact, not the business contact.

    Personal domains (gmail, outlook, etc.) are NOT flagged — businesses
    sometimes legitimately use personal emails.
    """
    if not email or not company_website:
        return False

    email = email.lower().strip()
    _, _, email_domain = email.partition("@")
    if not email_domain:
        return False

    # Personal providers are allowed (handled by is_personal_email separately)
    if email_domain in _PERSONAL_DOMAINS:
        return False

    # Already flagged as junk infrastructure or known agency
    if is_junk_email(email):
        return True

    # Extract the company website's root domain
    try:
        from urllib.parse import urlparse
        parsed = urlparse(company_website if "://" in company_website else f"https://{company_website}")
        site_host = parsed.hostname or ""
        # Get root domain: "www.transport-medina.com" → "transport-medina.com"
        site_parts = site_host.split(".")
        if len(site_parts) >= 2:
            site_root = ".".join(site_parts[-2:])
        else:
            site_root = site_host
    except Exception:
        return False  # Can't parse website, allow the email

    # Get root domain from email: "contact@anthedesign.fr" → "anthedesign.fr"
    email_parts = email_domain.split(".")
    if len(email_parts) >= 2:
        email_root = ".".join(email_parts[-2:])
    else:
        email_root = email_domain

    # If the email's root domain matches the website's root domain → it's the business
    if email_root == site_root:
        return False

    # Domains don't match — this email is likely from the web developer/agency
    return True


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


# ---------------------------------------------------------------------------
# Mentions Légales structured parser
# ---------------------------------------------------------------------------

# French legal pages (LCEN Article 6) must list specific information.
# This parser understands the structure and extracts director information
# that the generic email/phone regex would miss or misattribute.

# Director role keywords — proximity matching
_DIRECTOR_KEYWORDS = re.compile(
    r"directeur\s+de\s+(?:la\s+)?publication"
    r"|responsable\s+de\s+(?:la\s+)?publication"
    r"|responsable\s+(?:de\s+)?(?:la\s+)?rédaction"
    r"|gérant"
    r"|gérante"
    r"|représentant\s+légal"
    r"|représentante\s+légale"
    r"|président(?:e)?"
    r"|directeur\s+général"
    r"|directrice\s+général"
    r"|fondateur"
    r"|fondatrice",
    re.IGNORECASE,
)

# Hébergeur section — everything after these keywords is about the hosting provider
_HEBERGEUR_KEYWORDS = re.compile(
    r"h[ée]bergeur\s+du\s+site"
    r"|h[ée]bergement\s+du\s+site"
    r"|h[ée]bergement\s+web"
    r"|prestataire\s+d[''']h[ée]bergement"
    r"|soci[ée]t[ée]\s+d[''']h[ée]bergement"
    r"|stockage\s+direct\s+et\s+permanent"
    r"|hosting\s+provider"
    r"|site\s+h[ée]berg[ée]"
    r"|h[ée]berg[ée]\s+par"
    r"|h[ée]bergeur\s*:"
    r"|h[ée]bergement\s*:",
    re.IGNORECASE,
)

# Hébergeur company names (in addition to domain blacklist)
_HEBERGEUR_NAMES = frozenset({
    "ovh", "o2switch", "gandi", "online", "scaleway", "amazon",
    "google cloud", "microsoft azure", "ionos", "lws", "infomaniak",
    "planethoster", "hostinger", "cloudflare", "digitalocean",
    "vercel", "netlify", "heroku",
})

# Employee count patterns
_EFFECTIF_RE = re.compile(
    r"(\d+)\s*(?:salariés?|employés?|collaborateurs?|personnes)"
    r"|effectif\s*(?:de\s*)?:?\s*(\d+)"
    r"|(\d+)\s+(?:ETP|équivalents?\s+temps\s+plein)",
    re.IGNORECASE,
)

# French name pattern: 3 alternatives covering mixed-case, title case, and ALL CAPS
_NAME_RE = re.compile(
    r"(?:M(?:me|r|\.)?\.?\s+)?([A-ZÀ-Ÿ][a-zà-ÿ]+(?:-[A-ZÀ-Ÿ][a-zà-ÿ]+)?\s+[A-ZÀ-Ÿ]{2,}(?:\s+[A-ZÀ-Ÿ]{2,})?)"
    r"|(?:M(?:me|r|\.)?\.?\s+)?([A-ZÀ-Ÿ][a-zà-ÿ]+(?:\s+[A-ZÀ-Ÿ][a-zà-ÿ]+){0,2})"
    r"|([A-ZÀ-Ÿ]{2,}(?:\s+[A-ZÀ-Ÿ]{2,}){0,2})"
)

# SIREN/SIRET patterns for cross-validation
_SIREN_RE = re.compile(r"\b(\d{3}\s?\d{3}\s?\d{3})\b")
_SIRET_RE = re.compile(r"\b(\d{3}\s?\d{3}\s?\d{3}\s?\d{5})\b")


def extract_mentions_legales(
    html: str,
    *,
    company_siren: str | None = None,
    website_domain: str | None = None,
) -> dict[str, Any]:
    """Extract structured data from a French mentions-légales page.

    Goes beyond generic regex by understanding the legal page structure:
    1. Splits page into sections (before/after hébergeur)
    2. Finds director name near role keywords
    3. Extracts director email (relaxed domain matching)
    4. Extracts employee count

    Args:
        html: Raw HTML of the mentions-légales page.
        company_siren: Optional SIREN for cross-validation.
        website_domain: Optional domain of company website (e.g. "company.fr").

    Returns:
        Dict with keys: director_name, director_email, director_role,
                        director_civilite, effectif, siren_match.
        All values may be None.
    """
    result: dict[str, Any] = {
        "director_name": None,
        "director_email": None,
        "director_role": None,
        "director_civilite": None,
        "effectif": None,
        "siren_match": None,  # True/False/None
    }

    # Strip HTML tags for text analysis
    text = re.sub(r"<[^>]+>", " ", html)
    text = re.sub(r"\s+", " ", text).strip()

    # ── Split at hébergeur section ─────────────────────────────────
    # Everything before the hébergeur section is company data.
    # Everything after is hosting provider data — skip it.
    hebergeur_match = _HEBERGEUR_KEYWORDS.search(text)
    company_section = text[:hebergeur_match.start()] if hebergeur_match else text

    # ── SIREN cross-validation ─────────────────────────────────────
    if company_siren:
        siren_clean = company_siren.replace(" ", "")
        for m in _SIREN_RE.finditer(company_section):
            found_siren = m.group(1).replace(" ", "")
            if found_siren == siren_clean:
                result["siren_match"] = True
                break
            elif len(found_siren) == 9 and found_siren != siren_clean:
                # Found a different SIREN — might be wrong site
                result["siren_match"] = False

    # ── Director name + role ───────────────────────────────────────
    for role_match in _DIRECTOR_KEYWORDS.finditer(company_section):
        role_text = role_match.group(0).strip()
        # Look for a name within 150 characters after the keyword
        context = company_section[role_match.start():role_match.end() + 150]

        # Determine civilité
        civilite = None
        context_lower = context.lower()
        if any(w in context_lower for w in ("mme", "madame", "gérante", "directrice",
                                             "représentante", "présidente", "fondatrice")):
            civilite = "Mme"
        elif any(w in context_lower for w in ("m.", "monsieur", "gérant ", "directeur",
                                               "représentant ", "président ", "fondateur")):
            civilite = "M."

        # Find name after the role keyword
        after_keyword = company_section[role_match.end():role_match.end() + 150]
        # Clean up separators: "Directeur de la publication : Jean Dupont"
        after_keyword = re.sub(r"^[\s:–—\-]+", "", after_keyword)

        name_match = _NAME_RE.search(after_keyword)
        if name_match:
            name = (name_match.group(1) or name_match.group(2) or name_match.group(3) or "").strip()
            # Reject if name matches a known hébergeur
            if name and name.lower() not in _HEBERGEUR_NAMES and len(name) > 3:
                result["director_name"] = name
                result["director_role"] = role_text.title()
                result["director_civilite"] = civilite
                break

    # ── Director email ─────────────────────────────────────────────
    # Extract all emails from the company section (before hébergeur)
    all_emails = _EMAIL_RE.findall(company_section)

    for email in all_emails:
        email_lower = email.lower()
        local_part = email_lower.split("@")[0]
        domain = email_lower.split("@")[-1]

        # Skip junk emails (same filters as generic parser)
        if local_part in _JUNK_EMAIL_PREFIXES:
            continue
        if domain in _JUNK_INFRASTRUCTURE_DOMAINS:
            continue
        if domain in _WEB_AGENCY_DOMAINS:
            continue
        # Skip image file extensions misidentified as emails
        ext = domain.rsplit(".", 1)[-1]
        if ext in _IMAGE_EXTENSIONS:
            continue
        if _UUID_LOCAL_RE.match(local_part):
            continue

        # Prefer domain-matching email, but accept personal domains too
        if website_domain:
            website_dom = website_domain.lower().lstrip("www.")
            if domain == website_dom:
                result["director_email"] = email
                break
        # If no website domain or no match yet, accept this email
        if result["director_email"] is None:
            result["director_email"] = email

    # ── Employee count ─────────────────────────────────────────────
    effectif_match = _EFFECTIF_RE.search(company_section)
    if effectif_match:
        count = effectif_match.group(1) or effectif_match.group(2) or effectif_match.group(3)
        if count:
            try:
                effectif = int(count)
                if 1 <= effectif <= 100000:  # Sanity check
                    result["effectif"] = effectif
            except ValueError:
                pass

    return result


# ---------------------------------------------------------------------------
# Best email / best phone selection
# Used by the Enrichir button in companies.py
# ---------------------------------------------------------------------------

_PREFERRED_EMAIL_PREFIXES: tuple[str, ...] = (
    "contact",
    "info",
    "commercial",
    "vente",
    "ventes",
    "accueil",
    "bonjour",
    "hello",
    "secretariat",
    "direction",
)


def _extract_domain(url: str) -> str | None:
    """Return the registered domain (SLD + TLD) from a URL, or None."""
    try:
        netloc = urlparse(url).netloc.lower().lstrip("www.")
        parts = netloc.split(".")
        if len(parts) >= 2:
            return ".".join(parts[-2:])
        return parts[0] if parts else None
    except ValueError:
        return None


def _email_domain_matches(email: str, website_url: str | None) -> bool:
    """Return True if the email domain is plausibly related to the company website."""
    if not website_url:
        return True

    website_domain = _extract_domain(website_url)
    if not website_domain:
        return True

    _, _, email_domain = email.partition("@")
    email_sld = _extract_domain("https://" + email_domain)
    if not email_sld:
        return False

    if email_sld == website_domain:
        return True

    site_root = website_domain.split(".")[0]
    mail_root = email_sld.split(".")[0]
    mail_root_clean = re.sub(r"[^a-z0-9]", "", mail_root)
    site_root_clean = re.sub(r"[^a-z0-9]", "", site_root)
    if len(site_root_clean) >= 4 and (
        site_root_clean in mail_root_clean or mail_root_clean in site_root_clean
    ):
        return True

    return False


def _best_email(
    emails: list[str],
    website_url: str | None,
    siren: str,
    company_name: str | None = None,
) -> str | None:
    """Pick the single best business email from a list.

    Selection strategy (in order):
      1. Remove junk emails and personal-domain emails.
      2. Keep only emails whose domain matches the company website.
      3. Prefer emails with a preferred prefix (contact@ > info@ > commercial@…).
      4. Fall back to the first domain-matching email if no preferred prefix found.
      5. Return None if nothing usable.
    """
    if not emails:
        return None

    usable = [e for e in emails if not is_personal_email(e, company_name)]
    if not usable:
        return None

    domain_matched: list[str] = [
        e for e in usable if _email_domain_matches(e, website_url)
    ]
    candidates = domain_matched if domain_matched else usable

    local_map: dict[str, str] = {}
    for email in candidates:
        local = email.split("@")[0]
        local_map[local] = email

    for prefix in _PREFERRED_EMAIL_PREFIXES:
        if prefix in local_map:
            chosen = local_map[prefix]
            if chosen not in domain_matched:
                log.debug(
                    "contacts.email_domain_mismatch_accepted",
                    email=chosen,
                    website=website_url,
                    siren=siren,
                )
            return chosen

    return candidates[0]


_PHONE_DIGITS_RE = re.compile(r"[\s.\-()]")

_DEPT_TO_PHONE_PREFIX: dict[str, str] = {}

for _d in ("75", "77", "78", "91", "92", "93", "94", "95"):
    _DEPT_TO_PHONE_PREFIX[_d] = "01"

for _d in ("14", "22", "27", "28", "29", "35", "36", "37", "41", "44", "45",
           "49", "50", "53", "56", "61", "72", "76", "85"):
    _DEPT_TO_PHONE_PREFIX[_d] = "02"

for _d in ("02", "08", "10", "18", "21", "25", "39", "51", "52", "54", "55",
           "57", "58", "59", "60", "62", "67", "68", "70", "71", "80", "88",
           "89", "90"):
    _DEPT_TO_PHONE_PREFIX[_d] = "03"

for _d in ("01", "03", "04", "05", "06", "07", "11", "13", "15", "26", "30",
           "34", "38", "42", "43", "48", "63", "66", "69", "73", "74", "83",
           "84", "2A", "2B"):
    _DEPT_TO_PHONE_PREFIX[_d] = "04"

for _d in ("09", "12", "16", "17", "19", "23", "24", "31", "32", "33", "40",
           "46", "47", "64", "65", "79", "81", "82", "86", "87"):
    _DEPT_TO_PHONE_PREFIX[_d] = "05"

del _d


def _best_phone(
    phones: list[str],
    siren: str,
    departement: str | None = None,
) -> str | None:
    """Pick the best phone from a list, preferring geographic match + landlines.

    Priority:
      1. Landline matching company's département (e.g. 05 for dépt 33)
      2. Other geographic landlines (01-05)
      3. Mobile (06-07)
      4. VoIP (09)
    """
    if not phones:
        return None

    valid = [p for p in phones if _is_valid_french_phone(p)]
    if not valid:
        log.debug("contacts.all_phones_invalid", phones=phones, siren=siren)
        return None

    expected_prefix = _DEPT_TO_PHONE_PREFIX.get(departement or "", None)

    def _phone_priority(p: str) -> int:
        digits = _PHONE_DIGITS_RE.sub("", p)
        if digits.startswith("+33") and len(digits) == 12:
            digits = "0" + digits[3:]
        prefix = digits[:2]

        if expected_prefix and prefix == expected_prefix:
            return 0
        if prefix in ("01", "02", "03", "04", "05"):
            return 1
        if prefix in ("06", "07"):
            return 2
        if prefix == "09":
            return 3
        return 4

    chosen = sorted(valid, key=_phone_priority)[0]
    if expected_prefix:
        chosen_digits = _PHONE_DIGITS_RE.sub("", chosen)
        if chosen_digits.startswith("+33"):
            chosen_digits = "0" + chosen_digits[3:]
        if chosen_digits[:2] != expected_prefix:
            log.debug(
                "contacts.phone_geo_mismatch",
                siren=siren,
                departement=departement,
                expected_prefix=expected_prefix,
                chosen_prefix=chosen_digits[:2],
                chosen=chosen,
            )
    return chosen
