"""Phone enrichment for the conchyliculture (shellfish farmer) client list.

Standalone CLI — NOT wired into the discovery pipeline. Target universe: the
552 NAF 03.21Z companies in departments 11/34/66 (local Postgres fortress2),
of which only ~59 have a phone today (via Google Maps discovery links).

Scrapes public tourism-board sites for producer phones (Channel A), stands
ready for gated 118000.fr directory lookups (Channel B), matches scraped
rows back to the 552 with strict wrong-entity guards, validates itself
against known ground truth (phones already captured via Maps), and emits a
v2 client CSV.

Universe query:
    SELECT siren, denomination, enseigne, forme_juridique, departement,
           ville, code_postal, adresse
    FROM companies
    WHERE naf_code='03.21Z' AND departement IN ('11','34','66')
      AND statut='A' AND siren NOT LIKE 'MAPS%'
    -- verified 552 rows on 2026-08-26

ceiling: not every one of the 552 has a public web presence to scrape at
all — the addressable reachability cap for Channel A + Channel B combined
is roughly 250-330/552 (producers with a tourism-listing page or a
118000.fr entry). The remainder genuinely have no public phone anywhere
online; upgrade path is a paid directory API or manual client follow-up,
not more scraping.

Ground truth (for the report/emit-csv validation gate) is SPLIT:
  - confirmed-truth: 48 distinct universe SIRENs reachable via a MAPS entity
    with link_confidence='confirmed' that has a contacts.phone. Trusted.
  - pending-truth: 11 distinct universe SIRENs reachable ONLY via pending
    links (3 SIRENs that appear in both buckets belong to confirmed-truth;
    their pending rows are ignored). Signal, not proof — a scrape/truth
    disagreement there may mean the LINK is wrong, not the scrape.

Subcommands (dry-run-by-default where they write anything):
    aliases      fetch INPI dirigeants + officers table rows, cache to JSON
    channel-a    scrape archipel-thau.com, tourisme-leucate.com,
                 bienvenue-a-la-ferme.com (best-effort)
    channel-b    118000.fr lookups — gated behind --tos-ok (Alan's ToS call)
    report       run the shared matcher over cached scrape rows, print the
                 validation-gate report, decide AUTO-PASS / REVIEW / FAIL
    emit-csv     write the v2 client CSV (only on AUTO-PASS, or --force)
    persist      INSERT accepted phones into contacts (source=directory_search)

Usage:
    python3 -m fortress.scripts.enrich_conchyliculture_phones aliases
    python3 -m fortress.scripts.enrich_conchyliculture_phones channel-a
    python3 -m fortress.scripts.enrich_conchyliculture_phones report
    python3 -m fortress.scripts.enrich_conchyliculture_phones emit-csv
    python3 -m fortress.scripts.enrich_conchyliculture_phones emit-csv --force
    python3 -m fortress.scripts.enrich_conchyliculture_phones persist --apply
    python3 -m fortress.scripts.enrich_conchyliculture_phones channel-b --dry-run-urls
    python3 -m fortress.scripts.enrich_conchyliculture_phones channel-b --tos-ok --validate-only
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
import unicodedata
from dataclasses import dataclass, field
from pathlib import Path
from urllib.parse import quote, urlsplit

# Allow running from repo root with `python3 -m fortress.scripts.enrich_conchyliculture_phones`
# or direct `python3 fortress/scripts/enrich_conchyliculture_phones.py`.
_REPO = Path(__file__).resolve().parent.parent.parent
if str(_REPO) not in sys.path:
    sys.path.insert(0, str(_REPO))

import psycopg
from psycopg.rows import dict_row
from rapidfuzz import fuzz

from fortress.matching.entities import normalize_denomination
from fortress.matching.inpi import fetch_dirigeants
from fortress.scraping.http import CurlClient, CurlClientError
from fortress.utils.phone import normalize_phone, phones_equivalent

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

_DEFAULT_DB_URL = "postgresql://alancohen@localhost:5432/fortress2"
_CACHE_DIR = _REPO / "fortress" / "data" / "enrichment_conchyliculture"
_ALIASES_CACHE = _CACHE_DIR / "aliases.json"
_CHANNEL_A_CACHE = _CACHE_DIR / "channel_a.json"
_CHANNEL_B_CACHE = _CACHE_DIR / "channel_b.json"

_V1_CSV = os.path.expanduser("~/Downloads/conchyliculture_552_complet_11_34_66.csv")
_V2_CSV = os.path.expanduser("~/Downloads/conchyliculture_552_complet_11_34_66_v2.csv")

_UNIVERSE_SQL = """
    SELECT siren, denomination, enseigne, forme_juridique, departement,
           ville, code_postal, adresse
    FROM companies
    WHERE naf_code = '03.21Z' AND departement IN ('11','34','66')
      AND statut = 'A' AND siren NOT LIKE 'MAPS%'
    ORDER BY siren
"""

_GROUND_TRUTH_SQL = """
    WITH universe AS (
        SELECT siren FROM companies
        WHERE naf_code = '03.21Z' AND departement IN ('11','34','66')
          AND statut = 'A' AND siren NOT LIKE 'MAPS%'
    )
    SELECT DISTINCT co.siren, m.link_confidence, c.phone
    FROM universe co
    JOIN companies m ON m.linked_siren = co.siren AND m.siren LIKE 'MAPS%'
    JOIN contacts c ON c.siren = m.siren AND c.phone IS NOT NULL
"""

_OFFICERS_SQL = "SELECT siren, nom, prenom FROM officers WHERE siren = ANY(%s)"

# Matcher thresholds
_SCORE_THRESHOLD = 90.0

# Gate thresholds
_GATE_MIN_MATCHED = 7          # of 48 confirmed-truth
_GATE_MIN_AGREEMENT_PCT = 80.0

# Domains never trusted as a producer's own email/website (own-site + socials
# + obvious platform hosts). A blocklisted hit is dropped, not substituted —
# a producer whose only presence is Facebook simply yields no website.
# ceiling: this is a deliberate safe-false-negative — no attempt to resolve
# a Facebook/Instagram page to a real domain. Upgrade path: a dedicated
# social-profile resolver, if the client ever needs it.
_DOMAIN_BLOCKLIST = (
    # bare "tourisme-leucate" (no TLD) covers both .com and .fr — the site
    # owns both and links to its own .fr contact page from the .com detail
    # pages (observed 2026-08-26: "tourisme-leucate.fr/nous-contacter/").
    "tourisme-leucate", "archipel-thau", "bienvenue-a-la-ferme",
    "facebook.com", "instagram.com", "google.", "youtube.com", "twitter.com",
    "x.com", "linkedin.com", "tiktok.com", "pinterest.com", "goo.gl",
    "bit.ly", "wa.me", "whatsapp.com", "maps.app.goo.gl",
    # Map/directions widgets and CDN/CMP hosts observed on real pages during
    # dev probing (2026-08-26) — not the producer's own site.
    "waze.com", "mappy.com", "apple.com", "gstatic.com", "cloudly.space",
    "consentframework.com", "doubleclick.net", "cloudflare.com", "jsdelivr.net",
)

# Words that carry no matching signal for T3 surname extraction — stripped
# before looking for a leftover "first name" token.
_STOPWORDS = {
    "la", "le", "les", "l", "de", "des", "du", "d", "et", "chez", "cabane",
    "cabanes", "mas", "producteur", "productrice", "huitres", "huitre",
    "moules", "conchylicole", "conchylicoles", "earl", "gaec",
    "sarl", "sas", "eurl", "sci", "établissements", "etablissements", "ets",
    "coquillages", "fruits", "mer", "vente", "directe", "ostreiculteur",
    "ostreicultrice", "ferme", "marine", "the",
    # Regional geography, not a producer identity — 15/552 real universe
    # denominations contain "thau" alone (the lagoon this whole area is
    # named for: "COQUI THAU", "L'INSTANT THAU", "LA NACRE DE THAU", etc.),
    # which produced the exact same false-tie failure mode as the trade
    # words above (found live during report verification, 2026-08-26).
    "thau", "bassin", "lagune", "etang", "étang",
}

# Category text on 118000.fr cards that means "not a producer" — drop.
_CATEGORY_EXCLUSION = ("restaurant", "brasserie", "poissonnerie", "traiteur")

_TOS_FLAG = "--tos-ok"


# ---------------------------------------------------------------------------
# Small text utils
# ---------------------------------------------------------------------------

def _strip_accents_upper(s: str | None) -> str:
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFKD", s)
    return "".join(c for c in nfkd if not unicodedata.combining(c)).upper().strip()


def _norm_commune(s: str | None) -> str:
    """Light accent/case-fold for commune-name comparison only (not a
    business-name normalizer — normalize_denomination is reused for that)."""
    return _strip_accents_upper(s)


def _is_placeholder_phone(normalized: str) -> bool:
    """normalized is the '0XXXXXXXXX' form from normalize_phone(), or ''."""
    if not normalized:
        return True
    if len(set(normalized)) == 1:
        return True
    digits = [int(c) for c in normalized]
    ascending = all((digits[i + 1] - digits[i]) % 10 == 1 for i in range(len(digits) - 1))
    descending = all((digits[i] - digits[i + 1]) % 10 == 1 for i in range(len(digits) - 1))
    return ascending or descending


_TEL_HREF_RE = re.compile(r'href=["\']tel:([^"\']+)["\']', re.IGNORECASE)
_ITEMPROP_TEL_RE = re.compile(
    r'itemprop=["\']telephone["\'][^>]*(?:content=["\']([^"\']*)["\'])?[^>]*>([^<]*)<',
    re.IGNORECASE,
)
_VISIBLE_PHONE_RE = re.compile(r'\b0[1-9](?:[ .\-]?\d{2}){4}\b')
_SCRIPT_STYLE_RE = re.compile(r'<(script|style)\b[^>]*>.*?</\1>', re.IGNORECASE | re.DOTALL)
_TITLE_RE = re.compile(r'<title>([^<]*)</title>', re.IGNORECASE)
_TITLE_NAME_COMMUNE_RE = re.compile(r'^(.*?)\s*\(([^)]+)\)\s*[|–—-]', re.UNICODE)
_MAILTO_RE = re.compile(r'href=["\']mailto:([^"\'?]+)', re.IGNORECASE)
_HREF_RE = re.compile(r'href=["\'](https?://[^"\']+)["\']', re.IGNORECASE)


def extract_phone(html: str) -> tuple[str, str] | None:
    """Return (raw, normalized) for the first recognisable phone, or None.

    Tries tel: hrefs first (most reliable — present on every sampled page),
    then itemprop="telephone", then a visible-text fallback (script/style
    stripped first to avoid matching JS data blobs like map coordinates).
    """
    for raw in _TEL_HREF_RE.findall(html):
        norm = normalize_phone(raw)
        if norm:
            return raw.strip(), norm
    for content, text in _ITEMPROP_TEL_RE.findall(html):
        raw = (content or text or "").strip()
        norm = normalize_phone(raw)
        if norm:
            return raw, norm
    stripped = _SCRIPT_STYLE_RE.sub(" ", html)
    m = _VISIBLE_PHONE_RE.search(stripped)
    if m:
        norm = normalize_phone(m.group(0))
        if norm:
            return m.group(0), norm
    return None


def extract_title_name_commune(html: str) -> tuple[str, str] | None:
    """Parse '<title>NAME (Commune) | Site</title>' (also handles the
    en-dash/hyphen-separated 'NAME – descriptor (Commune) | Site' shape used
    by tourisme-leucate.com — the regex stops at the first '(' anyway)."""
    m = _TITLE_RE.search(html)
    if not m:
        return None
    title = m.group(1).strip()
    m2 = _TITLE_NAME_COMMUNE_RE.match(title)
    if not m2:
        return None
    name, commune = m2.group(1).strip(), m2.group(2).strip()
    if not name or not commune:
        return None
    return name, commune


def extract_email_and_website(html: str, source_site: str) -> tuple[str | None, str | None]:
    """Best-effort mailto:/outbound-link capture, domain-blocklisted.

    Scoped to the fetched page's own <body> — both Channel A sources use one
    dedicated detail page per producer, so the body IS the matched detail
    region (no shared listing card to accidentally bleed from). <head> is
    excluded on purpose: it's all CDN/font/tag-manager/consent-framework
    <link> hrefs on both sampled sites, which produced false-positive
    "websites" during dev probing (2026-08-26) before this restriction.
    """
    body_start = html.lower().find("<body")
    scope = html[body_start:] if body_start != -1 else html

    blocklist = _DOMAIN_BLOCKLIST + (source_site,)

    def _blocked(host: str) -> bool:
        host = host.lower()
        return any(b in host for b in blocklist)

    email = None
    for addr in _MAILTO_RE.findall(scope):
        addr = addr.strip()
        domain = addr.rsplit("@", 1)[-1] if "@" in addr else ""
        if addr and not _blocked(domain):
            email = addr
            break

    website = None
    for href in _HREF_RE.findall(scope):
        host = urlsplit(href).netloc
        if host and not _blocked(host):
            website = href
            break

    return email, website


# ---------------------------------------------------------------------------
# DB helpers
# ---------------------------------------------------------------------------

def get_db_url() -> str:
    return os.environ.get("DATABASE_URL", _DEFAULT_DB_URL)


def get_conn() -> psycopg.Connection:
    return psycopg.connect(get_db_url(), row_factory=dict_row)


def fetch_universe(conn: psycopg.Connection) -> list[dict]:
    with conn.cursor() as cur:
        cur.execute(_UNIVERSE_SQL)
        return cur.fetchall()


def fetch_ground_truth(conn: psycopg.Connection) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    """Returns (confirmed_truth, pending_truth) — siren -> [raw phone, ...].

    pending_truth excludes any siren already present in confirmed_truth
    (the 3-SIREN overlap belongs to confirmed-truth per the brief).
    """
    with conn.cursor() as cur:
        cur.execute(_GROUND_TRUTH_SQL)
        rows = cur.fetchall()
    confirmed: dict[str, list[str]] = {}
    pending: dict[str, list[str]] = {}
    for row in rows:
        target = confirmed if row["link_confidence"] == "confirmed" else pending
        target.setdefault(row["siren"], [])
        if row["phone"] not in target[row["siren"]]:
            target[row["siren"]].append(row["phone"])
    pending = {s: phones for s, phones in pending.items() if s not in confirmed}
    return confirmed, pending


def fetch_officers(conn: psycopg.Connection, sirens: list[str]) -> dict[str, list[dict]]:
    with conn.cursor() as cur:
        cur.execute(_OFFICERS_SQL, (sirens,))
        rows = cur.fetchall()
    out: dict[str, list[dict]] = {}
    for row in rows:
        out.setdefault(row["siren"], []).append({"nom": row["nom"], "prenom": row["prenom"]})
    return out


# ---------------------------------------------------------------------------
# Cache I/O
# ---------------------------------------------------------------------------

def _ensure_cache_dir() -> None:
    _CACHE_DIR.mkdir(parents=True, exist_ok=True)


def _load_json(path: Path, default):
    if not path.exists():
        return default
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def _save_json(path: Path, data) -> None:
    _ensure_cache_dir()
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# ---------------------------------------------------------------------------
# Alias index (shared by match_producer)
# ---------------------------------------------------------------------------

@dataclass
class Candidate:
    siren: str
    denomination: str
    ville: str
    aliases: set[str] = field(default_factory=set)
    officers: list[dict] = field(default_factory=list)  # [{"nom":..., "prenom":...}]


def build_alias_index(universe: list[dict], aliases_cache: dict) -> dict[str, Candidate]:
    index: dict[str, Candidate] = {}
    for row in universe:
        siren = row["siren"]
        cand = Candidate(
            siren=siren,
            denomination=row["denomination"] or "",
            ville=_norm_commune(row["ville"]),
        )
        norm_denom = normalize_denomination(row["denomination"])
        if norm_denom:
            cand.aliases.add(norm_denom)
        norm_ens = normalize_denomination(row.get("enseigne"))
        if norm_ens:
            cand.aliases.add(norm_ens)
        cached = aliases_cache.get(siren)
        if cached:
            for alias in cached.get("aliases", []):
                if alias:
                    cand.aliases.add(alias)
            cand.officers = cached.get("officers", [])
        index[siren] = cand
    return index


def _officer_alias(nom: str | None, prenom: str | None) -> str:
    parts = [p for p in (prenom, nom) if p]
    return normalize_denomination(" ".join(parts))


# ---------------------------------------------------------------------------
# Shared matcher
# ---------------------------------------------------------------------------

@dataclass
class MatchResult:
    matched: bool
    siren: str | None = None
    score: float = 0.0
    tier: str | None = None       # "T1" | "T2" | "T3"
    flag: str | None = None       # "commune_mismatch" | "surname_only"
    ambiguous: bool = False
    candidates: list[tuple[str, float]] = field(default_factory=list)  # for ambiguous reporting


def _strip_stopwords(name_norm: str) -> str:
    """Drop domain-generic conchyliculture/geo words before scoring only
    (the stored alias strings themselves are untouched). Found necessary
    during live verification (2026-08-26): every producer in this universe
    legitimately shares words like "cabane"/"producteur"/"huitres"/"thau",
    so token_set_ratio on the raw normalize_denomination() output scored a
    real scraped name ("La Cabane de Vincent Boniface — Producteur
    d'huitres") at 93 against an UNRELATED company ("LA CABANE DU
    PRODUCTEUR", a different real Leucate producer) — a wrong-entity risk
    the brief's own guard rail doesn't catch unless the wrong company also
    happens to be in confirmed-truth. This reuses normalize_denomination's
    output and the existing T3 stopword list — no new normalization
    function.

    Deliberately does NOT fall back to the raw string when everything gets
    stripped — a candidate whose whole denomination is generic filler
    (e.g. "THAU COQUILLAGES", "L HUITRE DE THAU" -> both reduce to just
    "thau") carries no real distinguishing signal and should score 0
    against everything, not silently reintroduce the noisy word. Confirmed
    live: token_set_ratio("thau", "ambre thau") == 100 — a subset match —
    which is exactly the false-tie this function exists to prevent, and a
    naive "fall back to raw" would have reintroduced it for degenerate
    candidates specifically. rapidfuzz already handles "" vs anything
    correctly (score 0), so an empty result here is safe, not a crash risk."""
    tokens = [t for t in name_norm.split() if t not in _STOPWORDS]
    return " ".join(tokens)


def _best_score(name_norm: str, cand: Candidate) -> float:
    if not cand.aliases:
        return 0.0
    query = _strip_stopwords(name_norm)
    return max(fuzz.token_set_ratio(query, _strip_stopwords(alias)) for alias in cand.aliases)


def _t3_surname_match(name_norm: str, commune_norm: str, index: dict[str, Candidate]) -> MatchResult:
    """Surname-only fallback when no candidate crosses the score threshold."""
    tokens = [t for t in name_norm.split() if t and t not in _STOPWORDS]
    if not tokens:
        return MatchResult(matched=False)

    hits: dict[str, list[dict]] = {}  # siren -> matching officer rows
    for siren, cand in index.items():
        if cand.ville != commune_norm or not commune_norm:
            continue
        for officer in cand.officers:
            surname_norm = normalize_denomination(officer.get("nom"))
            if surname_norm and surname_norm in tokens:
                hits.setdefault(siren, []).append(officer)

    if len(hits) != 1:
        return MatchResult(matched=False)

    siren, officers = next(iter(hits.items()))
    matched_surnames = {normalize_denomination(o.get("nom")) for o in officers}
    remaining = [t for t in tokens if t not in matched_surnames]

    known_prenoms = {normalize_denomination(o.get("prenom")) for o in officers if o.get("prenom")}
    conflicting = any(tok not in known_prenoms for tok in remaining) if remaining else False
    if conflicting:
        return MatchResult(matched=False)

    return MatchResult(matched=True, siren=siren, score=0.0, tier="T3", flag="surname_only")


def match_producer(scraped_name: str, scraped_commune: str, index: dict[str, Candidate]) -> MatchResult:
    name_norm = normalize_denomination(scraped_name)
    commune_norm = _norm_commune(scraped_commune)
    if not name_norm:
        return MatchResult(matched=False)

    scored = [(siren, _best_score(name_norm, cand)) for siren, cand in index.items()]
    above = [(s, sc) for s, sc in scored if sc >= _SCORE_THRESHOLD]

    if not above:
        return _t3_surname_match(name_norm, commune_norm, index)

    if len(above) >= 2:
        above.sort(key=lambda x: -x[1])
        return MatchResult(matched=False, ambiguous=True, candidates=above)

    siren, score = above[0]
    cand = index[siren]
    if cand.ville and commune_norm and cand.ville == commune_norm:
        return MatchResult(matched=True, siren=siren, score=score, tier="T1")
    return MatchResult(matched=True, siren=siren, score=score, tier="T2", flag="commune_mismatch")


# ---------------------------------------------------------------------------
# aliases subcommand
# ---------------------------------------------------------------------------

async def _fetch_aliases_for_siren(siren: str) -> tuple[list[dict], dict]:
    dirigeants, company_data = await fetch_dirigeants(siren)
    return dirigeants, company_data


async def cmd_aliases(args: argparse.Namespace) -> int:
    conn = get_conn()
    try:
        universe = fetch_universe(conn)
        sirens = [row["siren"] for row in universe]
        officers_by_siren = fetch_officers(conn, sirens)
    finally:
        conn.close()

    cache = _load_json(_ALIASES_CACHE, {})
    todo = [s for s in sirens if args.refresh or s not in cache]
    print(f"aliases: {len(sirens)} universe SIRENs, {len(todo)} to fetch "
          f"({len(sirens) - len(todo)} already cached)")

    for i, siren in enumerate(todo, 1):
        try:
            dirigeants, _company_data = await _fetch_aliases_for_siren(siren)
        except Exception as exc:  # best-effort — INPI is flaky, never fatal
            print(f"  [{i}/{len(todo)}] {siren}: ERROR {exc}")
            dirigeants = []

        officer_rows = officers_by_siren.get(siren, [])
        merged_officers = list(officer_rows)
        for d in dirigeants:
            entry = {"nom": d.get("nom"), "prenom": d.get("prenom")}
            if entry not in merged_officers:
                merged_officers.append(entry)

        aliases = set()
        for o in merged_officers:
            a = _officer_alias(o.get("nom"), o.get("prenom"))
            if a:
                aliases.add(a)

        cache[siren] = {"aliases": sorted(aliases), "officers": merged_officers}
        if i % 25 == 0 or i == len(todo):
            print(f"  [{i}/{len(todo)}] cached through {siren}")

    _save_json(_ALIASES_CACHE, cache)
    print(f"aliases: wrote {_ALIASES_CACHE}")
    return 0


# ---------------------------------------------------------------------------
# channel-a subcommand
# ---------------------------------------------------------------------------

_AT_SITEMAP_INDEX = "https://archipel-thau.com/sitemap.xml"
_AT_LEAF_PATTERN = re.compile(r"producteurs-de-terroir/[^/]+/?$")
_LEUCATE_LISTING = "https://www.tourisme-leucate.com/manger/cabanes-a-huitres/"

_LOC_RE = re.compile(r"<loc>([^<]+)</loc>")
_OFFRE_LINK_RE = re.compile(r'href=["\']([^"\']*offres/[^"\']+)["\']')


async def _scrape_archipel_thau(client: CurlClient) -> list[dict]:
    rows: list[dict] = []
    try:
        resp = await client.get(_AT_SITEMAP_INDEX)
    except CurlClientError as exc:
        print(f"  archipel-thau: sitemap index failed ({exc}) — skipping source")
        return rows
    children = _LOC_RE.findall(resp.text)
    print(f"  archipel-thau: {len(children)} child sitemaps")

    leaves: list[str] = []
    for child_url in children:
        try:
            child_resp = await client.get(child_url)
        except CurlClientError:
            continue
        for loc in _LOC_RE.findall(child_resp.text):
            if "producteurs-de-terroir" in loc and _AT_LEAF_PATTERN.search(loc):
                leaves.append(loc)
    print(f"  archipel-thau: {len(leaves)} producer leaf pages")

    for i, url in enumerate(leaves, 1):
        try:
            page = await client.get(url)
        except CurlClientError:
            continue
        parsed = extract_title_name_commune(page.text)
        if not parsed:
            continue
        name, commune = parsed
        phone = extract_phone(page.text)
        email, website = extract_email_and_website(page.text, "archipel-thau.com")
        rows.append({
            "name": name, "commune": commune,
            "phone_raw": phone[0] if phone else None,
            "phone_normalized": phone[1] if phone else None,
            "email": email, "website": website,
            "source_url": url, "source_site": "archipel-thau.com",
        })
        if i % 25 == 0 or i == len(leaves):
            print(f"    [{i}/{len(leaves)}] fetched")
    return rows


async def _leucate_listing_page(client: CurlClient, page_num: int) -> list[str] | None:
    url = _LEUCATE_LISTING if page_num == 1 else f"{_LEUCATE_LISTING}?listpage={page_num}"
    try:
        resp = await client.get(url)
    except CurlClientError:
        return None
    if resp.status_code == 404:
        return None
    links = sorted(set(_OFFRE_LINK_RE.findall(resp.text)))
    return links


async def _scrape_tourisme_leucate(client: CurlClient) -> list[dict]:
    rows: list[dict] = []
    all_links: set[str] = set()
    page_num = 1
    while True:
        links = await _leucate_listing_page(client, page_num)
        if not links:
            break
        all_links.update(links)
        page_num += 1
        if page_num > 10:  # safety cap — this listing has ~2 pages
            break
    print(f"  tourisme-leucate: {len(all_links)} unique detail links across {page_num - 1} pages")

    for i, url in enumerate(sorted(all_links), 1):
        try:
            page = await client.get(url)
        except CurlClientError:
            continue
        parsed = extract_title_name_commune(page.text)
        if not parsed:
            continue
        name, commune = parsed
        phone = extract_phone(page.text)
        email, website = extract_email_and_website(page.text, "tourisme-leucate.com")
        rows.append({
            "name": name, "commune": commune,
            "phone_raw": phone[0] if phone else None,
            "phone_normalized": phone[1] if phone else None,
            "email": email, "website": website,
            "source_url": url, "source_site": "tourisme-leucate.com",
        })
        if i % 10 == 0 or i == len(all_links):
            print(f"    [{i}/{len(all_links)}] fetched")
    return rows


async def _scrape_bienvenue_ferme(client: CurlClient) -> tuple[list[dict], str]:
    """Best-effort — bienvenue-a-la-ferme.com's dept/commune listing pages
    render their producer links client-side (no <a href="…ferme/…"> present
    in the static HTML curl_cffi fetches). Confirmed brittle on 2026-08-26:
    /occitanie and /occitanie/34/ both returned 0 parseable producer links.
    Bonus source, not a dependency — skip and say so, per the brief.
    """
    try:
        resp = await client.get("https://www.bienvenue-a-la-ferme.com/occitanie")
    except CurlClientError as exc:
        return [], f"skipped_brittle: fetch failed ({exc})"
    links = re.findall(r'href=["\']([^"\']*ferme/[^"\']+)["\']', resp.text)
    if not links:
        return [], "skipped_brittle: 0 producer links in static HTML (client-rendered listing)"
    # If the site shape ever changes and links DO show up, this is where a
    # future exec pass would add the follow-through fetch loop.
    return [], f"skipped_brittle: found {len(links)} links but no parse path implemented"


async def cmd_channel_a(args: argparse.Namespace) -> int:
    async with CurlClient(delay_min=1.5, delay_max=3.0) as client:
        print("channel-a: archipel-thau.com")
        at_rows = await _scrape_archipel_thau(client)
        print("channel-a: tourisme-leucate.com")
        leucate_rows = await _scrape_tourisme_leucate(client)
        print("channel-a: bienvenue-a-la-ferme.com (best-effort)")
        baf_rows, baf_status = await _scrape_bienvenue_ferme(client)
        print(f"  bienvenue-a-la-ferme: {baf_status}")

    data = {
        "archipel_thau": at_rows,
        "tourisme_leucate": leucate_rows,
        "bienvenue_ferme": baf_rows,
        "meta": {"bienvenue_ferme_status": baf_status},
    }
    _save_json(_CHANNEL_A_CACHE, data)
    print(f"channel-a: {len(at_rows)} + {len(leucate_rows)} + {len(baf_rows)} rows -> {_CHANNEL_A_CACHE}")
    return 0


# ---------------------------------------------------------------------------
# channel-b subcommand — 118000.fr, ToS-gated
# ---------------------------------------------------------------------------

_118000_SEARCH = "https://www.118000.fr/search?who={who}&where={where}"


# ceiling: GET https://www.118000.fr/search?who=&where= is a starting
# hypothesis only — whether where= actually localizes results by commune
# is UNPROVEN (that's the whole point of --validate-only). If validate-only
# finds it doesn't localize, a non-GET or path-based mechanism is a code
# revision (mini exec pass), not a runtime workaround — do not improvise
# a fix inside this file.
def _118000_url(name: str, commune: str) -> str:
    return _118000_SEARCH.format(who=quote(name), where=quote(commune))


def _parse_118000_cards(html: str) -> list[dict]:
    """ceiling: PROVISIONAL generic card parser — the real 118000.fr card
    markup is unverified (never fetched outside --tos-ok per the ToS gate).
    Windows around each tel: href since we don't know the real card
    container class names; --validate-only's job is to prove or disprove
    this shape empirically. Upgrade path: once --validate-only reveals the
    real markup, replace this with selectors matched to it."""
    cards = []
    for m in _TEL_HREF_RE.finditer(html):
        phone_raw = m.group(1)
        window = html[max(0, m.start() - 1200):m.end() + 400]
        text = re.sub(r"<[^>]+>", " ", window)
        text = re.sub(r"\s+", " ", text).strip()
        cards.append({"phone_raw": phone_raw, "text": text})
    return cards


def _card_matches_commune(card_text: str, commune: str, cp: str | None) -> bool:
    text_norm = _norm_commune(card_text)
    if commune and _norm_commune(commune) in text_norm:
        return True
    if cp and cp in card_text:
        return True
    return False


def _card_category_excluded(card_text: str) -> bool:
    lowered = card_text.lower()
    return any(kw in lowered for kw in _CATEGORY_EXCLUSION)


@dataclass
class ChannelBResult:
    siren: str
    query_name: str
    accepted: bool
    phone_raw: str | None = None
    phone_normalized: str | None = None
    reason: str | None = None


async def _query_118000(client: CurlClient, name: str, commune: str) -> tuple[int, list[dict]]:
    url = _118000_url(name, commune)
    resp = await client.get(url)
    if resp.status_code in (403, 429):
        raise CurlClientError(url, resp.status_code)
    return resp.status_code, _parse_118000_cards(resp.text)


async def _apply_guards(client: CurlClient, company: dict, first_cards: list[dict]) -> ChannelBResult:
    """Apply the acceptance guards for one company, reusing the already-fetched
    query-1 cards (no second fetch of the same URL — see cmd_channel_b)."""
    siren = company["siren"]
    name = company["denomination"] or ""
    commune = company["ville"] or ""
    cp = company.get("code_postal")

    cards = first_cards
    if not cards and company.get("forme_juridique") == "1000":
        # forme-1000 individual with no hit on the legal/denomination form —
        # retry inverted "prenom nom" (see shared matcher rationale). Cap
        # 2 queries/company — this is the only place a 2nd query happens.
        inverted = " ".join(reversed(name.split())) if name else name
        _status, cards = await _query_118000(client, inverted, commune)

    passing = [c for c in cards
               if _card_matches_commune(c["text"], commune, cp)
               and not _card_category_excluded(c["text"])]

    if len(passing) == 0:
        return ChannelBResult(siren=siren, query_name=name, accepted=False, reason="no_passing_card")
    if len(passing) >= 2:
        return ChannelBResult(siren=siren, query_name=name, accepted=False, reason="ambiguous")

    phone_norm = normalize_phone(passing[0]["phone_raw"])
    if not phone_norm or _is_placeholder_phone(phone_norm):
        return ChannelBResult(siren=siren, query_name=name, accepted=False, reason="placeholder_or_unparsable")

    return ChannelBResult(
        siren=siren, query_name=name, accepted=True,
        phone_raw=passing[0]["phone_raw"], phone_normalized=phone_norm,
    )


def _validate_localization(client_results: list[tuple[dict, list[dict]]]) -> bool:
    """First job of --validate-only: does where= actually localize results?
    Checks whether ANY returned card's own address text mentions the queried
    commune. If none do across the whole probe set, localization is unproven."""
    for company, cards in client_results:
        commune = company["ville"] or ""
        if any(_card_matches_commune(c["text"], commune, company.get("code_postal")) for c in cards):
            return True
    return False


def _select_validate_probe(confirmed_truth_sirens: set[str], universe: list[dict]) -> list[dict]:
    by_siren = {row["siren"]: row for row in universe}
    confirmed_rows = [by_siren[s] for s in sorted(confirmed_truth_sirens) if s in by_siren]
    individuals = [r for r in confirmed_rows if r["forme_juridique"] == "1000"][:5]
    societes = [r for r in confirmed_rows if r["forme_juridique"] != "1000"][:5]
    return individuals + societes


async def cmd_channel_b(args: argparse.Namespace) -> int:
    # GATE — enforced before any network object exists. Provably zero
    # network without --tos-ok, even under --dry-run-urls: the client's
    # ToS agreement is a business/legal decision (Alan's lever), not a
    # runtime convenience flag.
    if not args.tos_ok:
        print(f"channel-b: refusing to run without {_TOS_FLAG} (118000.fr terms of "
              f"service require explicit sign-off before any lookup — Alan's call).")
        return 2

    conn = get_conn()
    try:
        universe = fetch_universe(conn)
        confirmed_truth, _pending_truth = fetch_ground_truth(conn)
    finally:
        conn.close()

    if args.validate_only:
        targets = _select_validate_probe(set(confirmed_truth.keys()), universe)
    else:
        phoneless_sirens = {row["siren"] for row in universe} - set(confirmed_truth.keys())
        targets = [row for row in universe if row["siren"] in phoneless_sirens]

    if args.dry_run_urls:
        for company in targets:
            print(_118000_url(company["denomination"] or "", company["ville"] or ""))
        print(f"channel-b: {len(targets)} URL(s) — dry run, nothing fetched.")
        return 0

    results: list[ChannelBResult] = []
    consecutive_errors = 0
    # max_retries=1 (first attempt only, no internal retry) — CurlClient's
    # default (3 retries with backoff) would silently hit a blocking 403/429
    # up to 3 more times before ever raising, which defeats "ANY 403/429 ->
    # halt immediately" on a ToS-sensitive site. This makes CurlClientError
    # surface on the FIRST 403/429, and CurlClientError.status_code lets the
    # circuit breaker below distinguish "walled" from a generic transient error.
    async with CurlClient(delay_min=3.0, delay_max=6.0, max_retries=1) as client:
        # Query 1 for every target, ONCE — reused below both for the
        # --validate-only localization check and for guard evaluation, so
        # no company is fetched twice on the happy path.
        first_pass: list[tuple[dict, list[dict]]] = []
        halted = False
        for company in targets:
            try:
                _status, cards = await _query_118000(client, company["denomination"] or "", company["ville"] or "")
            except CurlClientError as exc:
                print(f"  {company['siren']}: fetch error {exc}")
                if exc.status_code in (403, 429):
                    print(f"channel-b: circuit breaker — HTTP {exc.status_code}, halting immediately.")
                    halted = True
                    break
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    print("channel-b: circuit breaker — 3 consecutive HTTP errors, halting.")
                    halted = True
                    break
                continue
            consecutive_errors = 0
            first_pass.append((company, cards))

        if halted:
            return 1

        if args.validate_only:
            localized = _validate_localization(first_pass)
            print(f"channel-b --validate-only: where= localization = {'YES' if localized else 'NO'}")
            if not localized:
                print("channel-b: results are NOT localized by where= — the GET hypothesis "
                      "does not hold as specified. This needs a mini exec pass to revise the "
                      "query mechanism, not a runtime workaround. Stopping.")
                return 1
            print("channel-b: localization confirmed — proceeding with guard evaluation on the probe set.")

        consecutive_errors = 0
        for company, cards in first_pass:
            try:
                result = await _apply_guards(client, company, cards)
            except CurlClientError as exc:
                print(f"  {company['siren']}: fetch error {exc}")
                if exc.status_code in (403, 429):
                    print(f"channel-b: circuit breaker — HTTP {exc.status_code}, halting immediately.")
                    break
                consecutive_errors += 1
                if consecutive_errors >= 3:
                    print("channel-b: circuit breaker — 3 consecutive HTTP errors, halting.")
                    break
                continue
            consecutive_errors = 0
            results.append(result)

    accepted = [r for r in results if r.accepted]
    print(f"channel-b: {len(results)} probed, {len(accepted)} accepted")
    _save_json(_CHANNEL_B_CACHE, [r.__dict__ for r in results])
    print(f"channel-b: wrote {_CHANNEL_B_CACHE}")
    return 0


# ---------------------------------------------------------------------------
# Shared matching run (used by report / emit-csv / persist)
# ---------------------------------------------------------------------------

@dataclass
class MatchRunResult:
    accepted: dict[str, dict]           # siren -> {"phone_normalized", "phone_raw", "source_bucket", "email", "website"}
    ambiguous_rows: list[dict]          # scraped rows that were dropped as ambiguous
    wrong_entity: list[dict]            # accepted rows whose phone matches a DIFFERENT confirmed truth siren
    disagreements: list[dict]           # accepted rows that conflict with truth for their OWN siren
    confirmed_matched: int
    confirmed_agree: int
    pending_matched: int
    pending_agree: int


def run_matching(index: dict[str, Candidate], channel_a: dict | None, channel_b: list[dict] | None,
                  confirmed_truth: dict[str, list[str]], pending_truth: dict[str, list[str]]) -> MatchRunResult:
    accepted: dict[str, dict] = {}
    ambiguous_rows: list[dict] = []

    def _consider(row: dict, source_bucket: str) -> None:
        # ceiling: first-match-wins per SIREN across sources — if two scraped
        # rows (e.g. archipel-thau + tourisme-leucate) both resolve to the
        # same SIREN, the first one processed is kept; the brief doesn't
        # specify cross-source conflict handling for this rare edge.
        phone_norm = row.get("phone_normalized")
        if not phone_norm or _is_placeholder_phone(phone_norm):
            return
        result = match_producer(row.get("name") or row.get("query_name") or "",
                                 row.get("commune") or "", index)
        if result.ambiguous:
            ambiguous_rows.append({"row": row, "candidates": result.candidates})
            return
        if not result.matched or not result.siren:
            return
        if result.siren in accepted:
            return
        accepted[result.siren] = {
            "phone_normalized": phone_norm,
            "phone_raw": row.get("phone_raw"),
            "source_bucket": source_bucket,
            "email": row.get("email"),
            "website": row.get("website"),
            "tier": result.tier,
            "flag": result.flag,
        }

    if channel_a:
        for row in channel_a.get("archipel_thau", []):
            _consider(row, "office de tourisme")
        for row in channel_a.get("tourisme_leucate", []):
            _consider(row, "office de tourisme")
        for row in channel_a.get("bienvenue_ferme", []):
            _consider(row, "office de tourisme")

    if channel_b:
        # channel-b results are already keyed by SIREN (the company that was
        # queried) — no free-text re-matching against the alias index here.
        # channel-b's own guards (commune-in-card, category exclusion,
        # ambiguous-card, placeholder) already ran inside the channel-b
        # subcommand; a cached row with accepted=True has passed all of them.
        for row in channel_b:
            if not row.get("accepted"):
                continue
            siren = row.get("siren")
            phone_norm = row.get("phone_normalized")
            if not siren or not phone_norm or _is_placeholder_phone(phone_norm):
                continue
            if siren in accepted:
                continue
            accepted[siren] = {
                "phone_normalized": phone_norm,
                "phone_raw": row.get("phone_raw"),
                "source_bucket": "annuaire",
                "email": None, "website": None,
                "tier": "channel_b", "flag": None,
            }

    wrong_entity: list[dict] = []
    disagreements: list[dict] = []
    confirmed_matched = 0
    confirmed_agree = 0
    pending_matched = 0
    pending_agree = 0

    all_confirmed_phones = [(s, p) for s, phones in confirmed_truth.items() for p in phones]

    for siren, info in accepted.items():
        phone = info["phone_normalized"]
        # wrong-entity: does this phone belong to a DIFFERENT confirmed company?
        for other_siren, other_phone in all_confirmed_phones:
            if other_siren == siren:
                continue
            if phones_equivalent(phone, other_phone):
                wrong_entity.append({"siren": siren, "phone": phone, "conflicts_with_siren": other_siren})
                break

        if siren in confirmed_truth:
            confirmed_matched += 1
            agree = any(phones_equivalent(phone, tp) for tp in confirmed_truth[siren])
            if agree:
                confirmed_agree += 1
            else:
                disagreements.append({"siren": siren, "scraped_phone": phone,
                                       "truth_phones": confirmed_truth[siren], "status": "confirmed"})
        elif siren in pending_truth:
            pending_matched += 1
            agree = any(phones_equivalent(phone, tp) for tp in pending_truth[siren])
            if agree:
                pending_agree += 1
            else:
                disagreements.append({"siren": siren, "scraped_phone": phone,
                                       "truth_phones": pending_truth[siren], "status": "pending"})

    return MatchRunResult(
        accepted=accepted, ambiguous_rows=ambiguous_rows, wrong_entity=wrong_entity,
        disagreements=disagreements, confirmed_matched=confirmed_matched,
        confirmed_agree=confirmed_agree, pending_matched=pending_matched, pending_agree=pending_agree,
    )


def compute_gate(run: MatchRunResult) -> str:
    """Returns 'AUTO-PASS' | 'REVIEW' | 'FAIL'."""
    if run.wrong_entity:
        return "FAIL"
    agreement_pct = (100.0 * run.confirmed_agree / run.confirmed_matched) if run.confirmed_matched else 0.0
    if run.confirmed_matched >= _GATE_MIN_MATCHED and agreement_pct >= _GATE_MIN_AGREEMENT_PCT:
        return "AUTO-PASS"
    return "REVIEW"


def _load_matching_inputs() -> tuple[dict[str, Candidate], dict | None, list[dict] | None,
                                      dict[str, list[str]], dict[str, list[str]], list[dict]]:
    conn = get_conn()
    try:
        universe = fetch_universe(conn)
        confirmed_truth, pending_truth = fetch_ground_truth(conn)
    finally:
        conn.close()
    aliases_cache = _load_json(_ALIASES_CACHE, {})
    index = build_alias_index(universe, aliases_cache)
    channel_a = _load_json(_CHANNEL_A_CACHE, None)
    channel_b = _load_json(_CHANNEL_B_CACHE, None)
    return index, channel_a, channel_b, confirmed_truth, pending_truth, universe


# ---------------------------------------------------------------------------
# report subcommand
# ---------------------------------------------------------------------------

def cmd_report(args: argparse.Namespace) -> int:
    index, channel_a, channel_b, confirmed_truth, pending_truth, _universe = _load_matching_inputs()

    if not channel_a and not channel_b:
        print("report: no scrape data cached (run channel-a and/or channel-b first)")
        return 0

    run = run_matching(index, channel_a, channel_b, confirmed_truth, pending_truth)
    gate = compute_gate(run)

    print("=" * 72)
    print("VALIDATION-GATE REPORT — conchyliculture phone enrichment")
    print("=" * 72)

    agreement_pct = (100.0 * run.confirmed_agree / run.confirmed_matched) if run.confirmed_matched else 0.0
    print(f"\n(i) CONFIRMED-TRUTH ({len(confirmed_truth)} SIRENs)")
    print(f"    matched:   {run.confirmed_matched}/{len(confirmed_truth)}")
    print(f"    agreement: {run.confirmed_agree}/{run.confirmed_matched} "
          f"({agreement_pct:.1f}%)" if run.confirmed_matched else "    agreement: n/a (0 matched)")

    pending_agree_pct = (100.0 * run.pending_agree / run.pending_matched) if run.pending_matched else 0.0
    print(f"\n(ii) PENDING-TRUTH ({len(pending_truth)} SIRENs — signal only, "
          f"counts toward NEITHER agreement% nor wrong-entity)")
    print(f"    matched:   {run.pending_matched}/{len(pending_truth)}")
    print(f"    agreement: {run.pending_agree}/{run.pending_matched} "
          f"({pending_agree_pct:.1f}%)" if run.pending_matched else "    agreement: n/a (0 matched)")

    print(f"\n(iii) WRONG-ENTITY CHECK: {len(run.wrong_entity)} hit(s)")
    for hit in run.wrong_entity:
        print(f"    {hit['siren']} scraped phone {hit['phone']} == confirmed phone of {hit['conflicts_with_siren']}")

    print(f"\n(iv) AMBIGUOUS-DROPPED: {len(run.ambiguous_rows)} row(s)")
    if not run.ambiguous_rows:
        print("    (none)")
    for item in run.ambiguous_rows[:20]:
        row = item["row"]
        cands = ", ".join(f"{s}({sc:.0f})" for s, sc in item["candidates"][:5])
        print(f"    '{row.get('name') or row.get('query_name')}' -> {cands}")

    print(f"\n(v) DISAGREEMENTS: {len(run.disagreements)} row(s)")
    for d in run.disagreements:
        print(f"    {d['siren']} [{d['status']}]: scraped={d['scraped_phone']} truth={d['truth_phones']}")

    print(f"\n{'=' * 72}")
    print(f"GATE: {gate}  "
          f"(need >= {_GATE_MIN_MATCHED}/{len(confirmed_truth)} confirmed matched AND "
          f">= {_GATE_MIN_AGREEMENT_PCT:.0f}% agreement AND 0 wrong-entity)")
    print("=" * 72)

    return {"AUTO-PASS": 0, "FAIL": 1, "REVIEW": 2}[gate]


# ---------------------------------------------------------------------------
# emit-csv subcommand
# ---------------------------------------------------------------------------

_V1_COLUMNS = ["SIREN", "Nom legal", "Departement", "Ville", "Adresse", "Forme juridique",
               "Telephone", "Email", "Site web", "Facebook", "Instagram", "Presence en ligne"]
_SOURCE_COLUMN = "Source du téléphone"


def _read_v1_csv(path: str) -> tuple[dict[str, dict], list[str]]:
    import csv
    with open(path, encoding="utf-8-sig", newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames or []
        if list(header) != _V1_COLUMNS:
            print("emit-csv: WARNING v1 header differs from expected — using file's own header order")
        rows = {row["SIREN"]: row for row in reader}
    return rows, list(header) or _V1_COLUMNS


def cmd_emit_csv(args: argparse.Namespace) -> int:
    import csv

    index, channel_a, channel_b, confirmed_truth, pending_truth, universe = _load_matching_inputs()
    if not channel_a and not channel_b:
        print("emit-csv: no scrape data cached (run channel-a and/or channel-b first)")
        return 1

    run = run_matching(index, channel_a, channel_b, confirmed_truth, pending_truth)
    gate = compute_gate(run)

    if gate == "FAIL":
        print(f"emit-csv: refusing — gate is FAIL (wrong-entity hits present). "
              f"--force does not override wrong-entity. See `report` for details.")
        return 1
    if gate != "AUTO-PASS" and not args.force:
        print(f"emit-csv: refusing — gate is {gate}, not AUTO-PASS. Pass --force to emit anyway "
              f"(per-row guards still apply; wrong-entity still refuses). See `report` for details.")
        return 1
    if gate != "AUTO-PASS" and args.force:
        print(f"emit-csv: gate is {gate} — proceeding under --force (0 wrong-entity confirmed).")

    if not os.path.exists(_V1_CSV):
        print(f"emit-csv: v1 CSV not found at {_V1_CSV}")
        return 1
    v1_rows, header = _read_v1_csv(_V1_CSV)

    maps_linked_sirens = set(confirmed_truth.keys()) | set(pending_truth.keys())

    out_header = header + [_SOURCE_COLUMN]
    out_rows = []
    for row in universe:
        siren = row["siren"]
        v1 = dict(v1_rows.get(siren, {}))
        for col in out_header[:-1]:
            v1.setdefault(col, "")

        match = run.accepted.get(siren)
        existing_phone = (v1.get("Telephone") or "").strip()
        if existing_phone:
            # confirmed-Maps-wins / pending-does-not-auto-win: v1's own
            # phone is never overwritten, agreeing or not — a conflict is
            # only ever logged (see `report`'s disagreement list).
            source = "maps" if siren in maps_linked_sirens else ""
        elif match:
            v1["Telephone"] = match["phone_raw"] or match["phone_normalized"]
            source = match["source_bucket"]
        else:
            source = ""

        # Email/website fill is independent of whether the phone was filled
        # — empty cells only, same conflict rule.
        if match:
            if not (v1.get("Email") or "").strip() and match.get("email"):
                v1["Email"] = match["email"]
            if not (v1.get("Site web") or "").strip() and match.get("website"):
                v1["Site web"] = match["website"]

        v1[_SOURCE_COLUMN] = source
        out_rows.append(v1)

    with open(_V2_CSV, "w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=out_header)
        writer.writeheader()
        writer.writerows(out_rows)

    filled = sum(1 for r in out_rows if r[_SOURCE_COLUMN] and r[_SOURCE_COLUMN] != "maps")
    print(f"emit-csv: wrote {len(out_rows)} rows to {_V2_CSV} "
          f"({filled} new phones filled, gate={gate})")
    return 0


# ---------------------------------------------------------------------------
# persist subcommand
# ---------------------------------------------------------------------------

_PERSIST_SQL = """
    INSERT INTO contacts (siren, phone, source, collected_at)
    VALUES (%s, %s, 'directory_search', now())
    ON CONFLICT (siren, source) DO NOTHING
"""


def cmd_persist(args: argparse.Namespace) -> int:
    index, channel_a, channel_b, confirmed_truth, pending_truth, _universe = _load_matching_inputs()
    if not channel_a and not channel_b:
        print("persist: no scrape data cached (run channel-a and/or channel-b first)")
        return 0

    run = run_matching(index, channel_a, channel_b, confirmed_truth, pending_truth)
    wrong_entity_sirens = {h["siren"] for h in run.wrong_entity}
    candidates = [(siren, info["phone_raw"] or info["phone_normalized"])
                  for siren, info in run.accepted.items()
                  if siren not in wrong_entity_sirens]

    print(f"persist: {len(candidates)} phone(s) passed all guards "
          f"({len(run.accepted) - len(candidates)} excluded as wrong-entity)")

    if not args.apply:
        print("persist: DRY RUN — pass --apply to write. Sample:")
        for siren, phone in candidates[:10]:
            print(f"    {siren} -> {phone}")
        return 0

    conn = get_conn()
    try:
        with conn.cursor() as cur:
            for siren, phone in candidates:
                cur.execute(_PERSIST_SQL, (siren, phone))
        conn.commit()
    finally:
        conn.close()
    print(f"persist: wrote {len(candidates)} row(s) (ON CONFLICT DO NOTHING — duplicates silently skipped)")
    return 0


# ---------------------------------------------------------------------------
# main
# ---------------------------------------------------------------------------

def build_argparser() -> argparse.ArgumentParser:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = ap.add_subparsers(dest="command", required=True)

    p_aliases = sub.add_parser("aliases", help="Fetch INPI dirigeants + officers table rows, cache to JSON")
    p_aliases.add_argument("--refresh", action="store_true", help="Re-fetch SIRENs already cached")

    sub.add_parser("channel-a", help="Scrape tourism-board sites for producer phones")

    p_cb = sub.add_parser("channel-b", help="118000.fr directory lookups (ToS-gated)")
    p_cb.add_argument("--tos-ok", dest="tos_ok", action="store_true",
                       help="Confirm ToS review is done before any 118000.fr request is made")
    p_cb.add_argument("--validate-only", dest="validate_only", action="store_true",
                       help="10-company probe: establish whether where= localizes results")
    p_cb.add_argument("--dry-run-urls", dest="dry_run_urls", action="store_true",
                       help="Print the URLs that would be fetched and exit — no network")

    sub.add_parser("report", help="Run the shared matcher and print the validation-gate report")

    p_emit = sub.add_parser("emit-csv", help="Write the v2 client CSV")
    p_emit.add_argument("--force", action="store_true",
                         help="Bypass aggregate gate thresholds only — per-row guards and the "
                              "wrong-entity refusal always apply")

    p_persist = sub.add_parser("persist", help="Insert accepted phones into contacts")
    p_persist.add_argument("--apply", action="store_true", help="Actually write (default: dry run)")

    return ap


async def _async_main(args: argparse.Namespace) -> int:
    if args.command == "aliases":
        return await cmd_aliases(args)
    if args.command == "channel-a":
        return await cmd_channel_a(args)
    if args.command == "channel-b":
        return await cmd_channel_b(args)
    raise AssertionError(f"unreachable async command: {args.command}")


def main() -> int:
    ap = build_argparser()
    args = ap.parse_args()

    if args.command == "channel-b":
        # Hard gate before any CurlClient (or any network object) is
        # constructed — checked here, synchronously, before asyncio.run
        # ever spins up an event loop.
        if not args.tos_ok:
            print(f"channel-b: refusing to run without {_TOS_FLAG} (118000.fr terms of "
                  f"service require explicit sign-off before any lookup — Alan's call).")
            return 2
        return asyncio.run(_async_main(args))

    if args.command in ("aliases", "channel-a"):
        return asyncio.run(_async_main(args))
    if args.command == "report":
        return cmd_report(args)
    if args.command == "emit-csv":
        return cmd_emit_csv(args)
    if args.command == "persist":
        return cmd_persist(args)
    raise AssertionError(f"unreachable command: {args.command}")


if __name__ == "__main__":
    # channel-a/aliases run for minutes against slow, rate-limited remote
    # sites — line-buffer stdout so progress is visible live (e.g. `tail -f`
    # on a redirected log) instead of only appearing when the process exits.
    try:
        sys.stdout.reconfigure(line_buffering=True)
    except AttributeError:
        pass
    sys.exit(main())
