"""Core data models for Fortress."""

from datetime import date, datetime
from decimal import Decimal
from enum import StrEnum

from pydantic import BaseModel


class CompanyStatus(StrEnum):
    ACTIVE = "A"
    CEASED = "C"


class ContactSource(StrEnum):
    WEBSITE_CRAWL = "website_crawl"
    GOOGLE_MAPS = "google_maps"
    GOOGLE_SEARCH = "google_search"
    INPI = "inpi"
    SIRENE = "sirene"
    SYNTHESIZED = "synthesized"
    RECHERCHE_ENTREPRISES = "recherche_entreprises"
    ANNUAIRE_ENTREPRISES = "annuaire_entreprises"
    DIRECTORY_SEARCH = "directory_search"
    PAGES_JAUNES = "pages_jaunes"
    MENTIONS_LEGALES = "mentions_legales"
    GOOGLE_CSE = "google_cse"


class EmailType(StrEnum):
    FOUND = "found"
    SYNTHESIZED = "synthesized"
    GENERIC = "generic"


class TriageBucket(StrEnum):
    BLACK = "black"
    GREEN = "green"
    YELLOW = "yellow"
    RED = "red"


class JobStatus(StrEnum):
    NEW = "new"
    TRIAGE = "triage"
    IN_PROGRESS = "in_progress"
    PAUSED = "paused"
    COMPLETED = "completed"
    FAILED = "failed"


class Company(BaseModel):
    """A company from SIRENE data."""

    siren: str
    siret_siege: str | None = None
    denomination: str
    enseigne: str | None = None  # Commercial/trade name (from SIRENE StockEtablissement)
    naf_code: str | None = None
    naf_libelle: str | None = None
    forme_juridique: str | None = None
    adresse: str | None = None
    code_postal: str | None = None
    ville: str | None = None
    departement: str | None = None
    region: str | None = None
    statut: CompanyStatus = CompanyStatus.ACTIVE
    date_creation: date | None = None
    tranche_effectif: str | None = None
    latitude: Decimal | None = None
    longitude: Decimal | None = None
    fortress_id: int | None = None
    missing_fields: list[str] = []  # Set by triage for YELLOW companies only


class Contact(BaseModel):
    """Contact information for a company."""

    siren: str
    phone: str | None = None
    email: str | None = None
    email_type: EmailType | None = None
    website: str | None = None
    address: str | None = None
    source: ContactSource
    social_linkedin: str | None = None
    social_facebook: str | None = None
    social_twitter: str | None = None
    social_instagram: str | None = None
    social_tiktok: str | None = None
    social_whatsapp: str | None = None
    social_youtube: str | None = None
    siren_match: bool | None = None
    match_confidence: str | None = None  # 'high', 'low', 'none' — Maps vs SIRENE match quality
    rating: Decimal | None = None
    review_count: int | None = None
    maps_url: str | None = None
    collected_at: datetime | None = None


class Officer(BaseModel):
    """Company officer / director."""

    siren: str
    nom: str
    prenom: str | None = None
    role: str | None = None
    civilite: str | None = None
    email_direct: str | None = None
    ligne_directe: str | None = None
    source: ContactSource = ContactSource.INPI
    collected_at: datetime | None = None


class TriageResult(BaseModel):
    """Result of triaging companies before scraping."""

    black: list[Company] = []
    blue: list[Company] = []   # client already has this company
    green: list[Company] = []
    yellow: list[Company] = []
    red: list[Company] = []

    @property
    def black_count(self) -> int:
        return len(self.black)

    @property
    def blue_count(self) -> int:
        return len(self.blue)

    @property
    def green_count(self) -> int:
        return len(self.green)

    @property
    def yellow_count(self) -> int:
        return len(self.yellow)

    @property
    def red_count(self) -> int:
        return len(self.red)

    @property
    def scrape_required(self) -> int:
        return self.yellow_count + self.red_count


class QueryResult(BaseModel):
    """Result of interpreting and executing a user query."""

    raw_query: str
    industry_name: str
    naf_codes: list[str]
    naf_pattern: str  # SQL LIKE pattern e.g. "01.%" or specific "62.01Z"
    department: str | None  # "66" or None for France-wide
    department_name: str | None  # "Pyrénées-Orientales" or None
    is_france_wide: bool = False
    company_count: int
    sample: list[Company] = []
