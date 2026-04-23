"""Fortress configuration — loaded from .env + defaults."""

from pathlib import Path

from pydantic_settings import BaseSettings


import os

class Settings(BaseSettings):
    """Application settings, loaded from environment variables and .env file."""

    model_config = {
        "env_file": ".env" if os.access(".env", os.R_OK) else None,
        "env_file_encoding": "utf-8"
    }

    # Paths
    data_dir: Path = Path("data")
    sirene_dir: Path = Path("data/sirene")
    checkpoints_dir: Path = Path("data/checkpoints")
    outputs_dir: Path = Path("data/outputs")

    # PostgreSQL — prefer DATABASE_URL (single connection string from Neon/Render)
    database_url: str = ""  # e.g. postgresql://user:pass@host/db?sslmode=require
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "fortress"
    db_user: str = "fortress"
    db_password: str = "fortress_dev"

    # CORS — set FRONTEND_URL env var for production
    frontend_url: str = "http://localhost:8080"

    # Auth — legacy API key (unused, kept for compat)
    api_key: str = ""

    # Session secret — used to sign session cookies (set a random string in production)
    session_secret: str = "fortress-dev-secret-change-me"

    # INPI API
    inpi_username: str = ""
    inpi_password: str = ""
    inpi_daily_limit: int = 10_000

    # Gemini shadow judge (Wave D1a) — observer only, never modifies linking.
    # Patch A/B (April 21): judge fires on strong-method rows (observation-mode)
    # and on zero-candidate rows (seeds trigram pool). Verdict never influences linking.
    gemini_api_key: str = ""
    gemini_enabled: bool = False
    gemini_model_fallback: str = ""  # e.g. "gemini-2.5-flash" — see R12
    gemini_batch_budget_usd: float = 0.50

    # D1b Hybrid (April 22) — Gemini influences linking decisions.
    # When True: rescue path (upgrade weak/maps_only on high-confidence match)
    #            + quarantine path (downgrade strong auto-confirm on high-confidence no_match,
    #             UNLESS the Frankenstein display-bug signature triggers).
    # Kill switch: set to False to revert to shadow-only behavior.
    gemini_d1b_hybrid_enabled: bool = True
    gemini_d1b_rescue_threshold: float = 0.85
    gemini_d1b_quarantine_threshold: float = 0.85

    # Lever A2 — legal name from mentions-légales → INPI retry
    a2_mentions_legales_enabled: bool = True

    # Agent B — chain/franchise detector (Paul, Franck Provost, McDonald's, etc.)
    chain_detector_enabled: bool = True

    # Batch processing
    wave_size: int = 50
    max_concurrent_scrapers: int = 3
    delay_between_requests_min: float = 3.0
    delay_between_requests_max: float = 8.0
    delay_jitter: float = 2.0
    delay_between_waves_min: float = 5.0
    delay_between_waves_max: float = 15.0

    # Scraping
    request_timeout: int = 15
    browser_timeout: int = 30
    max_pages_per_site: int = 5
    max_retries: int = 3

    # Lambda proxy (Phase 4)
    lambda_endpoints: list[str] = []
    lambda_monthly_budget: int = 800_000
    lambda_rotation_every: int = 5

    # Multi-worker — identifies this machine in batch_data.worker_id
    worker_id: str = ""

    # SMTP (Gmail) — for contact form email forwarding
    smtp_user: str = ""           # e.g. acohen.tlv@gmail.com
    smtp_password: str = ""       # Gmail App Password (16 chars, no spaces)
    contact_notify_email: str = ""  # Where to send notifications (defaults to smtp_user)

    # Testing
    test_db_url: str = ""  # Set via TEST_DB_URL env var to override db_url in tests

    @property
    def db_url(self) -> str:
        """PostgreSQL connection string.

        Priority:
          1. DATABASE_URL env var (single string from Neon/Render — used as-is)
          2. Build from individual parts (db_host, db_port, etc.)
        """
        if self.database_url:
            return self.database_url
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def effective_db_url(self) -> str:
        """Returns test_db_url if set, otherwise db_url. Use in all test fixtures."""
        return self.test_db_url or self.db_url

    @property
    def secure_cookies(self) -> bool:
        """True when frontend is served over HTTPS — enables Secure flag on cookies."""
        return self.frontend_url.startswith("https://")

    @property
    def effective_worker_id(self) -> str:
        """Worker identifier — uses WORKER_ID env var if set, otherwise the hostname."""
        if self.worker_id:
            return self.worker_id
        import socket
        return socket.gethostname()


settings = Settings()
