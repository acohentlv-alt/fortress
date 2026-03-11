"""Fortress configuration — loaded from .env + defaults."""

from pathlib import Path

from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    """Application settings, loaded from environment variables and .env file."""

    model_config = {"env_file": ".env", "env_file_encoding": "utf-8"}

    # Paths
    data_dir: Path = Path("data")
    sirene_dir: Path = Path("data/sirene")
    checkpoints_dir: Path = Path("data/checkpoints")
    outputs_dir: Path = Path("data/outputs")

    # PostgreSQL
    db_host: str = "localhost"
    db_port: int = 5432
    db_name: str = "fortress"
    db_user: str = "fortress"
    db_password: str = "fortress_dev"

    # Auth — set FORTRESS_API_KEY env var to enable API key protection
    api_key: str = ""

    # INPI API
    inpi_username: str = ""
    inpi_password: str = ""
    inpi_daily_limit: int = 10_000

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

    # Testing
    test_db_url: str = ""  # Set via TEST_DB_URL env var to override db_url in tests

    @property
    def db_url(self) -> str:
        """PostgreSQL connection string for psycopg."""
        return (
            f"postgresql://{self.db_user}:{self.db_password}"
            f"@{self.db_host}:{self.db_port}/{self.db_name}"
        )

    @property
    def effective_db_url(self) -> str:
        """Returns test_db_url if set, otherwise db_url. Use in all test fixtures."""
        return self.test_db_url or self.db_url


settings = Settings()
