"""Worker configuration models."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    """Environment-backed worker settings."""

    environment: str = "development"
    log_level: str = "INFO"
    poll_interval_seconds: int = 30
    max_backoff_seconds: int = 300
    database_path: str = "data/carryme.sqlite3"
    watchlist_path: str = "config/watchlists/default.json"

    model_config = SettingsConfigDict(
        env_prefix="CARRYME_WORKER_",
        extra="ignore",
    )
