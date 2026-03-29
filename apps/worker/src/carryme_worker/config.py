"""Worker configuration models."""

from typing import Literal

from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]


class WorkerSettings(BaseSettings):
    """Environment-backed worker settings."""

    environment: str = "development"
    log_level: LogLevel = "INFO"
    poll_interval_seconds: int = 30
    database_path: str = "data/carryme.sqlite3"
    watchlist_path: str = "config/watchlists/default.json"

    model_config = SettingsConfigDict(
        env_prefix="CARRYME_WORKER_",
        extra="ignore",
    )
