"""Worker configuration models."""

from pathlib import Path
from typing import Literal

from pydantic import Field, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]


class WorkerSettings(BaseSettings):
    """Environment-backed worker settings."""

    environment: str = "development"
    log_level: LogLevel = "INFO"
    poll_interval_seconds: int = Field(default=30, gt=0)
    max_backoff_seconds: int = Field(default=300, gt=0)
    score_timeout_seconds: float = Field(default=30.0, gt=0)
    min_candidate_entry_edge: float = Field(default=0.0, ge=0)
    min_candidate_capacity_notional: float = Field(default=0.0, ge=0)
    stop_signals: tuple[Literal["SIGINT", "SIGTERM"], ...] = ("SIGINT", "SIGTERM")
    database_path: str = "data/carryme.sqlite3"
    watchlist_path: str = "config/watchlists/default.json"

    model_config = SettingsConfigDict(
        env_prefix="CARRYME_WORKER_",
        extra="ignore",
    )

    @field_validator("watchlist_path")
    @classmethod
    def validate_watchlist_path(cls, value: str) -> str:
        """Fail fast when the configured watchlist path does not exist."""

        candidate = Path(value).expanduser()
        resolved = candidate if candidate.is_absolute() else (Path.cwd() / candidate).resolve()
        if not resolved.is_file():
            raise ValueError(f"watchlist_path does not exist or is not a file: {resolved}")
        return str(resolved)
