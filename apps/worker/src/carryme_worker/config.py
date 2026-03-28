"""Worker configuration models."""

from pydantic_settings import BaseSettings, SettingsConfigDict


class WorkerSettings(BaseSettings):
    """Environment-backed worker settings."""

    environment: str = "development"
    log_level: str = "INFO"
    poll_interval_seconds: int = 30

    model_config = SettingsConfigDict(
        env_prefix="CARRYME_WORKER_",
        extra="ignore",
    )
