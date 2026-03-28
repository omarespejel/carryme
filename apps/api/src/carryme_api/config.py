"""API configuration models."""

from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class ApiSettings(BaseSettings):
    """Environment-backed API settings."""

    environment: str = "development"
    database_path: str = "data/carryme.sqlite3"
    watchlist_path: str = "config/watchlists/default.json"

    model_config = SettingsConfigDict(
        env_prefix="CARRYME_API_",
        extra="ignore",
    )


@lru_cache
def get_api_settings() -> ApiSettings:
    """Return cached API settings."""

    return ApiSettings()
