"""API configuration models."""

import logging
from functools import lru_cache
from pathlib import Path

from carryme_runtime.preflight import LIVE_EXECUTION_VENUE_SPECS
from carryme_storage.db import redact_database_url
from pydantic import AliasChoices, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

logger = logging.getLogger(__name__)


class ApiSettings(BaseSettings):
    """Environment-backed API settings."""

    environment: str = "development"
    database_path: str = Field(
        default="data/carryme.sqlite3",
        validation_alias=AliasChoices(
            "database_path",
            "CARRYME_API_DATABASE_PATH",
            "CARRYME_API_DATABASE_URL",
            "CARRYME_DATABASE_URL",
            "DATABASE_URL",
        ),
    )
    watchlist_path: str = "config/watchlists/default.json"
    extended_live_enabled: bool = False
    extended_api_key: str | None = None
    extended_stark_private_key: str | None = None
    paradex_live_enabled: bool = False
    paradex_account_address: str | None = None
    paradex_private_key: str | None = None
    paradex_bearer_token: str | None = None
    paradex_recv_window_ms: int = 300000
    hyperliquid_live_enabled: bool = False
    hyperliquid_account_address: str | None = None
    hyperliquid_vault_address: str | None = None
    hyperliquid_api_wallet_private_key: str | None = None

    model_config = SettingsConfigDict(
        env_prefix="CARRYME_API_",
        extra="ignore",
    )

    @field_validator("watchlist_path")
    @classmethod
    def validate_watchlist_path(cls, value: str) -> str:
        """Resolve the configured watchlist path while allowing first-write creation."""

        candidate = Path(value).expanduser()
        resolved = candidate if candidate.is_absolute() else (Path.cwd() / candidate).resolve()
        if resolved.exists() and not resolved.is_file():
            raise ValueError(f"watchlist_path exists but is not a file: {resolved}")
        return str(resolved)

    @model_validator(mode="after")
    def warn_on_enabled_live_execution_without_credentials(self) -> "ApiSettings":
        """Warn operators when live execution flags are enabled without credentials."""

        for spec in LIVE_EXECUTION_VENUE_SPECS.values():
            flag_name = spec["enabled_setting"]
            if not getattr(self, flag_name):
                continue
            missing = [
                attribute_name
                for attribute_name in spec["credential_settings"].values()
                if not getattr(self, attribute_name)
            ]
            if missing:
                logger.warning(
                    "%s is enabled but missing live credentials: %s",
                    flag_name,
                    ", ".join(missing),
                )
        return self

    @property
    def database_target(self) -> str:
        """Return a log-safe identifier for the configured database."""

        return redact_database_url(self.database_path)


@lru_cache
def get_api_settings() -> ApiSettings:
    """Return cached API settings."""

    return ApiSettings()
