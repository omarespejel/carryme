"""Worker configuration models."""

from pathlib import Path
from typing import Literal

from carryme_models import SUPPORTED_UNIVERSE_VENUES
from carryme_normalizers import get_fee_profile
from pydantic import AliasChoices, Field, ValidationInfo, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict

LogLevel = Literal["CRITICAL", "ERROR", "WARNING", "INFO", "DEBUG"]


class WorkerSettings(BaseSettings):
    """Environment-backed worker settings."""

    environment: str = "development"
    log_level: LogLevel = "INFO"
    poll_interval_seconds: int = Field(default=30, gt=0)
    max_backoff_seconds: int = Field(default=300, gt=0)
    universe_scan_interval_seconds: int = Field(default=60, gt=0)
    universe_scan_max_backoff_seconds: int = Field(default=300, gt=0)
    approved_canary_scan_interval_seconds: int = Field(default=30, gt=0)
    approved_canary_scan_max_backoff_seconds: int = Field(default=300, gt=0)
    approved_canary_alert_max_snapshot_age_seconds: int = Field(default=300, gt=0)
    execution_observation_interval_seconds: int = Field(default=10, gt=0)
    execution_observation_max_backoff_seconds: int = Field(default=60, gt=0)
    execution_alert_webhook_url: str | None = None
    execution_alert_webhook_timeout_seconds: float = Field(default=10.0, gt=0)
    score_timeout_seconds: float = Field(default=30.0, gt=0)
    universe_scan_timeout_seconds: float = Field(default=30.0, gt=0)
    min_candidate_entry_edge: float = Field(default=0.0, ge=0)
    min_candidate_capacity_notional: float = Field(default=0.0, ge=0)
    universe_scan_venues: tuple[str, ...] = SUPPORTED_UNIVERSE_VENUES
    universe_scan_ranking: Literal[
        "roundtrip_edge",
        "entry_edge",
        "roundtrip_pnl",
        "entry_pnl",
        "quality_adjusted_roundtrip_pnl",
        "execution_adjusted_roundtrip_pnl",
        "execution_adjusted_quality_pnl",
        "stability_adjusted_roundtrip_pnl",
        "stability_adjusted_quality_pnl",
        "route_adjusted_quality_pnl",
    ] = "route_adjusted_quality_pnl"
    universe_scan_extended_fee_profile: str | None = None
    universe_scan_paradex_fee_profile: str | None = None
    universe_scan_hyperliquid_fee_profile: str | None = None
    universe_scan_target_notional: float = Field(default=5_000.0, ge=0)
    universe_scan_min_capacity_notional: float = Field(default=250.0, ge=0)
    universe_scan_min_daily_volume: float = Field(default=10_000.0, ge=0)
    universe_scan_min_open_interest: float = Field(default=50_000.0, ge=0)
    universe_scan_min_roundtrip_edge: float = Field(default=0.0, ge=0)
    universe_scan_min_execution_quality_score: float = Field(default=0.0, ge=0)
    universe_scan_min_execution_samples: int = Field(default=0, ge=0)
    universe_scan_min_route_stability_weight: float = Field(default=0.0, ge=0)
    universe_scan_min_route_presence_ratio: float = Field(default=0.0, ge=0, le=1)
    universe_scan_min_route_samples: int = Field(default=0, ge=0)
    universe_scan_limit: int = Field(default=10, gt=0)
    universe_scan_include_symbols: tuple[str, ...] = ()
    universe_scan_exclude_symbols: tuple[str, ...] = ()
    universe_scan_exclude_tags: tuple[str, ...] = ()
    approved_canary_scan_venues: tuple[str, ...] = ("extended", "paradex", "hyperliquid")
    approved_canary_scan_extended_fee_profile: str | None = None
    approved_canary_scan_paradex_fee_profile: str | None = "pro_fastfills"
    approved_canary_scan_hyperliquid_fee_profile: str | None = None
    approved_canary_scan_target_notional: float = 5_000.0
    approved_canary_scan_max_notional: float = 25.0
    approved_canary_scan_min_capacity_notional: float = 25.0
    approved_canary_scan_min_daily_volume: float = 0.0
    approved_canary_scan_min_open_interest: float = 0.0
    approved_canary_scan_min_roundtrip_edge: float = 0.0
    approved_canary_scan_min_execution_quality_score: float = 0.5
    approved_canary_scan_min_execution_samples: int = 0
    approved_canary_scan_min_route_stability_weight: float = 0.35
    approved_canary_scan_min_route_presence_ratio: float = 0.35
    approved_canary_scan_min_route_samples: int = 2
    approved_canary_scan_limit: int = 5
    approved_canary_scan_include_symbols: tuple[str, ...] = ()
    approved_canary_scan_exclude_symbols: tuple[str, ...] = ()
    approved_canary_scan_exclude_tags: tuple[str, ...] = ("meme", "political")
    stop_signals: tuple[Literal["SIGINT", "SIGTERM"], ...] = ("SIGINT", "SIGTERM")
    database_path: str = "data/carryme.sqlite3"
    watchlist_path: str = "config/watchlists/default.json"
    execution_observation_limit: int = Field(default=20, gt=0)
    extended_live_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "CARRYME_WORKER_EXTENDED_LIVE_ENABLED",
            "CARRYME_API_EXTENDED_LIVE_ENABLED",
        ),
    )
    extended_api_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "CARRYME_WORKER_EXTENDED_API_KEY",
            "CARRYME_API_EXTENDED_API_KEY",
        ),
    )
    paradex_live_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "CARRYME_WORKER_PARADEX_LIVE_ENABLED",
            "CARRYME_API_PARADEX_LIVE_ENABLED",
        ),
    )
    paradex_account_address: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "CARRYME_WORKER_PARADEX_ACCOUNT_ADDRESS",
            "CARRYME_API_PARADEX_ACCOUNT_ADDRESS",
        ),
    )
    paradex_private_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "CARRYME_WORKER_PARADEX_PRIVATE_KEY",
            "CARRYME_API_PARADEX_PRIVATE_KEY",
        ),
    )
    paradex_bearer_token: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "CARRYME_WORKER_PARADEX_BEARER_TOKEN",
            "CARRYME_API_PARADEX_BEARER_TOKEN",
        ),
    )
    hyperliquid_live_enabled: bool = Field(
        default=False,
        validation_alias=AliasChoices(
            "CARRYME_WORKER_HYPERLIQUID_LIVE_ENABLED",
            "CARRYME_API_HYPERLIQUID_LIVE_ENABLED",
        ),
    )
    hyperliquid_account_address: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "CARRYME_WORKER_HYPERLIQUID_ACCOUNT_ADDRESS",
            "CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS",
        ),
    )
    hyperliquid_vault_address: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "CARRYME_WORKER_HYPERLIQUID_VAULT_ADDRESS",
            "CARRYME_API_HYPERLIQUID_VAULT_ADDRESS",
        ),
    )
    hyperliquid_api_wallet_private_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "CARRYME_WORKER_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
            "CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
        ),
    )

    model_config = SettingsConfigDict(
        env_prefix="CARRYME_WORKER_",
        extra="ignore",
        populate_by_name=True,
    )

    @field_validator(
        "universe_scan_extended_fee_profile",
        "universe_scan_paradex_fee_profile",
        "universe_scan_hyperliquid_fee_profile",
    )
    @classmethod
    def validate_universe_fee_profile(cls, value: str | None, info: ValidationInfo) -> str | None:
        """Fail fast when configured universe fee profiles are unknown."""

        if value is None:
            return None
        normalized = value.strip().lower()
        if not normalized:
            raise ValueError("fee profile name must not be empty")
        field_name = info.field_name
        if field_name is None:
            raise ValueError("fee profile validator requires a field name")
        venue = field_name.removeprefix("universe_scan_").removesuffix("_fee_profile")
        get_fee_profile(venue, normalized)
        return normalized

    @field_validator("watchlist_path")
    @classmethod
    def validate_watchlist_path(cls, value: str) -> str:
        """Fail fast when the configured watchlist path does not exist."""

        candidate = Path(value).expanduser()
        resolved = candidate if candidate.is_absolute() else (Path.cwd() / candidate).resolve()
        if not resolved.is_file():
            raise ValueError(f"watchlist_path does not exist or is not a file: {resolved}")
        return str(resolved)

    @model_validator(mode="after")
    def validate_live_credentials(self) -> "WorkerSettings":
        """Fail fast when live venue observation is enabled without required credentials."""

        missing: list[str] = []
        if self.extended_live_enabled and not self.extended_api_key:
            missing.append("extended_api_key")
        if self.paradex_live_enabled:
            if not self.paradex_account_address:
                missing.append("paradex_account_address")
            if not (self.paradex_private_key or self.paradex_bearer_token):
                missing.append("paradex_private_key|paradex_bearer_token")
        if self.hyperliquid_live_enabled:
            if not self.hyperliquid_account_address:
                missing.append("hyperliquid_account_address")
            if not self.hyperliquid_api_wallet_private_key:
                missing.append("hyperliquid_api_wallet_private_key")
        if missing:
            raise ValueError(
                "Missing required live credentials for enabled venues: " + ", ".join(missing)
            )
        return self
