"""Worker configuration models."""

from pathlib import Path
from typing import Literal

from carryme_models import SUPPORTED_UNIVERSE_VENUES
from carryme_normalizers import get_fee_profile
from carryme_storage.db import redact_database_url
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
    universe_scan_snapshot_batch_size: int = Field(default=6, gt=0)
    universe_scan_extended_snapshot_concurrency: int = Field(default=2, gt=0)
    universe_scan_paradex_snapshot_concurrency: int = Field(default=2, gt=0)
    universe_scan_hyperliquid_snapshot_concurrency: int = Field(default=2, gt=0)
    approved_canary_scan_interval_seconds: int = Field(default=30, gt=0)
    approved_canary_scan_max_backoff_seconds: int = Field(default=300, gt=0)
    launch_ready_canary_interval_seconds: int = Field(default=15, gt=0)
    launch_ready_canary_max_backoff_seconds: int = Field(default=120, gt=0)
    stable_canary_launch_interval_seconds: int = Field(default=30, gt=0)
    stable_canary_launch_max_backoff_seconds: int = Field(default=300, gt=0)
    # `0` is intentionally fail-closed: any active live execution blocks unattended launch.
    stable_canary_launch_max_active_live_executions: int = Field(default=0, ge=0)
    # Fetch enough rows to decide whether the blocking threshold is exceeded.
    stable_canary_launch_active_execution_limit: int = Field(default=20, gt=0)
    stable_canary_launch_max_total_live_notional: float | None = Field(
        default=None,
        ge=0,
    )
    stable_canary_launch_max_live_notional_per_venue: float | None = Field(
        default=None,
        ge=0,
    )
    stable_canary_launch_recent_closed_trade_limit: int = Field(default=10, gt=0)
    stable_canary_launch_max_recent_negative_total_collateral: float | None = Field(
        default=None,
        ge=0,
    )
    stable_canary_launch_max_consecutive_losing_trades: int | None = Field(
        default=None,
        ge=1,
    )
    stable_canary_launch_global_cooldown_seconds: int | None = Field(
        default=None,
        ge=0,
    )
    stable_canary_launch_label_cooldown_seconds: int | None = Field(
        default=None,
        ge=0,
    )
    stable_canary_launch_recent_launch_window_seconds: int | None = Field(
        default=None,
        gt=0,
    )
    stable_canary_launch_max_launches_per_window: int | None = Field(
        default=None,
        ge=1,
    )
    stable_canary_launch_max_label_launches_per_window: int | None = Field(
        default=None,
        ge=1,
    )
    stable_canary_launch_min_execution_quality_score: float | None = Field(
        default=None,
        ge=0,
    )
    stable_canary_launch_min_execution_samples: int | None = Field(
        default=None,
        ge=0,
    )
    stable_canary_launch_min_daily_volume: float | None = Field(
        default=None,
        ge=0,
    )
    stable_canary_launch_min_deployable_notional: float | None = Field(
        default=None,
        ge=0,
    )
    stable_canary_launch_min_expected_one_day_round_trip_pnl: float | None = Field(
        default=None,
        ge=0,
    )
    stable_canary_launch_block_adverse_latest_outcome: bool = False
    launch_ready_canary_max_snapshot_age_seconds: int = Field(default=300, gt=0)
    stable_launch_ready_min_snapshot_count: int = Field(default=2, gt=0)
    stable_launch_ready_min_stable_seconds: float = Field(default=30.0, ge=0)
    stable_canary_launch_shadow_mode: bool = False
    stable_canary_launch_close_position: bool = True
    stable_launch_ready_min_edge_retention_ratio: float = Field(
        default=0.7,
        ge=0,
        le=1,
    )
    stable_launch_ready_max_entry_break_even_funding_windows: float = Field(
        default=6.0,
        gt=0,
    )
    stable_launch_ready_max_round_trip_break_even_funding_windows: float = Field(
        default=12.0,
        gt=0,
    )
    stable_launch_ready_alert_webhook_url: str | None = None
    stable_launch_ready_alert_webhook_timeout_seconds: float = Field(default=10.0, gt=0)
    approved_canary_alert_max_snapshot_age_seconds: int = Field(default=300, gt=0)
    approved_canary_alert_webhook_url: str | None = None
    approved_canary_alert_webhook_timeout_seconds: float = Field(default=10.0, gt=0)
    system_state_observation_interval_seconds: int = Field(default=15, gt=0)
    system_state_observation_max_backoff_seconds: int = Field(default=120, gt=0)
    system_state_alert_webhook_url: str | None = None
    system_state_alert_webhook_timeout_seconds: float = Field(default=10.0, gt=0)
    execution_observation_interval_seconds: int = Field(default=10, gt=0)
    execution_observation_max_backoff_seconds: int = Field(default=60, gt=0)
    execution_observation_max_age_seconds: int = Field(default=1800, gt=0)
    execution_balance_checkpoint_enabled: bool = True
    execution_auto_pair_close_enabled: bool = False
    execution_auto_pair_close_shadow_mode: bool = False
    execution_auto_pair_close_max_snapshot_age_seconds: int = Field(default=300, gt=0)
    execution_auto_pair_close_live_revalidation_timeout_seconds: float = Field(
        default=20.0,
        gt=0,
    )
    execution_auto_pair_close_min_entry_edge_retention_ratio: float = Field(
        default=0.35,
        ge=0,
        le=1,
    )
    execution_auto_pair_close_max_round_trip_break_even_hold_windows: float = Field(
        default=2.5,
        gt=0,
    )
    execution_auto_pair_close_max_hold_windows: float = Field(default=2.0, gt=0)
    execution_auto_pair_close_min_profit_total_collateral: float | None = Field(
        default=None,
        ge=0,
        description=(
            "Minimum absolute profit in summed total-collateral units across the live trade's "
            "tracked venues before profit-giveback auto-close can trigger."
        ),
    )
    execution_auto_pair_close_max_profit_giveback_ratio: float | None = Field(
        default=None,
        gt=0,
        le=1,
        description=(
            "Maximum allowed fraction of peak summed total-collateral profit that can be given "
            "back before profit-protection auto-close triggers."
        ),
    )
    execution_auto_pair_close_timeout_seconds: float = Field(default=30.0, gt=0)
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
    approved_canary_scan_min_execution_quality_score: float = 0.45
    approved_canary_scan_min_execution_samples: int = 0
    approved_canary_scan_min_route_stability_weight: float = 0.0
    approved_canary_scan_min_route_presence_ratio: float = 0.0
    approved_canary_scan_min_route_samples: int = 0
    # `0` means scan every approved label; concurrency still limits fan-out.
    approved_canary_scan_limit: int = Field(default=0, ge=0)
    approved_canary_scan_concurrency: int = Field(default=5, gt=0)
    approved_canary_exact_scan_limit: int = 25
    approved_canary_scan_include_symbols: tuple[str, ...] = ()
    approved_canary_scan_exclude_symbols: tuple[str, ...] = ()
    approved_canary_scan_exclude_tags: tuple[str, ...] = ("meme", "political")
    stop_signals: tuple[Literal["SIGINT", "SIGTERM"], ...] = ("SIGINT", "SIGTERM")
    database_path: str = Field(
        default="data/carryme.sqlite3",
        validation_alias=AliasChoices(
            "database_path",
            "CARRYME_WORKER_DATABASE_PATH",
            "CARRYME_WORKER_DATABASE_URL",
            "CARRYME_DATABASE_URL",
            "DATABASE_URL",
        ),
    )
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
    extended_stark_private_key: str | None = Field(
        default=None,
        validation_alias=AliasChoices(
            "CARRYME_WORKER_EXTENDED_STARK_PRIVATE_KEY",
            "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY",
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
    paradex_recv_window_ms: int = Field(
        default=300000,
        gt=0,
        validation_alias=AliasChoices(
            "CARRYME_WORKER_PARADEX_RECV_WINDOW_MS",
            "CARRYME_API_PARADEX_RECV_WINDOW_MS",
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

    @model_validator(mode="after")
    def validate_stable_launch_rate_caps(self) -> "WorkerSettings":
        """Require a launch window whenever stable launch rate caps are configured."""

        if (
            self.stable_canary_launch_recent_launch_window_seconds is None
            and (
                self.stable_canary_launch_max_launches_per_window is not None
                or self.stable_canary_launch_max_label_launches_per_window is not None
            )
        ):
            raise ValueError(
                "stable_canary_launch_recent_launch_window_seconds is required when "
                "stable launch rate caps are configured"
            )
        return self

    @property
    def database_target(self) -> str:
        """Return a log-safe identifier for the configured database."""

        return redact_database_url(self.database_path)
