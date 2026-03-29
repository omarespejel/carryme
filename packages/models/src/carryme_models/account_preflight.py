"""Authenticated account-state preflight models."""

from pydantic import BaseModel, Field


class VenueAccountPreflight(BaseModel):
    """Authenticated read-only account probe for one venue."""

    venue: str = Field(min_length=1)
    enabled: bool
    authenticated: bool
    ready: bool
    credential_mode: str = Field(min_length=1)
    missing_env_vars: list[str] = Field(default_factory=list)
    account_identifier: str | None = None
    account_status: str | None = None
    total_collateral: float | None = None
    available_to_trade: float | None = None
    free_collateral: float | None = None
    balance_count: int | None = Field(default=None, ge=0)
    position_count: int | None = Field(default=None, ge=0)
    notes: list[str] = Field(default_factory=list)
    blocking_reasons: list[str] = Field(default_factory=list)


class PaperTradeAccountPreflight(BaseModel):
    """Authenticated account-read readiness for a saved paper trade."""

    paper_trade_id: int
    label: str = Field(min_length=1)
    ready: bool
    venues: list[VenueAccountPreflight] = Field(default_factory=list)
    blocking_reasons: list[str] = Field(default_factory=list)
