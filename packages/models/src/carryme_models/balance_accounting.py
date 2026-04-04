"""Balance snapshot and delta models."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class VenueBalanceSnapshot(BaseModel):
    """One authenticated balance snapshot for one venue and paper trade."""

    snapshot_id: int | None = None
    captured_at: datetime
    paper_trade_id: int
    label: str = Field(min_length=1)
    stage: str = Field(min_length=1)
    venue: str = Field(min_length=1)
    total_collateral: float | None = Field(default=None, ge=0)
    available_to_trade: float | None = Field(default=None, ge=0)
    free_collateral: float | None = Field(default=None, ge=0)
    balance_assets: list[str] = Field(default_factory=list)
    position_symbols: list[str] = Field(default_factory=list)
    note: str | None = None


class VenueBalanceDelta(BaseModel):
    """Balance delta between the first and latest snapshot for one venue."""

    venue: str = Field(min_length=1)
    snapshot_count: int = Field(ge=0)
    first_captured_at: datetime
    latest_captured_at: datetime
    first_stage: str = Field(min_length=1)
    latest_stage: str = Field(min_length=1)
    first_total_collateral: float | None = Field(default=None, ge=0)
    latest_total_collateral: float | None = Field(default=None, ge=0)
    total_collateral_delta: float | None = None
    first_available_to_trade: float | None = Field(default=None, ge=0)
    latest_available_to_trade: float | None = Field(default=None, ge=0)
    available_to_trade_delta: float | None = None
    first_free_collateral: float | None = Field(default=None, ge=0)
    latest_free_collateral: float | None = Field(default=None, ge=0)
    free_collateral_delta: float | None = None


class PaperTradeBalanceDelta(BaseModel):
    """Per-paper-trade balance delta summary across all touched venues."""

    paper_trade_id: int
    label: str = Field(min_length=1)
    snapshot_count: int = Field(ge=0)
    venue_count: int = Field(ge=0)
    first_captured_at: datetime
    latest_captured_at: datetime
    total_collateral_delta: float | None = None
    total_available_to_trade_delta: float | None = None
    total_free_collateral_delta: float | None = None
    venues: list[VenueBalanceDelta] = Field(default_factory=list)


class BalanceAttributionPhase(BaseModel):
    """One attributed phase of a paper trade balance lifecycle."""

    phase: Literal["entry", "hold", "exit"]
    snapshot_count: int = Field(ge=0)
    venue_count: int = Field(ge=0)
    start_captured_at: datetime
    end_captured_at: datetime
    start_stage: str = Field(min_length=1)
    end_stage: str = Field(min_length=1)
    duration_seconds: float = Field(ge=0)
    total_collateral_delta: float | None = None
    total_available_to_trade_delta: float | None = None
    total_free_collateral_delta: float | None = None
    venues: list[VenueBalanceDelta] = Field(default_factory=list)


class PaperTradeBalanceAttribution(BaseModel):
    """Attributed entry, hold, and exit balance deltas for one paper trade."""

    paper_trade_id: int
    label: str = Field(min_length=1)
    snapshot_count: int = Field(ge=0)
    venue_count: int = Field(ge=0)
    funding_checkpoint_count: int = Field(ge=0)
    first_captured_at: datetime
    latest_captured_at: datetime
    total_collateral_delta: float | None = None
    total_available_to_trade_delta: float | None = None
    total_free_collateral_delta: float | None = None
    entry: BalanceAttributionPhase | None = None
    hold: BalanceAttributionPhase | None = None
    exit: BalanceAttributionPhase | None = None
