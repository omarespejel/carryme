"""Execution accounting models."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, Field


class ExecutionLegAccounting(BaseModel):
    """Derived fill and fee accounting for one executed leg."""

    venue: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    fee_profile: str = Field(min_length=1)
    side: Literal["buy", "sell"]
    status: Literal["accepted", "rejected", "submitted", "partial"]
    auth_usage: str | None = None
    derived_fill_state: Literal["filled", "partial_fill", "unfilled", "unknown"]
    filled_size: float | None = Field(default=None, ge=0)
    avg_fill_price: float | None = Field(default=None, ge=0)
    filled_notional: float | None = Field(default=None, ge=0)
    estimated_fee_rate: float | None = Field(default=None, ge=0)
    estimated_fee_paid: float | None = Field(default=None, ge=0)
    notes: list[str] = Field(default_factory=list)


class ExecutionAccountingSummary(BaseModel):
    """Derived accounting summary for one journaled execution entry."""

    execution_entry_id: int | None = None
    paper_trade_id: int | None = None
    label: str = Field(min_length=1)
    canonical_symbol: str = Field(min_length=1)
    adapter: str = Field(min_length=1)
    mode: Literal["mock", "live"]
    status: Literal["accepted", "rejected", "submitted", "partial"]
    executed_at: datetime
    total_leg_count: int = Field(ge=0)
    filled_leg_count: int = Field(ge=0)
    total_filled_notional: float = Field(ge=0)
    total_estimated_fee_paid: float = Field(ge=0)
    legs: list[ExecutionLegAccounting] = Field(default_factory=list)
    notes: list[str] = Field(default_factory=list)


class PaperTradeAccountingSummary(BaseModel):
    """Aggregated accounting summary across all execution entries for one paper trade."""

    paper_trade_id: int
    label: str = Field(min_length=1)
    canonical_symbol: str = Field(min_length=1)
    execution_count: int = Field(ge=0)
    latest_executed_at: datetime | None = None
    total_filled_notional: float = Field(ge=0)
    total_estimated_fee_paid: float = Field(ge=0)
    entries: list[ExecutionAccountingSummary] = Field(default_factory=list)


class RouteAccountingSummary(BaseModel):
    """Aggregated accounting across execution history for one route label."""

    label: str = Field(min_length=1)
    canonical_symbol: str = Field(min_length=1)
    short_venue: str = Field(min_length=1)
    long_venue: str = Field(min_length=1)
    short_fee_profile: str = Field(min_length=1)
    long_fee_profile: str = Field(min_length=1)
    execution_count: int = Field(ge=0)
    paper_trade_count: int = Field(ge=0)
    latest_executed_at: datetime | None = None
    total_filled_notional: float = Field(ge=0)
    total_estimated_fee_paid: float = Field(ge=0)
