"""Unsigned live order preview models."""

from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, Field


class VenueOrderPreview(BaseModel):
    """One venue-specific unsigned order template derived from a saved paper trade."""

    venue: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    fee_profile: str = Field(min_length=1)
    side: Literal["buy", "sell"]
    target_notional: float = Field(gt=0)
    quantity: float = Field(gt=0)
    quantity_text: str = Field(min_length=1)
    reference_price: float = Field(gt=0)
    reference_price_source: Literal["best_bid", "best_ask"]
    worst_acceptable_price: float = Field(gt=0)
    worst_price_text: str = Field(min_length=1)
    order_type: Literal["limit"] = "limit"
    time_in_force: Literal["ioc"] = "ioc"
    post_only: bool = False
    reduce_only: bool = False
    http_method: Literal["POST"] = "POST"
    endpoint_path_hint: str = Field(min_length=1)
    required_auth_env_vars: list[str] = Field(default_factory=list)
    auth_scheme: str = Field(min_length=1)
    payload: dict[str, Any] = Field(default_factory=dict)
    notes: list[str] = Field(default_factory=list)


class PaperTradeOrderPreview(BaseModel):
    """A deterministic pair of unsigned order templates for one saved paper trade."""

    paper_trade_id: int
    label: str = Field(min_length=1)
    generated_at: datetime
    slippage_tolerance_bps: int = Field(ge=0)
    preview_hash: str = Field(min_length=1)
    legs: list[VenueOrderPreview] = Field(min_length=1)
