"""Market data models shared across venue connectors."""

from datetime import UTC, datetime
from typing import Any

from pydantic import BaseModel, Field


class TopOfBook(BaseModel):
    """Best bid/ask snapshot for a market."""

    best_bid_price: float | None = None
    best_bid_size: float | None = None
    best_ask_price: float | None = None
    best_ask_size: float | None = None
    best_bid_api_price: float | None = None
    best_bid_api_size: float | None = None
    best_ask_api_price: float | None = None
    best_ask_api_size: float | None = None
    best_bid_interactive_price: float | None = None
    best_bid_interactive_size: float | None = None
    best_ask_interactive_price: float | None = None
    best_ask_interactive_size: float | None = None


class MarketStats(BaseModel):
    """Normalized market statistics for a venue symbol."""

    venue: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    captured_at: datetime = Field(
        default_factory=lambda: datetime.now(UTC),
        description="UTC timestamp when the market snapshot was captured.",
    )
    mark_price: float | None = Field(
        default=None,
        description="Mark price quoted in the venue's quote currency.",
    )
    funding_rate: float | None = Field(
        default=None,
        description="Funding rate in venue-native units; normalize before cross-venue comparison.",
    )
    open_interest: float | None = Field(
        default=None,
        description="Open interest in venue-native units reported by the source venue.",
    )
    daily_volume: float | None = Field(
        default=None,
        description="Rolling 24h volume in venue-native units reported by the source venue.",
    )
    top_of_book: TopOfBook | None = Field(
        default=None,
        description="Best bid/ask snapshot captured with the market stats when available.",
    )
    raw: dict[str, Any] | list[Any] = Field(
        default_factory=dict,
        description="Raw response payload captured from the source venue.",
    )
