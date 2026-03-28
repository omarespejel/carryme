"""Market data models shared across venue connectors."""

from typing import Any

from pydantic import BaseModel, Field


class TopOfBook(BaseModel):
    """Best bid/ask snapshot for a market."""

    best_bid_price: float | None = None
    best_bid_size: float | None = None
    best_ask_price: float | None = None
    best_ask_size: float | None = None


class MarketStats(BaseModel):
    """Normalized market statistics for a venue symbol."""

    venue: str = Field(min_length=1)
    symbol: str = Field(min_length=1)
    mark_price: float | None = None
    funding_rate: float | None = None
    open_interest: float | None = None
    daily_volume: float | None = None
    top_of_book: TopOfBook | None = None
    raw: dict[str, Any] = Field(default_factory=dict)
