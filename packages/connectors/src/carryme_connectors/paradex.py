"""Public REST connector for Paradex."""

from __future__ import annotations

from typing import Any

import httpx
from carryme_models.market import MarketStats, TopOfBook

from carryme_connectors.base import BaseHttpConnector, ConnectorError, parse_float


class ParadexPublicConnector(BaseHttpConnector):
    """Fetch public market data from Paradex."""

    venue = "paradex"

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        max_attempts: int = 3,
        base_backoff_seconds: float = 0.1,
    ) -> None:
        super().__init__(
            client,
            max_attempts=max_attempts,
            base_backoff_seconds=base_backoff_seconds,
        )

    async def fetch_market_stats(self, symbol: str) -> MarketStats:
        payload = await self._request_json("GET", "/v1/markets/summary", params={"market": symbol})
        if not isinstance(payload, dict):
            raise ConnectorError("Paradex market summary payload must be an object")
        rows = payload.get("results", [])
        row = _find_market(rows, symbol)
        return MarketStats(
            venue=self.venue,
            symbol=symbol,
            mark_price=parse_float(row.get("mark_price")),
            funding_rate=parse_float(row.get("funding_rate")),
            open_interest=parse_float(row.get("open_interest")),
            daily_volume=parse_float(row.get("volume_24h")),
            raw=payload,
        )

    async def fetch_top_of_book(self, symbol: str) -> TopOfBook:
        payload = await self._request_json("GET", f"/v1/bbo/{symbol}/interactive")
        if not isinstance(payload, dict):
            raise ConnectorError("Paradex BBO payload must be an object")
        return TopOfBook(
            best_bid_price=parse_float(payload.get("bid")),
            best_bid_size=parse_float(payload.get("bid_size")),
            best_ask_price=parse_float(payload.get("ask")),
            best_ask_size=parse_float(payload.get("ask_size")),
        )


def _find_market(rows: Any, symbol: str) -> dict[str, Any]:
    if not isinstance(rows, list):
        raise ConnectorError("Paradex market summary results must be a list")
    for row in rows:
        if isinstance(row, dict) and row.get("symbol") == symbol:
            return row
    raise ConnectorError(f"Paradex market {symbol} not found")
