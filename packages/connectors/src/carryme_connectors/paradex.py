"""Public REST connector for Paradex."""

from __future__ import annotations

from typing import Any

import httpx
from carryme_models.market import MarketStats, TopOfBook

from carryme_connectors.base import BaseHttpConnector, ConnectorError, parse_float


class ParadexPublicConnector(BaseHttpConnector):
    """Fetch public market data from Paradex."""

    venue = "paradex"

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        super().__init__(client)

    async def fetch_market_stats(self, symbol: str) -> MarketStats:
        summary_payload = await self._request_json(
            "GET",
            "/v1/markets/summary",
            params={"market": symbol},
        )
        if not isinstance(summary_payload, dict):
            raise ConnectorError("Paradex market summary payload must be an object")
        summary_row = _find_market(summary_payload.get("results", []), symbol)

        market_payload = await self._request_json("GET", "/v1/markets", params={"market": symbol})
        if not isinstance(market_payload, dict):
            raise ConnectorError("Paradex market config payload must be an object")
        market_row = _find_market(market_payload.get("results", []), symbol)
        combined_raw = {**market_row, **summary_row, "config": market_row, "summary": summary_row}
        return MarketStats(
            venue=self.venue,
            symbol=symbol,
            mark_price=parse_float(summary_row.get("mark_price")),
            funding_rate=parse_float(summary_row.get("funding_rate")),
            open_interest=parse_float(summary_row.get("open_interest")),
            daily_volume=parse_float(summary_row.get("volume_24h")),
            raw=combined_raw,
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
