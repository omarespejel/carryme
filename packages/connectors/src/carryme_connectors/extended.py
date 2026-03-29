"""Public REST connector for Extended."""

from __future__ import annotations

from typing import Any

import httpx
from carryme_models.market import MarketStats, TopOfBook

from carryme_connectors.base import BaseHttpConnector, ConnectorError, parse_float


class ExtendedPublicConnector(BaseHttpConnector):
    """Fetch public market data from Extended's Starknet API."""

    venue = "extended"

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        super().__init__(client)

    async def fetch_market_stats(self, symbol: str) -> MarketStats:
        payload = await self._request_json("GET", "/api/v1/info/markets", params={"market": symbol})
        if not isinstance(payload, dict):
            raise ConnectorError("Extended market metadata payload must be an object")
        rows = payload.get("data", [])
        if not isinstance(rows, list):
            raise ConnectorError("Extended market metadata missing data list")
        data = _find_market(rows, symbol)
        market_stats = data.get("marketStats", {})
        if not isinstance(market_stats, dict):
            raise ConnectorError("Extended market metadata missing marketStats object")
        return MarketStats(
            venue=self.venue,
            symbol=symbol,
            mark_price=parse_float(market_stats.get("markPrice")),
            funding_rate=parse_float(market_stats.get("fundingRate")),
            open_interest=parse_float(market_stats.get("openInterest")),
            daily_volume=parse_float(market_stats.get("dailyVolume")),
            raw=data,
        )

    async def fetch_top_of_book(self, symbol: str) -> TopOfBook:
        payload = await self._request_json("GET", f"/api/v1/info/markets/{symbol}/orderbook")
        if not isinstance(payload, dict):
            raise ConnectorError("Extended orderbook payload must be an object")
        data = payload.get("data", {})
        if not isinstance(data, dict):
            raise ConnectorError("Extended orderbook missing data object")
        best_bid = _first_level(data.get("bid"))
        best_ask = _first_level(data.get("ask"))
        return TopOfBook(
            best_bid_price=parse_float(best_bid.get("price")) if best_bid else None,
            best_bid_size=parse_float(best_bid.get("qty")) if best_bid else None,
            best_ask_price=parse_float(best_ask.get("price")) if best_ask else None,
            best_ask_size=parse_float(best_ask.get("qty")) if best_ask else None,
        )


def _first_level(value: Any) -> dict[str, Any] | None:
    if isinstance(value, list) and value:
        item = value[0]
        if isinstance(item, dict):
            return item
    return None


def _find_market(rows: Any, symbol: str) -> dict[str, Any]:
    if not isinstance(rows, list):
        raise ConnectorError("Extended market data must be a list")
    for row in rows:
        if isinstance(row, dict) and row.get("name") == symbol:
            return row
    raise ConnectorError(f"Extended market {symbol} not found")
