"""Public REST connector for Extended."""

from __future__ import annotations

import logging
from typing import Any

import httpx
from carryme_models.market import MarketStats, TopOfBook

from carryme_connectors.base import BaseHttpConnector, ConnectorError, parse_float

_logger = logging.getLogger(__name__)


class ExtendedPublicConnector(BaseHttpConnector):
    """Fetch public market data from Extended's Starknet API."""

    venue = "extended"

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
        payload = await self._request_json("GET", "/api/v1/info/markets", params={"market": symbol})
        if not isinstance(payload, dict):
            raise ConnectorError("Extended market stats payload must be an object")

        data = payload.get("data")
        raw: dict[str, Any]
        market_stats: dict[str, Any]
        if isinstance(data, list):
            raw = _find_market(data, symbol)
            market_stats_value = raw.get("marketStats")
            if not isinstance(market_stats_value, dict):
                raise ConnectorError("Extended market metadata missing marketStats object")
            market_stats = market_stats_value
        elif isinstance(data, dict):
            raw = data
            market_stats = data
            if "tradingConfig" not in raw:
                _logger.warning(
                    "Extended stats payload for %s omitted tradingConfig; "
                    "order_preview may run without snapping or minimum-notional enforcement",
                    symbol,
                )
        else:
            raise ConnectorError("Extended market stats missing data object")

        return MarketStats(
            venue=self.venue,
            symbol=symbol,
            mark_price=parse_float(market_stats.get("markPrice")),
            funding_rate=parse_float(market_stats.get("fundingRate")),
            open_interest=parse_float(market_stats.get("openInterest")),
            daily_volume=parse_float(market_stats.get("dailyVolume")),
            raw=raw,
        )

    async def fetch_top_of_book(self, symbol: str) -> TopOfBook:
        payload = await self._request_json("GET", f"/api/v1/info/markets/{symbol}/orderbook")
        if not isinstance(payload, dict):
            raise ConnectorError("Extended orderbook payload must be an object")
        data = payload.get("data")
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
