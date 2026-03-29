"""Public REST connector for Hyperliquid."""

from __future__ import annotations

from typing import Any

import httpx
from carryme_models.market import MarketStats, TopOfBook

from carryme_connectors.base import BaseHttpConnector, ConnectorError, parse_float


class HyperliquidPublicConnector(BaseHttpConnector):
    """Fetch public market data from Hyperliquid's info endpoint."""

    venue = "hyperliquid"

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
        payload = await self._request_json("POST", "/info", json_body={"type": "metaAndAssetCtxs"})
        if not isinstance(payload, list) or len(payload) != 2:
            raise ConnectorError("Hyperliquid metaAndAssetCtxs payload must be a two-item list")
        universe, contexts = payload
        row = _find_context(universe, contexts, symbol)
        return MarketStats(
            venue=self.venue,
            symbol=symbol,
            mark_price=parse_float(row.get("markPx")),
            funding_rate=parse_float(row.get("funding")),
            open_interest=parse_float(row.get("openInterest")),
            daily_volume=parse_float(row.get("dayNtlVlm")),
            raw=payload,
        )

    async def fetch_top_of_book(self, symbol: str) -> TopOfBook:
        payload = await self._request_json(
            "POST",
            "/info",
            json_body={"type": "l2Book", "coin": symbol},
        )
        if not isinstance(payload, dict):
            raise ConnectorError("Hyperliquid l2Book payload must be an object")
        levels = payload.get("levels")
        best_bid = _first_book_level(levels, 0)
        best_ask = _first_book_level(levels, 1)
        return TopOfBook(
            best_bid_price=parse_float(best_bid.get("px")) if best_bid else None,
            best_bid_size=parse_float(best_bid.get("sz")) if best_bid else None,
            best_ask_price=parse_float(best_ask.get("px")) if best_ask else None,
            best_ask_size=parse_float(best_ask.get("sz")) if best_ask else None,
        )


def _find_context(universe: Any, contexts: Any, symbol: str) -> dict[str, Any]:
    if not isinstance(universe, dict):
        raise ConnectorError("Hyperliquid universe metadata must be an object")
    rows = universe.get("universe", [])
    if not isinstance(rows, list) or not isinstance(contexts, list):
        raise ConnectorError("Hyperliquid universe/context payloads must be lists")
    for index, entry in enumerate(rows):
        if isinstance(entry, dict) and entry.get("name") == symbol:
            if index >= len(contexts):
                raise ConnectorError(f"Hyperliquid contexts missing entry for {symbol}")
            row = contexts[index]
            if isinstance(row, dict):
                return row
            break
    raise ConnectorError(f"Hyperliquid market {symbol} not found")


def _first_book_level(levels: Any, side_index: int) -> dict[str, Any] | None:
    if not isinstance(levels, list):
        raise ConnectorError("Hyperliquid orderbook levels must be a list")
    if len(levels) <= side_index:
        return None
    side = levels[side_index]
    if not isinstance(side, list):
        raise ConnectorError("Hyperliquid orderbook side levels must be lists")
    if not side:
        return None
    first_level = side[0]
    if not isinstance(first_level, dict):
        raise ConnectorError("Hyperliquid orderbook levels must contain objects")
    return first_level
