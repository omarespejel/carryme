"""Public REST connector for Paradex."""

from __future__ import annotations

import asyncio
from typing import Any

import httpx
from carryme_models.market import MarketStats, TopOfBook

from carryme_connectors.base import (
    BaseHttpConnector,
    ConnectorError,
    normalize_market_symbols,
    parse_float,
)


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

    async def list_market_symbols(self) -> list[str]:
        payload = await self._request_json("GET", "/v1/markets", params={"market_status": "ACTIVE"})
        if not isinstance(payload, dict):
            raise ConnectorError("Paradex market list payload must be an object")
        rows = payload.get("results", [])
        if not isinstance(rows, list):
            raise ConnectorError("Paradex market list missing results list")
        symbols: list[str] = []
        for row in rows:
            if not isinstance(row, dict):
                raise ConnectorError("Paradex market row must be an object")
            symbol = row.get("symbol")
            if not isinstance(symbol, str):
                raise ConnectorError("Paradex market row missing symbol string")
            normalized_symbol = symbol.strip().upper()
            if normalized_symbol.endswith("-PERP"):
                symbols.append(normalized_symbol)
        return normalize_market_symbols(symbols)

    async def fetch_market_stats(self, symbol: str) -> MarketStats:
        summary_payload, market_payload = await asyncio.gather(
            self._request_json(
                "GET",
                "/v1/markets/summary",
                params={"market": symbol},
            ),
            self._request_json("GET", "/v1/markets", params={"market": symbol}),
        )
        if not isinstance(summary_payload, dict):
            raise ConnectorError("Paradex market summary payload must be an object")
        summary_row = _find_market(summary_payload.get("results", []), symbol)
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
        payload = await self._request_json("GET", f"/v1/orderbook/{symbol}/interactive")
        if not isinstance(payload, dict):
            raise ConnectorError("Paradex orderbook payload must be an object")
        best_bid = _first_level(payload.get("bids"), field="bids")
        best_ask = _first_level(payload.get("asks"), field="asks")
        best_bid_api = _price_size_pair(
            payload.get("best_bid_api"),
            field="best_bid_api",
            optional=True,
        )
        best_ask_api = _price_size_pair(
            payload.get("best_ask_api"),
            field="best_ask_api",
            optional=True,
        )
        best_bid_interactive = _price_size_pair(
            payload.get("best_bid_interactive"),
            field="best_bid_interactive",
            optional=True,
        )
        best_ask_interactive = _price_size_pair(
            payload.get("best_ask_interactive"),
            field="best_ask_interactive",
            optional=True,
        )
        return TopOfBook(
            best_bid_price=best_bid[0],
            best_bid_size=best_bid[1],
            best_ask_price=best_ask[0],
            best_ask_size=best_ask[1],
            best_bid_api_price=best_bid_api[0],
            best_bid_api_size=best_bid_api[1],
            best_ask_api_price=best_ask_api[0],
            best_ask_api_size=best_ask_api[1],
            best_bid_interactive_price=best_bid_interactive[0],
            best_bid_interactive_size=best_bid_interactive[1],
            best_ask_interactive_price=best_ask_interactive[0],
            best_ask_interactive_size=best_ask_interactive[1],
        )


def _find_market(rows: Any, symbol: str) -> dict[str, Any]:
    if not isinstance(rows, list):
        raise ConnectorError("Paradex market results must be a list")
    for row in rows:
        if isinstance(row, dict) and row.get("symbol") == symbol:
            return row
    raise ConnectorError(f"Paradex market {symbol} not found")


def _first_level(
    levels: Any,
    *,
    field: str,
) -> tuple[float | None, float | None]:
    if levels is None:
        raise ConnectorError(f"Paradex orderbook missing {field}")
    if not isinstance(levels, list):
        raise ConnectorError(f"Paradex orderbook {field} must be a list")
    if not levels:
        return None, None
    first = levels[0]
    return _price_size_pair(first, field=f"{field}[0]")


def _price_size_pair(
    raw: Any,
    *,
    field: str,
    optional: bool = False,
) -> tuple[float | None, float | None]:
    if raw is None:
        if optional:
            return None, None
        raise ConnectorError(f"Paradex orderbook missing {field}")
    if not isinstance(raw, list | tuple) or len(raw) < 2:
        raise ConnectorError(f"Paradex orderbook {field} must be [price, size]")
    return parse_float(raw[0]), parse_float(raw[1])
