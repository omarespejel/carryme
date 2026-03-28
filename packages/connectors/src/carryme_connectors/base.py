"""Base helpers and protocols for public venue connectors."""

from __future__ import annotations

from typing import Any, Protocol

import httpx
from carryme_models.market import MarketStats, TopOfBook


class PublicVenueConnector(Protocol):
    """Protocol shared by all public REST connectors."""

    venue: str

    async def fetch_market_stats(self, symbol: str) -> MarketStats:
        """Return normalized market stats for a venue symbol."""

    async def fetch_top_of_book(self, symbol: str) -> TopOfBook:
        """Return best bid and ask information for a venue symbol."""


class ConnectorError(RuntimeError):
    """Raised when a connector cannot parse or find the requested market."""


class BaseHttpConnector:
    """Shared async HTTP helper for public venue connectors."""

    venue: str

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        self._client = client

    async def _request_json(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: dict[str, Any] | None = None,
    ) -> dict[str, Any] | list[Any]:
        if self._client is None:
            raise ConnectorError(f"{self.venue} connector requires an AsyncClient")

        response = await self._client.request(method, path, params=params, json=json_body)
        response.raise_for_status()
        payload = response.json()
        if not isinstance(payload, dict | list):
            raise ConnectorError(f"{self.venue} returned an unexpected payload shape")
        return payload


def parse_float(value: Any) -> float | None:
    """Convert numeric strings and numbers to floats when possible."""

    if value is None:
        return None
    if isinstance(value, int | float):
        return float(value)
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return None
        return float(stripped)
    raise ConnectorError(f"Cannot parse numeric value from {value!r}")
