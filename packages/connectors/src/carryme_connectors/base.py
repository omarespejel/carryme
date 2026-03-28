"""Base helpers and protocols for public venue connectors."""

from __future__ import annotations

import asyncio
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
    """Shared async HTTP helper for public venue connectors.

    Callers should configure request timeouts on the AsyncClient they pass in so
    venue-specific latency budgets stay explicit at the call site.
    """

    venue: str

    def __init__(
        self,
        client: httpx.AsyncClient | None = None,
        *,
        max_attempts: int = 3,
        base_backoff_seconds: float = 0.1,
    ) -> None:
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least 1")
        if base_backoff_seconds < 0:
            raise ValueError("base_backoff_seconds must be non-negative")
        self._client = client
        self._max_attempts = max_attempts
        self._base_backoff_seconds = base_backoff_seconds

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

        for attempt in range(1, self._max_attempts + 1):
            try:
                response = await self._client.request(method, path, params=params, json=json_body)
                response.raise_for_status()
                payload = response.json()
                if not isinstance(payload, dict | list):
                    raise ConnectorError(f"{self.venue} returned an unexpected payload shape")
                return payload
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code >= 500 and attempt < self._max_attempts:
                    await self._sleep_before_retry(attempt)
                    continue
                raise ConnectorError(
                    f"{self.venue} request failed with status {exc.response.status_code}"
                ) from exc
            except httpx.TransportError as exc:
                if attempt < self._max_attempts:
                    await self._sleep_before_retry(attempt)
                    continue
                raise ConnectorError(
                    f"{self.venue} request failed after {attempt} attempts"
                ) from exc
            except ValueError as exc:
                raise ConnectorError(f"{self.venue} returned invalid JSON") from exc

        raise ConnectorError(f"{self.venue} request exhausted all retry attempts")

    async def _sleep_before_retry(self, attempt: int) -> None:
        """Back off exponentially between transient request failures."""

        delay_seconds = self._base_backoff_seconds * (2 ** (attempt - 1))
        await asyncio.sleep(delay_seconds)


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
        try:
            return float(stripped)
        except ValueError as exc:
            raise ConnectorError(f"Cannot parse numeric value from {value!r}") from exc
    raise ConnectorError(f"Cannot parse numeric value from {value!r}")
