"""Base helpers and protocols for public venue connectors."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from email.utils import parsedate_to_datetime
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
                status_code = exc.response.status_code
                if self._is_retryable_status(status_code) and attempt < self._max_attempts:
                    delay_seconds = self._retry_after_seconds(exc.response)
                    await self._sleep_before_retry(attempt, delay_seconds=delay_seconds)
                    continue
                raise ConnectorError(
                    f"{self.venue} request failed with status {status_code}"
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

    def _is_retryable_status(self, status_code: int) -> bool:
        """Return True for transient HTTP statuses that should be retried."""

        return status_code == 429 or status_code >= 500

    def _retry_after_seconds(self, response: httpx.Response) -> float | None:
        """Parse Retry-After if present and usable."""

        retry_after = response.headers.get("Retry-After")
        if retry_after is None:
            return None
        try:
            delay_seconds = float(retry_after)
        except ValueError:
            try:
                retry_at = parsedate_to_datetime(retry_after)
            except (TypeError, ValueError, IndexError):
                return None
            if retry_at.tzinfo is None:
                retry_at = retry_at.replace(tzinfo=UTC)
            delay_seconds = (retry_at - datetime.now(tz=UTC)).total_seconds()
        return max(delay_seconds, 0.0)

    async def _sleep_before_retry(
        self, attempt: int, *, delay_seconds: float | None = None
    ) -> None:
        """Back off exponentially between transient request failures."""

        if delay_seconds is None:
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
