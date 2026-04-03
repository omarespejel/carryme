"""Authenticated REST connector for Extended private account reads."""

from __future__ import annotations

from typing import Any

import httpx

from carryme_connectors.base import BaseHttpConnector, ConnectorError


class ExtendedPrivateConnector(BaseHttpConnector):
    """Fetch authenticated account state from Extended."""

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

    async def fetch_account(self) -> dict[str, Any]:
        payload = await self._request_json("GET", "/api/v1/user/account/info")
        if not isinstance(payload, dict):
            raise ConnectorError("Extended account payload must be an object")
        return payload

    async def fetch_balances(self) -> dict[str, Any] | list[Any]:
        return await self._request_json("GET", "/api/v1/user/balance")

    async def fetch_fees(self, symbol: str | None = None) -> dict[str, Any] | list[Any]:
        params = {"market": symbol} if symbol else None
        payload = await self._request_json("GET", "/api/v1/user/fees", params=params)
        if not isinstance(payload, dict | list):
            raise ConnectorError("Extended fees payload must be an object or list")
        return payload

    async def fetch_orders(self) -> dict[str, Any] | list[Any]:
        payload = await self._request_json("GET", "/api/v1/user/orders")
        if not isinstance(payload, dict | list):
            raise ConnectorError("Extended orders payload must be an object or list")
        return payload

    async def fetch_positions(self) -> dict[str, Any] | list[Any]:
        return await self._request_json("GET", "/api/v1/user/positions")
