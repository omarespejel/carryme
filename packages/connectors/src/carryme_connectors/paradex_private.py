"""Authenticated REST connector for Paradex private account reads."""

from __future__ import annotations

from typing import Any

import httpx

from carryme_connectors.base import BaseHttpConnector, ConnectorError


class ParadexPrivateConnector(BaseHttpConnector):
    """Fetch authenticated account state from Paradex."""

    venue = "paradex"

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        super().__init__(client)

    async def fetch_account(self) -> dict[str, Any]:
        payload = await self._request_json("GET", "/v1/account")
        if not isinstance(payload, dict):
            raise ConnectorError("Paradex account payload must be an object")
        return payload

    async def fetch_balances(self) -> dict[str, Any] | list[Any]:
        payload = await self._request_json("GET", "/v1/balance")
        if not isinstance(payload, dict | list):
            raise ConnectorError("Paradex balances payload must be an object or list")
        return payload

    async def fetch_positions(self) -> dict[str, Any] | list[Any]:
        payload = await self._request_json("GET", "/v1/positions")
        if not isinstance(payload, dict | list):
            raise ConnectorError("Paradex positions payload must be an object or list")
        return payload
