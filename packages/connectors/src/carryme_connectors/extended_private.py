"""Authenticated REST connector for Extended private account reads."""

from __future__ import annotations

from typing import Any

import httpx

from carryme_connectors.base import BaseHttpConnector, ConnectorError


class ExtendedPrivateConnector(BaseHttpConnector):
    """Fetch authenticated account state from Extended."""

    venue = "extended"

    def __init__(self, client: httpx.AsyncClient | None = None) -> None:
        super().__init__(client)

    async def fetch_account(self) -> dict[str, Any]:
        payload = await self._request_json("GET", "/api/v1/user/account/info")
        if not isinstance(payload, dict):
            raise ConnectorError("Extended account payload must be an object")
        return payload

    async def fetch_balances(self) -> dict[str, Any] | list[Any]:
        payload = await self._request_json("GET", "/api/v1/user/balance")
        if not isinstance(payload, dict | list):
            raise ConnectorError("Extended balances payload must be an object or list")
        return payload

    async def fetch_positions(self) -> dict[str, Any] | list[Any]:
        payload = await self._request_json("GET", "/api/v1/user/positions")
        if not isinstance(payload, dict | list):
            raise ConnectorError("Extended positions payload must be an object or list")
        return payload
