"""Authenticated account-state preflight services."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol, TypedDict, cast

import httpx
from carryme_connectors import (
    ConnectorError,
    ExtendedPrivateConnector,
    ParadexJwtTokenProvider,
    ParadexPrivateConnector,
)
from carryme_connectors.base import parse_float
from carryme_models import (
    PaperTradeAccountPreflight,
    PaperTradeEntry,
    VenueAccountPreflight,
)


class VenueAccountConfig(TypedDict):
    """Configured authenticated-read credentials for one venue."""

    enabled: bool
    credentials: dict[str, str | None]


AccountPreflightConfigMap = dict[str, VenueAccountConfig]


class VenueAccountProbe(Protocol):
    """Protocol for one venue-specific authenticated account probe."""

    async def probe(self, config: VenueAccountConfig) -> VenueAccountPreflight: ...


class ParadexJwtTokenIssuer(Protocol):
    """Protocol for issuing short-lived Paradex JWTs."""

    async def issue_jwt_token(
        self,
        *,
        account_address: str,
        private_key: str,
        client: httpx.AsyncClient | None = None,
        now: int | None = None,
    ) -> str: ...


ACCOUNT_CONNECTOR_BASE_URLS: dict[str, str] = {
    "extended": "https://api.starknet.extended.exchange",
    "paradex": "https://api.prod.paradex.trade",
}


@dataclass
class AccountPreflightService:
    """Probe authenticated venue account state for the current live stack."""

    probes: dict[str, VenueAccountProbe] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.probes:
            self.probes = {
                "extended": ExtendedAccountProbe(),
                "paradex": ParadexAccountProbe(),
            }

    async def probe_venues(
        self,
        configs: AccountPreflightConfigMap,
    ) -> list[VenueAccountPreflight]:
        """Probe all supported authenticated-read venues."""

        tasks = [
            self.probes[venue].probe(configs.get(venue, {"enabled": False, "credentials": {}}))
            for venue in self.probes
        ]
        return list(await asyncio.gather(*tasks))

    async def probe_paper_trade(
        self,
        paper_trade: PaperTradeEntry,
        configs: AccountPreflightConfigMap,
    ) -> PaperTradeAccountPreflight:
        """Probe only the venues touched by one saved paper trade."""

        all_statuses = {item.venue: item for item in await self.probe_venues(configs)}
        venue_names = [paper_trade.intent.long_leg.venue, paper_trade.intent.short_leg.venue]
        selected_names: list[str] = []
        for venue in venue_names:
            if venue not in selected_names:
                selected_names.append(venue)
        selected = [all_statuses[venue] for venue in selected_names]

        blocking_reasons: list[str] = []
        for status in selected:
            if not status.enabled:
                blocking_reasons.append(f"Venue {status.venue} account preflight is not enabled")
            blocking_reasons.extend(status.blocking_reasons)

        return PaperTradeAccountPreflight(
            paper_trade_id=paper_trade.entry_id or 0,
            label=paper_trade.intent.label,
            ready=not blocking_reasons,
            venues=selected,
            blocking_reasons=blocking_reasons,
        )


class ExtendedAccountProbe:
    """Authenticated read-only account probe for Extended."""

    venue = "extended"

    async def probe(self, config: VenueAccountConfig) -> VenueAccountPreflight:
        enabled = bool(config["enabled"])
        api_key = config["credentials"].get("api_key")
        missing = [] if api_key else ["CARRYME_API_EXTENDED_API_KEY"]
        if not enabled or missing:
            return VenueAccountPreflight(
                venue=self.venue,
                enabled=enabled,
                authenticated=False,
                ready=False,
                credential_mode="api_key",
                missing_env_vars=missing,
                blocking_reasons=_blocking_reasons(enabled, missing, self.venue),
                notes=["Extended authenticated account reads use the API key header."],
            )

        headers = {"x-api-key": cast(str, api_key)}
        async with httpx.AsyncClient(
            base_url=ACCOUNT_CONNECTOR_BASE_URLS[self.venue],
            headers=headers,
            timeout=15.0,
        ) as client:
            connector = ExtendedPrivateConnector(client)
            try:
                account = await connector.fetch_account()
                balances = await _fetch_extended_optional_rows(
                    connector.fetch_balances,
                    resource_label="balance",
                )
                positions = await _fetch_extended_optional_rows(
                    connector.fetch_positions,
                    resource_label="positions",
                )
            except (ConnectorError, httpx.HTTPError) as exc:
                return VenueAccountPreflight(
                    venue=self.venue,
                    enabled=enabled,
                    authenticated=False,
                    ready=False,
                    credential_mode="api_key",
                    missing_env_vars=[],
                    blocking_reasons=[f"Extended authenticated read failed: {exc}"],
                    notes=["Check the Extended API key and target subaccount."],
                )

        account_body = _unwrap_payload(account)
        balance_body = _unwrap_payload(balances) if isinstance(balances, dict) else {}
        return VenueAccountPreflight(
            venue=self.venue,
            enabled=enabled,
            authenticated=True,
            ready=True,
            credential_mode="api_key",
            account_identifier=_pick_string(
                account_body,
                "subAccountId",
                "subaccountId",
                "accountId",
                "id",
                "address",
            ),
            account_status=_pick_string(account_body, "status", "accountStatus"),
            total_collateral=(
                _pick_float(account_body, "equity", "balance", "totalCollateral")
                or _pick_float(balance_body, "equity", "balance", "totalCollateral")
            ),
            available_to_trade=_pick_float(
                account_body,
                "availableForTrade",
                "availableBalance",
                "available_to_trade",
            )
            or _pick_float(
                balance_body,
                "availableForTrade",
                "availableBalance",
                "available_to_trade",
            ),
            balance_count=_count_rows(balances),
            position_count=_count_rows(positions),
            balance_assets=_extract_balance_assets(balances),
            position_symbols=_extract_position_symbols(positions),
            notes=[
                (
                    "Extended account preflight completed using authenticated private GETs. "
                    "Missing balance or position rows are treated as an empty account state."
                )
            ],
        )


class ParadexAccountProbe:
    """Authenticated read-only account probe for Paradex."""

    venue = "paradex"

    def __init__(self, token_provider: ParadexJwtTokenIssuer | None = None) -> None:
        self._token_provider = token_provider or ParadexJwtTokenProvider()

    async def probe(self, config: VenueAccountConfig) -> VenueAccountPreflight:
        enabled = bool(config["enabled"])
        account_address = config["credentials"].get("account_address")
        bearer_token = config["credentials"].get("bearer_token")
        private_key = config["credentials"].get("private_key")
        missing = []
        if not account_address:
            missing.append("CARRYME_API_PARADEX_ACCOUNT_ADDRESS")
        if not bearer_token and not private_key:
            missing.append("CARRYME_API_PARADEX_PRIVATE_KEY")
        if not enabled or missing:
            return VenueAccountPreflight(
                venue=self.venue,
                enabled=enabled,
                authenticated=False,
                ready=False,
                credential_mode="subkey_jwt",
                missing_env_vars=missing,
                blocking_reasons=_blocking_reasons(enabled, missing, self.venue),
                notes=[
                    (
                        "Paradex authenticated account reads derive a short-lived JWT from the "
                        "main account address plus the trading subkey private key. "
                        "A bearer token can still be supplied as an override."
                    )
                ],
            )

        credential_mode = "bearer_token" if bearer_token else "subkey_jwt"
        try:
            if bearer_token:
                jwt_token = cast(str, bearer_token)
            else:
                async with httpx.AsyncClient(
                    base_url=ACCOUNT_CONNECTOR_BASE_URLS[self.venue],
                    timeout=15.0,
                ) as auth_client:
                    jwt_token = await self._token_provider.issue_jwt_token(
                        account_address=cast(str, account_address),
                        private_key=cast(str, private_key),
                        client=auth_client,
                    )
        except (ConnectorError, httpx.HTTPError) as exc:
            return VenueAccountPreflight(
                venue=self.venue,
                enabled=enabled,
                authenticated=False,
                ready=False,
                credential_mode=credential_mode,
                missing_env_vars=[],
                blocking_reasons=[f"Paradex JWT issuance failed: {exc}"],
                notes=["Check the Paradex account address and trading subkey private key."],
            )

        headers = {"Authorization": f"Bearer {jwt_token}"}
        async with httpx.AsyncClient(
            base_url=ACCOUNT_CONNECTOR_BASE_URLS[self.venue],
            headers=headers,
            timeout=15.0,
        ) as client:
            connector = ParadexPrivateConnector(client)
            try:
                account, balances, positions = await asyncio.gather(
                    connector.fetch_account(),
                    connector.fetch_balances(),
                    connector.fetch_positions(),
                )
            except (ConnectorError, httpx.HTTPError) as exc:
                return VenueAccountPreflight(
                    venue=self.venue,
                    enabled=enabled,
                    authenticated=False,
                    ready=False,
                    credential_mode=credential_mode,
                    missing_env_vars=[],
                    blocking_reasons=[f"Paradex authenticated read failed: {exc}"],
                    notes=["Check the Paradex credentials and target account state."],
                )

        account_body = _unwrap_payload(account)
        account_identifier = _pick_string(
            account_body,
            "account",
            "account_address",
            "starknet_account",
            "id",
        ) or account_address
        return VenueAccountPreflight(
            venue=self.venue,
            enabled=enabled,
            authenticated=True,
            ready=True,
            credential_mode=credential_mode,
            account_identifier=account_identifier,
            account_status=_pick_string(account_body, "status", "account_status"),
            total_collateral=_pick_float(
                account_body,
                "account_value",
                "equity",
                "total_collateral",
            ),
            free_collateral=_pick_float(
                account_body,
                "free_collateral",
                "freeCollateral",
            ),
            balance_count=_count_rows(balances),
            position_count=_count_rows(positions),
            balance_assets=_extract_balance_assets(balances),
            position_symbols=_extract_position_symbols(positions),
            notes=[
                (
                    "Paradex account preflight completed using authenticated private GETs "
                    f"with {credential_mode}."
                )
            ],
        )


async def _fetch_extended_optional_rows(
    fetcher: Callable[[], Awaitable[dict[str, Any] | list[Any]]],
    *,
    resource_label: str,
) -> dict[str, Any] | list[Any]:
    try:
        return await fetcher()
    except httpx.HTTPStatusError as exc:
        if exc.response.status_code != 404:
            raise
        return {
            "data": [],
            "notes": [f"Extended {resource_label} endpoint returned 404; treated as empty"],
        }


def _blocking_reasons(enabled: bool, missing_env_vars: list[str], venue: str) -> list[str]:
    reasons: list[str] = []
    if not enabled:
        reasons.append(f"Venue {venue} account preflight is not enabled")
    if missing_env_vars:
        reasons.append(
            f"Venue {venue} is missing required account credentials: "
            + ", ".join(missing_env_vars)
        )
    return reasons


def _unwrap_payload(value: dict[str, Any] | list[Any]) -> dict[str, Any]:
    if isinstance(value, dict):
        for key in ("data", "results", "result"):
            nested = value.get(key)
            if isinstance(nested, dict):
                return nested
        return value
    return {}


def _count_rows(value: dict[str, Any] | list[Any]) -> int:
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        for key in ("data", "results", "result", "rows", "positions", "balances"):
            nested = value.get(key)
            if isinstance(nested, list):
                return len(nested)
    return 0


def _unwrap_rows(value: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        for key in ("data", "results", "result", "rows", "positions", "balances"):
            nested = value.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
    return []


def _extract_balance_assets(value: dict[str, Any] | list[Any]) -> list[str]:
    return _extract_row_strings(value, "asset", "token", "currency", "symbol")


def _extract_position_symbols(value: dict[str, Any] | list[Any]) -> list[str]:
    return _extract_row_strings(value, "symbol", "market", "instrument", "ticker")


def _extract_row_strings(value: dict[str, Any] | list[Any], *keys: str) -> list[str]:
    rows = _unwrap_rows(value)
    results: list[str] = []
    seen: set[str] = set()
    for row in rows:
        for key in keys:
            cell = row.get(key)
            if not isinstance(cell, str):
                continue
            normalized = cell.strip()
            if not normalized or normalized in seen:
                continue
            seen.add(normalized)
            results.append(normalized)
            break
    return results


def _pick_string(data: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _pick_float(data: dict[str, Any], *keys: str) -> float | None:
    for key in keys:
        try:
            value = parse_float(data.get(key))
        except ConnectorError:
            value = None
        if value is not None:
            return value
    return None
