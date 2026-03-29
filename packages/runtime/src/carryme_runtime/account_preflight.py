"""Authenticated account-state preflight services."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from typing import Any, Protocol, TypedDict, cast

import httpx
from carryme_connectors import (
    ConnectorError,
    ExtendedPrivateConnector,
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
                    credential_mode="api_key",
                    missing_env_vars=[],
                    blocking_reasons=[f"Extended authenticated read failed: {exc}"],
                    notes=["Check the Extended API key and target subaccount."],
                )

        account_body = _unwrap_payload(account)
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
            total_collateral=_pick_float(account_body, "equity", "balance", "totalCollateral"),
            available_to_trade=_pick_float(
                account_body,
                "availableForTrade",
                "availableBalance",
                "available_to_trade",
            ),
            balance_count=_count_rows(balances),
            position_count=_count_rows(positions),
            notes=["Extended account preflight completed using authenticated private GETs."],
        )


class ParadexAccountProbe:
    """Authenticated read-only account probe for Paradex."""

    venue = "paradex"

    async def probe(self, config: VenueAccountConfig) -> VenueAccountPreflight:
        enabled = bool(config["enabled"])
        bearer_token = config["credentials"].get("bearer_token")
        account_address = config["credentials"].get("account_address")
        missing = []
        if not bearer_token:
            missing.append("CARRYME_API_PARADEX_BEARER_TOKEN")
        if not account_address:
            missing.append("CARRYME_API_PARADEX_ACCOUNT_ADDRESS")
        if not enabled or missing:
            return VenueAccountPreflight(
                venue=self.venue,
                enabled=enabled,
                authenticated=False,
                ready=False,
                credential_mode="bearer_token",
                missing_env_vars=missing,
                blocking_reasons=_blocking_reasons(enabled, missing, self.venue),
                notes=[
                    (
                        "Paradex authenticated account reads currently use a bearer/JWT token "
                        "plus the main account address."
                    )
                ],
            )

        headers = {"Authorization": f"Bearer {cast(str, bearer_token)}"}
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
                    credential_mode="bearer_token",
                    missing_env_vars=[],
                    blocking_reasons=[f"Paradex authenticated read failed: {exc}"],
                    notes=["Check the Paradex bearer token and account address."],
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
            credential_mode="bearer_token",
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
            notes=["Paradex account preflight completed using authenticated private GETs."],
        )


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
