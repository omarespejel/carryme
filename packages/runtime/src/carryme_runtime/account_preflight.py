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
from pydantic import ValidationError

from carryme_runtime.opportunities import UpstreamDataError


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
            self.probes[venue].probe(configs.get(venue, _disabled_config()))
            for venue in self.probes
        ]
        return list(await asyncio.gather(*tasks))

    async def probe_paper_trade(
        self,
        paper_trade: PaperTradeEntry,
        configs: AccountPreflightConfigMap,
    ) -> PaperTradeAccountPreflight:
        """Probe only the venues touched by one saved paper trade."""

        venue_names = [paper_trade.intent.long_leg.venue, paper_trade.intent.short_leg.venue]
        selected_names: list[str] = []
        for venue in venue_names:
            if venue not in selected_names:
                selected_names.append(venue)

        supported_names: list[str] = []
        supported_tasks: list[Awaitable[VenueAccountPreflight]] = []
        selected_statuses: dict[str, VenueAccountPreflight] = {}
        for venue in selected_names:
            config = configs.get(venue, _disabled_config())
            probe = self.probes.get(venue)
            if probe is None:
                selected_statuses[venue] = _unsupported_venue_status(venue, config)
                continue
            supported_names.append(venue)
            supported_tasks.append(probe.probe(config))

        if supported_tasks:
            for venue, status in zip(
                supported_names,
                await asyncio.gather(*supported_tasks),
                strict=True,
            ):
                selected_statuses[venue] = status

        selected = [selected_statuses[venue] for venue in selected_names]

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
        try:
            account_body = _unwrap_payload(account, context="Extended account")
            balance_body: dict[str, Any] = {}
            if isinstance(balances, dict):
                try:
                    balance_body = _unwrap_payload(balances, context="Extended balances")
                except UpstreamDataError:
                    balance_body = {}
            balance_rows = balances.get("data") if isinstance(balances, dict) else None
            try:
                balance_count = _count_rows(balances, context="Extended balances")
            except UpstreamDataError as exc:
                if isinstance(balance_rows, dict):
                    balance_count = 0
                elif balance_body:
                    raise UpstreamDataError(
                        "Extended balances row count was malformed despite a balance payload"
                    ) from exc
                else:
                    raise
            account_total_collateral = _pick_float(
                account_body,
                "equity",
                "balance",
                "totalCollateral",
                context="Extended account",
            )
            account_available_to_trade = _pick_float(
                account_body,
                "availableForTrade",
                "availableBalance",
                "available_to_trade",
                context="Extended account",
            )
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
                    account_total_collateral
                    if account_total_collateral is not None
                    else _pick_float(
                        balance_body,
                        "equity",
                        "balance",
                        "totalCollateral",
                        context="Extended balances",
                    )
                ),
                available_to_trade=(
                    account_available_to_trade
                    if account_available_to_trade is not None
                    else _pick_float(
                        balance_body,
                        "availableForTrade",
                        "availableBalance",
                        "available_to_trade",
                        context="Extended balances",
                    )
                ),
                balance_count=balance_count,
                position_count=_count_rows(positions, context="Extended positions"),
                notes=[
                    (
                        "Extended account preflight completed using authenticated private GETs. "
                        "Missing balance or position rows are treated as an empty account state."
                    )
                ],
            )
        except (UpstreamDataError, ValidationError) as exc:
            return VenueAccountPreflight(
                venue=self.venue,
                enabled=enabled,
                authenticated=False,
                ready=False,
                credential_mode="api_key",
                blocking_reasons=[f"Extended authenticated read returned malformed payload: {exc}"],
                notes=[
                    "Extended authenticated read succeeded but returned malformed account data."
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
        try:
            account_body = _unwrap_payload(account, context="Paradex account")
            account_identifier = _pick_string(
                account_body,
                "account",
                "account_address",
                "starknet_account",
                "id",
            )
            if account_identifier is None:
                raise UpstreamDataError(
                    "Paradex account payload is missing an account identifier"
                )
            if account_identifier.casefold() != cast(str, account_address).casefold():
                raise UpstreamDataError(
                    "Paradex account payload did not match the configured account address"
                )
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
                    context="Paradex account",
                ),
                free_collateral=_pick_float(
                    account_body,
                    "free_collateral",
                    "freeCollateral",
                    context="Paradex account",
                ),
                balance_count=_count_rows(balances, context="Paradex balances"),
                position_count=_count_rows(positions, context="Paradex positions"),
                notes=[
                    (
                        "Paradex account preflight completed using authenticated private GETs "
                        f"with {credential_mode}."
                    )
                ],
            )
        except (UpstreamDataError, ValidationError) as exc:
            return VenueAccountPreflight(
                venue=self.venue,
                enabled=enabled,
                authenticated=False,
                ready=False,
                credential_mode=credential_mode,
                blocking_reasons=[f"Paradex authenticated read returned malformed payload: {exc}"],
                notes=[
                    "Paradex authenticated read succeeded but returned malformed account data."
                ],
            )


async def _fetch_extended_optional_rows(
    fetcher: Callable[[], Awaitable[dict[str, Any] | list[Any]]],
    *,
    resource_label: str,
) -> dict[str, Any] | list[Any]:
    try:
        return await fetcher()
    except ConnectorError as exc:
        cause = exc.__cause__
        if not (
            isinstance(cause, httpx.HTTPStatusError)
            and cause.response.status_code == 404
        ):
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


def _disabled_config() -> VenueAccountConfig:
    return {"enabled": False, "credentials": {}}


def _unsupported_venue_status(
    venue: str,
    config: VenueAccountConfig,
) -> VenueAccountPreflight:
    return VenueAccountPreflight(
        venue=venue,
        enabled=bool(config["enabled"]),
        authenticated=False,
        ready=False,
        credential_mode="unsupported",
        blocking_reasons=[f"Venue {venue} account preflight is not supported"],
        notes=[f"No authenticated account probe is implemented for venue {venue}."],
    )


def _unwrap_payload(value: dict[str, Any] | list[Any], *, context: str) -> dict[str, Any]:
    if isinstance(value, dict):
        for key in ("data", "results", "result"):
            if key not in value:
                continue
            nested = value.get(key)
            if isinstance(nested, dict):
                return nested
            raise UpstreamDataError(f"{context} payload field {key!r} must be an object")
        return value
    raise UpstreamDataError(f"{context} payload must be an object")


def _count_rows(value: dict[str, Any] | list[Any], *, context: str) -> int:
    if isinstance(value, list):
        return len(value)
    if isinstance(value, dict):
        for key in ("data", "results", "result", "rows", "positions", "balances"):
            if key not in value:
                continue
            nested = value.get(key)
            if isinstance(nested, list):
                return len(nested)
            raise UpstreamDataError(f"{context} payload field {key!r} must be a list")
    raise UpstreamDataError(f"{context} payload did not contain a row list")


def _pick_string(data: dict[str, Any], *keys: str) -> str | None:
    for key in keys:
        value = data.get(key)
        if isinstance(value, str) and value.strip():
            return value
    return None


def _pick_float(data: dict[str, Any], *keys: str, context: str) -> float | None:
    for key in keys:
        if key not in data:
            continue
        raw_value = data.get(key)
        if raw_value is None:
            continue
        try:
            value = parse_float(raw_value)
        except ConnectorError as exc:
            raise UpstreamDataError(
                f"{context} payload field {key!r} must be numeric"
            ) from exc
        if value is not None:
            return value
    return None
