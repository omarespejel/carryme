"""Authenticated account-state preflight services."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any, Protocol, TypedDict, cast

import httpx
from carryme_connectors import (
    HYPERLIQUID_API_BASE_URL,
    ConnectorError,
    ExtendedPrivateConnector,
    ParadexJwtTokenProvider,
    ParadexPrivateConnector,
    build_hyperliquid_info,
)
from carryme_connectors.base import parse_float
from carryme_models import (
    PaperTradeAccountPreflight,
    PaperTradeEntry,
    VenueAccountPreflight,
)
from pydantic import ValidationError

from carryme_runtime.opportunities import UpstreamDataError

AUTH_READ_RETRYABLE_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})
AUTH_READ_RETRY_ATTEMPTS = 3
AUTH_READ_RETRY_BACKOFF_SECONDS = 0.25


class VenueAccountConfig(TypedDict):
    """Configured authenticated-read credentials for one venue."""

    enabled: bool
    credentials: dict[str, str | None]


AccountPreflightConfigMap = dict[str, VenueAccountConfig]


def build_account_preflight_configs(
    *,
    extended_live_enabled: bool,
    extended_api_key: str | None,
    paradex_live_enabled: bool,
    paradex_account_address: str | None,
    paradex_private_key: str | None,
    paradex_bearer_token: str | None,
    hyperliquid_live_enabled: bool,
    hyperliquid_account_address: str | None,
    hyperliquid_api_wallet_private_key: str | None,
) -> AccountPreflightConfigMap:
    """Build the authenticated-read account probe config map."""

    return {
        "extended": {
            "enabled": extended_live_enabled,
            "credentials": {
                "api_key": extended_api_key,
            },
        },
        "paradex": {
            "enabled": paradex_live_enabled,
            "credentials": {
                "account_address": paradex_account_address,
                "bearer_token": paradex_bearer_token,
                "private_key": paradex_private_key,
            },
        },
        "hyperliquid": {
            "enabled": hyperliquid_live_enabled,
            "credentials": {
                "account_address": hyperliquid_account_address,
                "api_wallet_private_key": hyperliquid_api_wallet_private_key,
            },
        },
    }


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
    "hyperliquid": HYPERLIQUID_API_BASE_URL,
    "paradex": "https://api.prod.paradex.trade",
}
HYPERLIQUID_ACCOUNT_READ_TIMEOUT_SECONDS = 15.0


@dataclass
class AccountPreflightService:
    """Probe authenticated venue account state for the current live stack."""

    probes: dict[str, VenueAccountProbe] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.probes:
            self.probes = {
                "extended": ExtendedAccountProbe(),
                "hyperliquid": HyperliquidAccountProbe(),
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
                account = await _run_authenticated_read_with_retry(connector.fetch_account)
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
                if _looks_like_extended_balance_summary(balance_rows):
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
                balance_assets=_extract_balance_assets(balances),
                position_symbols=_extract_position_symbols(positions),
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
                    _run_authenticated_read_with_retry(connector.fetch_account),
                    _run_authenticated_read_with_retry(connector.fetch_balances),
                    _run_authenticated_read_with_retry(connector.fetch_positions),
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
                raise UpstreamDataError("Paradex account payload is missing an account identifier")
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
                position_count=_count_open_positions(positions, context="Paradex positions"),
                balance_assets=_extract_balance_assets(balances),
                position_symbols=_extract_position_symbols(positions),
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
                notes=["Paradex authenticated read succeeded but returned malformed account data."],
            )


class HyperliquidAccountProbe:
    """Address-backed account probe for Hyperliquid live trading."""

    venue = "hyperliquid"

    async def probe(self, config: VenueAccountConfig) -> VenueAccountPreflight:
        enabled = bool(config["enabled"])
        account_address = config["credentials"].get("account_address")
        api_wallet_private_key = config["credentials"].get("api_wallet_private_key")
        missing = []
        if not account_address:
            missing.append("CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS")
        if not api_wallet_private_key:
            missing.append("CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY")
        if not enabled or missing:
            return VenueAccountPreflight(
                venue=self.venue,
                enabled=enabled,
                authenticated=False,
                ready=False,
                credential_mode="api_wallet",
                missing_env_vars=missing,
                blocking_reasons=_blocking_reasons(enabled, missing, self.venue),
                notes=[
                    (
                        "Hyperliquid live trading requires the trading account address and an "
                        "authorized API wallet private key."
                    ),
                    (
                        "If orders should target a subaccount or vault, configure the "
                        "optional CARRYME_API_HYPERLIQUID_VAULT_ADDRESS for live execution."
                    ),
                ],
            )

        try:
            state, open_orders = await asyncio.wait_for(
                asyncio.to_thread(
                    _fetch_hyperliquid_account_state,
                    cast(str, account_address),
                ),
                timeout=HYPERLIQUID_ACCOUNT_READ_TIMEOUT_SECONDS,
            )
        except TimeoutError:
            return VenueAccountPreflight(
                venue=self.venue,
                enabled=enabled,
                authenticated=False,
                ready=False,
                credential_mode="api_wallet",
                missing_env_vars=[],
                blocking_reasons=["Hyperliquid account read timed out"],
                notes=[
                    (
                        "The Hyperliquid SDK request did not complete before the configured "
                        "timeout elapsed."
                    )
                ],
            )
        except Exception as exc:
            return VenueAccountPreflight(
                venue=self.venue,
                enabled=enabled,
                authenticated=False,
                ready=False,
                credential_mode="api_wallet",
                missing_env_vars=[],
                blocking_reasons=[f"Hyperliquid account read failed: {exc}"],
                notes=[
                    (
                        "Check the Hyperliquid account address and confirm the account is "
                        "queryable on the configured network."
                    )
                ],
            )

        try:
            margin_summary = state.get("marginSummary")
            if not isinstance(margin_summary, dict):
                margin_summary = {}
            positions = state.get("assetPositions")
            if not isinstance(positions, list):
                positions = []
            withdrawable = _pick_float(
                state,
                "withdrawable",
                context="hyperliquid withdrawable balance",
            )
            margin_collateral = _pick_float(
                margin_summary,
                "accountValue",
                "totalRawUsd",
                context="hyperliquid account collateral",
            )
            total_collateral = margin_collateral if margin_collateral is not None else withdrawable
            return VenueAccountPreflight(
                venue=self.venue,
                enabled=enabled,
                authenticated=True,
                ready=True,
                credential_mode="api_wallet",
                account_identifier=account_address,
                total_collateral=total_collateral,
                available_to_trade=withdrawable,
                free_collateral=withdrawable,
                balance_count=1 if total_collateral is not None else 0,
                position_count=len([item for item in positions if isinstance(item, dict)]),
                balance_assets=["USDC"] if total_collateral is not None else [],
                position_symbols=_extract_hyperliquid_position_symbols(positions),
                notes=[
                    (
                        "Hyperliquid account preflight completed using the official SDK "
                        "Info.user_state/open_orders flow. Account reads are address-based; the "
                        "API wallet private key remains required for live submission."
                    ),
                    (
                        "If this account is a subaccount or vault, live actions may also require "
                        "CARRYME_API_HYPERLIQUID_VAULT_ADDRESS to target the correct account."
                    ),
                    f"Observed {len(open_orders)} currently open Hyperliquid orders.",
                ],
            )
        except (UpstreamDataError, ValidationError) as exc:
            return VenueAccountPreflight(
                venue=self.venue,
                enabled=enabled,
                authenticated=False,
                ready=False,
                credential_mode="api_wallet",
                blocking_reasons=[f"Hyperliquid account read returned malformed payload: {exc}"],
                notes=[
                    "Hyperliquid authenticated read succeeded but returned malformed account data."
                ],
            )


async def _fetch_extended_optional_rows(
    fetcher: Callable[[], Awaitable[dict[str, Any] | list[Any]]],
    *,
    resource_label: str,
) -> dict[str, Any] | list[Any]:
    try:
        return await _run_authenticated_read_with_retry(fetcher)
    except ConnectorError as exc:
        cause = exc.__cause__
        if not (isinstance(cause, httpx.HTTPStatusError) and cause.response.status_code == 404):
            raise
        return {
            "data": [],
            "notes": [f"Extended {resource_label} endpoint returned 404; treated as empty"],
        }


async def _run_authenticated_read_with_retry(
    fetcher: Callable[[], Awaitable[dict[str, Any] | list[Any]]],
) -> dict[str, Any] | list[Any]:
    last_exc: ConnectorError | httpx.HTTPError | None = None
    for attempt in range(1, AUTH_READ_RETRY_ATTEMPTS + 1):
        try:
            return await fetcher()
        except (ConnectorError, httpx.HTTPError) as exc:
            last_exc = exc
            if attempt >= AUTH_READ_RETRY_ATTEMPTS or not _is_retryable_authenticated_read_error(
                exc
            ):
                raise
            await asyncio.sleep(AUTH_READ_RETRY_BACKOFF_SECONDS * (2 ** (attempt - 1)))
    assert last_exc is not None
    raise last_exc


def _is_retryable_authenticated_read_error(exc: ConnectorError | httpx.HTTPError) -> bool:
    if isinstance(exc, ConnectorError):
        if exc.status_code is None:
            return True
        return exc.status_code in AUTH_READ_RETRYABLE_STATUS_CODES
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in AUTH_READ_RETRYABLE_STATUS_CODES
    return isinstance(exc, httpx.TransportError)


def _blocking_reasons(enabled: bool, missing_env_vars: list[str], venue: str) -> list[str]:
    reasons: list[str] = []
    if not enabled:
        reasons.append(f"Venue {venue} account preflight is not enabled")
    if missing_env_vars:
        reasons.append(
            f"Venue {venue} is missing required account credentials: " + ", ".join(missing_env_vars)
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


def _looks_like_extended_balance_summary(value: Any) -> bool:
    if not isinstance(value, dict):
        return False
    return any(
        key in value
        for key in (
            "equity",
            "balance",
            "totalCollateral",
            "availableForTrade",
            "availableBalance",
            "available_to_trade",
        )
    )


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


def _unwrap_rows(value: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(value, list):
        return [item for item in value if isinstance(item, dict)]
    if isinstance(value, dict):
        for key in ("data", "results", "result", "rows", "positions", "balances"):
            nested = value.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
    return []


def _unwrap_rows_strict(value: dict[str, Any] | list[Any], *, context: str) -> list[dict[str, Any]]:
    if isinstance(value, list):
        if not all(isinstance(item, dict) for item in value):
            raise UpstreamDataError(f"{context} payload must be a list of objects")
        return cast(list[dict[str, Any]], value)
    if isinstance(value, dict):
        for key in ("data", "results", "result", "rows", "positions", "balances"):
            if key not in value:
                continue
            nested = value.get(key)
            if isinstance(nested, list):
                if not all(isinstance(item, dict) for item in nested):
                    raise UpstreamDataError(
                        f"{context} payload field {key!r} must be a list of objects"
                    )
                return cast(list[dict[str, Any]], nested)
            raise UpstreamDataError(f"{context} payload field {key!r} must be a list")
    raise UpstreamDataError(f"{context} payload did not contain a row list")


def _extract_balance_assets(value: dict[str, Any] | list[Any]) -> list[str]:
    return _extract_row_strings(value, "asset", "token", "currency", "symbol")


def _count_open_positions(value: dict[str, Any] | list[Any], *, context: str) -> int:
    rows = _unwrap_rows_strict(value, context=context)
    return sum(1 for row in rows if _row_represents_open_position(row))


def _extract_position_symbols(value: dict[str, Any] | list[Any]) -> list[str]:
    rows = [row for row in _unwrap_rows(value) if _row_represents_open_position(row)]
    return _extract_row_strings(rows, "symbol", "market", "instrument", "ticker")


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


def _row_represents_open_position(row: dict[str, Any]) -> bool:
    status = row.get("status")
    if isinstance(status, str) and status.strip().upper() == "CLOSED":
        return False

    saw_numeric = False
    for key in ("size", "position_size", "qty", "quantity", "szi"):
        if key not in row:
            continue
        try:
            parsed = parse_float(row.get(key))
        except ConnectorError:
            parsed = None
        if parsed is None:
            continue
        saw_numeric = True
        if parsed != 0:
            return True

    return not saw_numeric


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
            raise UpstreamDataError(f"{context} payload field {key!r} must be numeric") from exc
        if value is not None:
            return value
    return None


def _fetch_hyperliquid_account_state(
    account_address: str,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    info = build_hyperliquid_info(base_url=ACCOUNT_CONNECTOR_BASE_URLS["hyperliquid"])
    state = info.user_state(account_address)
    open_orders = info.open_orders(account_address)
    if not isinstance(state, dict):
        raise ConnectorError("Hyperliquid user_state payload must be an object")
    if not isinstance(open_orders, list):
        raise ConnectorError("Hyperliquid open_orders payload must be a list")
    return state, [item for item in open_orders if isinstance(item, dict)]


def _extract_hyperliquid_position_symbols(positions: list[Any]) -> list[str]:
    results: list[str] = []
    seen: set[str] = set()
    for item in positions:
        if not isinstance(item, dict):
            continue
        position = item.get("position")
        if not isinstance(position, dict):
            continue
        symbol = position.get("coin")
        if not isinstance(symbol, str):
            continue
        normalized = symbol.strip()
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        results.append(normalized)
    return results
