"""Official Hyperliquid SDK helpers for authenticated trading flows."""

from __future__ import annotations

from decimal import ROUND_FLOOR, Decimal
from typing import Any, cast

from eth_account import Account  # type: ignore[import-untyped]
from hyperliquid.exchange import Exchange  # type: ignore[import-untyped]
from hyperliquid.info import Info  # type: ignore[import-untyped]
from hyperliquid.utils.constants import MAINNET_API_URL  # type: ignore[import-untyped]
from hyperliquid.utils.signing import float_to_wire  # type: ignore[import-untyped]
from hyperliquid.websocket_manager import WebsocketManager  # type: ignore[import-untyped]

HYPERLIQUID_API_BASE_URL = MAINNET_API_URL


def build_hyperliquid_wallet(private_key: str) -> Any:
    """Return a local account object for the configured API wallet."""

    return Account.from_key(private_key)


def build_hyperliquid_exchange(
    *,
    private_key: str,
    account_address: str | None = None,
    vault_address: str | None = None,
    base_url: str = HYPERLIQUID_API_BASE_URL,
) -> Any:
    """Return an official Hyperliquid Exchange client for live actions.

    `account_address` is used for account-context reads in higher layers.
    `vault_address` targets subaccount/vault actions when required by Hyperliquid.
    """

    return Exchange(
        wallet=build_hyperliquid_wallet(private_key),
        base_url=base_url,
        vault_address=vault_address,
        account_address=account_address,
    )


def build_hyperliquid_info(
    *,
    base_url: str = HYPERLIQUID_API_BASE_URL,
    timeout: float | None = 15.0,
) -> Any:
    """Return an official Hyperliquid Info client with websocket startup disabled.
    
    The default timeout keeps authenticated account probes from hanging indefinitely if the
    upstream Hyperliquid API becomes unresponsive.
    """

    return Info(base_url=base_url, skip_ws=True, timeout=timeout)


def build_hyperliquid_websocket_manager(
    *,
    base_url: str = HYPERLIQUID_API_BASE_URL,
) -> Any:
    """Return an official Hyperliquid websocket manager with its thread started."""

    manager = WebsocketManager(base_url)
    manager.start()
    return manager


def format_hyperliquid_size(value: Decimal, *, sz_decimals: int) -> tuple[Decimal, str]:
    """Snap and format a Hyperliquid order size using the market size decimals."""

    increment = Decimal("1").scaleb(-sz_decimals)
    snapped = _floor_to_increment(value, increment)
    return snapped, _wire_decimal(snapped)


def format_hyperliquid_price(value: Decimal, *, sz_decimals: int) -> tuple[Decimal, str]:
    """Format a Hyperliquid limit price using the exchange's perps rounding rules."""

    rounded = round(float(f"{float(value):.5g}"), max(0, 6 - sz_decimals))
    quantized = Decimal(str(rounded))
    return quantized, float_to_wire(float(quantized))


def _floor_to_increment(value: Decimal, increment: Decimal) -> Decimal:
    units = (value / increment).to_integral_value(rounding=ROUND_FLOOR)
    return units * increment


def _wire_decimal(value: Decimal) -> str:
    return cast(str, float_to_wire(float(value)))
