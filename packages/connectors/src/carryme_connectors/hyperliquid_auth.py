"""Official Hyperliquid SDK helpers for authenticated trading flows."""

from __future__ import annotations

from typing import Any

from eth_account import Account  # type: ignore[import-untyped]
from hyperliquid.exchange import Exchange  # type: ignore[import-untyped]
from hyperliquid.info import Info  # type: ignore[import-untyped]
from hyperliquid.utils.constants import MAINNET_API_URL  # type: ignore[import-untyped]

HYPERLIQUID_API_BASE_URL = MAINNET_API_URL


def build_hyperliquid_wallet(private_key: str) -> Any:
    """Return a local account object for the configured API wallet."""

    return Account.from_key(private_key)


def build_hyperliquid_exchange(
    *,
    private_key: str,
    account_address: str | None = None,
    base_url: str = HYPERLIQUID_API_BASE_URL,
) -> Any:
    """Return an official Hyperliquid Exchange client for live actions."""

    return Exchange(
        wallet=build_hyperliquid_wallet(private_key),
        base_url=base_url,
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
