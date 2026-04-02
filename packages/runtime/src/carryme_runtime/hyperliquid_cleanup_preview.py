"""Reduce-only cleanup preview generation for open Hyperliquid positions."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal, cast

from carryme_connectors import (
    ConnectorError,
    build_hyperliquid_info,
    format_hyperliquid_price,
    format_hyperliquid_size,
)
from carryme_models import (
    ExecutionCleanupPreview,
    ExecutionJournalEntry,
    ExecutionLegResult,
    ExecutionPairStatus,
    VenueOrderPreview,
)

from carryme_runtime.opportunities import SnapshotFetcher, fetch_live_snapshot
from carryme_runtime.order_preview import (
    _bps_decimal,
    _extract_order_constraints,
    _raw_int,
    _snap_price,
    _snap_quantity,
    _to_float,
)


@dataclass(frozen=True)
class HyperliquidCleanupPreviewService:
    """Build a reduce-only Hyperliquid close preview from live position state."""

    account_address: str
    vault_address: str | None = None
    fetch_snapshot: SnapshotFetcher = fetch_live_snapshot

    async def preview_from_execution(
        self,
        *,
        entry: ExecutionJournalEntry,
        pair_status: ExecutionPairStatus,
        slippage_tolerance_bps: int = 10,
        generated_at: datetime | None = None,
    ) -> ExecutionCleanupPreview:
        if pair_status.recommended_action not in {
            "close_open_leg",
            "complete_or_unwind_missing_leg",
        }:
            raise ValueError("Cleanup preview is only available when pair status recommends it")

        target_leg = _select_open_hyperliquid_leg(entry, pair_status)
        position = await asyncio.to_thread(
            _fetch_hyperliquid_position,
            self.vault_address or self.account_address,
            target_leg.symbol,
        )
        preview_leg = await self._build_cleanup_leg(
            target_leg=target_leg,
            position=position,
            paper_trade_id=entry.paper_trade_id or 0,
            slippage_tolerance_bps=slippage_tolerance_bps,
        )
        timestamp = generated_at or datetime.now(UTC)
        preview_hash = _cleanup_hash(
            execution_entry_id=entry.entry_id,
            paper_trade_id=entry.paper_trade_id,
            leg=preview_leg,
        )
        return ExecutionCleanupPreview(
            execution_entry_id=entry.entry_id,
            paper_trade_id=entry.paper_trade_id,
            generated_at=timestamp,
            preview_hash=preview_hash,
            reason=pair_status.recommended_action,
            leg=preview_leg,
            notes=[
                "Cleanup preview was derived from live Hyperliquid position state.",
                "Order is reduce-only and intended to flatten the open leg.",
            ],
        )

    async def _build_cleanup_leg(
        self,
        *,
        target_leg: ExecutionLegResult,
        position: dict[str, Any],
        paper_trade_id: int,
        slippage_tolerance_bps: int,
    ) -> VenueOrderPreview:
        symbol = target_leg.symbol
        signed_size = _required_decimal(position, "szi")
        if signed_size == 0:
            raise ValueError(f"No open Hyperliquid position remained for {symbol}")

        side: Literal["buy", "sell"] = "sell" if signed_size > 0 else "buy"
        reference_price_source: Literal["best_bid", "best_ask"] = (
            "best_ask" if side == "buy" else "best_bid"
        )
        multiplier = (
            Decimal("1") + _bps_decimal(slippage_tolerance_bps)
            if side == "buy"
            else Decimal("1") - _bps_decimal(slippage_tolerance_bps)
        )

        quantity = abs(signed_size)
        snapshot = await self.fetch_snapshot("hyperliquid", symbol)
        top_of_book = snapshot.market.top_of_book
        if top_of_book is None:
            raise ValueError(f"Hyperliquid is missing top-of-book data for {symbol}")

        reference_price = (
            top_of_book.best_ask_price
            if reference_price_source == "best_ask"
            else top_of_book.best_bid_price
        )
        if reference_price is None or reference_price <= 0:
            raise ValueError(
                f"Hyperliquid is missing a usable {reference_price_source} for {symbol}"
            )

        reference = Decimal(str(reference_price))
        constraints = _extract_order_constraints("hyperliquid", snapshot)
        quantity = _snap_quantity(quantity, constraints.quantity_increment)
        if quantity <= 0:
            raise ValueError(f"Hyperliquid cleanup quantity snapped to zero for {symbol}")
        raw = snapshot.market.raw
        if not isinstance(raw, dict):
            raise ValueError(f"Hyperliquid is missing metadata required to format {symbol}")
        sz_decimals = _raw_int(cast(dict[str, object], raw), "szDecimals")
        if sz_decimals is None:
            raise ValueError(f"Hyperliquid is missing szDecimals metadata for {symbol}")
        quantity, quantity_text = format_hyperliquid_size(quantity, sz_decimals=sz_decimals)

        worst_price = _snap_price(reference * multiplier, constraints.price_increment, side=side)
        worst_price, worst_price_text = format_hyperliquid_price(
            worst_price,
            sz_decimals=sz_decimals,
        )
        effective_notional = quantity * reference
        client_order_id = f"carryme-cleanup-pt{paper_trade_id}-hyperliquid-{side}"

        return VenueOrderPreview(
            venue="hyperliquid",
            symbol=symbol,
            fee_profile=target_leg.fee_profile,
            side=side,
            target_notional=float(effective_notional),
            effective_notional=float(effective_notional),
            quantity=float(quantity),
            quantity_text=quantity_text,
            quantity_increment=_to_float(constraints.quantity_increment),
            minimum_order_size=_to_float(constraints.minimum_order_size),
            minimum_notional=_to_float(constraints.minimum_notional),
            reference_price=float(reference),
            reference_price_source=reference_price_source,
            worst_acceptable_price=float(worst_price),
            worst_price_text=worst_price_text,
            reduce_only=True,
            endpoint_path_hint="/exchange",
            required_auth_env_vars=[
                "CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS",
                "CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
            ],
            auth_scheme="account address + API wallet private key",
            payload={
                "coin": symbol,
                "is_buy": side == "buy",
                "sz": quantity_text,
                "limit_px": worst_price_text,
                "order_type": {"limit": {"tif": "Ioc"}},
                "reduce_only": True,
                "client_order_id": client_order_id,
            },
            notes=[
                "Cleanup preview uses live top-of-book and Hyperliquid wire formatting.",
                "Reduce-only close preview does not enforce standard minimum-notional checks.",
            ],
        )


def _select_open_hyperliquid_leg(
    entry: ExecutionJournalEntry,
    pair_status: ExecutionPairStatus,
) -> ExecutionLegResult:
    position_symbols_by_venue = {
        venue.venue: set(venue.position_symbols) for venue in pair_status.reconciliation.venues
    }
    candidates = [
        leg
        for leg in entry.legs
        if leg.venue == "hyperliquid"
        and leg.symbol in position_symbols_by_venue.get("hyperliquid", set())
    ]
    if len(candidates) != 1:
        raise ValueError("Cleanup preview requires exactly one open Hyperliquid leg")
    return candidates[0]


def _fetch_hyperliquid_position(account_address: str, symbol: str) -> dict[str, Any]:
    info = build_hyperliquid_info()
    state = info.user_state(account_address)
    if not isinstance(state, dict):
        raise ConnectorError("Hyperliquid account-state payload must be an object")
    rows = state.get("assetPositions")
    if not isinstance(rows, list):
        rows = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        position = row.get("position")
        if not isinstance(position, dict):
            continue
        if _string_value(position, "coin") == symbol:
            return position
    raise ValueError(f"No open Hyperliquid position matched {symbol}")


def _cleanup_hash(
    *,
    execution_entry_id: int | None,
    paper_trade_id: int | None,
    leg: VenueOrderPreview,
) -> str:
    encoded = json.dumps(
        {
            "execution_entry_id": execution_entry_id,
            "paper_trade_id": paper_trade_id,
            "leg": leg.model_dump(mode="json"),
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()


def _required_decimal(payload: dict[str, Any], *keys: str) -> Decimal:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        return Decimal(str(value))
    raise ValueError(f"Expected one of {keys!r} in the Hyperliquid position payload")


def _string_value(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str):
        return value
    if isinstance(value, int):
        return str(value)
    return None
