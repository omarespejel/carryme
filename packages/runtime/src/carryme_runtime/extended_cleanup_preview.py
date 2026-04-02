"""Reduce-only cleanup preview generation for open Extended positions."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

import httpx
from carryme_connectors import ExtendedPrivateConnector
from carryme_models import (
    ExecutionCleanupPreview,
    ExecutionJournalEntry,
    ExecutionLegResult,
    ExecutionPairStatus,
    NormalizedMarketSnapshot,
    VenueOrderPreview,
)

from carryme_runtime.opportunities import SnapshotFetcher, fetch_live_snapshot
from carryme_runtime.order_preview import (
    _bps_decimal,
    _extract_order_constraints,
    _format_order_value,
    _snap_price,
    _to_float,
)


@dataclass(frozen=True)
class ExtendedCleanupPreviewService:
    """Build a reduce-only Extended close preview from live position state."""

    api_key: str
    base_url: str = "https://api.starknet.extended.exchange"
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

        target_leg = _select_open_extended_leg(entry, pair_status)
        position = await self._fetch_live_position(target_leg.symbol)
        preview_leg = await self._build_cleanup_leg(
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
                "Cleanup preview was derived from live Extended position state.",
                "Order is reduce-only and intended to flatten the open leg.",
            ],
        )

    async def _fetch_live_position(self, symbol: str) -> dict[str, Any]:
        headers = {"x-api-key": self.api_key}
        async with httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=15.0,
        ) as client:
            payload = await ExtendedPrivateConnector(client).fetch_positions()

        rows = _unwrap_rows(payload)
        for row in rows:
            candidates = {_string_value(row, "market"), _string_value(row, "symbol")}
            if symbol in candidates:
                return row
        raise ValueError(f"No open Extended position matched {symbol}")

    async def _build_cleanup_leg(
        self,
        *,
        position: dict[str, Any],
        paper_trade_id: int,
        slippage_tolerance_bps: int,
    ) -> VenueOrderPreview:
        symbol = _required_string(position, "market")
        position_side = _required_string(position, "side").upper()
        if position_side == "SHORT":
            side: Literal["buy", "sell"] = "buy"
            reference_price_source: Literal["best_bid", "best_ask"] = "best_ask"
            multiplier = Decimal("1") + _bps_decimal(slippage_tolerance_bps)
        elif position_side == "LONG":
            side = "sell"
            reference_price_source = "best_bid"
            multiplier = Decimal("1") - _bps_decimal(slippage_tolerance_bps)
        else:
            raise ValueError(f"Unsupported Extended position side {position_side!r}")

        quantity = Decimal(_required_string(position, "size"))
        snapshot = await self.fetch_snapshot("extended", symbol)
        preview = _build_extended_cleanup_preview(
            symbol=symbol,
            side=side,
            quantity=quantity,
            snapshot=snapshot,
            reference_price_source=reference_price_source,
            paper_trade_id=paper_trade_id,
            slippage_tolerance_bps=slippage_tolerance_bps,
            multiplier=multiplier,
        )
        return preview


def _build_extended_cleanup_preview(
    *,
    symbol: str,
    side: Literal["buy", "sell"],
    quantity: Decimal,
    snapshot: NormalizedMarketSnapshot,
    reference_price_source: Literal["best_bid", "best_ask"],
    paper_trade_id: int,
    slippage_tolerance_bps: int,
    multiplier: Decimal,
) -> VenueOrderPreview:
    top_of_book = snapshot.market.top_of_book
    if top_of_book is None:
        raise ValueError(f"Extended is missing top-of-book data for {symbol}")

    reference_price = (
        top_of_book.best_ask_price
        if reference_price_source == "best_ask"
        else top_of_book.best_bid_price
    )
    if reference_price is None or reference_price <= 0:
        raise ValueError(f"Extended is missing a usable {reference_price_source} for {symbol}")

    reference = Decimal(str(reference_price))
    constraints = _extract_order_constraints("extended", snapshot)
    worst_price = _snap_price(reference * multiplier, constraints.price_increment, side=side)
    quantity_text = _format_order_value(
        venue="extended",
        value=quantity,
        increment=constraints.quantity_increment,
    )
    worst_price_text = _format_order_value(
        venue="extended",
        value=worst_price,
        increment=constraints.price_increment,
    )
    effective_notional = quantity * reference
    client_order_id = f"carryme-cleanup-pt{paper_trade_id}-{side}"

    return VenueOrderPreview(
        venue="extended",
        symbol=symbol,
        fee_profile="default",
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
        price_increment=_to_float(constraints.price_increment),
        max_order_value=_to_float(constraints.max_order_value),
        reduce_only=True,
        endpoint_path_hint="/api/v1/user/order",
        required_auth_env_vars=[
            "CARRYME_API_EXTENDED_API_KEY",
            "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY",
        ],
        auth_scheme="api key + Stark signing key",
        payload={
            "symbol": symbol,
            "side": side.upper(),
            "type": "LIMIT",
            "size": quantity_text,
            "price": worst_price_text,
            "time_in_force": "IOC",
            "client_order_id": client_order_id,
            "reduce_only": True,
        },
        notes=[
            "Cleanup preview uses live top-of-book and venue price increments.",
            "Reduce-only close preview does not enforce standard minimum-notional checks.",
        ],
    )


def _select_open_extended_leg(
    entry: ExecutionJournalEntry,
    pair_status: ExecutionPairStatus,
) -> ExecutionLegResult:
    position_symbols_by_venue = {
        venue.venue: set(venue.position_symbols) for venue in pair_status.reconciliation.venues
    }
    candidates = [
        leg
        for leg in entry.legs
        if leg.venue == "extended"
        and leg.symbol in position_symbols_by_venue.get("extended", set())
    ]
    if len(candidates) != 1:
        raise ValueError("Cleanup preview requires exactly one open Extended leg")
    return candidates[0]


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


def _unwrap_rows(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "rows", "results", "positions"):
            nested = payload.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
    return []


def _string_value(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value:
        return value
    if isinstance(value, int):
        return str(value)
    return None


def _required_string(payload: dict[str, Any], key: str) -> str:
    value = _string_value(payload, key)
    if value is None:
        raise ValueError(f"Extended position is missing {key}")
    return value
