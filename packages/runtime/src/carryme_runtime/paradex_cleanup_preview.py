"""Reduce-only cleanup preview generation for open Paradex positions."""

from __future__ import annotations

import hashlib
import json
import logging
from dataclasses import dataclass, field
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal, Protocol

import httpx
from carryme_connectors import ParadexJwtTokenProvider, ParadexPrivateConnector
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
    _format_order_value,
    _snap_price,
    _snap_quantity,
    _to_float,
)

_logger = logging.getLogger(__name__)


class ParadexJwtTokenIssuer(Protocol):
    """Issue a short-lived Paradex JWT for authenticated private reads."""

    async def issue_jwt_token(
        self,
        *,
        account_address: str,
        private_key: str,
        client: httpx.AsyncClient | None = None,
        now: int | None = None,
    ) -> str: ...


@dataclass(frozen=True)
class ParadexCleanupPreviewService:
    """Build a reduce-only Paradex close preview from live position state."""

    account_address: str
    private_key: str | None = None
    bearer_token: str | None = None
    token_provider: ParadexJwtTokenIssuer = field(default_factory=ParadexJwtTokenProvider)
    base_url: str = "https://api.prod.paradex.trade"
    fetch_snapshot: SnapshotFetcher = fetch_live_snapshot

    async def preview_from_execution(
        self,
        *,
        entry: ExecutionJournalEntry,
        pair_status: ExecutionPairStatus,
        slippage_tolerance_bps: int = 10,
        generated_at: datetime | None = None,
    ) -> ExecutionCleanupPreview:
        if pair_status.recommended_action != "close_open_leg":
            raise ValueError("Cleanup preview is only available when pair status recommends it")

        target_leg = _select_open_paradex_leg(entry, pair_status)
        position = await self._fetch_live_position(target_leg.symbol)
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
                "Cleanup preview was derived from live Paradex position state.",
                "Order is reduce-only and intended to flatten the open leg.",
            ],
        )

    async def _fetch_live_position(self, symbol: str) -> dict[str, Any]:
        jwt_token = self.bearer_token
        if jwt_token is None:
            if not self.private_key:
                raise ValueError(
                    "Paradex cleanup preview requires a subkey private key or bearer token"
                )
            async with httpx.AsyncClient(base_url=self.base_url, timeout=15.0) as auth_client:
                jwt_token = await self.token_provider.issue_jwt_token(
                    account_address=self.account_address,
                    private_key=self.private_key,
                    client=auth_client,
                )

        async with httpx.AsyncClient(
            base_url=self.base_url,
            headers={"Authorization": f"Bearer {jwt_token}"},
            timeout=15.0,
        ) as client:
            payload = await ParadexPrivateConnector(client).fetch_positions()

        rows, container_key = _unwrap_rows(payload)
        for row in rows:
            match_key = _matching_symbol_key(row, symbol)
            if match_key is None:
                continue
            if container_key not in (None, "results"):
                _logger.warning(
                    "Paradex positions used fallback container key key=%s symbol=%s row_keys=%s",
                    container_key,
                    symbol,
                    sorted(row.keys()),
                )
            if match_key != "market":
                _logger.warning(
                    "Paradex positions used fallback symbol key key=%s symbol=%s row_keys=%s",
                    match_key,
                    symbol,
                    sorted(row.keys()),
                )
            return row
        raise ValueError(f"No open Paradex position matched {symbol}")

    async def _build_cleanup_leg(
        self,
        *,
        target_leg: ExecutionLegResult,
        position: dict[str, Any],
        paper_trade_id: int,
        slippage_tolerance_bps: int,
    ) -> VenueOrderPreview:
        symbol = target_leg.symbol
        side = _determine_cleanup_side(position=position, fallback_side=target_leg.side)
        reference_price_source: Literal["best_bid", "best_ask"] = (
            "best_ask" if side == "buy" else "best_bid"
        )
        multiplier = (
            Decimal("1") + _bps_decimal(slippage_tolerance_bps)
            if side == "buy"
            else Decimal("1") - _bps_decimal(slippage_tolerance_bps)
        )

        quantity = abs(_required_decimal(position, "size", "position_size", "qty", "quantity"))
        snapshot = await self.fetch_snapshot("paradex", symbol)
        top_of_book = snapshot.market.top_of_book
        if top_of_book is None:
            raise ValueError(f"Paradex is missing top-of-book data for {symbol}")

        reference_price = (
            top_of_book.best_ask_price
            if reference_price_source == "best_ask"
            else top_of_book.best_bid_price
        )
        if reference_price is None or reference_price <= 0:
            raise ValueError(f"Paradex is missing a usable {reference_price_source} for {symbol}")

        reference = Decimal(str(reference_price))
        constraints = _extract_order_constraints("paradex", snapshot)
        quantity = _snap_quantity(quantity, constraints.quantity_increment)
        if quantity <= 0:
            raise ValueError(f"Paradex cleanup quantity snapped to zero for {symbol}")
        worst_price = _snap_price(reference * multiplier, constraints.price_increment, side=side)
        quantity_text = _format_order_value(
            venue="paradex",
            value=quantity,
            increment=constraints.quantity_increment,
        )
        worst_price_text = _format_order_value(
            venue="paradex",
            value=worst_price,
            increment=constraints.price_increment,
        )
        effective_notional = quantity * reference
        client_order_id = f"carryme-cleanup-pt{paper_trade_id}-paradex-{side}"

        return VenueOrderPreview(
            venue="paradex",
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
            price_increment=_to_float(constraints.price_increment),
            max_order_value=_to_float(constraints.max_order_value),
            reduce_only=True,
            endpoint_path_hint="/v1/orders",
            required_auth_env_vars=[
                "CARRYME_API_PARADEX_ACCOUNT_ADDRESS",
                "CARRYME_API_PARADEX_PRIVATE_KEY",
            ],
            auth_scheme="main account address + subkey private key",
            payload={
                "market": symbol,
                "side": side.upper(),
                "type": "LIMIT",
                "size": quantity_text,
                "price": worst_price_text,
                "instruction": "IOC",
                "client_id": client_order_id,
                "reduce_only": True,
            },
            notes=[
                "Cleanup preview uses live top-of-book and venue price increments.",
                "Reduce-only close preview does not enforce standard minimum-notional checks.",
            ],
        )


def _determine_cleanup_side(
    *,
    position: dict[str, Any],
    fallback_side: Literal["buy", "sell"],
) -> Literal["buy", "sell"]:
    signed_size = _optional_decimal(position, "size", "position_size", "qty", "quantity")
    if signed_size is not None:
        if signed_size > 0:
            return "sell"
        if signed_size < 0:
            return "buy"

    side_value = _string_value(position, "side")
    if side_value is not None:
        normalized_side = side_value.upper()
        if normalized_side in {"BUY", "LONG"}:
            return "sell"
        if normalized_side in {"SELL", "SHORT"}:
            return "buy"

    raise ValueError("Paradex position direction is ambiguous (missing/zero size and unknown side)")


def _optional_decimal(payload: dict[str, Any], *keys: str) -> Decimal | None:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        try:
            return Decimal(str(value))
        except Exception:
            continue
    return None


def _select_open_paradex_leg(
    entry: ExecutionJournalEntry,
    pair_status: ExecutionPairStatus,
) -> ExecutionLegResult:
    position_symbols_by_venue = {
        venue.venue: set(venue.position_symbols) for venue in pair_status.reconciliation.venues
    }
    candidates = [
        leg
        for leg in entry.legs
        if leg.venue == "paradex" and leg.symbol in position_symbols_by_venue.get("paradex", set())
    ]
    if len(candidates) != 1:
        raise ValueError("Cleanup preview requires exactly one open Paradex leg")
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


def _unwrap_rows(payload: dict[str, Any] | list[Any]) -> tuple[list[dict[str, Any]], str | None]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)], None
    if isinstance(payload, dict):
        for key in ("results", "positions", "data", "rows"):
            nested = payload.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)], key
        _logger.warning(
            "Paradex positions payload missing expected top-level containers keys=%s",
            sorted(payload.keys()),
        )
    return [], None


def _matching_symbol_key(payload: dict[str, Any], symbol: str) -> str | None:
    for key in ("market", "symbol", "ticker"):
        if _string_value(payload, key) == symbol:
            return key
    return None


def _string_value(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value:
        return value
    if isinstance(value, int):
        return str(value)
    return None


def _required_decimal(payload: dict[str, Any], *keys: str) -> Decimal:
    for key in keys:
        value = payload.get(key)
        if value is None:
            continue
        return Decimal(str(value))
    raise ValueError(f"Paradex position is missing all quantity keys: {', '.join(keys)}")
