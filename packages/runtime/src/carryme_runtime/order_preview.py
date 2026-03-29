"""Unsigned order preview builders for live execution planning."""

from __future__ import annotations

import asyncio
import hashlib
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import ROUND_CEILING, ROUND_FLOOR, ROUND_HALF_UP, Decimal
from typing import Literal, NamedTuple

from carryme_models import (
    NormalizedMarketSnapshot,
    PaperTradeEntry,
    PaperTradeOrderPreview,
    VenueOrderPreview,
)

from carryme_runtime.opportunities import SnapshotFetcher, fetch_live_snapshot


class VenueOrderSpec(NamedTuple):
    """Static preview metadata for one live venue."""

    endpoint_path_hint: str
    auth_scheme: str
    required_auth_env_vars: tuple[str, ...]
    notes: tuple[str, ...]


_VENUE_ORDER_SPECS: dict[str, VenueOrderSpec] = {
    "extended": VenueOrderSpec(
        endpoint_path_hint="/api/v1/user/order",
        auth_scheme="api key + Stark signing key",
        required_auth_env_vars=(
            "CARRYME_API_EXTENDED_API_KEY",
            "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY",
        ),
        notes=(
            "Preview uses an IOC limit cap from the public best bid/ask.",
            "Live submit expands this preview into the exact Extended settlement schema.",
        ),
    ),
    "paradex": VenueOrderSpec(
        endpoint_path_hint="/v1/orders",
        auth_scheme="main account address + subkey private key",
        required_auth_env_vars=(
            "CARRYME_API_PARADEX_ACCOUNT_ADDRESS",
            "CARRYME_API_PARADEX_PRIVATE_KEY",
        ),
        notes=(
            "Preview uses an IOC limit cap from the public best bid/ask.",
            (
                "Final live adapter must confirm any instrument-specific lot or "
                "price increments before submission."
            ),
        ),
    ),
}


@dataclass(frozen=True)
class OrderConstraints:
    """Venue-specific constraints used to make previews executable."""

    quantity_increment: Decimal | None = None
    minimum_order_size: Decimal | None = None
    minimum_notional: Decimal | None = None
    price_increment: Decimal | None = None
    max_order_value: Decimal | None = None


@dataclass
class OrderPreviewService:
    """Build deterministic unsigned order previews from saved paper trades."""

    fetch_snapshot: SnapshotFetcher = fetch_live_snapshot

    async def preview_paper_trade(
        self,
        paper_trade: PaperTradeEntry,
        *,
        slippage_tolerance_bps: int = 10,
        generated_at: datetime | None = None,
    ) -> PaperTradeOrderPreview:
        if paper_trade.entry_id is None:
            raise ValueError("Paper trade entry_id is required before preview generation")
        if slippage_tolerance_bps < 0:
            raise ValueError("slippage_tolerance_bps must be non-negative")

        long_leg = paper_trade.intent.long_leg
        short_leg = paper_trade.intent.short_leg
        long_snapshot, short_snapshot = await asyncio.gather(
            self.fetch_snapshot(long_leg.venue, long_leg.symbol),
            self.fetch_snapshot(short_leg.venue, short_leg.symbol),
        )

        legs = [
            _build_leg_preview(
                venue=long_leg.venue,
                symbol=long_leg.symbol,
                fee_profile=long_leg.fee_profile,
                side=long_leg.side,
                target_notional=long_leg.target_notional,
                snapshot=long_snapshot,
                paper_trade_id=paper_trade.entry_id,
                slippage_tolerance_bps=slippage_tolerance_bps,
            ),
            _build_leg_preview(
                venue=short_leg.venue,
                symbol=short_leg.symbol,
                fee_profile=short_leg.fee_profile,
                side=short_leg.side,
                target_notional=short_leg.target_notional,
                snapshot=short_snapshot,
                paper_trade_id=paper_trade.entry_id,
                slippage_tolerance_bps=slippage_tolerance_bps,
            ),
        ]
        timestamp = generated_at or datetime.now(UTC)
        preview_hash = _preview_hash(
            paper_trade_id=paper_trade.entry_id,
            label=paper_trade.intent.label,
            slippage_tolerance_bps=slippage_tolerance_bps,
            legs=legs,
        )
        return PaperTradeOrderPreview(
            paper_trade_id=paper_trade.entry_id,
            label=paper_trade.intent.label,
            generated_at=timestamp,
            slippage_tolerance_bps=slippage_tolerance_bps,
            preview_hash=preview_hash,
            legs=legs,
        )


def _build_leg_preview(
    *,
    venue: str,
    symbol: str,
    fee_profile: str,
    side: Literal["buy", "sell"],
    target_notional: float,
    snapshot: NormalizedMarketSnapshot,
    paper_trade_id: int,
    slippage_tolerance_bps: int,
) -> VenueOrderPreview:
    venue_key = venue.strip().lower()
    spec = _VENUE_ORDER_SPECS.get(venue_key)
    if spec is None:
        raise ValueError(f"Unsupported live order preview venue: {venue}")

    top_of_book = snapshot.market.top_of_book
    if top_of_book is None:
        raise ValueError(f"Venue {venue} did not return top-of-book data for {symbol}")

    if side == "buy":
        reference_price = top_of_book.best_ask_price
        reference_price_source: Literal["best_ask", "best_bid"] = "best_ask"
        multiplier = Decimal("1") + (_bps_decimal(slippage_tolerance_bps))
    else:
        reference_price = top_of_book.best_bid_price
        reference_price_source = "best_bid"
        multiplier = Decimal("1") - (_bps_decimal(slippage_tolerance_bps))

    if reference_price is None or reference_price <= 0:
        raise ValueError(
            f"Venue {venue} is missing a usable {reference_price_source} price for {symbol}"
        )

    reference = Decimal(str(reference_price))
    constraints = _extract_order_constraints(venue_key, snapshot)
    raw_quantity = Decimal(str(target_notional)) / reference
    quantity = _snap_quantity(raw_quantity, constraints.quantity_increment)
    if quantity <= 0:
        raise ValueError(f"Venue {venue} snapped quantity to zero for {symbol}")
    if constraints.minimum_order_size is not None and quantity < constraints.minimum_order_size:
        raise ValueError(
            f"Venue {venue} preview quantity for {symbol} fell below minimum order size"
        )

    raw_worst_price = reference * multiplier
    worst_price = _snap_price(
        raw_worst_price,
        constraints.price_increment,
        side=side,
    )
    effective_notional = quantity * reference
    limit_order_value = quantity * worst_price
    if (
        constraints.minimum_notional is not None
        and effective_notional < constraints.minimum_notional
    ):
        raise ValueError(f"Venue {venue} preview notional for {symbol} fell below minimum notional")
    if constraints.max_order_value is not None and limit_order_value > constraints.max_order_value:
        raise ValueError(f"Venue {venue} preview notional for {symbol} exceeded max order value")

    quantity_text = _format_decimal(quantity)
    worst_price_text = _format_decimal(worst_price)
    client_order_id = f"carryme-pt{paper_trade_id}-{venue_key}-{side}"

    payload = _build_payload(
        venue=venue_key,
        symbol=symbol,
        side=side,
        quantity_text=quantity_text,
        worst_price_text=worst_price_text,
        client_order_id=client_order_id,
    )

    return VenueOrderPreview(
        venue=venue_key,
        symbol=symbol,
        fee_profile=fee_profile,
        side=side,
        target_notional=target_notional,
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
        endpoint_path_hint=spec.endpoint_path_hint,
        required_auth_env_vars=list(spec.required_auth_env_vars),
        auth_scheme=spec.auth_scheme,
        payload=payload,
        notes=[
            *spec.notes,
            (
                "Preview quantity and limit price were snapped to the venue's public "
                "order-size and price increments."
            ),
        ],
    )


def _build_payload(
    *,
    venue: str,
    symbol: str,
    side: Literal["buy", "sell"],
    quantity_text: str,
    worst_price_text: str,
    client_order_id: str,
) -> dict[str, str | bool]:
    side_upper = side.upper()
    if venue == "paradex":
        return {
            "market": symbol,
            "side": side_upper,
            "type": "LIMIT",
            "size": quantity_text,
            "price": worst_price_text,
            "instruction": "IOC",
            "client_id": client_order_id,
        }
    if venue == "extended":
        return {
            "symbol": symbol,
            "side": side_upper,
            "type": "LIMIT",
            "size": quantity_text,
            "price": worst_price_text,
            "time_in_force": "IOC",
            "client_order_id": client_order_id,
            "reduce_only": False,
        }
    raise ValueError(f"Unsupported live order preview venue: {venue}")


def _format_decimal(value: Decimal, places: int = 8) -> str:
    quant = Decimal("1").scaleb(-places)
    return format(value.quantize(quant, rounding=ROUND_HALF_UP), "f")


def _bps_decimal(value: int) -> Decimal:
    return Decimal(value) / Decimal(10_000)


def _extract_order_constraints(
    venue: str,
    snapshot: NormalizedMarketSnapshot,
) -> OrderConstraints:
    raw = snapshot.market.raw if isinstance(snapshot.market.raw, dict) else {}
    mark_price = Decimal(str(snapshot.market.mark_price)) if snapshot.market.mark_price else None
    if venue == "paradex":
        max_order_size = _dict_decimal(raw, "max_order_size")
        return OrderConstraints(
            quantity_increment=_dict_decimal(raw, "order_size_increment"),
            minimum_order_size=_dict_decimal(raw, "order_size_increment"),
            minimum_notional=_dict_decimal(raw, "min_notional"),
            price_increment=_dict_decimal(raw, "price_tick_size"),
            max_order_value=max_order_size * mark_price
            if max_order_size is not None and mark_price is not None
            else None,
        )
    if venue == "extended":
        trading_config = raw.get("tradingConfig", {})
        if not isinstance(trading_config, dict):
            trading_config = {}
        minimum_order_size = _dict_decimal(trading_config, "minOrderSize")
        return OrderConstraints(
            quantity_increment=_dict_decimal(trading_config, "minOrderSizeChange"),
            minimum_order_size=minimum_order_size,
            minimum_notional=minimum_order_size * mark_price
            if minimum_order_size is not None and mark_price is not None
            else None,
            price_increment=_dict_decimal(trading_config, "minPriceChange"),
            max_order_value=_dict_decimal(trading_config, "maxLimitOrderValue"),
        )
    return OrderConstraints()


def _dict_decimal(raw: dict[str, object], key: str) -> Decimal | None:
    value = raw.get(key)
    if value is None:
        return None
    return Decimal(str(value))


def _snap_quantity(value: Decimal, increment: Decimal | None) -> Decimal:
    if increment is None or increment <= 0:
        return value
    units = (value / increment).to_integral_value(rounding=ROUND_FLOOR)
    return units * increment


def _snap_price(
    value: Decimal,
    increment: Decimal | None,
    *,
    side: Literal["buy", "sell"],
) -> Decimal:
    if increment is None or increment <= 0:
        return value
    rounding = ROUND_CEILING if side == "buy" else ROUND_FLOOR
    units = (value / increment).to_integral_value(rounding=rounding)
    return units * increment


def _to_float(value: Decimal | None) -> float | None:
    return float(value) if value is not None else None


def _preview_hash(
    *,
    paper_trade_id: int,
    label: str,
    slippage_tolerance_bps: int,
    legs: list[VenueOrderPreview],
) -> str:
    encoded = json.dumps(
        {
            "paper_trade_id": paper_trade_id,
            "label": label,
            "slippage_tolerance_bps": slippage_tolerance_bps,
            "legs": [leg.model_dump(mode="json") for leg in legs],
        },
        sort_keys=True,
        separators=(",", ":"),
    ).encode("utf-8")
    return hashlib.sha256(encoded).hexdigest()
