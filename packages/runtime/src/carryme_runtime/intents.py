"""Dry-run trade intent helpers."""

from __future__ import annotations

from carryme_models import FundingPairTradeIntent, OpportunityRecord, TradeLegIntent


def build_trade_intent(
    record: OpportunityRecord,
    *,
    capacity_fraction: float,
    max_target_notional: float,
    min_one_day_net_edge_after_entry: float,
    min_capacity_notional: float,
    max_break_even_days_entry: float | None = None,
) -> FundingPairTradeIntent:
    """Build a deterministic paired trade intent from a persisted opportunity record."""

    if capacity_fraction <= 0 or capacity_fraction > 1:
        raise ValueError("capacity_fraction must be within (0, 1]")
    if max_target_notional <= 0:
        raise ValueError("max_target_notional must be greater than zero")
    if record.opportunity.one_day_net_edge_after_entry < min_one_day_net_edge_after_entry:
        raise ValueError("Opportunity one-day net entry edge is below the configured threshold")

    capacity = (
        record.opportunity.capacity.max_entry_notional
        if record.opportunity.capacity is not None
        else None
    )
    if capacity is None or capacity <= 0:
        raise ValueError("Opportunity does not include a usable capacity estimate")
    if capacity < min_capacity_notional:
        raise ValueError("Opportunity capacity is below the configured threshold")

    break_even_days_entry = record.opportunity.break_even_days_entry
    if (
        max_break_even_days_entry is not None
        and (
            break_even_days_entry is None
            or break_even_days_entry > max_break_even_days_entry
        )
    ):
        raise ValueError("Opportunity break-even days exceed the configured threshold")

    target_notional = min(capacity * capacity_fraction, max_target_notional)
    if target_notional <= 0:
        raise ValueError("Calculated target notional must be greater than zero")

    label = record.pair.label or record.opportunity.canonical_symbol
    return FundingPairTradeIntent(
        label=label,
        canonical_symbol=record.opportunity.canonical_symbol,
        source_recorded_at=record.recorded_at,
        one_day_net_edge_after_entry=record.opportunity.one_day_net_edge_after_entry,
        break_even_days_entry=break_even_days_entry,
        capacity_limit_notional=capacity,
        target_notional=target_notional,
        capacity_fraction=capacity_fraction,
        max_target_notional=max_target_notional,
        long_leg=TradeLegIntent(
            venue=record.opportunity.long_venue,
            symbol=record.pair.right_symbol
            if record.opportunity.long_venue == record.pair.right_venue
            else record.pair.left_symbol,
            fee_profile=record.opportunity.long_fee_profile,
            side="buy",
            target_notional=target_notional,
        ),
        short_leg=TradeLegIntent(
            venue=record.opportunity.short_venue,
            symbol=record.pair.right_symbol
            if record.opportunity.short_venue == record.pair.right_venue
            else record.pair.left_symbol,
            fee_profile=record.opportunity.short_fee_profile,
            side="sell",
            target_notional=target_notional,
        ),
    )
