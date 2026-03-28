"""Shared candidate-selection helpers."""

from __future__ import annotations

from carryme_models import OpportunityRecord


def filter_candidate_records(
    records: list[OpportunityRecord],
    *,
    min_one_day_net_edge_after_entry: float | None = None,
    min_capacity_notional: float | None = None,
) -> list[OpportunityRecord]:
    """Keep only records that satisfy the requested candidate thresholds."""

    selected: list[OpportunityRecord] = []
    for record in records:
        if (
            min_one_day_net_edge_after_entry is not None
            and record.opportunity.one_day_net_edge_after_entry < min_one_day_net_edge_after_entry
        ):
            continue

        capacity = (
            record.opportunity.capacity.max_entry_notional
            if record.opportunity.capacity is not None
            else None
        )
        if (
            min_capacity_notional is not None
            and (capacity is None or capacity < min_capacity_notional)
        ):
            continue

        selected.append(record)
    return selected
