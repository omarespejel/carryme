"""Pair-level status derived from execution journal, order state, and reconciliation."""

from __future__ import annotations

from typing import Literal

from carryme_models import (
    ExecutionJournalEntry,
    ExecutionOrderState,
    ExecutionPairStatus,
    ExecutionReconciliation,
)

DerivedPairState = Literal[
    "hedged",
    "pending",
    "unfilled",
    "closed",
    "cleanup_needed",
    "review_required",
]


def build_execution_pair_status(
    entry: ExecutionJournalEntry,
    order_state: ExecutionOrderState,
    reconciliation: ExecutionReconciliation,
) -> ExecutionPairStatus:
    """Return a single pair-level state for a journaled execution attempt."""

    position_presence = _position_presence_by_leg(entry, reconciliation)
    order_states = {item.venue: item.derived_state for item in order_state.legs}

    any_position = any(position_presence.values())
    all_positions = bool(position_presence) and all(position_presence.values())
    any_open = any(state == "open" for state in order_states.values())
    any_partial_fill = any(state == "partial_fill" for state in order_states.values())
    any_filled = any(state == "filled" for state in order_states.values())
    any_unknown = any(state in {"unknown", "unsupported"} for state in order_states.values())
    any_unfilled = any(state == "unfilled" for state in order_states.values())
    any_account_blocker = any(
        (not venue.authenticated) or (not venue.ready) for venue in reconciliation.venues
    )

    notes: list[str] = []
    derived_state: DerivedPairState
    recommended_action: str
    if any_partial_fill:
        derived_state = "cleanup_needed"
        recommended_action = "manual_review_required"
        notes.append("At least one leg is partially filled; do not assume the pair is hedged.")
    elif any_account_blocker:
        derived_state = "review_required"
        recommended_action = "restore_account_read_access"
        notes.append("Account-state reconciliation is incomplete for at least one venue.")
    elif any_open:
        derived_state = "pending"
        recommended_action = "wait_for_fill_or_timeout"
        notes.append("At least one venue still reports the order as open.")
    elif all_positions:
        derived_state = "hedged"
        recommended_action = "monitor_open_hedge"
        notes.append("Both legs are currently reflected in live position state.")
    elif any_position:
        derived_state = "cleanup_needed"
        if any_unfilled and not any_filled:
            recommended_action = "close_open_leg"
            notes.append("One leg is open while another was reported unfilled.")
        else:
            recommended_action = "complete_or_unwind_missing_leg"
            notes.append("Only part of the intended hedge is present in live position state.")
    elif _is_cleanup_execution(entry) and any_filled and not any_position:
        derived_state = "closed"
        recommended_action = "no_action"
        notes.append("Reduce-only cleanup execution filled and no live positions remain.")
    elif any_filled:
        derived_state = "review_required"
        recommended_action = "manual_review_required"
        notes.append("A venue reported a fill, but no matching position is currently visible.")
    elif any_unfilled and not any_position:
        derived_state = "unfilled"
        recommended_action = "no_action"
        notes.append("No live positions are present and at least one leg was explicitly unfilled.")
    elif not any_position and not any_unknown:
        derived_state = "unfilled"
        recommended_action = "no_action"
        notes.append("No live positions are present after the submission attempt.")
    else:
        derived_state = "review_required"
        recommended_action = "manual_review_required"
        notes.append("Live order state and positions do not resolve to a clear pair outcome.")

    notes.extend(order_state.notes)
    notes.extend(reconciliation.notes)
    return ExecutionPairStatus(
        execution_entry_id=entry.entry_id,
        paper_trade_id=entry.paper_trade_id,
        preview_hash=entry.preview_hash,
        derived_state=derived_state,
        recommended_action=recommended_action,
        order_state=order_state,
        reconciliation=reconciliation,
        notes=notes,
    )


def _position_presence_by_leg(
    entry: ExecutionJournalEntry,
    reconciliation: ExecutionReconciliation,
) -> dict[str, bool]:
    venues = {venue.venue: venue for venue in reconciliation.venues}
    position_presence: dict[str, bool] = {}
    for leg in entry.legs:
        venue_state = venues.get(leg.venue)
        position_presence[leg.venue] = (
            leg.symbol in venue_state.position_symbols if venue_state is not None else False
        )
    return position_presence


def _is_cleanup_execution(entry: ExecutionJournalEntry) -> bool:
    adapter = entry.adapter.lower()
    if "cleanup" in adapter:
        return True
    for leg in entry.legs:
        payload = leg.request_payload if isinstance(leg.request_payload, dict) else None
        if payload is None:
            return False
        reduce_only = payload.get("reduce_only")
        if reduce_only is None:
            reduce_only = payload.get("reduceOnly")
        if reduce_only is not True:
            return False
    return bool(entry.legs)
