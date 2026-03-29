"""Read-only reconciliation for execution journal entries."""

from __future__ import annotations

from carryme_models import (
    ExecutionJournalEntry,
    ExecutionReconciliation,
    ExecutionVenueReconciliation,
    PaperTradeAccountPreflight,
)


def reconcile_execution(
    entry: ExecutionJournalEntry,
    account_preflight: PaperTradeAccountPreflight,
) -> ExecutionReconciliation:
    """Combine the latest execution journal entry with current account-state summaries."""

    legs_by_venue: dict[str, list[str]] = {}
    for leg in entry.legs:
        legs_by_venue.setdefault(leg.venue, []).append(leg.symbol)

    venue_results: list[ExecutionVenueReconciliation] = []
    matched_all_leg_symbols = True
    for venue_state in account_preflight.venues:
        expected_symbols = legs_by_venue.get(venue_state.venue, [])
        matched_symbols = [
            symbol for symbol in expected_symbols if symbol in venue_state.position_symbols
        ]
        unmatched_symbols = [
            symbol for symbol in expected_symbols if symbol not in venue_state.position_symbols
        ]
        if unmatched_symbols:
            matched_all_leg_symbols = False
        venue_results.append(
            ExecutionVenueReconciliation(
                venue=venue_state.venue,
                authenticated=venue_state.authenticated,
                ready=venue_state.ready,
                account_identifier=venue_state.account_identifier,
                total_collateral=venue_state.total_collateral,
                available_to_trade=venue_state.available_to_trade,
                free_collateral=venue_state.free_collateral,
                balance_assets=venue_state.balance_assets,
                position_symbols=venue_state.position_symbols,
                matched_leg_symbols=matched_symbols,
                unmatched_leg_symbols=unmatched_symbols,
                notes=venue_state.notes,
                blocking_reasons=venue_state.blocking_reasons,
            )
        )
        if not venue_state.ready:
            matched_all_leg_symbols = False

    recommended_action = _recommended_action(entry, venue_results)
    notes = _build_notes(entry, venue_results)
    return ExecutionReconciliation(
        execution_entry_id=entry.entry_id,
        paper_trade_id=entry.paper_trade_id,
        preview_hash=entry.preview_hash,
        status=entry.status,
        recommended_action=recommended_action,
        matched_all_leg_symbols=matched_all_leg_symbols,
        venues=venue_results,
        notes=notes,
    )


def _recommended_action(
    entry: ExecutionJournalEntry,
    venues: list[ExecutionVenueReconciliation],
) -> str:
    if any(not venue.authenticated or not venue.ready for venue in venues):
        return "restore_account_read_access"
    if entry.status == "partial":
        if any(venue.matched_leg_symbols for venue in venues):
            return "complete_or_unwind_missing_leg"
        return "manual_review_required"
    if entry.status == "submitted":
        if all(not venue.unmatched_leg_symbols for venue in venues):
            return "monitor_open_hedge"
        return "verify_fill_status"
    if entry.status == "rejected":
        return "no_action"
    return "manual_review_required"


def _build_notes(
    entry: ExecutionJournalEntry,
    venues: list[ExecutionVenueReconciliation],
) -> list[str]:
    notes: list[str] = []
    if entry.status == "partial":
        notes.append(
            "One leg failed or was not confirmed as submitted; manual hedge completion or unwind "
            "may be required."
        )
    elif entry.status == "submitted":
        notes.append(
            "Submission was journaled for both legs, but current positions still need to be "
            "verified against live venue state."
        )

    if any(venue.unmatched_leg_symbols for venue in venues):
        notes.append(
            "Absence of a matching symbol in the current account snapshot does not prove no fill; "
            "venue order status and eventual consistency still matter."
        )
    return notes
