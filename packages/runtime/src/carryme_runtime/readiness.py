"""Unified readiness helpers for future live submission."""

from carryme_models import (
    LiveSubmissionReadiness,
    PaperTradeAccountPreflight,
    PaperTradeExecutionPreflight,
    PaperTradeSystemState,
    PreviewConfirmationEntry,
    VenueAccountPreflight,
)

from carryme_runtime.confirmation import require_confirmed_preview


def build_live_submission_readiness(
    *,
    paper_trade_id: int,
    label: str,
    preview_hash: str,
    confirmations: list[PreviewConfirmationEntry],
    execution_preflight: PaperTradeExecutionPreflight,
    account_preflight: PaperTradeAccountPreflight,
    system_state: PaperTradeSystemState | None = None,
) -> LiveSubmissionReadiness:
    """Build one combined readiness decision for a saved paper trade."""

    blocking_reasons: list[str] = []
    confirmation_entry_id: int | None = None
    confirmed_preview = False

    try:
        confirmation = require_confirmed_preview(
            paper_trade_id=paper_trade_id,
            preview_hash=preview_hash,
            confirmations=confirmations,
        )
        confirmation_entry_id = confirmation.entry_id
        confirmed_preview = True
        blocking_reasons.extend(
            _build_zero_collateral_blockers(
                confirmation=confirmation,
                account_preflight=account_preflight,
            )
        )
    except ValueError as exc:
        blocking_reasons.append(str(exc))

    blocking_reasons.extend(execution_preflight.blocking_reasons)
    blocking_reasons.extend(account_preflight.blocking_reasons)
    if system_state is not None:
        blocking_reasons.extend(system_state.blocking_reasons)

    deduped_reasons: list[str] = []
    for reason in blocking_reasons:
        if reason not in deduped_reasons:
            deduped_reasons.append(reason)

    return LiveSubmissionReadiness(
        paper_trade_id=paper_trade_id,
        label=label,
        preview_hash=preview_hash,
        confirmation_entry_id=confirmation_entry_id,
        confirmed_preview=confirmed_preview,
        ready=not deduped_reasons,
        execution_preflight=execution_preflight,
        account_preflight=account_preflight,
        system_state=system_state,
        blocking_reasons=deduped_reasons,
    )


def _build_zero_collateral_blockers(
    *,
    confirmation: PreviewConfirmationEntry,
    account_preflight: PaperTradeAccountPreflight,
) -> list[str]:
    preview_legs = {leg.venue: leg for leg in confirmation.preview.legs}
    blockers: list[str] = []
    for venue_status in account_preflight.venues:
        preview_leg = preview_legs.get(venue_status.venue)
        if preview_leg is None:
            continue
        usable_collateral = _usable_collateral(venue_status)
        if usable_collateral is None:
            if venue_status.ready or venue_status.authenticated:
                blockers.append(
                    f"Venue {venue_status.venue} is missing collateral data for the "
                    f"confirmed {preview_leg.target_notional:.2f} notional preview"
                )
            continue
        if usable_collateral > 0:
            continue
        blockers.append(
            f"Venue {venue_status.venue} has no usable collateral for the confirmed "
            f"{preview_leg.target_notional:.2f} notional preview"
        )
    return blockers


def _usable_collateral(venue_status: VenueAccountPreflight) -> float | None:
    available_to_trade = venue_status.available_to_trade
    if isinstance(available_to_trade, int | float):
        return float(available_to_trade)

    free_collateral = venue_status.free_collateral
    if isinstance(free_collateral, int | float):
        return float(free_collateral)

    total_collateral = venue_status.total_collateral
    if isinstance(total_collateral, int | float):
        return float(total_collateral)

    return None
