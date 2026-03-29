"""Unified readiness helpers for future live submission."""

from carryme_models import (
    LiveSubmissionReadiness,
    PaperTradeAccountPreflight,
    PaperTradeExecutionPreflight,
    PreviewConfirmationEntry,
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
    except ValueError as exc:
        blocking_reasons.append(str(exc))

    blocking_reasons.extend(execution_preflight.blocking_reasons)
    blocking_reasons.extend(account_preflight.blocking_reasons)

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
        blocking_reasons=deduped_reasons,
    )
