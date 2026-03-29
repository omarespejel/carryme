"""Helpers for gating future live submission on explicit preview confirmation."""

from carryme_models import (
    CleanupPreviewConfirmationEntry,
    PairClosePreviewConfirmationEntry,
    PreviewConfirmationEntry,
)


def _require_confirmation[
    ConfirmationEntryT: (PreviewConfirmationEntry, CleanupPreviewConfirmationEntry)
](
    *,
    paper_trade_id: int,
    preview_hash: str,
    confirmations: list[ConfirmationEntryT],
    label: str,
) -> ConfirmationEntryT:
    """Return the matching confirmation entry or raise when none exists."""

    for confirmation in confirmations:
        if (
            confirmation.paper_trade_id == paper_trade_id
            and confirmation.preview_hash == preview_hash
        ):
            return confirmation
    raise ValueError(
        f"No {label} matched paper_trade_id={paper_trade_id} and preview_hash={preview_hash}"
    )


def require_confirmed_preview(
    *,
    paper_trade_id: int,
    preview_hash: str,
    confirmations: list[PreviewConfirmationEntry],
) -> PreviewConfirmationEntry:
    """Return the matching confirmation entry or raise when none exists."""

    return _require_confirmation(
        paper_trade_id=paper_trade_id,
        preview_hash=preview_hash,
        confirmations=confirmations,
        label="preview confirmation",
    )


def require_confirmed_cleanup_preview(
    *,
    paper_trade_id: int,
    preview_hash: str,
    confirmations: list[CleanupPreviewConfirmationEntry],
) -> CleanupPreviewConfirmationEntry:
    """Return the matching cleanup confirmation entry or raise when none exists."""

    return _require_confirmation(
        paper_trade_id=paper_trade_id,
        preview_hash=preview_hash,
        confirmations=confirmations,
        label="cleanup preview confirmation",
    )


def require_confirmed_pair_close_preview(
    *,
    paper_trade_id: int,
    preview_hash: str,
    confirmations: list[PairClosePreviewConfirmationEntry],
) -> PairClosePreviewConfirmationEntry:
    """Return the matching pair-close confirmation entry or raise when none exists."""

    for confirmation in confirmations:
        if (
            confirmation.paper_trade_id == paper_trade_id
            and confirmation.preview_hash == preview_hash
        ):
            return confirmation
    raise ValueError(
        f"No pair-close preview confirmation matched paper_trade_id={paper_trade_id} "
        f"and preview_hash={preview_hash}"
    )
