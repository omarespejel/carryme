"""Helpers for gating future live submission on explicit preview confirmation."""

from carryme_models import CleanupPreviewConfirmationEntry, PreviewConfirmationEntry


def require_confirmed_preview(
    *,
    paper_trade_id: int,
    preview_hash: str,
    confirmations: list[PreviewConfirmationEntry],
) -> PreviewConfirmationEntry:
    """Return the matching confirmation entry or raise when none exists."""

    for confirmation in confirmations:
        if (
            confirmation.paper_trade_id == paper_trade_id
            and confirmation.preview_hash == preview_hash
        ):
            return confirmation
    raise ValueError(
        f"No preview confirmation matched paper_trade_id={paper_trade_id} "
        f"and preview_hash={preview_hash}"
    )


def require_confirmed_cleanup_preview(
    *,
    paper_trade_id: int,
    preview_hash: str,
    confirmations: list[CleanupPreviewConfirmationEntry],
) -> CleanupPreviewConfirmationEntry:
    """Return the matching cleanup confirmation entry or raise when none exists."""

    for confirmation in confirmations:
        if (
            confirmation.paper_trade_id == paper_trade_id
            and confirmation.preview_hash == preview_hash
        ):
            return confirmation
    raise ValueError(
        f"No cleanup preview confirmation matched paper_trade_id={paper_trade_id} "
        f"and preview_hash={preview_hash}"
    )
