"""Preview confirmation models."""

from datetime import datetime

from pydantic import BaseModel, Field, model_validator

from carryme_models.preview import (
    ExecutionCleanupPreview,
    ExecutionPairClosePreview,
    PaperTradeOrderPreview,
)


class PreviewConfirmationEntry(BaseModel):
    """An append-only operator confirmation of one unsigned order preview."""

    entry_id: int | None = None
    confirmed_at: datetime
    paper_trade_id: int
    label: str = Field(min_length=1)
    preview_hash: str = Field(min_length=1)
    preview: PaperTradeOrderPreview
    note: str | None = None


class CleanupPreviewConfirmationEntry(BaseModel):
    """An append-only operator confirmation of one cleanup preview."""

    entry_id: int | None = None
    confirmed_at: datetime
    paper_trade_id: int
    label: str = Field(min_length=1)
    preview_hash: str = Field(min_length=1)
    preview: ExecutionCleanupPreview
    note: str | None = None


class PairClosePreviewConfirmationEntry(BaseModel):
    """An append-only operator confirmation of one pair-close preview."""

    entry_id: int | None = None
    confirmed_at: datetime
    paper_trade_id: int
    label: str = Field(min_length=1)
    preview_hash: str = Field(min_length=1)
    preview: ExecutionPairClosePreview
    note: str | None = None

    @model_validator(mode="after")
    def _validate_preview_hash_consistency(self) -> "PairClosePreviewConfirmationEntry":
        if self.preview_hash != self.preview.preview_hash:
            raise ValueError("Pair-close confirmation preview_hash must match preview.preview_hash")
        return self
