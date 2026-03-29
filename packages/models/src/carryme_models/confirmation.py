"""Preview confirmation models."""

from datetime import datetime

from pydantic import BaseModel, Field

from carryme_models.preview import ExecutionCleanupPreview, PaperTradeOrderPreview


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
