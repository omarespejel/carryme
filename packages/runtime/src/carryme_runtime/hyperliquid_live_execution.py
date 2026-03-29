"""Hyperliquid live execution service for confirmed preview legs."""

from __future__ import annotations

import asyncio
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal

from carryme_connectors import ConnectorError, build_hyperliquid_exchange
from carryme_models import (
    CleanupPreviewConfirmationEntry,
    ExecutionJournalEntry,
    ExecutionLegResult,
    PaperTradeEntry,
    PreviewConfirmationEntry,
    VenueOrderPreview,
)


@dataclass(frozen=True)
class HyperliquidLiveExecutionService:
    """Submit one confirmed Hyperliquid preview leg through the official SDK."""

    account_address: str
    api_wallet_private_key: str

    async def submit_confirmed_preview(
        self,
        *,
        paper_trade: PaperTradeEntry,
        confirmation: PreviewConfirmationEntry,
        executed_at: datetime | None = None,
    ) -> ExecutionJournalEntry:
        if paper_trade.entry_id is None:
            raise ValueError("Paper trade entry_id is required before live execution")
        if confirmation.entry_id is None:
            raise ValueError("Preview confirmation entry_id is required before live execution")

        return await self._submit_venue_order(
            paper_trade=paper_trade,
            preview_hash=confirmation.preview_hash,
            confirmation_entry_id=confirmation.entry_id,
            adapter_name="hyperliquid_live",
            leg=self._select_hyperliquid_leg(confirmation),
            executed_at=executed_at,
        )

    async def submit_confirmed_cleanup_preview(
        self,
        *,
        paper_trade: PaperTradeEntry,
        confirmation: CleanupPreviewConfirmationEntry,
        executed_at: datetime | None = None,
    ) -> ExecutionJournalEntry:
        """Submit one confirmed Hyperliquid cleanup preview to the live venue."""

        if paper_trade.entry_id is None:
            raise ValueError("Paper trade entry_id is required before live cleanup execution")
        if confirmation.entry_id is None:
            raise ValueError("Cleanup confirmation entry_id is required before live execution")

        return await self._submit_venue_order(
            paper_trade=paper_trade,
            preview_hash=confirmation.preview_hash,
            confirmation_entry_id=confirmation.entry_id,
            adapter_name="hyperliquid_cleanup_live",
            leg=confirmation.preview.leg,
            executed_at=executed_at,
        )

    async def _submit_venue_order(
        self,
        *,
        paper_trade: PaperTradeEntry,
        preview_hash: str,
        confirmation_entry_id: int,
        adapter_name: str,
        leg: VenueOrderPreview,
        executed_at: datetime | None = None,
    ) -> ExecutionJournalEntry:
        timestamp = executed_at or datetime.now(UTC)
        try:
            response_payload = await asyncio.to_thread(
                _submit_hyperliquid_order,
                account_address=self.account_address,
                api_wallet_private_key=self.api_wallet_private_key,
                leg=leg,
            )
        except Exception as exc:
            raise ConnectorError(f"Hyperliquid live submit failed: {exc}") from exc

        leg_status, external_reference = _extract_hyperliquid_submission_result(response_payload)
        return ExecutionJournalEntry(
            executed_at=timestamp,
            adapter=adapter_name,
            mode="live",
            status=leg_status,
            paper_trade_id=paper_trade.entry_id,
            preview_hash=preview_hash,
            confirmation_entry_id=confirmation_entry_id,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue=leg.venue,
                    symbol=leg.symbol,
                    fee_profile=leg.fee_profile,
                    side=leg.side,
                    target_notional=leg.target_notional,
                    status=leg_status,
                    simulated=False,
                    external_reference=external_reference,
                    request_payload=leg.payload,
                    response_payload=response_payload,
                )
            ],
        )

    @staticmethod
    def _select_hyperliquid_leg(confirmation: PreviewConfirmationEntry) -> VenueOrderPreview:
        legs = [leg for leg in confirmation.preview.legs if leg.venue == "hyperliquid"]
        if len(legs) != 1:
            raise ValueError(
                "Expected exactly one Hyperliquid leg in the confirmed preview "
                "before live submission"
            )
        return legs[0]


def _submit_hyperliquid_order(
    *,
    account_address: str,
    api_wallet_private_key: str,
    leg: VenueOrderPreview,
) -> dict[str, Any]:
    exchange = build_hyperliquid_exchange(
        private_key=api_wallet_private_key,
        account_address=account_address,
    )
    payload = leg.payload
    if not isinstance(payload, dict):
        raise ValueError("Hyperliquid preview payload must be a dictionary")
    result = exchange.order(
        name=leg.symbol,
        is_buy=leg.side == "buy",
        sz=float(leg.quantity_text),
        limit_px=float(leg.worst_price_text),
        order_type={"limit": {"tif": "Ioc"}},
        reduce_only=leg.reduce_only,
    )
    if not isinstance(result, dict):
        return {"payload": result}
    return result


def _extract_hyperliquid_submission_result(
    response_payload: dict[str, Any],
) -> tuple[Literal["submitted", "rejected"], str | None]:
    response = response_payload.get("response")
    if isinstance(response, dict):
        data = response.get("data")
        if isinstance(data, dict):
            statuses = data.get("statuses")
            if isinstance(statuses, list) and statuses:
                first = statuses[0]
                if isinstance(first, dict):
                    error = first.get("error")
                    if isinstance(error, str) and error:
                        return "rejected", None
                    for key in ("resting", "filled"):
                        value = first.get(key)
                        if isinstance(value, dict):
                            oid = value.get("oid")
                            if isinstance(oid, int | str):
                                return "submitted", str(oid)
                            return "submitted", None
    status = response_payload.get("status")
    if isinstance(status, str) and status.lower() == "ok":
        return "submitted", None
    return "rejected", None
