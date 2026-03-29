"""Extended live execution service for confirmed preview legs."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from typing import Any, Literal

import httpx
from carryme_connectors import (
    EXTENDED_API_BASE_URL,
    EXTENDED_ORDER_PATH,
    ConnectorError,
    ExtendedPrivateConnector,
    build_signed_extended_order_payload,
)
from carryme_models import (
    ExecutionJournalEntry,
    ExecutionLegResult,
    PaperTradeEntry,
    PreviewConfirmationEntry,
    VenueOrderPreview,
)
from carryme_normalizers import get_fee_profile

from carryme_runtime.opportunities import SnapshotFetcher, fetch_live_snapshot

_logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ExtendedLiveExecutionService:
    """Submit one confirmed Extended preview leg to the live venue."""

    api_key: str
    stark_private_key: str
    fetch_snapshot: SnapshotFetcher = fetch_live_snapshot
    base_url: str = EXTENDED_API_BASE_URL

    def __post_init__(self) -> None:
        if not self.api_key.strip():
            raise ValueError("Extended API key is required")
        if not self.stark_private_key.strip():
            raise ValueError("Extended Stark private key is required")

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

        leg = self._select_extended_leg(confirmation)
        snapshot = await self.fetch_snapshot("extended", leg.symbol)
        fee_rate = await self._resolve_taker_fee_rate(leg.symbol, leg.fee_profile)
        timestamp = executed_at or datetime.now(UTC)

        async with httpx.AsyncClient(
            base_url=self.base_url,
            headers={"x-api-key": self.api_key},
            timeout=15.0,
        ) as client:
            connector = ExtendedPrivateConnector(client)
            account, fees = await asyncio.gather(
                connector.fetch_account(),
                connector.fetch_fees(leg.symbol),
            )
            api_fee_rate = _pick_taker_fee_rate(fees)
            signed_payload = build_signed_extended_order_payload(
                api_key=self.api_key,
                stark_private_key=self.stark_private_key,
                account_payload=account,
                market_payload=_require_market_payload(snapshot.market.raw, symbol=leg.symbol),
                order_payload=leg.payload,
                taker_fee_rate=api_fee_rate if api_fee_rate is not None else fee_rate,
            )
            # Do not auto-retry live order writes: ambiguous transport failures require
            # operator reconciliation to avoid duplicating venue-side submissions.
            response = await client.post(EXTENDED_ORDER_PATH, json=signed_payload)

        response_payload = _response_payload(response)
        accepted = _is_success_response(response_payload)
        leg_status: Literal["submitted", "rejected"] = "submitted" if accepted else "rejected"
        external_reference = _pick_external_reference(response_payload, signed_payload)

        return ExecutionJournalEntry(
            executed_at=timestamp,
            adapter="extended_live",
            mode="live",
            status=leg_status,
            paper_trade_id=paper_trade.entry_id,
            preview_hash=confirmation.preview_hash,
            confirmation_entry_id=confirmation.entry_id,
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
                    request_payload=signed_payload,
                    response_payload=response_payload,
                )
            ],
        )

    async def _resolve_taker_fee_rate(self, symbol: str, fee_profile: str) -> Decimal:
        return Decimal(str(get_fee_profile("extended", fee_profile).taker_fee_rate))

    @staticmethod
    def _select_extended_leg(confirmation: PreviewConfirmationEntry) -> VenueOrderPreview:
        extended_legs = [leg for leg in confirmation.preview.legs if leg.venue == "extended"]
        if len(extended_legs) != 1:
            raise ValueError(
                "Expected exactly one Extended leg in the confirmed preview before live submission"
            )
        return extended_legs[0]


def _response_payload(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        return {
            "status_code": response.status_code,
            "text": response.text,
        }
    if isinstance(payload, dict):
        return {
            "status_code": response.status_code,
            **payload,
        }
    return {
        "status_code": response.status_code,
        "payload": payload,
    }


def _pick_external_reference(
    response_payload: dict[str, Any],
    signed_payload: dict[str, Any],
) -> str | None:
    body = response_payload.get("data")
    if isinstance(body, dict):
        for key in ("externalId", "id"):
            value = body.get(key)
            if isinstance(value, int | str):
                return str(value)
    for key in ("externalId", "id"):
        value = response_payload.get(key)
        if isinstance(value, int | str):
            return str(value)
    fallback_id = signed_payload.get("id")
    if isinstance(fallback_id, str) and fallback_id:
        return fallback_id
    return None


def _pick_taker_fee_rate(payload: dict[str, Any] | list[Any]) -> Decimal | None:
    if isinstance(payload, list):
        _logger.warning(
            "Extended fees payload returned a list; falling back to static fee profile"
        )
        return None
    body = payload.get("data", payload)
    if not isinstance(body, dict):
        _logger.warning(
            "Extended fees payload body was not an object; falling back to static fee profile"
        )
        return None
    for key in ("takerFee", "taker_fee", "takerFeeRate"):
        value = body.get(key)
        if value is None:
            continue
        return Decimal(str(value))
    return None


def _is_success_response(response_payload: dict[str, Any]) -> bool:
    status_code = response_payload.get("status_code")
    if not isinstance(status_code, int) or not 200 <= status_code < 300:
        return False
    if "error" in response_payload or "errors" in response_payload:
        return False
    status = response_payload.get("status")
    if isinstance(status, str) and status.strip().lower() in {
        "error",
        "failed",
        "fail",
        "rejected",
    }:
        return False
    code = response_payload.get("code")
    if isinstance(code, int):
        return code in {0, 200}
    if isinstance(code, str):
        return code.strip().lower() in {"0", "200", "ok", "success"}
    return True


def _require_market_payload(payload: dict[str, Any] | list[Any], *, symbol: str) -> dict[str, Any]:
    if isinstance(payload, dict):
        return payload
    raise ConnectorError(f"Extended market payload for {symbol} must be an object")
