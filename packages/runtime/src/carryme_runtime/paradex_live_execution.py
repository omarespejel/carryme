"""Paradex live execution service for confirmed preview legs."""

from __future__ import annotations

import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Any, Literal, Protocol

import httpx
from carryme_connectors import (
    PARADEX_API_BASE_URL,
    PARADEX_ORDER_PATH,
    ParadexJwtTokenProvider,
    build_signed_paradex_order_payload,
)
from carryme_models import (
    CleanupPreviewConfirmationEntry,
    ExecutionJournalEntry,
    ExecutionLegResult,
    PaperTradeEntry,
    PreviewConfirmationEntry,
    VenueOrderPreview,
)

_logger = logging.getLogger(__name__)


class ParadexLiveTokenProvider(Protocol):
    """Protocol for the subset of Paradex token-provider behavior this service needs."""

    async def fetch_system_config(
        self,
        client: httpx.AsyncClient | None = None,
    ) -> Any: ...

    async def issue_jwt_token(
        self,
        *,
        account_address: str,
        private_key: str,
        client: httpx.AsyncClient | None = None,
        now: int | None = None,
    ) -> str: ...


@dataclass(frozen=True)
class ParadexLiveExecutionService:
    """Submit one confirmed Paradex preview leg to the live venue."""

    account_address: str
    private_key: str
    recv_window_ms: int = 300_000
    base_url: str = PARADEX_API_BASE_URL
    token_provider: ParadexLiveTokenProvider = ParadexJwtTokenProvider()

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
            adapter_name="paradex_live",
            leg=self._select_paradex_leg(confirmation),
            executed_at=executed_at,
        )

    async def submit_confirmed_cleanup_preview(
        self,
        *,
        paper_trade: PaperTradeEntry,
        confirmation: CleanupPreviewConfirmationEntry,
        executed_at: datetime | None = None,
    ) -> ExecutionJournalEntry:
        """Submit one confirmed Paradex cleanup preview to the live venue."""

        if paper_trade.entry_id is None:
            raise ValueError("Paper trade entry_id is required before live cleanup execution")
        if confirmation.entry_id is None:
            raise ValueError("Cleanup confirmation entry_id is required before live execution")

        return await self._submit_venue_order(
            paper_trade=paper_trade,
            preview_hash=confirmation.preview_hash,
            confirmation_entry_id=confirmation.entry_id,
            adapter_name="paradex_cleanup_live",
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
        request_timeout = httpx.Timeout(15.0, connect=5.0)

        async with httpx.AsyncClient(base_url=self.base_url, timeout=request_timeout) as client:
            system_config = await self.token_provider.fetch_system_config(client)
            jwt_token = await self.token_provider.issue_jwt_token(
                account_address=self.account_address,
                private_key=self.private_key,
                client=client,
            )
            signed_payload = build_signed_paradex_order_payload(
                account_address=self.account_address,
                private_key=self.private_key,
                starknet_chain_id=system_config.starknet_chain_id,
                order_payload=leg.payload,
                recv_window_ms=self.recv_window_ms,
            )
            try:
                response = await client.post(
                    PARADEX_ORDER_PATH,
                    headers={"Authorization": f"Bearer {jwt_token}"},
                    json=signed_payload,
                    timeout=request_timeout,
                )
            except (httpx.TimeoutException, httpx.NetworkError) as exc:
                failure_payload = {
                    "error_type": type(exc).__name__,
                    "error_message": str(exc),
                    "account_address": self.account_address,
                    "recv_window_ms": self.recv_window_ms,
                }
                return ExecutionJournalEntry(
                    executed_at=timestamp,
                    adapter="paradex_live",
                    mode="live",
                    status="rejected",
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
                            status="rejected",
                            simulated=False,
                            external_reference=_pick_external_reference(
                                failure_payload,
                                signed_payload,
                            ),
                            request_payload=signed_payload,
                            response_payload=failure_payload,
                            signature_timestamp_ms=_coerce_int(
                                signed_payload.get("signature_timestamp")
                            ),
                        )
                    ],
                )

        response_payload = _response_payload(response)
        accepted = 200 <= response.status_code < 300
        leg_status: Literal["submitted", "rejected"] = "submitted" if accepted else "rejected"
        external_reference = _pick_external_reference(response_payload, signed_payload)

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
                    request_payload=signed_payload,
                    response_payload=response_payload,
                    signature_timestamp_ms=_coerce_int(signed_payload.get("signature_timestamp")),
                )
            ],
        )

    @staticmethod
    def _select_paradex_leg(confirmation: PreviewConfirmationEntry) -> VenueOrderPreview:
        paradex_legs = [leg for leg in confirmation.preview.legs if leg.venue == "paradex"]
        if len(paradex_legs) != 1:
            raise ValueError(
                "Expected exactly one Paradex leg in the confirmed preview before live submission"
            )
        return paradex_legs[0]


def _response_payload(response: httpx.Response) -> dict[str, Any]:
    try:
        payload = response.json()
    except ValueError:
        _logger.warning("Paradex response was not JSON: status=%s", response.status_code)
        return {
            "status_code": response.status_code,
            "text": response.text,
        }
    if isinstance(payload, dict):
        return {
            "status_code": response.status_code,
            **payload,
        }
    _logger.warning(
        "Paradex response JSON was not an object: status=%s type=%s",
        response.status_code,
        type(payload).__name__,
    )
    return {
        "status_code": response.status_code,
        "payload": payload,
    }


def _pick_external_reference(
    response_payload: dict[str, Any],
    signed_payload: dict[str, Any],
) -> str | None:
    for key in ("id", "order_id", "client_id"):
        value = response_payload.get(key)
        if isinstance(value, str) and value:
            return value
    client_id = signed_payload.get("client_id")
    if isinstance(client_id, str) and client_id:
        return client_id
    return None


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None
