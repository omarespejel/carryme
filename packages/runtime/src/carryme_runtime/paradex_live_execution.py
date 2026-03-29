"""Paradex live execution service for confirmed preview legs."""

from __future__ import annotations

import asyncio
import logging
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
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
    ExecutionLegOrderState,
    ExecutionLegResult,
    NormalizedMarketSnapshot,
    PaperTradeEntry,
    PreviewConfirmationEntry,
    VenueOrderPreview,
)

from carryme_runtime.execution_order_state import ParadexOrderStateObserver
from carryme_runtime.opportunities import SnapshotFetcher, fetch_live_snapshot
from carryme_runtime.order_preview import _format_order_value, _snap_price

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


class ParadexExecutionOrderObserver(Protocol):
    """Observe one Paradex order state from a submitted execution leg."""

    async def observe(self, leg: dict[str, Any]) -> ExecutionLegOrderState: ...


@dataclass(frozen=True)
class ParadexLiveExecutionService:
    """Submit one confirmed Paradex preview leg to the live venue."""

    account_address: str
    private_key: str
    recv_window_ms: int = 300_000
    adaptive_retry_attempts: int = 3
    adaptive_retry_poll_attempts: int = 3
    adaptive_retry_poll_interval_seconds: float = 0.5
    adaptive_retry_book_slippage_bps: int = 5
    base_url: str = PARADEX_API_BASE_URL
    token_provider: ParadexLiveTokenProvider = ParadexJwtTokenProvider()
    fetch_snapshot: SnapshotFetcher = fetch_live_snapshot

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

        return await self._submit_adaptive_confirmed_preview(
            paper_trade=paper_trade,
            preview_hash=confirmation.preview_hash,
            confirmation_entry_id=confirmation.entry_id,
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
        leg = self._select_paradex_cleanup_leg(confirmation)

        return await self._submit_adaptive_confirmed_preview(
            paper_trade=paper_trade,
            preview_hash=confirmation.preview_hash,
            confirmation_entry_id=confirmation.entry_id,
            leg=leg,
            executed_at=executed_at,
            adapter_name="paradex_cleanup_live",
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
        auth_usage = _paradex_auth_usage_for_fee_profile(leg.fee_profile)
        token_provider = _token_provider_for_auth_usage(
            default_provider=self.token_provider,
            auth_usage=auth_usage,
        )

        async with httpx.AsyncClient(base_url=self.base_url, timeout=request_timeout) as client:
            system_config = await token_provider.fetch_system_config(client)
            jwt_token = await token_provider.issue_jwt_token(
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
                    adapter=adapter_name,
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
                    auth_usage=auth_usage,
                    external_reference=external_reference,
                    request_payload=signed_payload,
                    response_payload=response_payload,
                    signature_timestamp_ms=_coerce_int(signed_payload.get("signature_timestamp")),
                )
            ],
        )

    async def _submit_adaptive_confirmed_preview(
        self,
        *,
        paper_trade: PaperTradeEntry,
        preview_hash: str,
        confirmation_entry_id: int,
        leg: VenueOrderPreview,
        executed_at: datetime | None = None,
        adapter_name: str = "paradex_live",
    ) -> ExecutionJournalEntry:
        if self.adaptive_retry_attempts <= 0:
            raise ValueError("adaptive_retry_attempts must be positive")
        if self.adaptive_retry_poll_attempts <= 0:
            raise ValueError("adaptive_retry_poll_attempts must be positive")
        if self.adaptive_retry_poll_interval_seconds < 0:
            raise ValueError("adaptive_retry_poll_interval_seconds must be non-negative")
        if self.adaptive_retry_book_slippage_bps < 0:
            raise ValueError("adaptive_retry_book_slippage_bps must be non-negative")

        observer = self._build_order_state_observer()
        attempt_history: list[dict[str, Any]] = []

        for attempt_index in range(1, self.adaptive_retry_attempts + 1):
            attempt_leg = await self._build_attempt_leg(
                base_leg=leg,
                attempt_index=attempt_index,
            )
            entry = await self._submit_venue_order(
                paper_trade=paper_trade,
                preview_hash=preview_hash,
                confirmation_entry_id=confirmation_entry_id,
                adapter_name=adapter_name,
                leg=attempt_leg,
                executed_at=executed_at,
            )
            current_leg = entry.legs[0]
            observed_state = None
            if current_leg.status == "submitted":
                observed_state = await self._observe_submitted_leg(
                    observer=observer,
                    leg=current_leg,
                )
            attempt_history.append(
                _build_attempt_history_entry(
                    entry=entry,
                    attempt_index=attempt_index,
                    observed_state=observed_state,
                )
            )
            entry = _entry_with_attempt_history(
                entry=entry,
                attempt_history=attempt_history,
                observed_state=observed_state,
            )
            if current_leg.status != "submitted":
                return entry
            if observed_state is None:
                return entry
            if observed_state.derived_state == "unfilled":
                if attempt_index < self.adaptive_retry_attempts:
                    continue
                return entry
            return entry
        raise AssertionError("adaptive retry loop exited unexpectedly")

    async def _build_attempt_leg(
        self,
        *,
        base_leg: VenueOrderPreview,
        attempt_index: int,
    ) -> VenueOrderPreview:
        if attempt_index == 1:
            return base_leg

        snapshot = await self.fetch_snapshot("paradex", base_leg.symbol)
        return _reprice_leg_within_confirmed_cap(
            leg=base_leg,
            snapshot=snapshot,
            attempt_index=attempt_index,
            book_slippage_bps=self.adaptive_retry_book_slippage_bps,
        )

    def _build_order_state_observer(self) -> ParadexExecutionOrderObserver:
        return ParadexOrderStateObserver(
            account_address=self.account_address,
            private_key=self.private_key,
            token_provider=self.token_provider,
            base_url=self.base_url,
        )

    async def _observe_submitted_leg(
        self,
        *,
        observer: ParadexExecutionOrderObserver,
        leg: ExecutionLegResult,
    ) -> ExecutionLegOrderState | None:
        request_payload = leg.request_payload if isinstance(leg.request_payload, dict) else {}
        leg_payload = {
            "venue": leg.venue,
            "external_reference": leg.external_reference,
            "request_payload": request_payload,
        }
        last_state: ExecutionLegOrderState | None = None
        for poll_index in range(self.adaptive_retry_poll_attempts):
            if poll_index > 0 and self.adaptive_retry_poll_interval_seconds > 0:
                await asyncio.sleep(self.adaptive_retry_poll_interval_seconds)
            last_state = await observer.observe(leg_payload)
            if last_state.derived_state != "open":
                return last_state
        return last_state

    @staticmethod
    def _select_paradex_leg(confirmation: PreviewConfirmationEntry) -> VenueOrderPreview:
        paradex_legs = [leg for leg in confirmation.preview.legs if leg.venue == "paradex"]
        if len(paradex_legs) != 1:
            raise ValueError(
                "Expected exactly one Paradex leg in the confirmed preview before live submission"
            )
        return paradex_legs[0]

    @staticmethod
    def _select_paradex_cleanup_leg(
        confirmation: CleanupPreviewConfirmationEntry,
    ) -> VenueOrderPreview:
        leg = confirmation.preview.leg
        if leg.venue != "paradex":
            raise ValueError("Cleanup confirmation must target Paradex venue")
        if leg.reduce_only is not True:
            raise ValueError("Cleanup confirmation must be reduce-only before live execution")
        return leg


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


def _paradex_auth_usage_for_fee_profile(fee_profile: str) -> str | None:
    normalized = fee_profile.strip().lower()
    if normalized == "retail":
        return "interactive"
    return None


def _token_provider_for_auth_usage(
    *,
    default_provider: ParadexLiveTokenProvider,
    auth_usage: str | None,
) -> ParadexLiveTokenProvider:
    if auth_usage == "interactive":
        return ParadexJwtTokenProvider(token_usage="interactive")
    return default_provider


def _coerce_int(value: Any) -> int | None:
    if isinstance(value, int):
        return value
    if isinstance(value, str):
        try:
            return int(value)
        except ValueError:
            return None
    return None


def _reprice_leg_within_confirmed_cap(
    *,
    leg: VenueOrderPreview,
    snapshot: NormalizedMarketSnapshot,
    attempt_index: int,
    book_slippage_bps: int,
) -> VenueOrderPreview:
    top_of_book = snapshot.market.top_of_book
    if top_of_book is None:
        raise ValueError(f"Paradex is missing top-of-book data for {leg.symbol}")

    if leg.side == "buy":
        book_price = top_of_book.best_ask_price
        if book_price is None or book_price <= 0:
            raise ValueError(f"Paradex is missing a usable best_ask for {leg.symbol}")
        if top_of_book.best_ask_size is None or top_of_book.best_ask_size <= 0:
            _logger.warning(
                "Paradex retry repricing saw thin ask liquidity for %s: best_ask_size=%s",
                leg.symbol,
                top_of_book.best_ask_size,
            )
        candidate = Decimal(str(book_price)) * (
            Decimal("1") + (Decimal(book_slippage_bps) / Decimal(10_000))
        )
        confirmed_limit = Decimal(leg.worst_price_text)
        adjusted = min(candidate, confirmed_limit)
    else:
        book_price = top_of_book.best_bid_price
        if book_price is None or book_price <= 0:
            raise ValueError(f"Paradex is missing a usable best_bid for {leg.symbol}")
        if top_of_book.best_bid_size is None or top_of_book.best_bid_size <= 0:
            _logger.warning(
                "Paradex retry repricing saw thin bid liquidity for %s: best_bid_size=%s",
                leg.symbol,
                top_of_book.best_bid_size,
            )
        candidate = Decimal(str(book_price)) * (
            Decimal("1") - (Decimal(book_slippage_bps) / Decimal(10_000))
        )
        confirmed_limit = Decimal(leg.worst_price_text)
        adjusted = max(candidate, confirmed_limit)

    price_increment = Decimal(str(leg.price_increment)) if leg.price_increment is not None else None
    snapped = _snap_price(adjusted, price_increment, side=leg.side)
    price_text = _format_order_value(
        venue="paradex",
        value=snapped,
        increment=price_increment,
    )
    payload = dict(leg.payload)
    client_id = payload.get("client_id")
    if isinstance(client_id, str) and client_id:
        payload["client_id"] = f"{client_id}-r{attempt_index}"
    payload["price"] = price_text
    notes = [
        *leg.notes,
        (
            "Retry repriced against the current Paradex top-of-book without exceeding "
            "the confirmed worst acceptable price."
        ),
    ]
    return leg.model_copy(
        update={
            "worst_acceptable_price": float(snapped),
            "worst_price_text": price_text,
            "payload": payload,
            "notes": notes,
        }
    )


def _build_attempt_history_entry(
    *,
    entry: ExecutionJournalEntry,
    attempt_index: int,
    observed_state: ExecutionLegOrderState | None,
) -> dict[str, Any]:
    leg = entry.legs[0]
    return {
        "attempt_index": attempt_index,
        "status": leg.status,
        "external_reference": leg.external_reference,
        "request_payload": leg.request_payload,
        "response_payload": leg.response_payload,
        "observed_order_state": (
            observed_state.model_dump(mode="json") if observed_state is not None else None
        ),
    }


def _entry_with_attempt_history(
    *,
    entry: ExecutionJournalEntry,
    attempt_history: list[dict[str, Any]],
    observed_state: ExecutionLegOrderState | None,
) -> ExecutionJournalEntry:
    leg = entry.legs[0]
    response_payload = dict(leg.response_payload) if isinstance(leg.response_payload, dict) else {}
    response_payload["attempt_history"] = attempt_history
    if observed_state is not None:
        response_payload["observed_order_state"] = observed_state.model_dump(mode="json")
    updated_leg = leg.model_copy(update={"response_payload": response_payload})
    return entry.model_copy(update={"legs": [updated_leg, *entry.legs[1:]]})
