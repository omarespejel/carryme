"""Venue order-state observation for journaled executions."""

from __future__ import annotations

import asyncio
import threading
from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

import httpx
from carryme_connectors import (
    ConnectorError,
    ExtendedPrivateConnector,
    ParadexJwtTokenProvider,
    ParadexPrivateConnector,
    build_hyperliquid_info,
    build_hyperliquid_websocket_manager,
)
from carryme_models import (
    ExecutionJournalEntry,
    ExecutionLegOrderState,
    ExecutionOrderState,
    ObservationSource,
)


class ExecutionLegOrderObserver(Protocol):
    """Observe one venue order from a journaled execution leg."""

    async def observe(self, leg: dict[str, Any]) -> ExecutionLegOrderState: ...


DerivedOrderState = Literal["open", "filled", "partial_fill", "unfilled", "unknown", "unsupported"]


class ParadexJwtTokenIssuer(Protocol):
    """Issue a short-lived Paradex JWT for private order-state reads."""

    async def issue_jwt_token(
        self,
        *,
        account_address: str,
        private_key: str,
        client: httpx.AsyncClient | None = None,
        now: int | None = None,
    ) -> str: ...


@dataclass(frozen=True)
class ParadexOrderStateObserver:
    """Observe Paradex order state by external order id."""

    account_address: str
    private_key: str | None = None
    bearer_token: str | None = None
    token_provider: ParadexJwtTokenIssuer = field(default_factory=ParadexJwtTokenProvider)
    base_url: str = "https://api.prod.paradex.trade"

    async def observe(self, leg: dict[str, Any]) -> ExecutionLegOrderState:
        external_reference = _string_value(leg, "external_reference")
        if external_reference is None:
            return ExecutionLegOrderState(
                venue="paradex",
                supported=True,
                derived_state="unknown",
                notes=["Execution leg is missing an external Paradex order reference."],
            )

        jwt_token = self.bearer_token
        if jwt_token is None:
            if not self.private_key:
                return ExecutionLegOrderState(
                    venue="paradex",
                    supported=False,
                    external_reference=external_reference,
                    derived_state="unsupported",
                    notes=[
                        (
                            "Paradex order-state observation requires a subkey private key "
                            "or bearer token."
                        )
                    ],
                )
            async with httpx.AsyncClient(base_url=self.base_url, timeout=15.0) as auth_client:
                jwt_token = await self.token_provider.issue_jwt_token(
                    account_address=self.account_address,
                    private_key=self.private_key,
                    client=auth_client,
                )

        headers = {"Authorization": f"Bearer {jwt_token}"}
        async with httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=15.0,
        ) as client:
            connector = ParadexPrivateConnector(client)
            notes: list[str] = []
            try:
                payload = await connector.fetch_order(external_reference)
            except ConnectorError as exc:
                if exc.status_code not in {400, 404}:
                    raise
                history_match = await _lookup_paradex_order_history(
                    connector=connector,
                    leg=leg,
                    external_reference=external_reference,
                )
                if history_match is None:
                    raise
                payload = history_match
                notes.append(
                    "Paradex direct order lookup missed the order; fell back to orders-history."
                )
            except httpx.HTTPStatusError as exc:
                response = exc.response
                if response is None or response.status_code not in {400, 404}:
                    raise
                history_match = await _lookup_paradex_order_history(
                    connector=connector,
                    leg=leg,
                    external_reference=external_reference,
                )
                if history_match is None:
                    raise
                payload = history_match
                notes.append(
                    "Paradex direct order lookup missed the order; fell back to orders-history."
                )

        status = _string_value(payload, "status")
        cancel_reason = _string_value(payload, "cancel_reason")
        avg_fill_price = _string_value(payload, "avg_fill_price")
        remaining_size = _string_value(payload, "remaining_size")
        size = _string_value(payload, "size")
        derived_state = _classify_paradex_order_state(
            status=status,
            remaining_size=remaining_size,
            size=size,
            avg_fill_price=avg_fill_price,
        )
        return ExecutionLegOrderState(
            venue="paradex",
            supported=True,
            external_reference=external_reference,
            client_id=_string_value(payload, "client_id"),
            derived_state=derived_state,
            order_status=status,
            cancel_reason=cancel_reason,
            avg_fill_price=avg_fill_price,
            remaining_size=remaining_size,
            size=size,
            notes=notes,
            raw_response=payload,
        )


@dataclass(frozen=True)
class ExtendedOrderStateObserver:
    """Observe Extended open-order state by external order id."""

    api_key: str
    base_url: str = "https://api.starknet.extended.exchange"

    async def observe(self, leg: dict[str, Any]) -> ExecutionLegOrderState:
        external_reference = _string_value(leg, "external_reference")
        client_id = _request_client_id(leg)
        if external_reference is None and client_id is None:
            return ExecutionLegOrderState(
                venue="extended",
                supported=True,
                derived_state="unknown",
                notes=[
                    ("Execution leg is missing an Extended external reference and client order id.")
                ],
            )

        headers = {"x-api-key": self.api_key}
        async with httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=15.0,
        ) as client:
            payload = await ExtendedPrivateConnector(client).fetch_orders()

        orders = _unwrap_rows(payload)
        match = None
        for item in orders:
            candidates = {
                _string_value(item, "externalId"),
                _string_value(item, "clientOrderId"),
                _string_value(item, "client_order_id"),
                _string_value(item, "id"),
            }
            if external_reference in candidates or client_id in candidates:
                match = item
                break

        notes = [
            (
                "Extended private API currently exposes open orders only; absence here "
                "does not distinguish filled from closed."
            ),
        ]
        if match is None:
            return ExecutionLegOrderState(
                venue="extended",
                supported=True,
                external_reference=external_reference,
                client_id=client_id,
                derived_state="unknown",
                notes=notes,
                raw_response={"orders": orders},
            )

        return ExecutionLegOrderState(
            venue="extended",
            supported=True,
            external_reference=external_reference,
            client_id=client_id,
            derived_state="open",
            order_status=_string_value(match, "status") or "OPEN",
            raw_response=match,
            notes=notes,
        )


@dataclass(frozen=True)
class HyperliquidOrderStateObserver:
    """Observe Hyperliquid order state, preferring official websocket updates before REST."""

    account_address: str
    vault_address: str | None = None
    websocket_timeout_seconds: float = 2.0

    async def observe(self, leg: dict[str, Any]) -> ExecutionLegOrderState:
        external_reference = _string_value(leg, "external_reference")
        if external_reference is None:
            return ExecutionLegOrderState(
                venue="hyperliquid",
                supported=True,
                derived_state="unknown",
                notes=["Execution leg is missing a Hyperliquid order reference."],
            )
        oid = _coerce_int(external_reference)
        if oid is None:
            return ExecutionLegOrderState(
                venue="hyperliquid",
                supported=True,
                external_reference=external_reference,
                derived_state="unknown",
                notes=["Hyperliquid order reference is not a valid integer oid."],
            )
        observed_address = self.vault_address or self.account_address
        fallback_notes: list[str] = []
        if self.websocket_timeout_seconds > 0:
            try:
                stream_state = await asyncio.to_thread(
                    _await_hyperliquid_order_state_from_stream,
                    observed_address,
                    oid,
                    self.websocket_timeout_seconds,
                )
            except Exception as exc:
                stream_state = ExecutionLegOrderState(
                    venue="hyperliquid",
                    supported=True,
                    observation_source="websocket_error",
                    external_reference=external_reference,
                    derived_state="unknown",
                    notes=[f"Hyperliquid websocket observation failed: {exc}"],
                )
                fallback_notes.extend(stream_state.notes)
            if stream_state is not None and stream_state.derived_state != "unknown":
                return stream_state
            if stream_state is not None and stream_state.derived_state == "unknown":
                fallback_notes.extend(
                    stream_state.notes
                    or [
                        (
                            "Hyperliquid websocket produced an unknown "
                            "order-state payload; fell back to REST."
                        )
                    ]
                )
            if not fallback_notes:
                fallback_notes.append(
                    "Hyperliquid websocket did not yield a terminal update before timeout; "
                    "fell back to REST."
                )
        try:
            payload = await asyncio.to_thread(
                _fetch_hyperliquid_order_state,
                observed_address,
                oid,
            )
        except Exception as exc:
            raise ConnectorError(f"Hyperliquid order-state observation failed: {exc}") from exc

        return _build_hyperliquid_order_state(
            external_reference=external_reference,
            payload=payload,
            observation_source="rest_poll",
            notes=fallback_notes,
        )


@dataclass(frozen=True)
class ExecutionOrderStateService:
    """Observe venue order-state for the latest journaled execution legs."""

    observers: dict[str, ExecutionLegOrderObserver]

    async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
        results = await asyncio.gather(
            *(self._observe_leg(leg) for leg in entry.legs),
            return_exceptions=False,
        )
        legs = [item[0] for item in results]
        notes = [note for _, leg_notes in results for note in leg_notes]
        return ExecutionOrderState(
            execution_entry_id=entry.entry_id,
            paper_trade_id=entry.paper_trade_id,
            preview_hash=entry.preview_hash,
            legs=legs,
            notes=notes,
        )

    async def _observe_leg(
        self,
        leg: Any,
    ) -> tuple[ExecutionLegOrderState, list[str]]:
        payload = leg.model_dump(mode="python")
        observer = self.observers.get(leg.venue)
        if observer is None:
            return (
                ExecutionLegOrderState(
                    venue=leg.venue,
                    supported=False,
                    external_reference=leg.external_reference,
                    client_id=_request_client_id(payload),
                    derived_state="unsupported",
                    notes=[f"No order-state observer is registered for venue {leg.venue}."],
                ),
                [],
            )
        try:
            return await observer.observe(payload), []
        except (ConnectorError, httpx.HTTPError) as exc:
            return (
                ExecutionLegOrderState(
                    venue=leg.venue,
                    supported=True,
                    external_reference=leg.external_reference,
                    client_id=_request_client_id(payload),
                    observation_source="observer_error",
                    derived_state="unknown",
                    notes=[f"Order-state observation failed: {exc}"],
                ),
                [f"Order-state observation failed for {leg.venue}: {exc}"],
            )


def _classify_paradex_order_state(
    *,
    status: str | None,
    remaining_size: str | None,
    size: str | None,
    avg_fill_price: str | None,
) -> DerivedOrderState:
    normalized = (status or "").upper()
    if normalized in {"NEW", "OPEN"}:
        return "open"
    if normalized == "CLOSED":
        if remaining_size is not None and size is not None and remaining_size == size:
            return "unfilled"
        if avg_fill_price and remaining_size == "0":
            return "filled"
        return "partial_fill"
    return "unknown"


def _classify_hyperliquid_order_state(
    *,
    status: str | None,
    remaining_size: str | None,
    size: str | None,
    avg_fill_price: str | None,
) -> DerivedOrderState:
    normalized = (status or "").lower()
    if normalized in {"open", "triggered", "activated"}:
        if (
            remaining_size is not None
            and size is not None
            and remaining_size != size
            and remaining_size != "0"
        ):
            return "partial_fill"
        return "open"
    if normalized == "filled":
        return "filled"
    if (
        normalized.endswith("canceled")
        or normalized.endswith("cancelled")
        or normalized.endswith("rejected")
    ):
        if avg_fill_price and remaining_size not in {None, size}:
            return "partial_fill"
        return "unfilled"
    return "unknown"


def _request_client_id(payload: dict[str, Any]) -> str | None:
    request_payload = payload.get("request_payload")
    if not isinstance(request_payload, dict):
        return None
    for key in ("client_id", "client_order_id", "id"):
        value = request_payload.get(key)
        if isinstance(value, str) and value:
            return value
    return None


async def _lookup_paradex_order_history(
    *,
    connector: ParadexPrivateConnector,
    leg: dict[str, Any],
    external_reference: str,
) -> dict[str, Any] | None:
    request_payload = leg.get("request_payload")
    market = None
    if isinstance(request_payload, dict):
        market = _string_value(request_payload, "market")
    client_id = _request_client_id(leg)
    history_payload = await connector.fetch_order_history(market=market)
    for item in _unwrap_rows(history_payload):
        candidates = {
            _string_value(item, "id"),
            _string_value(item, "order_id"),
            _string_value(item, "client_id"),
        }
        if external_reference in candidates or (client_id is not None and client_id in candidates):
            return item
    return None


def _unwrap_rows(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    if isinstance(payload, dict):
        for key in ("data", "results", "result", "rows", "orders"):
            nested = payload.get(key)
            if isinstance(nested, list):
                return [item for item in nested if isinstance(item, dict)]
    return []


def _string_value(payload: dict[str, Any], key: str) -> str | None:
    value = payload.get(key)
    if isinstance(value, str) and value:
        return value
    if isinstance(value, int):
        return str(value)
    return None


def _coerce_int(value: str) -> int | None:
    try:
        return int(value)
    except ValueError:
        return None


def _await_hyperliquid_order_state_from_stream(
    account_address: str,
    oid: int,
    timeout_seconds: float,
) -> ExecutionLegOrderState | None:
    event = threading.Event()
    result_holder: dict[str, ExecutionLegOrderState | None] = {"state": None}
    user_fill_notes: list[str] = []
    lock = threading.Lock()
    subscriptions: list[tuple[dict[str, str], int]] = []

    def capture(state: ExecutionLegOrderState) -> None:
        with lock:
            if result_holder["state"] is None:
                result_holder["state"] = state
                event.set()

    def on_order_updates(message: dict[str, Any]) -> None:
        for item in _unwrap_rows(message.get("data", [])):
            match = _extract_hyperliquid_order_update(item, oid)
            if match is not None:
                capture(match)
                return

    def on_user_fills(message: dict[str, Any]) -> None:
        data = message.get("data")
        if not isinstance(data, dict):
            return
        fills = data.get("fills")
        if not isinstance(fills, list):
            return
        is_snapshot = bool(data.get("isSnapshot"))
        for fill in fills:
            if not isinstance(fill, dict):
                continue
            fill_oid = fill.get("oid")
            if not isinstance(fill_oid, int) or fill_oid != oid:
                continue
            note = (
                "Observed via Hyperliquid websocket userFills"
                + (" snapshot; " if is_snapshot else "; ")
                + "fill events may be partial, so REST fallback confirms terminal order state."
            )
            with lock:
                if note not in user_fill_notes:
                    user_fill_notes.append(note)
            return

    with build_hyperliquid_websocket_manager() as manager:
        try:
            order_subscription: dict[str, str] = {"type": "orderUpdates", "user": account_address}
            fills_subscription: dict[str, str] = {"type": "userFills", "user": account_address}
            subscriptions.append(
                (
                    order_subscription,
                    manager.subscribe(order_subscription, on_order_updates),
                )
            )
            subscriptions.append(
                (
                    fills_subscription,
                    manager.subscribe(fills_subscription, on_user_fills),
                )
            )
            if event.wait(timeout_seconds):
                return result_holder["state"]
            if user_fill_notes:
                return ExecutionLegOrderState(
                    venue="hyperliquid",
                    supported=True,
                    observation_source="websocket_user_fills",
                    external_reference=str(oid),
                    derived_state="unknown",
                    notes=user_fill_notes,
                )
            return None
        finally:
            for subscription, subscription_id in subscriptions:
                try:
                    manager.unsubscribe(subscription, subscription_id)
                except Exception:
                    continue


def _extract_hyperliquid_order_update(
    payload: dict[str, Any],
    oid: int,
) -> ExecutionLegOrderState | None:
    if _coerce_int(_string_value(payload, "oid") or "") != oid:
        return None
    order_payload, status = _hyperliquid_order_payload_and_status(payload)
    remaining_size = _string_value(order_payload, "sz") or _string_value(payload, "sz")
    size = _string_value(order_payload, "origSz") or _string_value(payload, "origSz")
    avg_fill_price = _string_value(order_payload, "avgPx") or _string_value(payload, "avgPx")
    return ExecutionLegOrderState(
        venue="hyperliquid",
        supported=True,
        observation_source="websocket_order_updates",
        external_reference=str(oid),
        client_id=_string_value(order_payload, "cloid") or _string_value(payload, "cloid"),
        derived_state=_classify_hyperliquid_order_state(
            status=status,
            remaining_size=remaining_size,
            size=size,
            avg_fill_price=avg_fill_price,
        ),
        order_status=status,
        avg_fill_price=avg_fill_price,
        remaining_size=remaining_size,
        size=size,
        notes=["Observed via Hyperliquid websocket orderUpdates."],
        raw_response=payload,
    )


def _build_hyperliquid_order_state(
    *,
    external_reference: str,
    payload: dict[str, Any],
    observation_source: ObservationSource,
    notes: list[str] | None = None,
) -> ExecutionLegOrderState:
    order_payload, status = _hyperliquid_order_payload_and_status(payload)
    remaining_size = _string_value(order_payload, "sz")
    size = _string_value(order_payload, "origSz")
    avg_fill_price = _string_value(order_payload, "avgPx")
    return ExecutionLegOrderState(
        venue="hyperliquid",
        supported=True,
        observation_source=observation_source,
        external_reference=external_reference,
        client_id=_string_value(order_payload, "cloid"),
        derived_state=_classify_hyperliquid_order_state(
            status=status,
            remaining_size=remaining_size,
            size=size,
            avg_fill_price=avg_fill_price,
        ),
        order_status=status,
        avg_fill_price=avg_fill_price,
        remaining_size=remaining_size,
        size=size,
        raw_response=payload,
        notes=notes or [],
    )


def _hyperliquid_order_payload_and_status(
    payload: dict[str, Any],
) -> tuple[dict[str, Any], str | None]:
    top_level_status = _string_value(payload, "status")
    order = payload.get("order")
    if not isinstance(order, dict):
        return {}, top_level_status

    nested_order = order.get("order")
    if isinstance(nested_order, dict):
        nested_status = _string_value(order, "status")
        return nested_order, _prefer_hyperliquid_order_status(
            top_level_status=top_level_status,
            nested_status=nested_status,
        )

    nested_status = _string_value(order, "status")
    return order, _prefer_hyperliquid_order_status(
        top_level_status=top_level_status,
        nested_status=nested_status,
    )


def _prefer_hyperliquid_order_status(
    *,
    top_level_status: str | None,
    nested_status: str | None,
) -> str | None:
    if top_level_status is None:
        return nested_status
    if top_level_status.lower() == "order" and nested_status is not None:
        return nested_status
    return top_level_status


def _fetch_hyperliquid_order_state(account_address: str, oid: int) -> dict[str, Any]:
    info = build_hyperliquid_info()
    payload = info.query_order_by_oid(account_address, oid)
    if not isinstance(payload, dict):
        raise ConnectorError("Hyperliquid order status payload must be an object")
    return payload
