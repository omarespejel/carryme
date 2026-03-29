"""Venue order-state observation for journaled executions."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Protocol

import httpx
from carryme_connectors import (
    ConnectorError,
    ExtendedPrivateConnector,
    ParadexJwtTokenProvider,
    ParadexPrivateConnector,
)
from carryme_models import ExecutionJournalEntry, ExecutionLegOrderState, ExecutionOrderState


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
class ExecutionOrderStateService:
    """Observe venue order-state for the latest journaled execution legs."""

    observers: dict[str, ExecutionLegOrderObserver]

    async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
        legs: list[ExecutionLegOrderState] = []
        notes: list[str] = []
        for leg in entry.legs:
            payload = leg.model_dump(mode="python")
            observer = self.observers.get(leg.venue)
            if observer is None:
                legs.append(
                    ExecutionLegOrderState(
                        venue=leg.venue,
                        supported=False,
                        external_reference=leg.external_reference,
                        client_id=_request_client_id(payload),
                        derived_state="unsupported",
                        notes=[f"No order-state observer is registered for venue {leg.venue}."],
                    )
                )
                continue
            try:
                legs.append(await observer.observe(payload))
            except (ConnectorError, httpx.HTTPError) as exc:
                notes.append(f"Order-state observation failed for {leg.venue}: {exc}")
                legs.append(
                    ExecutionLegOrderState(
                        venue=leg.venue,
                        supported=True,
                        external_reference=leg.external_reference,
                        client_id=_request_client_id(payload),
                        derived_state="unknown",
                        notes=[f"Order-state observation failed: {exc}"],
                    )
                )
        return ExecutionOrderState(
            execution_entry_id=entry.entry_id,
            paper_trade_id=entry.paper_trade_id,
            preview_hash=entry.preview_hash,
            legs=legs,
            notes=notes,
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
