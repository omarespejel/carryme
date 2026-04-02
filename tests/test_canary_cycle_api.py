from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Any, cast
from unittest.mock import AsyncMock

import pytest
from carryme_api.app import (
    app,
    get_account_preflight_service,
    get_api_settings,
    get_approved_canary_store,
    get_balance_accounting_service,
    get_canary_basket_launch_store,
    get_cleanup_live_execution_router,
    get_cleanup_preview_confirmation_store,
    get_cleanup_preview_service,
    get_execution_journal_store,
    get_execution_observation_store,
    get_execution_order_state_service,
    get_launch_ready_canary_store,
    get_opportunity_universe_service,
    get_order_preview_service,
    get_pair_close_live_execution_coordinator,
    get_pair_close_preview_confirmation_store,
    get_pair_close_preview_service,
    get_paired_live_execution_coordinator,
    get_paper_trade_store,
    get_preview_confirmation_store,
    get_route_approval_service,
    get_system_state_service,
)
from carryme_api.config import ApiSettings
from carryme_models import (
    ApprovedCanaryBasketEntry,
    ApprovedCanaryBasketPlan,
    ApprovedCanarySnapshot,
    CanaryBasketLaunchResult,
    CapacityEstimate,
    ExecutionCleanupPreview,
    ExecutionJournalEntry,
    ExecutionLegOrderState,
    ExecutionLegResult,
    ExecutionOrderState,
    ExecutionPairClosePreview,
    ExecutionPairStatus,
    FundingArbOpportunity,
    FundingPairTradeIntent,
    FundingUniverseCanaryCandidate,
    FundingUniverseOpportunity,
    FundingUniverseVenueMarket,
    LaunchReadyCanarySnapshot,
    PairClosePreviewConfirmationEntry,
    PaperTradeAccountPreflight,
    PaperTradeEntry,
    PaperTradeOrderPreview,
    PaperTradeSystemState,
    PreviewConfirmationEntry,
    RouteApprovalEntry,
    TradeLegIntent,
    VenueAccountPreflight,
    VenueOrderPreview,
    VenueSystemState,
)
from carryme_runtime import (
    BalanceAccountingService,
    RouteApprovalService,
    build_pair_spec_from_universe_opportunity,
)
from carryme_storage import (
    ApprovedCanaryStore,
    BalanceSnapshotStore,
    CanaryBasketLaunchStore,
    CleanupPreviewConfirmationStore,
    ExecutionJournalStore,
    ExecutionObservationStore,
    LaunchReadyCanaryStore,
    PairClosePreviewConfirmationStore,
    PaperTradeStore,
    PreviewConfirmationStore,
    RouteApprovalStore,
)
from fastapi.testclient import TestClient


def _canary_candidate() -> FundingUniverseCanaryCandidate:
    return FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(
                    venue="extended",
                    symbol="ARB-USD",
                ),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=11.0,
    )


def _route_approval(base_time: datetime | None = None) -> RouteApprovalEntry:
    captured_at = base_time or datetime.now(UTC)
    return RouteApprovalEntry(
        updated_at=captured_at,
        label="arb_extended_paradex",
        canonical_symbol="ARB-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=11.0,
        note="approved canary",
    )


def _launch_ready_snapshot(base_time: datetime | None = None) -> LaunchReadyCanarySnapshot:
    captured_at = base_time or datetime.now(UTC)
    return LaunchReadyCanarySnapshot(
        captured_at=captured_at,
        label="arb_extended_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=ApprovedCanarySnapshot(
            snapshot_id=5,
            captured_at=captured_at - timedelta(minutes=1),
            label="arb_extended_paradex",
            candidate=_canary_candidate(),
            approval=_route_approval(captured_at),
        ),
        system_state=_system_state(
            healthy_extended=True,
            healthy_paradex=True,
        ),
    )


def _account_preflight(
    *,
    paper_trade: PaperTradeEntry,
    extended_total: float,
    paradex_total: float,
    hedged: bool,
) -> PaperTradeAccountPreflight:
    return PaperTradeAccountPreflight(
        paper_trade_id=paper_trade.entry_id or 0,
        label=paper_trade.intent.label,
        ready=True,
        blocking_reasons=[],
        venues=[
            VenueAccountPreflight(
                venue="extended",
                enabled=True,
                authenticated=True,
                ready=True,
                credential_mode="test",
                account_identifier="extended:test",
                total_collateral=extended_total,
                available_to_trade=extended_total,
                free_collateral=extended_total,
                balance_assets=["USD"],
                position_symbols=["ARB-USD"] if hedged else [],
                notes=[],
                blocking_reasons=[],
            ),
            VenueAccountPreflight(
                venue="paradex",
                enabled=True,
                authenticated=True,
                ready=True,
                credential_mode="test",
                account_identifier="paradex:test",
                total_collateral=paradex_total,
                available_to_trade=paradex_total,
                free_collateral=paradex_total,
                balance_assets=["USDC"],
                position_symbols=["ARB-USD-PERP"] if hedged else [],
                notes=[],
                blocking_reasons=[],
            ),
        ],
    )


def _system_state(
    *,
    healthy_extended: bool,
    healthy_paradex: bool,
) -> PaperTradeSystemState:
    blocking_reasons: list[str] = []
    venues = [
        VenueSystemState(
            venue="extended",
            enabled=True,
            checked=False,
            healthy=healthy_extended,
            status=None,
            blocking_reasons=[] if healthy_extended else ["Extended system state is degraded"],
            notes=[],
        ),
        VenueSystemState(
            venue="paradex",
            enabled=True,
            checked=True,
            healthy=healthy_paradex,
            status="ok" if healthy_paradex else "maintenance",
            blocking_reasons=[] if healthy_paradex else ["Paradex system state is maintenance"],
            notes=[],
        ),
    ]
    for venue in venues:
        blocking_reasons.extend(venue.blocking_reasons)
    return PaperTradeSystemState(
        paper_trade_id=0,
        label="arb_extended_paradex",
        ready=not blocking_reasons,
        venues=venues,
        blocking_reasons=blocking_reasons,
    )


class _StubUniverseService:
    async def scan_canary_candidates(self, **_: object) -> list[FundingUniverseCanaryCandidate]:
        return [_canary_candidate()]


class _StubRouteApprovalService:
    def __init__(self, approvals: list[RouteApprovalEntry] | None = None) -> None:
        self._approvals = list(
            approvals
            or [
                _route_approval(
                    datetime(2026, 3, 29, 16, 0, tzinfo=UTC),
                )
            ]
        )

    def list_recent(
        self,
        *,
        limit: int = 50,
        label: str | None = None,
        canonical_symbol: str | None = None,
        approved: bool | None = None,
    ) -> list[RouteApprovalEntry]:
        approvals = [
            approval
            for approval in self._approvals
            if (label is None or approval.label == label)
            and (canonical_symbol is None or approval.canonical_symbol == canonical_symbol)
            and (approved is None or approval.approved is approved)
        ]
        approvals.sort(key=lambda item: item.updated_at, reverse=True)
        return approvals[:limit]

    def filter_approved_canary_candidates(
        self,
        candidates: list[FundingUniverseCanaryCandidate],
    ) -> list[FundingUniverseCanaryCandidate]:
        approved_candidates: list[FundingUniverseCanaryCandidate] = []
        for candidate in candidates:
            approval = self.get_for_candidate(candidate)
            if approval is not None and approval.approved:
                approved_candidates.append(candidate)
        return approved_candidates

    def get_for_candidate(
        self,
        candidate: FundingUniverseCanaryCandidate,
    ) -> RouteApprovalEntry | None:
        opportunity = candidate.opportunity.opportunity
        label = build_pair_spec_from_universe_opportunity(candidate.opportunity).label
        for approval in self.list_recent(
            limit=len(self._approvals) or 1,
            label=label,
            canonical_symbol=opportunity.canonical_symbol,
            approved=None,
        ):
            if (
                approval.short_venue == opportunity.short_venue
                and approval.long_venue == opportunity.long_venue
                and approval.short_fee_profile == opportunity.short_fee_profile
                and approval.long_fee_profile == opportunity.long_fee_profile
            ):
                return approval
        return None

    def require_live_approval(self, intent: FundingPairTradeIntent) -> RouteApprovalEntry:
        assert intent.label == "arb_extended_paradex"
        approval = self.list_recent(limit=1, label=intent.label, approved=True)
        assert approval
        return approval[0]


class _StubOrderPreviewService:
    async def preview_paper_trade(
        self,
        paper_trade: PaperTradeEntry,
        *,
        slippage_tolerance_bps: int,
    ) -> PaperTradeOrderPreview:
        return PaperTradeOrderPreview(
            paper_trade_id=paper_trade.entry_id or 0,
            label=paper_trade.intent.label,
            generated_at=datetime(2026, 3, 30, 12, 0, tzinfo=UTC),
            slippage_tolerance_bps=slippage_tolerance_bps,
            preview_hash="open-hash",
            legs=[
                VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro_fastfills",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=123.0,
                    quantity_text="123",
                    quantity_increment=1.0,
                    minimum_order_size=1.0,
                    minimum_notional=10.0,
                    reference_price=0.0894,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0895,
                    worst_price_text="0.0895",
                    price_increment=0.0001,
                    order_type="limit",
                    time_in_force="ioc",
                    endpoint_path_hint="/v1/orders",
                    required_auth_env_vars=["CARRYME_API_PARADEX_PRIVATE_KEY"],
                    auth_scheme="starknet key",
                    payload={"market": "ARB-USD-PERP"},
                ),
                VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=123.0,
                    quantity_text="123",
                    quantity_increment=1.0,
                    minimum_order_size=1.0,
                    minimum_notional=10.0,
                    reference_price=0.0894,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0893,
                    worst_price_text="0.0893",
                    price_increment=0.0001,
                    order_type="limit",
                    time_in_force="ioc",
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=["CARRYME_API_EXTENDED_STARK_PRIVATE_KEY"],
                    auth_scheme="api key + stark key",
                    payload={"symbol": "ARB-USD"},
                ),
            ],
        )


class _StubPairedLiveExecutionCoordinator:
    async def submit_confirmed_preview(
        self,
        *,
        paper_trade: PaperTradeEntry,
        confirmation: PreviewConfirmationEntry,
        first_venue: str = "auto",
        executed_at: datetime | None = None,
    ) -> ExecutionJournalEntry:
        assert first_venue == "auto"
        return ExecutionJournalEntry(
            executed_at=executed_at or datetime(2026, 3, 30, 12, 1, tzinfo=UTC),
            adapter="paired_live:paradex_then_extended",
            mode="live",
            status="submitted",
            paper_trade_id=paper_trade.entry_id,
            preview_hash=confirmation.preview_hash,
            confirmation_entry_id=confirmation.entry_id,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro_fastfills",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="paradex-open-1",
                ),
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="extended-open-1",
                ),
            ],
        )


class _StubPairClosePreviewService:
    async def preview_from_execution(
        self,
        *,
        entry: ExecutionJournalEntry,
        pair_status: ExecutionPairStatus,
    ) -> ExecutionPairClosePreview:
        assert pair_status.derived_state == "hedged"
        return ExecutionPairClosePreview(
            execution_entry_id=entry.entry_id,
            paper_trade_id=entry.paper_trade_id,
            label=entry.paper_trade.intent.label,
            generated_at=datetime(2026, 3, 30, 12, 2, tzinfo=UTC),
            slippage_tolerance_bps=20,
            preview_hash="close-hash",
            reason="close_test_cycle",
            legs=[
                VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro_fastfills",
                    side="sell",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=123.0,
                    quantity_text="123",
                    quantity_increment=1.0,
                    minimum_order_size=1.0,
                    minimum_notional=10.0,
                    reference_price=0.0894,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0893,
                    worst_price_text="0.0893",
                    price_increment=0.0001,
                    order_type="limit",
                    time_in_force="ioc",
                    reduce_only=True,
                    endpoint_path_hint="/v1/orders",
                    required_auth_env_vars=["CARRYME_API_PARADEX_PRIVATE_KEY"],
                    auth_scheme="starknet key",
                    payload={"market": "ARB-USD-PERP", "reduce_only": True},
                ),
                VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=123.0,
                    quantity_text="123",
                    quantity_increment=1.0,
                    minimum_order_size=1.0,
                    minimum_notional=10.0,
                    reference_price=0.0894,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0895,
                    worst_price_text="0.0895",
                    price_increment=0.0001,
                    order_type="limit",
                    time_in_force="ioc",
                    reduce_only=True,
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=["CARRYME_API_EXTENDED_STARK_PRIVATE_KEY"],
                    auth_scheme="api key + stark key",
                    payload={"symbol": "ARB-USD", "reduce_only": True},
                ),
            ],
            notes=[],
        )


class _StubPairCloseLiveExecutionCoordinator:
    async def submit_confirmed_preview(
        self,
        *,
        paper_trade: PaperTradeEntry,
        confirmation: PairClosePreviewConfirmationEntry,
        first_venue: str = "auto",
        executed_at: datetime | None = None,
    ) -> ExecutionJournalEntry:
        assert first_venue == "auto"
        return ExecutionJournalEntry(
            executed_at=executed_at or datetime(2026, 3, 30, 12, 3, tzinfo=UTC),
            adapter="paired_cleanup:paradex_then_extended",
            mode="live",
            status="submitted",
            paper_trade_id=paper_trade.entry_id,
            preview_hash=confirmation.preview_hash,
            confirmation_entry_id=confirmation.entry_id,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro_fastfills",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="paradex-close-1",
                    request_payload={"reduce_only": True},
                ),
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="extended-close-1",
                    request_payload={"reduce_only": True},
                ),
            ],
        )


class _UnexpectedCleanupPreviewService:
    async def preview_from_execution(self, **_: object) -> ExecutionCleanupPreview:
        raise AssertionError("cleanup preview should not run in this test")


class _UnexpectedCleanupLiveRouter:
    async def submit_confirmed_cleanup_preview(self, **_: object) -> ExecutionJournalEntry:
        raise AssertionError("cleanup live submit should not run in this test")


def _override_common_dependencies(
    *,
    paper_store: PaperTradeStore,
    preview_confirmation_store: PreviewConfirmationStore,
    pair_close_confirmation_store: PairClosePreviewConfirmationStore,
    cleanup_confirmation_store: CleanupPreviewConfirmationStore,
    execution_store: ExecutionJournalStore,
    observation_store: ExecutionObservationStore,
    snapshot_store: BalanceSnapshotStore,
) -> None:
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-private",
    )
    app.dependency_overrides[get_opportunity_universe_service] = lambda: _StubUniverseService()
    app.dependency_overrides[get_route_approval_service] = lambda: _StubRouteApprovalService()
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: preview_confirmation_store
    app.dependency_overrides[get_pair_close_preview_confirmation_store] = (
        lambda: pair_close_confirmation_store
    )
    app.dependency_overrides[get_cleanup_preview_confirmation_store] = (
        lambda: cleanup_confirmation_store
    )
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_execution_observation_store] = lambda: observation_store
    app.dependency_overrides[get_balance_accounting_service] = lambda: BalanceAccountingService(
        store=snapshot_store
    )
    app.dependency_overrides[get_order_preview_service] = lambda: _StubOrderPreviewService()
    app.dependency_overrides[get_cleanup_preview_service] = (
        lambda: _UnexpectedCleanupPreviewService()
    )
    app.dependency_overrides[get_cleanup_live_execution_router] = (
        lambda: _UnexpectedCleanupLiveRouter()
    )
    app.dependency_overrides[get_paired_live_execution_coordinator] = (
        lambda: _StubPairedLiveExecutionCoordinator()
    )


def test_exact_canary_ranking_preserves_zero_scores() -> None:
    from carryme_api import app as app_module
    from carryme_runtime import universe as universe_module

    candidate = _canary_candidate()
    opportunity = candidate.opportunity.model_copy(
        update={
            "route_adjusted_quality_score": 0.0,
            "execution_adjusted_quality_score": 0.0,
            "estimated_one_day_pnl_after_round_trip": 0.0,
        }
    )
    candidate = candidate.model_copy(update={"opportunity": opportunity})

    assert app_module._rank_approved_canary_candidate(candidate) == (0.0, 0.0, 0.0)
    assert universe_module._ranking_value(opportunity, "roundtrip_pnl") == 0.0
    assert universe_module._ranking_value(opportunity, "execution_adjusted_quality_pnl") == 0.0
    assert universe_module._ranking_value(opportunity, "route_adjusted_quality_pnl") == 0.0


def test_execute_guarded_approved_canary_basket_uses_shared_cap_plan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    paper_store = PaperTradeStore(database_path)
    preview_confirmation_store = PreviewConfirmationStore(database_path)
    pair_close_confirmation_store = PairClosePreviewConfirmationStore(database_path)
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(database_path)
    execution_store = ExecutionJournalStore(database_path)
    observation_store = ExecutionObservationStore(database_path)
    snapshot_store = BalanceSnapshotStore(database_path)
    basket_store = CanaryBasketLaunchStore(database_path)

    _override_common_dependencies(
        paper_store=paper_store,
        preview_confirmation_store=preview_confirmation_store,
        pair_close_confirmation_store=pair_close_confirmation_store,
        cleanup_confirmation_store=cleanup_confirmation_store,
        execution_store=execution_store,
        observation_store=observation_store,
        snapshot_store=snapshot_store,
    )
    app.dependency_overrides[get_canary_basket_launch_store] = lambda: basket_store
    app.dependency_overrides[get_account_preflight_service] = lambda: object()
    app.dependency_overrides[get_system_state_service] = lambda: object()
    app.dependency_overrides[get_execution_order_state_service] = lambda: object()

    basket_plan = ApprovedCanaryBasketPlan(
        venues=["extended", "paradex"],
        fee_profiles={"extended": "default", "paradex": "pro_fastfills"},
        target_notional=20.0,
        allocated_notional=11.0,
        unused_notional=9.0,
        estimated_one_day_pnl_after_entry=0.02255,
        estimated_one_day_pnl_after_round_trip=0.0176,
        execution_adjusted_estimated_one_day_pnl_after_round_trip=0.0132,
        stability_adjusted_estimated_one_day_pnl_after_round_trip=0.0088,
        route_adjusted_estimated_one_day_pnl_after_round_trip=0.0066,
        entries=[
            ApprovedCanaryBasketEntry(
                label="arb_extended_paradex",
                approval=_route_approval(),
                candidate=_canary_candidate(),
                selected_notional=11.0,
                estimated_one_day_pnl_after_entry=0.02255,
                estimated_one_day_pnl_after_round_trip=0.0176,
                execution_adjusted_estimated_one_day_pnl_after_round_trip=0.0132,
                stability_adjusted_estimated_one_day_pnl_after_round_trip=0.0088,
                route_adjusted_estimated_one_day_pnl_after_round_trip=0.0066,
            )
        ],
    )
    captured: dict[str, object] = {}

    async def fake_scan_approved_canary_basket_plan(**kwargs: object) -> ApprovedCanaryBasketPlan:
        captured.update(kwargs)
        return basket_plan

    async def fake_execute_approved_canary_basket_plan(
        **kwargs: object,
    ) -> CanaryBasketLaunchResult:
        assert kwargs["basket_plan"] == basket_plan
        assert kwargs["continue_on_failure"] is False
        return CanaryBasketLaunchResult(
            launched_at=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
            status="completed",
            basket_plan=basket_plan,
            route_count=0,
            successful_route_count=0,
            failed_route_count=0,
            routes=[],
            balance_delta=None,
            notes=[],
        )

    monkeypatch.setattr(
        "carryme_api.app._scan_approved_canary_basket_plan",
        fake_scan_approved_canary_basket_plan,
    )
    monkeypatch.setattr(
        "carryme_api.app._execute_approved_canary_basket_plan",
        fake_execute_approved_canary_basket_plan,
    )

    client = TestClient(app)
    try:
        response = client.post(
            "/v1/executions/live/canary-cycle/approved-basket",
            params=[("venues", "extended"), ("venues", "paradex")],
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert captured["target_notional"] == 5000.0
    payload = response.json()
    assert payload["basket_plan"]["allocated_notional"] == 11.0
    assert payload["basket_plan"]["unused_notional"] == 9.0


def test_approved_canary_basket_launches_endpoint_lists_recent(tmp_path: Path) -> None:
    database_path = tmp_path / "history.sqlite3"
    store = CanaryBasketLaunchStore(database_path)
    candidate = _canary_candidate()
    approval = _route_approval()
    basket_plan = ApprovedCanaryBasketPlan(
        venues=["extended", "paradex"],
        fee_profiles={"extended": "default", "paradex": "pro_fastfills"},
        target_notional=11.0,
        allocated_notional=11.0,
        unused_notional=0.0,
        estimated_one_day_pnl_after_entry=0.02255,
        estimated_one_day_pnl_after_round_trip=0.0176,
        execution_adjusted_estimated_one_day_pnl_after_round_trip=0.0132,
        stability_adjusted_estimated_one_day_pnl_after_round_trip=0.0088,
        route_adjusted_estimated_one_day_pnl_after_round_trip=0.0066,
        entries=[
            ApprovedCanaryBasketEntry(
                label="arb_extended_paradex",
                approval=approval,
                candidate=candidate,
                selected_notional=11.0,
                estimated_one_day_pnl_after_entry=0.02255,
                estimated_one_day_pnl_after_round_trip=0.0176,
                execution_adjusted_estimated_one_day_pnl_after_round_trip=0.0132,
                stability_adjusted_estimated_one_day_pnl_after_round_trip=0.0088,
                route_adjusted_estimated_one_day_pnl_after_round_trip=0.0066,
            )
        ],
    )
    store.append(
        CanaryBasketLaunchResult(
            launched_at=datetime(2026, 4, 1, 14, 0, tzinfo=UTC),
            status="completed",
            basket_plan=basket_plan,
            route_count=1,
            successful_route_count=1,
            failed_route_count=0,
            routes=[],
            balance_delta=None,
            notes=[],
        )
    )
    app.dependency_overrides[get_canary_basket_launch_store] = lambda: store
    client = TestClient(app)

    try:
        response = client.get("/v1/executions/live/canary-cycle/approved-baskets")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["status"] == "completed"
    assert payload[0]["basket_plan"]["entries"][0]["label"] == "arb_extended_paradex"


def test_execute_guarded_canary_cycle_runs_open_and_close_with_balance_summary(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    paper_store = PaperTradeStore(database_path)
    preview_confirmation_store = PreviewConfirmationStore(database_path)
    pair_close_confirmation_store = PairClosePreviewConfirmationStore(database_path)
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(database_path)
    execution_store = ExecutionJournalStore(database_path)
    observation_store = ExecutionObservationStore(database_path)
    snapshot_store = BalanceSnapshotStore(database_path)

    class StubAccountPreflightService:
        def __init__(self) -> None:
            self._probe_count = 0

        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            _configs: object,
        ) -> PaperTradeAccountPreflight:
            self._probe_count += 1
            if self._probe_count in {1, 2}:
                return _account_preflight(
                    paper_trade=paper_trade,
                    extended_total=5.00,
                    paradex_total=15.00,
                    hedged=False,
                )
            if self._probe_count in {3, 4, 5}:
                return _account_preflight(
                    paper_trade=paper_trade,
                    extended_total=4.98,
                    paradex_total=14.98,
                    hedged=True,
                )
            return _account_preflight(
                paper_trade=paper_trade,
                extended_total=4.97,
                paradex_total=14.97,
                hedged=False,
            )

        async def probe_venues(self, _configs: object) -> list[VenueAccountPreflight]:
            return _account_preflight(
                paper_trade=PaperTradeEntry(
                    created_at=datetime(2026, 3, 30, 12, 0, tzinfo=UTC),
                    intent=FundingPairTradeIntent(
                        label="arb_extended_paradex",
                        canonical_symbol="ARB-USD-PERP",
                        source_recorded_at=datetime(2026, 3, 30, 11, 59, tzinfo=UTC),
                        one_day_net_edge_after_entry=0.00355,
                        break_even_days_entry=0.2,
                        capacity_limit_notional=900.0,
                        target_notional=11.0,
                        capacity_fraction=1.0,
                        max_target_notional=11.0,
                        long_leg=TradeLegIntent(
                            venue="paradex",
                            symbol="ARB-USD-PERP",
                            fee_profile="pro_fastfills",
                            side="buy",
                            target_notional=11.0,
                        ),
                        short_leg=TradeLegIntent(
                            venue="extended",
                            symbol="ARB-USD",
                            fee_profile="default",
                            side="sell",
                            target_notional=11.0,
                        ),
                    ),
                ),
                extended_total=4.98,
                paradex_total=14.98,
                hedged=True,
            ).venues

    class StubExecutionOrderStateService:
        async def observe_execution(self, execution: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=execution.paper_trade_id,
                preview_hash=execution.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue=leg.venue,
                        supported=True,
                        observation_source="rest_poll",
                        external_reference=leg.external_reference,
                        derived_state="filled",
                        order_status="filled",
                    )
                    for leg in execution.legs
                ],
                notes=[],
            )

    _override_common_dependencies(
        paper_store=paper_store,
        preview_confirmation_store=preview_confirmation_store,
        pair_close_confirmation_store=pair_close_confirmation_store,
        cleanup_confirmation_store=cleanup_confirmation_store,
        execution_store=execution_store,
        observation_store=observation_store,
        snapshot_store=snapshot_store,
    )
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_pair_close_preview_service] = (
        lambda: _StubPairClosePreviewService()
    )
    app.dependency_overrides[get_pair_close_live_execution_coordinator] = (
        lambda: _StubPairCloseLiveExecutionCoordinator()
    )

    client = TestClient(app)
    response = client.post(
        "/v1/executions/live/canary-cycle",
        params={
            "label": "arb_extended_paradex",
            "desired_notional": 11.0,
            "poll_attempts": 1,
            "poll_interval_seconds": 0,
            "auto_cleanup": "false",
            "close_position": "true",
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade"]["intent"]["target_notional"] == 11.0
    assert payload["approval"]["max_live_notional"] == 11.0
    assert payload["open_confirmation"]["preview_hash"] == "open-hash"
    assert payload["open_execution"]["pair_status"]["derived_state"] == "hedged"
    assert payload["close_confirmation"]["preview_hash"] == "close-hash"
    assert payload["close_execution"]["pair_status"]["derived_state"] == "closed"
    assert payload["final_pair_status"]["derived_state"] == "closed"
    assert len(payload["pre_open_snapshots"]) == 2
    assert len(payload["post_open_snapshots"]) == 2
    assert len(payload["post_close_snapshots"]) == 2
    assert payload["balance_delta"]["snapshot_count"] == 6
    assert payload["balance_delta"]["total_collateral_delta"] == pytest.approx(-0.06)
    assert len(execution_store.list_recent(limit=10)) == 2
    assert len(pair_close_confirmation_store.list_recent(limit=10)) == 1


def test_execute_guarded_canary_cycle_skips_unselected_live_credentials(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ETH-USD-PERP",
                long_venue="paradex",
                short_venue="hyperliquid",
                long_fee_profile="pro_fastfills",
                short_fee_profile="vip",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "hyperliquid": FundingUniverseVenueMarket(
                    venue="hyperliquid",
                    symbol="ETH",
                ),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="ETH-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=11.0,
    )
    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 3, 30, 12, 0, tzinfo=UTC),
        label="arb_hyperliquid_paradex",
        canonical_symbol="ETH-USD-PERP",
        short_venue="hyperliquid",
        long_venue="paradex",
        short_fee_profile="vip",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=11.0,
        note="approved canary",
    )

    async def fake_select_approved_canary_candidate(
        **_: object,
    ) -> tuple[FundingUniverseCanaryCandidate, RouteApprovalEntry]:
        return candidate, approval

    async def fake_run_guarded_canary_lifecycle(**kwargs: object) -> None:
        cleanup_preview_service = cast(Any, kwargs["cleanup_preview_service"])
        pair_close_preview_service = cast(Any, kwargs["pair_close_preview_service"])
        cleanup_live_router = cast(Any, kwargs["cleanup_live_router"])
        paired_service = cast(Any, kwargs["paired_service"])
        pair_close_live_service = cast(Any, kwargs["pair_close_live_service"])

        assert set(cleanup_preview_service.services) == {"hyperliquid", "paradex"}
        assert set(pair_close_preview_service.services) == {"hyperliquid", "paradex"}
        assert set(cleanup_live_router.services) == {"hyperliquid", "paradex"}
        assert set(paired_service.services) == {"hyperliquid", "paradex"}
        assert set(pair_close_live_service.services) == {"hyperliquid", "paradex"}
        raise RuntimeError("reached lifecycle")

    monkeypatch.setattr(
        "carryme_api.app._select_approved_canary_candidate",
        fake_select_approved_canary_candidate,
    )
    monkeypatch.setattr(
        "carryme_api.app._run_guarded_canary_lifecycle",
        fake_run_guarded_canary_lifecycle,
    )

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        database_path=str(database_path),
        extended_live_enabled=False,
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="0x123",
        hyperliquid_live_enabled=True,
        hyperliquid_account_address="0xdef",
        hyperliquid_api_wallet_private_key="0x456",
    )

    client = TestClient(app)
    try:
        with pytest.raises(RuntimeError, match="reached lifecycle"):
            client.post(
                "/v1/executions/live/canary-cycle",
                params={"label": "arb_hyperliquid_paradex"},
            )
    finally:
        app.dependency_overrides.clear()


def test_execute_guarded_canary_cycle_uses_approved_fee_profiles_for_label_selection(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    approval_store = RouteApprovalStore(database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 11, 1, tzinfo=UTC),
            label="s_extended_paradex",
            canonical_symbol="S-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro",
            approved=True,
            max_live_notional=25.0,
            note="lower-ranked newer route",
        )
    )
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 11, 0, tzinfo=UTC),
            label="s_extended_paradex",
            canonical_symbol="S-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=25.0,
            note="higher-ranked older route",
        )
    )
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime(2026, 4, 2, 11, 2, tzinfo=UTC),
            label="s_extended_paradex",
            canonical_symbol="S-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="vip",
            approved=False,
            max_live_notional=25.0,
            note="unapproved negative control",
        )
    )
    calls: list[dict[str, object]] = []

    class StubUniverseService:
        async def scan_canary_candidates(
            self,
            **kwargs: object,
        ) -> list[FundingUniverseCanaryCandidate]:
            calls.append(dict(kwargs))
            fee_profiles = cast(dict[str, str] | None, kwargs["fee_profile_overrides"])
            if fee_profiles == {"extended": "default", "paradex": "pro"}:
                route_adjusted_quality_score = 0.5
                long_fee_profile = "pro"
            elif fee_profiles == {"extended": "default", "paradex": "pro_fastfills"}:
                route_adjusted_quality_score = 0.9
                long_fee_profile = "pro_fastfills"
            else:
                return []
            return [
                FundingUniverseCanaryCandidate(
                    opportunity=FundingUniverseOpportunity(
                        opportunity=FundingArbOpportunity(
                            canonical_symbol="S-USD-PERP",
                            long_venue="paradex",
                            short_venue="extended",
                            long_fee_profile=long_fee_profile,
                            short_fee_profile="default",
                            gross_daily_edge=0.003,
                            entry_cost_rate=0.00045,
                            round_trip_cost_rate=0.0009,
                            one_day_net_edge_after_entry=0.00255,
                            one_day_net_edge_after_round_trip=0.0021,
                            break_even_days_entry=0.2,
                            break_even_days_round_trip=0.3,
                            capacity=CapacityEstimate(
                                short_bid_notional=1400.0,
                                long_ask_notional=900.0,
                                max_entry_notional=900.0,
                                limiting_venue="paradex",
                            ),
                        ),
                        venue_markets={
                            "extended": FundingUniverseVenueMarket(
                                venue="extended",
                                symbol="S-USD",
                            ),
                            "paradex": FundingUniverseVenueMarket(
                                venue="paradex",
                                symbol="S-USD-PERP",
                            ),
                        },
                        deployable_notional=900.0,
                        estimated_one_day_pnl_after_round_trip=1.89,
                        route_adjusted_quality_score=route_adjusted_quality_score,
                    ),
                    suggested_canary_notional=25.0,
                )
            ]

    async def fake_run_guarded_canary_lifecycle(**kwargs: object) -> object:
        candidate = cast(FundingUniverseCanaryCandidate, kwargs["candidate"])
        approval = cast(RouteApprovalEntry, kwargs["approval"])
        assert approval.label == "s_extended_paradex"
        assert approval.long_fee_profile == "pro_fastfills"
        assert candidate.opportunity.opportunity.long_fee_profile == "pro_fastfills"
        raise RuntimeError("reached lifecycle")

    monkeypatch.setattr(
        "carryme_api.app._run_guarded_canary_lifecycle",
        fake_run_guarded_canary_lifecycle,
    )

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        database_path=str(database_path),
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="0x123",
    )
    app.dependency_overrides[get_opportunity_universe_service] = lambda: StubUniverseService()
    app.dependency_overrides[get_route_approval_service] = lambda: RouteApprovalService(
        store=approval_store
    )

    client = TestClient(app)
    try:
        with pytest.raises(RuntimeError, match="reached lifecycle"):
            client.post(
                "/v1/executions/live/canary-cycle",
                params={"label": "s_extended_paradex"},
            )
    finally:
        app.dependency_overrides.clear()

    assert len(calls) == 2
    assert calls[0]["fee_profile_overrides"] == {"extended": "default", "paradex": "pro"}
    assert calls[0]["include_symbols"] == ["S-USD-PERP"]
    assert calls[0]["venues"] == ["extended", "paradex"]
    assert calls[1]["fee_profile_overrides"] == {
        "extended": "default",
        "paradex": "pro_fastfills",
    }
    assert calls[1]["include_symbols"] == ["S-USD-PERP"]
    assert calls[1]["venues"] == ["extended", "paradex"]
    assert all(
        call["fee_profile_overrides"] != {"extended": "default", "paradex": "vip"} for call in calls
    )


def test_execute_guarded_canary_cycle_rechecks_selected_exact_approval(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    approval = RouteApprovalEntry(
        updated_at=datetime(2026, 4, 2, 12, 0, tzinfo=UTC),
        label="s_extended_paradex",
        canonical_symbol="S-USD-PERP",
        short_venue="extended",
        long_venue="paradex",
        short_fee_profile="default",
        long_fee_profile="pro",
        approved=True,
        max_live_notional=25.0,
        note="stale exact approval",
    )

    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="S-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro",
                short_fee_profile="default",
                gross_daily_edge=0.003,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00255,
                one_day_net_edge_after_round_trip=0.0021,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "extended": FundingUniverseVenueMarket(
                    venue="extended",
                    symbol="S-USD",
                ),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="S-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=1.89,
            route_adjusted_quality_score=0.6,
        ),
        suggested_canary_notional=25.0,
    )

    async def fake_scan_exact_canary_candidate_for_approval(
        **_: object,
    ) -> tuple[FundingUniverseCanaryCandidate | None, int]:
        return candidate, 1

    class StubRouteApprovalService(_StubRouteApprovalService):
        def __init__(self) -> None:
            super().__init__([approval])

        def get_for_candidate(
            self,
            candidate: FundingUniverseCanaryCandidate,
        ) -> RouteApprovalEntry | None:
            _ = candidate
            return None

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        database_path=str(database_path),
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="0x123",
    )
    app.dependency_overrides[get_route_approval_service] = lambda: StubRouteApprovalService()
    monkeypatch.setattr(
        "carryme_api.app.scan_exact_canary_candidate_for_approval",
        fake_scan_exact_canary_candidate_for_approval,
    )
    monkeypatch.setattr(
        "carryme_api.app._run_guarded_canary_lifecycle",
        AsyncMock(side_effect=RuntimeError("should not be reached")),
    )

    client = TestClient(app)
    try:
        response = client.post(
            "/v1/executions/live/canary-cycle",
            params={"label": "s_extended_paradex"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 409
    assert (
        response.json()["detail"]
        == "Selected canary route is no longer approved for live execution"
    )


def test_execute_guarded_canary_cycle_prefers_fresh_approved_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    approved_store = ApprovedCanaryStore(database_path)
    approved_store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime.now(UTC),
            label="arb_extended_paradex",
            candidate=_canary_candidate(),
            approval=_route_approval(),
        )
    )
    approval_store = RouteApprovalStore(database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime.now(UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="fresh snapshot approval",
        )
    )
    paper_store = PaperTradeStore(database_path)
    preview_confirmation_store = PreviewConfirmationStore(database_path)
    pair_close_confirmation_store = PairClosePreviewConfirmationStore(database_path)
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(database_path)
    execution_store = ExecutionJournalStore(database_path)
    observation_store = ExecutionObservationStore(database_path)
    snapshot_store = BalanceSnapshotStore(database_path)

    _override_common_dependencies(
        paper_store=paper_store,
        preview_confirmation_store=preview_confirmation_store,
        pair_close_confirmation_store=pair_close_confirmation_store,
        cleanup_confirmation_store=cleanup_confirmation_store,
        execution_store=execution_store,
        observation_store=observation_store,
        snapshot_store=snapshot_store,
    )
    app.dependency_overrides[get_approved_canary_store] = lambda: approved_store
    app.dependency_overrides[get_route_approval_service] = lambda: RouteApprovalService(
        store=approval_store
    )
    to_thread_calls: list[
        tuple[Callable[..., object], tuple[object, ...], dict[str, object]]
    ] = []

    async def fail_scan_exact_canary_candidate_for_approval(
        **_: object,
    ) -> tuple[FundingUniverseCanaryCandidate | None, int]:
        raise AssertionError("exact canary scan should not run for a fresh approved snapshot")

    async def fake_run_guarded_canary_lifecycle(**kwargs: object) -> object:
        assert kwargs["approval"] is not None
        assert kwargs["candidate"] is not None
        assert "Launched from approved canary snapshot" in cast(str, kwargs["lifecycle_note"])
        raise RuntimeError("reached lifecycle")

    monkeypatch.setattr(
        "carryme_api.app.scan_exact_canary_candidate_for_approval",
        fail_scan_exact_canary_candidate_for_approval,
    )
    async def fake_to_thread(
        func: Any,
        /,
        *args: object,
        **kwargs: object,
    ) -> object:
        to_thread_calls.append((cast(Callable[..., object], func), args, dict(kwargs)))
        return func(*args, **kwargs)

    monkeypatch.setattr("carryme_api.app.asyncio.to_thread", fake_to_thread)
    monkeypatch.setattr(
        "carryme_api.app._run_guarded_canary_lifecycle",
        fake_run_guarded_canary_lifecycle,
    )

    client = TestClient(app)
    try:
        with pytest.raises(RuntimeError, match="reached lifecycle"):
            client.post(
                "/v1/executions/live/canary-cycle",
                params={"label": "arb_extended_paradex"},
            )
    finally:
        app.dependency_overrides.clear()

    assert len(to_thread_calls) == 1
    assert to_thread_calls[0][0].__name__ == "_select_latest_approved_canary_snapshot"
    assert to_thread_calls[0][2]["label"] == "arb_extended_paradex"


def test_execute_guarded_canary_cycle_falls_back_to_exact_scan_when_snapshot_is_stale(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    approved_store = ApprovedCanaryStore(database_path)
    approved_store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime(2025, 3, 30, 11, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=_canary_candidate(),
            approval=_route_approval(),
        )
    )
    approval_store = RouteApprovalStore(database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime.now(UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="stale snapshot approval",
        )
    )
    paper_store = PaperTradeStore(database_path)
    preview_confirmation_store = PreviewConfirmationStore(database_path)
    pair_close_confirmation_store = PairClosePreviewConfirmationStore(database_path)
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(database_path)
    execution_store = ExecutionJournalStore(database_path)
    observation_store = ExecutionObservationStore(database_path)
    snapshot_store = BalanceSnapshotStore(database_path)
    exact_scan_calls: list[dict[str, object]] = []

    _override_common_dependencies(
        paper_store=paper_store,
        preview_confirmation_store=preview_confirmation_store,
        pair_close_confirmation_store=pair_close_confirmation_store,
        cleanup_confirmation_store=cleanup_confirmation_store,
        execution_store=execution_store,
        observation_store=observation_store,
        snapshot_store=snapshot_store,
    )
    app.dependency_overrides[get_approved_canary_store] = lambda: approved_store
    app.dependency_overrides[get_route_approval_service] = lambda: RouteApprovalService(
        store=approval_store
    )

    async def fake_scan_exact_canary_candidate_for_approval(
        **kwargs: object,
    ) -> tuple[FundingUniverseCanaryCandidate | None, int]:
        exact_scan_calls.append(dict(kwargs))
        return _canary_candidate(), 1

    async def fake_run_guarded_canary_lifecycle(**kwargs: object) -> object:
        lifecycle_note = cast(str, kwargs["lifecycle_note"])
        assert "fell back to exact live scan" in lifecycle_note
        assert "stale" in lifecycle_note
        raise RuntimeError("reached lifecycle")

    monkeypatch.setattr(
        "carryme_api.app.scan_exact_canary_candidate_for_approval",
        fake_scan_exact_canary_candidate_for_approval,
    )
    monkeypatch.setattr(
        "carryme_api.app._run_guarded_canary_lifecycle",
        fake_run_guarded_canary_lifecycle,
    )

    client = TestClient(app)
    try:
        with pytest.raises(RuntimeError, match="reached lifecycle"):
            client.post(
                "/v1/executions/live/canary-cycle",
                params={
                    "label": "arb_extended_paradex",
                    "max_snapshot_age_seconds": 1,
                },
            )
    finally:
        app.dependency_overrides.clear()

    assert len(exact_scan_calls) == 1


def test_execute_guarded_canary_cycle_falls_back_to_exact_scan_when_snapshot_missing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    approved_store = ApprovedCanaryStore(database_path)
    approval_store = RouteApprovalStore(database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime.now(UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="missing snapshot approval",
        )
    )
    paper_store = PaperTradeStore(database_path)
    preview_confirmation_store = PreviewConfirmationStore(database_path)
    pair_close_confirmation_store = PairClosePreviewConfirmationStore(database_path)
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(database_path)
    execution_store = ExecutionJournalStore(database_path)
    observation_store = ExecutionObservationStore(database_path)
    snapshot_store = BalanceSnapshotStore(database_path)
    exact_scan_calls: list[dict[str, object]] = []

    _override_common_dependencies(
        paper_store=paper_store,
        preview_confirmation_store=preview_confirmation_store,
        pair_close_confirmation_store=pair_close_confirmation_store,
        cleanup_confirmation_store=cleanup_confirmation_store,
        execution_store=execution_store,
        observation_store=observation_store,
        snapshot_store=snapshot_store,
    )
    app.dependency_overrides[get_approved_canary_store] = lambda: approved_store
    app.dependency_overrides[get_route_approval_service] = lambda: RouteApprovalService(
        store=approval_store
    )

    async def fake_scan_exact_canary_candidate_for_approval(
        **kwargs: object,
    ) -> tuple[FundingUniverseCanaryCandidate | None, int]:
        exact_scan_calls.append(dict(kwargs))
        return _canary_candidate(), 1

    async def fake_run_guarded_canary_lifecycle(**kwargs: object) -> object:
        lifecycle_note = cast(str, kwargs["lifecycle_note"])
        assert "fell back to exact live scan" in lifecycle_note
        assert "No approved canary snapshot found" in lifecycle_note
        raise RuntimeError("reached lifecycle")

    monkeypatch.setattr(
        "carryme_api.app.scan_exact_canary_candidate_for_approval",
        fake_scan_exact_canary_candidate_for_approval,
    )
    monkeypatch.setattr(
        "carryme_api.app._run_guarded_canary_lifecycle",
        fake_run_guarded_canary_lifecycle,
    )

    client = TestClient(app)
    try:
        with pytest.raises(RuntimeError, match="reached lifecycle"):
            client.post(
                "/v1/executions/live/canary-cycle",
                params={"label": "arb_extended_paradex"},
            )
    finally:
        app.dependency_overrides.clear()

    assert len(exact_scan_calls) == 1


def test_execute_guarded_canary_cycle_rejects_blank_label(tmp_path: Path) -> None:
    database_path = tmp_path / "history.sqlite3"
    paper_store = PaperTradeStore(database_path)
    preview_confirmation_store = PreviewConfirmationStore(database_path)
    pair_close_confirmation_store = PairClosePreviewConfirmationStore(database_path)
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(database_path)
    execution_store = ExecutionJournalStore(database_path)
    observation_store = ExecutionObservationStore(database_path)
    snapshot_store = BalanceSnapshotStore(database_path)

    _override_common_dependencies(
        paper_store=paper_store,
        preview_confirmation_store=preview_confirmation_store,
        pair_close_confirmation_store=pair_close_confirmation_store,
        cleanup_confirmation_store=cleanup_confirmation_store,
        execution_store=execution_store,
        observation_store=observation_store,
        snapshot_store=snapshot_store,
    )
    app.dependency_overrides[get_approved_canary_store] = lambda: ApprovedCanaryStore(
        database_path
    )

    client = TestClient(app)
    try:
        response = client.post(
            "/v1/executions/live/canary-cycle",
            params={"label": "   "},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 400
    assert response.json()["detail"] == "label must be non-empty"


def test_execute_guarded_canary_cycle_caps_fresh_snapshot_to_request_max(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    approved_store = ApprovedCanaryStore(database_path)
    approved_store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime.now(UTC),
            label="arb_extended_paradex",
            candidate=_canary_candidate(),
            approval=_route_approval(),
        )
    )
    approval_store = RouteApprovalStore(database_path)
    approval_store.upsert(
        RouteApprovalEntry(
            updated_at=datetime.now(UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="fresh snapshot approval",
        )
    )
    paper_store = PaperTradeStore(database_path)
    preview_confirmation_store = PreviewConfirmationStore(database_path)
    pair_close_confirmation_store = PairClosePreviewConfirmationStore(database_path)
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(database_path)
    execution_store = ExecutionJournalStore(database_path)
    observation_store = ExecutionObservationStore(database_path)
    snapshot_store = BalanceSnapshotStore(database_path)

    _override_common_dependencies(
        paper_store=paper_store,
        preview_confirmation_store=preview_confirmation_store,
        pair_close_confirmation_store=pair_close_confirmation_store,
        cleanup_confirmation_store=cleanup_confirmation_store,
        execution_store=execution_store,
        observation_store=observation_store,
        snapshot_store=snapshot_store,
    )
    app.dependency_overrides[get_approved_canary_store] = lambda: approved_store
    app.dependency_overrides[get_route_approval_service] = lambda: RouteApprovalService(
        store=approval_store
    )

    async def fail_scan_exact_canary_candidate_for_approval(
        **_: object,
    ) -> tuple[FundingUniverseCanaryCandidate | None, int]:
        raise AssertionError("exact canary scan should not run for a fresh approved snapshot")

    async def fake_run_guarded_canary_lifecycle(**kwargs: object) -> object:
        candidate = cast(FundingUniverseCanaryCandidate, kwargs["candidate"])
        assert candidate.suggested_canary_notional == 5.0
        raise RuntimeError("reached lifecycle")

    monkeypatch.setattr(
        "carryme_api.app.scan_exact_canary_candidate_for_approval",
        fail_scan_exact_canary_candidate_for_approval,
    )
    monkeypatch.setattr(
        "carryme_api.app._run_guarded_canary_lifecycle",
        fake_run_guarded_canary_lifecycle,
    )

    client = TestClient(app)
    try:
        with pytest.raises(RuntimeError, match="reached lifecycle"):
            client.post(
                "/v1/executions/live/canary-cycle",
                params={
                    "label": "arb_extended_paradex",
                    "canary_max_notional": 5.0,
                },
            )
    finally:
        app.dependency_overrides.clear()


def test_execute_guarded_canary_cycle_skips_close_when_open_is_not_hedged(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    paper_store = PaperTradeStore(database_path)
    preview_confirmation_store = PreviewConfirmationStore(database_path)
    pair_close_confirmation_store = PairClosePreviewConfirmationStore(database_path)
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(database_path)
    execution_store = ExecutionJournalStore(database_path)
    observation_store = ExecutionObservationStore(database_path)
    snapshot_store = BalanceSnapshotStore(database_path)

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            _configs: object,
        ) -> PaperTradeAccountPreflight:
            return _account_preflight(
                paper_trade=paper_trade,
                extended_total=5.00,
                paradex_total=15.00,
                hedged=False,
            )

        async def probe_venues(self, _configs: object) -> list[VenueAccountPreflight]:
            raise AssertionError("close readiness should not be checked when the pair is unfilled")

    class StubExecutionOrderStateService:
        async def observe_execution(self, execution: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=execution.paper_trade_id,
                preview_hash=execution.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue=leg.venue,
                        supported=True,
                        observation_source="rest_poll",
                        external_reference=leg.external_reference,
                        derived_state="unfilled",
                        order_status="unfilled",
                    )
                    for leg in execution.legs
                ],
                notes=[],
            )

    class UnexpectedPairClosePreviewService:
        async def preview_from_execution(self, **_: object) -> ExecutionPairClosePreview:
            raise AssertionError("pair close preview should not run when the pair is unfilled")

    class UnexpectedPairCloseLiveExecutionCoordinator:
        async def submit_confirmed_preview(self, **_: object) -> ExecutionJournalEntry:
            raise AssertionError("pair close submit should not run when the pair is unfilled")

    _override_common_dependencies(
        paper_store=paper_store,
        preview_confirmation_store=preview_confirmation_store,
        pair_close_confirmation_store=pair_close_confirmation_store,
        cleanup_confirmation_store=cleanup_confirmation_store,
        execution_store=execution_store,
        observation_store=observation_store,
        snapshot_store=snapshot_store,
    )
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_pair_close_preview_service] = (
        lambda: UnexpectedPairClosePreviewService()
    )
    app.dependency_overrides[get_pair_close_live_execution_coordinator] = (
        lambda: UnexpectedPairCloseLiveExecutionCoordinator()
    )

    client = TestClient(app)
    response = client.post(
        "/v1/executions/live/canary-cycle",
        params={
            "label": "arb_extended_paradex",
            "desired_notional": 11.0,
            "poll_attempts": 1,
            "poll_interval_seconds": 0,
            "auto_cleanup": "false",
            "close_position": "true",
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["open_execution"]["pair_status"]["derived_state"] == "unfilled"
    assert payload["close_confirmation"] is None
    assert payload["close_execution"] is None
    assert payload["post_close_snapshots"] == []
    assert payload["final_pair_status"]["derived_state"] == "unfilled"
    assert any("Close step was skipped" in note for note in payload["notes"])


def test_execute_guarded_canary_cycle_from_latest_snapshot_runs_open_and_close(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    approved_store = ApprovedCanaryStore(database_path)
    approved_store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime.now(UTC),
            label="arb_extended_paradex",
            candidate=_canary_candidate(),
            approval=_route_approval(),
        )
    )
    paper_store = PaperTradeStore(database_path)
    preview_confirmation_store = PreviewConfirmationStore(database_path)
    pair_close_confirmation_store = PairClosePreviewConfirmationStore(database_path)
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(database_path)
    execution_store = ExecutionJournalStore(database_path)
    observation_store = ExecutionObservationStore(database_path)
    snapshot_store = BalanceSnapshotStore(database_path)

    class StubAccountPreflightService:
        def __init__(self) -> None:
            self._probe_count = 0

        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            _configs: object,
        ) -> PaperTradeAccountPreflight:
            self._probe_count += 1
            if self._probe_count in {1, 2}:
                return _account_preflight(
                    paper_trade=paper_trade,
                    extended_total=5.00,
                    paradex_total=15.00,
                    hedged=False,
                )
            if self._probe_count in {3, 4, 5}:
                return _account_preflight(
                    paper_trade=paper_trade,
                    extended_total=4.98,
                    paradex_total=14.98,
                    hedged=True,
                )
            return _account_preflight(
                paper_trade=paper_trade,
                extended_total=4.97,
                paradex_total=14.97,
                hedged=False,
            )

        async def probe_venues(self, _configs: object) -> list[VenueAccountPreflight]:
            return _account_preflight(
                paper_trade=PaperTradeEntry(
                    created_at=datetime(2026, 3, 30, 12, 0, tzinfo=UTC),
                    intent=FundingPairTradeIntent(
                        label="arb_extended_paradex",
                        canonical_symbol="ARB-USD-PERP",
                        source_recorded_at=datetime(2026, 3, 30, 11, 59, tzinfo=UTC),
                        one_day_net_edge_after_entry=0.00355,
                        break_even_days_entry=0.2,
                        capacity_limit_notional=900.0,
                        target_notional=11.0,
                        capacity_fraction=1.0,
                        max_target_notional=11.0,
                        long_leg=TradeLegIntent(
                            venue="paradex",
                            symbol="ARB-USD-PERP",
                            fee_profile="pro_fastfills",
                            side="buy",
                            target_notional=11.0,
                        ),
                        short_leg=TradeLegIntent(
                            venue="extended",
                            symbol="ARB-USD",
                            fee_profile="default",
                            side="sell",
                            target_notional=11.0,
                        ),
                    ),
                ),
                extended_total=4.98,
                paradex_total=14.98,
                hedged=True,
            ).venues

    class StubExecutionOrderStateService:
        async def observe_execution(self, execution: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=execution.paper_trade_id,
                preview_hash=execution.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue=leg.venue,
                        supported=True,
                        observation_source="rest_poll",
                        external_reference=leg.external_reference,
                        derived_state="filled",
                        order_status="filled",
                    )
                    for leg in execution.legs
                ],
                notes=[],
            )

    _override_common_dependencies(
        paper_store=paper_store,
        preview_confirmation_store=preview_confirmation_store,
        pair_close_confirmation_store=pair_close_confirmation_store,
        cleanup_confirmation_store=cleanup_confirmation_store,
        execution_store=execution_store,
        observation_store=observation_store,
        snapshot_store=snapshot_store,
    )
    app.dependency_overrides[get_approved_canary_store] = lambda: approved_store
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_pair_close_preview_service] = (
        lambda: _StubPairClosePreviewService()
    )
    app.dependency_overrides[get_pair_close_live_execution_coordinator] = (
        lambda: _StubPairCloseLiveExecutionCoordinator()
    )

    client = TestClient(app)
    response = client.post(
        "/v1/executions/live/canary-cycle/latest-approved",
        params={
            "label": "arb_extended_paradex",
            "desired_notional": 11.0,
            "poll_attempts": 1,
            "poll_interval_seconds": 0,
            "auto_cleanup": "false",
            "close_position": "true",
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade"]["intent"]["target_notional"] == 11.0
    assert payload["final_pair_status"]["derived_state"] == "closed"
    assert "Launched from approved canary snapshot" in payload["notes"][0]


def test_execute_guarded_canary_cycle_from_latest_snapshot_rejects_stale_snapshot(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    approved_store = ApprovedCanaryStore(database_path)
    approved_store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime(2025, 3, 30, 11, 0, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=_canary_candidate(),
            approval=_route_approval(),
        )
    )

    app.dependency_overrides[get_approved_canary_store] = lambda: approved_store
    app.dependency_overrides[get_route_approval_service] = lambda: _StubRouteApprovalService()
    client = TestClient(app)
    response = client.post(
        "/v1/executions/live/canary-cycle/latest-approved",
        params={
            "label": "arb_extended_paradex",
            "max_snapshot_age_seconds": 1,
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 409
    assert "stale" in response.json()["detail"]


def test_execute_guarded_canary_cycle_from_latest_launch_ready_snapshot(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    launch_ready_store = LaunchReadyCanaryStore(database_path)
    launch_ready_store.append(_launch_ready_snapshot())
    paper_store = PaperTradeStore(database_path)
    preview_confirmation_store = PreviewConfirmationStore(database_path)
    pair_close_confirmation_store = PairClosePreviewConfirmationStore(database_path)
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(database_path)
    execution_store = ExecutionJournalStore(database_path)
    observation_store = ExecutionObservationStore(database_path)
    snapshot_store = BalanceSnapshotStore(database_path)

    class StubAccountPreflightService:
        def __init__(self) -> None:
            self._probe_count = 0

        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: object,
        ) -> PaperTradeAccountPreflight:
            _ = configs
            self._probe_count += 1
            if self._probe_count in {1, 2}:
                return _account_preflight(
                    paper_trade=paper_trade,
                    extended_total=5.0,
                    paradex_total=15.0,
                    hedged=False,
                )
            if self._probe_count in {3, 4, 5}:
                return _account_preflight(
                    paper_trade=paper_trade,
                    extended_total=4.98,
                    paradex_total=14.98,
                    hedged=True,
                )
            return _account_preflight(
                paper_trade=paper_trade,
                extended_total=4.97,
                paradex_total=14.97,
                hedged=False,
            )

        async def probe_venues(self, _configs: object) -> list[VenueAccountPreflight]:
            return _account_preflight(
                paper_trade=PaperTradeEntry(
                    created_at=datetime(2026, 3, 30, 12, 0, tzinfo=UTC),
                    intent=FundingPairTradeIntent(
                        label="arb_extended_paradex",
                        canonical_symbol="ARB-USD-PERP",
                        source_recorded_at=datetime(2026, 3, 30, 11, 59, tzinfo=UTC),
                        one_day_net_edge_after_entry=0.00355,
                        break_even_days_entry=0.2,
                        capacity_limit_notional=900.0,
                        target_notional=11.0,
                        capacity_fraction=1.0,
                        max_target_notional=11.0,
                        long_leg=TradeLegIntent(
                            venue="paradex",
                            symbol="ARB-USD-PERP",
                            fee_profile="pro_fastfills",
                            side="buy",
                            target_notional=11.0,
                        ),
                        short_leg=TradeLegIntent(
                            venue="extended",
                            symbol="ARB-USD",
                            fee_profile="default",
                            side="sell",
                            target_notional=11.0,
                        ),
                    ),
                ),
                extended_total=4.98,
                paradex_total=14.98,
                hedged=True,
            ).venues

    class StubExecutionOrderStateService:
        async def observe_execution(
            self,
            execution: ExecutionJournalEntry,
            *,
            poll_attempts: int = 1,
            poll_interval_seconds: float = 0.0,
        ) -> ExecutionOrderState:
            _ = poll_attempts
            _ = poll_interval_seconds
            return ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=execution.paper_trade_id,
                preview_hash=execution.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue=leg.venue,
                        supported=True,
                        observation_source="rest_poll",
                        external_reference=leg.external_reference,
                        derived_state="filled",
                        order_status="filled",
                    )
                    for leg in execution.legs
                ],
                notes=[],
            )

    _override_common_dependencies(
        paper_store=paper_store,
        preview_confirmation_store=preview_confirmation_store,
        pair_close_confirmation_store=pair_close_confirmation_store,
        cleanup_confirmation_store=cleanup_confirmation_store,
        execution_store=execution_store,
        observation_store=observation_store,
        snapshot_store=snapshot_store,
    )
    app.dependency_overrides[get_launch_ready_canary_store] = lambda: launch_ready_store
    app.dependency_overrides[get_route_approval_service] = lambda: _StubRouteApprovalService()
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_pair_close_preview_service] = (
        lambda: _StubPairClosePreviewService()
    )
    app.dependency_overrides[get_pair_close_live_execution_coordinator] = (
        lambda: _StubPairCloseLiveExecutionCoordinator()
    )

    client = TestClient(app)
    try:
        response = client.post(
            "/v1/executions/live/canary-cycle/latest-launch-ready",
            params={
                "label": "arb_extended_paradex",
                "desired_notional": 11.0,
                "poll_attempts": 1,
                "poll_interval_seconds": 0,
                "auto_cleanup": "false",
                "close_position": "true",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade"]["intent"]["target_notional"] == 11.0
    assert payload["final_pair_status"]["derived_state"] == "closed"
    assert "Launched from launch-ready canary snapshot" in payload["notes"][0]


def test_execute_guarded_canary_cycle_from_latest_launch_ready_rejects_stale_snapshot(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    launch_ready_store = LaunchReadyCanaryStore(database_path)
    stale_snapshot = _launch_ready_snapshot().model_copy(
        update={
            "captured_at": datetime(2025, 3, 30, 11, 0, tzinfo=UTC),
            "max_snapshot_age_seconds": 1,
        }
    )
    launch_ready_store.append(stale_snapshot)

    app.dependency_overrides[get_launch_ready_canary_store] = lambda: launch_ready_store
    app.dependency_overrides[get_route_approval_service] = lambda: _StubRouteApprovalService()
    client = TestClient(app)
    try:
        response = client.post(
            "/v1/executions/live/canary-cycle/latest-launch-ready",
            params={
                "label": "arb_extended_paradex",
                "max_snapshot_age_seconds": 300,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 409
    assert "stale" in response.json()["detail"]


def test_execute_guarded_canary_cycle_from_latest_stable_launch_ready_snapshot(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    launch_ready_store = LaunchReadyCanaryStore(database_path)
    base_time = datetime.now(UTC) - timedelta(minutes=2)
    snapshot = _launch_ready_snapshot(base_time)
    launch_ready_store.append(snapshot)
    launch_ready_store.append(
        snapshot.model_copy(update={"captured_at": base_time + timedelta(minutes=1)})
    )
    paper_store = PaperTradeStore(database_path)
    preview_confirmation_store = PreviewConfirmationStore(database_path)
    pair_close_confirmation_store = PairClosePreviewConfirmationStore(database_path)
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(database_path)
    execution_store = ExecutionJournalStore(database_path)
    observation_store = ExecutionObservationStore(database_path)
    snapshot_store = BalanceSnapshotStore(database_path)

    class StubAccountPreflightService:
        def __init__(self) -> None:
            self._probe_count = 0

        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: object,
        ) -> PaperTradeAccountPreflight:
            _ = configs
            self._probe_count += 1
            if self._probe_count in {1, 2}:
                return _account_preflight(
                    paper_trade=paper_trade,
                    extended_total=5.0,
                    paradex_total=15.0,
                    hedged=False,
                )
            if self._probe_count in {3, 4, 5}:
                return _account_preflight(
                    paper_trade=paper_trade,
                    extended_total=4.98,
                    paradex_total=14.98,
                    hedged=True,
                )
            return _account_preflight(
                paper_trade=paper_trade,
                extended_total=4.97,
                paradex_total=14.97,
                hedged=False,
            )

        async def probe_venues(self, _configs: object) -> list[VenueAccountPreflight]:
            return _account_preflight(
                paper_trade=PaperTradeEntry(
                    created_at=datetime(2026, 3, 30, 12, 0, tzinfo=UTC),
                    intent=FundingPairTradeIntent(
                        label="arb_extended_paradex",
                        canonical_symbol="ARB-USD-PERP",
                        source_recorded_at=datetime(2026, 3, 30, 11, 59, tzinfo=UTC),
                        one_day_net_edge_after_entry=0.00355,
                        break_even_days_entry=0.2,
                        capacity_limit_notional=900.0,
                        target_notional=11.0,
                        capacity_fraction=1.0,
                        max_target_notional=11.0,
                        long_leg=TradeLegIntent(
                            venue="paradex",
                            symbol="ARB-USD-PERP",
                            fee_profile="pro_fastfills",
                            side="buy",
                            target_notional=11.0,
                        ),
                        short_leg=TradeLegIntent(
                            venue="extended",
                            symbol="ARB-USD",
                            fee_profile="default",
                            side="sell",
                            target_notional=11.0,
                        ),
                    ),
                ),
                extended_total=4.98,
                paradex_total=14.98,
                hedged=True,
            ).venues

    class StubExecutionOrderStateService:
        async def observe_execution(
            self,
            execution: ExecutionJournalEntry,
            *,
            poll_attempts: int = 1,
            poll_interval_seconds: float = 0.0,
        ) -> ExecutionOrderState:
            _ = poll_attempts
            _ = poll_interval_seconds
            return ExecutionOrderState(
                execution_entry_id=execution.entry_id,
                paper_trade_id=execution.paper_trade_id,
                preview_hash=execution.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue=leg.venue,
                        supported=True,
                        observation_source="rest_poll",
                        external_reference=leg.external_reference,
                        derived_state="filled",
                        order_status="filled",
                    )
                    for leg in execution.legs
                ],
                notes=[],
            )

    _override_common_dependencies(
        paper_store=paper_store,
        preview_confirmation_store=preview_confirmation_store,
        pair_close_confirmation_store=pair_close_confirmation_store,
        cleanup_confirmation_store=cleanup_confirmation_store,
        execution_store=execution_store,
        observation_store=observation_store,
        snapshot_store=snapshot_store,
    )
    app.dependency_overrides[get_launch_ready_canary_store] = lambda: launch_ready_store
    app.dependency_overrides[get_route_approval_service] = lambda: _StubRouteApprovalService()
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_pair_close_preview_service] = (
        lambda: _StubPairClosePreviewService()
    )
    app.dependency_overrides[get_pair_close_live_execution_coordinator] = (
        lambda: _StubPairCloseLiveExecutionCoordinator()
    )

    client = TestClient(app)
    try:
        response = client.post(
            "/v1/executions/live/canary-cycle/latest-stable-launch-ready",
            params={
                "label": "arb_extended_paradex",
                "desired_notional": 11.0,
                "min_snapshot_count": 2,
                "min_stable_seconds": 30,
                "poll_attempts": 1,
                "poll_interval_seconds": 0,
                "auto_cleanup": "false",
                "close_position": "true",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["final_pair_status"]["derived_state"] == "closed"
    assert "stable launch-ready canary snapshot" in payload["notes"][0]


def test_execute_guarded_canary_cycle_from_latest_stable_launch_ready_rejects_unstable_snapshot(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    launch_ready_store = LaunchReadyCanaryStore(database_path)
    launch_ready_store.append(_launch_ready_snapshot())

    app.dependency_overrides[get_launch_ready_canary_store] = lambda: launch_ready_store
    app.dependency_overrides[get_route_approval_service] = lambda: _StubRouteApprovalService()
    client = TestClient(app)
    try:
        response = client.post(
            "/v1/executions/live/canary-cycle/latest-stable-launch-ready",
            params={
                "label": "arb_extended_paradex",
                "min_snapshot_count": 2,
                "min_stable_seconds": 30,
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 409
    assert "not yet stable" in response.json()["detail"]


def test_execute_guarded_canary_cycle_from_latest_stable_launch_ready_skips_unselected_live_credentials(  # noqa: E501
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "history.sqlite3"
    launch_ready_store = LaunchReadyCanaryStore(database_path)
    base_time = datetime.now(UTC) - timedelta(minutes=2)

    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="ETH-USD-PERP",
                long_venue="paradex",
                short_venue="hyperliquid",
                long_fee_profile="pro_fastfills",
                short_fee_profile="vip",
                gross_daily_edge=0.004,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00355,
                one_day_net_edge_after_round_trip=0.0031,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.3,
                capacity=CapacityEstimate(
                    short_bid_notional=1400.0,
                    long_ask_notional=900.0,
                    max_entry_notional=900.0,
                    limiting_venue="paradex",
                ),
            ),
            venue_markets={
                "hyperliquid": FundingUniverseVenueMarket(
                    venue="hyperliquid",
                    symbol="ETH",
                ),
                "paradex": FundingUniverseVenueMarket(
                    venue="paradex",
                    symbol="ETH-USD-PERP",
                ),
            },
            deployable_notional=900.0,
            estimated_one_day_pnl_after_round_trip=2.79,
        ),
        suggested_canary_notional=11.0,
    )
    approval = RouteApprovalEntry(
        updated_at=base_time,
        label="arb_hyperliquid_paradex",
        canonical_symbol="ETH-USD-PERP",
        short_venue="hyperliquid",
        long_venue="paradex",
        short_fee_profile="vip",
        long_fee_profile="pro_fastfills",
        approved=True,
        max_live_notional=11.0,
        note="approved canary",
    )
    snapshot = LaunchReadyCanarySnapshot(
        captured_at=base_time,
        label="arb_hyperliquid_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=ApprovedCanarySnapshot(
            snapshot_id=5,
            captured_at=base_time - timedelta(minutes=1),
            label="arb_hyperliquid_paradex",
            candidate=candidate,
            approval=approval,
        ),
        system_state=PaperTradeSystemState(
            paper_trade_id=0,
            label="arb_hyperliquid_paradex",
            ready=True,
            venues=[
                VenueSystemState(
                    venue="hyperliquid",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                    blocking_reasons=[],
                    notes=[],
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                    blocking_reasons=[],
                    notes=[],
                ),
            ],
            blocking_reasons=[],
        ),
    )
    launch_ready_store.append(snapshot)
    launch_ready_store.append(
        snapshot.model_copy(update={"captured_at": base_time + timedelta(minutes=1)})
    )

    class StubRouteApprovalService:
        def filter_approved_canary_candidates(
            self,
            candidates: list[FundingUniverseCanaryCandidate],
        ) -> list[FundingUniverseCanaryCandidate]:
            return candidates

        def get_for_candidate(
            self,
            _candidate: FundingUniverseCanaryCandidate,
        ) -> RouteApprovalEntry:
            return approval

        def require_live_approval(self, intent: FundingPairTradeIntent) -> RouteApprovalEntry:
            assert intent.label == "arb_hyperliquid_paradex"
            return approval

    async def fake_run_guarded_canary_lifecycle(**kwargs: object) -> None:
        cleanup_preview_service = cast(Any, kwargs["cleanup_preview_service"])
        pair_close_preview_service = cast(Any, kwargs["pair_close_preview_service"])
        cleanup_live_router = cast(Any, kwargs["cleanup_live_router"])
        paired_service = cast(Any, kwargs["paired_service"])
        pair_close_live_service = cast(Any, kwargs["pair_close_live_service"])

        assert set(cleanup_preview_service.services) == {"hyperliquid", "paradex"}
        assert set(pair_close_preview_service.services) == {"hyperliquid", "paradex"}
        assert set(cleanup_live_router.services) == {"hyperliquid", "paradex"}
        assert set(paired_service.services) == {"hyperliquid", "paradex"}
        assert set(pair_close_live_service.services) == {"hyperliquid", "paradex"}
        raise RuntimeError("reached lifecycle")

    monkeypatch.setattr(
        "carryme_api.app._run_guarded_canary_lifecycle",
        fake_run_guarded_canary_lifecycle,
    )

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        database_path=str(database_path),
        extended_live_enabled=False,
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="0x123",
        hyperliquid_live_enabled=True,
        hyperliquid_account_address="0xdef",
        hyperliquid_api_wallet_private_key="0x456",
    )
    app.dependency_overrides[get_launch_ready_canary_store] = lambda: launch_ready_store
    app.dependency_overrides[get_route_approval_service] = lambda: StubRouteApprovalService()

    client = TestClient(app)
    try:
        with pytest.raises(RuntimeError, match="reached lifecycle"):
            client.post(
                "/v1/executions/live/canary-cycle/latest-stable-launch-ready",
                params={
                    "label": "arb_hyperliquid_paradex",
                    "min_snapshot_count": 2,
                    "min_stable_seconds": 30,
                },
            )
    finally:
        app.dependency_overrides.clear()
