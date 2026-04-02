import asyncio
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, cast

import carryme_api.app as app_module
import carryme_models as carryme_models_module
import pytest
from carryme_api.app import (
    app,
    get_api_settings,
    get_approved_canary_alert_store,
    get_approved_canary_store,
    get_balance_accounting_service,
    get_execution_accounting_service,
    get_execution_quality_service,
    get_history_store,
    get_launch_ready_canary_store,
    get_opportunity_service,
    get_opportunity_universe_service,
    get_route_approval_service,
    get_route_stability_service,
    get_stable_launch_ready_alert_store,
    get_system_state_service,
)
from carryme_api.config import ApiSettings
from carryme_models import (
    ApprovedCanaryAlertEvent,
    ApprovedCanaryBasketEntry,
    ApprovedCanaryBasketPlan,
    ApprovedCanarySnapshot,
    CandidateAlertEvent,
    CapacityEstimate,
    CleanupPreviewConfirmationEntry,
    ExecutionAccountingSummary,
    ExecutionAlertEvent,
    ExecutionCleanupPreview,
    ExecutionJournalEntry,
    ExecutionLegOrderState,
    ExecutionLegResult,
    ExecutionObservationEntry,
    ExecutionOrderState,
    ExecutionPairClosePreview,
    ExecutionPairStatus,
    ExecutionQualitySummary,
    ExecutionReconciliation,
    ExecutionVenueReconciliation,
    FundingArbOpportunity,
    FundingPairSpec,
    FundingPairTradeIntent,
    FundingUniverseCanaryCandidate,
    FundingUniverseOpportunity,
    FundingUniverseOverlap,
    FundingUniverseScan,
    FundingUniverseVenueMarket,
    LaunchReadyCanarySnapshot,
    LaunchReadyCanaryStability,
    OpportunityRecord,
    PaperTradeAccountingSummary,
    PaperTradeAccountPreflight,
    PaperTradeBalanceDelta,
    PaperTradeEntry,
    PaperTradeOrderPreview,
    PaperTradeSystemState,
    PreviewConfirmationEntry,
    RouteAccountingSummary,
    RouteApprovalEntry,
    RouteApprovalUpsert,
    RouteStabilitySummary,
    StableLaunchReadyAlertEvent,
    SystemStateAlertEvent,
    TradeLegIntent,
    VenueAccountPreflight,
    VenueOrderPreview,
    VenueSystemState,
)
from carryme_storage import (
    ApprovedCanaryAlertStore,
    ApprovedCanaryStore,
    BalanceSnapshotStore,
    CandidateAlertStore,
    CleanupPreviewConfirmationStore,
    ExecutionAlertStore,
    ExecutionJournalStore,
    ExecutionObservationStore,
    LaunchReadyCanaryStore,
    OpportunityHistoryStore,
    PairClosePreviewConfirmationStore,
    PaperTradeStore,
    PreviewConfirmationStore,
    StableLaunchReadyAlertStore,
    SystemStateAlertStore,
    WatchlistStore,
)
from fastapi.testclient import TestClient
from pydantic import SecretStr


def test_health_endpoint() -> None:
    client = TestClient(app)

    response = client.get("/health")

    assert response.status_code == 200
    assert response.json() == {
        "service": {
            "name": "carryme-api",
            "version": "0.1.0",
            "environment": "development",
        },
        "status": "ok",
    }


def test_versioned_health_endpoint() -> None:
    client = TestClient(app)

    response = client.get("/v1/health")

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_operator_auth_does_not_gate_get_requests(tmp_path: Path) -> None:
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        operator_api_key=SecretStr("operator-secret"),
    )
    client = TestClient(app)
    try:
        response = client.get("/health")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["status"] == "ok"


def test_operator_auth_rejects_missing_bearer_on_mutating_requests(tmp_path: Path) -> None:
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        operator_api_key=SecretStr("operator-secret"),
    )
    client = TestClient(app)
    try:
        response = client.put(
            "/v1/opportunities/route-approvals/s_extended_paradex",
            json={
                "canonical_symbol": "S-USD-PERP",
                "short_venue": "extended",
                "long_venue": "paradex",
                "short_fee_profile": "default",
                "long_fee_profile": "pro",
                "approved": True,
                "max_live_notional": 25.0,
                "note": "production route",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 401
    assert response.json()["detail"] == "Missing operator authorization header"
    assert response.headers["www-authenticate"] == "Bearer"


def test_operator_auth_rejects_wrong_bearer_on_mutating_requests(tmp_path: Path) -> None:
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        operator_api_key=SecretStr("operator-secret"),
    )
    client = TestClient(app)
    try:
        response = client.post(
            "/v1/executions/live/canary-cycle",
            headers={"Authorization": "Bearer wrong-secret"},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 403
    assert response.json()["detail"] == "Invalid operator authorization"


def test_operator_auth_allows_correct_bearer_on_mutating_requests(tmp_path: Path) -> None:
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        database_path=str(tmp_path / "history.sqlite3"),
        operator_api_key=SecretStr("operator-secret"),
    )
    client = TestClient(app)
    try:
        response = client.put(
            "/v1/opportunities/route-approvals/s_extended_paradex",
            headers={"Authorization": "Bearer operator-secret"},
            json={
                "canonical_symbol": "S-USD-PERP",
                "short_venue": "extended",
                "long_venue": "paradex",
                "short_fee_profile": "default",
                "long_fee_profile": "pro",
                "approved": True,
                "max_live_notional": 25.0,
                "note": "production route",
            },
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["label"] == "s_extended_paradex"
    assert payload["long_fee_profile"] == "pro"


def test_readiness_endpoint(tmp_path: Path) -> None:
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        database_path=str(tmp_path / "ready.sqlite3")
    )
    client = TestClient(app)

    response = client.get("/ready")

    app.dependency_overrides.clear()
    assert response.status_code == 200
    assert response.json() == {
        "service": {
            "name": "carryme-api",
            "version": "0.1.0",
            "environment": "development",
        },
        "status": "ready",
        "database": {
            "target": str(tmp_path / "ready.sqlite3"),
            "ready": True,
        },
    }


def test_versioned_readiness_endpoint_returns_503_when_database_ping_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from carryme_api import app as app_module

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        database_path="postgresql+psycopg://user:secret@db.example.com/carryme"
    )

    def fail_ping(self: object, timeout_seconds: float) -> None:
        assert timeout_seconds > 0
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(app_module.Database, "ping_with_timeout", fail_ping)
    client = TestClient(app)

    response = client.get("/v1/ready")

    app.dependency_overrides.clear()
    assert response.status_code == 503
    assert response.json() == {
        "service": {
            "name": "carryme-api",
            "version": "0.1.0",
            "environment": "development",
        },
        "status": "degraded",
        "database": {
            "target": "postgresql+psycopg://***@db.example.com/carryme",
            "ready": False,
        },
    }


def test_fee_profiles_endpoint() -> None:
    client = TestClient(app)

    response = client.get("/v1/reference/fees/paradex")

    assert response.status_code == 200
    payload = response.json()
    assert {item["profile"] for item in payload} == {"retail", "pro", "pro_fastfills"}


def test_paired_live_execution_coordinator_provider_includes_hyperliquid() -> None:
    from carryme_api.app import get_paired_live_execution_coordinator
    from carryme_runtime import (
        ExtendedLiveExecutionService,
        HyperliquidLiveExecutionService,
        ParadexLiveExecutionService,
    )

    class StubService:
        async def submit_confirmed_preview(self, **kwargs: object) -> None:
            return None

    extended_service = cast(ExtendedLiveExecutionService, StubService())
    hyperliquid_service = cast(HyperliquidLiveExecutionService, StubService())
    paradex_service = cast(ParadexLiveExecutionService, StubService())
    coordinator = get_paired_live_execution_coordinator(
        extended_service=extended_service,
        hyperliquid_service=hyperliquid_service,
        paradex_service=paradex_service,
    )

    assert set(coordinator.services) == {"extended", "hyperliquid", "paradex"}
    assert coordinator.services["extended"] is extended_service
    assert coordinator.services["hyperliquid"] is hyperliquid_service
    assert coordinator.services["paradex"] is paradex_service


def test_funding_pair_endpoint_uses_service_dependency() -> None:
    class StubOpportunityService:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            return FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            )

    app.dependency_overrides[get_opportunity_service] = lambda: StubOpportunityService()
    client = TestClient(app)

    response = client.get(
        "/v1/opportunities/funding-pair",
        params={
            "left_venue": "extended",
            "left_symbol": "STRK-USD",
            "left_fee_profile": "default",
            "right_venue": "hyperliquid",
            "right_symbol": "STRK",
            "right_fee_profile": "tier0",
        },
    )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json()["canonical_symbol"] == "STRK-USD-PERP"
    assert response.json()["capacity"]["limiting_venue"] == "hyperliquid"


def test_funding_pair_endpoint_maps_value_errors_to_bad_request() -> None:
    class FailingOpportunityService:
        async def score_pair(self, **_: str) -> FundingArbOpportunity:
            raise ValueError("Unsupported venue: nope")

    app.dependency_overrides[get_opportunity_service] = lambda: FailingOpportunityService()
    client = TestClient(app)

    response = client.get(
        "/v1/opportunities/funding-pair",
        params={
            "left_venue": "nope",
            "left_symbol": "STRK-USD",
            "left_fee_profile": "default",
            "right_venue": "hyperliquid",
            "right_symbol": "STRK",
            "right_fee_profile": "tier0",
        },
    )

    app.dependency_overrides.clear()

    assert response.status_code == 400
    assert response.json()["detail"] == "Unsupported venue: nope"


def test_funding_universe_endpoint_uses_service_dependency() -> None:
    class StubUniverseService:
        async def scan(self, **_: object) -> FundingUniverseScan:
            return FundingUniverseScan(
                venues=["extended", "paradex", "hyperliquid"],
                ranking="quality_adjusted_roundtrip_pnl",
                target_notional=5000.0,
                overlap_count=1,
                overlaps=[
                    FundingUniverseOverlap(
                        canonical_symbol="LIT-USD-PERP",
                        venues=["extended", "paradex"],
                        venue_symbols={
                            "extended": "LIT-USD",
                            "paradex": "LIT-USD-PERP",
                        },
                    )
                ],
                opportunities=[
                    FundingUniverseOpportunity(
                        opportunity=FundingArbOpportunity(
                            canonical_symbol="LIT-USD-PERP",
                            long_venue="paradex",
                            short_venue="extended",
                            long_fee_profile="pro",
                            short_fee_profile="default",
                            gross_daily_edge=0.0021,
                            entry_cost_rate=0.00045,
                            round_trip_cost_rate=0.0009,
                            one_day_net_edge_after_entry=0.00165,
                            one_day_net_edge_after_round_trip=0.0012,
                            break_even_days_entry=0.214,
                            break_even_days_round_trip=0.429,
                            capacity=CapacityEstimate(
                                short_bid_notional=1800.0,
                                long_ask_notional=900.0,
                                max_entry_notional=900.0,
                                limiting_venue="paradex",
                            ),
                        ),
                        venue_markets={
                            "extended": FundingUniverseVenueMarket(
                                venue="extended",
                                symbol="LIT-USD",
                                mark_price=0.83,
                                daily_funding_rate=0.000312,
                                open_interest=200_000,
                                daily_volume=120_000,
                                bid_notional=1800.0,
                                ask_notional=900.0,
                            ),
                            "paradex": FundingUniverseVenueMarket(
                                venue="paradex",
                                symbol="LIT-USD-PERP",
                                mark_price=0.831,
                                daily_funding_rate=-0.0018,
                                open_interest=150_000,
                                daily_volume=110_000,
                                bid_notional=1200.0,
                                ask_notional=900.0,
                            ),
                        },
                        min_daily_volume=110_000.0,
                        min_open_interest=150_000.0,
                        target_notional=5000.0,
                        deployable_notional=900.0,
                        estimated_one_day_pnl_after_entry=1.485,
                        estimated_one_day_pnl_after_round_trip=1.08,
                        quality_score=0.92,
                    )
                ],
            )

    app.dependency_overrides[get_opportunity_universe_service] = lambda: StubUniverseService()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/opportunities/funding-universe",
            params=[
                ("venues", "extended"),
                ("venues", "paradex"),
                ("venues", "hyperliquid"),
                ("target_notional", "5000"),
            ],
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["overlap_count"] == 1
    assert payload["opportunities"][0]["opportunity"]["canonical_symbol"] == "LIT-USD-PERP"


def test_funding_universe_endpoint_passes_policy_and_execution_filters() -> None:
    captured: dict[str, object] = {}

    class StubUniverseService:
        async def scan(self, **kwargs: object) -> FundingUniverseScan:
            captured.update(kwargs)
            ranking = kwargs["ranking"]
            target_notional = kwargs["target_notional"]
            assert isinstance(ranking, str)
            assert isinstance(target_notional, float)
            return FundingUniverseScan(
                venues=["extended", "paradex"],
                ranking=ranking,
                target_notional=target_notional,
                overlap_count=0,
                overlaps=[],
                opportunities=[],
            )

    app.dependency_overrides[get_opportunity_universe_service] = lambda: StubUniverseService()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/opportunities/funding-universe",
            params=[
                ("venues", "extended"),
                ("venues", "paradex"),
                ("include_symbols", "ARB-USD-PERP"),
                ("exclude_tags", "meme"),
                ("exclude_tags", "political"),
                ("exclude_symbols", "TRUMP-USD-PERP"),
                ("paradex_fee_profile", "retail"),
                ("min_execution_quality_score", "0.7"),
            ],
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert captured["ranking"] == "route_adjusted_quality_pnl"
    assert captured["include_symbols"] == ["ARB-USD-PERP"]
    assert captured["fee_profile_overrides"] == {"paradex": "retail"}
    assert captured["exclude_tags"] == ["meme", "political"]
    assert captured["exclude_symbols"] == ["TRUMP-USD-PERP"]
    assert captured["min_execution_quality_score"] == 0.7


def test_funding_universe_endpoint_passes_min_execution_samples() -> None:
    captured: dict[str, object] = {}

    class StubUniverseService:
        async def scan(self, **kwargs: object) -> FundingUniverseScan:
            captured.update(kwargs)
            return FundingUniverseScan(
                venues=["extended", "paradex"],
                ranking="execution_adjusted_quality_pnl",
                target_notional=5000.0,
                overlap_count=0,
                overlaps=[],
                opportunities=[],
            )

    app.dependency_overrides[get_opportunity_universe_service] = lambda: StubUniverseService()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/opportunities/funding-universe",
            params=[
                ("venues", "extended"),
                ("venues", "paradex"),
                ("min_execution_samples", "3"),
            ],
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert captured["min_execution_samples"] == 3


def test_funding_universe_endpoint_passes_route_stability_filters() -> None:
    captured: dict[str, object] = {}

    class StubUniverseService:
        async def scan(self, **kwargs: object) -> FundingUniverseScan:
            captured.update(kwargs)
            return FundingUniverseScan(
                venues=["extended", "paradex"],
                ranking=cast(str, kwargs["ranking"]),
                target_notional=cast(float, kwargs["target_notional"]),
                overlap_count=0,
                overlaps=[],
                opportunities=[],
            )

    app.dependency_overrides[get_opportunity_universe_service] = lambda: StubUniverseService()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/opportunities/funding-universe",
            params=[
                ("venues", "extended"),
                ("venues", "paradex"),
                ("min_route_stability_weight", "0.2"),
                ("min_route_presence_ratio", "0.5"),
                ("min_route_samples", "4"),
            ],
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert captured["min_route_stability_weight"] == 0.2
    assert captured["min_route_presence_ratio"] == 0.5
    assert captured["min_route_samples"] == 4


def test_funding_universe_canary_endpoint_uses_policy_defaults() -> None:
    captured: dict[str, object] = {}

    class StubUniverseService:
        async def scan_canary_candidates(
            self, **kwargs: object
        ) -> list[FundingUniverseCanaryCandidate]:
            captured.update(kwargs)
            return []

    app.dependency_overrides[get_opportunity_universe_service] = lambda: StubUniverseService()
    client = TestClient(app)

    response = client.get(
        "/v1/opportunities/funding-universe/canary",
        params=[
            ("venues", "extended"),
            ("venues", "paradex"),
        ],
    )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert captured["fee_profile_overrides"] == {"paradex": "pro_fastfills"}
    assert captured["min_execution_quality_score"] == 0.5
    assert captured["min_execution_samples"] == 0
    assert captured["min_route_stability_weight"] == 0.10
    assert captured["min_route_presence_ratio"] == 0.15
    assert captured["min_route_samples"] == 2
    assert captured["exclude_tags"] is None


def test_funding_universe_canary_endpoint_can_filter_approved_routes() -> None:
    candidate = FundingUniverseCanaryCandidate(
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
        suggested_canary_notional=25.0,
    )

    class StubUniverseService:
        async def scan_canary_candidates(self, **_: object) -> list[FundingUniverseCanaryCandidate]:
            return [candidate]

    class StubRouteApprovalService:
        def filter_approved_canary_candidates(
            self,
            candidates: list[FundingUniverseCanaryCandidate],
        ) -> list[FundingUniverseCanaryCandidate]:
            assert len(candidates) == 1
            return [candidates[0].model_copy(update={"suggested_canary_notional": 11.0})]

    app.dependency_overrides[get_opportunity_universe_service] = lambda: StubUniverseService()
    app.dependency_overrides[get_route_approval_service] = lambda: StubRouteApprovalService()
    client = TestClient(app)

    response = client.get(
        "/v1/opportunities/funding-universe/canary",
        params={"approved_only": "true"},
    )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["suggested_canary_notional"] == 11.0


def test_approved_canary_basket_endpoint_uses_service_dependency() -> None:
    candidate = FundingUniverseCanaryCandidate(
        opportunity=FundingUniverseOpportunity(
            opportunity=FundingArbOpportunity(
                canonical_symbol="S-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro_fastfills",
                short_fee_profile="default",
                gross_daily_edge=0.0025,
                entry_cost_rate=0.00045,
                round_trip_cost_rate=0.0009,
                one_day_net_edge_after_entry=0.00205,
                one_day_net_edge_after_round_trip=0.0016,
                break_even_days_entry=0.2,
                break_even_days_round_trip=0.4,
                capacity=CapacityEstimate(
                    short_bid_notional=2000.0,
                    long_ask_notional=300.0,
                    max_entry_notional=300.0,
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
            deployable_notional=300.0,
            estimated_one_day_pnl_after_entry=0.615,
            estimated_one_day_pnl_after_round_trip=0.48,
            execution_adjusted_one_day_pnl_after_round_trip=0.36,
            stability_adjusted_one_day_pnl_after_round_trip=0.24,
        ),
        suggested_canary_notional=11.0,
    )
    captured: dict[str, object] = {}

    class StubUniverseService:
        def resolve_fee_profiles(
            self,
            *,
            venues: list[str],
            fee_profile_overrides: dict[str, str] | None = None,
        ) -> dict[str, str]:
            assert venues == ["extended", "paradex"]
            assert fee_profile_overrides == {"paradex": "pro_fastfills"}
            return {"extended": "default", "paradex": "pro_fastfills"}

        async def scan_canary_candidates(
            self, **kwargs: object
        ) -> list[FundingUniverseCanaryCandidate]:
            captured.update(kwargs)
            return [candidate]

    class StubRouteApprovalService:
        def build_approved_canary_basket_plan(
            self,
            *,
            candidates: list[FundingUniverseCanaryCandidate],
            venues: list[str],
            fee_profiles: dict[str, str],
            target_notional: float,
        ) -> ApprovedCanaryBasketPlan:
            assert candidates == [candidate]
            assert target_notional == 5000.0
            return ApprovedCanaryBasketPlan(
                venues=venues,
                fee_profiles=fee_profiles,
                target_notional=5000.0,
                allocated_notional=11.0,
                unused_notional=4989.0,
                estimated_one_day_pnl_after_entry=0.02255,
                estimated_one_day_pnl_after_round_trip=0.0176,
                execution_adjusted_estimated_one_day_pnl_after_round_trip=0.0132,
                stability_adjusted_estimated_one_day_pnl_after_round_trip=0.0088,
                route_adjusted_estimated_one_day_pnl_after_round_trip=0.0066,
                entries=[
                    ApprovedCanaryBasketEntry(
                        label="s_extended_paradex",
                        approval=RouteApprovalEntry(
                            updated_at=datetime(2026, 4, 1, 12, 0, tzinfo=UTC),
                            label="s_extended_paradex",
                            canonical_symbol="S-USD-PERP",
                            short_venue="extended",
                            long_venue="paradex",
                            short_fee_profile="default",
                            long_fee_profile="pro_fastfills",
                            approved=True,
                            max_live_notional=11.0,
                            note="approved",
                        ),
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

    app.dependency_overrides[get_opportunity_universe_service] = lambda: StubUniverseService()
    app.dependency_overrides[get_route_approval_service] = lambda: StubRouteApprovalService()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/opportunities/funding-universe/canary/approved-basket",
            params=[("venues", "extended"), ("venues", "paradex")],
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    assert captured["fee_profile_overrides"] == {"paradex": "pro_fastfills"}
    assert captured["min_route_stability_weight"] == 0.10
    assert captured["min_route_presence_ratio"] == 0.15
    payload = response.json()
    assert payload["fee_profiles"]["extended"] == "default"
    assert payload["fee_profiles"]["paradex"] == "pro_fastfills"
    assert payload["target_notional"] == 5000.0
    assert payload["allocated_notional"] == 11.0
    assert payload["unused_notional"] == 4989.0
    assert payload["entries"][0]["label"] == "s_extended_paradex"
    assert payload["entries"][0]["selected_notional"] == 11.0


def test_funding_universe_portfolio_endpoint_uses_service_dependency() -> None:
    class StubUniverseService:
        async def scan(self, **_: object) -> FundingUniverseScan:
            opportunity = FundingUniverseOpportunity(
                opportunity=FundingArbOpportunity(
                    canonical_symbol="ARB-USD-PERP",
                    long_venue="paradex",
                    short_venue="extended",
                    long_fee_profile="pro",
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
                target_notional=5000.0,
                deployable_notional=900.0,
                estimated_one_day_pnl_after_entry=3.195,
                estimated_one_day_pnl_after_round_trip=2.79,
                quality_score=1.7,
            )
            return FundingUniverseScan(
                venues=["extended", "paradex", "hyperliquid"],
                ranking="quality_adjusted_roundtrip_pnl",
                target_notional=5000.0,
                overlap_count=1,
                overlaps=[],
                opportunities=[opportunity],
            )

    app.dependency_overrides[get_opportunity_universe_service] = lambda: StubUniverseService()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/opportunities/funding-universe/portfolio",
            params=[
                ("venues", "extended"),
                ("venues", "paradex"),
                ("venues", "hyperliquid"),
                ("target_notional", "5000"),
                ("max_positions", "3"),
            ],
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["allocated_notional"] == 900.0
    assert payload["entries"][0]["opportunity"]["opportunity"]["canonical_symbol"] == "ARB-USD-PERP"


def test_route_approvals_endpoint_uses_service_dependency() -> None:
    captured: dict[str, object] = {}

    class StubRouteApprovalService:
        def list_recent(self, **kwargs: object) -> list[RouteApprovalEntry]:
            captured.update(kwargs)
            return [
                RouteApprovalEntry(
                    updated_at=datetime(2026, 3, 29, 16, 0, tzinfo=UTC),
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    short_venue="extended",
                    long_venue="paradex",
                    short_fee_profile="default",
                    long_fee_profile="pro_fastfills",
                    approved=True,
                    max_live_notional=25.0,
                    note="canary approved",
                )
            ]

    app.dependency_overrides[get_route_approval_service] = lambda: StubRouteApprovalService()
    client = TestClient(app)
    response = client.get(
        "/v1/opportunities/route-approvals",
        params={"approved": "true", "limit": 5},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert captured["approved"] is True
    assert captured["limit"] == 5
    assert response.json()[0]["max_live_notional"] == 25.0


def test_upsert_route_approval_endpoint_uses_service_dependency() -> None:
    class StubRouteApprovalService:
        def upsert(self, *, label: str, payload: RouteApprovalUpsert) -> RouteApprovalEntry:
            assert label == "arb_extended_paradex"
            assert payload.max_live_notional == 15.0
            return RouteApprovalEntry(
                updated_at=datetime(2026, 3, 29, 16, 5, tzinfo=UTC),
                label=label,
                canonical_symbol=payload.canonical_symbol,
                short_venue=payload.short_venue,
                long_venue=payload.long_venue,
                short_fee_profile=payload.short_fee_profile,
                long_fee_profile=payload.long_fee_profile,
                approved=payload.approved,
                max_live_notional=payload.max_live_notional,
                note=payload.note,
            )

    app.dependency_overrides[get_route_approval_service] = lambda: StubRouteApprovalService()
    client = TestClient(app)
    response = client.put(
        "/v1/opportunities/route-approvals/arb_extended_paradex",
        json={
            "canonical_symbol": "ARB-USD-PERP",
            "short_venue": "extended",
            "long_venue": "paradex",
            "short_fee_profile": "default",
            "long_fee_profile": "pro_fastfills",
            "approved": True,
            "max_live_notional": 15.0,
            "note": "raise canary cap",
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["label"] == "arb_extended_paradex"
    assert payload["max_live_notional"] == 15.0


def test_approved_canary_snapshots_endpoint_lists_recent(tmp_path: Path) -> None:
    store = ApprovedCanaryStore(tmp_path / "history.sqlite3")
    store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime(2026, 3, 29, 16, 6, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=FundingUniverseCanaryCandidate(
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
            ),
            approval=RouteApprovalEntry(
                updated_at=datetime(2026, 3, 29, 16, 5, tzinfo=UTC),
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                short_venue="extended",
                long_venue="paradex",
                short_fee_profile="default",
                long_fee_profile="pro_fastfills",
                approved=True,
                max_live_notional=11.0,
                note="approved canary",
            ),
        )
    )

    app.dependency_overrides[get_approved_canary_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/v1/opportunities/funding-universe/canary/snapshots",
        params={"label": "arb_extended_paradex", "limit": 5},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["label"] == "arb_extended_paradex"
    assert payload[0]["candidate"]["suggested_canary_notional"] == 11.0


def test_latest_approved_canary_snapshot_endpoint_returns_latest(tmp_path: Path) -> None:
    store = ApprovedCanaryStore(tmp_path / "history.sqlite3")
    store.append(
        ApprovedCanarySnapshot(
            captured_at=datetime(2026, 3, 29, 16, 6, tzinfo=UTC),
            label="arb_extended_paradex",
            candidate=FundingUniverseCanaryCandidate(
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
            ),
            approval=RouteApprovalEntry(
                updated_at=datetime(2026, 3, 29, 16, 5, tzinfo=UTC),
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                short_venue="extended",
                long_venue="paradex",
                short_fee_profile="default",
                long_fee_profile="pro_fastfills",
                approved=True,
                max_live_notional=11.0,
                note="approved canary",
            ),
        )
    )

    app.dependency_overrides[get_approved_canary_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/v1/opportunities/funding-universe/canary/latest-approved",
        params={"label": "arb_extended_paradex"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["label"] == "arb_extended_paradex"
    assert payload["approval"]["max_live_notional"] == 11.0


def test_launch_ready_canary_snapshots_endpoint_lists_recent(tmp_path: Path) -> None:
    approved_snapshot = ApprovedCanarySnapshot(
        snapshot_id=3,
        captured_at=datetime(2026, 3, 29, 16, 6, tzinfo=UTC),
        label="arb_extended_paradex",
        candidate=FundingUniverseCanaryCandidate(
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
        ),
        approval=RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 16, 5, tzinfo=UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="approved canary",
        ),
    )
    store = LaunchReadyCanaryStore(tmp_path / "history.sqlite3")
    store.append(
        LaunchReadyCanarySnapshot(
            captured_at=datetime(2026, 3, 29, 16, 7, tzinfo=UTC),
            label="arb_extended_paradex",
            max_snapshot_age_seconds=100000000,
            approved_snapshot=approved_snapshot,
            system_state=PaperTradeSystemState(
                paper_trade_id=0,
                label="arb_extended_paradex",
                ready=True,
                venues=[
                    VenueSystemState(
                        venue="extended",
                        enabled=True,
                        checked=False,
                        healthy=True,
                        status=None,
                    ),
                    VenueSystemState(
                        venue="paradex",
                        enabled=True,
                        checked=True,
                        healthy=True,
                        status="ok",
                    ),
                ],
                blocking_reasons=[],
            ),
        )
    )

    app.dependency_overrides[get_launch_ready_canary_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/v1/executions/live/canary-cycle/launch-ready-snapshots",
        params={"label": "arb_extended_paradex", "limit": 5},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["label"] == "arb_extended_paradex"
    assert payload[0]["approved_snapshot"]["snapshot_id"] == 3


def test_latest_launch_ready_canary_snapshot_endpoint_returns_latest(tmp_path: Path) -> None:
    approved_snapshot = ApprovedCanarySnapshot(
        snapshot_id=4,
        captured_at=datetime(2026, 3, 29, 16, 6, tzinfo=UTC),
        label="arb_extended_paradex",
        candidate=FundingUniverseCanaryCandidate(
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
        ),
        approval=RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 16, 5, tzinfo=UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="approved canary",
        ),
    )
    store = LaunchReadyCanaryStore(tmp_path / "history.sqlite3")
    store.append(
        LaunchReadyCanarySnapshot(
            captured_at=datetime(2026, 3, 29, 16, 7, tzinfo=UTC),
            label="arb_extended_paradex",
            max_snapshot_age_seconds=100000000,
            approved_snapshot=approved_snapshot,
            system_state=PaperTradeSystemState(
                paper_trade_id=0,
                label="arb_extended_paradex",
                ready=True,
                venues=[
                    VenueSystemState(
                        venue="extended",
                        enabled=True,
                        checked=False,
                        healthy=True,
                        status=None,
                    ),
                    VenueSystemState(
                        venue="paradex",
                        enabled=True,
                        checked=True,
                        healthy=True,
                        status="ok",
                    ),
                ],
                blocking_reasons=[],
            ),
        )
    )

    app.dependency_overrides[get_launch_ready_canary_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/v1/executions/live/canary-cycle/latest-launch-ready",
        params={"label": "arb_extended_paradex"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["label"] == "arb_extended_paradex"
    assert payload["approved_snapshot"]["approval"]["max_live_notional"] == 11.0


def test_latest_stable_launch_ready_canary_snapshot_endpoint_returns_stability(
    tmp_path: Path,
) -> None:
    approved_snapshot = ApprovedCanarySnapshot(
        snapshot_id=4,
        captured_at=datetime(2026, 3, 29, 16, 6, tzinfo=UTC),
        label="arb_extended_paradex",
        candidate=FundingUniverseCanaryCandidate(
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
        ),
        approval=RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 16, 5, tzinfo=UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="approved canary",
        ),
    )
    store = LaunchReadyCanaryStore(tmp_path / "history.sqlite3")
    store.append(
        LaunchReadyCanarySnapshot(
            captured_at=datetime(2026, 3, 29, 16, 7, tzinfo=UTC),
            label="arb_extended_paradex",
            max_snapshot_age_seconds=100000000,
            approved_snapshot=approved_snapshot,
            system_state=PaperTradeSystemState(
                paper_trade_id=0,
                label="arb_extended_paradex",
                ready=True,
                venues=[
                    VenueSystemState(
                        venue="extended",
                        enabled=True,
                        checked=False,
                        healthy=True,
                        status=None,
                    ),
                    VenueSystemState(
                        venue="paradex",
                        enabled=True,
                        checked=True,
                        healthy=True,
                        status="ok",
                    ),
                ],
                blocking_reasons=[],
            ),
        )
    )
    store.append(
        LaunchReadyCanarySnapshot(
            captured_at=datetime(2026, 3, 29, 16, 8, tzinfo=UTC),
            label="arb_extended_paradex",
            max_snapshot_age_seconds=100000000,
            approved_snapshot=approved_snapshot,
            system_state=PaperTradeSystemState(
                paper_trade_id=0,
                label="arb_extended_paradex",
                ready=True,
                venues=[
                    VenueSystemState(
                        venue="extended",
                        enabled=True,
                        checked=False,
                        healthy=True,
                        status=None,
                    ),
                    VenueSystemState(
                        venue="paradex",
                        enabled=True,
                        checked=True,
                        healthy=True,
                        status="ok",
                    ),
                ],
                blocking_reasons=[],
            ),
        )
    )

    app.dependency_overrides[get_launch_ready_canary_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/v1/executions/live/canary-cycle/latest-stable-launch-ready",
        params={
            "label": "arb_extended_paradex",
            "max_snapshot_age_seconds": 100000000,
            "min_snapshot_count": 2,
            "min_stable_seconds": 30,
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["consecutive_snapshots"] == 2
    assert payload["stable_seconds"] == 60.0
    assert payload["snapshot"]["label"] == "arb_extended_paradex"


def test_approved_canary_alerts_endpoint_lists_recent(tmp_path: Path) -> None:
    store = ApprovedCanaryAlertStore(tmp_path / "history.sqlite3")
    current_snapshot = ApprovedCanarySnapshot(
        snapshot_id=2,
        captured_at=datetime(2026, 3, 29, 16, 6, tzinfo=UTC),
        label="arb_extended_paradex",
        candidate=FundingUniverseCanaryCandidate(
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
        ),
        approval=RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 16, 5, tzinfo=UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="approved canary",
        ),
    )
    store.append(
        ApprovedCanaryAlertEvent(
            emitted_at=datetime(2026, 3, 29, 16, 7, tzinfo=UTC),
            alert_type="approved_canary_available",
            max_snapshot_age_seconds=300,
            current_snapshot=current_snapshot,
            previous_snapshot=None,
        )
    )

    app.dependency_overrides[get_approved_canary_alert_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/v1/alerts/approved-canaries",
        params={"label": "arb_extended_paradex", "limit": 5},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["alert_type"] == "approved_canary_available"
    assert payload[0]["current_snapshot"]["label"] == "arb_extended_paradex"


def test_stable_launch_ready_alerts_endpoint_lists_recent(tmp_path: Path) -> None:
    approved_snapshot = ApprovedCanarySnapshot(
        snapshot_id=4,
        captured_at=datetime(2026, 3, 29, 16, 6, tzinfo=UTC),
        label="arb_extended_paradex",
        candidate=FundingUniverseCanaryCandidate(
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
        ),
        approval=RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 16, 5, tzinfo=UTC),
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            short_venue="extended",
            long_venue="paradex",
            short_fee_profile="default",
            long_fee_profile="pro_fastfills",
            approved=True,
            max_live_notional=11.0,
            note="approved canary",
        ),
    )
    snapshot = LaunchReadyCanarySnapshot(
        launch_ready_snapshot_id=3,
        captured_at=datetime(2026, 3, 29, 16, 7, tzinfo=UTC),
        label="arb_extended_paradex",
        max_snapshot_age_seconds=300,
        approved_snapshot=approved_snapshot,
        system_state=PaperTradeSystemState(
            paper_trade_id=0,
            label="arb_extended_paradex",
            ready=True,
            venues=[
                VenueSystemState(
                    venue="extended",
                    enabled=True,
                    checked=False,
                    healthy=True,
                    status=None,
                ),
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                ),
            ],
            blocking_reasons=[],
        ),
    )
    store = StableLaunchReadyAlertStore(tmp_path / "history.sqlite3")
    store.append(
        StableLaunchReadyAlertEvent(
            emitted_at=datetime(2026, 3, 29, 16, 8, tzinfo=UTC),
            alert_type="stable_launch_ready_available",
            max_snapshot_age_seconds=300,
            min_snapshot_count=2,
            min_stable_seconds=30.0,
            current_stability=LaunchReadyCanaryStability(
                snapshot=snapshot,
                consecutive_snapshots=2,
                stable_seconds=45.0,
                min_snapshot_count=2,
                min_stable_seconds=30.0,
            ),
            previous_stability=None,
        )
    )

    app.dependency_overrides[get_stable_launch_ready_alert_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/v1/alerts/stable-launch-ready",
        params={"label": "arb_extended_paradex", "limit": 5},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["alert_type"] == "stable_launch_ready_available"
    assert payload[0]["current_stability"]["snapshot"]["label"] == "arb_extended_paradex"


def test_system_state_alerts_endpoint_lists_recent(tmp_path: Path) -> None:
    store = SystemStateAlertStore(tmp_path / "history.sqlite3")
    store.append(
        SystemStateAlertEvent(
            emitted_at=datetime(2026, 3, 29, 16, 8, tzinfo=UTC),
            venue="paradex",
            alert_type="venue_degraded",
            current_state=VenueSystemState(
                venue="paradex",
                enabled=True,
                checked=True,
                healthy=False,
                status="maintenance",
                blocking_reasons=["Paradex system state is maintenance"],
            ),
            previous_state=VenueSystemState(
                venue="paradex",
                enabled=True,
                checked=True,
                healthy=True,
                status="ok",
            ),
        )
    )

    from carryme_api.app import get_system_state_alert_store

    app.dependency_overrides[get_system_state_alert_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/v1/alerts/system-state",
        params={"venue": "paradex", "limit": 5},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["venue"] == "paradex"
    assert payload[0]["alert_type"] == "venue_degraded"


def test_capture_balance_snapshots_for_paper_trade(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    snapshot_store = BalanceSnapshotStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 17, 0, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 16, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.001,
                break_even_days_entry=0.5,
                capacity_limit_notional=100.0,
                target_notional=11.0,
                capacity_fraction=0.25,
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
        )
    )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        total_collateral=4.9,
                        available_to_trade=4.9,
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        total_collateral=14.8,
                        available_to_trade=14.8,
                        free_collateral=14.8,
                    ),
                ],
                blocking_reasons=[],
            )

    from carryme_api.app import (
        get_account_preflight_service,
        get_balance_snapshot_store,
        get_paper_trade_store,
    )
    from carryme_runtime import BalanceAccountingService

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_balance_snapshot_store] = lambda: snapshot_store
    app.dependency_overrides[get_balance_accounting_service] = lambda: BalanceAccountingService(
        store=snapshot_store
    )
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    client = TestClient(app)
    response = client.post(
        f"/v1/accounting/balance-snapshots/from-paper-trade/{paper_trade.entry_id}",
        params={"stage": "pre_open", "note": "before canary"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 2
    assert snapshot_store.list_recent(limit=10, paper_trade_id=paper_trade.entry_id)


def test_balance_delta_endpoint_uses_service_dependency() -> None:
    class StubBalanceAccountingService:
        def summarize_paper_trade(self, paper_trade_id: int) -> PaperTradeBalanceDelta:
            assert paper_trade_id == 7
            return PaperTradeBalanceDelta(
                paper_trade_id=7,
                label="arb_extended_paradex",
                snapshot_count=4,
                venue_count=2,
                first_captured_at=datetime(2026, 3, 29, 17, 0, tzinfo=UTC),
                latest_captured_at=datetime(2026, 3, 29, 17, 30, tzinfo=UTC),
                total_collateral_delta=-0.23,
                total_available_to_trade_delta=-0.23,
                total_free_collateral_delta=-0.14,
                venues=[],
            )

    app.dependency_overrides[get_balance_accounting_service] = (
        lambda: StubBalanceAccountingService()
    )
    client = TestClient(app)
    response = client.get("/v1/accounting/balance-delta/from-paper-trade/7")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == 7
    assert payload["total_collateral_delta"] == -0.23


def test_execution_quality_endpoint_uses_service_dependency() -> None:
    captured: dict[str, object] = {}

    class StubExecutionQualityService:
        def list_summaries(self, **kwargs: object) -> list[ExecutionQualitySummary]:
            captured.update(kwargs)
            return [
                ExecutionQualitySummary(
                    canonical_symbol="ARB-USD-PERP",
                    short_venue="extended",
                    long_venue="paradex",
                    sample_size=2,
                    weighted_score=0.55,
                    latest_outcome="unfilled",
                    hedged_count=0,
                    closed_count=0,
                    unfilled_count=2,
                    cleanup_needed_count=0,
                    review_required_count=0,
                    pending_count=0,
                )
            ]

    app.dependency_overrides[get_execution_quality_service] = lambda: StubExecutionQualityService()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/opportunities/execution-quality",
            params={"min_sample_size": 2},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["canonical_symbol"] == "ARB-USD-PERP"
    assert payload[0]["weighted_score"] == 0.55
    assert captured["min_sample_size"] == 2


def test_execution_quality_service_provider_uses_shared_stores(tmp_path: Path) -> None:
    settings = ApiSettings(database_path=str(tmp_path / "quality.sqlite3"))
    service = get_execution_quality_service(settings)

    assert service.journal_store.database_path == Path(settings.database_path)
    assert service.observation_store.database_path == Path(settings.database_path)


def test_route_stability_endpoint_uses_service_dependency() -> None:
    captured: dict[str, object] = {}

    class StubRouteStabilityService:
        def list_summaries(self, **kwargs: object) -> list[RouteStabilitySummary]:
            captured.update(kwargs)
            return [
                RouteStabilitySummary(
                    canonical_symbol="ARB-USD-PERP",
                    short_venue="extended",
                    long_venue="paradex",
                    short_fee_profile="default",
                    long_fee_profile="pro",
                    sample_size=3,
                    window_count=3,
                    presence_ratio=0.75,
                    positive_roundtrip_share=1.0,
                    mean_roundtrip_edge=0.0012,
                    median_roundtrip_edge=0.0011,
                    edge_stddev=0.0001,
                    mean_capacity_notional=900.0,
                    median_capacity_notional=900.0,
                    capacity_stddev=50.0,
                    latest_roundtrip_edge=0.001,
                    latest_recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
                    stability_weight=0.6,
                    stability_score=0.00072,
                )
            ]

    app.dependency_overrides[get_route_stability_service] = lambda: StubRouteStabilityService()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/opportunities/route-stability",
            params={"min_sample_size": 2, "min_presence_ratio": 0.5},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["canonical_symbol"] == "ARB-USD-PERP"
    assert payload[0]["stability_weight"] == 0.6
    assert captured["min_sample_size"] == 2
    assert captured["min_presence_ratio"] == 0.5


def test_execution_accounting_latest_endpoint_uses_service_dependency() -> None:
    class StubAccountingService:
        def latest_for_paper_trade(self, paper_trade_id: int) -> PaperTradeAccountingSummary | None:
            assert paper_trade_id == 7
            return PaperTradeAccountingSummary(
                paper_trade_id=7,
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                execution_count=2,
                latest_executed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
                total_filled_notional=21.31,
                total_estimated_fee_paid=0.004262,
                entries=[
                    ExecutionAccountingSummary(
                        execution_entry_id=12,
                        paper_trade_id=7,
                        label="arb_extended_paradex",
                        canonical_symbol="ARB-USD-PERP",
                        adapter="paradex_cleanup_live",
                        mode="live",
                        status="submitted",
                        executed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
                        total_leg_count=1,
                        filled_leg_count=1,
                        total_filled_notional=21.31,
                        total_estimated_fee_paid=0.004262,
                        legs=[],
                        notes=[],
                    )
                ],
            )

    app.dependency_overrides[get_execution_accounting_service] = lambda: StubAccountingService()
    client = TestClient(app)

    response = client.get("/v1/executions/accounting/latest/from-paper-trade/7")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == 7
    assert payload["total_estimated_fee_paid"] == 0.004262


def test_execution_accounting_routes_endpoint_uses_service_dependency() -> None:
    class StubAccountingService:
        def list_route_summaries(
            self,
            *,
            canonical_symbol: str | None = None,
            label: str | None = None,
            limit: int = 50,
        ) -> list[RouteAccountingSummary]:
            assert canonical_symbol == "ARB-USD-PERP"
            assert label is None
            assert limit == 5
            return [
                RouteAccountingSummary(
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    short_venue="extended",
                    long_venue="paradex",
                    short_fee_profile="default",
                    long_fee_profile="pro",
                    execution_count=3,
                    paper_trade_count=1,
                    latest_executed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
                    total_filled_notional=42.0,
                    total_estimated_fee_paid=0.0084,
                )
            ]

    app.dependency_overrides[get_execution_accounting_service] = lambda: StubAccountingService()
    client = TestClient(app)

    response = client.get(
        "/v1/executions/accounting/routes",
        params=[("canonical_symbol", "ARB-USD-PERP"), ("limit", "5")],
    )

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["label"] == "arb_extended_paradex"
    assert payload[0]["total_estimated_fee_paid"] == 0.0084


def test_route_stability_service_provider_uses_shared_history_store(tmp_path: Path) -> None:
    settings = ApiSettings(database_path=str(tmp_path / "stability.sqlite3"))
    history_store = get_history_store(settings)
    service = get_route_stability_service(settings)

    assert service is get_route_stability_service(settings)
    assert service.history_store is history_store


def test_funding_universe_endpoint_rejects_invalid_route_stability_filters() -> None:
    class StubUniverseService:
        async def scan(self, **_: object) -> FundingUniverseScan:
            raise AssertionError("scan should not run for invalid route-stability filters")

    app.dependency_overrides[get_opportunity_universe_service] = lambda: StubUniverseService()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/opportunities/funding-universe",
            params={"min_route_stability_weight": 1.2},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 400
    assert response.json()["detail"] == "min_route_stability_weight must be between 0 and 1"


def test_funding_universe_portfolio_endpoint_rejects_invalid_route_stability_filters() -> None:
    class StubUniverseService:
        async def scan(self, **_: object) -> FundingUniverseScan:
            raise AssertionError("scan should not run for invalid route-stability filters")

    app.dependency_overrides[get_opportunity_universe_service] = lambda: StubUniverseService()
    try:
        client = TestClient(app)
        response = client.get(
            "/v1/opportunities/funding-universe/portfolio",
            params={"min_route_presence_ratio": -0.1},
        )
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 400
    assert response.json()["detail"] == "min_route_presence_ratio must be between 0 and 1"


def test_history_endpoint_reads_saved_records(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    store.append(
        OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
            pair=FundingPairSpec(
                label="strk_extended_hyperliquid",
                left_venue="extended",
                left_symbol="STRK-USD",
                left_fee_profile="default",
                right_venue="hyperliquid",
                right_symbol="STRK",
                right_fee_profile="tier0",
            ),
            opportunity=FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            ),
        )
    )

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)

    response = client.get("/v1/history/funding-pairs")

    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["pair"]["label"] == "strk_extended_hyperliquid"


def test_watchlist_endpoint_reads_saved_pairs(tmp_path: Path) -> None:
    store = WatchlistStore(tmp_path / "watchlist.json")
    store.replace(
        [
            FundingPairSpec(
                label="strk_extended_hyperliquid",
                left_venue="extended",
                left_symbol="STRK-USD",
                left_fee_profile="default",
                right_venue="hyperliquid",
                right_symbol="STRK",
                right_fee_profile="tier0",
            )
        ]
    )

    from carryme_api.app import get_watchlist_store

    app.dependency_overrides[get_watchlist_store] = lambda: store
    client = TestClient(app)
    response = client.get("/v1/watchlist")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert response.json() == {
        "pairs": [
            {
                "label": "strk_extended_hyperliquid",
                "left_venue": "extended",
                "left_symbol": "STRK-USD",
                "left_fee_profile": "default",
                "right_venue": "hyperliquid",
                "right_symbol": "STRK",
                "right_fee_profile": "tier0",
            }
        ]
    }


def test_watchlist_endpoint_replaces_pairs(tmp_path: Path) -> None:
    store = WatchlistStore(tmp_path / "watchlist.json")

    from carryme_api.app import get_watchlist_store

    app.dependency_overrides[get_watchlist_store] = lambda: store
    client = TestClient(app)
    response = client.put(
        "/v1/watchlist",
        json={
            "pairs": [
                {
                    "label": "arb_extended_paradex",
                    "left_venue": "extended",
                    "left_symbol": "ARB-USD",
                    "left_fee_profile": "default",
                    "right_venue": "paradex",
                    "right_symbol": "ARB-USD-PERP",
                    "right_fee_profile": "pro",
                }
            ]
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert store.load()[0].label == "arb_extended_paradex"


def test_candidate_alerts_endpoint_reads_saved_events(tmp_path: Path) -> None:
    store = CandidateAlertStore(tmp_path / "history.sqlite3")
    store.append(
        CandidateAlertEvent(
            emitted_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
            min_one_day_net_edge_after_entry=0.0,
            min_capacity_notional=2500.0,
            record=OpportunityRecord(
                recorded_at=datetime(2026, 3, 29, 12, 0, tzinfo=UTC),
                pair=FundingPairSpec(
                    label="strk_extended_hyperliquid",
                    left_venue="extended",
                    left_symbol="STRK-USD",
                    left_fee_profile="default",
                    right_venue="hyperliquid",
                    right_symbol="STRK",
                    right_fee_profile="tier0",
                ),
                opportunity=FundingArbOpportunity(
                    canonical_symbol="STRK-USD-PERP",
                    long_venue="hyperliquid",
                    short_venue="extended",
                    long_fee_profile="tier0",
                    short_fee_profile="default",
                    gross_daily_edge=0.0005,
                    entry_cost_rate=0.0003,
                    round_trip_cost_rate=0.0006,
                    one_day_net_edge_after_entry=0.0002,
                    one_day_net_edge_after_round_trip=-0.0001,
                    break_even_days_entry=0.6,
                    break_even_days_round_trip=1.2,
                    capacity=CapacityEstimate(
                        short_bid_notional=4000.0,
                        long_ask_notional=3000.0,
                        max_entry_notional=3000.0,
                        limiting_venue="hyperliquid",
                    ),
                ),
            ),
        )
    )

    from carryme_api.app import get_candidate_alert_store

    app.dependency_overrides[get_candidate_alert_store] = lambda: store
    client = TestClient(app)
    response = client.get("/v1/alerts/candidates")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["record"]["pair"]["label"] == "strk_extended_hyperliquid"


def test_execution_alerts_endpoint_reads_saved_events(tmp_path: Path) -> None:
    store = ExecutionAlertStore(tmp_path / "history.sqlite3")
    store.append(
        ExecutionAlertEvent(
            emitted_at=datetime(2026, 3, 29, 12, 5, tzinfo=UTC),
            alert_type="cleanup_needed",
            paper_trade_id=7,
            preview_hash="preview-hash",
            pair_status=ExecutionPairStatus(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                derived_state="cleanup_needed",
                recommended_action="close_open_leg",
                order_state=ExecutionOrderState(
                    execution_entry_id=12,
                    paper_trade_id=7,
                    preview_hash="preview-hash",
                    legs=[],
                    notes=[],
                ),
                reconciliation=ExecutionReconciliation(
                    execution_entry_id=12,
                    paper_trade_id=7,
                    preview_hash="preview-hash",
                    status="submitted",
                    recommended_action="verify_fill_status",
                    matched_all_leg_symbols=False,
                    venues=[],
                    notes=[],
                ),
                notes=[],
            ),
        )
    )
    store.append(
        ExecutionAlertEvent(
            emitted_at=datetime(2026, 3, 29, 12, 6, tzinfo=UTC),
            alert_type="review_required",
            paper_trade_id=8,
            preview_hash="other-preview-hash",
            pair_status=ExecutionPairStatus(
                execution_entry_id=13,
                paper_trade_id=8,
                preview_hash="other-preview-hash",
                derived_state="review_required",
                recommended_action="manual_review_required",
                order_state=ExecutionOrderState(
                    execution_entry_id=13,
                    paper_trade_id=8,
                    preview_hash="other-preview-hash",
                    legs=[],
                    notes=[],
                ),
                reconciliation=ExecutionReconciliation(
                    execution_entry_id=13,
                    paper_trade_id=8,
                    preview_hash="other-preview-hash",
                    status="submitted",
                    recommended_action="manual_review_required",
                    matched_all_leg_symbols=False,
                    venues=[],
                    notes=[],
                ),
                notes=[],
            ),
        )
    )

    from carryme_api.app import get_execution_alert_store

    app.dependency_overrides[get_execution_alert_store] = lambda: store
    try:
        client = TestClient(app)
        response = client.get("/v1/alerts/executions?paper_trade_id=7")
    finally:
        app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["alert_type"] == "cleanup_needed"
    assert payload[0]["paper_trade_id"] == 7
    assert all(item["paper_trade_id"] == 7 for item in payload)


def test_latest_history_endpoint_deduplicates_by_label(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    for recorded_at, entry_edge in [
        (datetime(2026, 3, 29, 0, 0, tzinfo=UTC), 0.0001),
        (datetime(2026, 3, 29, 1, 0, tzinfo=UTC), 0.0002),
    ]:
        store.append(
            OpportunityRecord(
                recorded_at=recorded_at,
                pair=FundingPairSpec(
                    label="strk_extended_hyperliquid",
                    left_venue="extended",
                    left_symbol="STRK-USD",
                    left_fee_profile="default",
                    right_venue="hyperliquid",
                    right_symbol="STRK",
                    right_fee_profile="tier0",
                ),
                opportunity=FundingArbOpportunity(
                    canonical_symbol="STRK-USD-PERP",
                    long_venue="hyperliquid",
                    short_venue="extended",
                    long_fee_profile="tier0",
                    short_fee_profile="default",
                    gross_daily_edge=0.0005,
                    entry_cost_rate=0.0003,
                    round_trip_cost_rate=0.0006,
                    one_day_net_edge_after_entry=entry_edge,
                    one_day_net_edge_after_round_trip=-0.0001,
                    break_even_days_entry=0.6,
                    break_even_days_round_trip=1.2,
                    capacity=CapacityEstimate(
                        short_bid_notional=4000.0,
                        long_ask_notional=3000.0,
                        max_entry_notional=3000.0,
                        limiting_venue="hyperliquid",
                    ),
                ),
            )
        )

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)
    response = client.get("/v1/history/funding-pairs/latest", params={"limit": 10})
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["recorded_at"] == "2026-03-29T01:00:00Z"


def test_ranked_history_endpoint_sorts_best_entry_edge_first(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    fixtures = [
        ("strk_extended_hyperliquid", "STRK-USD-PERP", 0.0002),
        ("arb_extended_paradex", "ARB-USD-PERP", 0.0008),
    ]
    for label, symbol, entry_edge in fixtures:
        store.append(
            OpportunityRecord(
                recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
                pair=FundingPairSpec(
                    label=label,
                    left_venue="extended",
                    left_symbol="STRK-USD" if "STRK" in symbol else "ARB-USD",
                    left_fee_profile="default",
                    right_venue="hyperliquid" if "STRK" in symbol else "paradex",
                    right_symbol="STRK" if "STRK" in symbol else "ARB-USD-PERP",
                    right_fee_profile="tier0" if "STRK" in symbol else "pro",
                ),
                opportunity=FundingArbOpportunity(
                    canonical_symbol=symbol,
                    long_venue="hyperliquid" if "STRK" in symbol else "paradex",
                    short_venue="extended",
                    long_fee_profile="tier0" if "STRK" in symbol else "pro",
                    short_fee_profile="default",
                    gross_daily_edge=0.001,
                    entry_cost_rate=0.0003,
                    round_trip_cost_rate=0.0006,
                    one_day_net_edge_after_entry=entry_edge,
                    one_day_net_edge_after_round_trip=entry_edge - 0.0003,
                    break_even_days_entry=0.5,
                    break_even_days_round_trip=1.0,
                    capacity=CapacityEstimate(
                        short_bid_notional=5000.0,
                        long_ask_notional=4500.0,
                        max_entry_notional=4500.0,
                        limiting_venue="paradex",
                    ),
                ),
            )
        )

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)
    response = client.get("/v1/history/funding-pairs/ranked", params={"limit": 10})
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload[0]["pair"]["label"] == "arb_extended_paradex"
    assert payload[1]["pair"]["label"] == "strk_extended_hyperliquid"


def test_dashboard_renders_saved_history(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    store.append(
        OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
            pair=FundingPairSpec(
                label="strk_extended_hyperliquid",
                left_venue="extended",
                left_symbol="STRK-USD",
                left_fee_profile="default",
                right_venue="hyperliquid",
                right_symbol="STRK",
                right_fee_profile="tier0",
            ),
            opportunity=FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            ),
        )
    )

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)
    response = client.get("/dashboard")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert "carryme operator view" in response.text
    assert "strk_extended_hyperliquid" in response.text


def test_candidate_history_endpoint_filters_by_thresholds(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    fixtures = [
        ("strk_extended_hyperliquid", 0.0002, 3000.0),
        ("arb_extended_paradex", -0.0001, 6000.0),
    ]
    for label, entry_edge, capacity in fixtures:
        store.append(
            OpportunityRecord(
                recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
                pair=FundingPairSpec(
                    label=label,
                    left_venue="extended",
                    left_symbol="STRK-USD" if "strk" in label else "ARB-USD",
                    left_fee_profile="default",
                    right_venue="hyperliquid" if "strk" in label else "paradex",
                    right_symbol="STRK" if "strk" in label else "ARB-USD-PERP",
                    right_fee_profile="tier0" if "strk" in label else "pro",
                ),
                opportunity=FundingArbOpportunity(
                    canonical_symbol="STRK-USD-PERP" if "strk" in label else "ARB-USD-PERP",
                    long_venue="hyperliquid" if "strk" in label else "paradex",
                    short_venue="extended",
                    long_fee_profile="tier0" if "strk" in label else "pro",
                    short_fee_profile="default",
                    gross_daily_edge=0.001,
                    entry_cost_rate=0.0003,
                    round_trip_cost_rate=0.0006,
                    one_day_net_edge_after_entry=entry_edge,
                    one_day_net_edge_after_round_trip=entry_edge - 0.0002,
                    break_even_days_entry=0.5,
                    break_even_days_round_trip=1.0,
                    capacity=CapacityEstimate(
                        short_bid_notional=capacity + 1000.0,
                        long_ask_notional=capacity,
                        max_entry_notional=capacity,
                        limiting_venue="paradex",
                    ),
                ),
            )
        )

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/v1/history/funding-pairs/candidates",
        params={
            "limit": 10,
            "min_one_day_net_edge_after_entry": 0.0,
            "min_capacity_notional": 2500.0,
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["pair"]["label"] == "strk_extended_hyperliquid"


def test_candidate_dashboard_renders_filtered_rows(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    store.append(
        OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
            pair=FundingPairSpec(
                label="strk_extended_hyperliquid",
                left_venue="extended",
                left_symbol="STRK-USD",
                left_fee_profile="default",
                right_venue="hyperliquid",
                right_symbol="STRK",
                right_fee_profile="tier0",
            ),
            opportunity=FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0002,
                one_day_net_edge_after_round_trip=-0.0001,
                break_even_days_entry=0.6,
                break_even_days_round_trip=1.2,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            ),
        )
    )

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/dashboard/candidates",
        params={
            "min_one_day_net_edge_after_entry": 0.0,
            "min_capacity_notional": 2500.0,
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert "carryme candidate view" in response.text
    assert "strk_extended_hyperliquid" in response.text


def test_trade_intents_endpoint_builds_ranked_intents(tmp_path: Path) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    fixtures = [
        ("strk_extended_hyperliquid", "STRK-USD-PERP", "hyperliquid", "tier0", 0.0002, 3000.0),
        ("arb_extended_paradex", "ARB-USD-PERP", "paradex", "pro", 0.0008, 4500.0),
    ]
    for label, symbol, long_venue, long_fee, entry_edge, capacity in fixtures:
        store.append(
            OpportunityRecord(
                recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
                pair=FundingPairSpec(
                    label=label,
                    left_venue="extended",
                    left_symbol="STRK-USD" if "STRK" in symbol else "ARB-USD",
                    left_fee_profile="default",
                    right_venue=long_venue,
                    right_symbol="STRK" if "STRK" in symbol else "ARB-USD-PERP",
                    right_fee_profile=long_fee,
                ),
                opportunity=FundingArbOpportunity(
                    canonical_symbol=symbol,
                    long_venue=long_venue,
                    short_venue="extended",
                    long_fee_profile=long_fee,
                    short_fee_profile="default",
                    gross_daily_edge=0.001,
                    entry_cost_rate=0.0003,
                    round_trip_cost_rate=0.0006,
                    one_day_net_edge_after_entry=entry_edge,
                    one_day_net_edge_after_round_trip=entry_edge - 0.0003,
                    break_even_days_entry=0.5,
                    break_even_days_round_trip=1.0,
                    capacity=CapacityEstimate(
                        short_bid_notional=capacity + 500.0,
                        long_ask_notional=capacity,
                        max_entry_notional=capacity,
                        limiting_venue=long_venue,
                    ),
                ),
            )
        )

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/v1/intents/funding-pairs",
        params={
            "limit": 10,
            "capacity_fraction": 0.25,
            "max_target_notional": 1000.0,
            "min_one_day_net_edge_after_entry": 0.0,
            "min_capacity_notional": 1000.0,
            "max_break_even_days_entry": 1.0,
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 2
    assert payload[0]["label"] == "arb_extended_paradex"
    assert payload[0]["target_notional"] == 1000.0


def test_trade_intent_endpoint_returns_not_found_when_thresholds_exclude_all(
    tmp_path: Path,
) -> None:
    store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    store.append(
        OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, tzinfo=UTC),
            pair=FundingPairSpec(
                label="strk_extended_hyperliquid",
                left_venue="extended",
                left_symbol="STRK-USD",
                left_fee_profile="default",
                right_venue="hyperliquid",
                right_symbol="STRK",
                right_fee_profile="tier0",
            ),
            opportunity=FundingArbOpportunity(
                canonical_symbol="STRK-USD-PERP",
                long_venue="hyperliquid",
                short_venue="extended",
                long_fee_profile="tier0",
                short_fee_profile="default",
                gross_daily_edge=0.0005,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=-0.0001,
                one_day_net_edge_after_round_trip=-0.0004,
                break_even_days_entry=0.8,
                break_even_days_round_trip=1.6,
                capacity=CapacityEstimate(
                    short_bid_notional=4000.0,
                    long_ask_notional=3000.0,
                    max_entry_notional=3000.0,
                    limiting_venue="hyperliquid",
                ),
            ),
        )
    )

    app.dependency_overrides[get_history_store] = lambda: store
    client = TestClient(app)
    response = client.get(
        "/v1/intents/funding-pair",
        params={
            "min_one_day_net_edge_after_entry": 0.0,
            "min_capacity_notional": 1000.0,
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json()["detail"] == "No trade intent candidate matched the requested filters"


def test_create_paper_trade_from_intent_persists_entry(tmp_path: Path) -> None:
    history_store = OpportunityHistoryStore(tmp_path / "history.sqlite3")
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    history_store.append(
        OpportunityRecord(
            recorded_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            pair=FundingPairSpec(
                label="arb_extended_paradex",
                left_venue="extended",
                left_symbol="ARB-USD",
                left_fee_profile="default",
                right_venue="paradex",
                right_symbol="ARB-USD-PERP",
                right_fee_profile="pro",
            ),
            opportunity=FundingArbOpportunity(
                canonical_symbol="ARB-USD-PERP",
                long_venue="paradex",
                short_venue="extended",
                long_fee_profile="pro",
                short_fee_profile="default",
                gross_daily_edge=0.001,
                entry_cost_rate=0.0003,
                round_trip_cost_rate=0.0006,
                one_day_net_edge_after_entry=0.0008,
                one_day_net_edge_after_round_trip=0.0005,
                break_even_days_entry=0.5,
                break_even_days_round_trip=1.0,
                capacity=CapacityEstimate(
                    short_bid_notional=5000.0,
                    long_ask_notional=4500.0,
                    max_entry_notional=4500.0,
                    limiting_venue="paradex",
                ),
            ),
        )
    )

    from carryme_api.app import get_paper_trade_store

    app.dependency_overrides[get_history_store] = lambda: history_store
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    client = TestClient(app)
    response = client.post(
        "/v1/paper-trades/from-intent",
        params={
            "capacity_fraction": 0.25,
            "max_target_notional": 1000.0,
            "min_one_day_net_edge_after_entry": 0.0,
            "min_capacity_notional": 1000.0,
            "max_break_even_days_entry": 1.0,
            "note": "operator accepted candidate",
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"]["label"] == "arb_extended_paradex"
    assert payload["note"] == "operator accepted candidate"
    assert payload["entry_id"] is not None
    assert len(paper_store.list_recent(limit=10)) == 1


def test_create_paper_trade_from_canary_caps_to_approved_notional(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "paper-trades.sqlite3")
    candidate = FundingUniverseCanaryCandidate(
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
        suggested_canary_notional=25.0,
    )

    class StubUniverseService:
        async def scan_canary_candidates(self, **_: object) -> list[FundingUniverseCanaryCandidate]:
            return [candidate]

    class StubRouteApprovalService:
        def filter_approved_canary_candidates(
            self,
            candidates: list[FundingUniverseCanaryCandidate],
        ) -> list[FundingUniverseCanaryCandidate]:
            assert len(candidates) == 1
            return [candidates[0].model_copy(update={"suggested_canary_notional": 7.5})]

        def get_for_candidate(
            self,
            _candidate: FundingUniverseCanaryCandidate,
        ) -> RouteApprovalEntry:
            return RouteApprovalEntry(
                updated_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                short_venue="extended",
                long_venue="paradex",
                short_fee_profile="default",
                long_fee_profile="pro_fastfills",
                approved=True,
                max_live_notional=7.5,
                note="approved canary",
            )

    from carryme_api.app import get_paper_trade_store

    app.dependency_overrides[get_opportunity_universe_service] = lambda: StubUniverseService()
    app.dependency_overrides[get_route_approval_service] = lambda: StubRouteApprovalService()
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    client = TestClient(app)
    response = client.post(
        "/v1/paper-trades/from-canary",
        params={
            "label": "arb_extended_paradex",
            "desired_notional": 20.0,
            "note": "approved canary",
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["intent"]["target_notional"] == 7.5
    assert payload["note"] == "approved canary"
    assert len(paper_store.list_recent(limit=10)) == 1


def test_paper_trades_endpoint_lists_saved_entries(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )

    from carryme_api.app import get_paper_trade_store

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    client = TestClient(app)
    response = client.get("/v1/paper-trades")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["intent"]["label"] == "arb_extended_paradex"


def test_mock_execution_endpoint_submits_saved_paper_trade(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )

    from carryme_api.app import get_execution_journal_store, get_paper_trade_store

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    client = TestClient(app)
    response = client.post(f"/v1/executions/mock/from-paper-trade/{paper_trade.entry_id}")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["adapter"] == "mock"


def test_live_extended_execution_requires_route_approval(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=11.0,
                capacity_fraction=0.25,
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
        )
    )

    class StubRouteApprovalService:
        def require_live_approval(self, intent: FundingPairTradeIntent) -> None:
            assert intent.label == "arb_extended_paradex"
            raise ValueError("Live execution is blocked because this route has not been approved")

    from carryme_api.app import get_api_settings, get_paper_trade_store

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_route_approval_service] = lambda: StubRouteApprovalService()
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
    )
    client = TestClient(app)
    response = client.post(
        f"/v1/executions/live/extended/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "preview-hash"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 409
    assert "has not been approved" in response.json()["detail"]


def test_executions_endpoint_lists_saved_entries(tmp_path: Path) -> None:
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="mock",
            mode="mock",
            status="accepted",
            paper_trade_id=7,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                note="operator accepted candidate",
                intent=FundingPairTradeIntent(
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=1000.0,
                    capacity_fraction=0.25,
                    max_target_notional=1000.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                    ),
                    short_leg=TradeLegIntent(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=1000.0,
                    ),
                ),
            ),
            legs=[
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                    status="accepted",
                    simulated=True,
                    external_reference="mock:7:buy",
                ),
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                    status="accepted",
                    simulated=True,
                    external_reference="mock:7:sell",
                ),
            ],
        )
    )

    from carryme_api.app import get_execution_journal_store

    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    client = TestClient(app)
    response = client.get("/v1/executions")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["paper_trade_id"] == 7


def test_execution_reconciliation_endpoint_reports_latest_execution_state(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="partial",
            paper_trade_id=paper_trade.entry_id,
            preview_hash="preview-hash",
            confirmation_entry_id=8,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                ),
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                    status="rejected",
                    simulated=False,
                ),
            ],
        )
    )

    from carryme_api.app import (
        get_account_preflight_service,
        get_execution_journal_store,
        get_paper_trade_store,
    )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id is not None
            assert configs
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=["ARB-USD"],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        balance_assets=["USDC"],
                        position_symbols=[],
                    ),
                ],
            )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    client = TestClient(app)
    response = client.get(
        f"/v1/executions/reconciliation/latest/from-paper-trade/{paper_trade.entry_id}"
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["status"] == "partial"
    assert payload["recommended_action"] == "complete_or_unwind_missing_leg"
    assert payload["matched_all_leg_symbols"] is False
    by_venue = {item["venue"]: item for item in payload["venues"]}
    assert by_venue["extended"]["matched_leg_symbols"] == ["ARB-USD"]
    assert by_venue["paradex"]["unmatched_leg_symbols"] == ["ARB-USD-PERP"]


def test_execution_order_state_endpoint_reports_latest_leg_state(tmp_path: Path) -> None:
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
            adapter="paradex_live",
            mode="live",
            status="submitted",
            paper_trade_id=7,
            preview_hash="preview-hash",
            confirmation_entry_id=9,
            paper_trade=PaperTradeEntry(
                entry_id=7,
                created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
                note="operator accepted candidate",
                intent=FundingPairTradeIntent(
                    label="arb_extended_paradex",
                    canonical_symbol="ARB-USD-PERP",
                    source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                    one_day_net_edge_after_entry=0.00055,
                    break_even_days_entry=0.45,
                    capacity_limit_notional=4500.0,
                    target_notional=11.0,
                    capacity_fraction=0.25,
                    max_target_notional=11.0,
                    long_leg=TradeLegIntent(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
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
            legs=[
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="order-1",
                    request_payload={"client_id": "carryme-pt1-paradex-buy"},
                )
            ],
        )
    )

    from carryme_api.app import (
        get_execution_journal_store,
        get_execution_order_state_service,
    )

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            assert entry.paper_trade_id == 7
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        external_reference="order-1",
                        client_id="carryme-pt1-paradex-buy",
                        derived_state="unfilled",
                        order_status="CLOSED",
                        cancel_reason="REMAINING_IOC_CANCEL",
                        remaining_size="122.7",
                        size="122.7",
                    )
                ],
            )

    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    client = TestClient(app)
    response = client.get("/v1/executions/order-state/latest/from-paper-trade/7")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == 7
    assert payload["legs"][0]["derived_state"] == "unfilled"
    assert payload["legs"][0]["cancel_reason"] == "REMAINING_IOC_CANCEL"


def test_execution_observation_endpoint_reports_latest_snapshot(tmp_path: Path) -> None:
    observation_store = ExecutionObservationStore(tmp_path / "history.sqlite3")
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 13, 7, tzinfo=UTC),
            context="guarded_pair_poll",
            execution_entry_id=12,
            paper_trade_id=7,
            preview_hash="preview-hash",
            order_state=ExecutionOrderState(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                legs=[
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="order-1",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    )
                ],
                notes=[],
            ),
            pair_status=ExecutionPairStatus(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                derived_state="unfilled",
                recommended_action="no_action",
                order_state=ExecutionOrderState(
                    execution_entry_id=12,
                    paper_trade_id=7,
                    preview_hash="preview-hash",
                    legs=[],
                    notes=[],
                ),
                reconciliation=ExecutionReconciliation(
                    execution_entry_id=12,
                    paper_trade_id=7,
                    preview_hash="preview-hash",
                    status="submitted",
                    recommended_action="verify_fill_status",
                    matched_all_leg_symbols=False,
                    venues=[
                        ExecutionVenueReconciliation(
                            venue="extended",
                            authenticated=True,
                            ready=True,
                            position_symbols=[],
                            matched_leg_symbols=[],
                            unmatched_leg_symbols=["ARB-USD"],
                        )
                    ],
                    notes=[],
                ),
                notes=[],
            ),
        )
    )
    latest = observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 13, 8, tzinfo=UTC),
            context="guarded_pair_poll",
            execution_entry_id=13,
            paper_trade_id=7,
            preview_hash="preview-hash-newer",
            order_state=ExecutionOrderState(
                execution_entry_id=13,
                paper_trade_id=7,
                preview_hash="preview-hash-newer",
                legs=[
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="order-2",
                        derived_state="open",
                        order_status="OPEN",
                    )
                ],
                notes=[],
            ),
            pair_status=ExecutionPairStatus(
                execution_entry_id=13,
                paper_trade_id=7,
                preview_hash="preview-hash-newer",
                derived_state="review_required",
                recommended_action="wait_for_fill",
                order_state=ExecutionOrderState(
                    execution_entry_id=13,
                    paper_trade_id=7,
                    preview_hash="preview-hash-newer",
                    legs=[],
                    notes=[],
                ),
                reconciliation=ExecutionReconciliation(
                    execution_entry_id=13,
                    paper_trade_id=7,
                    preview_hash="preview-hash-newer",
                    status="submitted",
                    recommended_action="verify_fill_status",
                    matched_all_leg_symbols=True,
                    venues=[
                        ExecutionVenueReconciliation(
                            venue="paradex",
                            authenticated=True,
                            ready=True,
                            position_symbols=["ARB-USD-PERP"],
                            matched_leg_symbols=["ARB-USD-PERP"],
                            unmatched_leg_symbols=[],
                        )
                    ],
                    notes=[],
                ),
                notes=[],
            ),
        )
    )
    observation_store.append(
        ExecutionObservationEntry(
            observed_at=datetime(2026, 3, 29, 13, 9, tzinfo=UTC),
            context="guarded_pair_poll",
            execution_entry_id=99,
            paper_trade_id=8,
            preview_hash="preview-hash-other-trade",
            order_state=ExecutionOrderState(
                execution_entry_id=99,
                paper_trade_id=8,
                preview_hash="preview-hash-other-trade",
                legs=[],
                notes=[],
            ),
        )
    )

    from carryme_api.app import get_execution_observation_store

    app.dependency_overrides[get_execution_observation_store] = lambda: observation_store
    client = TestClient(app)
    response = client.get("/v1/executions/observations/latest/from-paper-trade/7")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["entry_id"] == latest.entry_id
    assert payload["paper_trade_id"] == 7
    assert payload["context"] == "guarded_pair_poll"
    assert payload["pair_status"]["derived_state"] == "review_required"


def test_observe_pair_status_persists_observation_entries(tmp_path: Path) -> None:
    from carryme_api.app import _observe_pair_status_for_execution

    observation_store = ExecutionObservationStore(tmp_path / "history.sqlite3")
    paper_trade = PaperTradeEntry(
        entry_id=7,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="operator accepted candidate",
        intent=FundingPairTradeIntent(
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
            one_day_net_edge_after_entry=0.00055,
            break_even_days_entry=0.45,
            capacity_limit_notional=4500.0,
            target_notional=11.0,
            capacity_fraction=0.25,
            max_target_notional=11.0,
            long_leg=TradeLegIntent(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
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
    )
    execution = ExecutionJournalEntry(
        entry_id=12,
        executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
        adapter="paired_live:extended_then_paradex",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="preview-hash",
        confirmation_entry_id=9,
        paper_trade=paper_trade,
        legs=[
            ExecutionLegResult(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="sell",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="ext-order-1",
            )
        ],
    )

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id == 7
            _ = config_map
            return PaperTradeAccountPreflight(
                paper_trade_id=7,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=[],
                    )
                ],
                blocking_reasons=[],
            )

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            assert entry.entry_id == 12
            return ExecutionOrderState(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="ext-order-1",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    )
                ],
                notes=[],
            )

    async def run() -> None:
        from carryme_runtime import AccountPreflightService, ExecutionOrderStateService

        result = await _observe_pair_status_for_execution(
            paper_trade=paper_trade,
            execution=execution,
            settings=ApiSettings(database_path=str(tmp_path / "history.sqlite3")),
            account_service=cast(AccountPreflightService, StubAccountService()),
            order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
            observation_store=observation_store,
            poll_attempts=1,
            poll_interval_seconds=0.0,
        )
        assert result.derived_state == "unfilled"

    import asyncio

    asyncio.run(run())

    latest = observation_store.latest_for_paper_trade(7)
    assert latest is not None
    assert latest.context == "guarded_pair_poll"
    assert latest.order_state.legs[0].observation_source == "rest_poll"


def test_observe_pair_status_updates_latest_observation_across_polls(
    tmp_path: Path,
) -> None:
    from carryme_api.app import _observe_pair_status_for_execution

    observation_store = ExecutionObservationStore(tmp_path / "history.sqlite3")
    paper_trade = PaperTradeEntry(
        entry_id=7,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="operator accepted candidate",
        intent=FundingPairTradeIntent(
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
            one_day_net_edge_after_entry=0.00055,
            break_even_days_entry=0.45,
            capacity_limit_notional=4500.0,
            target_notional=11.0,
            capacity_fraction=0.25,
            max_target_notional=11.0,
            long_leg=TradeLegIntent(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
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
    )
    execution = ExecutionJournalEntry(
        entry_id=12,
        executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
        adapter="paired_live:extended_then_paradex",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="preview-hash",
        confirmation_entry_id=9,
        paper_trade=paper_trade,
        legs=[
            ExecutionLegResult(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="sell",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="ext-order-1",
            )
        ],
    )

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id == 7
            _ = config_map
            return PaperTradeAccountPreflight(
                paper_trade_id=7,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=[],
                    )
                ],
                blocking_reasons=[],
            )

    class SequencedOrderStateService:
        def __init__(self) -> None:
            self.calls = 0

        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            assert entry.entry_id == 12
            self.calls += 1
            if self.calls == 1:
                return ExecutionOrderState(
                    execution_entry_id=12,
                    paper_trade_id=7,
                    preview_hash="preview-hash",
                    legs=[
                        ExecutionLegOrderState(
                            venue="extended",
                            supported=True,
                            observation_source="rest_poll",
                            external_reference="ext-order-1",
                            derived_state="open",
                            order_status="OPEN",
                        )
                    ],
                    notes=[],
                )
            return ExecutionOrderState(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="ext-order-1",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    )
                ],
                notes=[],
            )

    async def run() -> None:
        from carryme_runtime import AccountPreflightService, ExecutionOrderStateService

        result = await _observe_pair_status_for_execution(
            paper_trade=paper_trade,
            execution=execution,
            settings=ApiSettings(database_path=str(tmp_path / "history.sqlite3")),
            account_service=cast(AccountPreflightService, StubAccountService()),
            order_state_service=cast(
                ExecutionOrderStateService,
                SequencedOrderStateService(),
            ),
            observation_store=observation_store,
            poll_attempts=2,
            poll_interval_seconds=0.0,
        )
        assert result.derived_state == "unfilled"

    import asyncio

    asyncio.run(run())

    observations = observation_store.list_recent(limit=10, paper_trade_id=7)
    latest = observation_store.latest_for_paper_trade(7)

    assert len(observations) == 2
    assert latest is not None
    assert latest.context == "guarded_pair_poll"
    assert latest.pair_status is not None
    assert latest.pair_status.derived_state == "unfilled"
    assert latest.order_state.legs[0].observation_source == "rest_poll"
    assert observations[1].pair_status is not None
    assert observations[1].pair_status.derived_state == "pending"


def test_observe_pair_status_tolerates_observation_store_failures(
    tmp_path: Path,
    caplog: pytest.LogCaptureFixture,
) -> None:
    from carryme_api.app import _observe_pair_status_for_execution

    paper_trade = PaperTradeEntry(
        entry_id=7,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="operator accepted candidate",
        intent=FundingPairTradeIntent(
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
            one_day_net_edge_after_entry=0.00055,
            break_even_days_entry=0.45,
            capacity_limit_notional=4500.0,
            target_notional=11.0,
            capacity_fraction=0.25,
            max_target_notional=11.0,
            long_leg=TradeLegIntent(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
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
    )
    execution = ExecutionJournalEntry(
        entry_id=12,
        executed_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
        adapter="paired_live:extended_then_paradex",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="preview-hash",
        confirmation_entry_id=9,
        paper_trade=paper_trade,
        legs=[
            ExecutionLegResult(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="sell",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="ext-order-1",
            )
        ],
    )

    class StubAccountService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            config_map: object,
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id == 7
            _ = config_map
            return PaperTradeAccountPreflight(
                paper_trade_id=7,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=[],
                    )
                ],
                blocking_reasons=[],
            )

    class StubOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            assert entry.entry_id == 12
            return ExecutionOrderState(
                execution_entry_id=12,
                paper_trade_id=7,
                preview_hash="preview-hash",
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        observation_source="rest_poll",
                        external_reference="ext-order-1",
                        derived_state="unfilled",
                        order_status="CLOSED",
                    )
                ],
                notes=[],
            )

    class FailingObservationStore:
        def append(self, entry: ExecutionObservationEntry) -> ExecutionObservationEntry:
            raise RuntimeError(f"failed to persist {entry.execution_entry_id}")

    async def run() -> None:
        from carryme_runtime import AccountPreflightService, ExecutionOrderStateService

        result = await _observe_pair_status_for_execution(
            paper_trade=paper_trade,
            execution=execution,
            settings=ApiSettings(database_path=str(tmp_path / "history.sqlite3")),
            account_service=cast(AccountPreflightService, StubAccountService()),
            order_state_service=cast(ExecutionOrderStateService, StubOrderStateService()),
            observation_store=cast(ExecutionObservationStore, FailingObservationStore()),
            poll_attempts=1,
            poll_interval_seconds=0.0,
        )
        assert result.derived_state == "unfilled"

    import asyncio

    with caplog.at_level("WARNING"):
        asyncio.run(run())

    assert "Failed to persist execution observation" in caplog.text


def test_execution_pair_status_endpoint_reports_cleanup_needed(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "paper.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 12, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 45, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    execution_store.append(
        ExecutionJournalEntry(
            entry_id=12,
            executed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=paper_trade.entry_id,
            preview_hash="preview-hash",
            confirmation_entry_id=3,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order",
                ),
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="order-1",
                    request_payload={
                        "client_id": "carryme-pt1-paradex-buy",
                        "market": "ARB-USD-PERP",
                    },
                ),
            ],
        )
    )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_execution_journal_store,
        get_execution_order_state_service,
        get_paper_trade_store,
    )

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            assert entry.paper_trade_id == paper_trade.entry_id
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        external_reference="ext-order",
                        derived_state="unknown",
                    ),
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        external_reference="order-1",
                        client_id="carryme-pt1-paradex-buy",
                        derived_state="unfilled",
                        order_status="CLOSED",
                        cancel_reason="REMAINING_IOC_CANCEL",
                    ),
                ],
            )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            entry: PaperTradeEntry,
            configs: object,
        ) -> PaperTradeAccountPreflight:
            assert entry.entry_id == paper_trade.entry_id
            return PaperTradeAccountPreflight(
                paper_trade_id=entry.entry_id or 0,
                label="arb_extended_paradex",
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=["ARB-USD"],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        position_symbols=[],
                    ),
                ],
                blocking_reasons=[],
            )

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings()
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    client = TestClient(app)
    assert paper_trade.entry_id is not None
    response = client.get(
        f"/v1/executions/pair-status/latest/from-paper-trade/{paper_trade.entry_id}"
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["derived_state"] == "cleanup_needed"
    assert payload["recommended_action"] == "close_open_leg"


def test_execution_cleanup_preview_endpoint_returns_reduce_only_preview(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "paper.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 12, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 45, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    execution_entry = execution_store.append(
        ExecutionJournalEntry(
            entry_id=12,
            executed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=paper_trade.entry_id,
            preview_hash="preview-hash",
            confirmation_entry_id=3,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order",
                ),
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="order-1",
                    request_payload={
                        "client_id": "carryme-pt1-paradex-buy",
                        "market": "ARB-USD-PERP",
                    },
                ),
            ],
        )
    )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_cleanup_preview_service,
        get_execution_journal_store,
        get_execution_order_state_service,
        get_paper_trade_store,
    )

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        external_reference="ext-order",
                        derived_state="unknown",
                    ),
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        external_reference="order-1",
                        client_id="carryme-pt1-paradex-buy",
                        derived_state="unfilled",
                    ),
                ],
            )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            entry: PaperTradeEntry,
            configs: object,
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=entry.entry_id or 0,
                label="arb_extended_paradex",
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=["ARB-USD"],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        position_symbols=[],
                    ),
                ],
                blocking_reasons=[],
            )

    class StubCleanupPreviewService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
            generated_at: datetime | None = None,
        ) -> ExecutionCleanupPreview:
            assert entry.entry_id == execution_entry.entry_id
            assert pair_status.recommended_action == "close_open_leg"
            return ExecutionCleanupPreview(
                execution_entry_id=execution_entry.entry_id,
                paper_trade_id=paper_trade.entry_id,
                generated_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
                preview_hash="cleanup-hash",
                reason="close_open_leg",
                leg=VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=123.0,
                    quantity_text="123",
                    reference_price=0.0895,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0896,
                    worst_price_text="0.0896",
                    reduce_only=True,
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=[
                        "CARRYME_API_EXTENDED_API_KEY",
                        "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY",
                    ],
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD", "reduce_only": True},
                    notes=[],
                ),
                notes=["cleanup preview"],
            )

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings()
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_cleanup_preview_service] = lambda: StubCleanupPreviewService()
    client = TestClient(app)
    assert paper_trade.entry_id is not None
    response = client.get(
        f"/v1/executions/cleanup-preview/latest/from-paper-trade/{paper_trade.entry_id}"
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["reason"] == "close_open_leg"
    assert payload["leg"]["reduce_only"] is True
    assert payload["leg"]["side"] == "buy"
    assert payload["leg"]["quantity_text"] == "123"


def test_execution_pair_close_preview_endpoint_returns_reduce_only_pair_close(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "paper.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            entry_id=7,
            created_at=datetime(2026, 3, 29, 12, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 45, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    execution_entry = execution_store.append(
        ExecutionJournalEntry(
            entry_id=12,
            executed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            adapter="paired_live:paradex_then_extended",
            mode="live",
            status="submitted",
            paper_trade_id=paper_trade.entry_id,
            preview_hash="preview-hash",
            confirmation_entry_id=3,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="pdx-order",
                ),
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order",
                ),
            ],
        )
    )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_execution_journal_store,
        get_execution_order_state_service,
        get_pair_close_preview_service,
        get_paper_trade_store,
    )

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        external_reference="pdx-order",
                        derived_state="filled",
                    ),
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        external_reference="ext-order",
                        derived_state="filled",
                    ),
                ],
            )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            entry: PaperTradeEntry,
            configs: object,
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=entry.entry_id or 0,
                label="arb_extended_paradex",
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        position_symbols=["ARB-USD-PERP"],
                    ),
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=["ARB-USD"],
                    ),
                ],
                blocking_reasons=[],
            )

    class StubPairClosePreviewService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
            generated_at: datetime | None = None,
        ) -> ExecutionPairClosePreview:
            assert entry.entry_id == execution_entry.entry_id
            assert pair_status.derived_state == "hedged"
            return ExecutionPairClosePreview(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                label=paper_trade.intent.label,
                generated_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
                slippage_tolerance_bps=10,
                preview_hash="pair-close-hash",
                reason="close_pair",
                legs=[
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="sell",
                        target_notional=11.0,
                        effective_notional=11.0,
                        quantity=124.4,
                        quantity_text="124.40000000",
                        reference_price=0.0885,
                        reference_price_source="best_bid",
                        worst_acceptable_price=0.0884,
                        worst_price_text="0.08840000",
                        reduce_only=True,
                        endpoint_path_hint="/v1/orders",
                        required_auth_env_vars=[],
                        auth_scheme="main account address + subkey private key",
                        payload={"market": "ARB-USD-PERP", "reduce_only": True},
                        notes=[],
                    ),
                    VenueOrderPreview(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="buy",
                        target_notional=11.0,
                        effective_notional=11.0,
                        quantity=124.0,
                        quantity_text="124",
                        reference_price=0.0881,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.0882,
                        worst_price_text="0.0882",
                        reduce_only=True,
                        endpoint_path_hint="/api/v1/user/order",
                        required_auth_env_vars=[],
                        auth_scheme="api key + Stark signing key",
                        payload={"symbol": "ARB-USD", "reduce_only": True},
                        notes=[],
                    ),
                ],
                notes=["pair close"],
            )

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings()
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_pair_close_preview_service] = lambda: StubPairClosePreviewService()
    client = TestClient(app)
    assert paper_trade.entry_id is not None
    response = client.get(
        f"/v1/executions/pair-close-preview/latest/from-paper-trade/{paper_trade.entry_id}"
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["reason"] == "close_pair"
    assert [leg["venue"] for leg in payload["legs"]] == ["paradex", "extended"]
    assert all(leg["reduce_only"] for leg in payload["legs"])


def test_execution_cleanup_preview_confirmation_endpoint_persists_confirmation(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "paper.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 12, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 45, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=paper_trade.entry_id,
            preview_hash="preview-hash",
            confirmation_entry_id=3,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order",
                )
            ],
        )
    )
    cleanup_store = CleanupPreviewConfirmationStore(tmp_path / "history.sqlite3")

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_cleanup_preview_confirmation_store,
        get_cleanup_preview_service,
        get_execution_journal_store,
        get_execution_order_state_service,
        get_paper_trade_store,
    )

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        external_reference="ext-order",
                        derived_state="unknown",
                    )
                ],
            )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            entry: PaperTradeEntry,
            configs: object,
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=entry.entry_id or 0,
                label="arb_extended_paradex",
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=["ARB-USD"],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        position_symbols=[],
                    ),
                ],
                blocking_reasons=[],
            )

    class StubCleanupPreviewService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
            generated_at: datetime | None = None,
        ) -> ExecutionCleanupPreview:
            return ExecutionCleanupPreview(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                generated_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
                preview_hash="cleanup-hash",
                reason="close_open_leg",
                leg=VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=123.0,
                    quantity_text="123",
                    reference_price=0.0895,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0896,
                    worst_price_text="0.0896",
                    reduce_only=True,
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=[],
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD", "reduce_only": True},
                    notes=[],
                ),
                notes=[],
            )

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings()
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_cleanup_preview_confirmation_store] = lambda: cleanup_store
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_cleanup_preview_service] = lambda: StubCleanupPreviewService()
    client = TestClient(app)
    assert paper_trade.entry_id is not None
    response = client.post(
        f"/v1/executions/cleanup-preview-confirmations/latest/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "cleanup-hash"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["preview_hash"] == "cleanup-hash"
    assert payload["preview"]["reason"] == "close_open_leg"
    stored_confirmations = cleanup_store.list_recent(limit=10, paper_trade_id=paper_trade.entry_id)
    assert len(stored_confirmations) == 1
    assert stored_confirmations[0].preview_hash == "cleanup-hash"
    assert stored_confirmations[0].preview.reason == "close_open_leg"


def test_execution_cleanup_preview_confirmation_endpoint_refreshes_stale_hash(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "paper.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 12, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 45, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="partial",
            paper_trade_id=paper_trade.entry_id,
            preview_hash="preview-hash",
            confirmation_entry_id=3,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="extended-order",
                    request_payload={"symbol": "ARB-USD"},
                )
            ],
        )
    )
    cleanup_store = CleanupPreviewConfirmationStore(tmp_path / "history.sqlite3")

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_cleanup_preview_confirmation_store,
        get_cleanup_preview_service,
        get_execution_journal_store,
        get_execution_order_state_service,
        get_paper_trade_store,
    )

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        external_reference="extended-order",
                        derived_state="filled",
                    )
                ],
            )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            entry: PaperTradeEntry,
            configs: object,
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=entry.entry_id or 0,
                label="arb_extended_paradex",
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=["ARB-USD"],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        position_symbols=[],
                    ),
                ],
                blocking_reasons=[],
            )

    class StubCleanupPreviewService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
            generated_at: datetime | None = None,
        ) -> ExecutionCleanupPreview:
            return ExecutionCleanupPreview(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                generated_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
                preview_hash="cleanup-hash-2",
                reason="close_open_leg",
                leg=VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=123.0,
                    quantity_text="123",
                    reference_price=0.0895,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0897,
                    worst_price_text="0.0897",
                    reduce_only=True,
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=[],
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD", "reduce_only": True},
                    notes=[],
                ),
                notes=[],
            )

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings()
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_cleanup_preview_confirmation_store] = lambda: cleanup_store
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_cleanup_preview_service] = lambda: StubCleanupPreviewService()
    client = TestClient(app)
    assert paper_trade.entry_id is not None
    response = client.post(
        f"/v1/executions/cleanup-preview-confirmations/latest/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "cleanup-hash-1"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["preview_hash"] == "cleanup-hash-2"
    assert "stale hash cleanup-hash-1" in payload["note"]
    stored_confirmations = cleanup_store.list_recent(limit=10, paper_trade_id=paper_trade.entry_id)
    assert len(stored_confirmations) == 1
    assert stored_confirmations[0].preview_hash == "cleanup-hash-2"


def test_execution_pair_close_preview_confirmation_endpoint_persists_confirmation(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "paper.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 12, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 45, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            adapter="paired_live:paradex_then_extended",
            mode="live",
            status="submitted",
            paper_trade_id=paper_trade.entry_id,
            preview_hash="preview-hash",
            confirmation_entry_id=3,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="pdx-order",
                ),
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order",
                ),
            ],
        )
    )
    confirmation_store = PairClosePreviewConfirmationStore(tmp_path / "history.sqlite3")

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_execution_journal_store,
        get_execution_order_state_service,
        get_pair_close_preview_confirmation_store,
        get_pair_close_preview_service,
        get_paper_trade_store,
    )

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        external_reference="pdx-order",
                        derived_state="filled",
                    ),
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        external_reference="ext-order",
                        derived_state="filled",
                    ),
                ],
            )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            entry: PaperTradeEntry,
            configs: object,
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=entry.entry_id or 0,
                label="arb_extended_paradex",
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        position_symbols=["ARB-USD-PERP"],
                    ),
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=["ARB-USD"],
                    ),
                ],
                blocking_reasons=[],
            )

    class StubPairClosePreviewService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
            generated_at: datetime | None = None,
        ) -> ExecutionPairClosePreview:
            return ExecutionPairClosePreview(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
                slippage_tolerance_bps=10,
                preview_hash="pair-close-hash",
                reason="close_pair",
                legs=[
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="sell",
                        target_notional=11.0,
                        effective_notional=11.0,
                        quantity=124.4,
                        quantity_text="124.40000000",
                        reference_price=0.0885,
                        reference_price_source="best_bid",
                        worst_acceptable_price=0.0884,
                        worst_price_text="0.08840000",
                        reduce_only=True,
                        endpoint_path_hint="/v1/orders",
                        required_auth_env_vars=[],
                        auth_scheme="main account address + subkey private key",
                        payload={"market": "ARB-USD-PERP", "reduce_only": True},
                        notes=[],
                    ),
                    VenueOrderPreview(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="buy",
                        target_notional=11.0,
                        effective_notional=11.0,
                        quantity=124.0,
                        quantity_text="124",
                        reference_price=0.0881,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.0882,
                        worst_price_text="0.0882",
                        reduce_only=True,
                        endpoint_path_hint="/api/v1/user/order",
                        required_auth_env_vars=[],
                        auth_scheme="api key + Stark signing key",
                        payload={"symbol": "ARB-USD", "reduce_only": True},
                        notes=[],
                    ),
                ],
                notes=[],
            )

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings()
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_pair_close_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_pair_close_preview_service] = lambda: StubPairClosePreviewService()
    client = TestClient(app)
    assert paper_trade.entry_id is not None
    response = client.post(
        f"/v1/executions/pair-close-preview-confirmations/latest/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "pair-close-hash"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["preview_hash"] == "pair-close-hash"
    assert payload["preview"]["reason"] == "close_pair"
    stored_confirmations = confirmation_store.list_recent(
        limit=10,
        paper_trade_id=paper_trade.entry_id,
    )
    assert len(stored_confirmations) == 1
    assert stored_confirmations[0].paper_trade_id == paper_trade.entry_id
    assert stored_confirmations[0].preview_hash == "pair-close-hash"
    assert stored_confirmations[0].preview.reason == "close_pair"


def test_execution_pair_close_preview_confirmation_endpoint_rejects_stale_client_preview(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "paper.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 12, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 45, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            adapter="paired_live:paradex_then_extended",
            mode="live",
            status="submitted",
            paper_trade_id=paper_trade.entry_id,
            preview_hash="preview-hash",
            confirmation_entry_id=3,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="pdx-order",
                ),
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order",
                ),
            ],
        )
    )
    confirmation_store = PairClosePreviewConfirmationStore(tmp_path / "history.sqlite3")

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_execution_journal_store,
        get_execution_order_state_service,
        get_pair_close_preview_confirmation_store,
        get_pair_close_preview_service,
        get_paper_trade_store,
    )

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        external_reference="pdx-order",
                        derived_state="filled",
                    ),
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        external_reference="ext-order",
                        derived_state="filled",
                    ),
                ],
            )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            entry: PaperTradeEntry,
            configs: object,
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=entry.entry_id or 0,
                label="arb_extended_paradex",
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        position_symbols=["ARB-USD-PERP"],
                    ),
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=["ARB-USD"],
                    ),
                ],
                blocking_reasons=[],
            )

    canonical_preview = ExecutionPairClosePreview(
        execution_entry_id=1,
        paper_trade_id=paper_trade.entry_id,
        label="arb_extended_paradex",
        generated_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
        slippage_tolerance_bps=10,
        preview_hash="pair-close-hash",
        reason="close_pair",
        legs=[
            VenueOrderPreview(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="sell",
                target_notional=11.0,
                effective_notional=11.0,
                quantity=124.4,
                quantity_text="124.40000000",
                reference_price=0.0885,
                reference_price_source="best_bid",
                worst_acceptable_price=0.0884,
                worst_price_text="0.08840000",
                reduce_only=True,
                endpoint_path_hint="/v1/orders",
                required_auth_env_vars=[],
                auth_scheme="main account address + subkey private key",
                payload={"market": "ARB-USD-PERP", "reduce_only": True},
                notes=[],
            ),
            VenueOrderPreview(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="buy",
                target_notional=11.0,
                effective_notional=11.0,
                quantity=124.0,
                quantity_text="124",
                reference_price=0.0881,
                reference_price_source="best_ask",
                worst_acceptable_price=0.0882,
                worst_price_text="0.0882",
                reduce_only=True,
                endpoint_path_hint="/api/v1/user/order",
                required_auth_env_vars=[],
                auth_scheme="api key + Stark signing key",
                payload={"symbol": "ARB-USD", "reduce_only": True},
                notes=[],
            ),
        ],
        notes=["canonical"],
    )

    class StubPairClosePreviewService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
            generated_at: datetime | None = None,
        ) -> ExecutionPairClosePreview:
            return canonical_preview

    stale_preview = canonical_preview.model_copy(
        update={
            "slippage_tolerance_bps": 99,
            "reason": "stale_close_pair",
            "notes": ["stale"],
        }
    )

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings()
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_pair_close_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_pair_close_preview_service] = lambda: StubPairClosePreviewService()
    client = TestClient(app)
    assert paper_trade.entry_id is not None
    response = client.post(
        f"/v1/executions/pair-close-preview-confirmations/latest/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "pair-close-hash"},
        json=stale_preview.model_dump(mode="json"),
    )
    app.dependency_overrides.clear()

    assert response.status_code == 409
    assert "current server preview" in response.json()["detail"]
    assert confirmation_store.list_recent(limit=10, paper_trade_id=paper_trade.entry_id) == []


def test_guarded_pair_close_endpoint_rejects_duplicate_retry(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PairClosePreviewConfirmationStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    observation_store = ExecutionObservationStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    confirmation_store.append(
        carryme_models_module.PairClosePreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="pair-close-hash",
            preview=ExecutionPairClosePreview(
                execution_entry_id=12,
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=10,
                preview_hash="pair-close-hash",
                reason="close_pair",
                legs=[
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="sell",
                        target_notional=11.0,
                        effective_notional=10.99,
                        quantity=123.1,
                        quantity_text="123.10000000",
                        quantity_increment=0.1,
                        minimum_order_size=0.1,
                        minimum_notional=10.0,
                        reference_price=0.0892,
                        reference_price_source="best_bid",
                        worst_acceptable_price=0.0891,
                        worst_price_text="0.08910000",
                        price_increment=0.0001,
                        max_order_value=1_000_000.0,
                        reduce_only=True,
                        endpoint_path_hint="/v1/orders",
                        auth_scheme="main account address + subkey private key",
                        payload={"market": "ARB-USD-PERP", "reduce_only": True},
                        notes=[],
                    ),
                    VenueOrderPreview(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="buy",
                        target_notional=11.0,
                        effective_notional=10.95,
                        quantity=123.0,
                        quantity_text="123",
                        quantity_increment=1.0,
                        minimum_order_size=10.0,
                        minimum_notional=0.918,
                        reference_price=0.0890,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.0891,
                        worst_price_text="0.0891",
                        price_increment=0.0001,
                        max_order_value=1_250_000.0,
                        reduce_only=True,
                        endpoint_path_hint="/api/v1/user/order",
                        auth_scheme="api key + Stark signing key",
                        payload={"symbol": "ARB-USD", "reduce_only": True},
                        notes=[],
                    ),
                ],
                notes=[],
            ),
            note="operator confirmed",
        )
    )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        free_collateral=25.0,
                        position_symbols=["ARB-USD"],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        available_to_trade=25.0,
                        position_symbols=["ARB-USD-PERP"],
                    ),
                ],
                blocking_reasons=[],
            )

        async def probe_venues(self, configs: object) -> list[VenueAccountPreflight]:
            return [
                VenueAccountPreflight(
                    venue="extended",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_key",
                    free_collateral=25.0,
                ),
                VenueAccountPreflight(
                    venue="paradex",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="subkey_jwt",
                    available_to_trade=25.0,
                ),
            ]

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        external_reference="paradex-close-1",
                        derived_state="filled",
                    ),
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        external_reference="extended-close-1",
                        derived_state="filled",
                    ),
                ],
            )

    class StubPairCloseLiveExecutionCoordinator:
        async def submit_confirmed_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: carryme_models_module.PairClosePreviewConfirmationEntry,
            first_venue: str,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            adapter_first = (
                confirmation.preview.legs[0].venue if first_venue in {"", "auto"} else first_venue
            )
            adapter_second = confirmation.preview.legs[1].venue
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
                adapter=f"paired_cleanup:{adapter_first}_then_{adapter_second}",
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
                        fee_profile="pro",
                        side="sell",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="paradex-close-1",
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
                    ),
                ],
            )

    class StubCleanupPreviewService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
        ) -> ExecutionCleanupPreview:
            raise AssertionError("cleanup preview should not be requested when auto_cleanup=false")

    class StubCleanupLiveExecutionRouter:
        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            raise AssertionError("cleanup live router should not be used when auto_cleanup=false")

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_cleanup_live_execution_router,
        get_cleanup_preview_service,
        get_execution_journal_store,
        get_execution_observation_store,
        get_execution_order_state_service,
        get_pair_close_live_execution_coordinator,
        get_pair_close_preview_confirmation_store,
        get_paper_trade_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_pair_close_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_execution_observation_store] = lambda: observation_store
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_pair_close_live_execution_coordinator] = (
        lambda: StubPairCloseLiveExecutionCoordinator()
    )
    app.dependency_overrides[get_cleanup_preview_service] = lambda: StubCleanupPreviewService()
    app.dependency_overrides[get_cleanup_live_execution_router] = (
        lambda: StubCleanupLiveExecutionRouter()
    )
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-private",
    )
    client = TestClient(app)
    first = client.post(
        f"/v1/executions/live/pair/close/from-paper-trade/{paper_trade.entry_id}",
        params={
            "preview_hash": "pair-close-hash",
            "poll_attempts": 1,
            "poll_interval_seconds": 0,
            "auto_cleanup": "false",
        },
    )
    second = client.post(
        f"/v1/executions/live/pair/close/from-paper-trade/{paper_trade.entry_id}",
        params={
            "preview_hash": "pair-close-hash",
            "poll_attempts": 1,
            "poll_interval_seconds": 0,
            "auto_cleanup": "false",
        },
    )
    app.dependency_overrides.clear()

    assert first.status_code == 200
    assert second.status_code == 409
    assert second.json()["detail"]["adapter"] == "paired_cleanup:paradex_then_extended"
    saved_executions = [
        entry
        for entry in execution_store.list_recent(limit=10)
        if entry.paper_trade_id == paper_trade.entry_id
    ]
    assert len(saved_executions) == 1


def test_execute_extended_cleanup_endpoint_submits_confirmed_cleanup_preview(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "paper.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 12, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 45, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    latest_execution = execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="submitted",
            paper_trade_id=paper_trade.entry_id,
            preview_hash="preview-hash",
            confirmation_entry_id=3,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="ext-order",
                )
            ],
        )
    )
    cleanup_store = CleanupPreviewConfirmationStore(tmp_path / "history.sqlite3")
    confirmation = cleanup_store.append(
        CleanupPreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 2, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="cleanup-hash",
            preview=ExecutionCleanupPreview(
                execution_entry_id=latest_execution.entry_id,
                paper_trade_id=paper_trade.entry_id,
                generated_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
                preview_hash="cleanup-hash",
                reason="close_open_leg",
                leg=VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=123.0,
                    quantity_text="123",
                    reference_price=0.0895,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0896,
                    worst_price_text="0.0896",
                    reduce_only=True,
                    endpoint_path_hint="/api/v1/user/order",
                    required_auth_env_vars=[],
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD", "reduce_only": True},
                    notes=[],
                ),
                notes=[],
            ),
            note="operator confirmed cleanup",
        )
    )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_cleanup_preview_confirmation_store,
        get_cleanup_preview_service,
        get_execution_journal_store,
        get_execution_order_state_service,
        get_extended_live_execution_service,
        get_paper_trade_store,
    )

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        external_reference="ext-order",
                        derived_state="unknown",
                    )
                ],
            )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            entry: PaperTradeEntry,
            configs: object,
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=entry.entry_id or 0,
                label="arb_extended_paradex",
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=["ARB-USD"],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        position_symbols=[],
                    ),
                ],
                blocking_reasons=[],
            )

        async def probe_venues(self, configs: object) -> list[VenueAccountPreflight]:
            return [
                VenueAccountPreflight(
                    venue="extended",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_key",
                ),
                VenueAccountPreflight(
                    venue="paradex",
                    enabled=False,
                    authenticated=False,
                    ready=False,
                    credential_mode="subkey_jwt",
                ),
            ]

    class StubCleanupPreviewService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
            generated_at: datetime | None = None,
        ) -> ExecutionCleanupPreview:
            return confirmation.preview

    class StubExtendedLiveExecutionService:
        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            assert confirmation.entry_id is not None
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 3, tzinfo=UTC),
                adapter="extended_cleanup_live",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="buy",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="cleanup-order-1",
                    )
                ],
            )

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="stark-key",
    )
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_cleanup_preview_confirmation_store] = lambda: cleanup_store
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_cleanup_preview_service] = lambda: StubCleanupPreviewService()
    app.dependency_overrides[get_extended_live_execution_service] = (
        lambda: StubExtendedLiveExecutionService()
    )
    app.dependency_overrides[get_route_approval_service] = lambda: _AllowAllRouteApprovalService()
    client = TestClient(app)
    assert paper_trade.entry_id is not None
    response = client.post(
        f"/v1/executions/live/extended/cleanup/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "cleanup-hash"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["adapter"] == "extended_cleanup_live"
    assert payload["confirmation_entry_id"] == confirmation.entry_id
    assert payload["preview_hash"] == "cleanup-hash"
    assert payload["legs"][0]["external_reference"] == "cleanup-order-1"
    stored_entries = execution_store.list_recent(limit=10)
    assert len(stored_entries) == 2
    assert stored_entries[0].paper_trade_id == paper_trade.entry_id
    assert stored_entries[0].preview_hash == "cleanup-hash"
    assert stored_entries[0].confirmation_entry_id == confirmation.entry_id


def test_execute_paradex_cleanup_endpoint_submits_confirmed_cleanup_preview(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "paper.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 12, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 45, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_paradex",
            mode="live",
            status="partial",
            paper_trade_id=paper_trade.entry_id,
            preview_hash="preview-hash",
            confirmation_entry_id=3,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="paradex-order",
                    request_payload={"market": "ARB-USD-PERP"},
                )
            ],
        )
    )
    cleanup_store = CleanupPreviewConfirmationStore(tmp_path / "history.sqlite3")
    confirmation = cleanup_store.append(
        CleanupPreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 2, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="cleanup-hash",
            preview=ExecutionCleanupPreview(
                execution_entry_id=5,
                paper_trade_id=paper_trade.entry_id or 0,
                generated_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
                preview_hash="cleanup-hash",
                reason="close_open_leg",
                leg=VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="sell",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=123.1,
                    quantity_text="123.10000000",
                    reference_price=0.0892,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0890,
                    worst_price_text="0.08900000",
                    reduce_only=True,
                    endpoint_path_hint="/v1/orders",
                    required_auth_env_vars=[],
                    auth_scheme="main account address + subkey private key",
                    payload={"market": "ARB-USD-PERP", "reduce_only": True},
                    notes=[],
                ),
                notes=[],
            ),
            note="operator confirmed cleanup",
        )
    )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_cleanup_preview_confirmation_store,
        get_cleanup_preview_service,
        get_execution_journal_store,
        get_execution_order_state_service,
        get_paper_trade_store,
        get_paradex_live_execution_service,
    )

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        external_reference="paradex-order",
                        derived_state="unknown",
                    )
                ],
            )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            entry: PaperTradeEntry,
            configs: object,
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=entry.entry_id or 0,
                label="arb_extended_paradex",
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=[],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        position_symbols=["ARB-USD-PERP"],
                    ),
                ],
                blocking_reasons=[],
            )

        async def probe_venues(self, configs: object) -> list[VenueAccountPreflight]:
            return [
                VenueAccountPreflight(
                    venue="extended",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_key",
                ),
                VenueAccountPreflight(
                    venue="paradex",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="subkey_jwt",
                ),
            ]

    class StubCleanupPreviewService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
            generated_at: datetime | None = None,
        ) -> ExecutionCleanupPreview:
            return confirmation.preview

    class StubParadexLiveExecutionService:
        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            assert confirmation.entry_id is not None
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 3, tzinfo=UTC),
                adapter="paradex_cleanup_live",
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
                        fee_profile="pro",
                        side="sell",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="cleanup-order-2",
                    )
                ],
            )

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-private",
    )
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_cleanup_preview_confirmation_store] = lambda: cleanup_store
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_cleanup_preview_service] = lambda: StubCleanupPreviewService()
    app.dependency_overrides[get_paradex_live_execution_service] = (
        lambda: StubParadexLiveExecutionService()
    )
    app.dependency_overrides[get_route_approval_service] = lambda: _AllowAllRouteApprovalService()
    client = TestClient(app)
    assert paper_trade.entry_id is not None
    response = client.post(
        f"/v1/executions/live/paradex/cleanup/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "cleanup-hash"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["adapter"] == "paradex_cleanup_live"
    assert payload["confirmation_entry_id"] == confirmation.entry_id
    assert payload["preview_hash"] == "cleanup-hash"
    assert payload["legs"][0]["external_reference"] == "cleanup-order-2"


def test_execute_paradex_cleanup_endpoint_refreshes_stale_confirmation(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "paper.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 12, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="wld_paradex_extended",
                canonical_symbol="WLD-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 45, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0012,
                break_even_days_entry=0.35,
                capacity_limit_notional=1000.0,
                target_notional=25.0,
                capacity_fraction=0.25,
                max_target_notional=25.0,
                long_leg=TradeLegIntent(
                    venue="extended",
                    symbol="WLD-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=25.0,
                ),
                short_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="WLD-USD-PERP",
                    fee_profile="pro_fastfills",
                    side="sell",
                    target_notional=25.0,
                ),
            ),
        )
    )
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            adapter="paired_live:paradex_then_extended",
            mode="live",
            status="partial",
            paper_trade_id=paper_trade.entry_id,
            preview_hash="preview-hash",
            confirmation_entry_id=7,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="paradex",
                    symbol="WLD-USD-PERP",
                    fee_profile="pro_fastfills",
                    side="sell",
                    target_notional=25.0,
                    status="submitted",
                    simulated=False,
                    external_reference="paradex-order",
                    request_payload={"market": "WLD-USD-PERP"},
                )
            ],
        )
    )
    cleanup_store = CleanupPreviewConfirmationStore(tmp_path / "history.sqlite3")
    cleanup_store.append(
        CleanupPreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 2, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="wld_paradex_extended",
            preview_hash="cleanup-hash-1",
            preview=ExecutionCleanupPreview(
                execution_entry_id=5,
                paper_trade_id=paper_trade.entry_id or 0,
                generated_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
                preview_hash="cleanup-hash-1",
                reason="close_open_leg",
                leg=VenueOrderPreview(
                    venue="paradex",
                    symbol="WLD-USD-PERP",
                    fee_profile="pro_fastfills",
                    side="buy",
                    target_notional=25.0,
                    effective_notional=25.0792,
                    quantity=94.0,
                    quantity_text="94.00000000",
                    reference_price=0.2668,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.2671,
                    worst_price_text="0.26710000",
                    reduce_only=True,
                    endpoint_path_hint="/v1/orders",
                    required_auth_env_vars=[],
                    auth_scheme="main account address + subkey private key",
                    payload={"market": "WLD-USD-PERP", "reduce_only": True},
                    notes=[],
                ),
                notes=[],
            ),
            note="auto cleanup",
        )
    )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_cleanup_preview_confirmation_store,
        get_cleanup_preview_service,
        get_execution_journal_store,
        get_execution_order_state_service,
        get_paper_trade_store,
        get_paradex_live_execution_service,
    )

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        external_reference="paradex-order",
                        derived_state="filled",
                    )
                ],
            )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            entry: PaperTradeEntry,
            configs: object,
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=entry.entry_id or 0,
                label="wld_paradex_extended",
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=[],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        position_symbols=["WLD-USD-PERP"],
                    ),
                ],
                blocking_reasons=[],
            )

        async def probe_venues(self, configs: object) -> list[VenueAccountPreflight]:
            return [
                VenueAccountPreflight(
                    venue="extended",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_key",
                ),
                VenueAccountPreflight(
                    venue="paradex",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="subkey_jwt",
                ),
            ]

    class StubCleanupPreviewService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
            generated_at: datetime | None = None,
        ) -> ExecutionCleanupPreview:
            return ExecutionCleanupPreview(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id or 0,
                generated_at=datetime(2026, 3, 29, 13, 3, tzinfo=UTC),
                preview_hash="cleanup-hash-2",
                reason="close_open_leg",
                leg=VenueOrderPreview(
                    venue="paradex",
                    symbol="WLD-USD-PERP",
                    fee_profile="pro_fastfills",
                    side="buy",
                    target_notional=25.0,
                    effective_notional=25.0792,
                    quantity=94.0,
                    quantity_text="94.00000000",
                    reference_price=0.2670,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.2673,
                    worst_price_text="0.26730000",
                    reduce_only=True,
                    endpoint_path_hint="/v1/orders",
                    required_auth_env_vars=[],
                    auth_scheme="main account address + subkey private key",
                    payload={"market": "WLD-USD-PERP", "reduce_only": True},
                    notes=[],
                ),
                notes=[],
            )

    class StubParadexLiveExecutionService:
        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            assert confirmation.preview_hash == "cleanup-hash-2"
            assert confirmation.preview.preview_hash == "cleanup-hash-2"
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 4, tzinfo=UTC),
                adapter="paradex_cleanup_live",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue="paradex",
                        symbol="WLD-USD-PERP",
                        fee_profile="pro_fastfills",
                        side="buy",
                        target_notional=25.0,
                        status="submitted",
                        simulated=False,
                        external_reference="cleanup-order-refresh",
                    )
                ],
            )

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-private",
    )
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_cleanup_preview_confirmation_store] = lambda: cleanup_store
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_cleanup_preview_service] = lambda: StubCleanupPreviewService()
    app.dependency_overrides[get_paradex_live_execution_service] = (
        lambda: StubParadexLiveExecutionService()
    )
    client = TestClient(app)
    assert paper_trade.entry_id is not None
    response = client.post(
        f"/v1/executions/live/paradex/cleanup/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "cleanup-hash-1"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["preview_hash"] == "cleanup-hash-2"
    confirmations = cleanup_store.list_recent(limit=10, paper_trade_id=paper_trade.entry_id)
    assert confirmations[0].preview_hash == "cleanup-hash-2"
    assert "stale hash cleanup-hash-1" in (confirmations[0].note or "")


def test_executions_endpoint_rejects_invalid_limit(tmp_path: Path) -> None:
    from carryme_api.app import get_execution_journal_store

    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    response = client.get("/v1/executions", params={"limit": 0})
    app.dependency_overrides.clear()

    assert response.status_code == 400
    assert response.json()["detail"] == "limit must be at least 1"


def test_executions_endpoint_rejects_limit_above_history_cap(tmp_path: Path) -> None:
    from carryme_api.app import get_execution_journal_store

    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")

    client = TestClient(app)
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    response = client.get("/v1/executions", params={"limit": 1001})
    app.dependency_overrides.clear()

    assert response.status_code == 400
    assert response.json()["detail"] == "limit must be at most 1000"


def test_execute_hyperliquid_cleanup_endpoint_submits_confirmed_cleanup_preview(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "paper.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 12, 50, tzinfo=UTC),
            intent=FundingPairTradeIntent(
                label="arb_extended_hyperliquid",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 45, tzinfo=UTC),
                one_day_net_edge_after_entry=0.0011,
                break_even_days_entry=0.63,
                capacity_limit_notional=126.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="hyperliquid",
                    symbol="ARB",
                    fee_profile="tier0",
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
        )
    )
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            adapter="paired_live:extended_then_hyperliquid",
            mode="live",
            status="partial",
            paper_trade_id=paper_trade.entry_id,
            preview_hash="preview-hash",
            confirmation_entry_id=3,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="hyperliquid",
                    symbol="ARB",
                    fee_profile="tier0",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="777",
                    request_payload={"coin": "ARB"},
                )
            ],
        )
    )
    cleanup_store = CleanupPreviewConfirmationStore(tmp_path / "history.sqlite3")
    confirmation = cleanup_store.append(
        CleanupPreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 2, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_hyperliquid",
            preview_hash="cleanup-hash",
            preview=ExecutionCleanupPreview(
                execution_entry_id=5,
                paper_trade_id=paper_trade.entry_id or 0,
                generated_at=datetime(2026, 3, 29, 13, 1, tzinfo=UTC),
                preview_hash="cleanup-hash",
                reason="close_open_leg",
                leg=VenueOrderPreview(
                    venue="hyperliquid",
                    symbol="ARB",
                    fee_profile="tier0",
                    side="sell",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=119.3,
                    quantity_text="119.3",
                    reference_price=0.0922,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.09171,
                    worst_price_text="0.09171",
                    reduce_only=True,
                    endpoint_path_hint="/exchange",
                    required_auth_env_vars=[],
                    auth_scheme="account address + API wallet private key",
                    payload={"coin": "ARB", "reduce_only": True},
                    notes=[],
                ),
                notes=[],
            ),
            note="operator confirmed cleanup",
        )
    )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_cleanup_preview_confirmation_store,
        get_cleanup_preview_service,
        get_execution_journal_store,
        get_execution_order_state_service,
        get_hyperliquid_live_execution_service,
        get_paper_trade_store,
    )

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="hyperliquid",
                        supported=True,
                        external_reference="777",
                        derived_state="unknown",
                    )
                ],
            )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            entry: PaperTradeEntry,
            configs: object,
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=entry.entry_id or 0,
                label="arb_extended_hyperliquid",
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=[],
                        free_collateral=25.0,
                    ),
                    VenueAccountPreflight(
                        venue="hyperliquid",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_wallet",
                        position_symbols=["ARB"],
                        available_to_trade=25.0,
                    ),
                ],
                blocking_reasons=[],
            )

        async def probe_venues(self, configs: object) -> list[VenueAccountPreflight]:
            return [
                VenueAccountPreflight(
                    venue="extended",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_key",
                    free_collateral=25.0,
                ),
                VenueAccountPreflight(
                    venue="hyperliquid",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_wallet",
                    available_to_trade=25.0,
                ),
            ]

    class StubCleanupPreviewService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
            generated_at: datetime | None = None,
        ) -> ExecutionCleanupPreview:
            return confirmation.preview

    class StubHyperliquidLiveExecutionService:
        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            assert confirmation.entry_id is not None
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 3, tzinfo=UTC),
                adapter="hyperliquid_cleanup_live",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue="hyperliquid",
                        symbol="ARB",
                        fee_profile="tier0",
                        side="sell",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="cleanup-order-3",
                    )
                ],
            )

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        hyperliquid_live_enabled=True,
        hyperliquid_account_address="0xhyper",
        hyperliquid_api_wallet_private_key="0xwallet",
    )
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_cleanup_preview_confirmation_store] = lambda: cleanup_store
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_cleanup_preview_service] = lambda: StubCleanupPreviewService()
    app.dependency_overrides[get_hyperliquid_live_execution_service] = (
        lambda: StubHyperliquidLiveExecutionService()
    )
    app.dependency_overrides[get_route_approval_service] = lambda: _AllowAllRouteApprovalService()
    client = TestClient(app)
    assert paper_trade.entry_id is not None
    response = client.post(
        f"/v1/executions/live/hyperliquid/cleanup/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "cleanup-hash"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["adapter"] == "hyperliquid_cleanup_live"
    assert payload["confirmation_entry_id"] == confirmation.entry_id
    assert payload["preview_hash"] == "cleanup-hash"
    assert payload["legs"][0]["external_reference"] == "cleanup-order-3"


def test_live_execution_preflight_venues_endpoint_reports_missing_envs() -> None:
    from carryme_api.app import get_api_settings

    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key=None,
        extended_stark_private_key=None,
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-secret",
        paradex_bearer_token=None,
        hyperliquid_live_enabled=False,
        hyperliquid_account_address=None,
        hyperliquid_api_wallet_private_key=None,
    )
    client = TestClient(app)
    response = client.get("/v1/executions/preflight/venues")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = {item["venue"]: item for item in response.json()}
    assert payload["extended"]["ready"] is False
    assert "CARRYME_API_EXTENDED_API_KEY" in payload["extended"]["missing_env_vars"]
    assert payload["paradex"]["ready"] is True
    assert payload["paradex"]["missing_env_vars"] == []


def test_live_execution_preflight_for_saved_paper_trade(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )

    from carryme_api.app import get_api_settings, get_paper_trade_store

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=False,
        paradex_account_address=None,
        paradex_private_key=None,
        paradex_bearer_token=None,
        hyperliquid_live_enabled=False,
        hyperliquid_account_address=None,
        hyperliquid_api_wallet_private_key=None,
    )
    client = TestClient(app)
    response = client.get(f"/v1/executions/preflight/from-paper-trade/{paper_trade.entry_id}")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["ready"] is False
    assert {item["venue"] for item in payload["venues"]} == {"extended", "paradex"}


def test_account_preflight_venues_endpoint_uses_service_dependency() -> None:
    class StubAccountPreflightService:
        async def probe_venues(self, configs: dict[str, object]) -> list[VenueAccountPreflight]:
            assert "extended" in configs
            assert "hyperliquid" in configs
            hyper_cfg = configs["hyperliquid"]
            if isinstance(hyper_cfg, dict):
                assert hyper_cfg["enabled"] is True
                credentials = hyper_cfg["credentials"]
                assert isinstance(credentials, dict)
                assert credentials["account_address"] == "0xhyper"
                assert credentials["api_wallet_private_key"] == "0xwallet"
            else:
                assert getattr(hyper_cfg, "enabled", None) is True
                credentials = getattr(hyper_cfg, "credentials", None)
                assert isinstance(credentials, dict)
                assert credentials.get("account_address") == "0xhyper"
                assert credentials.get("api_wallet_private_key") == "0xwallet"
            return [
                VenueAccountPreflight(
                    venue="extended",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_key",
                    account_identifier="ext-subaccount",
                    available_to_trade=1250.0,
                    balance_count=2,
                    position_count=1,
                ),
                VenueAccountPreflight(
                    venue="paradex",
                    enabled=True,
                    authenticated=False,
                    ready=False,
                    credential_mode="bearer_token",
                    missing_env_vars=["CARRYME_API_PARADEX_BEARER_TOKEN"],
                    blocking_reasons=[
                        (
                            "Venue paradex is missing required account credentials: "
                            "CARRYME_API_PARADEX_BEARER_TOKEN"
                        )
                    ],
                ),
            ]

    from carryme_api.app import get_account_preflight_service, get_api_settings

    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-secret",
        paradex_bearer_token=None,
        hyperliquid_live_enabled=True,
        hyperliquid_account_address="0xhyper",
        hyperliquid_api_wallet_private_key="0xwallet",
    )
    client = TestClient(app)
    response = client.get("/v1/executions/account-preflight/venues")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = {item["venue"]: item for item in response.json()}
    assert payload["extended"]["authenticated"] is True
    assert payload["paradex"]["authenticated"] is False


def test_account_preflight_for_saved_paper_trade_uses_service_dependency(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            assert paper_trade.entry_id is not None
            assert "paradex" in configs
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id,
                label=paper_trade.intent.label,
                ready=False,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=False,
                        ready=False,
                        credential_mode="bearer_token",
                        missing_env_vars=["CARRYME_API_PARADEX_BEARER_TOKEN"],
                        blocking_reasons=[
                            (
                                "Venue paradex is missing required account credentials: "
                                "CARRYME_API_PARADEX_BEARER_TOKEN"
                            )
                        ],
                    ),
                ],
                blocking_reasons=[
                    (
                        "Venue paradex is missing required account credentials: "
                        "CARRYME_API_PARADEX_BEARER_TOKEN"
                    )
                ],
            )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_paper_trade_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-secret",
        paradex_bearer_token=None,
    )
    client = TestClient(app)
    response = client.get(
        f"/v1/executions/account-preflight/from-paper-trade/{paper_trade.entry_id}"
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["ready"] is False
    assert {item["venue"] for item in payload["venues"]} == {"extended", "paradex"}


def test_order_preview_endpoint_returns_saved_paper_trade_preview(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )

    class StubOrderPreviewService:
        async def preview_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            *,
            slippage_tolerance_bps: int = 10,
        ) -> PaperTradeOrderPreview:
            return PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=slippage_tolerance_bps,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                        quantity=10_845.986984815618,
                        quantity_text="10845.98698482",
                        reference_price=0.0922,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.092292,
                        worst_price_text="0.09229200",
                        order_type="limit",
                        time_in_force="ioc",
                        post_only=False,
                        reduce_only=False,
                        http_method="POST",
                        endpoint_path_hint="/v1/orders",
                        required_auth_env_vars=["CARRYME_API_PARADEX_PRIVATE_KEY"],
                        auth_scheme="subkey private key",
                        payload={
                            "market": "ARB-USD-PERP",
                            "side": "BUY",
                        },
                        notes=[],
                    )
                ],
            )

    from carryme_api.app import get_order_preview_service, get_paper_trade_store

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_order_preview_service] = lambda: StubOrderPreviewService()
    client = TestClient(app)
    response = client.get(
        f"/v1/executions/preview/from-paper-trade/{paper_trade.entry_id}",
        params={"slippage_tolerance_bps": 12},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["slippage_tolerance_bps"] == 12
    assert payload["legs"][0]["venue"] == "paradex"
    assert payload["legs"][0]["endpoint_path_hint"] == "/v1/orders"


def test_order_preview_endpoint_returns_not_found_for_missing_trade(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")

    from carryme_api.app import get_paper_trade_store

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    client = TestClient(app)
    response = client.get("/v1/executions/preview/from-paper-trade/999")
    app.dependency_overrides.clear()

    assert response.status_code == 404
    assert response.json()["detail"] == "Paper trade 999 was not found"


def test_preview_confirmation_endpoint_persists_matching_preview(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )

    preview = PaperTradeOrderPreview(
        paper_trade_id=paper_trade.entry_id or 0,
        label="arb_extended_paradex",
        generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
        slippage_tolerance_bps=12,
        preview_hash="preview-hash",
        legs=[
            VenueOrderPreview(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="buy",
                target_notional=1000.0,
                quantity=10_845.0,
                quantity_text="10845.00000000",
                reference_price=0.0922,
                reference_price_source="best_ask",
                worst_acceptable_price=0.09231064,
                worst_price_text="0.09231064",
                order_type="limit",
                time_in_force="ioc",
                http_method="POST",
                endpoint_path_hint="/v1/orders",
                required_auth_env_vars=["CARRYME_API_PARADEX_PRIVATE_KEY"],
                auth_scheme="subkey private key",
                payload={"market": "ARB-USD-PERP"},
                notes=[],
            )
        ],
    )

    class StubOrderPreviewService:
        async def preview_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            *,
            slippage_tolerance_bps: int = 10,
        ) -> PaperTradeOrderPreview:
            assert slippage_tolerance_bps == 12
            return preview

    from carryme_api.app import (
        get_order_preview_service,
        get_paper_trade_store,
        get_preview_confirmation_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_order_preview_service] = lambda: StubOrderPreviewService()
    client = TestClient(app)
    response = client.post(
        f"/v1/executions/preview-confirmations/from-paper-trade/{paper_trade.entry_id}",
        json={
            "preview_hash": "preview-hash",
            "slippage_tolerance_bps": 12,
            "note": "operator confirmed",
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["preview_hash"] == "preview-hash"
    assert len(confirmation_store.list_recent(limit=10)) == 1


def test_preview_confirmation_endpoint_rejects_hash_mismatch(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )

    class StubOrderPreviewService:
        async def preview_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            *,
            slippage_tolerance_bps: int = 10,
        ) -> PaperTradeOrderPreview:
            return PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=slippage_tolerance_bps,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                        quantity=10_845.0,
                        quantity_text="10845.00000000",
                        reference_price=0.0922,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.09231064,
                        worst_price_text="0.09231064",
                        order_type="limit",
                        time_in_force="ioc",
                        http_method="POST",
                        endpoint_path_hint="/v1/orders",
                        required_auth_env_vars=["CARRYME_API_PARADEX_PRIVATE_KEY"],
                        auth_scheme="subkey private key",
                        payload={"market": "ARB-USD-PERP"},
                        notes=[],
                    )
                ],
            )

    from carryme_api.app import (
        get_order_preview_service,
        get_paper_trade_store,
        get_preview_confirmation_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_order_preview_service] = lambda: StubOrderPreviewService()
    client = TestClient(app)
    response = client.post(
        f"/v1/executions/preview-confirmations/from-paper-trade/{paper_trade.entry_id}",
        json={"preview_hash": "wrong-hash"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 409
    assert (
        response.json()["detail"] == "Preview hash did not match the current unsigned order preview"
    )


def test_preview_confirmations_endpoint_lists_saved_entries(tmp_path: Path) -> None:
    store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=7,
            label="arb_extended_paradex",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=7,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                        quantity=10_845.0,
                        quantity_text="10845.00000000",
                        reference_price=0.0922,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.09231064,
                        worst_price_text="0.09231064",
                        order_type="limit",
                        time_in_force="ioc",
                        http_method="POST",
                        endpoint_path_hint="/v1/orders",
                        required_auth_env_vars=["CARRYME_API_PARADEX_PRIVATE_KEY"],
                        auth_scheme="subkey private key",
                        payload={"market": "ARB-USD-PERP"},
                        notes=[],
                    )
                ],
            ),
            note="operator confirmed",
        )
    )

    from carryme_api.app import get_preview_confirmation_store

    app.dependency_overrides[get_preview_confirmation_store] = lambda: store
    client = TestClient(app)
    response = client.get("/v1/executions/preview-confirmations")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert len(payload) == 1
    assert payload[0]["paper_trade_id"] == 7


def test_execution_system_state_venues_endpoint_returns_probe_results() -> None:
    class StubSystemStateService:
        async def probe_venues(self, configs: dict[str, dict[str, bool]]) -> list[VenueSystemState]:
            assert configs["paradex"]["enabled"] is True
            return [
                VenueSystemState(
                    venue="paradex",
                    enabled=True,
                    checked=True,
                    healthy=True,
                    status="ok",
                )
            ]

    from carryme_api.app import get_api_settings

    app.dependency_overrides[get_system_state_service] = lambda: StubSystemStateService()
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(paradex_live_enabled=True)
    client = TestClient(app)
    response = client.get("/v1/executions/system-state/venues")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload == [
        {
            "venue": "paradex",
            "enabled": True,
            "checked": True,
            "healthy": True,
            "status": "ok",
            "blocking_reasons": [],
            "notes": [],
        }
    ]


def test_execution_system_state_for_paper_trade_endpoint_returns_status(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=11.0,
                capacity_fraction=0.25,
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
        )
    )

    class StubSystemStateService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, dict[str, bool]],
        ) -> PaperTradeSystemState:
            assert paper_trade.entry_id == 1
            assert configs["paradex"]["enabled"] is True
            return PaperTradeSystemState(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueSystemState(
                        venue="extended",
                        enabled=True,
                        checked=False,
                        healthy=True,
                    ),
                    VenueSystemState(
                        venue="paradex",
                        enabled=True,
                        checked=True,
                        healthy=True,
                        status="ok",
                    ),
                ],
                blocking_reasons=[],
            )

    from carryme_api.app import get_api_settings, get_paper_trade_store

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_system_state_service] = lambda: StubSystemStateService()
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        paradex_live_enabled=True,
    )
    client = TestClient(app)
    response = client.get(f"/v1/executions/system-state/from-paper-trade/{paper_trade.entry_id}")
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["ready"] is True
    assert payload["venues"][1]["status"] == "ok"


def test_live_submission_readiness_endpoint_combines_gates(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )
    confirmation_store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                        quantity=10_845.0,
                        quantity_text="10845.00000000",
                        reference_price=0.0922,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.09231064,
                        worst_price_text="0.09231064",
                        order_type="limit",
                        time_in_force="ioc",
                        http_method="POST",
                        endpoint_path_hint="/v1/orders",
                        required_auth_env_vars=[
                            "CARRYME_API_PARADEX_ACCOUNT_ADDRESS",
                            "CARRYME_API_PARADEX_PRIVATE_KEY",
                        ],
                        auth_scheme="main account address + subkey private key",
                        payload={"market": "ARB-USD-PERP"},
                        notes=[],
                    )
                ],
            ),
            note="operator confirmed",
        )
    )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=False,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=False,
                        ready=False,
                        credential_mode="bearer_token",
                        missing_env_vars=["CARRYME_API_PARADEX_BEARER_TOKEN"],
                        blocking_reasons=[
                            (
                                "Venue paradex is missing required account credentials: "
                                "CARRYME_API_PARADEX_BEARER_TOKEN"
                            )
                        ],
                    ),
                ],
                blocking_reasons=[
                    (
                        "Venue paradex is missing required account credentials: "
                        "CARRYME_API_PARADEX_BEARER_TOKEN"
                    )
                ],
            )

    class StubSystemStateService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, dict[str, bool]],
        ) -> PaperTradeSystemState:
            assert configs["paradex"]["enabled"] is True
            return PaperTradeSystemState(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueSystemState(
                        venue="extended",
                        enabled=True,
                        checked=False,
                        healthy=True,
                    ),
                    VenueSystemState(
                        venue="paradex",
                        enabled=True,
                        checked=True,
                        healthy=True,
                        status="ok",
                    ),
                ],
                blocking_reasons=[],
            )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_paper_trade_store,
        get_preview_confirmation_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_system_state_service] = lambda: StubSystemStateService()
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-private",
        paradex_bearer_token=None,
    )
    client = TestClient(app)
    response = client.get(
        f"/v1/executions/readiness/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "preview-hash"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["confirmed_preview"] is True
    assert payload["ready"] is False
    assert "CARRYME_API_PARADEX_BEARER_TOKEN" in str(payload["blocking_reasons"])


def test_live_submission_readiness_endpoint_blocks_zero_hyperliquid_collateral(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_hyperliquid",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00042,
                break_even_days_entry=0.63,
                capacity_limit_notional=126.83,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="hyperliquid",
                    symbol="ARB",
                    fee_profile="tier0",
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
        )
    )
    confirmation_store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_hyperliquid",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_hyperliquid",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="hyperliquid",
                        symbol="ARB",
                        fee_profile="tier0",
                        side="buy",
                        target_notional=11.0,
                        quantity=119.3,
                        quantity_text="119.3",
                        reference_price=0.0922,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.09229,
                        worst_price_text="0.09229",
                        order_type="limit",
                        time_in_force="ioc",
                        http_method="POST",
                        endpoint_path_hint="/exchange",
                        required_auth_env_vars=[
                            "CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS",
                            "CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
                        ],
                        auth_scheme="account address + API wallet private key",
                        payload={"coin": "ARB"},
                        notes=[],
                    )
                ],
            ),
            note="operator confirmed",
        )
    )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="hyperliquid",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_wallet",
                        total_collateral=0.0,
                        available_to_trade=0.0,
                        free_collateral=0.0,
                    ),
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        free_collateral=25.0,
                    ),
                ],
                blocking_reasons=[],
            )

    class StubSystemStateService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, dict[str, bool]],
        ) -> PaperTradeSystemState:
            return PaperTradeSystemState(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueSystemState(
                        venue="extended",
                        enabled=True,
                        checked=False,
                        healthy=True,
                    ),
                    VenueSystemState(
                        venue="hyperliquid",
                        enabled=True,
                        checked=False,
                        healthy=True,
                    ),
                ],
                blocking_reasons=[],
            )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_paper_trade_store,
        get_preview_confirmation_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_system_state_service] = lambda: StubSystemStateService()
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        hyperliquid_live_enabled=True,
        hyperliquid_account_address="0xhyper",
        hyperliquid_api_wallet_private_key="0xwallet",
    )
    client = TestClient(app)
    response = client.get(
        f"/v1/executions/readiness/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "preview-hash"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["confirmed_preview"] is True
    assert payload["ready"] is False
    assert payload["blocking_reasons"] == [
        "Venue hyperliquid has no usable collateral for the confirmed 11.00 notional preview"
    ]


def test_live_submission_readiness_endpoint_blocks_degraded_paradex_system_state(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=11.0,
                capacity_fraction=0.25,
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
        )
    )
    confirmation_store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro_fastfills",
                        side="buy",
                        target_notional=11.0,
                        quantity=120.0,
                        quantity_text="120.00000000",
                        reference_price=0.0915,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.0916,
                        worst_price_text="0.09160000",
                        order_type="limit",
                        time_in_force="ioc",
                        http_method="POST",
                        endpoint_path_hint="/v1/orders",
                        required_auth_env_vars=[
                            "CARRYME_API_PARADEX_ACCOUNT_ADDRESS",
                            "CARRYME_API_PARADEX_PRIVATE_KEY",
                        ],
                        auth_scheme="main account address + subkey private key",
                        payload={"market": "ARB-USD-PERP"},
                        notes=[],
                    )
                ],
            ),
            note="operator confirmed",
        )
    )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[],
                blocking_reasons=[],
            )

    class StubSystemStateService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, dict[str, bool]],
        ) -> PaperTradeSystemState:
            assert configs["paradex"]["enabled"] is True
            return PaperTradeSystemState(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=False,
                venues=[
                    VenueSystemState(
                        venue="extended",
                        enabled=True,
                        checked=False,
                        healthy=True,
                    ),
                    VenueSystemState(
                        venue="paradex",
                        enabled=True,
                        checked=True,
                        healthy=False,
                        status="maintenance",
                        blocking_reasons=["Paradex system state is maintenance"],
                    ),
                ],
                blocking_reasons=["Paradex system state is maintenance"],
            )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_paper_trade_store,
        get_preview_confirmation_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_system_state_service] = lambda: StubSystemStateService()
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-private",
    )
    client = TestClient(app)
    response = client.get(
        f"/v1/executions/readiness/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "preview-hash"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["ready"] is False
    assert payload["system_state"]["venues"][1]["status"] == "maintenance"
    assert "Paradex system state is maintenance" in payload["blocking_reasons"]


class _AllowAllRouteApprovalService:
    def require_live_approval(self, intent: FundingPairTradeIntent) -> RouteApprovalEntry:
        return RouteApprovalEntry(
            updated_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            label=intent.label,
            canonical_symbol=intent.canonical_symbol,
            short_venue=intent.short_leg.venue,
            long_venue=intent.long_leg.venue,
            short_fee_profile=intent.short_leg.fee_profile,
            long_fee_profile=intent.long_leg.fee_profile,
            approved=True,
            max_live_notional=max(intent.target_notional, 10_000.0),
            note="test allow",
        )


def test_paradex_live_execution_endpoint_submits_confirmed_preview(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )
    confirmation_store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                        effective_notional=999.99,
                        quantity=10845.9,
                        quantity_text="10845.90000000",
                        quantity_increment=0.1,
                        minimum_order_size=0.1,
                        minimum_notional=10.0,
                        reference_price=0.0922,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.0923,
                        worst_price_text="0.09230000",
                        price_increment=0.0001,
                        max_order_value=1_000_000.0,
                        order_type="limit",
                        time_in_force="ioc",
                        http_method="POST",
                        endpoint_path_hint="/v1/orders",
                        required_auth_env_vars=[
                            "CARRYME_API_PARADEX_ACCOUNT_ADDRESS",
                            "CARRYME_API_PARADEX_PRIVATE_KEY",
                        ],
                        auth_scheme="main account address + subkey private key",
                        payload={
                            "market": "ARB-USD-PERP",
                            "side": "BUY",
                            "type": "LIMIT",
                            "size": "10845.90000000",
                            "price": "0.09230000",
                            "instruction": "IOC",
                            "client_id": "carryme-pt7-paradex-buy",
                        },
                        notes=[],
                    )
                ],
            ),
            note="operator confirmed",
        )
    )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        free_collateral=25.0,
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        available_to_trade=25.0,
                    ),
                ],
                blocking_reasons=[],
            )

    class StubParadexLiveExecutionService:
        async def submit_confirmed_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: PreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
                adapter="paradex_live",
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
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                        status="submitted",
                        simulated=False,
                        external_reference="order-1",
                        request_payload={"market": "ARB-USD-PERP"},
                        response_payload={"id": "order-1", "status": "NEW"},
                        signature_timestamp_ms=1_700_000_000_000,
                    )
                ],
            )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_execution_journal_store,
        get_paper_trade_store,
        get_paradex_live_execution_service,
        get_preview_confirmation_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_paradex_live_execution_service] = (
        lambda: StubParadexLiveExecutionService()
    )
    app.dependency_overrides[get_route_approval_service] = lambda: _AllowAllRouteApprovalService()
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-private",
        paradex_bearer_token=None,
    )
    client = TestClient(app)
    response = client.post(
        f"/v1/executions/live/paradex/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "preview-hash"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["adapter"] == "paradex_live"
    assert payload["mode"] == "live"
    assert payload["status"] == "submitted"
    assert payload["preview_hash"] == "preview-hash"
    assert len(execution_store.list_recent(limit=10)) == 1


def test_extended_live_execution_endpoint_submits_confirmed_preview(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )
    confirmation_store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=1000.0,
                        effective_notional=999.96,
                        quantity=10881.0,
                        quantity_text="10881",
                        quantity_increment=1.0,
                        minimum_order_size=10.0,
                        minimum_notional=0.918,
                        reference_price=0.0919,
                        reference_price_source="best_bid",
                        worst_acceptable_price=0.0918,
                        worst_price_text="0.0918",
                        price_increment=0.0001,
                        max_order_value=1_250_000.0,
                        order_type="limit",
                        time_in_force="ioc",
                        http_method="POST",
                        endpoint_path_hint="/api/v1/user/order",
                        required_auth_env_vars=[
                            "CARRYME_API_EXTENDED_API_KEY",
                            "CARRYME_API_EXTENDED_STARK_PRIVATE_KEY",
                        ],
                        auth_scheme="api key + Stark signing key",
                        payload={
                            "symbol": "ARB-USD",
                            "side": "SELL",
                            "type": "LIMIT",
                            "size": "10881",
                            "price": "0.0918",
                            "time_in_force": "IOC",
                            "client_order_id": "carryme-pt8-extended-sell",
                            "reduce_only": False,
                        },
                        notes=[],
                    )
                ],
            ),
            note="operator confirmed",
        )
    )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        free_collateral=25.0,
                    )
                ],
                blocking_reasons=[],
            )

    class StubExtendedLiveExecutionService:
        async def submit_confirmed_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: PreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
                adapter="extended_live",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=1000.0,
                        status="submitted",
                        simulated=False,
                        external_reference="carryme-pt8-extended-sell",
                        request_payload={"market": "ARB-USD"},
                        response_payload={"id": 321, "externalId": "carryme-pt8-extended-sell"},
                    )
                ],
            )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_execution_journal_store,
        get_extended_live_execution_service,
        get_paper_trade_store,
        get_preview_confirmation_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_extended_live_execution_service] = (
        lambda: StubExtendedLiveExecutionService()
    )
    app.dependency_overrides[get_route_approval_service] = lambda: _AllowAllRouteApprovalService()
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=False,
    )
    client = TestClient(app)
    response = client.post(
        f"/v1/executions/live/extended/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "preview-hash"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["adapter"] == "extended_live"
    assert payload["mode"] == "live"
    assert payload["status"] == "submitted"
    assert payload["preview_hash"] == "preview-hash"
    assert len(execution_store.list_recent(limit=10)) == 1


def test_hyperliquid_live_execution_endpoint_submits_confirmed_preview(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_hyperliquid",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00065,
                break_even_days_entry=0.52,
                capacity_limit_notional=40.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="hyperliquid",
                    symbol="ARB",
                    fee_profile="tier0",
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
        )
    )
    confirmation_store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_hyperliquid",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_hyperliquid",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="hyperliquid",
                        symbol="ARB",
                        fee_profile="tier0",
                        side="buy",
                        target_notional=11.0,
                        effective_notional=10.99946,
                        quantity=119.3,
                        quantity_text="119.3",
                        quantity_increment=0.1,
                        minimum_order_size=0.1,
                        minimum_notional=10.0,
                        reference_price=0.0922,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.0923,
                        worst_price_text="0.0923",
                        order_type="limit",
                        time_in_force="ioc",
                        http_method="POST",
                        endpoint_path_hint="/exchange",
                        required_auth_env_vars=[
                            "CARRYME_API_HYPERLIQUID_ACCOUNT_ADDRESS",
                            "CARRYME_API_HYPERLIQUID_API_WALLET_PRIVATE_KEY",
                        ],
                        auth_scheme="account address + API wallet private key",
                        payload={
                            "coin": "ARB",
                            "is_buy": True,
                            "sz": "119.3",
                            "limit_px": "0.0923",
                            "order_type": {"limit": {"tif": "Ioc"}},
                            "reduce_only": False,
                            "client_order_id": "carryme-pt9-hyperliquid-buy",
                        },
                        notes=[],
                    )
                ],
            ),
            note="operator confirmed",
        )
    )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="hyperliquid",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_wallet",
                        available_to_trade=25.0,
                    ),
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        free_collateral=25.0,
                    ),
                ],
                blocking_reasons=[],
            )

    class StubHyperliquidLiveExecutionService:
        async def submit_confirmed_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: PreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
                adapter="hyperliquid_live",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue="hyperliquid",
                        symbol="ARB",
                        fee_profile="tier0",
                        side="buy",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="777",
                        request_payload={"coin": "ARB"},
                        response_payload={"status": "ok"},
                    )
                ],
            )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_execution_journal_store,
        get_hyperliquid_live_execution_service,
        get_paper_trade_store,
        get_preview_confirmation_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_hyperliquid_live_execution_service] = (
        lambda: StubHyperliquidLiveExecutionService()
    )
    app.dependency_overrides[get_route_approval_service] = lambda: _AllowAllRouteApprovalService()
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        hyperliquid_live_enabled=True,
        hyperliquid_account_address="0xhyper",
        hyperliquid_api_wallet_private_key="0xwallet",
    )
    client = TestClient(app)
    response = client.post(
        f"/v1/executions/live/hyperliquid/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "preview-hash"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["adapter"] == "hyperliquid_live"
    assert payload["mode"] == "live"
    assert payload["status"] == "submitted"
    assert payload["preview_hash"] == "preview-hash"
    assert len(execution_store.list_recent(limit=10)) == 1


def test_paired_live_execution_endpoint_submits_both_legs(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )
    confirmation_store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=1000.0,
                        effective_notional=999.96,
                        quantity=10881.0,
                        quantity_text="10881",
                        quantity_increment=1.0,
                        minimum_order_size=10.0,
                        minimum_notional=0.918,
                        reference_price=0.0919,
                        reference_price_source="best_bid",
                        worst_acceptable_price=0.0918,
                        worst_price_text="0.0918",
                        price_increment=0.0001,
                        max_order_value=1_250_000.0,
                        endpoint_path_hint="/api/v1/user/order",
                        auth_scheme="api key + Stark signing key",
                        payload={"symbol": "ARB-USD"},
                        notes=[],
                    ),
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                        effective_notional=999.99,
                        quantity=10845.9,
                        quantity_text="10845.90000000",
                        quantity_increment=0.1,
                        minimum_order_size=0.1,
                        minimum_notional=10.0,
                        reference_price=0.0922,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.0923,
                        worst_price_text="0.09230000",
                        price_increment=0.0001,
                        max_order_value=1_000_000.0,
                        endpoint_path_hint="/v1/orders",
                        auth_scheme="main account address + subkey private key",
                        payload={"market": "ARB-USD-PERP"},
                        notes=[],
                    ),
                ],
            ),
            note="operator confirmed",
        )
    )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        free_collateral=25.0,
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        available_to_trade=25.0,
                    ),
                ],
                blocking_reasons=[],
            )

    class StubPairedLiveExecutionCoordinator:
        async def submit_confirmed_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: PreviewConfirmationEntry,
            first_venue: str,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
                adapter=f"paired_live:{first_venue}_then_paradex",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=1000.0,
                        status="submitted",
                        simulated=False,
                        external_reference="extended-1",
                    ),
                    ExecutionLegResult(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                        status="submitted",
                        simulated=False,
                        external_reference="paradex-1",
                    ),
                ],
            )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_execution_journal_store,
        get_paired_live_execution_coordinator,
        get_paper_trade_store,
        get_preview_confirmation_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_paired_live_execution_coordinator] = (
        lambda: StubPairedLiveExecutionCoordinator()
    )
    app.dependency_overrides[get_route_approval_service] = lambda: _AllowAllRouteApprovalService()
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-private",
    )
    client = TestClient(app)
    response = client.post(
        f"/v1/executions/live/pair/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "preview-hash", "first_venue": "extended"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["adapter"] == "paired_live:extended_then_paradex"
    assert payload["status"] == "submitted"
    assert [leg["venue"] for leg in payload["legs"]] == ["extended", "paradex"]
    assert len(execution_store.list_recent(limit=10)) == 1


def test_paired_live_execution_endpoint_defaults_first_venue_to_auto(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=1000.0,
                capacity_fraction=0.25,
                max_target_notional=1000.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="buy",
                    target_notional=1000.0,
                ),
                short_leg=TradeLegIntent(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="sell",
                    target_notional=1000.0,
                ),
            ),
        )
    )
    confirmation_store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=1000.0,
                        effective_notional=999.96,
                        quantity=10881.0,
                        quantity_text="10881",
                        quantity_increment=1.0,
                        minimum_order_size=10.0,
                        minimum_notional=0.918,
                        reference_price=0.0919,
                        reference_price_source="best_bid",
                        worst_acceptable_price=0.0918,
                        worst_price_text="0.0918",
                        price_increment=0.0001,
                        max_order_value=1_250_000.0,
                        endpoint_path_hint="/api/v1/user/order",
                        auth_scheme="api key + Stark signing key",
                        payload={"symbol": "ARB-USD"},
                        notes=[],
                    ),
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                        effective_notional=999.99,
                        quantity=10845.9,
                        quantity_text="10845.90000000",
                        quantity_increment=0.1,
                        minimum_order_size=0.1,
                        minimum_notional=10.0,
                        reference_price=0.0922,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.0923,
                        worst_price_text="0.09230000",
                        price_increment=0.0001,
                        max_order_value=1_000_000.0,
                        endpoint_path_hint="/v1/orders",
                        auth_scheme="main account address + subkey private key",
                        payload={"market": "ARB-USD-PERP"},
                        notes=[],
                    ),
                ],
            ),
            note="operator confirmed",
        )
    )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        free_collateral=25.0,
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        available_to_trade=25.0,
                    ),
                ],
                blocking_reasons=[],
            )

    class StubPairedLiveExecutionCoordinator:
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
                executed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
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
                        fee_profile="pro",
                        side="buy",
                        target_notional=1000.0,
                        status="submitted",
                        simulated=False,
                        external_reference="paradex-1",
                    ),
                    ExecutionLegResult(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=1000.0,
                        status="submitted",
                        simulated=False,
                        external_reference="extended-1",
                    ),
                ],
            )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_execution_journal_store,
        get_paired_live_execution_coordinator,
        get_paper_trade_store,
        get_preview_confirmation_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_paired_live_execution_coordinator] = (
        lambda: StubPairedLiveExecutionCoordinator()
    )
    app.dependency_overrides[get_route_approval_service] = lambda: _AllowAllRouteApprovalService()
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-private",
    )
    client = TestClient(app)
    response = client.post(
        f"/v1/executions/live/pair/from-paper-trade/{paper_trade.entry_id}",
        params={"preview_hash": "preview-hash"},
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["adapter"] == "paired_live:paradex_then_extended"
    assert [leg["venue"] for leg in payload["legs"]] == ["paradex", "extended"]


def test_guarded_paired_live_execution_endpoint_auto_cleans_open_leg(tmp_path: Path) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    observation_store = ExecutionObservationStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    confirmation_store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                        effective_notional=10.95,
                        quantity=123.0,
                        quantity_text="123",
                        quantity_increment=1.0,
                        minimum_order_size=10.0,
                        minimum_notional=0.918,
                        reference_price=0.0890,
                        reference_price_source="best_bid",
                        worst_acceptable_price=0.0889,
                        worst_price_text="0.0889",
                        price_increment=0.0001,
                        max_order_value=1_250_000.0,
                        endpoint_path_hint="/api/v1/user/order",
                        auth_scheme="api key + Stark signing key",
                        payload={"symbol": "ARB-USD"},
                        notes=[],
                    ),
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                        effective_notional=10.99,
                        quantity=123.1,
                        quantity_text="123.10000000",
                        quantity_increment=0.1,
                        minimum_order_size=0.1,
                        minimum_notional=10.0,
                        reference_price=0.0892,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.0893,
                        worst_price_text="0.08930000",
                        price_increment=0.0001,
                        max_order_value=1_000_000.0,
                        endpoint_path_hint="/v1/orders",
                        auth_scheme="main account address + subkey private key",
                        payload={"market": "ARB-USD-PERP"},
                        notes=[],
                    ),
                ],
            ),
            note="operator confirmed",
        )
    )

    class StubAccountPreflightService:
        def __init__(self) -> None:
            self.probe_paper_trade_calls = 0

        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            self.probe_paper_trade_calls += 1
            if self.probe_paper_trade_calls <= 2:
                positions = {"extended": [], "paradex": ["ARB-USD-PERP"]}
            else:
                positions = {"extended": [], "paradex": []}
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        free_collateral=25.0,
                        position_symbols=positions["extended"],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        available_to_trade=25.0,
                        position_symbols=positions["paradex"],
                    ),
                ],
                blocking_reasons=[],
            )

        async def probe_venues(self, configs: object) -> list[VenueAccountPreflight]:
            return [
                VenueAccountPreflight(
                    venue="extended",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_key",
                    free_collateral=25.0,
                ),
                VenueAccountPreflight(
                    venue="paradex",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="subkey_jwt",
                    available_to_trade=25.0,
                ),
            ]

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        external_reference="extended-1",
                        derived_state="unknown",
                    ),
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        external_reference="paradex-1",
                        derived_state="filled",
                    ),
                ],
            )

    class StubPairedLiveExecutionCoordinator:
        async def submit_confirmed_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: PreviewConfirmationEntry,
            first_venue: str,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
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
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="paradex-1",
                    ),
                    ExecutionLegResult(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="extended-1",
                    ),
                ],
            )

    class StubCleanupPreviewService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
        ) -> ExecutionCleanupPreview:
            assert pair_status.recommended_action == "complete_or_unwind_missing_leg"
            return ExecutionCleanupPreview(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                generated_at=datetime(2026, 3, 29, 13, 16, tzinfo=UTC),
                preview_hash="cleanup-hash",
                reason="complete_or_unwind_missing_leg",
                leg=VenueOrderPreview(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
                    side="sell",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=123.1,
                    quantity_text="123.10000000",
                    reference_price=0.0892,
                    reference_price_source="best_bid",
                    worst_acceptable_price=0.0891,
                    worst_price_text="0.08910000",
                    reduce_only=True,
                    endpoint_path_hint="/v1/orders",
                    auth_scheme="main account address + subkey private key",
                    payload={"market": "ARB-USD-PERP", "reduce_only": True},
                    notes=[],
                ),
                notes=[],
            )

    class StubCleanupLiveExecutionRouter:
        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            assert confirmation.preview.leg.venue == "paradex"
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 16, tzinfo=UTC),
                adapter="paradex_cleanup_live",
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
                        fee_profile="pro",
                        side="sell",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="cleanup-1",
                    )
                ],
            )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_cleanup_live_execution_router,
        get_cleanup_preview_confirmation_store,
        get_cleanup_preview_service,
        get_execution_journal_store,
        get_execution_observation_store,
        get_execution_order_state_service,
        get_paired_live_execution_coordinator,
        get_paper_trade_store,
        get_preview_confirmation_store,
    )

    account_service = StubAccountPreflightService()
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_cleanup_preview_confirmation_store] = (
        lambda: cleanup_confirmation_store
    )
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_account_preflight_service] = lambda: account_service
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_paired_live_execution_coordinator] = (
        lambda: StubPairedLiveExecutionCoordinator()
    )
    app.dependency_overrides[get_route_approval_service] = lambda: _AllowAllRouteApprovalService()
    app.dependency_overrides[get_cleanup_preview_service] = lambda: StubCleanupPreviewService()
    app.dependency_overrides[get_cleanup_live_execution_router] = (
        lambda: StubCleanupLiveExecutionRouter()
    )
    app.dependency_overrides[get_execution_observation_store] = lambda: observation_store
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-private",
    )
    client = TestClient(app)
    response = client.post(
        f"/v1/executions/live/pair/guarded/from-paper-trade/{paper_trade.entry_id}",
        params={
            "preview_hash": "preview-hash",
            "first_venue": "paradex",
            "poll_attempts": 2,
            "poll_interval_seconds": 0,
            "auto_cleanup": "true",
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["paper_trade_id"] == paper_trade.entry_id
    assert payload["primary_execution"]["adapter"] == "paired_live:paradex_then_extended"
    assert payload["cleanup_execution"]["adapter"] == "paradex_cleanup_live"
    cleanup_confirmations = cleanup_confirmation_store.list_recent(limit=10)
    assert len(cleanup_confirmations) == 1
    assert cleanup_confirmations[0].preview_hash == "cleanup-hash"
    assert cleanup_confirmations[0].note == "guarded pair auto-cleanup"
    assert cleanup_confirmations[0].preview.reason == "complete_or_unwind_missing_leg"
    assert cleanup_confirmations[0].preview.leg.venue == "paradex"
    saved_executions = sorted(
        execution_store.list_recent(limit=10),
        key=lambda entry: entry.adapter,
    )
    assert [entry.adapter for entry in saved_executions] == [
        "paired_live:paradex_then_extended",
        "paradex_cleanup_live",
    ]
    assert saved_executions[1].preview_hash == "cleanup-hash"
    assert saved_executions[1].confirmation_entry_id == cleanup_confirmations[0].entry_id
    assert saved_executions[0].preview_hash == "preview-hash"
    assert paper_trade.entry_id is not None
    observations = [
        entry
        for entry in observation_store.list_recent(limit=10)
        if entry.paper_trade_id == paper_trade.entry_id
    ]
    assert len(observations) >= 2
    assert {entry.execution_entry_id for entry in observations} >= {
        saved_executions[0].entry_id,
        saved_executions[1].entry_id,
    }
    assert {entry.preview_hash for entry in observations} >= {
        "preview-hash",
        "cleanup-hash",
    }
    latest_observation = observation_store.latest_for_paper_trade(paper_trade.entry_id)
    assert latest_observation is not None
    assert latest_observation.execution_entry_id == saved_executions[1].entry_id
    assert latest_observation.preview_hash == "cleanup-hash"


def test_guarded_paired_live_execution_endpoint_reuses_existing_cleanup_confirmation(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    confirmation_store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                        effective_notional=10.95,
                        quantity=123.0,
                        quantity_text="123",
                        quantity_increment=1.0,
                        minimum_order_size=10.0,
                        minimum_notional=0.918,
                        reference_price=0.0890,
                        reference_price_source="best_bid",
                        worst_acceptable_price=0.0889,
                        worst_price_text="0.0889",
                        price_increment=0.0001,
                        max_order_value=1_250_000.0,
                        endpoint_path_hint="/api/v1/user/order",
                        auth_scheme="api key + Stark signing key",
                        payload={"symbol": "ARB-USD"},
                        notes=[],
                    ),
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                        effective_notional=10.99,
                        quantity=123.1,
                        quantity_text="123.10000000",
                        quantity_increment=0.1,
                        minimum_order_size=0.1,
                        minimum_notional=10.0,
                        reference_price=0.0892,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.0893,
                        worst_price_text="0.08930000",
                        price_increment=0.0001,
                        max_order_value=1_000_000.0,
                        endpoint_path_hint="/v1/orders",
                        auth_scheme="main account address + subkey private key",
                        payload={"market": "ARB-USD-PERP"},
                        notes=[],
                    ),
                ],
            ),
            note="operator confirmed",
        )
    )
    existing_cleanup = cleanup_confirmation_store.append(
        CleanupPreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 16, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="cleanup-hash",
            preview=ExecutionCleanupPreview(
                execution_entry_id=1,
                paper_trade_id=paper_trade.entry_id or 0,
                generated_at=datetime(2026, 3, 29, 13, 16, tzinfo=UTC),
                preview_hash="cleanup-hash",
                reason="close_open_leg",
                leg=VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=123.0,
                    quantity_text="123",
                    reference_price=0.0891,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0892,
                    worst_price_text="0.0892",
                    reduce_only=True,
                    endpoint_path_hint="/api/v1/user/order",
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD", "reduce_only": True},
                    notes=[],
                ),
                notes=[],
            ),
            note="guarded pair auto-cleanup",
        )
    )

    class StubAccountPreflightService:
        def __init__(self) -> None:
            self.probe_paper_trade_calls = 0

        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            self.probe_paper_trade_calls += 1
            if self.probe_paper_trade_calls <= 2:
                positions = {"extended": ["ARB-USD"], "paradex": []}
            else:
                positions = {"extended": [], "paradex": []}
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        free_collateral=25.0,
                        position_symbols=positions["extended"],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        available_to_trade=25.0,
                        position_symbols=positions["paradex"],
                    ),
                ],
                blocking_reasons=[],
            )

        async def probe_venues(self, configs: object) -> list[VenueAccountPreflight]:
            return [
                VenueAccountPreflight(
                    venue="extended",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_key",
                    free_collateral=25.0,
                ),
                VenueAccountPreflight(
                    venue="paradex",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="subkey_jwt",
                    available_to_trade=25.0,
                ),
            ]

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        external_reference="extended-1",
                        derived_state="unknown",
                    ),
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        external_reference="paradex-1",
                        derived_state="unfilled",
                    ),
                ],
            )

    class StubPairedLiveExecutionCoordinator:
        async def submit_confirmed_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: PreviewConfirmationEntry,
            first_venue: str,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
                adapter=f"paired_live:{first_venue}_then_paradex",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="extended-1",
                    ),
                    ExecutionLegResult(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="paradex-1",
                    ),
                ],
            )

    class StubCleanupPreviewService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
        ) -> ExecutionCleanupPreview:
            return existing_cleanup.preview

    class StubCleanupLiveExecutionRouter:
        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            assert confirmation.entry_id == existing_cleanup.entry_id
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 16, tzinfo=UTC),
                adapter="extended_cleanup_live",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="buy",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="cleanup-1",
                    )
                ],
            )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_cleanup_live_execution_router,
        get_cleanup_preview_confirmation_store,
        get_cleanup_preview_service,
        get_execution_journal_store,
        get_execution_order_state_service,
        get_paired_live_execution_coordinator,
        get_paper_trade_store,
        get_preview_confirmation_store,
        get_route_approval_service,
    )

    account_service = StubAccountPreflightService()
    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_cleanup_preview_confirmation_store] = (
        lambda: cleanup_confirmation_store
    )
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_account_preflight_service] = lambda: account_service
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_paired_live_execution_coordinator] = (
        lambda: StubPairedLiveExecutionCoordinator()
    )
    app.dependency_overrides[get_route_approval_service] = lambda: _AllowAllRouteApprovalService()
    app.dependency_overrides[get_cleanup_preview_service] = lambda: StubCleanupPreviewService()
    app.dependency_overrides[get_cleanup_live_execution_router] = (
        lambda: StubCleanupLiveExecutionRouter()
    )
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-private",
    )
    client = TestClient(app)
    response = client.post(
        f"/v1/executions/live/pair/guarded/from-paper-trade/{paper_trade.entry_id}",
        params={
            "preview_hash": "preview-hash",
            "first_venue": "extended",
            "poll_attempts": 2,
            "poll_interval_seconds": 0,
            "auto_cleanup": "true",
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    assert len(cleanup_confirmation_store.list_recent(limit=10)) == 1
    assert len(execution_store.list_recent(limit=10)) == 2


def test_guarded_paired_live_execution_endpoint_returns_existing_cleanup_execution(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    confirmation_store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                        effective_notional=10.95,
                        quantity=123.0,
                        quantity_text="123",
                        quantity_increment=1.0,
                        minimum_order_size=10.0,
                        minimum_notional=0.918,
                        reference_price=0.0890,
                        reference_price_source="best_bid",
                        worst_acceptable_price=0.0889,
                        worst_price_text="0.0889",
                        price_increment=0.0001,
                        max_order_value=1_250_000.0,
                        endpoint_path_hint="/api/v1/user/order",
                        auth_scheme="api key + Stark signing key",
                        payload={"symbol": "ARB-USD"},
                        notes=[],
                    ),
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                        effective_notional=10.99,
                        quantity=123.1,
                        quantity_text="123.10000000",
                        quantity_increment=0.1,
                        minimum_order_size=0.1,
                        minimum_notional=10.0,
                        reference_price=0.0892,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.0893,
                        worst_price_text="0.08930000",
                        price_increment=0.0001,
                        max_order_value=1_000_000.0,
                        endpoint_path_hint="/v1/orders",
                        auth_scheme="main account address + subkey private key",
                        payload={"market": "ARB-USD-PERP"},
                        notes=[],
                    ),
                ],
            ),
            note="operator confirmed",
        )
    )
    existing_cleanup = cleanup_confirmation_store.append(
        CleanupPreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 16, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="cleanup-hash",
            preview=ExecutionCleanupPreview(
                execution_entry_id=1,
                paper_trade_id=paper_trade.entry_id or 0,
                generated_at=datetime(2026, 3, 29, 13, 16, tzinfo=UTC),
                preview_hash="cleanup-hash",
                reason="close_open_leg",
                leg=VenueOrderPreview(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    effective_notional=11.0,
                    quantity=123.0,
                    quantity_text="123",
                    reference_price=0.0891,
                    reference_price_source="best_ask",
                    worst_acceptable_price=0.0892,
                    worst_price_text="0.0892",
                    reduce_only=True,
                    endpoint_path_hint="/api/v1/user/order",
                    auth_scheme="api key + Stark signing key",
                    payload={"symbol": "ARB-USD", "reduce_only": True},
                    notes=[],
                ),
                notes=[],
            ),
            note="guarded pair auto-cleanup",
        )
    )
    existing_cleanup_execution = execution_store.append(
        ExecutionJournalEntry(
            executed_at=datetime(2026, 3, 29, 13, 16, tzinfo=UTC),
            adapter="extended_cleanup_live",
            mode="live",
            status="submitted",
            paper_trade_id=paper_trade.entry_id,
            preview_hash=existing_cleanup.preview_hash,
            confirmation_entry_id=existing_cleanup.entry_id,
            paper_trade=paper_trade,
            legs=[
                ExecutionLegResult(
                    venue="extended",
                    symbol="ARB-USD",
                    fee_profile="default",
                    side="buy",
                    target_notional=11.0,
                    status="submitted",
                    simulated=False,
                    external_reference="cleanup-1",
                )
            ],
        )
    )
    assert existing_cleanup.entry_id is not None
    assert existing_cleanup_execution.entry_id is not None
    assert execution_store.reserve_live_submission(
        confirmation_entry_id=existing_cleanup.entry_id,
        preview_hash=existing_cleanup.preview_hash,
    )
    execution_store.mark_live_submission_completed(
        confirmation_entry_id=existing_cleanup.entry_id,
        preview_hash=existing_cleanup.preview_hash,
        execution_entry_id=existing_cleanup_execution.entry_id,
    )

    class StubAccountPreflightService:
        def __init__(self) -> None:
            self.probe_paper_trade_calls = 0

        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            self.probe_paper_trade_calls += 1
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        free_collateral=25.0,
                        position_symbols=["ARB-USD"],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        available_to_trade=25.0,
                        position_symbols=[],
                    ),
                ],
                blocking_reasons=[],
            )

        async def probe_venues(self, configs: object) -> list[VenueAccountPreflight]:
            return [
                VenueAccountPreflight(
                    venue="extended",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="api_key",
                    free_collateral=25.0,
                ),
                VenueAccountPreflight(
                    venue="paradex",
                    enabled=True,
                    authenticated=True,
                    ready=True,
                    credential_mode="subkey_jwt",
                    available_to_trade=25.0,
                ),
            ]

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        external_reference="extended-1",
                        derived_state="unknown",
                    ),
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        external_reference="paradex-1",
                        derived_state="unfilled",
                    ),
                ],
            )

    class StubPairedLiveExecutionCoordinator:
        async def submit_confirmed_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: PreviewConfirmationEntry,
            first_venue: str,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
                adapter=f"paired_live:{first_venue}_then_paradex",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="extended-1",
                    ),
                    ExecutionLegResult(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="paradex-1",
                    ),
                ],
            )

    class StubCleanupPreviewService:
        async def preview_from_execution(
            self,
            *,
            entry: ExecutionJournalEntry,
            pair_status: ExecutionPairStatus,
            slippage_tolerance_bps: int = 10,
        ) -> ExecutionCleanupPreview:
            return existing_cleanup.preview

    class StubCleanupLiveExecutionRouter:
        async def submit_confirmed_cleanup_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: CleanupPreviewConfirmationEntry,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            raise AssertionError("cleanup live router should not submit a duplicate execution")

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_cleanup_live_execution_router,
        get_cleanup_preview_confirmation_store,
        get_cleanup_preview_service,
        get_execution_journal_store,
        get_execution_order_state_service,
        get_paired_live_execution_coordinator,
        get_paper_trade_store,
        get_preview_confirmation_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_cleanup_preview_confirmation_store] = (
        lambda: cleanup_confirmation_store
    )
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_execution_order_state_service] = (
        lambda: StubExecutionOrderStateService()
    )
    app.dependency_overrides[get_paired_live_execution_coordinator] = (
        lambda: StubPairedLiveExecutionCoordinator()
    )
    app.dependency_overrides[get_route_approval_service] = lambda: _AllowAllRouteApprovalService()
    app.dependency_overrides[get_cleanup_preview_service] = lambda: StubCleanupPreviewService()
    app.dependency_overrides[get_cleanup_live_execution_router] = (
        lambda: StubCleanupLiveExecutionRouter()
    )
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-private",
    )
    client = TestClient(app)
    response = client.post(
        f"/v1/executions/live/pair/guarded/from-paper-trade/{paper_trade.entry_id}",
        params={
            "preview_hash": "preview-hash",
            "first_venue": "extended",
            "poll_attempts": 2,
            "poll_interval_seconds": 0,
            "auto_cleanup": "true",
        },
    )
    app.dependency_overrides.clear()

    assert response.status_code == 200
    payload = response.json()
    assert payload["cleanup_execution"]["entry_id"] == existing_cleanup_execution.entry_id
    assert payload["cleanup_execution"]["adapter"] == "extended_cleanup_live"
    assert (
        "Existing cleanup execution reused for this confirmation" in payload["pair_status"]["notes"]
    )
    assert len(cleanup_confirmation_store.list_recent(limit=10)) == 1
    saved_executions = execution_store.list_recent(limit=10)
    assert len(saved_executions) == 2
    cleanup_entries = [
        entry
        for entry in saved_executions
        if entry.confirmation_entry_id == existing_cleanup.entry_id
        and entry.preview_hash == existing_cleanup.preview_hash
    ]
    assert len(cleanup_entries) == 1
    assert cleanup_entries[0].entry_id == existing_cleanup_execution.entry_id


def test_guarded_paired_live_execution_endpoint_rejects_duplicate_retry(
    tmp_path: Path,
) -> None:
    paper_store = PaperTradeStore(tmp_path / "history.sqlite3")
    confirmation_store = PreviewConfirmationStore(tmp_path / "history.sqlite3")
    cleanup_confirmation_store = CleanupPreviewConfirmationStore(tmp_path / "history.sqlite3")
    execution_store = ExecutionJournalStore(tmp_path / "history.sqlite3")
    paper_trade = paper_store.append(
        PaperTradeEntry(
            created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
            note="operator accepted candidate",
            intent=FundingPairTradeIntent(
                label="arb_extended_paradex",
                canonical_symbol="ARB-USD-PERP",
                source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
                one_day_net_edge_after_entry=0.00055,
                break_even_days_entry=0.45,
                capacity_limit_notional=4500.0,
                target_notional=11.0,
                capacity_fraction=0.25,
                max_target_notional=11.0,
                long_leg=TradeLegIntent(
                    venue="paradex",
                    symbol="ARB-USD-PERP",
                    fee_profile="pro",
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
        )
    )
    confirmation_store.append(
        PreviewConfirmationEntry(
            confirmed_at=datetime(2026, 3, 29, 13, 10, tzinfo=UTC),
            paper_trade_id=paper_trade.entry_id or 0,
            label="arb_extended_paradex",
            preview_hash="preview-hash",
            preview=PaperTradeOrderPreview(
                paper_trade_id=paper_trade.entry_id or 0,
                label="arb_extended_paradex",
                generated_at=datetime(2026, 3, 29, 13, 5, tzinfo=UTC),
                slippage_tolerance_bps=12,
                preview_hash="preview-hash",
                legs=[
                    VenueOrderPreview(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                        effective_notional=10.95,
                        quantity=123.0,
                        quantity_text="123",
                        quantity_increment=1.0,
                        minimum_order_size=10.0,
                        minimum_notional=0.918,
                        reference_price=0.0890,
                        reference_price_source="best_bid",
                        worst_acceptable_price=0.0889,
                        worst_price_text="0.0889",
                        price_increment=0.0001,
                        max_order_value=1_250_000.0,
                        endpoint_path_hint="/api/v1/user/order",
                        auth_scheme="api key + Stark signing key",
                        payload={"symbol": "ARB-USD"},
                        notes=[],
                    ),
                    VenueOrderPreview(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                        effective_notional=10.99,
                        quantity=123.1,
                        quantity_text="123.10000000",
                        quantity_increment=0.1,
                        minimum_order_size=0.1,
                        minimum_notional=10.0,
                        reference_price=0.0892,
                        reference_price_source="best_ask",
                        worst_acceptable_price=0.0893,
                        worst_price_text="0.08930000",
                        price_increment=0.0001,
                        max_order_value=1_000_000.0,
                        endpoint_path_hint="/v1/orders",
                        auth_scheme="main account address + subkey private key",
                        payload={"market": "ARB-USD-PERP"},
                        notes=[],
                    ),
                ],
            ),
            note="operator confirmed",
        )
    )

    class StubAccountPreflightService:
        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        free_collateral=25.0,
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        available_to_trade=25.0,
                    ),
                ],
                blocking_reasons=[],
            )

    class StubPairedLiveExecutionCoordinator:
        async def submit_confirmed_preview(
            self,
            *,
            paper_trade: PaperTradeEntry,
            confirmation: PreviewConfirmationEntry,
            first_venue: str,
            executed_at: datetime | None = None,
        ) -> ExecutionJournalEntry:
            return ExecutionJournalEntry(
                executed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
                adapter=f"paired_live:{first_venue}_then_paradex",
                mode="live",
                status="submitted",
                paper_trade_id=paper_trade.entry_id,
                preview_hash=confirmation.preview_hash,
                confirmation_entry_id=confirmation.entry_id,
                paper_trade=paper_trade,
                legs=[
                    ExecutionLegResult(
                        venue="extended",
                        symbol="ARB-USD",
                        fee_profile="default",
                        side="sell",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="extended-1",
                    ),
                    ExecutionLegResult(
                        venue="paradex",
                        symbol="ARB-USD-PERP",
                        fee_profile="pro",
                        side="buy",
                        target_notional=11.0,
                        status="submitted",
                        simulated=False,
                        external_reference="paradex-1",
                    ),
                ],
            )

    from carryme_api.app import (
        get_account_preflight_service,
        get_api_settings,
        get_cleanup_preview_confirmation_store,
        get_execution_journal_store,
        get_paired_live_execution_coordinator,
        get_paper_trade_store,
        get_preview_confirmation_store,
    )

    app.dependency_overrides[get_paper_trade_store] = lambda: paper_store
    app.dependency_overrides[get_preview_confirmation_store] = lambda: confirmation_store
    app.dependency_overrides[get_cleanup_preview_confirmation_store] = (
        lambda: cleanup_confirmation_store
    )
    app.dependency_overrides[get_execution_journal_store] = lambda: execution_store
    app.dependency_overrides[get_account_preflight_service] = lambda: StubAccountPreflightService()
    app.dependency_overrides[get_paired_live_execution_coordinator] = (
        lambda: StubPairedLiveExecutionCoordinator()
    )
    app.dependency_overrides[get_route_approval_service] = lambda: _AllowAllRouteApprovalService()
    app.dependency_overrides[get_api_settings] = lambda: ApiSettings(
        extended_live_enabled=True,
        extended_api_key="extended-key",
        extended_stark_private_key="extended-stark",
        paradex_live_enabled=True,
        paradex_account_address="0xabc",
        paradex_private_key="paradex-private",
    )
    client = TestClient(app)
    first = client.post(
        f"/v1/executions/live/pair/guarded/from-paper-trade/{paper_trade.entry_id}",
        params={
            "preview_hash": "preview-hash",
            "first_venue": "extended",
            "poll_attempts": 1,
            "poll_interval_seconds": 0,
            "auto_cleanup": "false",
        },
    )
    second = client.post(
        f"/v1/executions/live/pair/guarded/from-paper-trade/{paper_trade.entry_id}",
        params={
            "preview_hash": "preview-hash",
            "first_venue": "extended",
            "poll_attempts": 1,
            "poll_interval_seconds": 0,
            "auto_cleanup": "false",
        },
    )
    app.dependency_overrides.clear()

    assert first.status_code == 200
    assert second.status_code == 409
    assert second.json()["detail"]["adapter"] == "paired_live:extended_then_paradex"
    saved_executions = [
        entry
        for entry in execution_store.list_recent(limit=10)
        if entry.paper_trade_id == paper_trade.entry_id
    ]
    assert len(saved_executions) == 1
    assert cleanup_confirmation_store.list_recent(limit=10) == []


def test_observe_pair_status_retries_after_probe_timeout(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(app_module, "PAIR_STATUS_POLL_CALL_TIMEOUT_SECONDS", 0.01)

    paper_trade = PaperTradeEntry(
        entry_id=7,
        created_at=datetime(2026, 3, 29, 13, 0, tzinfo=UTC),
        note="operator accepted candidate",
        intent=FundingPairTradeIntent(
            label="arb_extended_paradex",
            canonical_symbol="ARB-USD-PERP",
            source_recorded_at=datetime(2026, 3, 29, 12, 55, tzinfo=UTC),
            one_day_net_edge_after_entry=0.00055,
            break_even_days_entry=0.45,
            capacity_limit_notional=4500.0,
            target_notional=11.0,
            capacity_fraction=0.25,
            max_target_notional=11.0,
            long_leg=TradeLegIntent(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
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
    )
    execution = ExecutionJournalEntry(
        entry_id=1,
        executed_at=datetime(2026, 3, 29, 13, 15, tzinfo=UTC),
        adapter="paired_live:extended_then_paradex",
        mode="live",
        status="submitted",
        paper_trade_id=7,
        preview_hash="preview-hash",
        confirmation_entry_id=3,
        paper_trade=paper_trade,
        legs=[
            ExecutionLegResult(
                venue="extended",
                symbol="ARB-USD",
                fee_profile="default",
                side="sell",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="extended-1",
            ),
            ExecutionLegResult(
                venue="paradex",
                symbol="ARB-USD-PERP",
                fee_profile="pro",
                side="buy",
                target_notional=11.0,
                status="submitted",
                simulated=False,
                external_reference="paradex-1",
            ),
        ],
    )

    class SlowAccountPreflightService:
        def __init__(self) -> None:
            self.calls = 0

        async def probe_paper_trade(
            self,
            paper_trade: PaperTradeEntry,
            configs: dict[str, object],
        ) -> PaperTradeAccountPreflight:
            self.calls += 1
            if self.calls == 1:
                await asyncio.sleep(0.05)
            return PaperTradeAccountPreflight(
                paper_trade_id=paper_trade.entry_id or 0,
                label=paper_trade.intent.label,
                ready=True,
                venues=[
                    VenueAccountPreflight(
                        venue="extended",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="api_key",
                        position_symbols=[],
                    ),
                    VenueAccountPreflight(
                        venue="paradex",
                        enabled=True,
                        authenticated=True,
                        ready=True,
                        credential_mode="subkey_jwt",
                        position_symbols=[],
                    ),
                ],
                blocking_reasons=[],
            )

    class StubExecutionOrderStateService:
        async def observe_execution(self, entry: ExecutionJournalEntry) -> ExecutionOrderState:
            return ExecutionOrderState(
                execution_entry_id=entry.entry_id,
                paper_trade_id=entry.paper_trade_id,
                preview_hash=entry.preview_hash,
                legs=[
                    ExecutionLegOrderState(
                        venue="extended",
                        supported=True,
                        external_reference="extended-1",
                        derived_state="filled",
                    ),
                    ExecutionLegOrderState(
                        venue="paradex",
                        supported=True,
                        external_reference="paradex-1",
                        derived_state="filled",
                    ),
                ],
            )

    async def run() -> None:
        account_service = SlowAccountPreflightService()
        status = await app_module._observe_pair_status_for_execution(
            paper_trade=paper_trade,
            execution=execution,
            settings=ApiSettings(),
            account_service=cast(Any, account_service),
            order_state_service=cast(Any, StubExecutionOrderStateService()),
            poll_attempts=2,
            poll_interval_seconds=0,
        )
        assert status.paper_trade_id == 7
        assert account_service.calls == 2

    asyncio.run(run())
