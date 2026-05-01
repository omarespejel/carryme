"""Route approval helpers for live-money execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TypedDict

from carryme_models import (
    ApprovedCanaryBasketEntry,
    ApprovedCanaryBasketPlan,
    FundingPairTradeIntent,
    FundingUniverseCanaryCandidate,
    FundingUniverseOpportunity,
    RouteApprovalEntry,
    RouteApprovalUpsert,
)
from carryme_storage import RouteApprovalStore

from carryme_runtime.universe import (
    OpportunityUniverseService,
    build_pair_spec_from_universe_opportunity,
)


class _ScaledCandidatePnl(TypedDict):
    entry: float
    round_trip: float
    execution_adjusted: float | None
    stability_adjusted: float | None
    route_adjusted: float


@dataclass
class RouteApprovalService:
    """Resolve and enforce operator route approvals."""

    store: RouteApprovalStore

    def upsert(self, *, label: str, payload: RouteApprovalUpsert) -> RouteApprovalEntry:
        """Persist a route approval entry."""

        entry = RouteApprovalEntry(
            label=label,
            updated_at=datetime.now(UTC),
            canonical_symbol=payload.canonical_symbol,
            short_venue=payload.short_venue,
            long_venue=payload.long_venue,
            short_fee_profile=payload.short_fee_profile,
            long_fee_profile=payload.long_fee_profile,
            approved=payload.approved,
            max_live_notional=payload.max_live_notional,
            note=payload.note,
        )
        return self.store.upsert(entry)

    def list_recent(
        self,
        *,
        limit: int | None = 50,
        label: str | None = None,
        canonical_symbol: str | None = None,
        approved: bool | None = None,
    ) -> list[RouteApprovalEntry]:
        """Return recent route approvals."""

        return self.store.list_recent(
            limit=limit,
            label=label,
            canonical_symbol=canonical_symbol,
            approved=approved,
        )

    def get_for_intent(self, intent: FundingPairTradeIntent) -> RouteApprovalEntry | None:
        """Return the route approval matching one trade intent."""

        return self.store.get_route(
            label=intent.label,
            canonical_symbol=intent.canonical_symbol,
            short_venue=intent.short_leg.venue,
            long_venue=intent.long_leg.venue,
            short_fee_profile=intent.short_leg.fee_profile,
            long_fee_profile=intent.long_leg.fee_profile,
        )

    def get_for_candidate(
        self,
        candidate: FundingUniverseCanaryCandidate,
    ) -> RouteApprovalEntry | None:
        """Return the route approval matching one canary candidate."""

        opportunity = candidate.opportunity.opportunity
        pair = build_pair_spec_from_universe_opportunity(candidate.opportunity)
        return self.store.get_route(
            label=pair.label or opportunity.canonical_symbol,
            canonical_symbol=opportunity.canonical_symbol,
            short_venue=opportunity.short_venue,
            long_venue=opportunity.long_venue,
            short_fee_profile=opportunity.short_fee_profile,
            long_fee_profile=opportunity.long_fee_profile,
        )

    def require_live_approval(self, intent: FundingPairTradeIntent) -> RouteApprovalEntry:
        """Return the approval for a live route or raise if it is not allowed."""

        approval = self.get_for_intent(intent)
        if approval is None:
            raise ValueError(
                "Live execution is blocked because this route has not been approved"
            )
        if not approval.approved:
            raise ValueError(
                "Live execution is blocked because this route is explicitly disabled"
            )
        if intent.target_notional - approval.max_live_notional > 1e-9:
            raise ValueError(
                "Live execution is blocked because paper trade notional exceeds the "
                f"approved cap ({approval.max_live_notional})"
            )
        return approval

    def filter_approved_canary_candidates(
        self,
        candidates: list[FundingUniverseCanaryCandidate],
    ) -> list[FundingUniverseCanaryCandidate]:
        """Keep only approved canary candidates and cap their notionals."""

        approved_candidates: list[FundingUniverseCanaryCandidate] = []
        for candidate in candidates:
            approval = self.get_for_candidate(candidate)
            if approval is None or not approval.approved:
                continue
            capped_notional = min(
                candidate.suggested_canary_notional,
                approval.max_live_notional,
            )
            if capped_notional <= 0:
                continue
            approved_candidates.append(
                FundingUniverseCanaryCandidate(
                    opportunity=candidate.opportunity,
                    suggested_canary_notional=capped_notional,
                )
            )
        return approved_candidates

    def build_approved_canary_basket_plan(
        self,
        *,
        candidates: list[FundingUniverseCanaryCandidate],
        venues: list[str],
        fee_profiles: dict[str, str],
        target_notional: float,
    ) -> ApprovedCanaryBasketPlan:
        """Return a capped live-approved canary basket plan."""

        approved_candidates = self.filter_approved_canary_candidates(candidates)
        entries: list[ApprovedCanaryBasketEntry] = []
        remaining_notional = max(0.0, target_notional)
        total_notional = 0.0
        total_entry_pnl = 0.0
        total_round_trip_pnl = 0.0
        total_execution_adjusted_round_trip_pnl: float | None = 0.0
        total_stability_adjusted_round_trip_pnl: float | None = 0.0
        total_route_adjusted_round_trip_pnl = 0.0

        for candidate in approved_candidates:
            if remaining_notional <= 0:
                break
            approval = self.get_for_candidate(candidate)
            if approval is None or not approval.approved:
                continue
            selected_notional = min(candidate.suggested_canary_notional, remaining_notional)
            if selected_notional <= 0:
                continue
            scaled = _scaled_candidate_pnl(
                opportunity=candidate.opportunity,
                selected_notional=selected_notional,
            )
            entries.append(
                ApprovedCanaryBasketEntry(
                    label=approval.label,
                    approval=approval,
                    candidate=candidate,
                    selected_notional=selected_notional,
                    estimated_one_day_pnl_after_entry=scaled["entry"],
                    estimated_one_day_pnl_after_round_trip=scaled["round_trip"],
                    execution_adjusted_estimated_one_day_pnl_after_round_trip=scaled[
                        "execution_adjusted"
                    ],
                    stability_adjusted_estimated_one_day_pnl_after_round_trip=scaled[
                        "stability_adjusted"
                    ],
                    route_adjusted_estimated_one_day_pnl_after_round_trip=scaled[
                        "route_adjusted"
                    ],
                )
            )
            total_notional += selected_notional
            total_entry_pnl += scaled["entry"]
            total_round_trip_pnl += scaled["round_trip"]
            total_execution_adjusted_round_trip_pnl = _sum_optional(
                total_execution_adjusted_round_trip_pnl,
                scaled["execution_adjusted"],
            )
            total_stability_adjusted_round_trip_pnl = _sum_optional(
                total_stability_adjusted_round_trip_pnl,
                scaled["stability_adjusted"],
            )
            total_route_adjusted_round_trip_pnl += scaled["route_adjusted"]
            remaining_notional -= selected_notional

        return ApprovedCanaryBasketPlan(
            venues=venues,
            fee_profiles=fee_profiles,
            target_notional=target_notional,
            allocated_notional=total_notional,
            unused_notional=max(0.0, remaining_notional),
            estimated_one_day_pnl_after_entry=total_entry_pnl,
            estimated_one_day_pnl_after_round_trip=total_round_trip_pnl,
            execution_adjusted_estimated_one_day_pnl_after_round_trip=(
                total_execution_adjusted_round_trip_pnl
            ),
            stability_adjusted_estimated_one_day_pnl_after_round_trip=(
                total_stability_adjusted_round_trip_pnl
            ),
            route_adjusted_estimated_one_day_pnl_after_round_trip=(
                total_route_adjusted_round_trip_pnl
            ),
            entries=entries,
        )


async def scan_exact_canary_candidate_for_approval(
    *,
    scanner: OpportunityUniverseService,
    approval_service: RouteApprovalService,
    approval: RouteApprovalEntry,
    venues: list[str],
    target_notional: float,
    canary_max_notional: float,
    min_capacity_notional: float,
    min_daily_volume: float,
    min_open_interest: float,
    min_roundtrip_edge: float,
    min_execution_quality_score: float,
    min_execution_samples: int,
    min_route_stability_weight: float,
    min_route_presence_ratio: float,
    min_route_samples: int,
    include_symbols: list[str] | None,
    exclude_symbols: list[str] | None,
    exclude_tags: list[str] | None,
    limit: int,
    fee_profile_overrides: dict[str, str] | None = None,
) -> tuple[FundingUniverseCanaryCandidate | None, int]:
    """Scan one exact approved route using the approval's configured fee profiles."""

    selected_venues = {venue.lower() for venue in venues}
    approval_venues = {approval.short_venue.lower(), approval.long_venue.lower()}
    if not approval_venues.issubset(selected_venues):
        return None, 0
    if include_symbols is not None and approval.canonical_symbol not in include_symbols:
        return None, 0
    if exclude_symbols is not None and approval.canonical_symbol in exclude_symbols:
        return None, 0

    exact_fee_profiles = dict(fee_profile_overrides or {})
    exact_fee_profiles[approval.short_venue] = approval.short_fee_profile
    exact_fee_profiles[approval.long_venue] = approval.long_fee_profile

    candidates = await scanner.scan_canary_candidates(
        venues=[approval.short_venue, approval.long_venue],
        fee_profile_overrides=exact_fee_profiles,
        target_notional=target_notional,
        canary_max_notional=canary_max_notional,
        min_capacity_notional=min_capacity_notional,
        min_daily_volume=min_daily_volume,
        min_open_interest=min_open_interest,
        min_roundtrip_edge=min_roundtrip_edge,
        min_execution_quality_score=min_execution_quality_score,
        min_execution_samples=min_execution_samples,
        min_route_stability_weight=min_route_stability_weight,
        min_route_presence_ratio=min_route_presence_ratio,
        min_route_samples=min_route_samples,
        include_symbols=[approval.canonical_symbol],
        exclude_symbols=exclude_symbols,
        exclude_tags=exclude_tags,
        limit=max(1, limit),
    )

    for candidate in approval_service.filter_approved_canary_candidates(candidates):
        matched = approval_service.get_for_candidate(candidate)
        if matched is not None and _same_route_identity(matched, approval):
            return candidate, len(candidates)
    return None, len(candidates)


async def scan_live_route_candidate_for_approval(
    *,
    scanner: OpportunityUniverseService,
    approval: RouteApprovalEntry,
    venues: list[str],
    target_notional: float,
    canary_max_notional: float,
    min_capacity_notional: float,
    min_daily_volume: float,
    min_open_interest: float,
    min_roundtrip_edge: float,
    min_execution_quality_score: float,
    min_execution_samples: int,
    min_route_stability_weight: float,
    min_route_presence_ratio: float,
    min_route_samples: int,
    include_symbols: list[str] | None,
    exclude_symbols: list[str] | None,
    exclude_tags: list[str] | None,
    limit: int,
    fee_profile_overrides: dict[str, str] | None = None,
) -> tuple[FundingUniverseCanaryCandidate | None, int]:
    """Revalidate one exact live route even after its canary edge has decayed."""

    selected_venues = {venue.lower() for venue in venues}
    approval_venues = {approval.short_venue.lower(), approval.long_venue.lower()}
    if not approval_venues.issubset(selected_venues):
        return None, 0
    if include_symbols is not None and approval.canonical_symbol not in include_symbols:
        return None, 0
    if exclude_symbols is not None and approval.canonical_symbol in exclude_symbols:
        return None, 0

    exact_fee_profiles = dict(fee_profile_overrides or {})
    exact_fee_profiles[approval.short_venue] = approval.short_fee_profile
    exact_fee_profiles[approval.long_venue] = approval.long_fee_profile

    scan = await scanner.scan(
        venues=[approval.short_venue, approval.long_venue],
        ranking="route_adjusted_quality_pnl",
        fee_profile_overrides=exact_fee_profiles,
        target_notional=target_notional,
        min_capacity_notional=min_capacity_notional,
        min_daily_volume=min_daily_volume,
        min_open_interest=min_open_interest,
        min_roundtrip_edge=min_roundtrip_edge,
        min_execution_quality_score=min_execution_quality_score,
        min_execution_samples=min_execution_samples,
        min_route_stability_weight=min_route_stability_weight,
        min_route_presence_ratio=min_route_presence_ratio,
        min_route_samples=min_route_samples,
        include_symbols=[approval.canonical_symbol],
        exclude_symbols=exclude_symbols,
        exclude_tags=exclude_tags,
        limit=max(2, limit),
    )

    for opportunity in scan.opportunities:
        modeled = opportunity.opportunity
        if (
            modeled.canonical_symbol != approval.canonical_symbol
            or modeled.short_venue != approval.short_venue
            or modeled.long_venue != approval.long_venue
            or modeled.short_fee_profile != approval.short_fee_profile
            or modeled.long_fee_profile != approval.long_fee_profile
        ):
            continue
        deployable_notional = opportunity.deployable_notional
        suggested_canary_notional = canary_max_notional
        if deployable_notional is not None and deployable_notional > 0:
            suggested_canary_notional = min(canary_max_notional, deployable_notional)
        return (
            FundingUniverseCanaryCandidate(
                opportunity=opportunity,
                suggested_canary_notional=max(suggested_canary_notional, 0.0),
            ),
            len(scan.opportunities),
        )
    return None, len(scan.opportunities)


def _same_route_identity(left: RouteApprovalEntry, right: RouteApprovalEntry) -> bool:
    return (
        left.label == right.label
        and left.canonical_symbol == right.canonical_symbol
        and left.short_venue == right.short_venue
        and left.long_venue == right.long_venue
        and left.short_fee_profile == right.short_fee_profile
        and left.long_fee_profile == right.long_fee_profile
    )


def _scaled_candidate_pnl(
    *,
    opportunity: FundingUniverseOpportunity,
    selected_notional: float,
) -> _ScaledCandidatePnl:
    deployable_notional = opportunity.deployable_notional or 0.0
    if deployable_notional <= 0 or selected_notional <= 0:
        return {
            "entry": 0.0,
            "round_trip": 0.0,
            "execution_adjusted": 0.0,
            "stability_adjusted": 0.0,
            "route_adjusted": 0.0,
        }
    scale = selected_notional / deployable_notional

    def _scaled(value: float | None) -> float:
        if value is None:
            return 0.0
        return value * scale

    raw_round_trip = opportunity.estimated_one_day_pnl_after_round_trip or 0.0
    execution_adjusted = opportunity.execution_adjusted_one_day_pnl_after_round_trip
    execution_weight = 1.0
    if raw_round_trip not in {0.0, -0.0} and execution_adjusted is not None:
        execution_weight = execution_adjusted / raw_round_trip
    stability_weight = (
        opportunity.route_stability.stability_weight
        if opportunity.route_stability is not None
        else 1.0
    )
    scaled_round_trip = _scaled(opportunity.estimated_one_day_pnl_after_round_trip)

    return {
        "entry": _scaled(opportunity.estimated_one_day_pnl_after_entry),
        "round_trip": scaled_round_trip,
        "execution_adjusted": (
            None
            if opportunity.execution_adjusted_one_day_pnl_after_round_trip is None
            else _scaled(opportunity.execution_adjusted_one_day_pnl_after_round_trip)
        ),
        "stability_adjusted": (
            None
            if opportunity.stability_adjusted_one_day_pnl_after_round_trip is None
            else _scaled(opportunity.stability_adjusted_one_day_pnl_after_round_trip)
        ),
        "route_adjusted": scaled_round_trip * execution_weight * stability_weight,
    }


def _sum_optional(total: float | None, value: float | None) -> float | None:
    if total is None or value is None:
        return None
    return total + value
