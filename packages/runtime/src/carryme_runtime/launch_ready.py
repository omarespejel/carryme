"""Shared launch-ready canary gating helpers."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass

from carryme_models import (
    ApprovedCanarySnapshot,
    FundingUniverseCanaryCandidate,
    PaperTradeExecutionPreflight,
    PaperTradeSystemState,
    VenueExecutionPreflight,
    VenueSystemState,
)

from carryme_runtime.system_state import SystemStateConfigMap, SystemStateService

FUNDING_WINDOW_HOURS_BY_VENUE: dict[str, float] = {
    "extended": 1.0,
    "paradex": 8.0,
    "hyperliquid": 8.0,
}


@dataclass(frozen=True)
class LaunchReadyAutomationGatePolicy:
    """Configuration for unattended launch-ready edge and break-even gates."""

    min_edge_retention_ratio: float = 0.7
    max_entry_break_even_funding_windows: float = 6.0
    max_round_trip_break_even_funding_windows: float = 12.0


def candidate_execution_venue_names(
    candidate: FundingUniverseCanaryCandidate,
) -> list[str]:
    """Return de-duplicated venue names touched by a canary candidate."""

    venue_names = [
        candidate.opportunity.opportunity.long_venue,
        candidate.opportunity.opportunity.short_venue,
    ]
    selected_names: list[str] = []
    for venue in venue_names:
        if venue not in selected_names:
            selected_names.append(venue)
    return selected_names


def build_candidate_live_execution_preflight(
    *,
    venue_preflights: Iterable[VenueExecutionPreflight],
    candidate: FundingUniverseCanaryCandidate,
    label: str,
) -> PaperTradeExecutionPreflight:
    """Build live-execution readiness for the exact venues touched by one canary."""

    all_statuses = {item.venue: item for item in venue_preflights}
    selected: list[VenueExecutionPreflight] = []
    blocking_reasons: list[str] = []
    for venue in candidate_execution_venue_names(candidate):
        status = all_statuses.get(venue)
        if status is None:
            blocking_reasons.append(f"Venue {venue} live execution is not configured")
            continue
        selected.append(status)
        if not status.enabled:
            blocking_reasons.append(f"Venue {venue} live execution is not enabled")
        if status.missing_env_vars:
            blocking_reasons.append(
                f"Venue {venue} is missing required credentials: "
                + ", ".join(status.missing_env_vars)
            )

    return PaperTradeExecutionPreflight(
        paper_trade_id=0,
        label=label,
        ready=not blocking_reasons,
        venues=selected,
        blocking_reasons=blocking_reasons,
    )


async def probe_candidate_system_state(
    *,
    service: SystemStateService,
    configs: SystemStateConfigMap,
    candidate: FundingUniverseCanaryCandidate,
    label: str,
) -> PaperTradeSystemState:
    """Probe only the venues touched by one canary candidate."""

    all_statuses = {item.venue: item for item in await service.probe_venues(configs)}
    selected: list[VenueSystemState] = []
    blocking_reasons: list[str] = []
    for venue in candidate_execution_venue_names(candidate):
        status = all_statuses.get(venue)
        if status is None:
            blocking_reasons.append(f"Venue {venue} system state is unknown")
            continue
        selected.append(status)
        blocking_reasons.extend(status.blocking_reasons)

    return PaperTradeSystemState(
        paper_trade_id=0,
        label=label,
        ready=not blocking_reasons,
        venues=selected,
        blocking_reasons=blocking_reasons,
    )


def approved_snapshot_route_key(
    snapshot: ApprovedCanarySnapshot,
) -> tuple[str, str, str, str, str, str]:
    """Return the normalized identity key for one approved snapshot route."""

    opportunity = snapshot.candidate.opportunity.opportunity
    return (
        snapshot.label,
        opportunity.canonical_symbol,
        opportunity.short_venue,
        opportunity.long_venue,
        opportunity.short_fee_profile,
        opportunity.long_fee_profile,
    )


def normalized_approved_snapshot_launch_payload(
    snapshot: ApprovedCanarySnapshot,
) -> dict[str, object]:
    """Return the approved-snapshot fields that must stay fixed for launch safety."""

    candidate = snapshot.candidate
    opportunity = candidate.opportunity.opportunity
    approval = snapshot.approval
    venue_markets = candidate.opportunity.venue_markets
    return {
        "label": snapshot.label,
        "suggested_canary_notional": candidate.suggested_canary_notional,
        "route": {
            "canonical_symbol": opportunity.canonical_symbol,
            "long_venue": opportunity.long_venue,
            "short_venue": opportunity.short_venue,
            "long_fee_profile": opportunity.long_fee_profile,
            "short_fee_profile": opportunity.short_fee_profile,
            "venue_markets": tuple(
                sorted(
                    (venue, market.symbol)
                    for venue, market in venue_markets.items()
                )
            ),
        },
        "approval": {
            "label": approval.label,
            "canonical_symbol": approval.canonical_symbol,
            "short_venue": approval.short_venue,
            "long_venue": approval.long_venue,
            "short_fee_profile": approval.short_fee_profile,
            "long_fee_profile": approval.long_fee_profile,
            "approved": approval.approved,
            "max_live_notional": approval.max_live_notional,
        },
    }


def approved_snapshot_launch_payload_changed(
    previous_snapshot: ApprovedCanarySnapshot,
    current_snapshot: ApprovedCanarySnapshot,
) -> bool:
    """Return whether a newer approved snapshot invalidates stable launch evidence."""

    return (
        normalized_approved_snapshot_launch_payload(previous_snapshot)
        != normalized_approved_snapshot_launch_payload(current_snapshot)
    )


def effective_funding_window_hours(snapshot: ApprovedCanarySnapshot) -> float:
    """Return the fastest relevant funding interval across the route venues."""

    opportunity = snapshot.candidate.opportunity.opportunity
    hours = [
        FUNDING_WINDOW_HOURS_BY_VENUE.get(opportunity.short_venue, 8.0),
        FUNDING_WINDOW_HOURS_BY_VENUE.get(opportunity.long_venue, 8.0),
    ]
    return min(hours)


def hold_window_hours(snapshot: ApprovedCanarySnapshot) -> float:
    """Return the slowest relevant funding interval across the route venues."""

    opportunity = snapshot.candidate.opportunity.opportunity
    hours = [
        FUNDING_WINDOW_HOURS_BY_VENUE.get(opportunity.short_venue, 8.0),
        FUNDING_WINDOW_HOURS_BY_VENUE.get(opportunity.long_venue, 8.0),
    ]
    return max(hours)


def list_recent_approved_snapshot_chain(
    *,
    recent_snapshots: Iterable[ApprovedCanarySnapshot],
    snapshot: ApprovedCanarySnapshot,
    max_snapshot_age_seconds: int,
) -> list[ApprovedCanarySnapshot]:
    """Return the recent same-route approved snapshot chain for one label."""

    route_key = approved_snapshot_route_key(snapshot)
    chain: list[ApprovedCanarySnapshot] = []
    for recent_snapshot in recent_snapshots:
        if approved_snapshot_route_key(recent_snapshot) != route_key:
            break
        age_seconds = max(
            0.0,
            (snapshot.captured_at - recent_snapshot.captured_at).total_seconds(),
        )
        if age_seconds > max_snapshot_age_seconds:
            break
        chain.append(recent_snapshot)
    return chain


def build_approved_snapshot_automation_gate_reason(
    *,
    snapshot: ApprovedCanarySnapshot,
    recent_chain: list[ApprovedCanarySnapshot],
    policy: LaunchReadyAutomationGatePolicy,
) -> str | None:
    """Return the blocking reason for unattended launch-readiness, if any."""

    opportunity = snapshot.candidate.opportunity.opportunity
    current_entry_edge = opportunity.one_day_net_edge_after_entry
    current_round_trip_edge = opportunity.one_day_net_edge_after_round_trip

    max_entry_edge = max(
        item.candidate.opportunity.opportunity.one_day_net_edge_after_entry
        for item in recent_chain
    )
    max_round_trip_edge = max(
        item.candidate.opportunity.opportunity.one_day_net_edge_after_round_trip
        for item in recent_chain
    )
    if max_entry_edge <= 0 or max_round_trip_edge <= 0:
        return "recent approved snapshot chain has non-positive net edge"

    min_retention_ratio = policy.min_edge_retention_ratio
    entry_retention_ratio = current_entry_edge / max_entry_edge
    if entry_retention_ratio < min_retention_ratio:
        return (
            "entry edge retention "
            f"{entry_retention_ratio:.2f} below minimum {min_retention_ratio:.2f}"
        )

    round_trip_retention_ratio = current_round_trip_edge / max_round_trip_edge
    if round_trip_retention_ratio < min_retention_ratio:
        return (
            "round-trip edge retention "
            f"{round_trip_retention_ratio:.2f} below minimum {min_retention_ratio:.2f}"
        )

    break_even_days_entry = opportunity.break_even_days_entry
    break_even_days_round_trip = opportunity.break_even_days_round_trip
    if break_even_days_entry is None or break_even_days_round_trip is None:
        return "approved canary snapshot is missing break-even timing"

    funding_window_hours = effective_funding_window_hours(snapshot)
    entry_break_even_windows = break_even_days_entry * 24.0 / funding_window_hours
    if entry_break_even_windows > policy.max_entry_break_even_funding_windows:
        return (
            "entry break-even funding windows "
            f"{entry_break_even_windows:.2f} exceeds maximum "
            f"{policy.max_entry_break_even_funding_windows:.2f}"
        )

    round_trip_break_even_windows = (
        break_even_days_round_trip * 24.0 / funding_window_hours
    )
    if round_trip_break_even_windows > policy.max_round_trip_break_even_funding_windows:
        return (
            "round-trip break-even funding windows "
            f"{round_trip_break_even_windows:.2f} exceeds maximum "
            f"{policy.max_round_trip_break_even_funding_windows:.2f}"
        )

    return None
