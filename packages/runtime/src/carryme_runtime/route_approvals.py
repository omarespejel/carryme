"""Route approval helpers for live-money execution."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import UTC, datetime

from carryme_models import (
    FundingPairTradeIntent,
    FundingUniverseCanaryCandidate,
    RouteApprovalEntry,
    RouteApprovalUpsert,
)
from carryme_storage import RouteApprovalStore

from carryme_runtime.universe import build_pair_spec_from_universe_opportunity


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
        limit: int = 50,
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
