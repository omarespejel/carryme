"""Funding-universe discovery and ranking services."""

from __future__ import annotations

import asyncio
import itertools
import math
import random
from dataclasses import dataclass, field
from datetime import datetime
from typing import Literal, Protocol

import httpx
from carryme_connectors import (
    ConnectorError,
    ExtendedPublicConnector,
    HyperliquidPublicConnector,
    ParadexPublicConnector,
    PublicVenueConnector,
)
from carryme_models import (
    ExecutionQualitySummary,
    FundingArbOpportunity,
    FundingPairSpec,
    FundingUniverseCanaryCandidate,
    FundingUniverseOpportunity,
    FundingUniverseOverlap,
    FundingUniversePortfolioEntry,
    FundingUniversePortfolioPlan,
    FundingUniverseScan,
    FundingUniverseVenueMarket,
    MarketStats,
    NormalizedMarketSnapshot,
    OpportunityRecord,
    RouteStabilitySummary,
    TopOfBook,
)
from carryme_normalizers import (
    NormalizationError,
    get_fee_profile,
    normalize_market_snapshot,
    normalize_symbol,
)
from carryme_scoring import score_funding_pair

from carryme_runtime.execution_quality import ExecutionQualityService
from carryme_runtime.opportunities import (
    VENUE_REGISTRY,
    MarketStatsFetcher,
    SnapshotFetcher,
    UpstreamDataError,
    fetch_live_market_stats,
    fetch_live_snapshot,
)
from carryme_runtime.route_stability import RouteStabilityService
from carryme_runtime.universe_policy import passes_symbol_policy, policy_tags_for_symbol

UniverseRanking = Literal[
    "roundtrip_edge",
    "entry_edge",
    "roundtrip_pnl",
    "entry_pnl",
    "quality_adjusted_roundtrip_pnl",
    "execution_adjusted_roundtrip_pnl",
    "execution_adjusted_quality_pnl",
    "stability_adjusted_roundtrip_pnl",
    "stability_adjusted_quality_pnl",
    "route_adjusted_quality_pnl",
]


class VenueSymbolLister(Protocol):
    """Interface for listing active perp symbols on a venue."""

    async def __call__(self, venue: str) -> list[str]: ...


DEFAULT_FEE_PROFILES: dict[str, str] = {
    "extended": "default",
    "hyperliquid": "tier0",
    "paradex": "pro",
}
DEFAULT_SNAPSHOT_CONCURRENCY_BY_VENUE: dict[str, int] = {
    "extended": 8,
    "hyperliquid": 8,
    "paradex": 3,
}
DEFAULT_SNAPSHOT_SHORTLIST_MIN_OVERLAPS = 20
DEFAULT_SNAPSHOT_SHORTLIST_MULTIPLIER = 4
RETRYABLE_SNAPSHOT_STATUS_CODES = frozenset({408, 429, 500, 502, 503, 504})
UNIVERSE_RANKINGS: tuple[UniverseRanking, ...] = (
    "roundtrip_edge",
    "entry_edge",
    "roundtrip_pnl",
    "entry_pnl",
    "quality_adjusted_roundtrip_pnl",
    "execution_adjusted_roundtrip_pnl",
    "execution_adjusted_quality_pnl",
    "stability_adjusted_roundtrip_pnl",
    "stability_adjusted_quality_pnl",
    "route_adjusted_quality_pnl",
)
SHORTLIST_CAPPED_RANKINGS = frozenset({"roundtrip_edge", "entry_edge"})


def _default_universe_fee_profiles() -> dict[str, str]:
    return {venue: DEFAULT_FEE_PROFILES.get(venue, "default") for venue in VENUE_REGISTRY}


@dataclass
class OpportunityUniverseService:
    """Discover overlapping markets and rank the live funding universe."""

    list_symbols: VenueSymbolLister = field(default_factory=lambda: list_live_symbols)
    fetch_snapshot: SnapshotFetcher = field(default_factory=lambda: fetch_live_snapshot)
    fetch_market_stats: MarketStatsFetcher = field(default_factory=lambda: fetch_live_market_stats)
    default_fee_profiles: dict[str, str] = field(default_factory=_default_universe_fee_profiles)
    snapshot_concurrency_by_venue: dict[str, int] = field(
        default_factory=lambda: dict(DEFAULT_SNAPSHOT_CONCURRENCY_BY_VENUE)
    )
    snapshot_batch_size: int | None = None
    snapshot_shortlist_min_overlaps: int = DEFAULT_SNAPSHOT_SHORTLIST_MIN_OVERLAPS
    snapshot_shortlist_multiplier: int = DEFAULT_SNAPSHOT_SHORTLIST_MULTIPLIER
    snapshot_retry_attempts: int = 3
    snapshot_retry_backoff_seconds: float = 0.25
    execution_quality_service: ExecutionQualityService | None = None
    route_stability_service: RouteStabilityService | None = None
    _overlap_tasks: dict[tuple[str, ...], asyncio.Task[list[FundingUniverseOverlap]]] = field(
        default_factory=dict,
        init=False,
        repr=False,
    )
    _overlap_cache_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        init=False,
        repr=False,
    )
    _execution_quality_index_task: (
        asyncio.Task[tuple[dict[tuple[str, str, str], ExecutionQualitySummary], float]] | None
    ) = field(default=None, init=False, repr=False)
    _execution_quality_index_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        init=False,
        repr=False,
    )
    _route_stability_index_task: (
        asyncio.Task[dict[tuple[str, str, str, str, str], RouteStabilitySummary]] | None
    ) = field(default=None, init=False, repr=False)
    _route_stability_index_lock: asyncio.Lock = field(
        default_factory=asyncio.Lock,
        init=False,
        repr=False,
    )

    def resolve_fee_profiles(
        self,
        *,
        venues: list[str],
        fee_profile_overrides: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Return the normalized venue fee-profile map used for universe scans."""

        normalized_venues = _normalize_venues(venues)
        return _resolve_fee_profiles(
            normalized_venues,
            self.default_fee_profiles,
            fee_profile_overrides,
        )

    async def scan_canary_candidates(
        self,
        *,
        venues: list[str],
        fee_profile_overrides: dict[str, str] | None = None,
        target_notional: float = 5_000.0,
        canary_max_notional: float = 25.0,
        min_capacity_notional: float = 25.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        min_execution_quality_score: float = 0.5,
        min_execution_samples: int = 0,
        min_route_stability_weight: float = 0.10,
        min_route_presence_ratio: float = 0.15,
        min_route_samples: int = 2,
        include_symbols: list[str] | None = None,
        exclude_symbols: list[str] | None = None,
        exclude_tags: list[str] | None = None,
        limit: int = 10,
    ) -> list[FundingUniverseCanaryCandidate]:
        scan = await self.scan(
            venues=venues,
            ranking="route_adjusted_quality_pnl",
            fee_profile_overrides=fee_profile_overrides,
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
            include_symbols=include_symbols,
            exclude_symbols=exclude_symbols,
            exclude_tags=exclude_tags or ["meme", "political"],
            limit=limit,
        )
        candidates: list[FundingUniverseCanaryCandidate] = []
        for opportunity in scan.opportunities:
            deployable = opportunity.deployable_notional or 0.0
            if deployable <= 0:
                continue
            modeled_round_trip_pnl = opportunity.estimated_one_day_pnl_after_round_trip or 0.0
            if modeled_round_trip_pnl <= 0:
                continue
            candidates.append(
                FundingUniverseCanaryCandidate(
                    opportunity=opportunity,
                    suggested_canary_notional=min(canary_max_notional, deployable),
                )
            )
        return candidates

    async def scan(
        self,
        *,
        venues: list[str],
        ranking: UniverseRanking = "execution_adjusted_quality_pnl",
        fee_profile_overrides: dict[str, str] | None = None,
        target_notional: float = 5_000.0,
        min_capacity_notional: float = 0.0,
        min_daily_volume: float = 0.0,
        min_open_interest: float = 0.0,
        min_roundtrip_edge: float = 0.0,
        min_execution_quality_score: float = 0.0,
        min_execution_samples: int = 0,
        min_route_stability_weight: float = 0.0,
        min_route_presence_ratio: float = 0.0,
        min_route_samples: int = 0,
        include_symbols: list[str] | None = None,
        exclude_symbols: list[str] | None = None,
        exclude_tags: list[str] | None = None,
        limit: int = 20,
    ) -> FundingUniverseScan:
        normalized_venues = _normalize_venues(venues)
        normalized_ranking = _normalize_ranking(ranking)
        fee_profiles = _resolve_fee_profiles(
            normalized_venues,
            self.default_fee_profiles,
            fee_profile_overrides,
        )
        overlaps = await self._discover_overlaps_cached(normalized_venues)
        filtered_overlaps = [
            overlap
            for overlap in overlaps
            if passes_symbol_policy(
                overlap.canonical_symbol,
                include_symbols=include_symbols,
                exclude_symbols=exclude_symbols,
                exclude_tags=exclude_tags,
            )
        ]
        snapshot_overlaps = await self._shortlist_overlaps_for_snapshots(
            filtered_overlaps,
            ranking=normalized_ranking,
            fee_profiles=fee_profiles,
            limit=limit,
            min_daily_volume=min_daily_volume,
            min_open_interest=min_open_interest,
            min_roundtrip_edge=min_roundtrip_edge,
        )
        snapshots = await self._fetch_overlapping_snapshots(snapshot_overlaps)
        (
            execution_quality_index,
            execution_prior_score,
        ) = await self._build_execution_quality_index_cached()
        route_stability_index = await self._build_route_stability_index_cached()

        opportunities: list[FundingUniverseOpportunity] = []
        for overlap in snapshot_overlaps:
            entries = [
                (venue, symbol, snapshots[(venue, symbol)])
                for venue, symbol in overlap.venue_symbols.items()
                if (venue, symbol) in snapshots
            ]
            for (left_venue, _left_symbol, left), (
                right_venue,
                _right_symbol,
                right,
            ) in itertools.combinations(entries, 2):
                scored = _build_universe_opportunity(
                    left=left,
                    right=right,
                    left_fee_profile=fee_profiles[left_venue],
                    right_fee_profile=fee_profiles[right_venue],
                    target_notional=target_notional,
                    execution_quality_index=execution_quality_index,
                    execution_prior_score=execution_prior_score,
                    route_stability_index=route_stability_index,
                )
                if not _passes_filters(
                    scored,
                    min_capacity_notional=min_capacity_notional,
                    min_daily_volume=min_daily_volume,
                    min_open_interest=min_open_interest,
                    min_roundtrip_edge=min_roundtrip_edge,
                    min_execution_quality_score=min_execution_quality_score,
                    default_execution_quality_score=execution_prior_score,
                    min_execution_samples=min_execution_samples,
                    min_route_stability_weight=min_route_stability_weight,
                    min_route_presence_ratio=min_route_presence_ratio,
                    min_route_samples=min_route_samples,
                ):
                    continue
                opportunities.append(scored)

        ranked = sorted(
            opportunities,
            key=lambda item: _ranking_value(item, normalized_ranking),
            reverse=True,
        )
        if limit > 0:
            ranked = ranked[:limit]

        return FundingUniverseScan(
            venues=normalized_venues,
            fee_profiles=fee_profiles,
            ranking=normalized_ranking,
            target_notional=target_notional,
            overlap_count=len(filtered_overlaps),
            overlaps=filtered_overlaps,
            opportunities=ranked,
        )

    async def discover_overlaps(self, venues: list[str]) -> list[FundingUniverseOverlap]:
        symbol_lists = await asyncio.gather(*(self.list_symbols(venue) for venue in venues))
        by_canonical_symbol: dict[str, dict[str, str]] = {}
        for venue, symbols in zip(venues, symbol_lists, strict=True):
            for symbol in symbols:
                try:
                    identity = normalize_symbol(venue, symbol)
                except ValueError:
                    continue
                by_canonical_symbol.setdefault(identity.canonical_symbol, {})[venue] = symbol

        overlaps = [
            FundingUniverseOverlap(
                canonical_symbol=canonical_symbol,
                venues=sorted(venue_symbols),
                venue_symbols=dict(sorted(venue_symbols.items())),
            )
            for canonical_symbol, venue_symbols in by_canonical_symbol.items()
            if len(venue_symbols) >= 2
        ]
        overlaps.sort(key=lambda item: item.canonical_symbol)
        return overlaps

    async def _discover_overlaps_cached(
        self,
        venues: list[str],
    ) -> list[FundingUniverseOverlap]:
        key = tuple(venues)
        async with self._overlap_cache_lock:
            task = self._overlap_tasks.get(key)
            if task is None or task.done():
                task = asyncio.create_task(self.discover_overlaps(list(key)))
                self._overlap_tasks[key] = task
        return await asyncio.shield(task)

    async def _build_execution_quality_index_cached(
        self,
    ) -> tuple[dict[tuple[str, str, str], ExecutionQualitySummary], float]:
        if self.execution_quality_service is None:
            return {}, 1.0
        async with self._execution_quality_index_lock:
            task = self._execution_quality_index_task
            if task is None or task.done():
                task = asyncio.create_task(
                    self._build_execution_quality_index_once(),
                )
                self._execution_quality_index_task = task
        return await asyncio.shield(task)

    async def _build_route_stability_index_cached(
        self,
    ) -> dict[tuple[str, str, str, str, str], RouteStabilitySummary]:
        if self.route_stability_service is None:
            return {}
        async with self._route_stability_index_lock:
            task = self._route_stability_index_task
            if task is None or task.done():
                task = asyncio.create_task(
                    self._build_route_stability_index_once(),
                )
                self._route_stability_index_task = task
        return await asyncio.shield(task)

    async def _build_execution_quality_index_once(
        self,
    ) -> tuple[dict[tuple[str, str, str], ExecutionQualitySummary], float]:
        assert self.execution_quality_service is not None
        return (
            self.execution_quality_service.build_index(),
            self.execution_quality_service.prior_score,
        )

    async def _build_route_stability_index_once(
        self,
    ) -> dict[tuple[str, str, str, str, str], RouteStabilitySummary]:
        assert self.route_stability_service is not None
        return self.route_stability_service.build_index()

    async def _shortlist_overlaps_for_snapshots(
        self,
        overlaps: list[FundingUniverseOverlap],
        *,
        ranking: UniverseRanking,
        fee_profiles: dict[str, str],
        limit: int,
        min_daily_volume: float,
        min_open_interest: float,
        min_roundtrip_edge: float,
    ) -> list[FundingUniverseOverlap]:
        """Use lightweight market stats to choose which overlaps need full orderbooks."""

        shortlist_size = self._snapshot_shortlist_size(limit)
        if len(overlaps) <= 1:
            return overlaps

        stats = await self._fetch_overlapping_market_stats(overlaps)
        scored: list[tuple[tuple[float, float, float, str], FundingUniverseOverlap]] = []
        unscored: list[FundingUniverseOverlap] = []
        for overlap in overlaps:
            best_score: tuple[float, float, float, str] | None = None
            entries = [
                (venue, symbol, stats[(venue, symbol)])
                for venue, symbol in overlap.venue_symbols.items()
                if (venue, symbol) in stats
            ]
            for (left_venue, _left_symbol, left), (
                right_venue,
                _right_symbol,
                right,
            ) in itertools.combinations(entries, 2):
                try:
                    opportunity = score_funding_pair(
                        left,
                        right,
                        get_fee_profile(left_venue, fee_profiles[left_venue]),
                        get_fee_profile(right_venue, fee_profiles[right_venue]),
                    )
                except ValueError:
                    continue
                venue_markets = [_venue_market(left), _venue_market(right)]
                min_volume = _min_metric(venue_markets, "daily_volume")
                min_oi = _min_metric(venue_markets, "open_interest")
                if opportunity.one_day_net_edge_after_round_trip < min_roundtrip_edge:
                    continue
                if (min_volume or 0.0) < min_daily_volume:
                    continue
                if (min_oi or 0.0) < min_open_interest:
                    continue
                score = (
                    _shortlist_ranking_value(opportunity, ranking),
                    min_volume or 0.0,
                    min_oi or 0.0,
                    overlap.canonical_symbol,
                )
                if best_score is None or score > best_score:
                    best_score = score
            if best_score is not None:
                scored.append((best_score, overlap))
            else:
                unscored.append(overlap)

        ranked = sorted(scored, key=lambda item: item[0], reverse=True)
        ordered = [overlap for _score, overlap in ranked]
        ordered.extend(unscored)
        if shortlist_size <= 0 or ranking not in SHORTLIST_CAPPED_RANKINGS:
            return ordered
        return ordered[:shortlist_size]

    async def _fetch_overlapping_market_stats(
        self,
        overlaps: list[FundingUniverseOverlap],
    ) -> dict[tuple[str, str], NormalizedMarketSnapshot]:
        semaphores = {
            venue: asyncio.Semaphore(max(self.snapshot_concurrency_by_venue.get(venue, 1), 1))
            for venue in VENUE_REGISTRY
        }
        snapshots: dict[tuple[str, str], NormalizedMarketSnapshot] = {}
        items = [
            (venue, symbol)
            for overlap in overlaps
            for venue, symbol in overlap.venue_symbols.items()
        ]
        batch_size = self._effective_snapshot_batch_size()
        for start in range(0, len(items), batch_size):
            batch = items[start : start + batch_size]
            tasks = {
                (venue, symbol): asyncio.create_task(
                    self._fetch_market_stats_with_controls(
                        venue,
                        symbol,
                        semaphore=semaphores[venue],
                    )
                )
                for venue, symbol in batch
            }
            for key, task in tasks.items():
                try:
                    snapshots[key] = await task
                except (
                    ValueError,
                    NormalizationError,
                    UpstreamDataError,
                    ConnectorError,
                    httpx.HTTPError,
                ):
                    continue
        return snapshots

    async def _fetch_overlapping_snapshots(
        self,
        overlaps: list[FundingUniverseOverlap],
    ) -> dict[tuple[str, str], NormalizedMarketSnapshot]:
        semaphores = {
            venue: asyncio.Semaphore(max(self.snapshot_concurrency_by_venue.get(venue, 1), 1))
            for venue in VENUE_REGISTRY
        }
        snapshots: dict[tuple[str, str], NormalizedMarketSnapshot] = {}
        items = [
            (venue, symbol)
            for overlap in overlaps
            for venue, symbol in overlap.venue_symbols.items()
        ]
        batch_size = self._effective_snapshot_batch_size()
        for start in range(0, len(items), batch_size):
            batch = items[start : start + batch_size]
            tasks = {
                (venue, symbol): asyncio.create_task(
                    self._fetch_snapshot_with_controls(
                        venue,
                        symbol,
                        semaphore=semaphores[venue],
                    )
                )
                for venue, symbol in batch
            }
            for key, task in tasks.items():
                try:
                    snapshots[key] = await task
                except (ValueError, UpstreamDataError, ConnectorError, httpx.HTTPError):
                    continue
        return snapshots

    def _effective_snapshot_batch_size(self) -> int:
        if self.snapshot_batch_size is not None:
            return max(self.snapshot_batch_size, 1)
        return max(
            sum(
                max(self.snapshot_concurrency_by_venue.get(venue, 1), 1)
                for venue in VENUE_REGISTRY
            ),
            1,
        )

    def _snapshot_shortlist_size(self, limit: int) -> int:
        if limit <= 0:
            return 0
        return max(
            max(self.snapshot_shortlist_min_overlaps, 1),
            limit * max(self.snapshot_shortlist_multiplier, 1),
        )

    async def _fetch_market_stats_with_controls(
        self,
        venue: str,
        symbol: str,
        *,
        semaphore: asyncio.Semaphore,
    ) -> NormalizedMarketSnapshot:
        attempts = max(self.snapshot_retry_attempts, 1)
        for attempt in range(1, attempts + 1):
            try:
                async with semaphore:
                    market = await self.fetch_market_stats(venue, symbol)
                return _normalize_market_stats_snapshot(venue, symbol, market)
            except (ValueError, NormalizationError, UpstreamDataError):
                raise
            except (ConnectorError, httpx.HTTPError) as exc:
                if attempt >= attempts or not _is_retryable_snapshot_error(exc):
                    raise
                base_delay = self.snapshot_retry_backoff_seconds * (2 ** (attempt - 1))
                await asyncio.sleep(base_delay * random.uniform(0.75, 1.25))
        raise UpstreamDataError(f"Could not fetch market stats for {venue}:{symbol}")

    async def _fetch_snapshot_with_controls(
        self,
        venue: str,
        symbol: str,
        *,
        semaphore: asyncio.Semaphore,
    ) -> NormalizedMarketSnapshot:
        attempts = max(self.snapshot_retry_attempts, 1)
        for attempt in range(1, attempts + 1):
            try:
                async with semaphore:
                    return await self.fetch_snapshot(venue, symbol)
            except ValueError:
                raise
            except (ConnectorError, httpx.HTTPError) as exc:
                if attempt >= attempts or not _is_retryable_snapshot_error(exc):
                    raise
                base_delay = self.snapshot_retry_backoff_seconds * (2 ** (attempt - 1))
                await asyncio.sleep(base_delay * random.uniform(0.75, 1.25))
        raise RuntimeError("unreachable snapshot retry loop")


@dataclass(frozen=True)
class ModeledPairEconomics:
    """Expected-value economics for a routed pair at a concrete notional."""

    entry_cost_rate: float
    round_trip_cost_rate: float
    one_day_pnl_after_entry: float | None
    one_day_pnl_after_round_trip: float | None
    paradex_fastfill_share: float | None = None
    paradex_fastfill_eligible_notional: float | None = None


async def list_live_symbols(venue: str) -> list[str]:
    """List active perp symbols on a supported venue."""

    key = venue.strip().lower()
    venue_config = VENUE_REGISTRY.get(key)
    if venue_config is None:
        raise ValueError(f"Unsupported venue: {venue}")
    base_url, _connector_factory = venue_config

    async with httpx.AsyncClient(base_url=base_url, timeout=20.0) as client:
        connector = _build_connector(key, client)
        return await connector.list_market_symbols()


def _normalize_market_stats_snapshot(
    venue: str,
    symbol: str,
    market: MarketStats,
) -> NormalizedMarketSnapshot:
    key = venue.strip().lower()
    expected_identity = normalize_symbol(key, symbol)
    normalized = normalize_market_snapshot(key, market)
    if normalized.identity.canonical_symbol != expected_identity.canonical_symbol:
        raise UpstreamDataError(
            f"Upstream market data symbol mismatch for {key}:{symbol}: "
            "expected "
            f"{expected_identity.canonical_symbol}, got {normalized.identity.canonical_symbol}"
        )
    return normalized


def _build_connector(venue: str, client: httpx.AsyncClient) -> PublicVenueConnector:
    if venue == "extended":
        return ExtendedPublicConnector(client)
    if venue == "hyperliquid":
        return HyperliquidPublicConnector(client)
    if venue == "paradex":
        return ParadexPublicConnector(client)
    raise ValueError(f"Unsupported venue: {venue}")


def _is_retryable_snapshot_error(exc: ConnectorError | httpx.HTTPError) -> bool:
    if isinstance(exc, ConnectorError):
        if exc.status_code is None:
            return True
        return exc.status_code in RETRYABLE_SNAPSHOT_STATUS_CODES
    if isinstance(exc, httpx.HTTPStatusError):
        return exc.response.status_code in RETRYABLE_SNAPSHOT_STATUS_CODES
    return True


def _build_universe_opportunity(
    *,
    left: NormalizedMarketSnapshot,
    right: NormalizedMarketSnapshot,
    left_fee_profile: str,
    right_fee_profile: str,
    target_notional: float,
    execution_quality_index: dict[tuple[str, str, str], ExecutionQualitySummary],
    execution_prior_score: float,
    route_stability_index: dict[tuple[str, str, str, str, str], RouteStabilitySummary],
) -> FundingUniverseOpportunity:
    opportunity = score_funding_pair(
        left,
        right,
        get_fee_profile(left.identity.venue, left_fee_profile),
        get_fee_profile(right.identity.venue, right_fee_profile),
    )
    venue_markets = {
        left.identity.venue: _venue_market(left),
        right.identity.venue: _venue_market(right),
    }
    min_daily_volume = _min_metric(list(venue_markets.values()), "daily_volume")
    min_open_interest = _min_metric(list(venue_markets.values()), "open_interest")
    deployable_notional = _deployable_notional(opportunity, target_notional)
    modeled = _model_pair_economics(
        opportunity=opportunity,
        venue_markets=venue_markets,
        notional=deployable_notional,
    )
    pnl_after_entry = modeled.one_day_pnl_after_entry
    pnl_after_round_trip = modeled.one_day_pnl_after_round_trip
    quality_score = _quality_score(
        estimated_one_day_pnl_after_round_trip=pnl_after_round_trip,
        deployable_notional=deployable_notional,
        min_daily_volume=min_daily_volume,
        min_open_interest=min_open_interest,
    )
    policy_tags = policy_tags_for_symbol(opportunity.canonical_symbol)
    execution_quality = execution_quality_index.get(
        (opportunity.canonical_symbol, opportunity.short_venue, opportunity.long_venue)
    )
    execution_quality_score = (
        execution_quality.weighted_score if execution_quality is not None else execution_prior_score
    )
    route_stability = route_stability_index.get(
        (
            opportunity.canonical_symbol,
            opportunity.short_venue,
            opportunity.long_venue,
            opportunity.short_fee_profile,
            opportunity.long_fee_profile,
        )
    )
    execution_adjusted_round_trip_pnl = (
        pnl_after_round_trip * execution_quality_score
        if pnl_after_round_trip is not None
        else pnl_after_round_trip
    )
    execution_adjusted_quality_score = (
        quality_score * execution_quality_score if quality_score is not None else quality_score
    )
    stability_adjusted_round_trip_pnl = (
        pnl_after_round_trip * route_stability.stability_weight
        if pnl_after_round_trip is not None and route_stability is not None
        else pnl_after_round_trip
    )
    stability_adjusted_quality_score = (
        quality_score * route_stability.stability_weight
        if quality_score is not None and route_stability is not None
        else quality_score
    )
    route_adjusted_quality_score = (
        execution_adjusted_quality_score * route_stability.stability_weight
        if execution_adjusted_quality_score is not None and route_stability is not None
        else execution_adjusted_quality_score
    )
    return FundingUniverseOpportunity(
        opportunity=opportunity,
        policy_tags=policy_tags,
        venue_markets=venue_markets,
        min_daily_volume=min_daily_volume,
        min_open_interest=min_open_interest,
        target_notional=target_notional,
        deployable_notional=deployable_notional,
        modeled_entry_cost_rate=modeled.entry_cost_rate,
        modeled_round_trip_cost_rate=modeled.round_trip_cost_rate,
        estimated_one_day_pnl_after_entry=pnl_after_entry,
        estimated_one_day_pnl_after_round_trip=pnl_after_round_trip,
        paradex_fastfill_share=modeled.paradex_fastfill_share,
        paradex_fastfill_eligible_notional=modeled.paradex_fastfill_eligible_notional,
        quality_score=quality_score,
        execution_quality=execution_quality,
        route_stability=route_stability,
        execution_adjusted_one_day_pnl_after_round_trip=execution_adjusted_round_trip_pnl,
        execution_adjusted_quality_score=execution_adjusted_quality_score,
        stability_adjusted_one_day_pnl_after_round_trip=stability_adjusted_round_trip_pnl,
        stability_adjusted_quality_score=stability_adjusted_quality_score,
        route_adjusted_quality_score=route_adjusted_quality_score,
    )


def _venue_market(snapshot: NormalizedMarketSnapshot) -> FundingUniverseVenueMarket:
    book = snapshot.market.top_of_book
    bid_notional = _book_side_notional(book, "bid")
    ask_notional = _book_side_notional(book, "ask")
    return FundingUniverseVenueMarket(
        venue=snapshot.identity.venue,
        symbol=snapshot.identity.venue_symbol,
        mark_price=snapshot.market.mark_price,
        daily_funding_rate=snapshot.funding.daily_rate,
        open_interest=snapshot.market.open_interest,
        daily_volume=snapshot.market.daily_volume,
        bid_notional=bid_notional,
        ask_notional=ask_notional,
        bid_notional_api=_book_side_api_notional(book, "bid"),
        ask_notional_api=_book_side_api_notional(book, "ask"),
        bid_notional_interactive=_book_side_interactive_notional(book, "bid"),
        ask_notional_interactive=_book_side_interactive_notional(book, "ask"),
    )


def _min_metric(markets: list[FundingUniverseVenueMarket], field_name: str) -> float | None:
    values: list[float] = []
    for market in markets:
        value = getattr(market, field_name)
        if isinstance(value, int | float):
            values.append(float(value))
    if not values:
        return None
    return min(values)


def _deployable_notional(
    opportunity: FundingArbOpportunity,
    target_notional: float,
) -> float | None:
    if opportunity.capacity is None or opportunity.capacity.max_entry_notional is None:
        return None
    return min(target_notional, opportunity.capacity.max_entry_notional)


def _quality_score(
    *,
    estimated_one_day_pnl_after_round_trip: float | None,
    deployable_notional: float | None,
    min_daily_volume: float | None,
    min_open_interest: float | None,
) -> float | None:
    if estimated_one_day_pnl_after_round_trip is None:
        return None
    if estimated_one_day_pnl_after_round_trip <= 0:
        return estimated_one_day_pnl_after_round_trip

    volume_factor = _bounded_log_factor(min_daily_volume, normalization=5.0)
    oi_factor = _bounded_log_factor(min_open_interest, normalization=6.0)
    depth_factor = _bounded_log_factor(deployable_notional, normalization=4.0)
    return estimated_one_day_pnl_after_round_trip * volume_factor * oi_factor * depth_factor


def _bounded_log_factor(value: float | None, *, normalization: float) -> float:
    if value is None or value <= 0:
        return 0.0
    return min(math.log10(1.0 + value) / normalization, 1.0)


def _passes_filters(
    opportunity: FundingUniverseOpportunity,
    *,
    min_capacity_notional: float,
    min_daily_volume: float,
    min_open_interest: float,
    min_roundtrip_edge: float,
    min_execution_quality_score: float,
    default_execution_quality_score: float,
    min_execution_samples: int,
    min_route_stability_weight: float,
    min_route_presence_ratio: float,
    min_route_samples: int,
) -> bool:
    deployable_notional = opportunity.deployable_notional or 0.0
    if deployable_notional < min_capacity_notional:
        return False
    if (opportunity.min_daily_volume or 0.0) < min_daily_volume:
        return False
    if (opportunity.min_open_interest or 0.0) < min_open_interest:
        return False
    if _modeled_net_edge_after_round_trip(opportunity) < min_roundtrip_edge:
        return False
    execution_quality_score = (
        opportunity.execution_quality.weighted_score
        if opportunity.execution_quality is not None
        else default_execution_quality_score
    )
    execution_sample_size = (
        opportunity.execution_quality.sample_size
        if opportunity.execution_quality is not None
        else 0
    )
    if execution_quality_score < min_execution_quality_score:
        return False
    if execution_sample_size < min_execution_samples:
        return False
    route_stability_weight = (
        opportunity.route_stability.stability_weight
        if opportunity.route_stability is not None
        else 1.0
    )
    route_presence_ratio = (
        opportunity.route_stability.presence_ratio
        if opportunity.route_stability is not None
        else 0.0
    )
    route_sample_size = (
        opportunity.route_stability.sample_size if opportunity.route_stability is not None else 0
    )
    if route_stability_weight < min_route_stability_weight:
        return False
    if route_presence_ratio < min_route_presence_ratio:
        return False
    return route_sample_size >= min_route_samples


def _model_pair_economics(
    *,
    opportunity: FundingArbOpportunity,
    venue_markets: dict[str, FundingUniverseVenueMarket],
    notional: float | None,
) -> ModeledPairEconomics:
    short_market = venue_markets.get(opportunity.short_venue)
    long_market = venue_markets.get(opportunity.long_venue)
    if short_market is None or long_market is None:
        return ModeledPairEconomics(
            entry_cost_rate=opportunity.entry_cost_rate,
            round_trip_cost_rate=opportunity.round_trip_cost_rate,
            one_day_pnl_after_entry=(
                notional * opportunity.one_day_net_edge_after_entry
                if notional is not None
                else None
            ),
            one_day_pnl_after_round_trip=(
                notional * opportunity.one_day_net_edge_after_round_trip
                if notional is not None
                else None
            ),
        )

    short_fee_rate, short_fastfill_share, short_fastfill_eligible = _modeled_taker_fee_rate(
        market=short_market,
        fee_profile=opportunity.short_fee_profile,
        side="bid",
        notional=notional,
    )
    long_fee_rate, long_fastfill_share, long_fastfill_eligible = _modeled_taker_fee_rate(
        market=long_market,
        fee_profile=opportunity.long_fee_profile,
        side="ask",
        notional=notional,
    )
    entry_cost_rate = short_fee_rate + long_fee_rate
    round_trip_cost_rate = entry_cost_rate * 2.0
    one_day_pnl_after_entry = (
        notional * (opportunity.gross_daily_edge - entry_cost_rate)
        if notional is not None
        else None
    )
    one_day_pnl_after_round_trip = (
        notional * (opportunity.gross_daily_edge - round_trip_cost_rate)
        if notional is not None
        else None
    )
    paradex_fastfill_share = short_fastfill_share
    paradex_fastfill_eligible = short_fastfill_eligible
    if paradex_fastfill_share is None:
        paradex_fastfill_share = long_fastfill_share
        paradex_fastfill_eligible = long_fastfill_eligible
    return ModeledPairEconomics(
        entry_cost_rate=entry_cost_rate,
        round_trip_cost_rate=round_trip_cost_rate,
        one_day_pnl_after_entry=one_day_pnl_after_entry,
        one_day_pnl_after_round_trip=one_day_pnl_after_round_trip,
        paradex_fastfill_share=paradex_fastfill_share,
        paradex_fastfill_eligible_notional=paradex_fastfill_eligible,
    )


def _modeled_taker_fee_rate(
    *,
    market: FundingUniverseVenueMarket,
    fee_profile: str,
    side: Literal["bid", "ask"],
    notional: float | None,
) -> tuple[float, float | None, float | None]:
    selected_fee = get_fee_profile(market.venue, fee_profile).taker_fee_rate
    if market.venue != "paradex" or fee_profile != "pro_fastfills":
        return selected_fee, None, None
    pro_fee = get_fee_profile("paradex", "pro").taker_fee_rate
    fastfill_fee = get_fee_profile("paradex", "pro_fastfills").taker_fee_rate
    if notional is None or notional <= 0:
        return pro_fee, 0.0, 0.0
    interactive_notional = _market_side_interactive_notional(market, side)
    if interactive_notional is None or interactive_notional <= 0:
        return pro_fee, 0.0, 0.0
    eligible_notional = min(interactive_notional, notional)
    eligible_share = min(eligible_notional / notional, 1.0)
    effective_fee = pro_fee - ((pro_fee - fastfill_fee) * eligible_share)
    return effective_fee, eligible_share, eligible_notional


def _market_side_interactive_notional(
    market: FundingUniverseVenueMarket,
    side: Literal["bid", "ask"],
) -> float | None:
    if side == "bid":
        return market.bid_notional_interactive
    return market.ask_notional_interactive


def _book_side_notional(book: object, side: Literal["bid", "ask"]) -> float | None:
    if not isinstance(book, TopOfBook):
        return None
    if side == "bid":
        return _notional_from_price_size(book.best_bid_price, book.best_bid_size)
    return _notional_from_price_size(book.best_ask_price, book.best_ask_size)


def _book_side_api_notional(book: object, side: Literal["bid", "ask"]) -> float | None:
    if not isinstance(book, TopOfBook):
        return None
    if side == "bid":
        return _notional_from_price_size(book.best_bid_api_price, book.best_bid_api_size)
    return _notional_from_price_size(book.best_ask_api_price, book.best_ask_api_size)


def _book_side_interactive_notional(book: object, side: Literal["bid", "ask"]) -> float | None:
    if not isinstance(book, TopOfBook):
        return None
    total_price, total_size, api_price, api_size = _book_side_components(book, side)
    explicit = _book_side_explicit_interactive_notional(book, side)
    if explicit is not None:
        interactive_price = (
            book.best_bid_interactive_price if side == "bid" else book.best_ask_interactive_price
        )
        if total_price is None or interactive_price is None or interactive_price != total_price:
            return 0.0
        return explicit
    if total_price is None or total_size is None:
        return None
    if api_price is None:
        return None
    if api_price != total_price:
        return 0.0
    if api_size is None:
        return None
    interactive_size = max(total_size - api_size, 0.0)
    return total_price * interactive_size


def _book_side_explicit_interactive_notional(
    book: TopOfBook,
    side: Literal["bid", "ask"],
) -> float | None:
    if side == "bid":
        return _notional_from_price_size(
            book.best_bid_interactive_price,
            book.best_bid_interactive_size,
        )
    return _notional_from_price_size(
        book.best_ask_interactive_price,
        book.best_ask_interactive_size,
    )


def _book_side_components(
    book: TopOfBook,
    side: Literal["bid", "ask"],
) -> tuple[float | None, float | None, float | None, float | None]:
    if side == "bid":
        return (
            book.best_bid_price,
            book.best_bid_size,
            book.best_bid_api_price,
            book.best_bid_api_size,
        )
    return (
        book.best_ask_price,
        book.best_ask_size,
        book.best_ask_api_price,
        book.best_ask_api_size,
    )


def _notional_from_price_size(price: float | None, size: float | None) -> float | None:
    if price is None or size is None:
        return None
    return price * size


def _modeled_net_edge_after_entry(opportunity: FundingUniverseOpportunity) -> float:
    modeled_entry_cost_rate = opportunity.modeled_entry_cost_rate
    if modeled_entry_cost_rate is None:
        return opportunity.opportunity.one_day_net_edge_after_entry
    return opportunity.opportunity.gross_daily_edge - modeled_entry_cost_rate


def _modeled_net_edge_after_round_trip(opportunity: FundingUniverseOpportunity) -> float:
    modeled_round_trip_cost_rate = opportunity.modeled_round_trip_cost_rate
    if modeled_round_trip_cost_rate is None:
        return opportunity.opportunity.one_day_net_edge_after_round_trip
    return opportunity.opportunity.gross_daily_edge - modeled_round_trip_cost_rate


def _ranking_value(opportunity: FundingUniverseOpportunity, ranking: UniverseRanking) -> float:
    def _rankable(value: float | None) -> float:
        return value if value is not None else float("-inf")

    if ranking == "roundtrip_edge":
        return _modeled_net_edge_after_round_trip(opportunity)
    if ranking == "entry_edge":
        return _modeled_net_edge_after_entry(opportunity)
    if ranking == "roundtrip_pnl":
        return _rankable(opportunity.estimated_one_day_pnl_after_round_trip)
    if ranking == "entry_pnl":
        return _rankable(opportunity.estimated_one_day_pnl_after_entry)
    if ranking == "execution_adjusted_roundtrip_pnl":
        return _rankable(opportunity.execution_adjusted_one_day_pnl_after_round_trip)
    if ranking == "execution_adjusted_quality_pnl":
        return _rankable(opportunity.execution_adjusted_quality_score)
    if ranking == "stability_adjusted_roundtrip_pnl":
        return _rankable(opportunity.stability_adjusted_one_day_pnl_after_round_trip)
    if ranking == "route_adjusted_quality_pnl":
        return _rankable(opportunity.route_adjusted_quality_score)
    if ranking == "stability_adjusted_quality_pnl":
        return _rankable(opportunity.stability_adjusted_quality_score)
    return _rankable(opportunity.quality_score)


def _shortlist_ranking_value(opportunity: FundingArbOpportunity, ranking: UniverseRanking) -> float:
    if ranking == "entry_edge":
        return opportunity.one_day_net_edge_after_entry
    return opportunity.one_day_net_edge_after_round_trip


def _normalize_venues(venues: list[str]) -> list[str]:
    if not venues:
        raise ValueError("At least one venue must be selected")
    normalized = sorted({venue.strip().lower() for venue in venues if venue.strip()})
    if len(normalized) < 2:
        raise ValueError("At least two venues are required for funding universe scans")
    unsupported = [venue for venue in normalized if venue not in VENUE_REGISTRY]
    if unsupported:
        joined = ", ".join(sorted(unsupported))
        raise ValueError(f"Unsupported venue(s): {joined}")
    return normalized


def _normalize_ranking(ranking: str) -> UniverseRanking:
    normalized = ranking.strip()
    if normalized not in UNIVERSE_RANKINGS:
        supported = ", ".join(UNIVERSE_RANKINGS)
        raise ValueError(f"Unsupported ranking: {ranking}. Expected one of: {supported}")
    return normalized  # type: ignore[return-value]


def _resolve_fee_profiles(
    venues: list[str],
    default_fee_profiles: dict[str, str],
    fee_profile_overrides: dict[str, str] | None,
) -> dict[str, str]:
    resolved = {venue: default_fee_profiles[venue] for venue in venues}
    if not fee_profile_overrides:
        return resolved
    for venue, profile in fee_profile_overrides.items():
        normalized_venue = venue.strip().lower()
        if normalized_venue not in resolved:
            raise ValueError(f"Unsupported fee-profile override venue: {venue}")
        normalized_profile = profile.strip().lower()
        get_fee_profile(normalized_venue, normalized_profile)
        resolved[normalized_venue] = normalized_profile
    return resolved


def build_portfolio_plan(
    scan: FundingUniverseScan,
    *,
    target_notional: float,
    max_positions: int = 5,
    min_selected_notional: float = 0.0,
    one_position_per_symbol: bool = True,
) -> FundingUniversePortfolioPlan:
    """Greedily allocate capital across the ranked universe opportunities."""

    if target_notional < 0:
        raise ValueError("target_notional must be non-negative")
    if max_positions <= 0:
        raise ValueError("max_positions must be positive")
    if min_selected_notional < 0:
        raise ValueError("min_selected_notional must be non-negative")

    remaining = target_notional
    selected_symbols: set[str] = set()
    entries: list[FundingUniversePortfolioEntry] = []
    total_entry_pnl = 0.0
    total_round_trip_pnl = 0.0
    total_execution_adjusted_round_trip_pnl = 0.0
    total_stability_adjusted_round_trip_pnl = 0.0
    total_route_adjusted_round_trip_pnl = 0.0

    for opportunity in scan.opportunities:
        if remaining <= 0 or len(entries) >= max_positions:
            break
        canonical_symbol = opportunity.opportunity.canonical_symbol
        if one_position_per_symbol and canonical_symbol in selected_symbols:
            continue
        available = opportunity.deployable_notional or 0.0
        selected_notional = min(remaining, available)
        if selected_notional < min_selected_notional:
            continue

        modeled = _model_pair_economics(
            opportunity=opportunity.opportunity,
            venue_markets=opportunity.venue_markets,
            notional=selected_notional,
        )
        execution_weight = _execution_weight(opportunity)
        stability_weight = (
            opportunity.route_stability.stability_weight
            if opportunity.route_stability is not None
            else 1.0
        )
        entry_pnl = modeled.one_day_pnl_after_entry or 0.0
        round_trip_pnl = modeled.one_day_pnl_after_round_trip or 0.0
        execution_adjusted_round_trip_pnl = round_trip_pnl * execution_weight
        stability_adjusted_round_trip_pnl = round_trip_pnl * stability_weight
        route_adjusted_round_trip_pnl = round_trip_pnl * execution_weight * stability_weight

        entries.append(
            FundingUniversePortfolioEntry(
                opportunity=opportunity,
                selected_notional=selected_notional,
                estimated_one_day_pnl_after_entry=entry_pnl,
                estimated_one_day_pnl_after_round_trip=round_trip_pnl,
                execution_adjusted_estimated_one_day_pnl_after_round_trip=(
                    execution_adjusted_round_trip_pnl
                ),
                stability_adjusted_estimated_one_day_pnl_after_round_trip=(
                    stability_adjusted_round_trip_pnl
                ),
                route_adjusted_estimated_one_day_pnl_after_round_trip=(
                    route_adjusted_round_trip_pnl
                ),
            )
        )
        remaining -= selected_notional
        total_entry_pnl += entry_pnl
        total_round_trip_pnl += round_trip_pnl
        total_execution_adjusted_round_trip_pnl += execution_adjusted_round_trip_pnl
        total_stability_adjusted_round_trip_pnl += stability_adjusted_round_trip_pnl
        total_route_adjusted_round_trip_pnl += route_adjusted_round_trip_pnl
        selected_symbols.add(canonical_symbol)

    allocated = target_notional - remaining
    return FundingUniversePortfolioPlan(
        ranking=scan.ranking,
        target_notional=target_notional,
        allocated_notional=allocated,
        unused_notional=max(remaining, 0.0),
        estimated_one_day_pnl_after_entry=total_entry_pnl,
        estimated_one_day_pnl_after_round_trip=total_round_trip_pnl,
        execution_adjusted_estimated_one_day_pnl_after_round_trip=(
            total_execution_adjusted_round_trip_pnl
        ),
        stability_adjusted_estimated_one_day_pnl_after_round_trip=(
            total_stability_adjusted_round_trip_pnl
        ),
        route_adjusted_estimated_one_day_pnl_after_round_trip=(total_route_adjusted_round_trip_pnl),
        entries=entries,
    )


def _execution_weight(opportunity: FundingUniverseOpportunity) -> float:
    if opportunity.execution_quality is not None:
        return opportunity.execution_quality.weighted_score
    raw_pnl = opportunity.estimated_one_day_pnl_after_round_trip
    adjusted_pnl = opportunity.execution_adjusted_one_day_pnl_after_round_trip
    if (
        raw_pnl is not None
        and adjusted_pnl is not None
        and not math.isclose(raw_pnl, 0.0, rel_tol=0.0, abs_tol=1e-12)
    ):
        return adjusted_pnl / raw_pnl
    return 1.0


def build_pair_spec_from_universe_opportunity(
    opportunity: FundingUniverseOpportunity,
) -> FundingPairSpec:
    """Convert a universe opportunity into a reusable pair spec."""

    scored = opportunity.opportunity
    if scored.short_venue not in opportunity.venue_markets:
        raise ValueError(f"Missing venue_markets entry for short_venue: {scored.short_venue}")
    if scored.long_venue not in opportunity.venue_markets:
        raise ValueError(f"Missing venue_markets entry for long_venue: {scored.long_venue}")
    short_market = opportunity.venue_markets[scored.short_venue]
    long_market = opportunity.venue_markets[scored.long_venue]
    base_asset = scored.canonical_symbol.split("-", 1)[0].lower()
    return FundingPairSpec(
        label=f"{base_asset}_{scored.short_venue}_{scored.long_venue}",
        left_venue=scored.short_venue,
        left_symbol=short_market.symbol,
        left_fee_profile=scored.short_fee_profile,
        right_venue=scored.long_venue,
        right_symbol=long_market.symbol,
        right_fee_profile=scored.long_fee_profile,
    )


def build_opportunity_record_from_universe_opportunity(
    *,
    recorded_at: datetime,
    opportunity: FundingUniverseOpportunity,
) -> OpportunityRecord:
    """Convert a universe opportunity into a persisted opportunity record."""

    return OpportunityRecord(
        recorded_at=recorded_at,
        pair=build_pair_spec_from_universe_opportunity(opportunity),
        opportunity=opportunity.opportunity,
    )
