"""Route-stability summaries derived from repeated opportunity scans."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from statistics import fmean, median, pstdev

from carryme_models import OpportunityRecord, RouteStabilitySummary
from carryme_storage import OpportunityHistoryStore


@dataclass
class _RouteStabilityBucket:
    sample_size: int = 0
    window_ids: set[str] = field(default_factory=set)
    latest_recorded_at: datetime | None = None
    latest_roundtrip_edge: float | None = None
    roundtrip_edges: list[float] = field(default_factory=list)
    capacities: list[float] = field(default_factory=list)
    positive_roundtrip_count: int = 0

    def add_record(self, *, record: OpportunityRecord, window_id: str) -> None:
        roundtrip_edge = record.opportunity.one_day_net_edge_after_round_trip
        self.sample_size += 1
        self.window_ids.add(window_id)
        self.roundtrip_edges.append(roundtrip_edge)
        if roundtrip_edge > 0:
            self.positive_roundtrip_count += 1
        capacity = None
        if record.opportunity.capacity is not None:
            capacity = record.opportunity.capacity.max_entry_notional
        if capacity is not None:
            self.capacities.append(capacity)
        if self.latest_recorded_at is None or record.recorded_at >= self.latest_recorded_at:
            self.latest_recorded_at = record.recorded_at
            self.latest_roundtrip_edge = roundtrip_edge


@dataclass
class RouteStabilityService:
    """Summarize repeated-scan route persistence for ranking and filtering."""

    history_store: OpportunityHistoryStore
    sample_limit: int = 5_000
    min_window_cardinality: int = 2

    def build_index(self) -> dict[tuple[str, str, str, str, str], RouteStabilitySummary]:
        """Aggregate recent opportunity history into per-route stability summaries."""

        records = self.history_store.list_recent(limit=self.sample_limit)
        windows_by_timestamp: dict[str, list[OpportunityRecord]] = defaultdict(list)
        for record in records:
            windows_by_timestamp[record.recorded_at.isoformat()].append(record)

        eligible_windows = {
            window_id: window_records
            for window_id, window_records in windows_by_timestamp.items()
            if len(window_records) >= self.min_window_cardinality
        }
        total_windows = len(eligible_windows)
        if total_windows == 0:
            return {}

        buckets: dict[tuple[str, str, str, str, str], _RouteStabilityBucket] = {}
        for window_id, window_records in eligible_windows.items():
            for record in window_records:
                key = (
                    record.opportunity.canonical_symbol,
                    record.opportunity.short_venue,
                    record.opportunity.long_venue,
                    record.opportunity.short_fee_profile,
                    record.opportunity.long_fee_profile,
                )
                buckets.setdefault(key, _RouteStabilityBucket()).add_record(
                    record=record,
                    window_id=window_id,
                )

        result: dict[tuple[str, str, str, str, str], RouteStabilitySummary] = {}
        for key, bucket in buckets.items():
            presence_ratio = len(bucket.window_ids) / total_windows
            positive_share = bucket.positive_roundtrip_count / bucket.sample_size
            mean_roundtrip_edge = fmean(bucket.roundtrip_edges)
            median_roundtrip_edge = float(median(bucket.roundtrip_edges))
            edge_stddev = (
                float(pstdev(bucket.roundtrip_edges))
                if len(bucket.roundtrip_edges) > 1
                else 0.0
            )
            mean_capacity = fmean(bucket.capacities) if bucket.capacities else None
            median_capacity = float(median(bucket.capacities)) if bucket.capacities else None
            capacity_stddev = (
                float(pstdev(bucket.capacities)) if len(bucket.capacities) > 1 else 0.0
            ) if bucket.capacities else None
            edge_stability = _stability_ratio(edge_stddev, mean_roundtrip_edge)
            capacity_stability = (
                _stability_ratio(capacity_stddev, mean_capacity)
                if mean_capacity is not None and capacity_stddev is not None
                else 0.5
            )
            stability_weight = max(
                0.0,
                min(
                    presence_ratio * positive_share * edge_stability * capacity_stability,
                    1.0,
                ),
            )
            stability_score = mean_roundtrip_edge * stability_weight
            result[key] = RouteStabilitySummary(
                canonical_symbol=key[0],
                short_venue=key[1],
                long_venue=key[2],
                short_fee_profile=key[3],
                long_fee_profile=key[4],
                sample_size=bucket.sample_size,
                window_count=len(bucket.window_ids),
                presence_ratio=presence_ratio,
                positive_roundtrip_share=positive_share,
                mean_roundtrip_edge=mean_roundtrip_edge,
                median_roundtrip_edge=median_roundtrip_edge,
                edge_stddev=edge_stddev,
                mean_capacity_notional=mean_capacity,
                median_capacity_notional=median_capacity,
                capacity_stddev=capacity_stddev,
                latest_roundtrip_edge=bucket.latest_roundtrip_edge,
                latest_recorded_at=bucket.latest_recorded_at,
                stability_weight=stability_weight,
                stability_score=stability_score,
            )
        return result

    def list_summaries(
        self,
        *,
        canonical_symbol: str | None = None,
        short_venue: str | None = None,
        long_venue: str | None = None,
        min_sample_size: int = 0,
        min_presence_ratio: float = 0.0,
        limit: int = 50,
    ) -> list[RouteStabilitySummary]:
        """Return ranked route-stability summaries with optional filters."""

        normalized_symbol = canonical_symbol.strip().upper() if canonical_symbol else None
        normalized_short_venue = short_venue.strip().lower() if short_venue else None
        normalized_long_venue = long_venue.strip().lower() if long_venue else None

        summaries: list[RouteStabilitySummary] = []
        for summary in self.build_index().values():
            if normalized_symbol and summary.canonical_symbol.upper() != normalized_symbol:
                continue
            if normalized_short_venue and summary.short_venue != normalized_short_venue:
                continue
            if normalized_long_venue and summary.long_venue != normalized_long_venue:
                continue
            if summary.sample_size < min_sample_size:
                continue
            if summary.presence_ratio < min_presence_ratio:
                continue
            summaries.append(summary)

        ranked = sorted(
            summaries,
            key=lambda item: (
                -item.stability_score,
                -item.stability_weight,
                -item.sample_size,
                item.canonical_symbol,
                item.short_venue,
                item.long_venue,
                item.short_fee_profile,
                item.long_fee_profile,
            ),
        )
        if limit > 0:
            return ranked[:limit]
        return ranked


def _stability_ratio(stddev: float | None, baseline: float | None) -> float:
    if stddev is None or baseline is None:
        return 0.0
    denominator = max(abs(baseline), 1e-9)
    return 1.0 / (1.0 + (stddev / denominator))
