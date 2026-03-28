"""Funding normalization across venues."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Literal

from carryme_models.normalization import FundingRateNormalization

AccrualStyle = Literal["continuous", "scheduled"]


@dataclass(frozen=True)
class FundingRule:
    quoted_interval_hours: float
    payment_interval_hours: float | None
    formula_interval_hours: float | None
    accrual_style: AccrualStyle
    notes: tuple[str, ...]


FUNDING_RULES: dict[str, FundingRule] = {
    "extended": FundingRule(
        quoted_interval_hours=1.0,
        payment_interval_hours=1.0,
        formula_interval_hours=8.0,
        accrual_style="scheduled",
        notes=(
            "Funding is charged hourly on Extended.",
            "The funding formula uses an 8 hour realization period.",
        ),
    ),
    "hyperliquid": FundingRule(
        quoted_interval_hours=1.0,
        payment_interval_hours=1.0,
        formula_interval_hours=8.0,
        accrual_style="scheduled",
        notes=(
            "Funding is paid every hour on Hyperliquid.",
            "The funding formula applies to an 8 hour rate and settles one eighth per hour.",
        ),
    ),
    "paradex": FundingRule(
        quoted_interval_hours=8.0,
        payment_interval_hours=None,
        formula_interval_hours=8.0,
        accrual_style="continuous",
        notes=(
            "Funding accrues continuously on Paradex and realizes when the position changes.",
            "The funding premium represents an 8 hour amount.",
        ),
    ),
}


def normalize_funding_rate(venue: str, raw_rate: float | None) -> FundingRateNormalization:
    """Convert a venue-specific funding quote to hourly and daily equivalents."""

    key = venue.strip().lower()
    rule = FUNDING_RULES.get(key)
    if rule is None:
        raise ValueError(f"Unsupported venue for funding normalization: {venue}")

    hourly_rate = None
    daily_rate = None
    if raw_rate is not None:
        hourly_rate = raw_rate / rule.quoted_interval_hours
        daily_rate = hourly_rate * 24.0

    return FundingRateNormalization(
        venue=key,
        raw_rate=raw_rate,
        quoted_interval_hours=rule.quoted_interval_hours,
        payment_interval_hours=rule.payment_interval_hours,
        formula_interval_hours=rule.formula_interval_hours,
        accrual_style=rule.accrual_style,
        hourly_rate=hourly_rate,
        daily_rate=daily_rate,
        notes=list(rule.notes),
    )
