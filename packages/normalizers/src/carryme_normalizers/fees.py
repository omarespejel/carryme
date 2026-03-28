"""Trading fee profile normalization across venues."""

from __future__ import annotations

from datetime import UTC, datetime

from carryme_models.normalization import (
    FeeModifierType,
    ModifierValueUnit,
    TradingFeeProfile,
)

FEE_TABLE_AS_OF = datetime(2026, 3, 29, tzinfo=UTC)


def _profile(
    *,
    venue: str,
    profile: str,
    maker_fee_rate: float,
    taker_fee_rate: float,
    notes: list[str],
    modifier_type: FeeModifierType = "none",
    modifier_value: float | None = None,
    modifier_value_unit: ModifierValueUnit = "none",
) -> TradingFeeProfile:
    return TradingFeeProfile(
        venue=venue,
        profile=profile,
        maker_fee_rate=maker_fee_rate,
        taker_fee_rate=taker_fee_rate,
        modifier_type=modifier_type,
        modifier_value=modifier_value,
        modifier_value_unit=modifier_value_unit,
        source=f"docs:{venue}:trading-fees",
        as_of=FEE_TABLE_AS_OF,
        notes=notes,
    )


FEE_PROFILES: dict[str, dict[str, TradingFeeProfile]] = {
    "extended": {
        "default": _profile(
            venue="extended",
            profile="default",
            maker_fee_rate=0.0,
            taker_fee_rate=0.00025,
            notes=["Flat fee schedule from Extended trading fees."],
        ),
        "maker_share_0_5pct": _profile(
            venue="extended",
            profile="maker_share_0_5pct",
            maker_fee_rate=-0.00002,
            taker_fee_rate=0.00025,
            modifier_type="maker_share_fraction",
            modifier_value=0.005,
            modifier_value_unit="fraction",
            notes=["Effective maker fee after the 0.002% rebate tier."],
        ),
        "maker_share_1pct": _profile(
            venue="extended",
            profile="maker_share_1pct",
            maker_fee_rate=-0.00004,
            taker_fee_rate=0.00025,
            modifier_type="maker_share_fraction",
            modifier_value=0.01,
            modifier_value_unit="fraction",
            notes=["Effective maker fee after the 0.004% rebate tier."],
        ),
        "maker_share_2_5pct": _profile(
            venue="extended",
            profile="maker_share_2_5pct",
            maker_fee_rate=-0.00008,
            taker_fee_rate=0.00025,
            modifier_type="maker_share_fraction",
            modifier_value=0.025,
            modifier_value_unit="fraction",
            notes=["Effective maker fee after the 0.008% rebate tier."],
        ),
        "maker_share_5pct": _profile(
            venue="extended",
            profile="maker_share_5pct",
            maker_fee_rate=-0.00013,
            taker_fee_rate=0.00025,
            modifier_type="maker_share_fraction",
            modifier_value=0.05,
            modifier_value_unit="fraction",
            notes=["Effective maker fee after the 0.013% rebate tier."],
        ),
    },
    "hyperliquid": {
        "tier0": _profile(
            venue="hyperliquid",
            profile="tier0",
            maker_fee_rate=0.00015,
            taker_fee_rate=0.00045,
            notes=["Base perp tier for less than $5M rolling 14 day volume."],
        ),
        "tier1": _profile(
            venue="hyperliquid",
            profile="tier1",
            maker_fee_rate=0.00012,
            taker_fee_rate=0.0004,
            modifier_type="rolling_volume_usd",
            modifier_value=5_000_000,
            modifier_value_unit="usd_notional",
            notes=["Base perp tier for more than $5M rolling 14 day volume."],
        ),
        "tier2": _profile(
            venue="hyperliquid",
            profile="tier2",
            maker_fee_rate=0.00008,
            taker_fee_rate=0.00035,
            modifier_type="rolling_volume_usd",
            modifier_value=25_000_000,
            modifier_value_unit="usd_notional",
            notes=["Base perp tier for more than $25M rolling 14 day volume."],
        ),
        "tier3": _profile(
            venue="hyperliquid",
            profile="tier3",
            maker_fee_rate=0.00004,
            taker_fee_rate=0.0003,
            modifier_type="rolling_volume_usd",
            modifier_value=100_000_000,
            modifier_value_unit="usd_notional",
            notes=["Base perp tier for more than $100M rolling 14 day volume."],
        ),
        "tier4": _profile(
            venue="hyperliquid",
            profile="tier4",
            maker_fee_rate=0.0,
            taker_fee_rate=0.00028,
            modifier_type="rolling_volume_usd",
            modifier_value=500_000_000,
            modifier_value_unit="usd_notional",
            notes=["Base perp tier for more than $500M rolling 14 day volume."],
        ),
        "tier5": _profile(
            venue="hyperliquid",
            profile="tier5",
            maker_fee_rate=0.0,
            taker_fee_rate=0.00026,
            modifier_type="rolling_volume_usd",
            modifier_value=2_000_000_000,
            modifier_value_unit="usd_notional",
            notes=["Base perp tier for more than $2B rolling 14 day volume."],
        ),
        "tier6": _profile(
            venue="hyperliquid",
            profile="tier6",
            maker_fee_rate=0.0,
            taker_fee_rate=0.00024,
            modifier_type="rolling_volume_usd",
            modifier_value=7_000_000_000,
            modifier_value_unit="usd_notional",
            notes=["Base perp tier for more than $7B rolling 14 day volume."],
        ),
    },
    "paradex": {
        "retail": _profile(
            venue="paradex",
            profile="retail",
            maker_fee_rate=0.0,
            taker_fee_rate=0.000075,
            notes=["Retail profile from Paradex trading fees."],
        ),
        "pro": _profile(
            venue="paradex",
            profile="pro",
            maker_fee_rate=0.0,
            taker_fee_rate=0.0002,
            notes=["Pro profile from Paradex trading fees."],
        ),
        "pro_fastfills": _profile(
            venue="paradex",
            profile="pro_fastfills",
            maker_fee_rate=0.0,
            taker_fee_rate=0.00014,
            modifier_type="fastfills_discount_fraction",
            modifier_value=0.30,
            modifier_value_unit="fraction",
            notes=["Pro taker fee after the documented 30% FastFills discount."],
        ),
    },
}


def list_fee_profiles(venue: str) -> list[TradingFeeProfile]:
    """Return all known fee profiles for a venue."""

    key = venue.strip().lower()
    profiles = FEE_PROFILES.get(key)
    if profiles is None:
        raise ValueError(f"Unsupported venue for fee normalization: {venue}")
    return [profile.model_copy(deep=True) for profile in profiles.values()]


def get_fee_profile(venue: str, profile: str) -> TradingFeeProfile:
    """Return a concrete fee profile by venue and profile name."""

    key = venue.strip().lower()
    profiles = FEE_PROFILES.get(key)
    if profiles is None:
        raise ValueError(f"Unsupported venue for fee normalization: {venue}")

    normalized_profile = profile.strip().lower()
    result = profiles.get(normalized_profile)
    if result is None:
        raise ValueError(f"Unknown fee profile {profile!r} for venue {venue!r}")
    return result.model_copy(deep=True)
