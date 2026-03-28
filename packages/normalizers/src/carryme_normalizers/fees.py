"""Trading fee profile normalization across venues."""

from __future__ import annotations

from carryme_models.normalization import TradingFeeProfile

FEE_PROFILES: dict[str, dict[str, TradingFeeProfile]] = {
    "extended": {
        "default": TradingFeeProfile(
            venue="extended",
            profile="default",
            maker_fee_rate=0.0,
            taker_fee_rate=0.00025,
            notes=["Flat fee schedule from Extended trading fees."],
        ),
        "maker_share_0_5pct": TradingFeeProfile(
            venue="extended",
            profile="maker_share_0_5pct",
            maker_fee_rate=-0.00002,
            taker_fee_rate=0.00025,
            notes=["Effective maker fee after the 0.002% rebate tier."],
        ),
        "maker_share_1pct": TradingFeeProfile(
            venue="extended",
            profile="maker_share_1pct",
            maker_fee_rate=-0.00004,
            taker_fee_rate=0.00025,
            notes=["Effective maker fee after the 0.004% rebate tier."],
        ),
        "maker_share_2_5pct": TradingFeeProfile(
            venue="extended",
            profile="maker_share_2_5pct",
            maker_fee_rate=-0.00008,
            taker_fee_rate=0.00025,
            notes=["Effective maker fee after the 0.008% rebate tier."],
        ),
        "maker_share_5pct": TradingFeeProfile(
            venue="extended",
            profile="maker_share_5pct",
            maker_fee_rate=-0.00013,
            taker_fee_rate=0.00025,
            notes=["Effective maker fee after the 0.013% rebate tier."],
        ),
    },
    "hyperliquid": {
        "tier0": TradingFeeProfile(
            venue="hyperliquid",
            profile="tier0",
            maker_fee_rate=0.00015,
            taker_fee_rate=0.00045,
            notes=["Base perp tier for less than $5M rolling 14 day volume."],
        ),
        "tier1": TradingFeeProfile(
            venue="hyperliquid",
            profile="tier1",
            maker_fee_rate=0.00012,
            taker_fee_rate=0.0004,
            notes=["Base perp tier for more than $5M rolling 14 day volume."],
        ),
        "tier2": TradingFeeProfile(
            venue="hyperliquid",
            profile="tier2",
            maker_fee_rate=0.00008,
            taker_fee_rate=0.00035,
            notes=["Base perp tier for more than $25M rolling 14 day volume."],
        ),
        "tier3": TradingFeeProfile(
            venue="hyperliquid",
            profile="tier3",
            maker_fee_rate=0.00004,
            taker_fee_rate=0.0003,
            notes=["Base perp tier for more than $100M rolling 14 day volume."],
        ),
        "tier4": TradingFeeProfile(
            venue="hyperliquid",
            profile="tier4",
            maker_fee_rate=0.0,
            taker_fee_rate=0.00028,
            notes=["Base perp tier for more than $500M rolling 14 day volume."],
        ),
        "tier5": TradingFeeProfile(
            venue="hyperliquid",
            profile="tier5",
            maker_fee_rate=0.0,
            taker_fee_rate=0.00026,
            notes=["Base perp tier for more than $2B rolling 14 day volume."],
        ),
        "tier6": TradingFeeProfile(
            venue="hyperliquid",
            profile="tier6",
            maker_fee_rate=0.0,
            taker_fee_rate=0.00024,
            notes=["Base perp tier for more than $7B rolling 14 day volume."],
        ),
    },
    "paradex": {
        "retail": TradingFeeProfile(
            venue="paradex",
            profile="retail",
            maker_fee_rate=0.0,
            taker_fee_rate=0.000075,
            notes=["Retail profile from Paradex trading fees."],
        ),
        "pro": TradingFeeProfile(
            venue="paradex",
            profile="pro",
            maker_fee_rate=0.0,
            taker_fee_rate=0.0002,
            notes=["Pro profile from Paradex trading fees."],
        ),
        "pro_fastfills": TradingFeeProfile(
            venue="paradex",
            profile="pro_fastfills",
            maker_fee_rate=0.0,
            taker_fee_rate=0.00014,
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
    return list(profiles.values())


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
    return result
