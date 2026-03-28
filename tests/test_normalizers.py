import pytest
from carryme_models import MarketStats
from carryme_normalizers import (
    NormalizationError,
    get_fee_profile,
    list_fee_profiles,
    normalize_funding_rate,
    normalize_market_snapshot,
    normalize_symbol,
)


def test_normalize_extended_symbol() -> None:
    identity = normalize_symbol("extended", "strk-usd")

    assert identity.venue == "extended"
    assert identity.base_asset == "STRK"
    assert identity.quote_asset == "USD"
    assert identity.canonical_symbol == "STRK-USD-PERP"


def test_normalize_paradex_symbol() -> None:
    identity = normalize_symbol("paradex", "arb-usd-perp")

    assert identity.venue == "paradex"
    assert identity.base_asset == "ARB"
    assert identity.quote_asset == "USD"
    assert identity.canonical_symbol == "ARB-USD-PERP"


def test_normalize_hyperliquid_symbol() -> None:
    identity = normalize_symbol("hyperliquid", "strk")

    assert identity.venue == "hyperliquid"
    assert identity.base_asset == "STRK"
    assert identity.quote_asset == "USD"
    assert identity.canonical_symbol == "STRK-USD-PERP"


def test_reject_invalid_paradex_symbol() -> None:
    with pytest.raises(NormalizationError, match="Invalid Paradex"):
        normalize_symbol("paradex", "ARB-USD")


def test_reject_unsupported_symbol_venue() -> None:
    with pytest.raises(NormalizationError, match="Unsupported venue"):
        normalize_symbol("unknown", "STRK")


def test_reject_invalid_extended_symbol() -> None:
    with pytest.raises(NormalizationError, match="Invalid Extended"):
        normalize_symbol("extended", "STRKUSD")


def test_reject_invalid_hyperliquid_symbol() -> None:
    with pytest.raises(NormalizationError, match="Invalid Hyperliquid"):
        normalize_symbol("hyperliquid", "STRK-USD")


def test_normalize_extended_funding_rate() -> None:
    normalized = normalize_funding_rate("extended", 0.00025)

    assert normalized.quoted_interval_hours == 1.0
    assert normalized.payment_interval_hours == 1.0
    assert normalized.formula_interval_hours == 8.0
    assert normalized.settlement_timing == "fixed_utc_windows"
    assert normalized.settlement_interval_hours == 1.0
    assert normalized.hourly_rate == 0.00025
    assert normalized.daily_rate == 0.006


def test_normalize_paradex_funding_rate() -> None:
    normalized = normalize_funding_rate("paradex", -0.0008)

    assert normalized.quoted_interval_hours == 8.0
    assert normalized.payment_interval_hours is None
    assert normalized.formula_interval_hours == 8.0
    assert normalized.accrual_style == "continuous"
    assert normalized.settlement_timing == "rolling"
    assert normalized.settlement_interval_hours is None
    assert normalized.hourly_rate == -0.0001
    assert normalized.daily_rate == pytest.approx(-0.0024)


def test_normalize_hyperliquid_funding_rate() -> None:
    normalized = normalize_funding_rate("hyperliquid", 0.00005)

    assert normalized.quoted_interval_hours == 1.0
    assert normalized.payment_interval_hours == 1.0
    assert normalized.formula_interval_hours == 8.0
    assert normalized.settlement_timing == "fixed_utc_windows"
    assert normalized.settlement_interval_hours == 1.0
    assert normalized.hourly_rate == 0.00005
    assert normalized.daily_rate == pytest.approx(0.0012)


def test_normalize_funding_rate_none_preserves_metadata() -> None:
    normalized = normalize_funding_rate("extended", None)

    assert normalized.raw_rate is None
    assert normalized.quoted_interval_hours == 1.0
    assert normalized.payment_interval_hours == 1.0
    assert normalized.formula_interval_hours == 8.0
    assert normalized.accrual_style == "scheduled"
    assert normalized.settlement_timing == "fixed_utc_windows"
    assert normalized.hourly_rate is None
    assert normalized.daily_rate is None


def test_normalize_funding_rate_zero_rate() -> None:
    normalized = normalize_funding_rate("paradex", 0.0)

    assert normalized.hourly_rate == pytest.approx(0.0)
    assert normalized.daily_rate == pytest.approx(0.0)
    assert normalized.quoted_interval_hours == 8.0
    assert normalized.accrual_style == "continuous"


def test_list_extended_fee_profiles_includes_rebate_tiers() -> None:
    profiles = {profile.profile: profile for profile in list_fee_profiles("extended")}

    assert profiles["default"].taker_fee_rate == 0.00025
    assert profiles["maker_share_5pct"].maker_fee_rate == -0.00013
    assert profiles["maker_share_5pct"].modifier_type == "maker_share_fraction"
    assert profiles["maker_share_5pct"].modifier_value == pytest.approx(0.05)
    assert profiles["default"].source == "docs:extended:trading-fees"
    assert profiles["default"].as_of.tzinfo is not None


def test_get_paradex_fastfills_fee_profile() -> None:
    profile = get_fee_profile("paradex", "pro_fastfills")

    assert profile.maker_fee_rate == 0.0
    assert profile.taker_fee_rate == 0.00014
    assert profile.modifier_type == "fastfills_discount_fraction"
    assert profile.modifier_value == pytest.approx(0.30)


def test_get_hyperliquid_base_fee_profile() -> None:
    profile = get_fee_profile("hyperliquid", "tier0")

    assert profile.maker_fee_rate == 0.00015
    assert profile.taker_fee_rate == 0.00045
    assert profile.fee_rate_unit == "fraction_of_notional"


def test_fee_profile_accessors_return_defensive_copies() -> None:
    profiles = list_fee_profiles("extended")
    profiles[0].notes.append("mutated")
    profiles[0].taker_fee_rate = 123.0

    reloaded_profiles = list_fee_profiles("extended")
    single_profile = get_fee_profile("extended", "default")

    assert "mutated" not in reloaded_profiles[0].notes
    assert reloaded_profiles[0].taker_fee_rate == pytest.approx(0.00025)
    assert single_profile.taker_fee_rate == pytest.approx(0.00025)


def test_normalize_market_snapshot() -> None:
    market = MarketStats(
        venue="paradex",
        symbol="ARB-USD-PERP",
        mark_price=0.0915,
        funding_rate=-0.0008,
        open_interest=1351727.4,
        daily_volume=22989.5,
    )

    snapshot = normalize_market_snapshot("paradex", market)

    assert snapshot.identity.canonical_symbol == "ARB-USD-PERP"
    assert snapshot.identity.resolution_status == "resolved"
    assert snapshot.market.mark_price == 0.0915
    assert snapshot.funding.settlement_timing == "rolling"
    assert snapshot.funding.daily_rate == pytest.approx(-0.0024)
