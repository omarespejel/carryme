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
    try:
        normalize_symbol("paradex", "ARB-USD")
    except NormalizationError as exc:
        assert "Invalid Paradex" in str(exc)
    else:
        raise AssertionError("Expected invalid symbol to raise")


def test_normalize_extended_funding_rate() -> None:
    normalized = normalize_funding_rate("extended", 0.00025)

    assert normalized.quoted_interval_hours == 1.0
    assert normalized.payment_interval_hours == 1.0
    assert normalized.formula_interval_hours == 8.0
    assert normalized.hourly_rate == 0.00025
    assert normalized.daily_rate == 0.006


def test_normalize_paradex_funding_rate() -> None:
    normalized = normalize_funding_rate("paradex", -0.0008)

    assert normalized.quoted_interval_hours == 8.0
    assert normalized.payment_interval_hours is None
    assert normalized.formula_interval_hours == 8.0
    assert normalized.accrual_style == "continuous"
    assert normalized.hourly_rate == -0.0001
    assert normalized.daily_rate == pytest.approx(-0.0024)


def test_normalize_hyperliquid_funding_rate() -> None:
    normalized = normalize_funding_rate("hyperliquid", 0.00005)

    assert normalized.quoted_interval_hours == 1.0
    assert normalized.payment_interval_hours == 1.0
    assert normalized.formula_interval_hours == 8.0
    assert normalized.hourly_rate == 0.00005
    assert normalized.daily_rate == pytest.approx(0.0012)


def test_list_extended_fee_profiles_includes_rebate_tiers() -> None:
    profiles = {profile.profile: profile for profile in list_fee_profiles("extended")}

    assert profiles["default"].taker_fee_rate == 0.00025
    assert profiles["maker_share_5pct"].maker_fee_rate == -0.00013


def test_get_paradex_fastfills_fee_profile() -> None:
    profile = get_fee_profile("paradex", "pro_fastfills")

    assert profile.maker_fee_rate == 0.0
    assert profile.taker_fee_rate == 0.00014


def test_get_hyperliquid_base_fee_profile() -> None:
    profile = get_fee_profile("hyperliquid", "tier0")

    assert profile.maker_fee_rate == 0.00015
    assert profile.taker_fee_rate == 0.00045


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
    assert snapshot.market.mark_price == 0.0915
    assert snapshot.funding.daily_rate == pytest.approx(-0.0024)
