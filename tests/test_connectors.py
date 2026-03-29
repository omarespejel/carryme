import asyncio
from collections.abc import Awaitable, Callable

import httpx
import pytest
from carryme_connectors import (
    ConnectorError,
    ExtendedPublicConnector,
    HyperliquidPublicConnector,
    ParadexPublicConnector,
)

Handler = Callable[[httpx.Request], httpx.Response]


async def _run_with_client(
    base_url: str,
    handler: Handler,
    coro_factory: Callable[[httpx.AsyncClient], Awaitable[None]],
) -> None:
    transport = httpx.MockTransport(handler)
    async with httpx.AsyncClient(base_url=base_url, transport=transport) as client:
        await coro_factory(client)


def test_extended_connector_parses_stats_and_top_of_book() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "GET"
        if request.url.path == "/api/v1/info/markets":
            assert request.url.params["market"] == "STRK-USD"
            return httpx.Response(
                200,
                json={
                    "status": "OK",
                    "data": [
                        {
                            "name": "STRK-USD",
                            "tradingConfig": {
                                "minOrderSize": "10",
                                "minOrderSizeChange": "1",
                                "minPriceChange": "0.00001",
                                "maxLimitOrderValue": "1250000",
                            },
                            "marketStats": {
                                "markPrice": "0.03448",
                                "fundingRate": "0.000013",
                                "openInterest": "280337.789214",
                                "dailyVolume": "157165.809800",
                            },
                        }
                    ],
                },
            )
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "data": {
                    "bid": [{"price": "0.03448", "qty": "117410"}],
                    "ask": [{"price": "0.03449", "qty": "28990"}],
                },
            },
        )

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = ExtendedPublicConnector(client)
        stats = await connector.fetch_market_stats("STRK-USD")
        book = await connector.fetch_top_of_book("STRK-USD")

        assert stats.venue == "extended"
        assert stats.symbol == "STRK-USD"
        assert stats.captured_at.tzinfo is not None
        assert stats.mark_price == pytest.approx(0.03448)
        assert stats.funding_rate == pytest.approx(0.000013)
        assert stats.open_interest == pytest.approx(280337.789214)
        assert stats.daily_volume == pytest.approx(157165.8098)
        assert isinstance(stats.raw, dict)
        assert stats.raw["tradingConfig"]["minOrderSizeChange"] == "1"
        assert book.best_bid_price == pytest.approx(0.03448)
        assert book.best_bid_size == pytest.approx(117410.0)
        assert book.best_ask_price == pytest.approx(0.03449)
        assert book.best_ask_size == pytest.approx(28990.0)

    asyncio.run(_run_with_client("https://api.starknet.extended.exchange", handler, exercise))


def test_extended_connector_raises_for_missing_market_stats_in_list() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "data": [
                    {
                        "name": "STRK-USD",
                        "tradingConfig": {},
                    }
                ],
            },
        )

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = ExtendedPublicConnector(client)
        with pytest.raises(ConnectorError, match="missing marketStats"):
            await connector.fetch_market_stats("STRK-USD")

    asyncio.run(_run_with_client("https://api.starknet.extended.exchange", handler, exercise))


def test_extended_connector_raises_for_unknown_symbol_in_list() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "data": [
                    {
                        "name": "OTHER-USD",
                        "marketStats": {},
                    }
                ],
            },
        )

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = ExtendedPublicConnector(client)
        with pytest.raises(ConnectorError, match="Extended market STRK-USD not found"):
            await connector.fetch_market_stats("STRK-USD")

    asyncio.run(_run_with_client("https://api.starknet.extended.exchange", handler, exercise))


def test_extended_connector_parses_legacy_dict_payload() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "data": {
                    "markPrice": "0.03448",
                    "fundingRate": "0.000013",
                    "openInterest": "280337.789214",
                    "dailyVolume": "157165.809800",
                },
            },
        )

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = ExtendedPublicConnector(client)
        stats = await connector.fetch_market_stats("STRK-USD")

        assert stats.mark_price == pytest.approx(0.03448)
        assert stats.funding_rate == pytest.approx(0.000013)
        assert stats.open_interest == pytest.approx(280337.789214)
        assert stats.daily_volume == pytest.approx(157165.8098)
        assert isinstance(stats.raw, dict)
        assert "tradingConfig" not in stats.raw

    asyncio.run(_run_with_client("https://api.starknet.extended.exchange", handler, exercise))


def test_paradex_connector_parses_stats_and_top_of_book() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/markets/summary":
            assert request.method == "GET"
            assert request.url.params["market"] == "ARB-USD-PERP"
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "symbol": "ARB-USD-PERP",
                            "mark_price": "0.09173146",
                            "funding_rate": "-0.00041270496476",
                            "open_interest": "1351727.4",
                            "volume_24h": "22989.54947000001",
                        }
                    ]
                },
            )
        if request.url.path == "/v1/markets":
            return httpx.Response(
                200,
                json={
                    "results": [
                        {
                            "symbol": "ARB-USD-PERP",
                            "price_tick_size": "0.0001",
                            "order_size_increment": "0.1",
                            "min_notional": "10",
                            "max_order_size": "12000000",
                        }
                    ]
                },
            )
        return httpx.Response(
            200,
            json={
                "market": "ARB-USD-PERP",
                "bid": "0.0913",
                "bid_size": "42584.8",
                "ask": "0.0919",
                "ask_size": "42473",
            },
        )

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = ParadexPublicConnector(client)
        stats = await connector.fetch_market_stats("ARB-USD-PERP")
        book = await connector.fetch_top_of_book("ARB-USD-PERP")

        assert stats.venue == "paradex"
        assert stats.mark_price == pytest.approx(0.09173146)
        assert stats.funding_rate == pytest.approx(-0.00041270496476)
        assert stats.open_interest == pytest.approx(1351727.4)
        assert stats.daily_volume == pytest.approx(22989.54947000001)
        assert isinstance(stats.raw, dict)
        assert stats.raw["price_tick_size"] == "0.0001"
        assert stats.raw["config"]["order_size_increment"] == "0.1"
        assert book.best_bid_price == pytest.approx(0.0913)
        assert book.best_bid_size == pytest.approx(42584.8)
        assert book.best_ask_price == pytest.approx(0.0919)
        assert book.best_ask_size == pytest.approx(42473.0)

    asyncio.run(_run_with_client("https://api.prod.paradex.trade", handler, exercise))


def test_hyperliquid_connector_parses_stats_and_top_of_book() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        assert request.method == "POST"
        payload = request.content.decode("utf-8")
        if "metaAndAssetCtxs" in payload:
            return httpx.Response(
                200,
                json=[
                    {"universe": [{"name": "SOL"}, {"name": "STRK"}]},
                    [
                        {},
                        {
                            "markPx": "0.03451",
                            "funding": "-0.0000418197",
                            "openInterest": "85274675.599999994",
                            "dayNtlVlm": "296593.6985990002",
                        },
                    ],
                ],
            )
        return httpx.Response(
            200,
            json={
                "levels": [
                    [{"px": "0.03452", "sz": "129958.3", "n": 5}],
                    [{"px": "0.03454", "sz": "70290.2", "n": 4}],
                ]
            },
        )

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = HyperliquidPublicConnector(client)
        stats = await connector.fetch_market_stats("STRK")
        book = await connector.fetch_top_of_book("STRK")

        assert stats.venue == "hyperliquid"
        assert stats.mark_price == pytest.approx(0.03451)
        assert stats.funding_rate == pytest.approx(-0.0000418197)
        assert stats.open_interest == pytest.approx(85274675.599999994)
        assert stats.daily_volume == pytest.approx(296593.6985990002)
        assert isinstance(stats.raw, list)
        assert book.best_bid_price == pytest.approx(0.03452)
        assert book.best_bid_size == pytest.approx(129958.3)
        assert book.best_ask_price == pytest.approx(0.03454)
        assert book.best_ask_size == pytest.approx(70290.2)

    asyncio.run(_run_with_client("https://api.hyperliquid.xyz", handler, exercise))


def test_extended_connector_lists_deterministic_perp_symbols() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "data": [
                    {"name": "strk-usd"},
                    {"name": "LIT-USD"},
                    {"name": "LIT-USD"},
                    {"name": "SPOT_ONLY"},
                ]
            },
        )

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = ExtendedPublicConnector(client)
        assert await connector.list_market_symbols() == ["LIT-USD", "STRK-USD"]

    asyncio.run(_run_with_client("https://api.starknet.extended.exchange", handler, exercise))


def test_paradex_connector_lists_deterministic_symbols_and_rejects_malformed_rows() -> None:
    def malformed_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"results": ["bad-row"]})

    async def exercise_malformed(client: httpx.AsyncClient) -> None:
        connector = ParadexPublicConnector(client)
        with pytest.raises(ConnectorError, match="market row must be an object"):
            await connector.list_market_symbols()

    asyncio.run(
        _run_with_client("https://api.prod.paradex.trade", malformed_handler, exercise_malformed)
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "results": [
                    {"symbol": "strk-usd-perp"},
                    {"symbol": "ARB-USD-PERP"},
                    {"symbol": "ARB-USD-PERP"},
                    {"symbol": "ARB-USD"},
                ]
            },
        )

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = ParadexPublicConnector(client)
        assert await connector.list_market_symbols() == ["ARB-USD-PERP", "STRK-USD-PERP"]

    asyncio.run(_run_with_client("https://api.prod.paradex.trade", handler, exercise))


def test_hyperliquid_connector_lists_deterministic_symbols_and_rejects_malformed_rows() -> None:
    def malformed_handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=[{"universe": ["bad-row"]}, []])

    async def exercise_malformed(client: httpx.AsyncClient) -> None:
        connector = HyperliquidPublicConnector(client)
        with pytest.raises(ConnectorError, match="rows must be objects"):
            await connector.list_market_symbols()

    asyncio.run(
        _run_with_client("https://api.hyperliquid.xyz", malformed_handler, exercise_malformed)
    )

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {"universe": [{"name": "strk"}, {"name": "BTC"}, {"name": "BTC"}]},
                [{}, {}, {}],
            ],
        )

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = HyperliquidPublicConnector(client)
        assert await connector.list_market_symbols() == ["BTC", "STRK"]

    asyncio.run(_run_with_client("https://api.hyperliquid.xyz", handler, exercise))


def test_connector_requires_async_client() -> None:
    async def exercise() -> None:
        connector = ExtendedPublicConnector()
        with pytest.raises(ConnectorError, match="requires an AsyncClient"):
            await connector.fetch_market_stats("STRK-USD")

    asyncio.run(exercise())


def test_extended_connector_retries_transient_server_errors() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            return httpx.Response(503, request=request, json={"error": "temporary"})
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "data": {
                    "markPrice": "0.03448",
                    "fundingRate": "0.000013",
                    "openInterest": "280337.789214",
                    "dailyVolume": "157165.809800",
                },
            },
        )

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = ExtendedPublicConnector(client, base_backoff_seconds=0.0)
        stats = await connector.fetch_market_stats("STRK-USD")

        assert attempts == 3
        assert stats.mark_price == pytest.approx(0.03448)

    asyncio.run(_run_with_client("https://api.starknet.extended.exchange", handler, exercise))


def test_extended_connector_retries_rate_limit_responses() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts < 3:
            return httpx.Response(
                429,
                request=request,
                headers={"Retry-After": "0"},
                json={"error": "slow down"},
            )
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "data": {
                    "markPrice": "0.03448",
                    "fundingRate": "0.000013",
                    "openInterest": "280337.789214",
                    "dailyVolume": "157165.809800",
                },
            },
        )

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = ExtendedPublicConnector(client, base_backoff_seconds=0.0)
        stats = await connector.fetch_market_stats("STRK-USD")

        assert attempts == 3
        assert stats.mark_price == pytest.approx(0.03448)

    asyncio.run(_run_with_client("https://api.starknet.extended.exchange", handler, exercise))


def test_extended_connector_preserves_http_status_on_non_retryable_failure() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            404,
            request=request,
            json={"error": "missing"},
        )

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = ExtendedPublicConnector(client, base_backoff_seconds=0.0)
        with pytest.raises(ConnectorError, match="request failed with status 404") as exc_info:
            await connector.fetch_market_stats("STRK-USD")

        assert exc_info.value.status_code == 404

    asyncio.run(_run_with_client("https://api.starknet.extended.exchange", handler, exercise))


def test_extended_connector_raises_for_invalid_numeric_values() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json={
                "status": "OK",
                "data": {
                    "markPrice": "N/A",
                    "fundingRate": "0.000013",
                    "openInterest": "280337.789214",
                    "dailyVolume": "157165.809800",
                },
            },
        )

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = ExtendedPublicConnector(client)
        with pytest.raises(ConnectorError, match="Cannot parse numeric value"):
            await connector.fetch_market_stats("STRK-USD")

    asyncio.run(_run_with_client("https://api.starknet.extended.exchange", handler, exercise))


def test_extended_connector_rejects_missing_data_object() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/api/v1/info/markets":
            return httpx.Response(200, json={"status": "OK"})
        return httpx.Response(200, json={"status": "OK", "data": "not-a-dict"})

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = ExtendedPublicConnector(client)
        with pytest.raises(ConnectorError, match="market stats missing data object"):
            await connector.fetch_market_stats("STRK-USD")
        with pytest.raises(ConnectorError, match="orderbook missing data object"):
            await connector.fetch_top_of_book("STRK-USD")

    asyncio.run(_run_with_client("https://api.starknet.extended.exchange", handler, exercise))


def test_extended_connector_handles_empty_orderbook_levels() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "OK", "data": {"bid": [], "ask": []}})

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = ExtendedPublicConnector(client)
        book = await connector.fetch_top_of_book("STRK-USD")

        assert book.best_bid_price is None
        assert book.best_bid_size is None
        assert book.best_ask_price is None
        assert book.best_ask_size is None

    asyncio.run(_run_with_client("https://api.starknet.extended.exchange", handler, exercise))


def test_paradex_connector_does_not_retry_client_errors() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(404, request=request, json={"error": "missing"})

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = ParadexPublicConnector(client, base_backoff_seconds=0.0)
        with pytest.raises(ConnectorError, match="status 404"):
            await connector.fetch_market_stats("ARB-USD-PERP")
        # test_paradex_connector_does_not_retry_client_errors still sees 2 initial
        # requests because fetch_market_stats issues both metadata calls concurrently.
        assert attempts == 2

    asyncio.run(_run_with_client("https://api.prod.paradex.trade", handler, exercise))


def test_hyperliquid_connector_raises_for_misaligned_contexts() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200,
            json=[
                {"universe": [{"name": "SOL"}, {"name": "STRK"}]},
                [{}],
            ],
        )

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = HyperliquidPublicConnector(client)
        with pytest.raises(ConnectorError, match="contexts missing entry for STRK"):
            await connector.fetch_market_stats("STRK")

    asyncio.run(_run_with_client("https://api.hyperliquid.xyz", handler, exercise))


def test_hyperliquid_connector_rejects_malformed_orderbook_levels() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"levels": [123, []]})

    async def exercise(client: httpx.AsyncClient) -> None:
        connector = HyperliquidPublicConnector(client)
        with pytest.raises(ConnectorError, match="side levels must be lists"):
            await connector.fetch_top_of_book("STRK")

    asyncio.run(_run_with_client("https://api.hyperliquid.xyz", handler, exercise))
