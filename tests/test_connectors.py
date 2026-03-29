import asyncio
from collections.abc import Awaitable, Callable

import httpx
from carryme_connectors import (
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
        if request.url.path == "/api/v1/info/markets":
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
        assert stats.mark_price == 0.03448
        assert stats.funding_rate == 0.000013
        assert stats.open_interest == 280337.789214
        assert stats.daily_volume == 157165.8098
        assert stats.raw["tradingConfig"]["minOrderSizeChange"] == "1"
        assert book.best_bid_price == 0.03448
        assert book.best_bid_size == 117410.0
        assert book.best_ask_price == 0.03449
        assert book.best_ask_size == 28990.0

    asyncio.run(_run_with_client("https://api.starknet.extended.exchange", handler, exercise))


def test_paradex_connector_parses_stats_and_top_of_book() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path == "/v1/markets/summary":
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
        assert stats.mark_price == 0.09173146
        assert stats.funding_rate == -0.00041270496476
        assert stats.open_interest == 1351727.4
        assert stats.daily_volume == 22989.54947000001
        assert stats.raw["price_tick_size"] == "0.0001"
        assert stats.raw["config"]["order_size_increment"] == "0.1"
        assert book.best_bid_price == 0.0913
        assert book.best_bid_size == 42584.8
        assert book.best_ask_price == 0.0919
        assert book.best_ask_size == 42473.0

    asyncio.run(_run_with_client("https://api.prod.paradex.trade", handler, exercise))


def test_hyperliquid_connector_parses_stats_and_top_of_book() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        payload = request.content.decode("utf-8")
        if 'metaAndAssetCtxs' in payload:
            return httpx.Response(
                200,
                json=[
                    {"universe": [{"name": "STRK"}, {"name": "SOL"}]},
                    [
                        {
                            "markPx": "0.03451",
                            "funding": "-0.0000418197",
                            "openInterest": "85274675.599999994",
                            "dayNtlVlm": "296593.6985990002",
                        },
                        {},
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
        assert stats.mark_price == 0.03451
        assert stats.funding_rate == -0.0000418197
        assert stats.open_interest == 85274675.599999994
        assert stats.daily_volume == 296593.6985990002
        assert book.best_bid_price == 0.03452
        assert book.best_bid_size == 129958.3
        assert book.best_ask_price == 0.03454
        assert book.best_ask_size == 70290.2

    asyncio.run(_run_with_client("https://api.hyperliquid.xyz", handler, exercise))
