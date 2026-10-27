"""Deterministic public response tests; no exchange or trading credentials."""

from __future__ import annotations

import json
from decimal import Decimal

from gold_basis_mm_bot.squeeze_engine.connectors import (
    BinanceAdapter, BybitAdapter, CollectionResult, KucoinAdapter, MarketFetchError, PublicHttpClient, epoch_utc,
)
from gold_basis_mm_bot.squeeze_engine.storage import RawJsonlStore


class FakeClient:
    def __init__(self, replies):
        self.replies = replies
        self.calls = []

    def get_json(self, url, params):
        self.calls.append((url, dict(params)))
        payload = self.replies[url]
        if isinstance(payload, Exception):
            raise payload
        return payload


def values(result, metric):
    return [item for item in result.snapshots if item.metric == metric]


def test_kucoin_contract_count_multiplied_by_verified_100_and_raw_retained(tmp_path):
    client = FakeClient({
        "https://api-futures.kucoin.com/api/v1/contracts/IRYSUSDTM": {
            "code": "200000", "data": {
                "symbol": "IRYSUSDTM", "status": "Open", "baseCurrency": "IRYS",
                "quoteCurrency": "USDT", "settleCurrency": "USDT", "isInverse": False, "multiplier": 100,
                "openInterest": "5406720", "lastTradePrice": "0.068766",
                "markPrice": "0.074551", "indexPrice": "0.074976",
                "fundingFeeRate": "0.00296", "volumeOf24h": "2781460400",
            },
        },
        "https://api.kucoin.com/api/v1/market/orderbook/level1": {
            "code": "200000", "data": {"price": "0.06712", "time": 1792786509569},
        },
    })
    path = tmp_path / "raw.jsonl"
    result = KucoinAdapter(client, RawJsonlStore(path)).collect("IRYS")
    oi = values(result, "open_interest")[0]
    assert oi.value == Decimal("540672000")
    assert oi.related_value == Decimal("540672000") * Decimal("0.074551")
    assert oi.unit == "tokens" and oi.related_unit == "USD"
    assert "missing_source_timestamp" in oi.quality_flags
    assert values(result, "premium_bps")
    assert values(result, "spot_last")[0].provenance.source_timestamp is not None
    raw = [json.loads(line) for line in path.read_text().splitlines()]
    assert len(raw) == 2 and {item["raw_id"] for item in raw}
    assert oi.provenance.raw_id == raw[0]["raw_id"]


def test_kucoin_inverse_or_missing_multiplier_never_emits_oi(tmp_path):
    for inverse, multiplier in ((True, 100), (False, None)):
        client = FakeClient({
            "https://api-futures.kucoin.com/api/v1/contracts/IRYSUSDTM": {
                "code": "200000", "data": {
                    "symbol": "IRYSUSDTM", "status": "Open", "baseCurrency": "IRYS",
                    "quoteCurrency": "USDT", "settleCurrency": "USDT", "isInverse": inverse, "multiplier": multiplier,
                    "openInterest": "100", "markPrice": "0.1", "indexPrice": "0.1",
                },
            },
            "https://api.kucoin.com/api/v1/market/orderbook/level1": MarketFetchError("unlisted"),
        })
        result = KucoinAdapter(client, RawJsonlStore(tmp_path / f"{inverse}-{multiplier}.jsonl")).collect("IRYS")
        assert not values(result, "open_interest")
        assert result.errors


def test_binance_linear_oi_and_absent_spot_kept_separate(tmp_path):
    client = FakeClient({
        "https://fapi.binance.com/fapi/v1/exchangeInfo": {
            "symbols": [{"symbol": "IRYSUSDT", "status": "TRADING", "contractType": "PERPETUAL",
                         "baseAsset": "IRYS", "quoteAsset": "USDT"}]},
        "https://fapi.binance.com/fapi/v1/premiumIndex": {
            "symbol": "IRYSUSDT", "markPrice": "0.07", "indexPrice": "0.069",
            "lastFundingRate": "0.005", "time": 1792786509000},
        "https://fapi.binance.com/fapi/v1/openInterest": {
            "symbol": "IRYSUSDT", "openInterest": "407422116", "time": 1792786504936},
        "https://fapi.binance.com/fapi/v1/ticker/24hr": {
            "symbol": "IRYSUSDT", "lastPrice": "0.0686", "volume": "33316940088",
            "closeTime": 1792786509000},
        "https://api.binance.com/api/v3/exchangeInfo": MarketFetchError("HTTP 400: symbol absent"),
    })
    result = BinanceAdapter(client, RawJsonlStore(tmp_path / "raw.jsonl")).collect("IRYS")
    oi = values(result, "open_interest")[0]
    assert oi.value == Decimal("407422116")
    assert oi.related_value == Decimal("28519548.12")
    assert not values(result, "spot_last")
    assert values(result, "perp_last") and values(result, "funding_rate")


def test_bybit_oi_uses_base_coin_and_empty_spot_is_gap(tmp_path):
    client = FakeClient({
        "https://api.bybit.com/v5/market/instruments-info": {
            "retCode": 0, "result": {"list": [{"symbol": "IRYSUSDT", "status": "Trading",
                                               "contractType": "LinearPerpetual", "baseCoin": "IRYS",
                                               "quoteCoin": "USDT", "settleCoin": "USDT"}]}},
        "https://api.bybit.com/v5/market/tickers": {
            "retCode": 0, "time": 1792786509000,
            "result": {"list": [{"symbol": "IRYSUSDT", "markPrice": "0.069071",
                                  "indexPrice": "0.067285", "lastPrice": "0.067459",
                                  "openInterest": "186777450", "openInterestValue": "12900905.25",
                                  "fundingRate": "0.00549055", "volume24h": "7331318840"}]},
        },
    })
    # The fake returns the same data for both categories. Spot is not inferred
    # from a linear response: the fake client explicitly returns an empty list.
    class CategoryClient(FakeClient):
        def get_json(self, url, params):
            if url.endswith("/v5/market/tickers") and params.get("category") == "spot":
                return {"retCode": 0, "time": 1792786509000, "result": {"list": []}}
            return super().get_json(url, params)

    result = BybitAdapter(CategoryClient(client.replies), RawJsonlStore(tmp_path / "raw.jsonl")).collect("IRYS")
    oi = values(result, "open_interest")[0]
    assert oi.value == Decimal("186777450")
    assert oi.related_value == oi.value * Decimal("0.069071")
    assert oi.provenance.source_timestamp is None
    assert "response_time_only" in oi.quality_flags
    assert values(result, "premium_bps")
    assert not values(result, "spot_last")


def test_epoch_parser_preserves_documented_resolution():
    assert epoch_utc(1792786509) == epoch_utc(1792786509000)
    assert epoch_utc(1792786509000000000) == epoch_utc(1792786509000)


def test_bybit_business_error_isolated_without_fake_instrument(tmp_path):
    client = FakeClient({
        "https://api.bybit.com/v5/market/instruments-info": {
            "retCode": 10001, "retMsg": "unknown symbol", "result": {"list": []},
        },
    })
    result = BybitAdapter(client, RawJsonlStore(tmp_path / "raw.jsonl")).collect("IRYS")
    assert not result.snapshots
    assert result.errors and "listing" in result.errors[0]


def test_nonpositive_mark_never_reaches_price_chart_or_oi(tmp_path):
    client = FakeClient({
        "https://api-futures.kucoin.com/api/v1/contracts/IRYSUSDTM": {
            "code": "200000", "data": {
                "symbol": "IRYSUSDTM", "status": "Open", "baseCurrency": "IRYS",
                "quoteCurrency": "USDT", "settleCurrency": "USDT", "isInverse": False, "multiplier": 100,
                "openInterest": "100", "lastTradePrice": "0.1", "markPrice": "0",
                "indexPrice": "0.1",
            },
        },
        "https://api.kucoin.com/api/v1/market/orderbook/level1": MarketFetchError("unlisted"),
    })
    result = KucoinAdapter(client, RawJsonlStore(tmp_path / "raw.jsonl")).collect("IRYS")
    assert values(result, "perp_last")
    assert not values(result, "mark_price")
    assert not values(result, "premium_bps")
    assert not values(result, "open_interest")
    assert any("invalid" in error for error in result.errors)


def test_binance_reuses_large_metadata_and_rejects_misaligned_oi_mark(tmp_path):
    client = FakeClient({
        "https://fapi.binance.com/fapi/v1/exchangeInfo": {
            "symbols": [{"symbol": "IRYSUSDT", "status": "TRADING", "contractType": "PERPETUAL",
                         "baseAsset": "IRYS", "quoteAsset": "USDT"}]},
        "https://fapi.binance.com/fapi/v1/premiumIndex": {
            "symbol": "IRYSUSDT", "markPrice": "0.07", "indexPrice": "0.069",
            "time": 1792786509000},
        "https://fapi.binance.com/fapi/v1/openInterest": {
            "symbol": "IRYSUSDT", "openInterest": "1000", "time": 1792786450000},
        "https://fapi.binance.com/fapi/v1/ticker/24hr": {
            "symbol": "IRYSUSDT", "lastPrice": "0.0686", "volume": "1000",
            "closeTime": 1792786509000},
        "https://api.binance.com/api/v3/exchangeInfo": MarketFetchError("HTTP 400", status_code=400),
    })
    adapter = BinanceAdapter(client, RawJsonlStore(tmp_path / "raw.jsonl"))
    for _ in range(2):
        result = adapter.collect("IRYS")
        assert not values(result, "open_interest")
        assert any("30 seconds" in error for error in result.errors)
    assert sum(url.endswith("/fapi/v1/exchangeInfo") for url, _ in client.calls) == 1


def test_public_client_retains_exact_response_json_text(monkeypatch):
    raw_body = b'{"price":0.001,"count":2}'

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self, size):
            return raw_body

    monkeypatch.setattr("gold_basis_mm_bot.squeeze_engine.connectors.urlopen", lambda *_args, **_kwargs: Response())
    reply = PublicHttpClient(retries=0).get_json("https://public.example.test/market", {})
    assert reply.raw_text.encode() == raw_body
    assert reply.parsed["price"] == Decimal("0.001")


def test_invalid_public_json_body_is_archived_before_parse_failure(tmp_path, monkeypatch):
    raw_body = b'{"price":NaN,broken}'

    class Response:
        status = 200

        def __enter__(self):
            return self

        def __exit__(self, *args):
            return None

        def read(self, size):
            return raw_body

    monkeypatch.setattr("gold_basis_mm_bot.squeeze_engine.connectors.urlopen", lambda *_args, **_kwargs: Response())
    path = tmp_path / "raw.jsonl"
    adapter = KucoinAdapter(PublicHttpClient(retries=0), RawJsonlStore(path))
    result = CollectionResult()
    assert adapter._fetch("https://public.example.test", "/market", {}, result) is None
    record = json.loads(path.read_text().splitlines()[0])
    import base64
    assert base64.b64decode(record["raw_body_base64"]) == raw_body
    assert result.errors and "invalid JSON" in result.errors[0]
