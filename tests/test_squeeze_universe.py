"""Broad public-data scan: exact instruments, source links and bounded GETs."""

from __future__ import annotations

import json
from datetime import datetime, timezone
from decimal import Decimal

from gold_basis_mm_bot.squeeze_engine.connectors import HttpPayload, MarketFetchError
from gold_basis_mm_bot.squeeze_engine.storage import RawJsonlStore
from gold_basis_mm_bot.squeeze_engine.universe import collect_universe_cycle


class FixtureClient:
    def __init__(self, replies: dict[tuple[str, tuple[tuple[str, str], ...]], object]):
        self.replies = replies
        self.calls: list[tuple[str, tuple[tuple[str, str], ...]]] = []

    def get_json(self, url: str, params: dict[str, str]) -> HttpPayload:
        key = (url, tuple(sorted(params.items())))
        self.calls.append(key)
        reply = self.replies[key]
        if isinstance(reply, Exception):
            raise reply
        raw = json.dumps(reply, separators=(",", ":"))
        return HttpPayload(reply, raw, 200)


def key(url: str, **params: str) -> tuple[str, tuple[tuple[str, str], ...]]:
    return url, tuple(sorted(params.items()))


BINANCE = "https://fapi.binance.com"
BYBIT = "https://api.bybit.com"
KUCOIN = "https://api-futures.kucoin.com"
NOW_MS = 1792800000000


def fixtures() -> dict[tuple[str, tuple[tuple[str, str], ...]], object]:
    return {
        key(BINANCE + "/fapi/v1/exchangeInfo"): {
            "symbols": [
                {"symbol": "IRYSUSDT", "baseAsset": "IRYS", "quoteAsset": "USDT",
                 "marginAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING",
                 "underlyingType": "COIN"},
                {"symbol": "1000IRYSUSDT", "baseAsset": "IRYS", "quoteAsset": "USDT",
                 "marginAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING",
                 "underlyingType": "COIN"},
                {"symbol": "HALTUSDT", "baseAsset": "HALT", "quoteAsset": "USDT",
                 "marginAsset": "USDT", "contractType": "PERPETUAL", "status": "BREAK",
                 "underlyingType": "COIN"},
                {"symbol": "NVDAUSDT", "baseAsset": "NVDA", "quoteAsset": "USDT",
                 "marginAsset": "USDT", "contractType": "PERPETUAL", "status": "TRADING",
                 "underlyingType": "EQUITY"},
            ]
        },
        key(BYBIT + "/v5/market/instruments-info", category="linear", limit="1000"): {
            "retCode": 0, "result": {"category": "linear", "nextPageCursor": "next", "list": [
                {"symbol": "IRYSUSDT", "baseCoin": "IRYS", "quoteCoin": "USDT",
                 "settleCoin": "USDT", "contractType": "LinearPerpetual", "status": "Trading",
                 "marketRegion": "", "underlyingTicker": "", "fundingInterval": 240},
                {"symbol": "STOCKUSDT", "baseCoin": "STOCK", "quoteCoin": "USDT",
                 "settleCoin": "USDT", "contractType": "LinearPerpetual", "status": "Trading",
                 "marketRegion": "US", "underlyingTicker": "STOCK"},
            ]}
        },
        key(BYBIT + "/v5/market/instruments-info", category="linear", limit="1000", cursor="next"): {
            "retCode": 0, "result": {"category": "linear", "nextPageCursor": "", "list": [
                {"symbol": "CLOSEDUSDT", "baseCoin": "CLOSED", "quoteCoin": "USDT",
                 "settleCoin": "USDT", "contractType": "LinearPerpetual", "status": "Closed",
                 "marketRegion": "", "underlyingTicker": ""},
            ]}
        },
        key(KUCOIN + "/api/v1/contracts/active"): {
            "code": "200000", "data": [
                {"symbol": "IRYSUSDTM", "baseCurrency": "IRYS", "quoteCurrency": "USDT",
                 "settleCurrency": "USDT", "rootSymbol": "USDT", "isInverse": False,
                 "status": "Open", "assetClass": "CRYPTO", "expireDate": None,
                 "lastTradePrice": "0.101", "markPrice": "0.1", "indexPrice": "0.099",
                 "openInterest": "10", "multiplier": "100", "fundingFeeRate": "0.001",
                 "currentFundingRateGranularity": 28800000,
                 "volumeOf24h": "100000", "turnoverOf24h": "10000"},
                {"symbol": "NVDAUSDTM", "baseCurrency": "NVDA", "quoteCurrency": "USDT",
                 "settleCurrency": "USDT", "rootSymbol": "USDT", "isInverse": False,
                 "status": "Open", "assetClass": "EQUITY", "expireDate": None,
                 "markPrice": "50", "openInterest": "100", "multiplier": "1"},
                {"symbol": "BADUSDTM", "baseCurrency": "BAD", "quoteCurrency": "USDT",
                 "settleCurrency": "USDT", "rootSymbol": "USDT", "isInverse": False,
                 "status": "Open", "assetClass": "CRYPTO", "expireDate": None,
                 "markPrice": "1", "openInterest": "100", "multiplier": None},
            ]
        },
        key(BYBIT + "/v5/market/tickers", category="linear"): {
            "retCode": 0, "time": NOW_MS,
            "result": {"category": "linear", "list": [
                {"symbol": "IRYSUSDT", "lastPrice": "0.101", "markPrice": "0.1",
                 "indexPrice": "0.099", "openInterest": "500", "openInterestValue": "50",
                 "fundingRate": "0.002", "volume24h": "100000", "turnover24h": "10000"},
                {"symbol": "STOCKUSDT", "lastPrice": "51", "markPrice": "50",
                 "openInterest": "1000"},
            ]}
        },
        key(BINANCE + "/fapi/v1/premiumIndex"): [
            {"symbol": "IRYSUSDT", "markPrice": "0.1", "indexPrice": "0.099",
             "lastFundingRate": "0.001", "time": NOW_MS},
            {"symbol": "1000IRYSUSDT", "markPrice": "0.2", "indexPrice": "0.2",
             "lastFundingRate": "0", "time": NOW_MS},
        ],
        key(BINANCE + "/fapi/v1/ticker/24hr"): [
            {"symbol": "IRYSUSDT", "lastPrice": "0.101", "volume": "100000",
             "quoteVolume": "10000", "closeTime": NOW_MS},
            {"symbol": "NVDAUSDT", "lastPrice": "50", "volume": "100000",
             "quoteVolume": "5000000", "closeTime": NOW_MS},
        ],
    }


def test_bulk_scan_filters_exact_crypto_instruments_and_normalizes_oi(tmp_path):
    client = FixtureClient(fixtures())
    raw = RawJsonlStore(tmp_path / "raw.jsonl")
    result = collect_universe_cycle(client, raw)
    assert len(client.calls) == 7  # Four market feeds + Binance catalog + two Bybit pages.
    assert {row.asset for row in result.snapshots} == {"IRYS", "BAD"}
    assert not any(row.provenance.instrument.startswith("1000IRYS") for row in result.snapshots)
    assert not any(row.provenance.instrument.startswith("NVDA") for row in result.snapshots)
    kucoin_oi = next(row for row in result.snapshots
                     if row.metric == "open_interest" and row.provenance.venue == "kucoin")
    bybit_oi = next(row for row in result.snapshots
                    if row.metric == "open_interest" and row.provenance.venue == "bybit")
    assert kucoin_oi.value == Decimal("1000") and kucoin_oi.related_value == Decimal("100.0")
    assert bybit_oi.value == Decimal("500") and bybit_oi.related_value == Decimal("50.0")
    assert "bybit_oi_both_sides" in bybit_oi.quality_flags
    funding_intervals = {row.provenance.venue: row.value for row in result.snapshots
                         if row.metric == "funding_interval_hours" and row.asset == "IRYS"}
    assert funding_intervals == {"bybit": Decimal("4"), "kucoin": Decimal("8")}
    assert "missing_source_timestamp" in bybit_oi.quality_flags
    assert all(row.provenance.venue != "binance" for row in result.snapshots if row.metric == "open_interest")
    turnover = [row for row in result.snapshots if row.metric == "perp_turnover_24h"]
    assert all(row.value == Decimal("10000") for row in turnover)
    assert {row.provenance.venue for row in turnover} == {"kucoin", "bybit", "binance"}
    assert all("turnover_quote_unit_inferred" in row.quality_flags for row in turnover
               if row.provenance.venue in {"kucoin", "bybit"})
    assert all("turnover_quote_unit_inferred" not in row.quality_flags for row in turnover
               if row.provenance.venue == "binance")
    assert any("BADUSDTM OI" in error for error in result.errors)
    raw_rows = [json.loads(line) for line in (tmp_path / "raw.jsonl").read_text().splitlines()]
    assert len(raw_rows) == len(client.calls)
    assert all(row.provenance.raw_id in {record["raw_id"] for record in raw_rows}
               for row in result.snapshots)
    bybit_raw = next(record for record in raw_rows if record["venue"] == "bybit"
                     and record["url"].endswith("/v5/market/tickers"))
    assert bybit_raw["raw_body"] == json.dumps(fixtures()[key(BYBIT + "/v5/market/tickers", category="linear")],
                                               separators=(",", ":"))

    client.calls.clear()
    second = collect_universe_cycle(client, raw)
    assert len(client.calls) == 4
    assert second.snapshots


def test_malformed_bulk_reply_is_isolated_and_archived(tmp_path):
    replies = fixtures()
    replies[key(BYBIT + "/v5/market/tickers", category="linear")] = {
        "retCode": 0, "result": {"category": "linear", "list": "not a list"}}
    replies[key(BINANCE + "/fapi/v1/premiumIndex")] = MarketFetchError(
        "invalid JSON response", status_code=200, raw_body=b"{not-json")
    client = FixtureClient(replies)
    result = collect_universe_cycle(client, RawJsonlStore(tmp_path / "raw.jsonl"))
    assert any("bybit universe: malformed" in error for error in result.errors)
    assert any("binance /fapi/v1/premiumIndex" in error for error in result.errors)
    assert any(row.provenance.venue == "kucoin" for row in result.snapshots)
    assert any(row.provenance.venue == "binance" and row.metric == "perp_last"
               for row in result.snapshots)
    assert not any(row.provenance.venue == "bybit" and row.metric in {
        "perp_last", "open_interest", "funding_rate"} for row in result.snapshots)
    raw_rows = [json.loads(line) for line in (tmp_path / "raw.jsonl").read_text().splitlines()]
    malformed = next(row for row in raw_rows if row["url"].endswith("/fapi/v1/premiumIndex"))
    assert malformed["raw_body_base64"] == "e25vdC1qc29u"


def test_binance_source_time_is_preserved(tmp_path):
    result = collect_universe_cycle(FixtureClient(fixtures()),
                                    RawJsonlStore(tmp_path / "raw.jsonl"))
    mark = next(row for row in result.snapshots
                if row.provenance.venue == "binance" and row.metric == "mark_price")
    assert mark.provenance.source_timestamp == datetime.fromtimestamp(NOW_MS / 1000, tz=timezone.utc)
    assert mark.provenance.received_at.tzinfo is not None
