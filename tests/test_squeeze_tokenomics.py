"""Tokenomics enrichment must never turn ticker collisions or gaps into facts."""

from __future__ import annotations

import json
from datetime import date, datetime, timezone
from decimal import Decimal

from gold_basis_mm_bot.squeeze_engine.connectors import MarketFetchError
from gold_basis_mm_bot.squeeze_engine.storage import RawJsonlStore
from gold_basis_mm_bot.squeeze_engine.tokenomics import (
    CMC_MAP_URL, CMC_QUOTES_URL, PATTERNPLUS_UNLOCKS_URL, collect_tokenomics,
)


NOW = datetime(2026, 10, 23, 17, 0, tzinfo=timezone.utc)


class FakeClient:
    def __init__(self, replies):
        self.replies = replies
        self.calls = []

    def get_json(self, url, params):
        self.calls.append((url, dict(params)))
        reply = self.replies[url]
        if isinstance(reply, Exception):
            raise reply
        return reply


def cmc(rows):
    return {"status": {"error_code": 0, "timestamp": "2026-10-23T17:00:00Z"}, "data": rows}


def quote(cmc_id, symbol, name, slug, *, supply="1000000", price="2",
          market_cap="2000000", fdv="10000000"):
    return {
        "id": cmc_id, "symbol": symbol, "name": name, "slug": slug,
        "circulating_supply": supply, "total_supply": "4000000",
        "max_supply": "5000000", "last_updated": "2026-10-23T16:55:00Z",
        "quote": {"USD": {"price": price, "market_cap": market_cap,
                          "fully_diluted_market_cap": fdv,
                          "last_updated": "2026-10-23T16:59:00Z"}},
    }


def pattern(rows, *, as_of="2026-10-23"):
    return {
        "as_of": as_of, "rebuilt": "2026-10-23T16:27:51Z",
        "attribution": {"text": "Data: PatternPlus", "url": "https://pattern.plus"},
        "data": rows,
    }


def test_unique_id_and_explicit_override_normalize_supply_and_unlock(tmp_path):
    client = FakeClient({
        CMC_MAP_URL: cmc([
            {"id": 35214, "symbol": "BMT", "name": "Bubblemaps", "slug": "bubblemaps"},
            {"id": 99999, "symbol": "BMT", "name": "Other BMT", "slug": "other-bmt"},
            {"id": 44444, "symbol": "ICNT", "name": "Impossible Cloud Network",
             "slug": "impossible-cloud-network"},
        ]),
        CMC_QUOTES_URL: cmc([
            quote(35214, "BMT", "Bubblemaps", "bubblemaps"),
            quote(44444, "ICNT", "Impossible Cloud Network", "impossible-cloud-network"),
        ]),
        PATTERNPLUS_UNLOCKS_URL: pattern([
            {"date": "2026-10-25", "token": "bmt-bubblemaps", "symbol": "BMT",
             "name": "Bubblemaps", "amount": "50000", "value_usd": "100000",
             "pct_circulating": "5", "pct_total": "1", "basis": ["project document"],
             "types": ["cliff"], "categories": ["team"],
             "file": "https://pattern.plus/api/v1/unlocks/bmt-bubblemaps.json"},
        ]),
    })
    path = tmp_path / "tokenomics.jsonl"
    result = collect_tokenomics(["icnt", "bmt", "BMT"], client, RawJsonlStore(path), NOW)

    assert result.errors == []
    assert result.identity_status["BMT"]["status"] == "override"
    assert result.identity_status["BMT"]["provider_id"] == 35214
    assert result.identity_status["ICNT"]["status"] == "matched"
    assert result.identity_status["ICNT"]["unlock_status"] == "unknown"
    assert [item.metric for item in result.snapshots if item.asset == "BMT"] == [
        "circulating_supply", "total_supply", "max_supply", "market_cap_usd", "fdv_usd", "cmc_price"
    ]
    assert next(item for item in result.snapshots if item.asset == "BMT" and
                item.metric == "fdv_usd").value == Decimal("10000000")
    assert all(item.provenance.source_timestamp is not None for item in result.snapshots)
    assert all(item.provenance.received_at == NOW for item in result.snapshots)
    assert len(result.unlock_events) == 1
    event = result.unlock_events[0]
    assert event.asset == "BMT" and event.unlock_date == date(2026, 10, 25)
    assert event.amount_tokens == Decimal("50000")
    assert event.basis == ("project document",)
    assert event.attribution_url == "https://pattern.plus"
    assert result.identity_status["BMT"]["unlock_status"] == "upcoming"
    assert len(path.read_text().splitlines()) == 3
    assert {item["raw_id"] for item in map(json.loads, path.read_text().splitlines())}
    quotes_call = next(params for url, params in client.calls if url == CMC_QUOTES_URL)
    assert set(quotes_call["id"].split(",")) == {"35214", "44444"}
    assert all("key" not in params for _, params in client.calls)


def test_unoverridden_symbol_collision_is_ambiguous_and_not_quoted(tmp_path):
    client = FakeClient({
        CMC_MAP_URL: cmc([
            {"id": 10, "symbol": "AAA", "name": "First", "slug": "first"},
            {"id": 11, "symbol": "AAA", "name": "Second", "slug": "second"},
        ]),
        PATTERNPLUS_UNLOCKS_URL: pattern([]),
    })
    result = collect_tokenomics(["AAA"], client, RawJsonlStore(tmp_path / "raw.jsonl"), NOW)
    assert result.identity_status["AAA"]["status"] == "ambiguous"
    assert result.snapshots == [] and result.unlock_events == []
    assert all(url != CMC_QUOTES_URL for url, _ in client.calls)


def test_missing_and_zero_values_do_not_become_zero_snapshots(tmp_path):
    sparse = quote(44444, "ICNT", "Impossible Cloud Network", "impossible-cloud-network",
                   supply=None, price="1.2", market_cap=0, fdv=None)
    sparse["total_supply"] = "not-a-number"
    sparse["max_supply"] = None
    client = FakeClient({
        CMC_MAP_URL: cmc([{"id": 44444, "symbol": "ICNT", "name": "Impossible Cloud Network",
                           "slug": "impossible-cloud-network"}]),
        CMC_QUOTES_URL: cmc([sparse]),
        PATTERNPLUS_UNLOCKS_URL: pattern([]),
    })
    result = collect_tokenomics(["ICNT"], client, RawJsonlStore(tmp_path / "raw.jsonl"), NOW)
    assert [(item.metric, item.value) for item in result.snapshots] == [("cmc_price", Decimal("1.2"))]
    assert result.identity_status["ICNT"]["unlock_status"] == "unknown"


def test_invalid_map_symbol_isolated_without_losing_valid_batch_members(tmp_path):
    rows = {
        "BTC": {"id": 1, "symbol": "BTC", "name": "Bitcoin", "slug": "bitcoin"},
        "ETH": {"id": 1027, "symbol": "ETH", "name": "Ethereum", "slug": "ethereum"},
        "SOL": {"id": 5426, "symbol": "SOL", "name": "Solana", "slug": "solana"},
    }

    class RejectOneSymbolClient(FakeClient):
        def get_json(self, url, params):
            self.calls.append((url, dict(params)))
            if url == CMC_MAP_URL:
                batch = params["symbol"].split(",")
                if "BAD" in batch:
                    raise MarketFetchError("HTTP 400: invalid symbol BAD", status_code=400)
                return cmc([rows[symbol] for symbol in batch])
            if url == CMC_QUOTES_URL:
                return cmc([quote(1, "BTC", "Bitcoin", "bitcoin"),
                            quote(1027, "ETH", "Ethereum", "ethereum"),
                            quote(5426, "SOL", "Solana", "solana")])
            return pattern([])

    path = tmp_path / "raw.jsonl"
    client = RejectOneSymbolClient({})
    result = collect_tokenomics(["BTC", "ETH", "BAD", "SOL"], client,
                               RawJsonlStore(path), NOW)

    assert result.errors == []
    assert result.identity_status["BAD"]["status"] == "missing"
    assert "HTTP 400" in result.identity_status["BAD"]["reason"]
    assert {item.asset for item in result.snapshots} == {"BTC", "ETH", "SOL"}
    map_calls = [params["symbol"] for url, params in client.calls if url == CMC_MAP_URL]
    assert map_calls == ["BAD,BTC,ETH,SOL", "BAD,BTC", "BAD", "BTC", "ETH,SOL"]
    audit = [json.loads(line) for line in path.read_text().splitlines()]
    assert sum(entry.get("http_status") == 400 for entry in audit) == 3
    assert any(entry.get("params", {}).get("symbol") == "BAD" and
               entry.get("http_status") == 400 for entry in audit)


def test_map_rate_limit_does_not_trigger_recursive_requests(tmp_path):
    class RateLimitedClient(FakeClient):
        def get_json(self, url, params):
            self.calls.append((url, dict(params)))
            if url == CMC_MAP_URL:
                raise MarketFetchError("HTTP 429", status_code=429)
            return pattern([])

    client = RateLimitedClient({})
    result = collect_tokenomics(["BTC", "ETH"], client,
                               RawJsonlStore(tmp_path / "raw.jsonl"), NOW)
    assert sum(url == CMC_MAP_URL for url, _ in client.calls) == 1
    assert result.snapshots == []
    assert any("HTTP 429" in error for error in result.errors)


def test_quote_id_symbol_disagreement_is_rejected(tmp_path):
    client = FakeClient({
        CMC_MAP_URL: cmc([{"id": 35214, "symbol": "BMT", "name": "Bubblemaps"}]),
        CMC_QUOTES_URL: cmc([quote(35214, "OTHER", "Other", "other")]),
        PATTERNPLUS_UNLOCKS_URL: pattern([]),
    })
    result = collect_tokenomics(["BMT"], client, RawJsonlStore(tmp_path / "raw.jsonl"), NOW)
    assert result.snapshots == []
    assert result.identity_status["BMT"]["status"] == "missing"
    assert any("mismatch" in error for error in result.errors)


def test_unlock_name_collision_is_not_accepted_and_provider_error_isolated(tmp_path):
    map_data = cmc([{"id": 44444, "symbol": "ICNT", "name": "Impossible Cloud Network",
                     "slug": "impossible-cloud-network"}])
    wrong = {"date": "2026-10-25", "token": "icnt-other", "symbol": "ICNT",
             "name": "Other", "amount": 1000,
             "file": "https://pattern.plus/api/v1/unlocks/icnt-other.json"}
    client = FakeClient({CMC_MAP_URL: map_data,
                         CMC_QUOTES_URL: cmc([quote(44444, "ICNT", "Impossible Cloud Network",
                                                     "impossible-cloud-network")]),
                         PATTERNPLUS_UNLOCKS_URL: pattern([wrong])})
    result = collect_tokenomics(["ICNT"], client, RawJsonlStore(tmp_path / "raw.jsonl"), NOW)
    assert result.snapshots
    assert not result.unlock_events
    assert result.identity_status["ICNT"]["unlock_status"] == "unknown"

    client.replies[PATTERNPLUS_UNLOCKS_URL] = MarketFetchError("unavailable", status_code=503)
    result = collect_tokenomics(["ICNT"], client, RawJsonlStore(tmp_path / "raw2.jsonl"), NOW)
    assert result.snapshots
    assert not result.unlock_events
    assert any("unavailable" in error for error in result.errors)


def test_stale_calendar_is_visible_as_stale_evidence(tmp_path):
    event = {"date": "2026-10-26", "token": "icnt-impossible-cloud-network",
             "symbol": "ICNT", "name": "Impossible Cloud Network", "amount": 1000,
             "file": "https://pattern.plus/api/v1/unlocks/icnt-impossible-cloud-network.json"}
    client = FakeClient({
        CMC_MAP_URL: cmc([{"id": 44444, "symbol": "ICNT", "name": "Impossible Cloud Network",
                           "slug": "impossible-cloud-network"}]),
        CMC_QUOTES_URL: cmc([quote(44444, "ICNT", "Impossible Cloud Network",
                                    "impossible-cloud-network")]),
        PATTERNPLUS_UNLOCKS_URL: pattern([event], as_of="2026-10-19"),
    })
    result = collect_tokenomics(["ICNT"], client, RawJsonlStore(tmp_path / "raw.jsonl"), NOW)
    assert result.unlock_events[0].quality_flags == ("date_precision_day", "stale_unlock_calendar")
    assert result.identity_status["ICNT"]["unlock_status"] == "stale"


def test_invalid_cmc_envelope_still_has_raw_audit_record(tmp_path):
    path = tmp_path / "raw.jsonl"
    client = FakeClient({CMC_MAP_URL: {"status": {"error_code": 1002}, "data": []},
                         PATTERNPLUS_UNLOCKS_URL: pattern([])})
    result = collect_tokenomics(["ICNT"], client, RawJsonlStore(path), NOW)
    assert result.identity_status["ICNT"]["status"] == "missing"
    assert any("provider error" in error for error in result.errors)
    raw = [json.loads(line) for line in path.read_text().splitlines()]
    assert raw[0]["fixture_payload"]["status"]["error_code"] == 1002
