from __future__ import annotations

import gzip
import json
import sqlite3
from dataclasses import asdict, replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal

from gold_basis_mm_bot.squeeze_engine.discovery import discovery_payload
from gold_basis_mm_bot.squeeze_engine.models import NormalizedSnapshot, Provenance
from gold_basis_mm_bot.squeeze_engine.storage import CompressedDailyRawStore, NormalizedSqliteStore
from gold_basis_mm_bot.squeeze_engine.tokenomics import UnlockEvent


NOW = datetime(2026, 10, 23, 12, 0, tzinfo=timezone.utc)


def sample(asset: str, venue: str, metric: str, value: str, at: datetime,
           *, related: str | None = None) -> NormalizedSnapshot:
    return NormalizedSnapshot(
        asset=asset, metric=metric, value=Decimal(value),
        unit="tokens" if metric == "open_interest" else "USD",
        related_value=Decimal(related) if related else None,
        related_unit="USD" if related else None,
        provenance=Provenance(
            source="fixture:bulk", venue=venue, instrument=f"{asset}USDT",
            source_timestamp=at, received_at=at + timedelta(seconds=1),
        ),
    )


def series(asset: str, venue: str, *, price_before: str, price_now: str,
           oi_before: str, oi_now: str) -> list[NormalizedSnapshot]:
    before = NOW - timedelta(minutes=65)
    return [
        sample(asset, venue, "perp_last", price_before, before),
        sample(asset, venue, "open_interest", oi_before, before,
               related=str(Decimal(oi_before) * Decimal(price_before))),
        sample(asset, venue, "perp_last", price_now, NOW),
        sample(asset, venue, "open_interest", oi_now, NOW,
               related=str(Decimal(oi_now) * Decimal(price_now))),
    ]


def test_discovery_ranks_same_venue_token_oi_moves_and_explains_confirmation(tmp_path) -> None:
    with NormalizedSqliteStore(tmp_path / "obs.sqlite") as store:
        store.append_many([
            *series("AAA", "bybit", price_before="100", price_now="110",
                    oi_before="20000", oi_now="22000"),
            *series("AAA", "kucoin", price_before="100", price_now="108",
                    oi_before="25000", oi_now="27500"),
            *series("BBB", "bybit", price_before="100", price_now="120",
                    oi_before="20000", oi_now="20000"),
        ])
        payload = discovery_payload(store, now=NOW + timedelta(minutes=1))
    assert payload["coverage"]["assets_seen"] == 2
    assert payload["coverage"]["assets_with_1h_baseline"] == 2
    aaa = next(item for item in payload["candidates"] if item["asset"] == "AAA")
    bbb = next(item for item in payload["candidates"] if item["asset"] == "BBB")
    assert aaa["status"] == "watch" and aaa["pattern"] == "build_up"
    assert aaa["structural_scores"]["early_accumulation"]["score"] is None
    assert aaa["structural_scores"]["early_accumulation"]["coverage_pct"] < 100
    assert aaa["oi_structure"]["coverage_label"].startswith("Bybit 单所")
    assert aaa["price_change_1h_pct"] == "10.00"
    assert aaa["oi_change_1h_pct"] == "10.00"
    assert "2 个交易所" in aaa["reasons"][-1]
    assert bbb["status"] == "watch"
    assert bbb["oi_change_1h_pct"] == "0.00"


def test_discovery_needs_history_and_excludes_future_or_unsafe_samples(tmp_path) -> None:
    with NormalizedSqliteStore(tmp_path / "obs.sqlite") as store:
        data = series("AAA", "bybit", price_before="100", price_now="110",
                      oi_before="20000", oi_now="22000")
        assert len(store.append_many(data[:2])) == 2
        early = discovery_payload(store, now=NOW - timedelta(minutes=30))
        assert early["candidates"] == []
        store.append_many(data[2:])
        assert discovery_payload(store, now=NOW - timedelta(seconds=1))["candidates"] == []
        assert discovery_payload(store, now=NOW + timedelta(minutes=1))["candidates"][0]["asset"] == "AAA"


def test_structural_enrichment_preserves_partial_oi_and_unlock_provenance(tmp_path) -> None:
    def extra(metric: str, value: str, unit: str, venue: str = "bybit",
              at: datetime = NOW) -> NormalizedSnapshot:
        return NormalizedSnapshot(
            asset="AAA", metric=metric, value=Decimal(value), unit=unit,
            provenance=Provenance(
                source="fixture", venue=venue,
                instrument="CMC:123" if venue == "coinmarketcap" else "AAAUSDT",
                source_timestamp=at, received_at=at + timedelta(seconds=1),
                raw_id="cmc-quote" if venue == "coinmarketcap" else None,
            ),
        )

    with NormalizedSqliteStore(tmp_path / "obs.sqlite") as store:
        store.append_many([
            *series("AAA", "bybit", price_before="100", price_now="110",
                    oi_before="20000", oi_now="22000"),
            extra("funding_rate", "-0.001", "decimal"),
            extra("funding_rate", "-0.0002", "decimal", at=NOW - timedelta(minutes=60)),
            extra("funding_interval_hours", "4", "hours"),
            extra("circulating_supply", "100000", "tokens", venue="coinmarketcap"),
            extra("max_supply", "500000", "tokens", venue="coinmarketcap"),
            extra("cmc_price", "100", "USD", venue="coinmarketcap"),
            extra("fdv_usd", "50000000", "USD", venue="coinmarketcap"),
        ])
        store.append_tokenomics_scan(
            received_at=NOW,
            identity_status={"AAA": {"status": "matched", "provider_id": 123}},
            unlock_events=[{
                "asset": "AAA", "provider_id": 123, "unlock_date": "2026-10-25",
                "amount_tokens": "5000", "pct_circulating": "5",
                "source_url": "https://pattern.plus/api/v1/unlocks/aaa.json",
                "attribution_text": "Data: PatternPlus",
                "attribution_url": "https://pattern.plus", "quality_flags": [],
            }],
        )
        # A later partial refresh must not replace the valid identity and
        # unlock calendar used by discovery.
        store.append_tokenomics_scan(
            received_at=NOW + timedelta(seconds=30), identity_status={},
            unlock_events=[], errors=["coinmarketcap: HTTP 429"],
        )
        assert store.tokenomics_scan_as_of(NOW - timedelta(seconds=1)) is None
        assert store.tokenomics_scan_as_of(NOW + timedelta(minutes=1))["errors"]
        assert datetime.fromisoformat(store.tokenomics_scan_as_of(
            NOW + timedelta(minutes=1), successful_only=True)["received_at"]) == NOW
        assert store.tokenomics_scan_as_of(
            NOW + timedelta(hours=37), successful_only=True,
            max_age=timedelta(hours=36)) is None
        row = discovery_payload(store, now=NOW + timedelta(minutes=1))["candidates"][0]
    assert row["oi_structure"]["observed_oi_usd"] == "1210000"
    assert row["oi_structure"]["oi_to_float_pct"] == "11.00"
    assert row["oi_structure"]["oi_to_fdv_pct"] == "2.200"
    assert row["unlock"]["pct_circulating"] == "5"
    assert row["unlock"]["attribution_text"] == "Data: PatternPlus"
    assert row["funding_carry"]["rate_8h_pct"] == "-0.200"
    assert row["structural_scores"]["short_risk"]["score"] is None
    assert row["structural_scores"]["short_risk"]["observed_score"] > 0


def test_unlock_dataclass_date_is_persisted_as_received_evidence(tmp_path) -> None:
    event = UnlockEvent(
        asset="AAA", provider_id=123, unlock_date=date(2026, 10, 25),
        amount_tokens=Decimal("5000"), value_usd=Decimal("500000"),
        pct_circulating=Decimal("5"), pct_total=Decimal("1"),
        basis=("project schedule",), types=("cliff",), categories=("team",),
        source_url="https://pattern.plus/api/v1/unlocks/aaa.json",
        source_as_of=date(2026, 10, 23), source_updated_at=NOW,
        received_at=NOW, attribution_text="Data: PatternPlus",
        attribution_url="https://pattern.plus",
    )
    with NormalizedSqliteStore(tmp_path / "obs.sqlite") as store:
        store.append_tokenomics_scan(received_at=NOW, identity_status={},
                                     unlock_events=[asdict(event)])
        scan = store.tokenomics_scan_as_of(NOW)
    assert scan["unlock_events"][0]["unlock_date"] == "2026-10-25"
    assert scan["unlock_events"][0]["source_as_of"] == "2026-10-23"


def test_bulk_raw_records_are_compressed_daily_with_traceable_ids(tmp_path) -> None:
    raw = CompressedDailyRawStore(tmp_path / "bulk")
    first = raw.append({"venue": "kucoin", "payload": {"mark": Decimal("0.123")}})
    second = raw.append({"venue": "bybit", "payload": [1, 2, 3]})
    files = list((tmp_path / "bulk").glob("*.jsonl.gz"))
    assert len(files) == 1
    with gzip.open(files[0], "rt", encoding="utf-8") as handle:
        rows = [json.loads(line) for line in handle]
    assert [row["raw_id"] for row in rows] == [first, second]
    assert rows[0]["payload"]["mark"] == "0.123"


def test_bulk_retention_preserves_detail_and_existing_sqlite_migrates(tmp_path) -> None:
    path = tmp_path / "existing.sqlite3"
    with sqlite3.connect(path) as connection:
        connection.execute("""
            CREATE TABLE normalized_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT, asset TEXT NOT NULL,
                metric TEXT NOT NULL, value TEXT NOT NULL, unit TEXT NOT NULL,
                related_value TEXT, related_unit TEXT, source TEXT NOT NULL,
                venue TEXT NOT NULL, instrument TEXT NOT NULL,
                source_timestamp TEXT, received_at TEXT NOT NULL,
                as_of TEXT NOT NULL, raw_id TEXT, quality_flags TEXT NOT NULL
            )
        """)
    older = NOW - timedelta(days=30)
    bulk = replace(sample("AAA", "bybit", "perp_last", "10", older),
                   quality_flags=("bulk_scan",))
    detail = sample("AAA", "kucoin", "perp_last", "10", older)
    with NormalizedSqliteStore(path) as store:
        store.append_many([bulk, detail])
        assert store.prune_bulk(NOW - timedelta(days=21)) == 1
        assert store.get_as_of("AAA", "perp_last", NOW, venue="bybit") is None
        assert store.get_as_of("AAA", "perp_last", NOW, venue="kucoin") == detail
