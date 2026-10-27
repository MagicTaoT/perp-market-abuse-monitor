"""Collector configuration and durable partial-cycle behavior."""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from gold_basis_mm_bot.squeeze_engine import NormalizedSnapshot, Provenance
from gold_basis_mm_bot.squeeze_engine import __main__ as cli
from gold_basis_mm_bot.squeeze_engine.connectors import CollectionResult
from gold_basis_mm_bot.squeeze_engine.storage import NormalizedSqliteStore, RawJsonlStore
from gold_basis_mm_bot.squeeze_engine.storage import CompressedDailyRawStore
from gold_basis_mm_bot.squeeze_engine.tokenomics import TokenomicsResult


def test_config_rejects_remote_dashboard_and_unbounded_request_rate(tmp_path):
    path = tmp_path / "config.json"
    base = {"assets": ["IRYS"], "data_dir": str(tmp_path / "state"),
            "poll_interval_seconds": 60, "dashboard_host": "127.0.0.1"}
    path.write_text(json.dumps(base))
    loaded = cli.load_config(path)
    assert loaded.assets == ("IRYS",) and loaded.dashboard_port == 8770
    assert loaded.universe_scan_interval_seconds is None
    for change in ({"dashboard_host": "0.0.0.0"}, {"poll_interval_seconds": 0},
                   {"assets": ["IRYS", "IRYS"]}, {"assets": [123]}, {"api_key": "unused"},
                   {"universe_scan_interval_seconds": 59}, {"universe_scan_interval_seconds": True}):
        path.write_text(json.dumps(base | change))
        with pytest.raises(ValueError):
            cli.load_config(path)
    path.write_text(json.dumps(base | {"universe_scan_interval_seconds": 900}))
    assert cli.load_config(path).universe_scan_interval_seconds == 900


def test_cycle_persists_healthy_venue_when_other_venue_fails(tmp_path, monkeypatch):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"assets": ["IRYS"], "data_dir": str(tmp_path / "state")}))
    config = cli.load_config(path)
    now = datetime.now(timezone.utc)
    sample = NormalizedSnapshot(
        asset="IRYS", metric="mark_price", value=Decimal("0.1"), unit="USD",
        provenance=Provenance(source="fixture", venue="healthy", instrument="IRYSUSDT",
                              source_timestamp=now, received_at=now),
    )

    class Healthy:
        venue = "healthy"

        def __init__(self, client, raw):
            self.raw = raw

        def collect(self, asset):
            assert asset == "IRYS"
            self.raw.append({"source": "fixture", "asset": asset})
            return CollectionResult([sample], [])

    class Broken:
        venue = "broken"

        def __init__(self, client, raw):
            pass

        def collect(self, asset):
            raise RuntimeError("venue unavailable")

    monkeypatch.setattr(cli, "ADAPTERS", (Healthy, Broken))
    raw = RawJsonlStore(config.data_dir / "raw.jsonl")
    with NormalizedSqliteStore(config.data_dir / "normalized.sqlite3") as store:
        result = cli.collect_cycle(config, store, raw, object())
        assert result["counts"] == {"IRYS/healthy": 1, "IRYS/broken": 0}
        assert store.get_as_of("IRYS", "mark_price", datetime.now(timezone.utc)).value == Decimal("0.1")
    assert len((config.data_dir / "raw.jsonl").read_text().splitlines()) == 1


def test_universe_cycle_persists_bulk_results_and_reports_coverage(tmp_path, monkeypatch):
    now = datetime.now(timezone.utc)
    sample = NormalizedSnapshot(
        asset="LAB", metric="perp_last", value=Decimal("1.5"), unit="USD",
        provenance=Provenance(source="fixture:bulk", venue="kucoin", instrument="LABUSDTM",
                              source_timestamp=now, received_at=now),
    )

    def fixture_scan(_client, raw):
        raw_id = raw.append({"venue": "kucoin", "test": True})
        assert raw_id
        return CollectionResult([sample], ["bybit unavailable"])

    monkeypatch.setattr(cli, "fetch_universe_cycle", fixture_scan)
    raw = CompressedDailyRawStore(tmp_path / "bulk")
    with NormalizedSqliteStore(tmp_path / "state.sqlite") as store:
        summary = cli.scan_universe_cycle(store, raw, object())
        assert summary["counts"] == {"kucoin": 1, "bybit": 0, "binance": 0}
        assert summary["assets"] == 1 and summary["errors_count"] == 1
        assert store.get_as_of("LAB", "perp_last", datetime.now(timezone.utc)) == sample


def test_tokenomics_schedule_survives_restart_and_prioritizes_recent_success(tmp_path):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"assets": ["IRYS"], "data_dir": str(tmp_path / "state"),
                                "tokenomics_refresh_interval_seconds": 86400}))
    config = cli.load_config(path)
    at = datetime(2026, 10, 23, 12, tzinfo=timezone.utc)
    with NormalizedSqliteStore(tmp_path / "state.sqlite") as store:
        assert cli._next_tokenomics_due(config, store, at) == at
        store.append_tokenomics_scan(
            received_at=at, identity_status={"IRYS": {"status": "matched"}},
            unlock_events=[], errors=[])
        store.append_tokenomics_scan(
            received_at=at + timedelta(minutes=2), identity_status={},
            unlock_events=[], errors=["HTTP 429"])
        assert cli._next_tokenomics_due(config, store, at + timedelta(minutes=3)) == (
            at + timedelta(days=1))
        # Once the successful scan has aged out, a new failed attempt still
        # gets its one-hour backoff even across another process restart.
        failed_at = at + timedelta(days=1, minutes=2)
        store.append_tokenomics_scan(
            received_at=failed_at, identity_status={}, unlock_events=[],
            errors=["HTTP 429"])
        assert cli._next_tokenomics_due(config, store, failed_at + timedelta(minutes=3)) == (
            failed_at + timedelta(hours=1))
        assert cli._next_tokenomics_due(config, store, failed_at + timedelta(hours=2)) == (
            failed_at + timedelta(hours=2))


def test_empty_tokenomics_scan_is_persisted_as_failed_attempt(tmp_path, monkeypatch):
    path = tmp_path / "config.json"
    path.write_text(json.dumps({"assets": ["IRYS"], "data_dir": str(tmp_path / "state")}))
    config = cli.load_config(path)
    monkeypatch.setattr(cli, "collect_tokenomics", lambda *_args, **_kwargs: TokenomicsResult())
    with NormalizedSqliteStore(tmp_path / "state.sqlite") as store:
        summary = cli.scan_tokenomics_cycle(
            config, store, CompressedDailyRawStore(tmp_path / "raw"), object())
        scan = store.tokenomics_scan_as_of(datetime.now(timezone.utc))
        assert summary["supply_observations"] == 0
        assert scan["errors"] == ["tokenomics: no supply observations"]
        assert store.tokenomics_scan_as_of(
            datetime.now(timezone.utc), successful_only=True) is None
