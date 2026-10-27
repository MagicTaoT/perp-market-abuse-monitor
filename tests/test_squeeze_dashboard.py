from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from io import BytesIO
from pathlib import Path

import pytest

from gold_basis_mm_bot.squeeze_engine.dashboard import HTML, make_handler, make_server, snapshot_json
from gold_basis_mm_bot.squeeze_engine.models import NormalizedSnapshot, Provenance
from gold_basis_mm_bot.squeeze_engine.storage import NormalizedSqliteStore


def at(value: str) -> datetime:
    return datetime.fromisoformat(value.replace("Z", "+00:00"))


def observation(
    metric: str,
    value: str,
    *,
    source_time: str | None,
    received: str,
    venue: str = "hyperliquid",
    unit: str = "USD",
    related_value: str | None = None,
    related_unit: str | None = None,
    flags: tuple[str, ...] = (),
    asset: str = "IRYS",
) -> NormalizedSnapshot:
    return NormalizedSnapshot(
        asset=asset,
        metric=metric,
        value=Decimal(value),
        unit=unit,
        related_value=Decimal(related_value) if related_value is not None else None,
        related_unit=related_unit,
        provenance=Provenance(
            source="public_market_data",
            venue=venue,
            instrument=f"{asset}-USD",
            source_timestamp=at(source_time) if source_time is not None else None,
            received_at=at(received),
            raw_id="private-raw-id",
        ),
        quality_flags=flags,
    )


def dispatch(store: NormalizedSqliteStore, path: str, method: str = "GET") -> tuple[int, dict[str, str], bytes]:
    """Exercise the actual request handler without opening a sandboxed socket."""
    handler_type = make_handler(store)
    handler = handler_type.__new__(handler_type)
    handler.path = path
    handler.wfile = BytesIO()
    result: dict[str, int] = {}
    headers: dict[str, str] = {}
    handler.send_response = lambda status: result.update(status=int(status))
    handler.send_header = lambda key, value: headers.__setitem__(key, value)
    handler.end_headers = lambda: None
    getattr(handler, "do_" + method)()
    return result["status"], headers, handler.wfile.getvalue()


def get_json(store: NormalizedSqliteStore, path: str) -> dict:
    status, headers, body = dispatch(store, path)
    assert status == 200
    assert headers["Cache-Control"] == "no-store"
    assert headers["X-Content-Type-Options"] == "nosniff"
    return json.loads(body)


def test_snapshot_honors_available_time_preserves_decimal_and_exposes_quality(tmp_path: Path) -> None:
    with NormalizedSqliteStore(tmp_path / "observations.sqlite") as store:
        store.append(observation(
            "mark_price", "1.00000000000000000001",
            source_time="2026-10-20T00:00:00Z", received="2026-10-20T00:00:02Z",
        ))
        store.append(observation(
            "mark_price", "2.00000000000000000002",
            source_time="2026-10-20T00:01:00Z", received="2026-10-20T01:00:00Z",
        ))
        store.append(observation(
            "open_interest", "12345.67890123456789", unit="tokens",
            related_value="24691.35780246913578", related_unit="USD",
            source_time="2026-10-20T00:02:00Z", received="2026-10-20T00:02:01Z",
            flags=("stale_source",),
        ))
        early = get_json(store, "/api/snapshot?asset=IRYS&as_of=2026-10-20T00%3A30%3A00Z")
        mark = next(row for row in early["observations"] if row["metric"] == "mark_price")
        assert mark["value"] == "1.00000000000000000001"
        assert mark["as_of"] == "2026-10-20T00:00:00Z"
        assert mark["available_at"] == "2026-10-20T00:00:02Z"
        assert mark["receipt_delay_seconds"] == 2.0
        assert "funding_rate" in early["missing"]
        oi = next(row for row in early["observations"] if row["metric"] == "open_interest")
        assert oi["value"] == "12345.67890123456789"
        assert oi["related_value"] == "24691.35780246913578"
        assert oi["quality_status"] == "stale"
        assert oi["chartable"] is True
        assert "raw_id" not in json.dumps(early)

        late = get_json(store, "/api/snapshot?asset=IRYS&as_of=2026-10-20T01%3A00%3A00Z")
        mark = next(row for row in late["observations"] if row["metric"] == "mark_price")
        assert mark["value"] == "2.00000000000000000002"


def test_history_filters_by_available_time_and_orders_on_chart_time(tmp_path: Path) -> None:
    with NormalizedSqliteStore(tmp_path / "observations.sqlite") as store:
        store.append(observation(
            "perp_last", "1", source_time="2026-10-20T00:00:00Z",
            received="2026-10-20T00:00:01Z",
        ))
        store.append(observation(
            "perp_last", "3", source_time="2026-10-20T00:03:00Z",
            received="2026-10-20T00:03:01Z",
        ))
        store.append(observation(
            "perp_last", "2", source_time="2026-10-20T00:02:00Z",
            received="2026-10-20T00:04:00Z", flags=("out_of_order_source_timestamp",),
        ))
        store.append(observation(
            "perp_last", "9", source_time="2026-10-20T00:04:00Z",
            received="2026-10-20T00:04:01Z", venue="other",
        ))
        rows = get_json(store, "/api/history?asset=IRYS&metric=perp_last&venue=hyperliquid&since=2026-10-20T00%3A03%3A00Z&limit=2")
        assert rows["filter_time"] == "available_at"
        assert rows["axis_time"] == "source_timestamp_or_received_at"
        assert [row["value"] for row in rows["observations"]] == ["2", "3"]
        assert rows["observations"][0]["quality_flags"] == ["out_of_order_source_timestamp"]
        assert all(row["venue"] == "hyperliquid" for row in rows["observations"])


def test_missing_timestamp_bad_units_and_read_only_http(tmp_path: Path) -> None:
    missing_timestamp = observation(
        "funding_rate", "0.0001", unit="decimal", source_time=None,
        received="2026-10-20T00:00:00Z",
    )
    encoded = snapshot_json(missing_timestamp, now=at("2026-10-20T00:01:00Z"))
    assert encoded["source_timestamp"] is None
    assert encoded["as_of"] == encoded["received_at"]
    assert encoded["quality_flags"] == ["missing_source_timestamp"]
    assert encoded["receipt_delay_seconds"] is None
    assert encoded["age_seconds"] == 60.0

    invalid_unit = observation(
        "funding_rate", "1", unit="percent", source_time="2026-10-20T00:00:00Z",
        received="2026-10-20T00:00:01Z",
    )
    assert snapshot_json(invalid_unit, now=at("2026-10-20T00:01:00Z"))["chartable"] is False
    assert "OI · 区间变化 %" in HTML and "OI · tokens" in HTML
    assert "Premium · bps" in HTML and "Funding · % / interval" in HTML
    assert "发现榜单" in HTML and 'fetch("/api/discovery?limit=50")' in HTML
    assert 'document.getElementById("asset").value=candidate.asset' in HTML
    assert 'id="chart-view"' in HTML and "自动适配" in HTML

    with NormalizedSqliteStore(tmp_path / "observations.sqlite") as store:
        health = get_json(store, "/healthz")
        assert health == {
            "ok": True,
            "service": "squeeze-research-dashboard",
            "read_only": True,
            "asset": None,
            "latest_received_at": None,
            "age_seconds": None,
            "data_ready": False,
            "max_age_seconds": 180,
        }
        status, headers, body = dispatch(store, "/")
        assert status == 200
        assert "default-src 'none'" in headers["Content-Security-Policy"]
        assert "统一 UTC 时间轴" in body.decode("utf-8")
        for path in ("/api/history?metric=not_a_metric", "/api/history?metric=perp_last&limit=50000", "/api/snapshot?as_of=2026-10-20T00:00:00"):
            status, _, _ = dispatch(store, path)
            assert status == 400
        status, _, body = dispatch(store, "/api/snapshot", "POST")
        assert status == 405
        assert json.loads(body)["error"] == "read-only dashboard"
        status, _, _ = dispatch(store, "/trade")
        assert status == 404


def test_discovery_route_is_bounded_and_read_only(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    from gold_basis_mm_bot.squeeze_engine import dashboard

    seen: list[tuple[int, datetime]] = []

    def fake_discovery_payload(store: NormalizedSqliteStore, *, now: datetime, limit: int) -> dict:
        seen.append((limit, now))
        return {
            "generated_at": "2026-10-23T00:00:00Z",
            "coverage": {"assets_seen": 2, "assets_eligible": 1},
            "candidates": [{
                "asset": "IRYS", "score": 74, "status": "watch", "venues": ["binance", "bybit"],
                "price_change_1h_pct": "-2", "oi_change_1h_pct": "4",
                "funding_rate": "0.0001", "premium_bps": "-8",
                "updated_at": "2026-10-23T00:00:00Z", "quality_flags": [],
                "reasons": ["OI 上升而价格下跌"],
            }],
        }

    monkeypatch.setattr(dashboard, "discovery_payload", fake_discovery_payload)
    with NormalizedSqliteStore(tmp_path / "observations.sqlite") as store:
        payload = get_json(store, "/api/discovery?limit=1")
        assert payload["coverage"]["assets_seen"] == 2
        assert payload["candidates"][0]["asset"] == "IRYS"
        assert seen[0][0] == 1 and seen[0][1].tzinfo is not None
        assert get_json(store, "/api/discovery")["candidates"]
        assert seen[1][0] == 50
        for path in ("/api/discovery?limit=0", "/api/discovery?limit=201", "/api/discovery?limit=abc", "/api/discovery?limit=1&limit=2"):
            status, _, _ = dispatch(store, path)
            assert status == 400
        status, _, _ = dispatch(store, "/api/discovery", "POST")
        assert status == 405


def test_discovery_http_reads_ranked_multisymbol_data(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc)
    old = (now - timedelta(minutes=65)).isoformat()
    recent = (now - timedelta(minutes=5)).isoformat()
    with NormalizedSqliteStore(tmp_path / "observations.sqlite") as store:
        for metric, value, unit, related, related_unit, stamp in (
            ("perp_last", "1", "USD", None, None, old),
            ("open_interest", "1000000", "tokens", "2000000", "USD", old),
            ("perp_last", "1.05", "USD", None, None, recent),
            ("open_interest", "1100000", "tokens", "2310000", "USD", recent),
        ):
            store.append(observation(
                metric, value, unit=unit, related_value=related, related_unit=related_unit,
                source_time=stamp, received=stamp, venue="binance", asset="LAB",
            ))
        payload = get_json(store, "/api/discovery?limit=5")
        assert payload["coverage"]["assets_seen"] == 1
        assert payload["coverage"]["assets_with_1h_baseline"] == 1
        assert payload["candidates"][0]["asset"] == "LAB"
        assert payload["candidates"][0]["price_change_1h_pct"] == "5.00"
        assert payload["candidates"][0]["oi_change_1h_pct"] == "10.00"


def test_healthz_reports_stale_and_fresh_ingestion_even_for_late_old_event(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc)
    old_source = (now - timedelta(seconds=500)).isoformat()
    old_receipt = (now - timedelta(seconds=495)).isoformat()
    late_source = (now - timedelta(seconds=800)).isoformat()
    fresh_receipt = (now - timedelta(seconds=5)).isoformat()
    db_path = tmp_path / "observations.sqlite"
    with NormalizedSqliteStore(db_path) as store:
        store.append(observation(
            "mark_price", "1", source_time=old_source, received=old_receipt,
        ))
        stale = get_json(store, "/healthz")
        assert stale["data_ready"] is False
        assert stale["latest_received_at"] == old_receipt.replace("+00:00", "Z")
        assert stale["age_seconds"] >= 490

        # A late historical source event is still proof that ingestion is alive.
        store.append(observation(
            "mark_price", "0.9", source_time=late_source, received=fresh_receipt,
        ))
        fresh = get_json(store, "/healthz")
        assert fresh["data_ready"] is True
        assert fresh["latest_received_at"] == fresh_receipt.replace("+00:00", "Z")
        assert 0 <= fresh["age_seconds"] < 180
        with NormalizedSqliteStore(db_path, read_only=True) as reader:
            assert get_json(reader, "/healthz")["data_ready"] is True
            assert get_json(reader, "/api/snapshot")["observations"]


def test_healthz_tracks_all_assets_not_only_irys(tmp_path: Path) -> None:
    now = datetime.now(timezone.utc)
    with NormalizedSqliteStore(tmp_path / "observations.sqlite") as store:
        store.append(observation(
            "mark_price", "1", source_time=(now - timedelta(seconds=5)).isoformat(),
            received=(now - timedelta(seconds=4)).isoformat(), asset="LAB",
        ))
        health = get_json(store, "/healthz")
        assert health["asset"] is None
        assert health["data_ready"] is True
        assert health["age_seconds"] < 180


def test_healthz_uses_receipt_index_without_counting_future_source(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch,
) -> None:
    now = datetime.now(timezone.utc)
    visible_receipt = (now - timedelta(seconds=10)).isoformat()
    future_receipt = (now - timedelta(seconds=2)).isoformat()
    with NormalizedSqliteStore(tmp_path / "observations.sqlite") as store:
        store.append(observation(
            "mark_price", "1", source_time=(now - timedelta(seconds=11)).isoformat(),
            received=visible_receipt,
        ))
        store.append(observation(
            "mark_price", "2", source_time=(now + timedelta(days=1)).isoformat(),
            received=future_receipt,
        ))

        real_connect = sqlite3.connect
        plan_details: list[str] = []

        class PlanConnection:
            def __init__(self, connection: sqlite3.Connection) -> None:
                self.connection = connection

            def execute(self, query: str, params: tuple[str, ...]) -> sqlite3.Cursor:
                plan_details.extend(row[3] for row in self.connection.execute(
                    "EXPLAIN QUERY PLAN " + query, params,
                ))
                return self.connection.execute(query, params)

            def close(self) -> None:
                self.connection.close()

        monkeypatch.setattr(
            sqlite3, "connect", lambda *args, **kwargs: PlanConnection(
                real_connect(*args, **kwargs),
            ),
        )
        health = get_json(store, "/healthz")
        assert health["latest_received_at"] == visible_receipt.replace("+00:00", "Z")
        assert health["data_ready"] is True
        assert any("idx_normalized_received_at" in detail for detail in plan_details)
        assert all("TEMP B-TREE" not in detail for detail in plan_details)


def test_dashboard_rejects_non_loopback_bind(tmp_path: Path) -> None:
    with NormalizedSqliteStore(tmp_path / "observations.sqlite") as store:
        with pytest.raises(ValueError, match="127.0.0.1"):
            make_server(store, host="0.0.0.0", port=0)
