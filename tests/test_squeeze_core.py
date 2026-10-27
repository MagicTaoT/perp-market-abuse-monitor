from __future__ import annotations

import json
import sqlite3
from datetime import datetime, timedelta, timezone
from decimal import Decimal

import pytest

from gold_basis_mm_bot.squeeze_engine import (
    NormalizedSnapshot,
    NormalizedSqliteStore,
    NormalizationError,
    Provenance,
    RawJsonlStore,
    change_pct,
    normalize_open_interest,
    open_interest_change,
    premium_bps,
    ratio,
)


BASE_TIME = datetime(2026, 10, 23, 12, 0, tzinfo=timezone.utc)


def _snapshot(
    metric: str,
    value: str,
    at: datetime,
    *,
    venue: str = "venue-a",
    related: str | None = None,
    raw_id: str | None = None,
) -> NormalizedSnapshot:
    return NormalizedSnapshot(
        asset="BTC",
        metric=metric,
        value=Decimal(value),
        unit="tokens" if metric == "open_interest" else "USD",
        provenance=Provenance(
            source="public_api",
            venue=venue,
            instrument="BTCUSDT",
            source_timestamp=at,
            received_at=at + timedelta(milliseconds=30),
            raw_id=raw_id,
        ),
        related_value=Decimal(related) if related is not None else None,
        related_unit="USD" if related is not None else None,
    )


def test_contract_oi_requires_verified_linear_base_multiplier() -> None:
    normalized = normalize_open_interest(
        "12", unit="contracts", price_usd="50000", contract_type="linear",
        contract_multiplier="0.001", multiplier_unit="tokens",
    )
    assert normalized.tokens == Decimal("0.012")
    assert normalized.usd == Decimal("600.000")

    for metadata in (
        {},
        {"contract_type": "inverse", "contract_multiplier": "1", "multiplier_unit": "tokens"},
        {"contract_type": "linear", "contract_multiplier": "0", "multiplier_unit": "tokens"},
        {"contract_type": "linear", "contract_multiplier": "1", "multiplier_unit": "USD"},
        {"contract_type": "linear", "multiplier_unit": "tokens"},
    ):
        with pytest.raises(NormalizationError):
            normalize_open_interest("12", unit="contracts", price_usd="50000", **metadata)


def test_explicit_token_and_usd_oi_units_do_not_guess_contract_size() -> None:
    assert normalize_open_interest("2", unit="tokens", price_usd="50") == (
        normalize_open_interest("100", unit="usd", price_usd="50")
    )
    with pytest.raises(NormalizationError):
        normalize_open_interest("2", unit="mystery", price_usd="50")
    with pytest.raises(NormalizationError):
        normalize_open_interest("2", unit="tokens", price_usd="0")
    with pytest.raises(NormalizationError):
        normalize_open_interest("2", unit="tokens", price_usd="50", contract_type="nonlinear")


def test_snapshot_requires_utc_aware_provenance_and_tracks_missing_source_time() -> None:
    with pytest.raises(ValueError, match="timezone-aware"):
        Provenance("api", "venue", "BTC", BASE_TIME.replace(tzinfo=None), BASE_TIME)
    missing = Provenance("api", "venue", "BTC", None, BASE_TIME)
    snapshot = NormalizedSnapshot("BTC", "mark_price", Decimal("10"), "USD", missing)
    assert snapshot.as_of == BASE_TIME
    assert snapshot.quality_flags == ("missing_source_timestamp",)
    assert snapshot.quality_status == "degraded"
    offset = timezone(timedelta(hours=9))
    shifted = Provenance("api", "venue", "BTC", BASE_TIME.astimezone(offset), BASE_TIME)
    assert shifted.source_timestamp == BASE_TIME


def test_raw_id_links_jsonl_and_normalized_as_of_sqlite(tmp_path) -> None:
    raw_store = RawJsonlStore(tmp_path / "raw" / "events.jsonl")
    raw_id = raw_store.append({"venue": "venue-a", "oi": Decimal("100.25"), "at": BASE_TIME})
    written = json.loads((tmp_path / "raw" / "events.jsonl").read_text())
    assert written["raw_id"] == raw_id
    assert written["oi"] == "100.25"
    assert written["at"] == BASE_TIME.isoformat(timespec="microseconds")

    earlier = _snapshot("open_interest", "2", BASE_TIME, related="100", raw_id=raw_id)
    later = _snapshot("open_interest", "3", BASE_TIME + timedelta(minutes=1), related="180")
    other_venue = _snapshot("open_interest", "7", BASE_TIME, venue="venue-b", related="350")
    mark = _snapshot("mark_price", "60", BASE_TIME, venue="venue-a")
    with NormalizedSqliteStore(tmp_path / "normalized.sqlite") as store:
        for sample in (earlier, later, other_venue, mark):
            store.append(sample)
        before_later = BASE_TIME + timedelta(seconds=30)
        selected = store.get_as_of("BTC", "open_interest", before_later, venue="venue-a")
        assert selected == earlier
        assert selected is not None and selected.provenance.raw_id == raw_id
        assert store.get_as_of("BTC", "open_interest", BASE_TIME - timedelta(seconds=1)) is None
        latest_pairs = store.list_as_of("BTC", before_later, metrics=["open_interest"])
        assert {(item.metric, item.provenance.venue) for item in latest_pairs} == {
            ("open_interest", "venue-a"), ("open_interest", "venue-b")
        }
        assert store.list_as_of("BTC", before_later, metrics=[]) == []


def test_sqlite_history_is_bounded_and_ordered_as_of(tmp_path) -> None:
    with NormalizedSqliteStore(tmp_path / "observations.sqlite") as store:
        assert store.history("BTC", "mark_price") == []
        for minute in (2, 0, 1):
            store.append(_snapshot("mark_price", str(100 + minute), BASE_TIME + timedelta(minutes=minute)))
        history = store.history("BTC", "mark_price", limit=2)
        assert [sample.value for sample in history] == [Decimal("101"), Decimal("102")]
        window = store.history(
            "BTC", "mark_price", since=BASE_TIME,
            until=BASE_TIME + timedelta(minutes=1, seconds=1),
        )
        assert [sample.value for sample in window] == [Decimal("100"), Decimal("101")]
        with pytest.raises(ValueError, match="positive"):
            store.history("BTC", "mark_price", limit=0)
        with pytest.raises(ValueError, match="timezone-aware"):
            store.get_as_of("BTC", "mark_price", BASE_TIME.replace(tzinfo=None))


def test_sqlite_read_only_mode_reads_existing_database_without_write_access(tmp_path) -> None:
    path = tmp_path / "observations.sqlite"
    sample = _snapshot("mark_price", "100", BASE_TIME)
    with NormalizedSqliteStore(path) as writer:
        writer.append(sample)
    with NormalizedSqliteStore(path, read_only=True) as reader:
        assert reader.get_as_of("BTC", "mark_price", BASE_TIME + timedelta(seconds=1)) == sample
        with pytest.raises(PermissionError, match="read-only"):
            reader.append(sample)
    absent = tmp_path / "does-not-exist" / "missing.sqlite"
    with pytest.raises(sqlite3.OperationalError):
        NormalizedSqliteStore(absent, read_only=True)
    assert not absent.parent.exists()


def test_as_of_never_looks_ahead_to_late_receipt_or_downgrades_on_old_event(tmp_path) -> None:
    source_time = BASE_TIME
    late_receipt = BASE_TIME + timedelta(minutes=5)
    delayed = NormalizedSnapshot(
        asset="BTC", metric="mark_price", value=Decimal("100"), unit="USD",
        provenance=Provenance(
            source="api", venue="venue-a", instrument="BTCUSDT",
            source_timestamp=source_time, received_at=late_receipt,
        ),
    )
    assert delayed.as_of == late_receipt
    with NormalizedSqliteStore(tmp_path / "late.sqlite") as store:
        store.append(delayed)
        assert store.get_as_of(
            "BTC", "mark_price", BASE_TIME + timedelta(minutes=3), venue="venue-a"
        ) is None
        assert store.history(
            "BTC", "mark_price", until=BASE_TIME + timedelta(minutes=3)
        ) == []
        assert store.get_as_of(
            "BTC", "mark_price", BASE_TIME + timedelta(minutes=5), venue="venue-a"
        ) == delayed

        newer_source = NormalizedSnapshot(
            asset="BTC", metric="mark_price", value=Decimal("102"), unit="USD",
            provenance=Provenance(
                source="api", venue="venue-a", instrument="BTCUSDT",
                source_timestamp=BASE_TIME + timedelta(minutes=4),
                received_at=BASE_TIME + timedelta(minutes=6),
            ),
        )
        store.append(newer_source)
        older_source_later_receipt = NormalizedSnapshot(
            asset="BTC", metric="mark_price", value=Decimal("99"), unit="USD",
            provenance=Provenance(
                source="api", venue="venue-a", instrument="BTCUSDT",
                source_timestamp=BASE_TIME + timedelta(minutes=2),
                received_at=BASE_TIME + timedelta(minutes=7),
            ),
        )
        store.append(older_source_later_receipt)
        as_of = BASE_TIME + timedelta(minutes=8)
        assert store.get_as_of("BTC", "mark_price", as_of, venue="venue-a") == newer_source
        history = store.history("BTC", "mark_price")
        assert history[-1].value == Decimal("99")
        assert "out_of_order_source_timestamp" in history[-1].quality_flags
        assert store.list_as_of("BTC", as_of, metrics=["mark_price"])[0] == newer_source

        old_oi = _snapshot("open_interest", "10", BASE_TIME + timedelta(minutes=4), related="1000")
        late_old_oi = NormalizedSnapshot(
            asset="BTC", metric="open_interest", value=Decimal("11"), unit="tokens",
            related_value=Decimal("1100"), related_unit="USD",
            provenance=Provenance(
                source="public_api", venue="venue-a", instrument="BTCUSDT",
                source_timestamp=BASE_TIME + timedelta(minutes=3),
                received_at=BASE_TIME + timedelta(minutes=8),
            ),
        )
        with pytest.raises(ValueError, match="source event predates"):
            open_interest_change(late_old_oi, old_oi, at=as_of)


def test_premium_ratio_and_change_math() -> None:
    assert premium_bps("101", "100") == Decimal("100.00")
    assert ratio("3", "2") == Decimal("1.5")
    assert change_pct("110", "100") == Decimal("10.0")
    assert change_pct("3", "0") is None
    with pytest.raises(ValueError):
        ratio("1", "0")


def test_oi_change_marks_stale_price_revaluation_and_separates_flow() -> None:
    previous = _snapshot("open_interest", "10", BASE_TIME, related="1000")
    revalued = _snapshot("open_interest", "10", BASE_TIME + timedelta(minutes=1), related="1200")
    result = open_interest_change(
        revalued, previous, at=BASE_TIME + timedelta(minutes=10), max_age_seconds=120
    )
    assert result.tokens_delta == 0
    assert result.usd_delta == 200
    assert result.token_flow_usd == 0
    assert result.price_revaluation_usd == 200
    assert "price_revaluation_only" in result.quality_flags
    assert "stale_oi" in result.quality_flags

    grown = _snapshot("open_interest", "12", BASE_TIME + timedelta(minutes=2), related="1440")
    movement = open_interest_change(grown, previous, at=BASE_TIME + timedelta(minutes=2))
    assert movement.tokens_change_pct == 20
    assert movement.token_flow_usd == 240
    assert movement.price_revaluation_usd == 200
    assert movement.token_flow_usd + movement.price_revaluation_usd == movement.usd_delta
    assert "price_revaluation_only" not in movement.quality_flags

    delayed = NormalizedSnapshot(
        asset="BTC", metric="open_interest", value=Decimal("12"), unit="tokens",
        related_value=Decimal("1440"), related_unit="USD",
        provenance=Provenance(
            source="public_api", venue="venue-a", instrument="BTCUSDT",
            source_timestamp=BASE_TIME + timedelta(minutes=2),
            received_at=BASE_TIME + timedelta(minutes=7),
        ),
    )
    delayed_change = open_interest_change(
        delayed, previous, at=BASE_TIME + timedelta(minutes=7), max_age_seconds=120
    )
    assert "stale_oi" in delayed_change.quality_flags
