"""Append-only raw records and as-of normalized observations."""

from __future__ import annotations

import json
import os
import sqlite3
import threading
import uuid
import gzip
from dataclasses import replace
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Mapping, Sequence

from .models import NormalizedSnapshot, Provenance, utc_datetime


def _timestamp(value: datetime) -> str:
    return utc_datetime(value).isoformat(timespec="microseconds")


def _json_default(value: object) -> str:
    if isinstance(value, datetime):
        return _timestamp(value)
    if isinstance(value, date):
        return value.isoformat()
    if isinstance(value, Decimal):
        if not value.is_finite():
            raise ValueError("raw record contains a non-finite Decimal")
        return str(value)
    raise TypeError(f"cannot serialize {type(value).__name__}")


class RawJsonlStore:
    """Write one complete JSON object per line and return a traceable raw ID."""

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def append(self, record: Mapping[str, Any]) -> str:
        if not isinstance(record, Mapping):
            raise TypeError("raw record must be a mapping")
        raw_id = uuid.uuid4().hex
        payload = dict(record)
        if "raw_id" in payload:
            payload["upstream_raw_id"] = payload.pop("raw_id")
        payload["raw_id"] = raw_id
        encoded = (json.dumps(
            payload, default=_json_default, ensure_ascii=False,
            allow_nan=False, separators=(",", ":"),
        ) + "\n").encode("utf-8")
        with self._lock:
            fd = os.open(self.path, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
            try:
                written = os.write(fd, encoded)
                if written != len(encoded):
                    raise OSError("short JSONL append")
            finally:
                os.close(fd)
        return raw_id


class CompressedDailyRawStore:
    """Append bulk response records to UTC-day gzip JSONL files.

    A fresh gzip member is appended for each response, so a completed append is
    readable without closing a long-lived compressor. Standard gzip readers
    transparently read concatenated members.
    """

    def __init__(self, directory: str | Path) -> None:
        self.directory = Path(directory)
        self.directory.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()

    def append(self, record: Mapping[str, Any]) -> str:
        if not isinstance(record, Mapping):
            raise TypeError("raw record must be a mapping")
        raw_id = uuid.uuid4().hex
        payload = dict(record)
        if "raw_id" in payload:
            payload["upstream_raw_id"] = payload.pop("raw_id")
        payload["raw_id"] = raw_id
        encoded = (json.dumps(payload, default=_json_default, ensure_ascii=False,
                              allow_nan=False, separators=(",", ":")) + "\n").encode("utf-8")
        day = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        target = self.directory / f"{day}.jsonl.gz"
        with self._lock:
            fd = os.open(target, os.O_WRONLY | os.O_APPEND | os.O_CREAT, 0o600)
            try:
                with os.fdopen(fd, "ab") as file, gzip.GzipFile(fileobj=file, mode="wb") as compressed:
                    compressed.write(encoded)
            except Exception:
                # fdopen owns fd after success; if it failed, close the raw fd.
                try:
                    os.close(fd)
                except OSError:
                    pass
                raise
        return raw_id


class NormalizedSqliteStore:
    """SQLite store for UTC observations, with availability-time as-of queries."""

    def __init__(self, path: str | Path, *, read_only: bool = False) -> None:
        self.path = Path(path)
        self.read_only = read_only
        if not read_only:
            self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        if read_only:
            self._conn = sqlite3.connect(
                self.path.resolve().as_uri() + "?mode=ro",
                uri=True, timeout=10, check_same_thread=False,
            )
        else:
            self._conn = sqlite3.connect(self.path, timeout=10, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        if read_only:
            return
        self._conn.execute("PRAGMA busy_timeout=10000")
        self._conn.execute("PRAGMA journal_mode=WAL")
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS normalized_snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                asset TEXT NOT NULL,
                metric TEXT NOT NULL,
                value TEXT NOT NULL,
                unit TEXT NOT NULL,
                related_value TEXT,
                related_unit TEXT,
                source TEXT NOT NULL,
                venue TEXT NOT NULL,
                instrument TEXT NOT NULL,
                source_timestamp TEXT,
                received_at TEXT NOT NULL,
                as_of TEXT NOT NULL,
                raw_id TEXT,
                quality_flags TEXT NOT NULL,
                collection_tier TEXT NOT NULL DEFAULT 'detail'
            );
            CREATE INDEX IF NOT EXISTS idx_normalized_as_of
                ON normalized_snapshots (asset, metric, venue, as_of DESC, id DESC);
            CREATE INDEX IF NOT EXISTS idx_normalized_asset_time
                ON normalized_snapshots (asset, as_of DESC, id DESC);
            CREATE INDEX IF NOT EXISTS idx_normalized_recent
                ON normalized_snapshots (as_of DESC, asset, venue, metric);
            CREATE INDEX IF NOT EXISTS idx_normalized_received_at
                ON normalized_snapshots (received_at DESC);
        """)
        columns = {row["name"] for row in self._conn.execute("PRAGMA table_info(normalized_snapshots)")}
        if "collection_tier" not in columns:
            self._conn.execute("""
                ALTER TABLE normalized_snapshots
                ADD COLUMN collection_tier TEXT NOT NULL DEFAULT 'detail'
            """)
        self._conn.execute("""
            CREATE INDEX IF NOT EXISTS idx_normalized_bulk_age
            ON normalized_snapshots (collection_tier, as_of, id)
        """)
        self._conn.executescript("""
            CREATE TABLE IF NOT EXISTS tokenomics_scans (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                received_at TEXT NOT NULL,
                identity_status TEXT NOT NULL,
                unlock_events TEXT NOT NULL,
                errors TEXT NOT NULL
            );
            CREATE INDEX IF NOT EXISTS idx_tokenomics_scans_received
                ON tokenomics_scans (received_at DESC, id DESC);
        """)

    def close(self) -> None:
        with self._lock:
            self._conn.close()

    def __enter__(self) -> NormalizedSqliteStore:
        return self

    def __exit__(self, _exc_type: object, _exc: object, _tb: object) -> None:
        self.close()

    def append(self, snapshot: NormalizedSnapshot) -> int:
        if self.read_only:
            raise PermissionError("normalized SQLite store is read-only")
        if not isinstance(snapshot, NormalizedSnapshot):
            raise TypeError("snapshot must be a NormalizedSnapshot")
        with self._lock, self._conn:
            return self._append_locked(snapshot)

    def append_many(self, snapshots: Sequence[NormalizedSnapshot]) -> list[int]:
        """Commit a complete collection batch in one SQLite transaction."""
        if self.read_only:
            raise PermissionError("normalized SQLite store is read-only")
        if any(not isinstance(item, NormalizedSnapshot) for item in snapshots):
            raise TypeError("all snapshots must be NormalizedSnapshot values")
        with self._lock, self._conn:
            return [self._append_locked(item) for item in snapshots]

    def append_tokenomics_scan(
        self,
        *,
        received_at: datetime,
        identity_status: Mapping[str, Any],
        unlock_events: Sequence[Mapping[str, Any]],
        errors: Sequence[str] = (),
    ) -> int:
        """Keep provider matches and unlock calendar as point-in-time evidence."""
        if self.read_only:
            raise PermissionError("normalized SQLite store is read-only")
        values = (
            _timestamp(received_at),
            json.dumps(identity_status, default=_json_default, ensure_ascii=False),
            json.dumps(unlock_events, default=_json_default, ensure_ascii=False),
            json.dumps(errors, ensure_ascii=False),
        )
        with self._lock, self._conn:
            cursor = self._conn.execute("""
                INSERT INTO tokenomics_scans
                    (received_at, identity_status, unlock_events, errors)
                VALUES (?, ?, ?, ?)
            """, values)
            return int(cursor.lastrowid)

    def tokenomics_scan_as_of(
        self, as_of: datetime, *, successful_only: bool = False,
        max_age: timedelta | None = None,
    ) -> dict[str, Any] | None:
        """Return a received scan, optionally requiring complete, recent evidence."""
        conditions = ["received_at <= ?"]
        params = [_timestamp(as_of)]
        if successful_only:
            conditions.append("errors = '[]'")
        if max_age is not None:
            if max_age < timedelta(0):
                raise ValueError("max_age must be nonnegative")
            conditions.append("received_at >= ?")
            params.append(_timestamp(as_of - max_age))
        with self._lock:
            try:
                row = self._conn.execute("""
                    SELECT received_at, identity_status, unlock_events, errors
                    FROM tokenomics_scans WHERE """ + " AND ".join(conditions) + """
                    ORDER BY received_at DESC, id DESC LIMIT 1
                """, params).fetchone()
            except sqlite3.OperationalError as exc:
                if "no such table: tokenomics_scans" not in str(exc):
                    raise
                return None
        if row is None:
            return None
        return {
            "received_at": row["received_at"],
            "identity_status": json.loads(row["identity_status"]),
            "unlock_events": json.loads(row["unlock_events"]),
            "errors": json.loads(row["errors"]),
        }

    def _append_locked(self, snapshot: NormalizedSnapshot) -> int:
        provenance = snapshot.provenance
        if provenance.source_timestamp is not None:
            previous = self._conn.execute("""
                SELECT MAX(source_timestamp) AS latest_source_timestamp
                FROM normalized_snapshots
                WHERE asset = ? AND metric = ? AND venue = ? AND source = ?
                  AND instrument = ? AND received_at <= ?
                  AND source_timestamp IS NOT NULL
            """, (
                snapshot.asset, snapshot.metric, provenance.venue,
                provenance.source, provenance.instrument,
                _timestamp(provenance.received_at),
            )).fetchone()
            latest = previous["latest_source_timestamp"] if previous else None
            if latest is not None and _timestamp(provenance.source_timestamp) < latest:
                snapshot = replace(snapshot, quality_flags=(
                    *snapshot.quality_flags, "out_of_order_source_timestamp"
                ))
        values = (
            snapshot.asset, snapshot.metric, str(snapshot.value), snapshot.unit,
            str(snapshot.related_value) if snapshot.related_value is not None else None,
            snapshot.related_unit, provenance.source, provenance.venue,
            provenance.instrument,
            _timestamp(provenance.source_timestamp) if provenance.source_timestamp else None,
            _timestamp(provenance.received_at), _timestamp(snapshot.as_of),
            provenance.raw_id, json.dumps(snapshot.quality_flags),
            "bulk" if "bulk_scan" in snapshot.quality_flags else "detail",
        )
        cursor = self._conn.execute("""
            INSERT INTO normalized_snapshots (
                asset, metric, value, unit, related_value, related_unit,
                source, venue, instrument, source_timestamp, received_at,
                as_of, raw_id, quality_flags, collection_tier
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, values)
        return int(cursor.lastrowid)

    def prune_bulk(self, before: datetime, *, max_rows: int = 20000) -> int:
        """Discard old derived bulk rows; compressed source replies stay intact."""
        if self.read_only:
            raise PermissionError("normalized SQLite store is read-only")
        if isinstance(max_rows, bool) or not isinstance(max_rows, int) or max_rows <= 0:
            raise ValueError("max_rows must be a positive integer")
        with self._lock, self._conn:
            cursor = self._conn.execute("""
                DELETE FROM normalized_snapshots WHERE id IN (
                    SELECT id FROM normalized_snapshots
                    WHERE collection_tier = 'bulk' AND as_of < ?
                    ORDER BY as_of ASC, id ASC LIMIT ?
                )
            """, (_timestamp(before), max_rows))
            return cursor.rowcount

    def discovery_samples(
        self,
        as_of: datetime,
        *,
        min_age_seconds: int,
        max_age_seconds: int,
        metrics: Sequence[str],
    ) -> list[NormalizedSnapshot]:
        """Latest available observation per instrument/metric in one age band."""
        if not 0 <= min_age_seconds <= max_age_seconds or not metrics:
            raise ValueError("invalid discovery age band or metrics")
        high = _timestamp(as_of - timedelta(seconds=min_age_seconds))
        low = _timestamp(as_of - timedelta(seconds=max_age_seconds))
        placeholders = ",".join("?" for _ in metrics)
        query = f"""
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY asset, venue, instrument, metric
                    ORDER BY COALESCE(source_timestamp, received_at) DESC,
                             as_of DESC, id DESC
                ) AS rank_in_group
                FROM normalized_snapshots
                WHERE as_of >= ? AND as_of <= ?
                  AND metric IN ({placeholders})
            ) WHERE rank_in_group = 1
        """
        with self._lock:
            rows = self._conn.execute(query, (low, high, *metrics)).fetchall()
        return [_from_row(row) for row in rows]

    def get_as_of(
        self, asset: str, metric: str, as_of: datetime, *, venue: str | None = None
    ) -> NormalizedSnapshot | None:
        clauses = ["asset = ?", "metric = ?", "as_of <= ?"]
        params: list[object] = [asset, metric, _timestamp(as_of)]
        if venue is not None:
            clauses.append("venue = ?")
            params.append(venue)
        query = (
            "SELECT * FROM normalized_snapshots WHERE " + " AND ".join(clauses)
            + " ORDER BY COALESCE(source_timestamp, received_at) DESC,"
              " as_of DESC, id DESC LIMIT 1"
        )
        with self._lock:
            row = self._conn.execute(query, params).fetchone()
        return _from_row(row) if row is not None else None

    def list_as_of(
        self,
        asset: str,
        as_of: datetime,
        *,
        metrics: Sequence[str] | None = None,
        venue: str | None = None,
    ) -> list[NormalizedSnapshot]:
        """Return the latest observation for each (metric, venue) pair."""
        clauses = ["asset = ?", "as_of <= ?"]
        params: list[object] = [asset, _timestamp(as_of)]
        if metrics is not None:
            if not metrics:
                return []
            clauses.append("metric IN (" + ",".join("?" for _ in metrics) + ")")
            params.extend(metrics)
        if venue is not None:
            clauses.append("venue = ?")
            params.append(venue)
        query = """
            SELECT * FROM (
                SELECT *, ROW_NUMBER() OVER (
                    PARTITION BY metric, venue
                    ORDER BY COALESCE(source_timestamp, received_at) DESC,
                             as_of DESC, id DESC
                ) AS rank_in_group
                FROM normalized_snapshots WHERE %s
            ) WHERE rank_in_group = 1 ORDER BY metric, venue
        """ % " AND ".join(clauses)
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [_from_row(row) for row in rows]

    def history(
        self,
        asset: str,
        metric: str,
        *,
        venue: str | None = None,
        since: datetime | None = None,
        until: datetime | None = None,
        limit: int = 500,
    ) -> list[NormalizedSnapshot]:
        """Return up to ``limit`` most recent rows, in ascending time order."""
        if isinstance(limit, bool) or not isinstance(limit, int) or limit <= 0:
            raise ValueError("limit must be a positive integer")
        clauses = ["asset = ?", "metric = ?"]
        params: list[object] = [asset, metric]
        if venue is not None:
            clauses.append("venue = ?")
            params.append(venue)
        if since is not None:
            clauses.append("as_of >= ?")
            params.append(_timestamp(since))
        if until is not None:
            clauses.append("as_of <= ?")
            params.append(_timestamp(until))
        query = (
            "SELECT * FROM (SELECT * FROM normalized_snapshots WHERE "
            + " AND ".join(clauses)
            + " ORDER BY as_of DESC, id DESC LIMIT ?) ORDER BY as_of ASC, id ASC"
        )
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(query, params).fetchall()
        return [_from_row(row) for row in rows]


def _from_row(row: sqlite3.Row) -> NormalizedSnapshot:
    return NormalizedSnapshot(
        asset=row["asset"], metric=row["metric"], value=Decimal(row["value"]),
        unit=row["unit"],
        provenance=Provenance(
            source=row["source"], venue=row["venue"], instrument=row["instrument"],
            source_timestamp=(
                datetime.fromisoformat(row["source_timestamp"])
                if row["source_timestamp"] else None
            ),
            received_at=datetime.fromisoformat(row["received_at"]),
            raw_id=row["raw_id"],
        ),
        quality_flags=tuple(json.loads(row["quality_flags"])),
        related_value=Decimal(row["related_value"]) if row["related_value"] else None,
        related_unit=row["related_unit"],
    )
