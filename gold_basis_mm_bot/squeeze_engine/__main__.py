"""Public market-data collector and local read-only dashboard entry point."""

from __future__ import annotations

import argparse
import json
import re
import sys
import threading
import time
from collections import defaultdict
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any

from .connectors import ADAPTERS, PublicHttpClient
from .dashboard import make_server
from .features import change_pct
from .storage import CompressedDailyRawStore, NormalizedSqliteStore, RawJsonlStore
from .tokenomics import collect_tokenomics
from .universe import collect_universe_cycle as fetch_universe_cycle


_ASSET_RE = re.compile(r"[A-Z0-9][A-Z0-9_-]{0,23}\Z")


@dataclass(frozen=True, slots=True)
class CollectorConfig:
    assets: tuple[str, ...]
    poll_interval_seconds: int
    universe_scan_interval_seconds: int | None
    universe_retention_days: int
    data_dir: Path
    dashboard_host: str
    dashboard_port: int
    http_timeout_seconds: float
    http_retries: int
    tokenomics_refresh_interval_seconds: int
    tokenomics_max_assets: int


def load_config(path: str | Path) -> CollectorConfig:
    with Path(path).open(encoding="utf-8") as file:
        data = json.load(file)
    if not isinstance(data, dict):
        raise ValueError("config must be a JSON object")
    allowed = {"assets", "poll_interval_seconds", "data_dir", "dashboard_host", "dashboard_port",
               "http_timeout_seconds", "http_retries", "universe_scan_interval_seconds",
               "universe_retention_days", "tokenomics_refresh_interval_seconds",
               "tokenomics_max_assets"}
    if set(data) - allowed:
        raise ValueError("unsupported config fields: " + ", ".join(sorted(set(data) - allowed)))
    assets = data.get("assets", ["IRYS"])
    if not isinstance(assets, list) or not 1 <= len(assets) <= 30:
        raise ValueError("assets must be a list of 1-30 symbols")
    if any(not isinstance(asset, str) for asset in assets):
        raise ValueError("assets must contain only symbol strings")
    normalized_assets = tuple(asset.upper() for asset in assets)
    if any(not _ASSET_RE.fullmatch(asset) for asset in normalized_assets) or len(set(normalized_assets)) != len(normalized_assets):
        raise ValueError("asset names must be unique 1-24 character symbols")
    interval = data.get("poll_interval_seconds", 60)
    if isinstance(interval, bool) or not isinstance(interval, int) or interval < 15:
        raise ValueError("poll_interval_seconds must be an integer >= 15")
    universe_interval = data.get("universe_scan_interval_seconds")
    if universe_interval is not None and (
        isinstance(universe_interval, bool) or not isinstance(universe_interval, int)
        or not 600 <= universe_interval <= 3600
    ):
        raise ValueError("universe_scan_interval_seconds must be 600-3600 seconds")
    retention_days = data.get("universe_retention_days", 21)
    if isinstance(retention_days, bool) or not isinstance(retention_days, int) or not 7 <= retention_days <= 60:
        raise ValueError("universe_retention_days must be an integer from 7 to 60")
    data_dir = data.get("data_dir")
    if not isinstance(data_dir, str) or not data_dir:
        raise ValueError("data_dir must be a nonempty path")
    host = data.get("dashboard_host", "127.0.0.1")
    if host != "127.0.0.1":
        raise ValueError("dashboard_host must be 127.0.0.1")
    port = data.get("dashboard_port", 8770)
    if isinstance(port, bool) or not isinstance(port, int) or not 1 <= port <= 65535:
        raise ValueError("dashboard_port must be an integer from 1 to 65535")
    timeout = data.get("http_timeout_seconds", 8)
    if isinstance(timeout, bool) or not isinstance(timeout, (int, float)) or not 1 <= timeout <= 30:
        raise ValueError("http_timeout_seconds must be from 1 to 30")
    retries = data.get("http_retries", 1)
    if isinstance(retries, bool) or not isinstance(retries, int) or not 0 <= retries <= 3:
        raise ValueError("http_retries must be from 0 to 3")
    tokenomics_interval = data.get("tokenomics_refresh_interval_seconds", 86400)
    if (isinstance(tokenomics_interval, bool) or not isinstance(tokenomics_interval, int)
            or not 3600 <= tokenomics_interval <= 86400):
        raise ValueError("tokenomics_refresh_interval_seconds must be 3600-86400 seconds")
    tokenomics_max_assets = data.get("tokenomics_max_assets", 75)
    if (isinstance(tokenomics_max_assets, bool) or not isinstance(tokenomics_max_assets, int)
            or not 10 <= tokenomics_max_assets <= 100):
        raise ValueError("tokenomics_max_assets must be 10-100")
    return CollectorConfig(normalized_assets, interval, universe_interval, retention_days,
                           Path(data_dir), host, port, float(timeout), retries,
                           tokenomics_interval, tokenomics_max_assets)


def _log(event: str, **details: Any) -> None:
    print(json.dumps({"ts": datetime.now(timezone.utc).isoformat(), "event": event, **details},
                     ensure_ascii=False, separators=(",", ":")), flush=True)


def collect_cycle(config: CollectorConfig, store: NormalizedSqliteStore, raw: RawJsonlStore,
                  client: PublicHttpClient, *, adapters: list[Any] | None = None) -> dict[str, Any]:
    counts: dict[str, int] = {}
    errors: list[str] = []
    if adapters is None:
        adapters = [adapter_type(client, raw) for adapter_type in ADAPTERS]
    for asset in config.assets:
        for adapter in adapters:
            name = f"{asset}/{adapter.venue}"
            try:
                result = adapter.collect(asset)
                store.append_many(result.snapshots)
                counts[name] = len(result.snapshots)
                errors.extend(result.errors)
            except Exception as exc:
                counts[name] = 0
                errors.append(f"{name} failed: {type(exc).__name__}: {exc}")
    summary: dict[str, Any] = {"counts": counts, "errors": errors}
    _log("collection_cycle", **summary)
    return summary


def scan_universe_cycle(store: NormalizedSqliteStore, raw: CompressedDailyRawStore,
                        client: PublicHttpClient, *, retention_days: int = 21) -> dict[str, Any]:
    """Persist one low-frequency all-market scan, keeping venue failures partial."""
    result = fetch_universe_cycle(client, raw)
    store.append_many(result.snapshots)
    pruned = store.prune_bulk(datetime.now(timezone.utc) - timedelta(days=retention_days))
    counts = {venue: sum(item.provenance.venue == venue for item in result.snapshots)
              for venue in ("kucoin", "bybit", "binance")}
    summary = {
        "counts": counts,
        "assets": len({item.asset for item in result.snapshots}),
        "pruned_bulk_rows": pruned,
        "errors_count": len(result.errors),
        "errors": result.errors[:20],
    }
    _log("universe_scan_cycle", **summary)
    return summary


def scan_tokenomics_cycle(
    config: CollectorConfig, store: NormalizedSqliteStore,
    raw: CompressedDailyRawStore, client: PublicHttpClient,
) -> dict[str, Any]:
    """Enrich a bounded shortlist with public supply and unlock evidence."""
    at = datetime.now(timezone.utc)
    # This inexpensive preselector reads only three narrow age-band queries.
    # It must not run the dashboard's multi-day structural scorer in a collector
    # thread beside the constrained trading host.
    current_rows = store.discovery_samples(
        at, min_age_seconds=0, max_age_seconds=25 * 60,
        metrics=("perp_last", "open_interest", "funding_rate"),
    )
    older_rows = store.discovery_samples(
        at, min_age_seconds=55 * 60, max_age_seconds=85 * 60,
        metrics=("perp_last", "open_interest"),
    )
    current = defaultdict(dict)
    older = defaultdict(dict)
    for row in current_rows:
        current[(row.asset, row.provenance.venue, row.provenance.instrument)][row.metric] = row
    for row in older_rows:
        older[(row.asset, row.provenance.venue, row.provenance.instrument)][row.metric] = row
    movement: dict[str, Decimal] = {}
    oi_size: dict[str, Decimal] = {}
    funding_extreme: dict[str, Decimal] = {}
    for key, rows in current.items():
        asset = key[0]
        oi = rows.get("open_interest")
        price = rows.get("perp_last")
        funding = rows.get("funding_rate")
        if oi and oi.related_value is not None:
            oi_size[asset] = max(oi_size.get(asset, Decimal(0)), oi.related_value)
        if funding:
            funding_extreme[asset] = max(funding_extreme.get(asset, Decimal(0)), abs(funding.value))
        older_pair = older.get(key, {})
        old_price, old_oi = older_pair.get("perp_last"), older_pair.get("open_interest")
        if price and oi and old_price and old_oi and old_price.value > 0 and old_oi.value > 0:
            price_move = change_pct(price.value, old_price.value)
            oi_move = change_pct(oi.value, old_oi.value)
            if price_move is not None and oi_move is not None:
                priority = abs(price_move) * 4 + abs(oi_move) * Decimal("1.5")
                movement[asset] = max(movement.get(asset, Decimal(0)), priority)
    ranked_assets = list(config.assets)
    ranked_assets.extend(sorted(movement, key=lambda asset: movement[asset], reverse=True)[:50])
    ranked_assets.extend(sorted(oi_size, key=lambda asset: oi_size[asset], reverse=True)[:25])
    ranked_assets.extend(sorted(funding_extreme,
                                key=lambda asset: funding_extreme[asset], reverse=True)[:25])
    ranked_assets.extend(sorted(oi_size, key=lambda asset: oi_size[asset], reverse=True))
    assets = tuple(dict.fromkeys(ranked_assets))[:config.tokenomics_max_assets]
    result = collect_tokenomics(assets, client, raw, now=at)
    errors = list(result.errors)
    if not result.snapshots:
        errors.append("tokenomics: no supply observations")
    store.append_many(result.snapshots)
    store.append_tokenomics_scan(
        received_at=datetime.now(timezone.utc),
        identity_status=result.identity_status,
        unlock_events=[asdict(item) for item in result.unlock_events],
        errors=errors,
    )
    summary = {
        "assets_requested": len(assets),
        "assets_matched": sum(item.get("status") in {"matched", "override"}
                              for item in result.identity_status.values()),
        "supply_observations": len(result.snapshots),
        "unlock_events": len(result.unlock_events),
        "errors_count": len(errors),
        "errors": errors[:10],
    }
    _log("tokenomics_scan_cycle", **summary)
    return summary


def _next_tokenomics_due(
    config: CollectorConfig, store: NormalizedSqliteStore, now: datetime,
) -> datetime:
    """Schedule from persisted scan times so a service restart cannot refetch early."""
    successful = store.tokenomics_scan_as_of(now, successful_only=True)
    if successful is not None:
        due = (datetime.fromisoformat(successful["received_at"])
               + timedelta(seconds=config.tokenomics_refresh_interval_seconds))
        if due > now:
            return due
    latest = store.tokenomics_scan_as_of(now)
    if latest is not None and latest["errors"]:
        due = datetime.fromisoformat(latest["received_at"]) + timedelta(hours=1)
        if due > now:
            return due
    return now


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Perp Market Abuse Monitor (read-only research)")
    parser.add_argument("--config", type=Path, required=True)
    mode = parser.add_mutually_exclusive_group(required=True)
    mode.add_argument("--once", action="store_true", help="collect one public REST snapshot")
    mode.add_argument("--serve", action="store_true", help="collect continuously and serve local read-only UI")
    args = parser.parse_args(argv)
    try:
        config = load_config(args.config)
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        parser.error(str(exc))
    config.data_dir.mkdir(parents=True, exist_ok=True)
    raw = RawJsonlStore(config.data_dir / "raw.jsonl")
    universe_raw = (CompressedDailyRawStore(config.data_dir / "raw_universe")
                    if config.universe_scan_interval_seconds is not None else None)
    tokenomics_raw = CompressedDailyRawStore(config.data_dir / "raw_tokenomics")
    with NormalizedSqliteStore(config.data_dir / "normalized.sqlite3") as store:
        client = PublicHttpClient(timeout=config.http_timeout_seconds, retries=config.http_retries)
        if args.once:
            summary = collect_cycle(config, store, raw, client)
            scanned = (scan_universe_cycle(store, universe_raw, client,
                       retention_days=config.universe_retention_days) if universe_raw else None)
            if universe_raw is not None:
                try:
                    scan_tokenomics_cycle(config, store, tokenomics_raw, client)
                except Exception as exc:
                    _log("tokenomics_scan_failed", error=f"{type(exc).__name__}: {exc}")
            return 0 if any(summary["counts"].values()) or (
                scanned is not None and any(scanned["counts"].values())
            ) else 2
        stop = threading.Event()
        first_scan_done = threading.Event()
        adapters = [adapter_type(client, raw) for adapter_type in ADAPTERS]

        def worker() -> None:
            while not stop.is_set():
                started = time.monotonic()
                collect_cycle(config, store, raw, client, adapters=adapters)
                wait = max(0.0, config.poll_interval_seconds - (time.monotonic() - started))
                stop.wait(wait)

        def universe_worker() -> None:
            assert config.universe_scan_interval_seconds is not None and universe_raw is not None
            while not stop.is_set():
                started = time.monotonic()
                try:
                    scan_universe_cycle(store, universe_raw, client,
                                        retention_days=config.universe_retention_days)
                except Exception as exc:
                    _log("universe_scan_failed", error=f"{type(exc).__name__}: {exc}")
                finally:
                    first_scan_done.set()
                wait = max(0.0, config.universe_scan_interval_seconds - (time.monotonic() - started))
                stop.wait(wait)

        def tokenomics_worker() -> None:
            if not first_scan_done.wait(timeout=120) or stop.is_set():
                return
            while not stop.is_set():
                now = datetime.now(timezone.utc)
                due = _next_tokenomics_due(config, store, now)
                if stop.wait(max(0.0, (due - now).total_seconds())):
                    return
                try:
                    scan_tokenomics_cycle(config, store, tokenomics_raw, client)
                except Exception as exc:
                    error = f"{type(exc).__name__}: {exc}"
                    _log("tokenomics_scan_failed", error=error)
                    try:
                        store.append_tokenomics_scan(
                            received_at=datetime.now(timezone.utc),
                            identity_status={}, unlock_events=[], errors=[error])
                    except Exception:
                        # A storage failure cannot persist the retry timer.
                        # Keep this process from spinning until storage recovers.
                        stop.wait(3600)

        server = make_server(store, host=config.dashboard_host, port=config.dashboard_port,
                             data_max_age_seconds=max(180, config.poll_interval_seconds * 2))
        thread = threading.Thread(target=worker, name="public-market-collector", daemon=True)
        thread.start()
        scanner = None
        tokenomics_scanner = None
        if universe_raw is not None:
            scanner = threading.Thread(target=universe_worker, name="public-universe-scanner", daemon=True)
            scanner.start()
            tokenomics_scanner = threading.Thread(target=tokenomics_worker,
                                                  name="public-tokenomics-scanner", daemon=True)
            tokenomics_scanner.start()
        _log("dashboard_started", host=config.dashboard_host, port=config.dashboard_port)
        try:
            server.serve_forever(poll_interval=0.5)
        except KeyboardInterrupt:
            pass
        finally:
            stop.set()
            server.server_close()
            thread.join(timeout=config.http_timeout_seconds + 5)
            if scanner is not None:
                scanner.join(timeout=config.http_timeout_seconds + 5)
            if tokenomics_scanner is not None:
                tokenomics_scanner.join(timeout=config.http_timeout_seconds + 5)
    return 0


if __name__ == "__main__":
    sys.exit(main())
