"""Explainable, point-in-time shortlist from low-frequency market snapshots.

Scores are research triage priorities, not trade signals or probabilities.
Each change is calculated within one venue and instrument. OI changes use
token quantity so price revaluation cannot masquerade as new positioning.
"""

from __future__ import annotations

from collections import defaultdict
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any

from .features import change_pct
from .discovery_enrichment import enrich_candidate
from .models import NormalizedSnapshot, utc_datetime
from .storage import NormalizedSqliteStore


CURRENT_METRICS = ("perp_last", "open_interest", "funding_rate", "premium_bps", "perp_turnover_24h")
BASELINE_METRICS = ("perp_last", "open_interest")
_UNSAFE = frozenset({
    "out_of_order_source_timestamp", "stale_exchange_timestamp",
    "exchange_clock_ahead", "oi_usd_value_disagreement",
})
_MIN_OI_USD = Decimal("1000000")


def _group(samples: list[NormalizedSnapshot]) -> dict[tuple[str, str, str], dict[str, NormalizedSnapshot]]:
    result: dict[tuple[str, str, str], dict[str, NormalizedSnapshot]] = defaultdict(dict)
    for snapshot in samples:
        key = (snapshot.asset, snapshot.provenance.venue, snapshot.provenance.instrument)
        result[key][snapshot.metric] = snapshot
    return result


def _pattern(price_pct: Decimal, oi_pct: Decimal) -> tuple[str, str]:
    if price_pct >= 3 and oi_pct >= 5:
        return "build_up", "上涨增仓，需核查杠杆是否继续累积"
    if price_pct >= 5 and oi_pct <= -5:
        return "short_cover", "上涨减仓，可能有空头回补"
    if price_pct <= -3 and oi_pct <= -5:
        return "unwind", "下跌减仓，可能处于去杠杆阶段"
    if price_pct <= -3 and oi_pct >= 5:
        return "short_build", "下跌增仓，需核查空头积累"
    return "watch", "价格与持仓出现变化，尚未达到候选阈值"


def _rounded(value: Decimal | None) -> str | None:
    return str(value.quantize(Decimal("0.01"))) if value is not None else None


def discovery_payload(
    store: NormalizedSqliteStore, *, now: datetime | None = None, limit: int = 50,
) -> dict[str, Any]:
    """Rank current assets using only observations available by ``now``.

    A one-hour comparison requires a same-instrument baseline received 55–85
    minutes earlier. Broad scans at ten or fifteen minute cadence satisfy that
    window; new installations explicitly warm up rather than inventing change.
    """
    if isinstance(limit, bool) or not isinstance(limit, int) or not 1 <= limit <= 200:
        raise ValueError("limit must be an integer from 1 to 200")
    at = utc_datetime(now, "now") if now is not None else datetime.now(timezone.utc)
    current = _group(store.discovery_samples(
        at, min_age_seconds=0, max_age_seconds=25 * 60, metrics=CURRENT_METRICS,
    ))
    baseline = _group(store.discovery_samples(
        at, min_age_seconds=55 * 60, max_age_seconds=85 * 60,
        metrics=BASELINE_METRICS,
    ))
    all_assets = {key[0] for key, rows in current.items() if "perp_last" in rows}
    baseline_assets: set[str] = set()
    comparable_assets: set[str] = set()
    by_asset: dict[str, list[dict[str, Any]]] = defaultdict(list)
    latest_at: datetime | None = None

    for key, rows in current.items():
        price = rows.get("perp_last")
        oi = rows.get("open_interest")
        if price is not None and (latest_at is None or price.as_of > latest_at):
            latest_at = price.as_of
        older = baseline.get(key, {})
        old_price = older.get("perp_last")
        old_oi = older.get("open_interest")
        if not all((price, oi, old_price, old_oi)):
            continue
        assert price is not None and oi is not None
        assert old_price is not None and old_oi is not None
        baseline_assets.add(key[0])
        if (price.unit != "USD" or old_price.unit != "USD"
                or oi.unit != "tokens" or old_oi.unit != "tokens"
                or oi.related_unit != "USD" or oi.related_value is None
                or oi.related_value < _MIN_OI_USD
                or price.value <= 0 or old_price.value <= 0 or old_oi.value <= 0):
            continue
        if any(_UNSAFE.intersection(s.quality_flags) for s in (price, oi, old_price, old_oi)):
            continue
        price_pct = change_pct(price.value, old_price.value)
        oi_pct = change_pct(oi.value, old_oi.value)
        if price_pct is None or oi_pct is None:
            continue
        comparable_assets.add(key[0])
        pattern, explanation = _pattern(price_pct, oi_pct)
        score = min(100, int(
            min(abs(price_pct), Decimal("15")) * 4
            + min(abs(oi_pct), Decimal("30")) * Decimal("1.5")
            + (Decimal("10") if pattern != "watch" else Decimal("0"))
        ))
        funding = rows.get("funding_rate")
        premium = rows.get("premium_bps")
        turnover = rows.get("perp_turnover_24h")
        by_asset[key[0]].append({
            "venue": key[1], "instrument": key[2], "price_pct": price_pct,
            "oi_pct": oi_pct, "score": score, "pattern": pattern,
            "explanation": explanation,
            "price_snapshot": price, "oi_snapshot": oi,
            "funding_snapshot": funding, "premium_snapshot": premium,
            "funding_rate": str(funding.value) if funding else None,
            "premium_bps": str(premium.value) if premium else None,
            "turnover_usd": str(turnover.value) if turnover and turnover.unit == "USD" else None,
            "updated_at": min(price.as_of, oi.as_of),
            "flags": set((*price.quality_flags, *oi.quality_flags)),
        })

    candidates: list[dict[str, Any]] = []
    contexts: dict[str, tuple[dict[str, Any], list[dict[str, Any]]]] = {}
    for asset, signals in by_asset.items():
        signals.sort(key=lambda item: (-item["score"], item["venue"]))
        # Bybit publishes an explicit two-sided OI convention. Anchor the
        # structural calculation there when a comparable Bybit series exists.
        lead = next((item for item in signals if item["venue"] == "bybit"), signals[0])
        agreeing = sum(1 for item in signals if item["pattern"] == lead["pattern"] and item["pattern"] != "watch")
        score = min(100, lead["score"] + (15 if agreeing >= 2 else 0))
        flags = set(lead["flags"])
        if len(signals) == 1:
            flags.add("single_venue_oi_confirmation")
        reasons = [
            f"{lead['venue']} 近1小时价格 {_rounded(lead['price_pct'])}%、持仓数量 {_rounded(lead['oi_pct'])}%",
            lead["explanation"],
        ]
        if agreeing >= 2:
            reasons.append(f"{agreeing} 个交易所出现同方向价格与持仓形态")
        if lead["turnover_usd"] is None:
            flags.add("turnover_unverified_or_missing")
        candidates.append({
            "asset": asset,
            "score": score,
            "status": "candidate" if lead["pattern"] in {"build_up", "short_cover", "unwind"} else "watch",
            "pattern": lead["pattern"],
            "venues": sorted({item["venue"] for item in signals}),
            "price_change_1h_pct": _rounded(lead["price_pct"]),
            "oi_change_1h_pct": _rounded(lead["oi_pct"]),
            "funding_rate": lead["funding_rate"],
            "premium_bps": lead["premium_bps"],
            "turnover_24h_usd": lead["turnover_usd"],
            "updated_at": lead["updated_at"].isoformat(),
            "quality_flags": sorted(flags),
            "reasons": reasons,
        })
        contexts[asset] = (lead, signals)
    candidates.sort(key=lambda item: (-item["score"], item["asset"]))
    # A partial provider failure must not hide the last complete calendar and
    # identity map. The 36-hour bound matches enrichment's evidence gate.
    scan = store.tokenomics_scan_as_of(
        at, successful_only=True, max_age=timedelta(hours=36))
    top_by_old_score = {item["asset"] for item in candidates[:max(35, limit)]}
    top_by_oi = {item["asset"] for item in sorted(
        candidates,
        key=lambda item: max((signal["oi_snapshot"].related_value or Decimal(0)
                              for signal in contexts[item["asset"]][1]), default=Decimal(0)),
        reverse=True,
    )[:20]}
    top_by_funding = {item["asset"] for item in sorted(
        candidates,
        key=lambda item: abs(Decimal(item["funding_rate"])) if item["funding_rate"] else Decimal(0),
        reverse=True,
    )[:20]}
    float_pressure: dict[str, Decimal] = {}
    for item in candidates:
        asset = item["asset"]
        bybit = next((signal["oi_snapshot"] for signal in contexts[asset][1]
                      if signal["venue"] == "bybit"), None)
        if bybit is None:
            continue
        circ = store.get_as_of(asset, "circulating_supply", at, venue="coinmarketcap")
        if (circ is None or circ.value <= 0 or circ.provenance.source_timestamp is None
                or at - circ.as_of > timedelta(days=2)):
            continue
        identity = (scan or {}).get("identity_status", {}).get(asset, {})
        if (not circ.provenance.instrument.startswith("CMC:")
                or identity.get("status") not in {"matched", "override"}
                or str(identity.get("provider_id")) != circ.provenance.instrument[4:]):
            continue
        float_pressure[asset] = bybit.value / 2 / circ.value * 100
    top_by_float = {asset for asset in sorted(
        float_pressure, key=lambda asset: float_pressure[asset], reverse=True)[:20]}
    unlock_assets: set[str] = set()
    if scan:
        for event in scan.get("unlock_events", []):
            if not isinstance(event, dict) or event.get("asset") not in contexts:
                continue
            try:
                days = (date.fromisoformat(str(event["unlock_date"])) - at.date()).days
            except (ValueError, KeyError):
                continue
            if 0 <= days <= 30:
                unlock_assets.add(event["asset"])
    selected_assets = (top_by_old_score | top_by_oi | top_by_funding |
                       top_by_float | unlock_assets)
    candidates = [item for item in candidates if item["asset"] in selected_assets]
    for candidate in candidates:
        lead, signals = contexts[candidate["asset"]]
        enrich_candidate(store, at, candidate, lead, signals, scan)
    candidates.sort(key=lambda item: (-item["rank_lower_bound"], -item["score"], item["asset"]))
    return {
        "generated_at": at.isoformat(),
        "coverage": {
            "assets_seen": len(all_assets),
            "assets_with_1h_baseline": len(baseline_assets),
            "assets_eligible": len(comparable_assets),
            "assets_structurally_enriched": len(candidates),
            "candidate_count": sum(item["status"] == "candidate" for item in candidates),
            "lead_count": sum(item["status"] == "lead" for item in candidates),
            "latest_market_at": latest_at.isoformat() if latest_at else None,
        },
        "candidates": candidates[:limit],
    }
