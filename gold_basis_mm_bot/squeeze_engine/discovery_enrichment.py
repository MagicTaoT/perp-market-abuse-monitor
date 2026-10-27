"""Point-in-time structural context for the read-only discovery shortlist."""

from __future__ import annotations

from bisect import bisect_right
from datetime import date, datetime, timedelta
from decimal import Decimal
from typing import Any, Mapping

from .features import change_pct
from .models import NormalizedSnapshot
from .storage import NormalizedSqliteStore
from .structural_scoring import score_structure


_UNSAFE = frozenset({
    "out_of_order_source_timestamp", "stale_exchange_timestamp",
    "exchange_clock_ahead", "oi_usd_value_disagreement", "source_clock_ahead",
    "stale_tokenomics_timestamp",
})


def _usable(sample: NormalizedSnapshot | None, at: datetime, max_age: timedelta) -> bool:
    return bool(sample is not None and sample.as_of <= at and at - sample.as_of <= max_age
                and not _UNSAFE.intersection(sample.quality_flags))


def _baseline(
    store: NormalizedSqliteStore, current: NormalizedSnapshot | None,
    hours: int,
) -> NormalizedSnapshot | None:
    if current is None:
        return None
    target = current.as_of - timedelta(hours=hours)
    rows = store.history(
        current.asset, current.metric, venue=current.provenance.venue,
        since=target - timedelta(minutes=18),
        until=target + timedelta(minutes=18), limit=12,
    )
    rows = [row for row in rows if row.provenance.instrument == current.provenance.instrument
            and row.unit == current.unit and not _UNSAFE.intersection(row.quality_flags)]
    return min(rows, key=lambda row: abs((row.as_of - target).total_seconds())) if rows else None


def _change(store: NormalizedSqliteStore, current: NormalizedSnapshot | None,
            hours: int) -> Decimal | None:
    baseline = _baseline(store, current, hours)
    if current is None or baseline is None or baseline.value <= 0:
        return None
    return change_pct(current.value, baseline.value)


def _percentile(current: Decimal, values: list[Decimal]) -> Decimal:
    return Decimal(sum(item <= current for item in values)) * 100 / Decimal(len(values))


def _price_percentile(store: NormalizedSqliteStore, current: NormalizedSnapshot | None,
                      at: datetime) -> Decimal | None:
    if current is None:
        return None
    rows = store.history(current.asset, "perp_last", venue=current.provenance.venue,
                         since=at - timedelta(days=21), until=at, limit=40000)
    rows = [row for row in rows if row.provenance.instrument == current.provenance.instrument
            and row.unit == "USD" and row.value > 0
            and not _UNSAFE.intersection(row.quality_flags)]
    if len(rows) < 100 or rows[0].as_of > at - timedelta(days=20):
        return None
    return _percentile(current.value, [row.value for row in rows])


def _oi_float_percentile(store: NormalizedSqliteStore,
                         current: NormalizedSnapshot | None,
                         current_supply: NormalizedSnapshot | None,
                         at: datetime) -> Decimal | None:
    if current is None or current_supply is None or current_supply.value <= 0:
        return None
    oi_rows = store.history(current.asset, "open_interest", venue="bybit",
                            since=at - timedelta(days=21), until=at, limit=40000)
    supplies = store.history(current.asset, "circulating_supply", venue="coinmarketcap",
                             since=at - timedelta(days=22), until=at, limit=200)
    supplies = [row for row in supplies if row.value > 0
                and row.provenance.instrument == current_supply.provenance.instrument
                and row.provenance.source_timestamp is not None
                and not _UNSAFE.intersection(row.quality_flags)]
    if not supplies:
        return None
    supply_times = [row.as_of for row in supplies]
    ratios: list[tuple[datetime, Decimal]] = []
    for row in oi_rows:
        if row.provenance.instrument != current.provenance.instrument or row.unit != "tokens":
            continue
        if _UNSAFE.intersection(row.quality_flags):
            continue
        index = bisect_right(supply_times, row.as_of) - 1
        if index < 0 or row.as_of - supplies[index].as_of > timedelta(days=2):
            continue
        ratios.append((row.as_of, row.value / 2 / supplies[index].value * 100))
    if len(ratios) < 100 or ratios[0][0] > at - timedelta(days=20):
        return None
    current_ratio = current.value / 2 / current_supply.value * 100
    return _percentile(current_ratio, [value for _, value in ratios])


def _funding_percentile(store: NormalizedSqliteStore,
                        current: NormalizedSnapshot | None,
                        interval: NormalizedSnapshot | None,
                        at: datetime) -> Decimal | None:
    if current is None or interval is None or interval.value <= 0:
        return None
    intervals = store.history(current.asset, "funding_interval_hours",
                              venue=current.provenance.venue,
                              since=at - timedelta(days=7), until=at, limit=1000)
    intervals = [row for row in intervals
                 if row.provenance.instrument == current.provenance.instrument and row.value > 0]
    if (not intervals or intervals[0].as_of > at - timedelta(days=6, hours=12)
            or any(row.value != interval.value for row in intervals)):
        return None
    rows = store.history(current.asset, "funding_rate", venue=current.provenance.venue,
                         since=at - timedelta(days=7), until=at, limit=5000)
    rows = [row for row in rows if row.provenance.instrument == current.provenance.instrument
            and row.unit == "decimal" and not _UNSAFE.intersection(row.quality_flags)]
    if len(rows) < 48 or rows[0].as_of > at - timedelta(days=6, hours=12):
        return None
    return _percentile(current.value * 8 / interval.value,
                       [row.value * 8 / interval.value for row in rows])


def _recent_supply(store: NormalizedSqliteStore, asset: str, metric: str,
                   at: datetime) -> NormalizedSnapshot | None:
    sample = store.get_as_of(asset, metric, at, venue="coinmarketcap")
    return (sample if sample is not None and sample.provenance.source_timestamp is not None
            and _usable(sample, at, timedelta(days=2)) else None)


def _unlock_context(asset: str, at: datetime, scan: Mapping[str, Any] | None,
                    cmc_id: int | None, circulating: Decimal | None
                    ) -> tuple[dict[str, Any], Decimal | None, Decimal | None]:
    unknown = {"status": "unknown", "next_at": None, "days_until": None,
               "pct_circulating": None, "amount_tokens": None,
               "source": None, "attribution_text": None, "attribution_url": None}
    if not scan:
        return unknown, None, None
    try:
        received = datetime.fromisoformat(str(scan["received_at"]))
    except (ValueError, KeyError):
        return unknown, None, None
    if not 0 <= (at - received).total_seconds() <= 36 * 3600:
        return unknown, None, None
    events: list[tuple[date, Mapping[str, Any]]] = []
    for event in scan.get("unlock_events", []):
        if not isinstance(event, Mapping) or event.get("asset") != asset:
            continue
        if cmc_id is None or event.get("provider_id") != cmc_id:
            continue
        if "stale_unlock_calendar" in event.get("quality_flags", ()):
            continue
        try:
            day = date.fromisoformat(str(event["unlock_date"]))
        except (KeyError, ValueError):
            continue
        if day >= at.date():
            events.append((day, event))
    if not events:
        return unknown, None, None
    day, event = min(events, key=lambda pair: pair[0])
    try:
        amount = Decimal(str(event["amount_tokens"]))
        pct = Decimal(str(event["pct_circulating"])) if event.get("pct_circulating") is not None else None
    except (ValueError, ArithmeticError, KeyError):
        return unknown, None, None
    days = Decimal((day - at.date()).days)
    verified_pct = amount / circulating * 100 if circulating is not None and circulating > 0 else None
    flags = list(event.get("quality_flags", []))
    if verified_pct is not None and pct is not None and abs(verified_pct - pct) > max(
            Decimal("0.25"), max(verified_pct, pct) * Decimal("0.25")):
        flags.append("unlock_float_source_disagreement")
        verified_pct = None
    if days == 0:
        flags.append("date_only_today_time_unknown")
    return {
        "status": "event" if verified_pct is not None and days > 0 else "unknown",
        "next_at": day.isoformat(), "days_until": int(days),
        "pct_circulating": str(pct) if pct is not None else None,
        "verified_pct_circulating": str(verified_pct) if verified_pct is not None else None,
        "amount_tokens": str(amount), "source": event.get("source_url"),
        "attribution_text": event.get("attribution_text"),
        "attribution_url": event.get("attribution_url"),
        "basis": event.get("basis", []), "quality_flags": flags,
    }, days if verified_pct is not None and days > 0 else None, (
        verified_pct if verified_pct is not None and days > 0 else None)


def enrich_candidate(
    store: NormalizedSqliteStore, at: datetime, candidate: dict[str, Any],
    lead: Mapping[str, Any], signals: list[Mapping[str, Any]],
    scan: Mapping[str, Any] | None,
) -> None:
    """Add four score tracks without promoting unknown evidence into a claim."""
    asset = candidate["asset"]
    price = lead.get("price_snapshot")
    oi = lead.get("oi_snapshot")
    funding = lead.get("funding_snapshot")
    premium = lead.get("premium_snapshot")
    assert isinstance(price, NormalizedSnapshot) and isinstance(oi, NormalizedSnapshot)
    price_moves = {hours: _change(store, price, hours) for hours in (1, 4, 24)}
    oi_moves = {hours: _change(store, oi, hours) for hours in (1, 4, 24)}
    circ = _recent_supply(store, asset, "circulating_supply", at)
    max_supply = _recent_supply(store, asset, "max_supply", at)
    fdv = _recent_supply(store, asset, "fdv_usd", at)
    cmc_price = _recent_supply(store, asset, "cmc_price", at)
    cap = _recent_supply(store, asset, "market_cap_usd", at)
    cmc_id: int | None = None
    if circ is not None and circ.provenance.instrument.startswith("CMC:"):
        try:
            cmc_id = int(circ.provenance.instrument[4:])
        except ValueError:
            pass
    identity = (scan or {}).get("identity_status", {}).get(asset, {})
    try:
        scan_age = (at - datetime.fromisoformat(str(scan["received_at"]))).total_seconds() if scan else None
    except (ValueError, KeyError):
        scan_age = None
    identity_ok = (isinstance(identity, Mapping)
                   and identity.get("status") in {"matched", "override"}
                   and identity.get("provider_id") == cmc_id
                   and scan_age is not None and 0 <= scan_age <= 36 * 3600)
    quote_group = circ.provenance.raw_id if circ is not None else None
    def same_quote(row: NormalizedSnapshot | None) -> NormalizedSnapshot | None:
        return (row if row is not None and circ is not None and quote_group is not None
                and row.provenance.instrument == circ.provenance.instrument
                and row.provenance.raw_id == quote_group else None)
    max_supply, fdv, cmc_price, cap = map(same_quote,
                                          (max_supply, fdv, cmc_price, cap))
    price_aligned = (cmc_price is not None and cmc_price.value > 0
                     and Decimal("0.5") <= price.value / cmc_price.value <= Decimal("2"))
    if not identity_ok or not price_aligned:
        circ = max_supply = fdv = cmc_price = cap = None
        cmc_id = None
    bybit = next((item.get("oi_snapshot") for item in signals if item.get("venue") == "bybit"), None)
    bybit = bybit if isinstance(bybit, NormalizedSnapshot) and _usable(bybit, at, timedelta(minutes=25)) else None
    known_oi_tokens = bybit.value / 2 if bybit is not None else None
    known_oi_usd = bybit.related_value / 2 if bybit is not None and bybit.related_value is not None else None
    oi_float = known_oi_tokens / circ.value * 100 if known_oi_tokens is not None and circ else None
    fdv_supply = (fdv.value / cmc_price.value if fdv and cmc_price and cmc_price.value > 0
                  else max_supply.value if max_supply else None)
    oi_fdv = known_oi_tokens / fdv_supply * 100 if known_oi_tokens is not None and fdv_supply else None
    coverage_label = ("Bybit 单所 OI 已换算为单边；Binance/KuCoin 未纳入跨所总量"
                      if bybit is not None else "交易所 OI 口径未统一，市场总量比例未知")
    candidate["oi_structure"] = {
        "observed_oi_usd": str(known_oi_usd) if known_oi_usd is not None else None,
        "oi_to_float_pct": str(oi_float) if oi_float is not None else None,
        "oi_to_fdv_pct": str(oi_fdv) if oi_fdv is not None else None,
        "coverage_label": coverage_label,
        "trend_1h_pct": str(oi_moves[1]) if oi_moves[1] is not None else None,
        "trend_4h_pct": str(oi_moves[4]) if oi_moves[4] is not None else None,
        "trend_24h_pct": str(oi_moves[24]) if oi_moves[24] is not None else None,
        "denominator_basis": "Bybit OI tokens/2 ÷ CMC circulating supply and FDV/CMC price",
    }
    candidate["tokenomics"] = {
        "circulating_supply": str(circ.value) if circ else None,
        "max_supply": str(max_supply.value) if max_supply else None,
        "fdv_usd": str(fdv.value) if fdv else None,
        "market_cap_usd": str(cap.value) if cap else None,
        "source": "CoinMarketCap keyless" if circ or max_supply or fdv or cap else None,
        "as_of": max((row.as_of for row in (circ, max_supply, fdv, cap, cmc_price) if row), default=None),
    }
    if candidate["tokenomics"]["as_of"] is not None:
        candidate["tokenomics"]["as_of"] = candidate["tokenomics"]["as_of"].isoformat()
    unlock, unlock_days, unlock_pct = _unlock_context(
        asset, at, scan, cmc_id, circ.value if circ else None)
    candidate["unlock"] = unlock

    interval = store.get_as_of(asset, "funding_interval_hours", at,
                               venue=lead["venue"])
    interval = interval if (_usable(interval, at, timedelta(hours=2))
                            and interval.provenance.instrument == lead["instrument"]
                            and interval.unit == "hours" and 1 <= interval.value <= 24) else None
    funding = funding if (isinstance(funding, NormalizedSnapshot)
                          and funding.unit == "decimal"
                          and _usable(funding, at, timedelta(minutes=25))) else None
    premium = premium if (isinstance(premium, NormalizedSnapshot)
                          and premium.unit == "bps"
                          and _usable(premium, at, timedelta(minutes=25))) else None
    # Express an older funding rate in the current interval's units before
    # passing the bps change to score_structure. Its own 8h conversion then
    # gives exactly the difference between two 8h-equivalent rates.
    funding_deltas: dict[int, Decimal | None] = {1: None, 4: None}
    if funding is not None and interval is not None:
        for hours in (1, 4):
            older = _baseline(store, funding, hours)
            if older is None or older.unit != "decimal":
                continue
            older_interval = store.get_as_of(asset, "funding_interval_hours", older.as_of,
                                             venue=lead["venue"])
            if (older_interval is None or not _usable(older_interval, older.as_of, timedelta(hours=2))
                    or older_interval.provenance.instrument != lead["instrument"]
                    or older_interval.unit != "hours" or not 1 <= older_interval.value <= 24):
                continue
            funding_deltas[hours] = (
                funding.value - older.value * interval.value / older_interval.value
            ) * Decimal(10000)
    funding_bps = funding.value * 10000 if funding else None
    candidate["funding_rate"] = str(funding.value) if funding else None
    candidate["premium_bps"] = str(premium.value) if premium else None
    carry: dict[str, Any] = {"rate_8h_pct": None, "long_annualized_pct": None,
                             "short_annualized_pct": None, "interval_hours": None,
                             "rate_per_interval_pct": str(funding.value * 100) if funding else None,
                             "venue": lead["venue"]}
    if funding is not None and interval is not None:
        rate_8h_pct = funding.value * 100 * 8 / interval.value
        annualized_short = funding.value * 100 * 24 * 365 / interval.value
        carry.update({"rate_8h_pct": str(rate_8h_pct),
                      "long_annualized_pct": str(-annualized_short),
                      "short_annualized_pct": str(annualized_short),
                      "interval_hours": str(interval.value)})
    candidate["funding_carry"] = carry

    confirmations = len({item["venue"] for item in signals
                         if item["pattern"] == lead["pattern"] and lead["pattern"] != "watch"})
    structural = score_structure(
        price_change_1h_pct=price_moves[1], price_change_4h_pct=price_moves[4],
        price_change_24h_pct=price_moves[24],
        price_percentile_21d=_price_percentile(store, price, at),
        oi_change_1h_pct=oi_moves[1], oi_change_4h_pct=oi_moves[4],
        oi_change_24h_pct=oi_moves[24], oi_usd=known_oi_usd,
        oi_float_pct=oi_float, oi_fdv_pct=oi_fdv,
        oi_float_percentile_21d=_oi_float_percentile(store, bybit, circ, at),
        funding_bps_per_interval=funding_bps,
        funding_change_1h_bps=funding_deltas[1],
        funding_change_4h_bps=funding_deltas[4],
        funding_percentile_7d=_funding_percentile(store, funding, interval, at),
        funding_interval_hours=interval.value if interval else None,
        premium_bps=premium.value if premium else None,
        unlock_days=unlock_days, unlock_float_pct=unlock_pct,
        unlock_schedule_status=unlock["status"], confirming_venues=confirmations,
    )
    candidate["structural_scores"] = structural
    candidate["price_change_1h_pct"] = (str(price_moves[1].quantize(Decimal("0.01")))
                                        if price_moves[1] is not None else None)
    candidate["oi_change_1h_pct"] = (str(oi_moves[1].quantize(Decimal("0.01")))
                                     if oi_moves[1] is not None else None)
    early = structural["early_accumulation"]
    short = structural["elevated_short_watch"]
    short_risk = structural["short_risk"]
    if early["score"] is not None and early["score"] >= 75:
        candidate["status"] = "candidate"
        candidate["research_track"] = "early_accumulation"
    elif (short["score"] is not None and short["score"] >= 75
          and short_risk["score"] is not None and short_risk["score"] < 50):
        candidate["status"] = "candidate"
        candidate["research_track"] = "elevated_short_watch"
    elif (max(early["score_range"]["min"], short["score_range"]["min"]) >= 40
          and max(early["coverage_pct"], short["coverage_pct"]) >= 50):
        candidate["status"] = "lead"
        candidate["research_track"] = (
            "early_accumulation" if early["score_range"]["min"] >= short["score_range"]["min"]
            else "elevated_short_watch"
        )
    else:
        candidate["status"] = "watch"
        candidate["research_track"] = None
    candidate["rank_lower_bound"] = max(early["score_range"]["min"],
                                         short["score_range"]["min"])
