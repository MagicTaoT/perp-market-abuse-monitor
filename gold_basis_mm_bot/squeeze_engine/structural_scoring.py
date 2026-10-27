"""Pure, explainable structural research scores for perpetual markets.

Inputs must already refer to the *same verified token* and point in time. In
particular, the caller must reconcile contract multipliers, the two-sided
Bybit OI convention, and venue coverage before supplying aggregate OI. OI
ratios are percentages: 10 means 10% of circulating supply or FDV, not 0.10.
Funding inputs are bps per settlement interval; this module converts them to
an eight-hour equivalent before applying bands. The funding historical
percentile must rank eight-hour-equivalent values within the same instrument's
own history. All funding components require a verified settlement interval.

Each track is out of 100 when all of its inputs are known. Weights:

* early_accumulation: OI size/float/FDV/relative rank 40, OI growth 25,
  negative funding and deterioration 20, restrained price 5, restrained
  premium 5, independent venue confirmation 5.
* elevated_short_watch: elevated price 25, OI size/float/FDV 25, OI build then
  decline 10, positive funding and acceleration 15, unlock pressure 15,
  positive premium 5, venue confirmation 5.
* long_risk: positive funding and acceleration 45, unlock pressure 20,
  elevated price 15, positive premium 10, OI/float 10.
* short_risk: negative funding and acceleration 45, rising price 20,
  OI/float 15, fresh OI growth 10, negative premium 10.

The bands are research priorities, not probabilities, position directions, or
evidence of manipulation. Negative funding raises the cost and squeeze risk
of shorts; it does not by itself establish who built a position. Unknown inputs
never contribute zero: an incomplete track has no rankable ``score`` or tier,
but exposes its observed lower bound, upper bound, and coverage.
"""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP
from typing import Any, Callable


Number = Decimal | str | int | float
_UNLOCK_STATUSES = frozenset({"event", "none_within_30d", "unknown"})


def _number(value: Number | None, name: str, *, minimum: Decimal | None = None,
            maximum: Decimal | None = None) -> Decimal | None:
    if value is None:
        return None
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite number")
    try:
        result = Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite number") from exc
    if not result.is_finite() or (minimum is not None and result < minimum) or (
        maximum is not None and result > maximum
    ):
        raise ValueError(f"{name} is outside its valid range")
    return result


def _at_least(value: Decimal, bands: tuple[tuple[str, int], ...]) -> int:
    return max((points for threshold, points in bands if value >= Decimal(threshold)), default=0)


def _at_most(value: Decimal, bands: tuple[tuple[str, int], ...]) -> int:
    return max((points for threshold, points in bands if value <= Decimal(threshold)), default=0)


def _decimal(values: dict[str, Any], name: str) -> Decimal:
    value = values[name]
    assert isinstance(value, Decimal)
    return value


def _funding_8h(values: dict[str, Any], name: str) -> Decimal:
    """Normalize the rate or change of one settlement interval to eight hours."""
    return _decimal(values, name) * Decimal("8") / _decimal(values, "funding_interval_hours")


def _unlock_points(values: dict[str, Any], maximum: int) -> int:
    if values["unlock_schedule_status"] == "none_within_30d":
        return 0
    days = _decimal(values, "unlock_days")
    float_pct = _decimal(values, "unlock_float_pct")
    size = _at_least(float_pct, (("1", 1), ("3", 2), ("7", 3), ("15", 4)))
    urgency = _at_most(days, (("30", 1), ("14", 2), ("7", 3), ("2", 4)))
    return int((Decimal(maximum * size * urgency) / Decimal(16)).quantize(
        Decimal("1"), rounding=ROUND_HALF_UP
    ))


def _early_price(values: dict[str, Any]) -> int:
    change = _decimal(values, "price_change_1h_pct")
    if Decimal("-3") <= change <= Decimal("5"):
        return 5
    if Decimal("-8") <= change <= Decimal("10"):
        return 3
    return 0


def _early_premium(values: dict[str, Any]) -> int:
    absolute = abs(_decimal(values, "premium_bps"))
    if absolute <= 10:
        return 5
    if absolute <= 25:
        return 3
    if absolute <= 50:
        return 1
    return 0


def _venue_points(values: dict[str, Any]) -> int:
    count = values["confirming_venues"]
    assert isinstance(count, int)
    return 5 if count >= 3 else 3 if count == 2 else 1 if count == 1 else 0


@dataclass(frozen=True, slots=True)
class _Component:
    name: str
    maximum: int
    required: tuple[str, ...]
    scorer: Callable[[dict[str, Any]], int]


def _spec(name: str, maximum: int, required: tuple[str, ...],
          scorer: Callable[[dict[str, Any]], int]) -> _Component:
    return _Component(name, maximum, required, scorer)


_EARLY = (
    _spec("oi_float_pressure", 18, ("oi_float_pct",),
          lambda v: _at_least(_decimal(v, "oi_float_pct"),
                              (("1", 3), ("3", 6), ("7", 10), ("15", 14), ("30", 18)))),
    _spec("oi_fdv_pressure", 7, ("oi_fdv_pct",),
          lambda v: _at_least(_decimal(v, "oi_fdv_pct"),
                              (("0.25", 2), ("0.5", 3), ("1", 5), ("3", 7)))),
    _spec("oi_absolute", 10, ("oi_usd",),
          lambda v: _at_least(_decimal(v, "oi_usd"),
                              (("1000000", 2), ("5000000", 5),
                               ("20000000", 8), ("100000000", 10)))),
    _spec("oi_relative_rank", 5, ("oi_float_percentile_21d",),
          lambda v: _at_least(_decimal(v, "oi_float_percentile_21d"),
                              (("70", 2), ("90", 4), ("97", 5)))),
    _spec("oi_growth_1h", 10, ("oi_change_1h_pct",),
          lambda v: _at_least(_decimal(v, "oi_change_1h_pct"),
                              (("2", 3), ("5", 6), ("10", 8), ("20", 10)))),
    _spec("oi_growth_4h", 8, ("oi_change_4h_pct",),
          lambda v: _at_least(_decimal(v, "oi_change_4h_pct"),
                              (("5", 2), ("10", 4), ("20", 6), ("40", 8)))),
    _spec("oi_growth_24h", 7, ("oi_change_24h_pct",),
          lambda v: _at_least(_decimal(v, "oi_change_24h_pct"),
                              (("10", 2), ("20", 4), ("40", 6), ("80", 7)))),
    _spec("negative_funding", 8, ("funding_bps_per_interval", "funding_interval_hours"),
          lambda v: _at_most(_funding_8h(v, "funding_bps_per_interval"),
                             (("-5", 2), ("-10", 4), ("-25", 6), ("-50", 8)))),
    _spec("funding_fall_1h", 4, ("funding_change_1h_bps", "funding_interval_hours"),
          lambda v: _at_most(_funding_8h(v, "funding_change_1h_bps"),
                             (("-3", 1), ("-8", 3), ("-20", 4)))),
    _spec("funding_fall_4h", 4, ("funding_change_4h_bps", "funding_interval_hours"),
          lambda v: _at_most(_funding_8h(v, "funding_change_4h_bps"),
                             (("-5", 1), ("-15", 3), ("-30", 4)))),
    _spec("funding_low_rank", 4, ("funding_percentile_7d", "funding_interval_hours"),
          lambda v: _at_most(_decimal(v, "funding_percentile_7d"),
                             (("30", 1), ("10", 3), ("3", 4)))),
    _spec("restrained_price", 5, ("price_change_1h_pct",), _early_price),
    _spec("restrained_premium", 5, ("premium_bps",), _early_premium),
    _spec("venue_confirmation", 5, ("confirming_venues",), _venue_points),
)

_SHORT_WATCH = (
    _spec("elevated_price_rank", 15, ("price_percentile_21d",),
          lambda v: _at_least(_decimal(v, "price_percentile_21d"),
                              (("70", 3), ("85", 7), ("95", 12), ("99", 15)))),
    _spec("price_rise_4h", 6, ("price_change_4h_pct",),
          lambda v: _at_least(_decimal(v, "price_change_4h_pct"),
                              (("5", 2), ("10", 4), ("20", 6)))),
    _spec("price_rise_24h", 4, ("price_change_24h_pct",),
          lambda v: _at_least(_decimal(v, "price_change_24h_pct"),
                              (("10", 1), ("25", 2), ("50", 3), ("100", 4)))),
    _spec("oi_float_pressure", 13, ("oi_float_pct",),
          lambda v: _at_least(_decimal(v, "oi_float_pct"),
                              (("3", 2), ("7", 5), ("15", 9), ("30", 13)))),
    _spec("oi_fdv_pressure", 7, ("oi_fdv_pct",),
          lambda v: _at_least(_decimal(v, "oi_fdv_pct"),
                              (("0.5", 2), ("1", 4), ("3", 7)))),
    _spec("oi_absolute", 5, ("oi_usd",),
          lambda v: _at_least(_decimal(v, "oi_usd"),
                              (("5000000", 2), ("20000000", 4), ("100000000", 5)))),
    _spec("recent_oi_decline", 4, ("oi_change_1h_pct",),
          lambda v: _at_most(_decimal(v, "oi_change_1h_pct"),
                             (("-2", 1), ("-5", 3), ("-10", 4)))),
    _spec("oi_growth_4h", 3, ("oi_change_4h_pct",),
          lambda v: _at_least(_decimal(v, "oi_change_4h_pct"),
                              (("5", 1), ("10", 2), ("20", 3)))),
    _spec("oi_growth_24h", 3, ("oi_change_24h_pct",),
          lambda v: _at_least(_decimal(v, "oi_change_24h_pct"),
                              (("10", 1), ("20", 2), ("50", 3)))),
    _spec("positive_funding", 6, ("funding_bps_per_interval", "funding_interval_hours"),
          lambda v: _at_least(_funding_8h(v, "funding_bps_per_interval"),
                              (("5", 2), ("10", 3), ("25", 5), ("50", 6)))),
    _spec("funding_rise_1h", 3, ("funding_change_1h_bps", "funding_interval_hours"),
          lambda v: _at_least(_funding_8h(v, "funding_change_1h_bps"),
                              (("3", 1), ("8", 2), ("20", 3)))),
    _spec("funding_rise_4h", 3, ("funding_change_4h_bps", "funding_interval_hours"),
          lambda v: _at_least(_funding_8h(v, "funding_change_4h_bps"),
                              (("5", 1), ("15", 2), ("30", 3)))),
    _spec("funding_high_rank", 3, ("funding_percentile_7d", "funding_interval_hours"),
          lambda v: _at_least(_decimal(v, "funding_percentile_7d"),
                              (("70", 1), ("90", 2), ("97", 3)))),
    _spec("upcoming_unlock", 15, ("unlock_schedule_status",),
          lambda v: _unlock_points(v, 15)),
    _spec("positive_premium", 5, ("premium_bps",),
          lambda v: _at_least(_decimal(v, "premium_bps"),
                              (("10", 2), ("25", 4), ("50", 5)))),
    _spec("venue_confirmation", 5, ("confirming_venues",), _venue_points),
)

_LONG_RISK = (
    _spec("positive_funding_cost", 20, ("funding_bps_per_interval", "funding_interval_hours"),
          lambda v: _at_least(_funding_8h(v, "funding_bps_per_interval"),
                              (("5", 5), ("10", 10), ("25", 15), ("50", 20)))),
    _spec("funding_rise_1h", 10, ("funding_change_1h_bps", "funding_interval_hours"),
          lambda v: _at_least(_funding_8h(v, "funding_change_1h_bps"),
                              (("3", 3), ("8", 6), ("20", 10)))),
    _spec("funding_rise_4h", 10, ("funding_change_4h_bps", "funding_interval_hours"),
          lambda v: _at_least(_funding_8h(v, "funding_change_4h_bps"),
                              (("5", 3), ("15", 6), ("30", 10)))),
    _spec("funding_high_rank", 5, ("funding_percentile_7d", "funding_interval_hours"),
          lambda v: _at_least(_decimal(v, "funding_percentile_7d"),
                              (("70", 1), ("90", 3), ("97", 5)))),
    _spec("upcoming_unlock", 20, ("unlock_schedule_status",),
          lambda v: _unlock_points(v, 20)),
    _spec("elevated_price_rank", 10, ("price_percentile_21d",),
          lambda v: _at_least(_decimal(v, "price_percentile_21d"),
                              (("70", 2), ("85", 5), ("95", 8), ("99", 10)))),
    _spec("price_rise_24h", 5, ("price_change_24h_pct",),
          lambda v: _at_least(_decimal(v, "price_change_24h_pct"),
                              (("10", 1), ("25", 3), ("50", 5)))),
    _spec("positive_premium", 10, ("premium_bps",),
          lambda v: _at_least(_decimal(v, "premium_bps"),
                              (("10", 3), ("25", 6), ("50", 10)))),
    _spec("oi_float_pressure", 10, ("oi_float_pct",),
          lambda v: _at_least(_decimal(v, "oi_float_pct"),
                              (("3", 2), ("7", 4), ("15", 7), ("30", 10)))),
)

_SHORT_RISK = (
    _spec("negative_funding_cost", 20, ("funding_bps_per_interval", "funding_interval_hours"),
          lambda v: _at_most(_funding_8h(v, "funding_bps_per_interval"),
                             (("-5", 5), ("-10", 10), ("-25", 15), ("-50", 20)))),
    _spec("funding_fall_1h", 10, ("funding_change_1h_bps", "funding_interval_hours"),
          lambda v: _at_most(_funding_8h(v, "funding_change_1h_bps"),
                             (("-3", 3), ("-8", 6), ("-20", 10)))),
    _spec("funding_fall_4h", 10, ("funding_change_4h_bps", "funding_interval_hours"),
          lambda v: _at_most(_funding_8h(v, "funding_change_4h_bps"),
                             (("-5", 3), ("-15", 6), ("-30", 10)))),
    _spec("funding_low_rank", 5, ("funding_percentile_7d", "funding_interval_hours"),
          lambda v: _at_most(_decimal(v, "funding_percentile_7d"),
                             (("30", 1), ("10", 3), ("3", 5)))),
    _spec("price_rise_1h", 10, ("price_change_1h_pct",),
          lambda v: _at_least(_decimal(v, "price_change_1h_pct"),
                              (("2", 2), ("5", 5), ("10", 8), ("20", 10)))),
    _spec("price_rise_4h", 10, ("price_change_4h_pct",),
          lambda v: _at_least(_decimal(v, "price_change_4h_pct"),
                              (("5", 2), ("10", 5), ("20", 8), ("40", 10)))),
    _spec("oi_float_pressure", 15, ("oi_float_pct",),
          lambda v: _at_least(_decimal(v, "oi_float_pct"),
                              (("3", 3), ("7", 6), ("15", 10), ("30", 15)))),
    _spec("fresh_oi_growth", 10, ("oi_change_1h_pct",),
          lambda v: _at_least(_decimal(v, "oi_change_1h_pct"),
                              (("2", 2), ("5", 5), ("10", 8), ("20", 10)))),
    _spec("negative_premium", 10, ("premium_bps",),
          lambda v: _at_most(_decimal(v, "premium_bps"),
                             (("-10", 3), ("-25", 6), ("-50", 10)))),
)


def _track(specs: tuple[_Component, ...], values: dict[str, Any], *, risk: bool) -> dict[str, Any]:
    assert sum(spec.maximum for spec in specs) == 100
    rows: list[dict[str, Any]] = []
    known_max = 0
    observed = 0
    unknown: set[str] = set()
    for spec in specs:
        needed = list(spec.required)
        if spec.name == "upcoming_unlock" and values["unlock_schedule_status"] == "event":
            needed.extend(("unlock_days", "unlock_float_pct"))
        missing = [name for name in needed if values.get(name) is None or (
            name == "unlock_schedule_status" and values[name] == "unknown"
        )]
        if missing:
            unknown.update(missing)
            rows.append({"name": spec.name, "points": None, "max_points": spec.maximum,
                         "status": "unknown", "missing_inputs": missing})
            continue
        points = spec.scorer(values)
        if not 0 <= points <= spec.maximum:
            raise AssertionError(f"invalid score contribution: {spec.name}")
        known_max += spec.maximum
        observed += points
        rows.append({"name": spec.name, "points": points, "max_points": spec.maximum,
                     "status": "known", "missing_inputs": []})
    if known_max == 100:
        tier = ("severe" if observed >= 75 else "high" if observed >= 50
                else "moderate" if observed >= 25 else "low") if risk else (
            "high" if observed >= 75 else "elevated" if observed >= 50
            else "watch" if observed >= 25 else "low"
        )
        score: int | None = observed
        status = "complete"
    else:
        score = None
        status = "partial" if known_max else "unknown"
        tier = status
    return {
        "score": score,
        "observed_score": observed if known_max else None,
        "score_range": {"min": observed, "max": observed + (100 - known_max)},
        "tier": tier,
        "status": status,
        "coverage_pct": known_max,
        "unknown_inputs": sorted(unknown),
        "contributions": rows,
    }


def score_structure(
    *,
    price_change_1h_pct: Number | None = None,
    price_change_4h_pct: Number | None = None,
    price_change_24h_pct: Number | None = None,
    price_percentile_21d: Number | None = None,
    oi_change_1h_pct: Number | None = None,
    oi_change_4h_pct: Number | None = None,
    oi_change_24h_pct: Number | None = None,
    oi_usd: Number | None = None,
    oi_float_pct: Number | None = None,
    oi_fdv_pct: Number | None = None,
    oi_float_percentile_21d: Number | None = None,
    funding_bps_per_interval: Number | None = None,
    funding_change_1h_bps: Number | None = None,
    funding_change_4h_bps: Number | None = None,
    funding_percentile_7d: Number | None = None,
    funding_interval_hours: Number | None = None,
    premium_bps: Number | None = None,
    unlock_days: Number | None = None,
    unlock_float_pct: Number | None = None,
    unlock_schedule_status: str = "unknown",
    confirming_venues: int | None = None,
) -> dict[str, Any]:
    """Score four distinct research dimensions from point-in-time verified data.

    ``unlock_schedule_status='none_within_30d'`` means an authoritative,
    recently checked schedule confirms no event within 30 days. Omitted data
    must use ``'unknown'``; an upcoming event uses ``'event'`` plus its days
    and unlocked amount as a percentage of current circulating supply.

    Higher opportunity scores mean *investigate first*. Higher risk scores mean
    greater directional holding risk. Neither asserts causation or manipulation.
    """
    if unlock_schedule_status not in _UNLOCK_STATUSES:
        raise ValueError("unlock_schedule_status must be event, none_within_30d, or unknown")
    if confirming_venues is not None and (
        isinstance(confirming_venues, bool) or not isinstance(confirming_venues, int)
        or confirming_venues < 0
    ):
        raise ValueError("confirming_venues must be a nonnegative integer")
    values: dict[str, Any] = {
        "price_change_1h_pct": _number(price_change_1h_pct, "price_change_1h_pct", minimum=Decimal("-100")),
        "price_change_4h_pct": _number(price_change_4h_pct, "price_change_4h_pct", minimum=Decimal("-100")),
        "price_change_24h_pct": _number(price_change_24h_pct, "price_change_24h_pct", minimum=Decimal("-100")),
        "price_percentile_21d": _number(price_percentile_21d, "price_percentile_21d", minimum=Decimal(0), maximum=Decimal(100)),
        "oi_change_1h_pct": _number(oi_change_1h_pct, "oi_change_1h_pct", minimum=Decimal("-100")),
        "oi_change_4h_pct": _number(oi_change_4h_pct, "oi_change_4h_pct", minimum=Decimal("-100")),
        "oi_change_24h_pct": _number(oi_change_24h_pct, "oi_change_24h_pct", minimum=Decimal("-100")),
        "oi_usd": _number(oi_usd, "oi_usd", minimum=Decimal(0)),
        "oi_float_pct": _number(oi_float_pct, "oi_float_pct", minimum=Decimal(0)),
        "oi_fdv_pct": _number(oi_fdv_pct, "oi_fdv_pct", minimum=Decimal(0)),
        "oi_float_percentile_21d": _number(oi_float_percentile_21d, "oi_float_percentile_21d", minimum=Decimal(0), maximum=Decimal(100)),
        "funding_bps_per_interval": _number(funding_bps_per_interval, "funding_bps_per_interval"),
        "funding_change_1h_bps": _number(funding_change_1h_bps, "funding_change_1h_bps"),
        "funding_change_4h_bps": _number(funding_change_4h_bps, "funding_change_4h_bps"),
        "funding_percentile_7d": _number(funding_percentile_7d, "funding_percentile_7d", minimum=Decimal(0), maximum=Decimal(100)),
        "funding_interval_hours": _number(funding_interval_hours, "funding_interval_hours", minimum=Decimal("0.000001"), maximum=Decimal(24)),
        "premium_bps": _number(premium_bps, "premium_bps"),
        "unlock_days": _number(unlock_days, "unlock_days", minimum=Decimal(0)),
        "unlock_float_pct": _number(unlock_float_pct, "unlock_float_pct", minimum=Decimal(0)),
        "unlock_schedule_status": unlock_schedule_status,
        "confirming_venues": confirming_venues,
    }
    if unlock_schedule_status == "none_within_30d" and (
        values["unlock_days"] is not None or values["unlock_float_pct"] is not None
    ):
        raise ValueError("none_within_30d cannot include an unlock event")
    tracks = {
        "early_accumulation": _track(_EARLY, values, risk=False),
        "elevated_short_watch": _track(_SHORT_WATCH, values, risk=False),
        "long_risk": _track(_LONG_RISK, values, risk=True),
        "short_risk": _track(_SHORT_RISK, values, risk=True),
    }
    return {
        **tracks,
        "unknown_inputs": sorted({name for track in tracks.values() for name in track["unknown_inputs"]}),
        "manipulation_claim": False,
    }
