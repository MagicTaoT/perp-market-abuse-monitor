"""Basic squeeze signals with explicit freshness and revaluation quality."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal

from .models import NormalizedSnapshot, finite_decimal, utc_datetime


@dataclass(frozen=True, slots=True)
class FeatureValue:
    value: Decimal | None
    unit: str
    quality_flags: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class OpenInterestChange:
    tokens_delta: Decimal
    tokens_change_pct: Decimal | None
    usd_delta: Decimal
    usd_change_pct: Decimal | None
    token_flow_usd: Decimal | None
    price_revaluation_usd: Decimal | None
    quality_flags: tuple[str, ...]


def premium_bps(mark_price: Decimal | str | int | float, index_price: Decimal | str | int | float) -> Decimal:
    """Signed mark premium over index, in basis points."""
    mark = finite_decimal(mark_price, "mark_price")
    index = finite_decimal(index_price, "index_price")
    if mark <= 0 or index <= 0:
        raise ValueError("mark and index prices must be positive")
    return (mark / index - Decimal("1")) * Decimal("10000")


def ratio(numerator: Decimal | str | int | float, denominator: Decimal | str | int | float) -> Decimal:
    """Dimensionless nonnegative ratio, e.g. long accounts / short accounts."""
    top = finite_decimal(numerator, "numerator")
    bottom = finite_decimal(denominator, "denominator")
    if top < 0 or bottom <= 0:
        raise ValueError("ratio requires nonnegative numerator and positive denominator")
    return top / bottom


def change_pct(current: Decimal | str | int | float, previous: Decimal | str | int | float) -> Decimal | None:
    """Percentage change; zero baseline has no finite percentage change."""
    now = finite_decimal(current, "current")
    before = finite_decimal(previous, "previous")
    if before == 0:
        return None
    return (now - before) / abs(before) * Decimal("100")


def snapshot_change(
    current: NormalizedSnapshot,
    previous: NormalizedSnapshot,
    *,
    at: datetime | None = None,
    max_age_seconds: float = 300,
) -> FeatureValue:
    """Percentage change for the same metric, asset, venue, and unit."""
    _same_series(current, previous)
    if current.unit != previous.unit:
        raise ValueError("snapshot units must match")
    flags = _quality_flags(current, previous, at, max_age_seconds)
    value = change_pct(current.value, previous.value)
    if value is None:
        flags.append("zero_baseline")
    return FeatureValue(value=value, unit="pct", quality_flags=tuple(dict.fromkeys(flags)))


def open_interest_change(
    current: NormalizedSnapshot,
    previous: NormalizedSnapshot,
    *,
    at: datetime | None = None,
    max_age_seconds: float = 300,
    price_revaluation_tolerance: Decimal | str = Decimal("0.000001"),
) -> OpenInterestChange:
    """Separate OI token movement from USD changes caused by price alone.

    ``stale_oi`` is measured against ``at`` (UTC now by default).  When token
    OI is essentially unchanged while USD OI changes, ``price_revaluation_only``
    warns consumers not to treat the USD move as new positioning.
    """
    _same_series(current, previous)
    if current.metric != "open_interest":
        raise ValueError("open_interest_change requires open_interest snapshots")
    assert current.related_value is not None and previous.related_value is not None
    flags = _quality_flags(current, previous, at, max_age_seconds)
    tolerance = finite_decimal(price_revaluation_tolerance, "price_revaluation_tolerance")
    if tolerance < 0 or tolerance >= 1:
        raise ValueError("price_revaluation_tolerance must be in [0, 1)")

    tokens_delta = current.value - previous.value
    usd_delta = current.related_value - previous.related_value
    tokens_pct = change_pct(current.value, previous.value)
    usd_pct = change_pct(current.related_value, previous.related_value)
    if tokens_pct is None or usd_pct is None:
        flags.append("zero_baseline")

    if previous.value > 0 and current.value > 0:
        previous_price = previous.related_value / previous.value
        current_price = current.related_value / current.value
        price_revaluation = previous.value * (current_price - previous_price)
        token_flow = tokens_delta * current_price
    else:
        price_revaluation = None
        token_flow = None

    token_tolerance = max(Decimal("0.00000001"), previous.value * tolerance)
    usd_tolerance = max(Decimal("0.00000001"), previous.related_value * tolerance)
    if abs(tokens_delta) <= token_tolerance and abs(usd_delta) > usd_tolerance:
        flags.append("price_revaluation_only")
    return OpenInterestChange(
        tokens_delta=tokens_delta,
        tokens_change_pct=tokens_pct,
        usd_delta=usd_delta,
        usd_change_pct=usd_pct,
        token_flow_usd=token_flow,
        price_revaluation_usd=price_revaluation,
        quality_flags=tuple(dict.fromkeys(flags)),
    )


def _same_series(current: NormalizedSnapshot, previous: NormalizedSnapshot) -> None:
    if (
        current.asset != previous.asset
        or current.metric != previous.metric
        or current.provenance.venue != previous.provenance.venue
        or current.provenance.source != previous.provenance.source
        or current.provenance.instrument != previous.provenance.instrument
    ):
        raise ValueError("snapshots must describe the same source series")
    if current.as_of < previous.as_of:
        raise ValueError("current snapshot predates previous snapshot")
    current_event = current.provenance.source_timestamp or current.provenance.received_at
    previous_event = previous.provenance.source_timestamp or previous.provenance.received_at
    if current_event < previous_event:
        raise ValueError("current source event predates previous source event")


def _quality_flags(
    current: NormalizedSnapshot,
    previous: NormalizedSnapshot,
    at: datetime | None,
    max_age_seconds: float,
) -> list[str]:
    if max_age_seconds <= 0:
        raise ValueError("max_age_seconds must be positive")
    reference = utc_datetime(at, "at") if at is not None else datetime.now(timezone.utc)
    flags = list(current.quality_flags)
    flags.extend(f"previous_{flag}" for flag in previous.quality_flags)
    # Availability controls replay visibility, but freshness concerns the
    # exchange observation itself.  A five-minute-delayed receipt is already
    # five minutes stale when it first becomes available.
    event_time = current.provenance.source_timestamp or current.provenance.received_at
    age = (reference - event_time).total_seconds()
    if age > max_age_seconds:
        flags.append("stale_oi" if current.metric == "open_interest" else "stale_snapshot")
    elif age < -1:
        flags.append("future_source_timestamp")
    return flags
