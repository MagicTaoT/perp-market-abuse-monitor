"""Small, explicit data contract for independent squeeze observations."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation


def utc_datetime(value: datetime, name: str = "timestamp") -> datetime:
    """Return an aware UTC timestamp; never guess the timezone of a naive value."""
    if not isinstance(value, datetime) or value.tzinfo is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")
    if value.utcoffset() is None:
        raise ValueError(f"{name} must be a timezone-aware datetime")
    return value.astimezone(timezone.utc)


def finite_decimal(value: Decimal | str | int | float, name: str) -> Decimal:
    if isinstance(value, bool):
        raise ValueError(f"{name} must be a finite decimal")
    try:
        result = value if isinstance(value, Decimal) else Decimal(str(value))
    except (InvalidOperation, TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be a finite decimal") from exc
    if not result.is_finite():
        raise ValueError(f"{name} must be a finite decimal")
    return result


@dataclass(frozen=True, slots=True)
class Provenance:
    source: str
    venue: str
    instrument: str
    source_timestamp: datetime | None
    received_at: datetime
    raw_id: str | None = None

    def __post_init__(self) -> None:
        for name in ("source", "venue", "instrument"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a nonempty string")
        if self.source_timestamp is not None:
            object.__setattr__(
                self, "source_timestamp", utc_datetime(self.source_timestamp, "source_timestamp")
            )
        object.__setattr__(self, "received_at", utc_datetime(self.received_at, "received_at"))
        if self.raw_id is not None and not isinstance(self.raw_id, str):
            raise ValueError("raw_id must be a string or None")

    @property
    def as_of(self) -> datetime:
        # Replay must not see an event before it was actually received.  A
        # future-dated source timestamp also cannot become visible early.
        return max(self.received_at, self.source_timestamp or self.received_at)


@dataclass(frozen=True, slots=True)
class NormalizedSnapshot:
    """One normalized metric from one source at one as-of time.

    Open interest uses ``value`` in tokens and ``related_value`` in USD.  This
    pair is kept in one record so a price revaluation cannot masquerade as an
    OI quantity change.
    """

    asset: str
    metric: str
    value: Decimal
    unit: str
    provenance: Provenance
    quality_flags: tuple[str, ...] = ()
    related_value: Decimal | None = None
    related_unit: str | None = None

    def __post_init__(self) -> None:
        for name in ("asset", "metric", "unit"):
            value = getattr(self, name)
            if not isinstance(value, str) or not value.strip():
                raise ValueError(f"{name} must be a nonempty string")
        if not isinstance(self.provenance, Provenance):
            raise ValueError("provenance must be a Provenance")
        object.__setattr__(self, "value", finite_decimal(self.value, "value"))
        if self.related_value is None:
            if self.related_unit is not None:
                raise ValueError("related_unit requires related_value")
        else:
            object.__setattr__(
                self, "related_value", finite_decimal(self.related_value, "related_value")
            )
            if not isinstance(self.related_unit, str) or not self.related_unit.strip():
                raise ValueError("related_value requires related_unit")
        flags = tuple(self.quality_flags)
        if any(not isinstance(flag, str) or not flag.strip() for flag in flags):
            raise ValueError("quality_flags must contain nonempty strings")
        if self.provenance.source_timestamp is None and "missing_source_timestamp" not in flags:
            flags += ("missing_source_timestamp",)
        object.__setattr__(self, "quality_flags", tuple(dict.fromkeys(flags)))
        if self.metric == "open_interest":
            if self.unit != "tokens" or self.related_unit != "USD":
                raise ValueError("open_interest requires tokens and related USD values")
            if self.value < 0 or self.related_value is None or self.related_value < 0:
                raise ValueError("open_interest values cannot be negative")

    @property
    def as_of(self) -> datetime:
        return self.provenance.as_of

    @property
    def quality_status(self) -> str:
        if not self.quality_flags:
            return "ok"
        if any(flag.startswith("stale") for flag in self.quality_flags):
            return "stale"
        return "degraded"
