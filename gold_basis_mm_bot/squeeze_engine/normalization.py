"""Strict market data normalization.  No contract multipliers are guessed."""

from __future__ import annotations

from dataclasses import dataclass
from decimal import Decimal

from .models import finite_decimal


class NormalizationError(ValueError):
    """Input units or contract metadata cannot support a safe conversion."""


@dataclass(frozen=True, slots=True)
class OpenInterestNormalized:
    tokens: Decimal
    usd: Decimal


def normalize_open_interest(
    raw_oi: Decimal | str | int | float,
    *,
    unit: str,
    price_usd: Decimal | str | int | float,
    contract_type: str | None = None,
    contract_multiplier: Decimal | str | int | float | None = None,
    multiplier_unit: str | None = None,
) -> OpenInterestNormalized:
    """Convert an explicitly identified OI unit to base tokens and USD.

    ``contracts`` requires verified linear base-token contracts and a positive
    token multiplier.  Inverse, nonlinear, and unknown contract types cannot
    be converted here.  ``tokens`` and ``usd`` already identify their units,
    so contract metadata is unnecessary, but conflicting metadata is rejected.
    The USD price must itself be supplied explicitly by the caller.
    """
    try:
        quantity = finite_decimal(raw_oi, "raw_oi")
        price = finite_decimal(price_usd, "price_usd")
    except ValueError as exc:
        raise NormalizationError(str(exc)) from exc
    if quantity < 0:
        raise NormalizationError("raw_oi cannot be negative")
    if price <= 0:
        raise NormalizationError("price_usd must be positive")
    if not isinstance(unit, str):
        raise NormalizationError("unit must be explicit")
    normalized_unit = unit.strip().lower()
    normalized_type = contract_type.strip().lower() if isinstance(contract_type, str) else None
    if normalized_type is not None and normalized_type != "linear":
        raise NormalizationError(f"unsupported contract_type: {contract_type!r}")

    if normalized_unit == "contracts":
        if normalized_type != "linear":
            raise NormalizationError("contracts require explicit linear contract_type")
        if multiplier_unit not in ("tokens", "base"):
            raise NormalizationError("contracts require an explicit token multiplier_unit")
        if contract_multiplier is None:
            raise NormalizationError("contracts require contract_multiplier")
        try:
            multiplier = finite_decimal(contract_multiplier, "contract_multiplier")
        except ValueError as exc:
            raise NormalizationError(str(exc)) from exc
        if multiplier <= 0:
            raise NormalizationError("contract_multiplier must be positive")
        tokens = quantity * multiplier
        return OpenInterestNormalized(tokens=tokens, usd=tokens * price)

    if contract_multiplier is not None or multiplier_unit is not None:
        raise NormalizationError("contract multiplier is only valid for contract-count OI")
    if normalized_unit in ("tokens", "base"):
        return OpenInterestNormalized(tokens=quantity, usd=quantity * price)
    if normalized_unit == "usd":
        return OpenInterestNormalized(tokens=quantity / price, usd=quantity)
    raise NormalizationError(f"unsupported OI unit: {unit!r}")
