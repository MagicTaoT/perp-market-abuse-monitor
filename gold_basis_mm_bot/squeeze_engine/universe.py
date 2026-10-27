"""Low-request, read-only snapshots of the eligible USDT perpetual universe.

Four bulk market replies are collected per cycle. Binance and Bybit listing
catalogs are refreshed hourly, before market replies, and cached on the HTTP
client used by the long-running collector. A ticker without a verified listing
never becomes a normalized observation. Unusual symbol/base aliases are left
out until an explicit instrument mapping can establish their token unit.
"""

from __future__ import annotations

import threading
import time
from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Any, Mapping
from weakref import WeakKeyDictionary

from .connectors import (
    BINANCE_FUTURES,
    BYBIT,
    KUCOIN_FUTURES,
    CollectionResult,
    MarketDataError,
    PublicExchangeAdapter,
    PublicHttpClient,
    epoch_utc,
)
from .models import finite_decimal
from .normalization import NormalizationError, normalize_open_interest
from .storage import RawJsonlStore


CATALOG_TTL_SECONDS = 3600
MAX_BYBIT_CATALOG_PAGES = 20


@dataclass(slots=True)
class _CatalogCache:
    binance: dict[str, str] | None = None
    binance_expires: float = 0.0
    bybit: dict[str, str] | None = None
    bybit_expires: float = 0.0


_catalogs: WeakKeyDictionary[object, _CatalogCache] = WeakKeyDictionary()
_catalog_lock = threading.Lock()


def _cache_for(client: PublicHttpClient) -> _CatalogCache:
    with _catalog_lock:
        try:
            cache = _catalogs.get(client)
            if cache is None:
                cache = _CatalogCache()
                _catalogs[client] = cache
            return cache
        except TypeError:
            # An unhashable fixture client can still collect safely; it simply
            # cannot retain a catalog between cycles.
            return _CatalogCache()


class _Reader(PublicExchangeAdapter):
    def __init__(self, venue: str, client: PublicHttpClient, raw_store: RawJsonlStore) -> None:
        super().__init__(client, raw_store)
        self.venue = venue

    def fetch(self, base: str, path: str, params: Mapping[str, str],
              result: CollectionResult) -> tuple[Any, datetime, str] | None:
        return self._fetch(base, path, params, result)


def _asset(base: Any, symbol: Any, suffix: str) -> str | None:
    """Accept only identity-preserving symbols, never a guessed 1000x alias."""
    if not isinstance(base, str) or not isinstance(symbol, str):
        return None
    if not 1 <= len(base) <= 24 or not base.isascii() or not base.isupper():
        return None
    if not base[0].isalnum() or not all(c.isalnum() or c in "_-" for c in base):
        return None
    return base if symbol == base + suffix else None


def _positive(value: Any, name: str) -> Decimal:
    number = finite_decimal(value, name)
    if number <= 0:
        raise MarketDataError(f"{name} must be positive")
    return number


def _nonnegative(value: Any, name: str) -> Decimal:
    number = finite_decimal(value, name)
    if number < 0:
        raise MarketDataError(f"{name} must be nonnegative")
    return number


def _append_optional(reader: _Reader, result: CollectionResult, asset: str,
                     symbol: str, source: str, received: datetime, raw_id: str,
                     stamp: datetime | None, row: Mapping[str, Any],
                     metric: str, field: str, unit: str, flags: tuple[str, ...]) -> None:
    if row.get(field) in (None, ""):
        return
    try:
        value = _nonnegative(row[field], field) if metric in {
            "perp_volume_24h", "perp_turnover_24h"
        } else finite_decimal(row[field], field)
        result.snapshots.append(reader._metric(asset, symbol, source, received,
                                                raw_id, stamp, metric, value, unit,
                                                flags=flags))
    except (ValueError, MarketDataError) as exc:
        result.errors.append(f"{reader.venue} {symbol} {field}: {exc}")


def _price_fields(reader: _Reader, result: CollectionResult, asset: str,
                  symbol: str, source: str, received: datetime, raw_id: str,
                  stamp: datetime | None, row: Mapping[str, Any],
                  fields: Mapping[str, str], flags: tuple[str, ...]) -> None:
    reader._price_and_premium(result, asset, symbol, source, received, raw_id,
                              stamp, row, fields, flags)


def _kucoin_eligible(row: Mapping[str, Any]) -> tuple[str, str] | None:
    asset = _asset(row.get("baseCurrency"), row.get("symbol"), "USDTM")
    if asset is None or row.get("status") != "Open":
        return None
    if row.get("quoteCurrency") != "USDT" or row.get("settleCurrency") != "USDT":
        return None
    if row.get("isInverse") is not False:
        return None
    if row.get("expireDate") not in (None, "", 0, "0"):
        return None
    if row.get("settleDate") not in (None, "", 0, "0"):
        return None
    if row.get("marketStage") not in (None, "NORMAL"):
        return None
    if row.get("rootSymbol") not in (None, "USDT"):
        return None
    # assetClass is the current classification; marketType covers older replies.
    classification = row.get("assetClass", row.get("marketType"))
    if classification != "CRYPTO":
        return None
    if row.get("marketType") not in (None, "CRYPTO"):
        return None
    return asset, row["symbol"]


def _binance_catalog(reader: _Reader, result: CollectionResult,
                     cache: _CatalogCache) -> dict[str, str] | None:
    if cache.binance is not None and time.monotonic() < cache.binance_expires:
        return cache.binance
    cache.binance = None
    reply = reader.fetch(BINANCE_FUTURES, "/fapi/v1/exchangeInfo", {}, result)
    if reply is None:
        return None
    payload = reply[0]
    if not isinstance(payload, Mapping) or not isinstance(payload.get("symbols"), list):
        result.errors.append("binance universe catalog: malformed symbols list")
        return None
    eligible: dict[str, str] = {}
    for row in payload["symbols"]:
        if not isinstance(row, Mapping):
            continue
        asset = _asset(row.get("baseAsset"), row.get("symbol"), "USDT")
        if asset is None or row.get("quoteAsset") != "USDT" or row.get("marginAsset") != "USDT":
            continue
        if row.get("contractType") != "PERPETUAL" or row.get("status") != "TRADING":
            continue
        if row.get("underlyingType") != "COIN":
            continue
        eligible[row["symbol"]] = asset
    cache.binance = eligible
    cache.binance_expires = time.monotonic() + CATALOG_TTL_SECONDS
    return eligible


def _bybit_catalog(reader: _Reader, result: CollectionResult,
                   cache: _CatalogCache) -> dict[str, str] | None:
    if cache.bybit is not None and time.monotonic() < cache.bybit_expires:
        return cache.bybit
    cache.bybit = None
    eligible: dict[str, str] = {}
    cursor = ""
    seen_cursors: set[str] = set()
    for _ in range(MAX_BYBIT_CATALOG_PAGES):
        params = {"category": "linear", "limit": "1000"}
        if cursor:
            params["cursor"] = cursor
        reply = reader.fetch(BYBIT, "/v5/market/instruments-info", params, result)
        if reply is None:
            return None
        payload = reply[0]
        if not isinstance(payload, Mapping) or payload.get("retCode") != 0:
            result.errors.append("bybit universe catalog: invalid response code")
            return None
        page = payload.get("result")
        if not isinstance(page, Mapping) or page.get("category") != "linear" or not isinstance(page.get("list"), list):
            result.errors.append("bybit universe catalog: malformed page")
            return None
        for row in page["list"]:
            if not isinstance(row, Mapping):
                continue
            asset = _asset(row.get("baseCoin"), row.get("symbol"), "USDT")
            if asset is None or row.get("status") != "Trading" or row.get("contractType") != "LinearPerpetual":
                continue
            if row.get("quoteCoin") != "USDT" or row.get("settleCoin") != "USDT":
                continue
            if row.get("deliveryTime") not in (None, "", 0, "0"):
                continue
            if row.get("isPreListing") is True or row.get("marketRegion") != "":
                continue
            if row.get("underlyingTicker") not in (None, ""):
                continue
            eligible[row["symbol"]] = asset
            # The instrument catalog reports the settlement period in minutes.
            # Keep it with the same point-in-time provenance as the catalog.
            try:
                interval_minutes = _positive(row.get("fundingInterval"), "fundingInterval")
                interval_hours = interval_minutes / Decimal(60)
                if 1 <= interval_hours <= 24:
                    result.snapshots.append(reader._metric(
                        asset, row["symbol"],
                        "bybit:/v5/market/instruments-info?category=linear",
                        reply[1], reply[2], None, "funding_interval_hours",
                        interval_hours, "hours", flags=("response_time_only", "bulk_scan"),
                    ))
            except (ValueError, TypeError, MarketDataError):
                pass
        next_cursor = page.get("nextPageCursor", "")
        if not isinstance(next_cursor, str):
            result.errors.append("bybit universe catalog: invalid cursor")
            return None
        if not next_cursor:
            cache.bybit = eligible
            cache.bybit_expires = time.monotonic() + CATALOG_TTL_SECONDS
            return eligible
        if next_cursor in seen_cursors:
            result.errors.append("bybit universe catalog: repeated cursor")
            return None
        seen_cursors.add(next_cursor)
        cursor = next_cursor
    result.errors.append("bybit universe catalog: pagination limit exceeded")
    return None


def _parse_kucoin(reader: _Reader, result: CollectionResult,
                  reply: tuple[Any, datetime, str] | None) -> None:
    if reply is None:
        return
    payload, received, raw_id = reply
    if not isinstance(payload, Mapping) or payload.get("code") != "200000" or not isinstance(payload.get("data"), list):
        result.errors.append("kucoin universe: malformed active contracts response")
        return
    source = "kucoin:/api/v1/contracts/active"
    flags = ("usdt_quoted_usd_proxy", "bulk_scan")
    for row in payload["data"]:
        if not isinstance(row, Mapping):
            continue
        identity = _kucoin_eligible(row)
        if identity is None:
            continue
        asset, symbol = identity
        try:
            stamp = epoch_utc(row.get("ts") or row.get("time"))
        except MarketDataError as exc:
            result.errors.append(f"kucoin {symbol} timestamp: {exc}")
            continue
        _price_fields(reader, result, asset, symbol, source, received, raw_id,
                      stamp, row, {"perp_last": "lastTradePrice", "mark_price": "markPrice",
                                   "index_price": "indexPrice"}, flags)
        if row.get("openInterest") not in (None, "") and row.get("markPrice") not in (None, ""):
            try:
                mark = _positive(row["markPrice"], "markPrice")
                oi = normalize_open_interest(row["openInterest"], unit="contracts",
                    price_usd=mark, contract_type="linear", contract_multiplier=row.get("multiplier"),
                    multiplier_unit="tokens")
                result.snapshots.append(reader._metric(asset, symbol, source, received,
                    raw_id, stamp, "open_interest", oi.tokens, "tokens",
                    flags=flags + ("oi_unit_inferred_contract_count",),
                    related_value=oi.usd, related_unit="USD"))
            except (ValueError, NormalizationError) as exc:
                result.errors.append(f"kucoin {symbol} OI: {exc}")
        _append_optional(reader, result, asset, symbol, source, received, raw_id,
                         stamp, row, "funding_rate", "fundingFeeRate", "decimal", flags)
        granularity = row.get("currentFundingRateGranularity") or row.get("fundingRateGranularity")
        if granularity not in (None, ""):
            try:
                interval_hours = _positive(granularity, "fundingRateGranularity") / Decimal(3600000)
                if 1 <= interval_hours <= 24:
                    result.snapshots.append(reader._metric(
                        asset, symbol, source, received, raw_id, stamp,
                        "funding_interval_hours", interval_hours, "hours", flags=flags,
                    ))
            except (ValueError, TypeError, MarketDataError):
                pass
        _append_optional(reader, result, asset, symbol, source, received, raw_id,
                         stamp, row, "perp_volume_24h", "volumeOf24h", "tokens",
                         flags + ("volume_unit_inferred",))
        _append_optional(reader, result, asset, symbol, source, received, raw_id,
                         stamp, row, "perp_turnover_24h", "turnoverOf24h", "USD",
                         flags + ("turnover_quote_unit_inferred",))


def _parse_bybit(reader: _Reader, result: CollectionResult,
                 reply: tuple[Any, datetime, str] | None,
                 catalog: dict[str, str] | None) -> None:
    if reply is None or catalog is None:
        return
    payload, received, raw_id = reply
    if not isinstance(payload, Mapping) or payload.get("retCode") != 0:
        result.errors.append("bybit universe: invalid ticker response code")
        return
    data = payload.get("result")
    if not isinstance(data, Mapping) or data.get("category") != "linear" or not isinstance(data.get("list"), list):
        result.errors.append("bybit universe: malformed linear ticker list")
        return
    source = "bybit:/v5/market/tickers?category=linear"
    flags = ("usdt_quoted_usd_proxy", "response_time_only", "bulk_scan")
    for row in data["list"]:
        if not isinstance(row, Mapping):
            continue
        symbol = row.get("symbol")
        if not isinstance(symbol, str) or symbol not in catalog:
            continue
        asset = catalog[symbol]
        # The V5 top-level time is response generation time, not a per-ticker event.
        stamp = None
        _price_fields(reader, result, asset, symbol, source, received, raw_id,
                      stamp, row, {"perp_last": "lastPrice", "mark_price": "markPrice",
                                   "index_price": "indexPrice"}, flags)
        if row.get("openInterest") not in (None, "") and row.get("markPrice") not in (None, ""):
            try:
                mark = _positive(row["markPrice"], "markPrice")
                oi = normalize_open_interest(row["openInterest"], unit="tokens",
                    price_usd=mark, contract_type="linear")
                oi_flags = flags + ("bybit_oi_both_sides",)
                if row.get("openInterestValue") not in (None, ""):
                    reported = _nonnegative(row["openInterestValue"], "openInterestValue")
                    if reported > 0 and abs(reported - oi.usd) / reported > Decimal("0.1"):
                        oi_flags += ("oi_usd_value_disagreement",)
                result.snapshots.append(reader._metric(asset, symbol, source, received,
                    raw_id, stamp, "open_interest", oi.tokens, "tokens", flags=oi_flags,
                    related_value=oi.usd, related_unit="USD"))
            except (ValueError, NormalizationError) as exc:
                result.errors.append(f"bybit {symbol} OI: {exc}")
        _append_optional(reader, result, asset, symbol, source, received, raw_id,
                         stamp, row, "funding_rate", "fundingRate", "decimal", flags)
        _append_optional(reader, result, asset, symbol, source, received, raw_id,
                         stamp, row, "perp_volume_24h", "volume24h", "tokens", flags)
        _append_optional(reader, result, asset, symbol, source, received, raw_id,
                         stamp, row, "perp_turnover_24h", "turnover24h", "USD",
                         flags + ("turnover_quote_unit_inferred",))


def _parse_binance(reader: _Reader, result: CollectionResult,
                   premium_reply: tuple[Any, datetime, str] | None,
                   ticker_reply: tuple[Any, datetime, str] | None,
                   catalog: dict[str, str] | None) -> None:
    if catalog is None:
        return
    source = "binance:/fapi/v1/premiumIndex"
    flags = ("usdt_quoted_usd_proxy", "bulk_scan")
    if premium_reply is not None:
        payload, received, raw_id = premium_reply
        if not isinstance(payload, list):
            result.errors.append("binance universe: malformed premium list")
        else:
            for row in payload:
                if not isinstance(row, Mapping):
                    continue
                symbol = row.get("symbol")
                if not isinstance(symbol, str) or symbol not in catalog:
                    continue
                try:
                    stamp = epoch_utc(row.get("time"))
                except MarketDataError as exc:
                    result.errors.append(f"binance {symbol} premium timestamp: {exc}")
                    continue
                asset = catalog[symbol]
                _price_fields(reader, result, asset, symbol, source, received,
                              raw_id, stamp, row, {"mark_price": "markPrice",
                                                   "index_price": "indexPrice"}, flags)
                _append_optional(reader, result, asset, symbol, source, received,
                                 raw_id, stamp, row, "funding_rate", "lastFundingRate",
                                 "decimal", flags)
    if ticker_reply is not None:
        payload, received, raw_id = ticker_reply
        if not isinstance(payload, list):
            result.errors.append("binance universe: malformed 24h ticker list")
        else:
            source = "binance:/fapi/v1/ticker/24hr"
            for row in payload:
                if not isinstance(row, Mapping):
                    continue
                symbol = row.get("symbol")
                if not isinstance(symbol, str) or symbol not in catalog:
                    continue
                try:
                    stamp = epoch_utc(row.get("closeTime"))
                except MarketDataError as exc:
                    result.errors.append(f"binance {symbol} ticker timestamp: {exc}")
                    continue
                asset = catalog[symbol]
                _price_fields(reader, result, asset, symbol, source, received,
                              raw_id, stamp, row, {"perp_last": "lastPrice"}, flags)
                _append_optional(reader, result, asset, symbol, source, received,
                                 raw_id, stamp, row, "perp_volume_24h", "volume",
                                 "tokens", flags)
                _append_optional(reader, result, asset, symbol, source, received,
                                 raw_id, stamp, row, "perp_turnover_24h", "quoteVolume",
                                 "USD", flags)


def collect_universe_cycle(client: PublicHttpClient, raw_store: RawJsonlStore) -> CollectionResult:
    """Collect broad USDT linear-perp snapshots using four market GETs.

    The first call (then hourly refreshes) also fetches Binance exchangeInfo and
    every page of Bybit instruments-info. Binance OI is absent from these bulk
    replies and is deliberately not invented. Each venue can fail independently.
    """
    result = CollectionResult()
    cache = _cache_for(client)
    kucoin = _Reader("kucoin", client, raw_store)
    bybit = _Reader("bybit", client, raw_store)
    binance = _Reader("binance", client, raw_store)

    # Catalogs precede market replies so their eligibility facts were available
    # no later than each normalized observation's received_at timestamp.
    binance_listings = _binance_catalog(binance, result, cache)
    bybit_listings = _bybit_catalog(bybit, result, cache)

    kucoin_reply = kucoin.fetch(KUCOIN_FUTURES, "/api/v1/contracts/active", {}, result)
    bybit_reply = bybit.fetch(BYBIT, "/v5/market/tickers", {"category": "linear"}, result)
    premium_reply = binance.fetch(BINANCE_FUTURES, "/fapi/v1/premiumIndex", {}, result)
    ticker_reply = binance.fetch(BINANCE_FUTURES, "/fapi/v1/ticker/24hr", {}, result)

    _parse_kucoin(kucoin, result, kucoin_reply)
    _parse_bybit(bybit, result, bybit_reply, bybit_listings)
    _parse_binance(binance, result, premium_reply, ticker_reply, binance_listings)
    return result
