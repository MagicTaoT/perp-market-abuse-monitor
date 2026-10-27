"""Read-only public REST adapters for an initial IRYS market surveillance feed.

Each successful HTTP payload is written before it is parsed.  An unsupported
listing, failed request, or unknown contract unit never becomes a fabricated
normalized observation.
"""

from __future__ import annotations

import base64
import hashlib
import json
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from decimal import Decimal
from typing import Any, Mapping
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request, urlopen

from .models import NormalizedSnapshot, Provenance, finite_decimal
from .normalization import NormalizationError, normalize_open_interest
from .storage import RawJsonlStore


KUCOIN_FUTURES = "https://api-futures.kucoin.com"
KUCOIN_SPOT = "https://api.kucoin.com"
BINANCE_FUTURES = "https://fapi.binance.com"
BINANCE_SPOT = "https://api.binance.com"
BYBIT = "https://api.bybit.com"


class MarketFetchError(RuntimeError):
    """A public endpoint failed; other venues can still be collected."""

    def __init__(self, message: str, *, status_code: int | None = None,
                 raw_body: bytes | None = None) -> None:
        super().__init__(message)
        self.status_code = status_code
        self.raw_body = raw_body


class MarketDataError(ValueError):
    """A response is structurally unsafe to normalize."""


@dataclass(frozen=True, slots=True)
class HttpPayload:
    parsed: Any
    raw_text: str
    status_code: int


def _reject_nonfinite_json(value: str) -> None:
    raise ValueError(f"nonfinite JSON constant: {value}")


def epoch_utc(value: Any) -> datetime | None:
    """Parse documented Unix seconds, milliseconds, microseconds, or nanoseconds."""
    if value is None or value == "":
        return None
    try:
        numeric = finite_decimal(value, "epoch")
    except ValueError as exc:
        raise MarketDataError("invalid exchange timestamp") from exc
    if numeric <= 0:
        raise MarketDataError("exchange timestamp must be positive")
    if numeric >= Decimal("1e17"):
        numeric /= Decimal("1e9")
    elif numeric >= Decimal("1e14"):
        numeric /= Decimal("1e6")
    elif numeric >= Decimal("1e11"):
        numeric /= Decimal("1e3")
    try:
        return datetime.fromtimestamp(float(numeric), tz=timezone.utc)
    except (OverflowError, OSError, ValueError) as exc:
        raise MarketDataError("exchange timestamp out of range") from exc


class PublicHttpClient:
    """Small bounded client with no auth or order methods."""

    def __init__(self, *, timeout: float = 8.0, retries: int = 1) -> None:
        if timeout <= 0 or retries < 0 or retries > 3:
            raise ValueError("invalid HTTP timeout or retry count")
        self.timeout = timeout
        self.retries = retries

    def get_json(self, url: str, params: Mapping[str, str]) -> HttpPayload:
        if not url.startswith("https://"):
            raise MarketFetchError("public API URL must use HTTPS")
        full_url = url + ("?" + urlencode(params) if params else "")
        for attempt in range(self.retries + 1):
            try:
                request = Request(full_url, headers={"User-Agent": "squeeze-research/0.1", "Accept": "application/json"})
                with urlopen(request, timeout=self.timeout) as response:
                    body = response.read(4_000_001)
                    if len(body) > 4_000_000:
                        raise MarketFetchError(f"response exceeds 4 MB for {url}")
                    try:
                        raw_text = body.decode("utf-8")
                        parsed = json.loads(raw_text, parse_float=Decimal,
                                            parse_constant=_reject_nonfinite_json)
                    except (UnicodeDecodeError, ValueError) as exc:
                        raise MarketFetchError(f"invalid JSON response for {url}",
                            status_code=response.status, raw_body=body) from exc
                    return HttpPayload(parsed, raw_text, response.status)
            except HTTPError as exc:
                retryable = exc.code == 429 or 500 <= exc.code < 600
                if retryable and attempt < self.retries:
                    try:
                        delay = min(5.0, max(0.5, float(exc.headers.get("Retry-After", "1"))))
                    except (TypeError, ValueError):
                        delay = 1.0
                    time.sleep(delay)
                    continue
                raise MarketFetchError(f"HTTP {exc.code} for {url}", status_code=exc.code) from exc
            except (URLError, TimeoutError) as exc:
                if attempt < self.retries:
                    time.sleep(min(2.0, 0.5 * (2 ** attempt)))
                    continue
                raise MarketFetchError(f"request failed for {url}: {type(exc).__name__}") from exc
        raise AssertionError("unreachable")


@dataclass(slots=True)
class CollectionResult:
    snapshots: list[NormalizedSnapshot] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)


class PublicExchangeAdapter:
    venue: str = ""

    def __init__(self, client: PublicHttpClient, raw_store: RawJsonlStore) -> None:
        self.client = client
        self.raw_store = raw_store

    def _fetch(self, base: str, path: str, params: Mapping[str, str], result: CollectionResult,
               *, optional_statuses: frozenset[int] = frozenset()) -> tuple[Any, datetime, str] | None:
        url = base + path
        received = datetime.now(timezone.utc)
        try:
            reply = self.client.get_json(url, params)
        except MarketFetchError as exc:
            if exc.status_code not in optional_statuses:
                result.errors.append(f"{self.venue} {path}: {exc}")
            raw_record = {"venue": self.venue, "url": url, "params": dict(params),
                          "received_at": received, "error": str(exc),
                          "http_status": exc.status_code}
            if exc.raw_body is not None:
                raw_record["raw_body_base64"] = base64.b64encode(exc.raw_body).decode("ascii")
                raw_record["raw_body_sha256"] = hashlib.sha256(exc.raw_body).hexdigest()
            self.raw_store.append(raw_record)
            return None
        received = datetime.now(timezone.utc)
        if isinstance(reply, HttpPayload):
            payload = reply.parsed
            raw_record = {"venue": self.venue, "url": url, "params": dict(params),
                          "received_at": received, "http_status": reply.status_code,
                          "raw_body": reply.raw_text}
        else:
            # Deterministic test clients can return parsed fixtures directly.
            payload = reply
            raw_record = {"venue": self.venue, "url": url, "params": dict(params),
                          "received_at": received, "fixture_payload": payload}
        raw_id = self.raw_store.append(raw_record)
        return payload, received, raw_id

    def _metric(self, asset: str, instrument: str, source: str, received: datetime, raw_id: str,
                source_time: datetime | None, metric: str, value: Any, unit: str,
                *, flags: tuple[str, ...] = (), related_value: Any = None,
                related_unit: str | None = None) -> NormalizedSnapshot:
        age = (received - source_time).total_seconds() if source_time else None
        if age is not None and age > 180:
            flags += ("stale_exchange_timestamp",)
        if age is not None and age < -30:
            flags += ("exchange_clock_ahead",)
        decimal_value = finite_decimal(value, metric)
        if metric in {"perp_last", "spot_last", "mark_price", "index_price"} and decimal_value <= 0:
            raise MarketDataError(f"{metric} must be positive")
        return NormalizedSnapshot(
            asset=asset, metric=metric, value=decimal_value, unit=unit,
            provenance=Provenance(source=source, venue=self.venue, instrument=instrument,
                                  source_timestamp=source_time, received_at=received, raw_id=raw_id),
            quality_flags=flags, related_value=related_value, related_unit=related_unit,
        )

    def _price_and_premium(self, result: CollectionResult, asset: str, symbol: str, source: str,
                           received: datetime, raw_id: str, source_time: datetime | None,
                           data: Mapping[str, Any], fields: Mapping[str, str],
                           flags: tuple[str, ...]) -> None:
        if not isinstance(data, Mapping):
            raise MarketDataError("price response is not an object")
        valid_prices: dict[str, Decimal] = {}
        for metric, key in fields.items():
            if data.get(key) not in (None, ""):
                try:
                    price = finite_decimal(data[key], metric)
                    if price <= 0:
                        raise ValueError("price must be positive")
                except ValueError:
                    result.errors.append(f"{self.venue} {metric}: invalid nonpositive or nonfinite price")
                    continue
                valid_prices[metric] = price
                result.snapshots.append(self._metric(asset, symbol, source, received, raw_id,
                    source_time, metric, price, "USD", flags=flags))
        if "mark_price" in valid_prices and "index_price" in valid_prices:
            premium = (valid_prices["mark_price"] / valid_prices["index_price"] - 1) * Decimal(10000)
            result.snapshots.append(self._metric(asset, symbol, source, received, raw_id,
                source_time, "premium_bps", premium, "bps", flags=flags))


class KucoinAdapter(PublicExchangeAdapter):
    venue = "kucoin"

    def collect(self, asset: str) -> CollectionResult:
        result = CollectionResult()
        asset = asset.upper()
        future = f"{asset}USDTM"
        spot = f"{asset}-USDT"
        reply = self._fetch(KUCOIN_FUTURES, f"/api/v1/contracts/{future}", {}, result)
        if reply:
            payload, received, raw_id = reply
            try:
                data = payload["data"]
                if payload.get("code") != "200000" or data.get("symbol") != future or data.get("status") != "Open":
                    raise MarketDataError("future is not confirmed open")
                if (data.get("baseCurrency") != asset or data.get("quoteCurrency") != "USDT"
                    or data.get("settleCurrency") != "USDT" or data.get("isInverse") is not False):
                    raise MarketDataError("future contract type or currency mismatch")
                source_time = epoch_utc(data.get("ts") or data.get("time"))
                source = "kucoin:/api/v1/contracts/{symbol}"
                flags = ("usdt_quoted_usd_proxy",)
                self._price_and_premium(result, asset, future, source, received, raw_id, source_time,
                    data, {"perp_last": "lastTradePrice", "mark_price": "markPrice", "index_price": "indexPrice"}, flags)
                if data.get("openInterest") not in (None, "") and data.get("markPrice") not in (None, ""):
                    oi = normalize_open_interest(data["openInterest"], unit="contracts", price_usd=data["markPrice"],
                        contract_type="linear", contract_multiplier=data.get("multiplier"), multiplier_unit="tokens")
                    result.snapshots.append(self._metric(asset, future, source, received, raw_id, source_time,
                        "open_interest", oi.tokens, "tokens", flags=flags + ("oi_unit_inferred_contract_count",),
                        related_value=oi.usd, related_unit="USD"))
                if data.get("fundingFeeRate") not in (None, ""):
                    result.snapshots.append(self._metric(asset, future, source, received, raw_id, source_time,
                        "funding_rate", data["fundingFeeRate"], "decimal", flags=flags))
                if data.get("volumeOf24h") not in (None, ""):
                    result.snapshots.append(self._metric(asset, future, source, received, raw_id, source_time,
                        "perp_volume_24h", data["volumeOf24h"], "tokens", flags=flags + ("volume_unit_inferred",)))
            except (KeyError, TypeError, ValueError, NormalizationError) as exc:
                result.errors.append(f"kucoin futures parse: {exc}")
        reply = self._fetch(KUCOIN_SPOT, "/api/v1/market/orderbook/level1", {"symbol": spot}, result,
                            optional_statuses=frozenset({400, 404}))
        if reply:
            payload, received, raw_id = reply
            try:
                data = payload["data"]
                if payload.get("code") != "200000" or not isinstance(data, Mapping) or not data.get("price"):
                    raise MarketDataError("spot listing or price unavailable")
                result.snapshots.append(self._metric(asset, spot, "kucoin:/api/v1/market/orderbook/level1",
                    received, raw_id, epoch_utc(data.get("time")), "spot_last", data["price"], "USD",
                    flags=("usdt_quoted_usd_proxy",)))
            except (KeyError, TypeError, ValueError) as exc:
                result.errors.append(f"kucoin spot parse: {exc}")
        return result


class BinanceAdapter(PublicExchangeAdapter):
    venue = "binance"

    def __init__(self, client: PublicHttpClient, raw_store: RawJsonlStore) -> None:
        super().__init__(client, raw_store)
        self._exchange_info: Any = None
        self._exchange_info_expires = 0.0

    def collect(self, asset: str) -> CollectionResult:
        result = CollectionResult()
        asset = asset.upper()
        symbol = f"{asset}USDT"
        fetched_meta = False
        if self._exchange_info is None or time.monotonic() >= self._exchange_info_expires:
            # Binance currently ignores the symbol query here and sends ~1 MB
            # for all contracts. Do not archive that response every minute.
            meta_reply = self._fetch(BINANCE_FUTURES, "/fapi/v1/exchangeInfo", {}, result)
            if not meta_reply:
                return result
            meta = meta_reply[0]
            fetched_meta = True
        else:
            meta = self._exchange_info
        try:
            instrument = next(s for s in meta["symbols"] if s.get("symbol") == symbol)
            if (instrument.get("status") != "TRADING" or instrument.get("contractType") != "PERPETUAL"
                or instrument.get("baseAsset") != asset or instrument.get("quoteAsset") != "USDT"):
                raise MarketDataError("future is not confirmed linear trading perpetual")
        except (KeyError, TypeError, StopIteration, ValueError) as exc:
            result.errors.append(f"binance listing: {exc}")
            return result
        if fetched_meta:
            self._exchange_info = meta
            self._exchange_info_expires = time.monotonic() + 3600
        premium_reply = self._fetch(BINANCE_FUTURES, "/fapi/v1/premiumIndex", {"symbol": symbol}, result)
        mark = None
        mark_stamp = None
        mark_raw_id = None
        if premium_reply:
            data, received, raw_id = premium_reply
            try:
                if data.get("symbol") != symbol:
                    raise MarketDataError("premium symbol mismatch")
                stamp = epoch_utc(data.get("time"))
                mark_stamp = stamp
                mark_raw_id = raw_id
                source = "binance:/fapi/v1/premiumIndex"
                flags = ("usdt_quoted_usd_proxy",)
                self._price_and_premium(result, asset, symbol, source, received, raw_id, stamp, data,
                    {"mark_price": "markPrice", "index_price": "indexPrice"}, flags)
                mark = data.get("markPrice")
                if data.get("lastFundingRate") not in (None, ""):
                    result.snapshots.append(self._metric(asset, symbol, source, received, raw_id, stamp,
                        "funding_rate", data["lastFundingRate"], "decimal", flags=flags))
            except (TypeError, ValueError) as exc:
                result.errors.append(f"binance premium parse: {exc}")
        oi_reply = self._fetch(BINANCE_FUTURES, "/fapi/v1/openInterest", {"symbol": symbol}, result)
        if oi_reply and mark is not None:
            data, received, raw_id = oi_reply
            try:
                if data.get("symbol") != symbol:
                    raise MarketDataError("OI symbol mismatch")
                oi_stamp = epoch_utc(data.get("time"))
                if oi_stamp is None or mark_stamp is None or abs((oi_stamp - mark_stamp).total_seconds()) > 30:
                    raise MarketDataError("OI/mark source times differ by more than 30 seconds")
                # USD-M linear quantity is base-asset units; see source audit.
                oi = normalize_open_interest(data["openInterest"], unit="tokens", price_usd=mark, contract_type="linear")
                derived_raw_id = self.raw_store.append({
                    "venue": self.venue, "kind": "derived_open_interest",
                    "instrument": symbol, "received_at": received,
                    "input_raw_ids": [raw_id, mark_raw_id],
                    "open_interest_source_time": oi_stamp,
                    "mark_source_time": mark_stamp,
                    "mark_price_usdt": str(mark),
                    "unit_basis": "USD-M base-asset OI, crosschecked against OI history USD value",
                })
                result.snapshots.append(self._metric(asset, symbol, "binance:/fapi/v1/openInterest",
                    received, derived_raw_id, oi_stamp, "open_interest", oi.tokens, "tokens",
                    flags=("usdt_quoted_usd_proxy", "oi_unit_inferred_crosschecked"),
                    related_value=oi.usd, related_unit="USD"))
            except (KeyError, TypeError, ValueError, NormalizationError) as exc:
                result.errors.append(f"binance OI parse: {exc}")
        ticker_reply = self._fetch(BINANCE_FUTURES, "/fapi/v1/ticker/24hr", {"symbol": symbol}, result)
        if ticker_reply:
            data, received, raw_id = ticker_reply
            try:
                if data.get("symbol") != symbol:
                    raise MarketDataError("ticker symbol mismatch")
                stamp = epoch_utc(data.get("closeTime"))
                source = "binance:/fapi/v1/ticker/24hr"
                for metric, key in (("perp_last", "lastPrice"), ("perp_volume_24h", "volume")):
                    if data.get(key) not in (None, ""):
                        unit = "USD" if metric == "perp_last" else "tokens"
                        result.snapshots.append(self._metric(asset, symbol, source, received, raw_id, stamp,
                            metric, data[key], unit, flags=("usdt_quoted_usd_proxy",)))
            except (TypeError, ValueError) as exc:
                result.errors.append(f"binance ticker parse: {exc}")
        # A spot symbol is optional. Binance currently returns HTTP 400 for IRYSUSDT.
        spot_meta = self._fetch(BINANCE_SPOT, "/api/v3/exchangeInfo", {"symbol": symbol}, result,
                                optional_statuses=frozenset({400, 404}))
        if spot_meta:
            try:
                listed = any(s.get("symbol") == symbol and s.get("status") == "TRADING"
                             for s in spot_meta[0].get("symbols", []))
            except (AttributeError, TypeError):
                listed = False
            if listed:
                spot_reply = self._fetch(BINANCE_SPOT, "/api/v3/ticker/price", {"symbol": symbol}, result)
                if spot_reply:
                    data, received, raw_id = spot_reply
                    if isinstance(data, Mapping) and data.get("symbol") == symbol and data.get("price"):
                        try:
                            price = finite_decimal(data["price"], "spot_last")
                            if price <= 0:
                                raise ValueError("price must be positive")
                            result.snapshots.append(self._metric(asset, symbol, "binance:/api/v3/ticker/price",
                                received, raw_id, None, "spot_last", price, "USD",
                                flags=("usdt_quoted_usd_proxy",)))
                        except ValueError:
                            result.errors.append("binance spot: invalid price")
        return result


class BybitAdapter(PublicExchangeAdapter):
    venue = "bybit"

    def collect(self, asset: str) -> CollectionResult:
        result = CollectionResult()
        asset = asset.upper()
        symbol = f"{asset}USDT"
        meta_reply = self._fetch(BYBIT, "/v5/market/instruments-info", {"category": "linear", "symbol": symbol}, result)
        if not meta_reply:
            return result
        try:
            meta = meta_reply[0]
            if not isinstance(meta, Mapping) or meta.get("retCode") != 0:
                raise MarketDataError("instrument response error code")
            listed = next(x for x in meta["result"]["list"] if x.get("symbol") == symbol)
            if (listed.get("status") != "Trading" or listed.get("contractType") != "LinearPerpetual"
                or listed.get("baseCoin") != asset or listed.get("quoteCoin") != "USDT"
                or listed.get("settleCoin") != "USDT"):
                raise MarketDataError("future is not confirmed linear trading perpetual")
        except (KeyError, TypeError, StopIteration, ValueError) as exc:
            result.errors.append(f"bybit listing: {exc}")
            return result
        reply = self._fetch(BYBIT, "/v5/market/tickers", {"category": "linear", "symbol": symbol}, result)
        if reply:
            payload, received, raw_id = reply
            try:
                data = next(x for x in payload["result"]["list"] if x.get("symbol") == symbol)
                if payload.get("retCode") != 0:
                    raise MarketDataError("ticker error code")
                # V5 top-level `time` is the response generation time, not a
                # per-ticker event timestamp. Do not call it source freshness.
                stamp = None
                source = "bybit:/v5/market/tickers?category=linear"
                flags = ("usdt_quoted_usd_proxy", "response_time_only")
                self._price_and_premium(result, asset, symbol, source, received, raw_id, stamp, data,
                    {"perp_last": "lastPrice", "mark_price": "markPrice", "index_price": "indexPrice"}, flags)
                if data.get("openInterest") not in (None, "") and data.get("markPrice") not in (None, ""):
                    oi = normalize_open_interest(data["openInterest"], unit="tokens", price_usd=data["markPrice"], contract_type="linear")
                    oi_flags = flags
                    if data.get("openInterestValue") not in (None, ""):
                        reported = finite_decimal(data["openInterestValue"], "openInterestValue")
                        if reported > 0 and abs(reported - oi.usd) / reported > Decimal("0.1"):
                            oi_flags += ("oi_usd_value_disagreement",)
                    result.snapshots.append(self._metric(asset, symbol, source, received, raw_id, stamp,
                        "open_interest", oi.tokens, "tokens", flags=oi_flags,
                        related_value=oi.usd, related_unit="USD"))
                for metric, key, unit in (("funding_rate", "fundingRate", "decimal"),
                                          ("perp_volume_24h", "volume24h", "tokens")):
                    if data.get(key) not in (None, ""):
                        result.snapshots.append(self._metric(asset, symbol, source, received, raw_id, stamp,
                            metric, data[key], unit, flags=flags))
            except (KeyError, TypeError, StopIteration, ValueError, NormalizationError) as exc:
                result.errors.append(f"bybit ticker parse: {exc}")
        spot_meta = self._fetch(BYBIT, "/v5/market/instruments-info", {"category": "spot", "symbol": symbol}, result,
                                optional_statuses=frozenset({400, 404}))
        spot_listed = False
        if spot_meta:
            payload = spot_meta[0]
            try:
                spot_listed = payload.get("retCode") == 0 and any(
                    x.get("symbol") == symbol and x.get("status") == "Trading"
                    for x in payload["result"]["list"]
                )
            except (KeyError, TypeError, AttributeError):
                spot_listed = False
        spot_reply = self._fetch(BYBIT, "/v5/market/tickers", {"category": "spot", "symbol": symbol}, result) if spot_listed else None
        if spot_reply:
            payload, received, raw_id = spot_reply
            try:
                if payload.get("retCode") == 0:
                    matching = [x for x in payload["result"]["list"] if x.get("symbol") == symbol]
                    if matching and matching[0].get("lastPrice"):
                        result.snapshots.append(self._metric(asset, symbol,
                            "bybit:/v5/market/tickers?category=spot", received, raw_id,
                            None, "spot_last", matching[0]["lastPrice"], "USD",
                            flags=("usdt_quoted_usd_proxy", "response_time_only")))
            except (KeyError, TypeError, ValueError) as exc:
                result.errors.append(f"bybit spot parse: {exc}")
        return result


ADAPTERS = (KucoinAdapter, BinanceAdapter, BybitAdapter)
