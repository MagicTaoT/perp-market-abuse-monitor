"""Keyless, read-only token supply and optional unlock enrichment.

CoinMarketCap IDs are resolved before quotes are requested. A ticker collision
never silently selects the highest-ranked token. PatternPlus covers only a
small subset of tokens, so no event means *unknown*, not no future unlocks.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime, timedelta, timezone
from decimal import Decimal
from typing import Any, Mapping, Sequence

from .connectors import HttpPayload, MarketFetchError, PublicHttpClient
from .models import NormalizedSnapshot, Provenance, finite_decimal, utc_datetime
from .storage import RawJsonlStore


CMC_BASE = "https://pro-api.coinmarketcap.com/public-api"
CMC_MAP_URL = CMC_BASE + "/v1/cryptocurrency/map"
CMC_QUOTES_URL = CMC_BASE + "/v3/cryptocurrency/quotes/latest"
PATTERNPLUS_UNLOCKS_URL = "https://pattern.plus/api/v1/unlocks.json"

# BMT's project identity was checked against KuCoin's listing announcement.
# An override still has to agree with the CMC quote's ID and symbol.
CMC_ID_OVERRIDES: Mapping[str, int] = {"BMT": 35214}
_MAP_BATCH = 40
_QUOTE_BATCH = 80


@dataclass(frozen=True, slots=True)
class UnlockEvent:
    asset: str
    provider_id: int
    unlock_date: date
    amount_tokens: Decimal
    value_usd: Decimal | None
    pct_circulating: Decimal | None
    pct_total: Decimal | None
    basis: tuple[str, ...]
    types: tuple[str, ...]
    categories: tuple[str, ...]
    source_url: str
    source_as_of: date
    source_updated_at: datetime | None
    received_at: datetime
    attribution_text: str
    attribution_url: str
    quality_flags: tuple[str, ...] = ()


@dataclass(slots=True)
class TokenomicsResult:
    snapshots: list[NormalizedSnapshot] = field(default_factory=list)
    unlock_events: list[UnlockEvent] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    identity_status: dict[str, dict[str, str | int | None]] = field(default_factory=dict)


def _chunks(values: Sequence[str | int], size: int) -> list[Sequence[str | int]]:
    return [values[index:index + size] for index in range(0, len(values), size)]


def _timestamp(value: Any) -> datetime | None:
    if not isinstance(value, str) or not value:
        return None
    try:
        parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
        return utc_datetime(parsed)
    except ValueError:
        return None


def _positive(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        number = finite_decimal(value, "tokenomics value")
    except ValueError:
        return None
    return number if number > 0 else None


def _nonnegative(value: Any) -> Decimal | None:
    if value is None or value == "":
        return None
    try:
        number = finite_decimal(value, "unlock value")
    except ValueError:
        return None
    return number if number >= 0 else None


def _fetch(client: PublicHttpClient, raw_store: RawJsonlStore, result: TokenomicsResult,
           url: str, params: Mapping[str, str], now: datetime, *,
           failure: list[MarketFetchError] | None = None,
           report_error: bool = True) -> tuple[Any, str] | None:
    try:
        reply = client.get_json(url, params)
    except MarketFetchError as exc:
        if failure is not None:
            failure.append(exc)
        if report_error:
            result.errors.append(f"{url}: {exc}")
        raw_store.append({"source": url, "params": dict(params), "received_at": now,
                          "error": str(exc), "http_status": exc.status_code})
        return None
    if isinstance(reply, HttpPayload):
        payload = reply.parsed
        raw = {"source": url, "params": dict(params), "received_at": now,
               "http_status": reply.status_code, "raw_body": reply.raw_text}
    else:
        payload = reply
        raw = {"source": url, "params": dict(params), "received_at": now,
               "fixture_payload": payload}
    # Save the response before validating or normalizing any field.
    return payload, raw_store.append(raw)


def _cmc_data(payload: Any, result: TokenomicsResult, source: str) -> list[Mapping[str, Any]]:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("status"), Mapping):
        result.errors.append(f"{source}: invalid response envelope")
        return []
    if payload["status"].get("error_code") not in (0, "0"):
        result.errors.append(f"{source}: provider error {payload['status'].get('error_code')}")
        return []
    data = payload.get("data")
    if isinstance(data, list):
        return [row for row in data if isinstance(row, Mapping)]
    # CMC's keyed API has returned ID-keyed maps for some endpoint versions.
    if isinstance(data, Mapping):
        return [row for row in data.values() if isinstance(row, Mapping)]
    result.errors.append(f"{source}: missing data array")
    return []


def _cmc_id(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    try:
        numeric = int(value)
    except (TypeError, ValueError):
        return None
    return numeric if numeric > 0 and str(value) == str(numeric) else None


def _quote_usd(row: Mapping[str, Any]) -> Mapping[str, Any] | None:
    quotes = row.get("quote")
    if isinstance(quotes, Mapping):
        usd = quotes.get("USD")
        return usd if isinstance(usd, Mapping) else None
    if isinstance(quotes, list):
        for quote in quotes:
            if isinstance(quote, Mapping) and quote.get("symbol") == "USD":
                return quote
    return None


def _metric(asset: str, cmc_id: int, name: str, value: Decimal, unit: str,
            source_time: datetime | None, received_at: datetime, raw_id: str) -> NormalizedSnapshot:
    flags: list[str] = []
    if source_time is not None:
        if received_at - source_time > timedelta(days=2):
            flags.append("stale_tokenomics_timestamp")
        if source_time - received_at > timedelta(minutes=5):
            flags.append("source_clock_ahead")
    return NormalizedSnapshot(
        asset=asset, metric=name, value=value, unit=unit,
        provenance=Provenance(
            source="coinmarketcap:/public-api/v3/cryptocurrency/quotes/latest",
            venue="coinmarketcap", instrument=f"CMC:{cmc_id}",
            source_timestamp=source_time, received_at=received_at, raw_id=raw_id,
        ),
        quality_flags=tuple(flags),
    )


def _clean_strings(value: Any) -> tuple[str, ...]:
    if not isinstance(value, list):
        return ()
    return tuple(item.strip() for item in value if isinstance(item, str) and item.strip())


def _identity(asset: str, row: Mapping[str, Any], status: str, reason: str = "") -> dict[str, str | int | None]:
    return {"status": status, "provider_id": _cmc_id(row.get("id")),
            "name": row.get("name") if isinstance(row.get("name"), str) else None,
            "slug": row.get("slug") if isinstance(row.get("slug"), str) else None,
            "reason": reason, "unlock_status": "unknown"}


def _unlock_matches(asset: str, identity: Mapping[str, str | int | None],
                    row: Mapping[str, Any]) -> bool:
    if str(row.get("symbol", "")).upper() != asset:
        return False
    provider_name = identity.get("name")
    provider_slug = identity.get("slug")
    row_name = row.get("name")
    row_slug = row.get("token")
    def folded(value: Any) -> str:
        return "".join(char for char in str(value).casefold() if char.isalnum())
    name_match = bool(provider_name and row_name and folded(provider_name) == folded(row_name))
    slug_match = bool(provider_slug and row_slug and str(row_slug).casefold() ==
                      f"{asset.lower()}-{str(provider_slug).casefold()}")
    return name_match or slug_match


def _collect_unlocks(client: PublicHttpClient, raw_store: RawJsonlStore,
                     result: TokenomicsResult, now: datetime) -> None:
    fetched = _fetch(client, raw_store, result, PATTERNPLUS_UNLOCKS_URL, {}, now)
    if fetched is None:
        return
    payload, _raw_id = fetched
    if not isinstance(payload, Mapping) or not isinstance(payload.get("data"), list):
        result.errors.append("patternplus: invalid unlock response")
        return
    attribution = payload.get("attribution")
    if not isinstance(attribution, Mapping) or attribution.get("url") != "https://pattern.plus":
        result.errors.append("patternplus: missing required attribution")
        return
    text = attribution.get("text")
    if not isinstance(text, str) or not text.strip():
        result.errors.append("patternplus: missing required attribution text")
        return
    try:
        source_as_of = date.fromisoformat(payload["as_of"])
    except (KeyError, TypeError, ValueError):
        result.errors.append("patternplus: missing as-of date")
        return
    updated = _timestamp(payload.get("rebuilt"))
    calendar_is_stale = now.date() - source_as_of > timedelta(days=2)
    seen: set[tuple[str, date]] = set()
    for row in payload["data"]:
        if not isinstance(row, Mapping):
            continue
        asset = str(row.get("symbol", "")).upper()
        identity = result.identity_status.get(asset)
        if not identity or identity["status"] not in ("matched", "override"):
            continue
        if not _unlock_matches(asset, identity, row):
            continue
        try:
            unlock_date = date.fromisoformat(row["date"])
        except (KeyError, TypeError, ValueError):
            result.errors.append(f"patternplus {asset}: invalid unlock date")
            continue
        if unlock_date < now.date() or (asset, unlock_date) in seen:
            continue
        amount = _positive(row.get("amount"))
        if amount is None:
            result.errors.append(f"patternplus {asset}: invalid unlock amount")
            continue
        source_url = row.get("file")
        if not isinstance(source_url, str) or not source_url.startswith(
                "https://pattern.plus/api/v1/unlocks/"):
            result.errors.append(f"patternplus {asset}: invalid source URL")
            continue
        seen.add((asset, unlock_date))
        flags = ("date_precision_day", "stale_unlock_calendar") if calendar_is_stale else ("date_precision_day",)
        result.unlock_events.append(UnlockEvent(
            asset=asset, provider_id=int(identity["provider_id"]),
            unlock_date=unlock_date, amount_tokens=amount,
            value_usd=_nonnegative(row.get("value_usd")),
            pct_circulating=_nonnegative(row.get("pct_circulating")),
            pct_total=_nonnegative(row.get("pct_total")),
            basis=_clean_strings(row.get("basis")), types=_clean_strings(row.get("types")),
            categories=_clean_strings(row.get("categories")), source_url=source_url,
            source_as_of=source_as_of, source_updated_at=updated, received_at=now,
            attribution_text=text.strip(), attribution_url="https://pattern.plus",
            quality_flags=flags,
        ))
        identity["unlock_status"] = "stale" if calendar_is_stale else "upcoming"


def collect_tokenomics(assets: Sequence[str], client: PublicHttpClient,
                       raw_store: RawJsonlStore, now: datetime | None = None) -> TokenomicsResult:
    """Fetch supply/valuation and a best-effort unlock calendar without secrets.

    The caller controls cadence. A failed provider or unresolved ticker leaves
    the affected metric absent; it never supplies a zero or a false identity.
    """
    received_at = utc_datetime(now or datetime.now(timezone.utc), "now")
    symbols = sorted({str(asset).strip().upper() for asset in assets if str(asset).strip()})
    result = TokenomicsResult()
    if not symbols:
        return result

    mapped: dict[str, dict[int, Mapping[str, Any]]] = {asset: {} for asset in symbols}
    map_rejected: set[str] = set()

    def fetch_map_batch(batch: Sequence[str]) -> None:
        # CMC map rejects an entire symbol batch if even one ticker is unknown.
        # Split only HTTP 400 responses: a 429 or network failure must not turn
        # into more requests. Keep every failed request in the raw audit trail.
        failure: list[MarketFetchError] = []
        fetched = _fetch(client, raw_store, result, CMC_MAP_URL,
                         {"symbol": ",".join(batch)}, received_at,
                         failure=failure, report_error=False)
        if fetched is None:
            if failure and failure[0].status_code == 400:
                if len(batch) > 1:
                    middle = len(batch) // 2
                    fetch_map_batch(batch[:middle])
                    fetch_map_batch(batch[middle:])
                else:
                    map_rejected.add(batch[0])
            elif failure:
                result.errors.append(f"{CMC_MAP_URL}: {failure[0]}")
            return
        payload, _raw_id = fetched
        for row in _cmc_data(payload, result, "coinmarketcap map"):
            symbol = str(row.get("symbol", "")).upper()
            cmc_id = _cmc_id(row.get("id"))
            if symbol in mapped and symbol in batch and cmc_id is not None:
                mapped[symbol][cmc_id] = row

    for batch in _chunks(symbols, _MAP_BATCH):
        fetch_map_batch(batch)

    ids: dict[int, str] = {}
    for asset in symbols:
        choices = mapped[asset]
        override = CMC_ID_OVERRIDES.get(asset)
        if override is not None:
            row = choices.get(override, {"id": override, "symbol": asset})
            result.identity_status[asset] = _identity(asset, row, "override", "explicit CMC ID override")
            ids[override] = asset
        elif len(choices) == 1:
            row = next(iter(choices.values()))
            result.identity_status[asset] = _identity(asset, row, "matched", "unique active symbol")
            ids[int(row["id"])] = asset
        elif choices:
            result.identity_status[asset] = {"status": "ambiguous", "provider_id": None,
                "name": None, "slug": None, "reason": f"{len(choices)} active CMC IDs share ticker",
                "unlock_status": "unknown"}
        else:
            result.identity_status[asset] = {"status": "missing", "provider_id": None,
                "name": None, "slug": None,
                "reason": ("CMC map rejected symbol (HTTP 400)" if asset in map_rejected
                           else "no active CMC ID returned"),
                "unlock_status": "unknown"}

    for batch in _chunks(sorted(ids), _QUOTE_BATCH):
        fetched = _fetch(client, raw_store, result, CMC_QUOTES_URL,
                         {"id": ",".join(str(item) for item in batch),
                          "convert": "USD", "skip_invalid": "true"}, received_at)
        if fetched is None:
            continue
        payload, raw_id = fetched
        for row in _cmc_data(payload, result, "coinmarketcap quotes"):
            cmc_id = _cmc_id(row.get("id"))
            asset = ids.get(cmc_id) if cmc_id is not None else None
            if asset is None:
                continue
            if str(row.get("symbol", "")).upper() != asset:
                result.errors.append(f"coinmarketcap {asset}: quote symbol/ID mismatch")
                result.identity_status[asset]["status"] = "missing"
                result.identity_status[asset]["reason"] = "quote symbol/ID mismatch"
                continue
            # Quotes are authoritative for name/slug, including explicit IDs.
            result.identity_status[asset]["name"] = row.get("name") if isinstance(row.get("name"), str) else None
            result.identity_status[asset]["slug"] = row.get("slug") if isinstance(row.get("slug"), str) else None
            quote = _quote_usd(row)
            if quote is None:
                result.errors.append(f"coinmarketcap {asset}: missing USD quote")
                continue
            asset_time = _timestamp(row.get("last_updated"))
            quote_time = _timestamp(quote.get("last_updated"))
            fields = (("circulating_supply", row.get("circulating_supply"), "tokens", asset_time),
                      ("total_supply", row.get("total_supply"), "tokens", asset_time),
                      ("max_supply", row.get("max_supply"), "tokens", asset_time),
                      ("market_cap_usd", quote.get("market_cap"), "USD", quote_time or asset_time),
                      ("fdv_usd", quote.get("fully_diluted_market_cap"), "USD", quote_time or asset_time),
                      ("cmc_price", quote.get("price"), "USD", quote_time or asset_time))
            for metric, raw_value, unit, source_time in fields:
                value = _positive(raw_value)
                if value is not None:
                    result.snapshots.append(_metric(asset, cmc_id, metric, value, unit,
                                                    source_time, received_at, raw_id))

    _collect_unlocks(client, raw_store, result, received_at)
    return result
