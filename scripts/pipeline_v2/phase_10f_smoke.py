"""Outcome-blind Phase 10F-B candlestick validation and price extraction."""

from __future__ import annotations

from collections import Counter
from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import json
import math
from typing import Any, Iterable, Mapping, Sequence

from scripts.common.probability_utils import safe_float
from scripts.common.time_utils import format_iso_utc, parse_iso_utc
from scripts.pipeline_v2.kalshi_metadata_cache import canonical_json


SCHEMA_VERSION = "phase-10f-b-bounded-smoke-v1"
MAX_SMOKE_FAMILIES = 200
LOOKBACK_SECONDS = 60 * 60
PRIMARY_STALENESS_MINUTES = 15.0
ROBUSTNESS_STALENESS_MINUTES = 60.0
CANDLE_TIMESTAMP_SEMANTICS = "inclusive_end_period"
BATCH_ENDPOINT = "/trade-api/v2/markets/candlesticks"
OFFICIAL_BATCH_DOC_URL = (
    "https://docs.kalshi.com/api-reference/market/batch-get-market-candlesticks"
)
OFFICIAL_BATCH_DOC_SHA256 = (
    "b1c0f8b4de3671d237c81411099e44e7b0d09cdacd67cc1db1465121209ea6fa"
)


class SmokeValidationError(RuntimeError):
    pass


@dataclass(frozen=True)
class SmokeFamily:
    family_id: str
    family_id_source: str
    event_ticker: str
    rule: str
    category: str
    timing_structure: str
    target_time: str
    market_existence_at_target: str
    eligible_tickers: tuple[str, ...]

    @property
    def identity(self) -> tuple[str, str]:
        return self.family_id, self.family_id_source

    @property
    def target_ts(self) -> int:
        value = parse_iso_utc(self.target_time)
        if value is None:
            raise SmokeValidationError("smoke family target must be exact")
        return int(value.timestamp())


@dataclass(frozen=True)
class RequestGroup:
    request_id: str
    tickers: tuple[str, ...]
    start_ts: int
    end_ts: int
    purpose: str = "smoke_price_window"

    @property
    def params(self) -> dict[str, Any]:
        return {
            "market_tickers": ",".join(self.tickers),
            "start_ts": self.start_ts,
            "end_ts": self.end_ts,
            "period_interval": 1,
            "include_latest_before_start": "false",
        }


def sha256_bytes(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def request_group_id(
    tickers: Iterable[str], start_ts: int, end_ts: int, purpose: str
) -> str:
    identity = {
        "endpoint": BATCH_ENDPOINT,
        "tickers": sorted(set(tickers)),
        "start_ts": int(start_ts),
        "end_ts": int(end_ts),
        "period_interval": 1,
        "include_latest_before_start": False,
        "purpose": purpose,
    }
    return sha256_bytes(canonical_json(identity))[:24]


def build_request_groups(
    families: Sequence[SmokeFamily], *, batch_size: int = 100
) -> list[RequestGroup]:
    if len(families) > MAX_SMOKE_FAMILIES:
        raise SmokeValidationError("bounded smoke cannot exceed 200 families")
    if batch_size <= 0 or batch_size > 100:
        raise SmokeValidationError("Kalshi batch size must be in [1, 100]")
    by_target: dict[int, set[str]] = {}
    ticker_targets: dict[str, int] = {}
    for family in families:
        for ticker in family.eligible_tickers:
            previous = ticker_targets.setdefault(ticker, family.target_ts)
            if previous != family.target_ts:
                raise SmokeValidationError(
                    f"ticker {ticker} maps to multiple target timestamps"
                )
            by_target.setdefault(family.target_ts, set()).add(ticker)
    groups: list[RequestGroup] = []
    for target_ts in sorted(by_target):
        tickers = sorted(by_target[target_ts])
        for offset in range(0, len(tickers), batch_size):
            batch = tuple(tickers[offset : offset + batch_size])
            start_ts = target_ts - LOOKBACK_SECONDS
            groups.append(
                RequestGroup(
                    request_id=request_group_id(
                        batch, start_ts, target_ts, "smoke_price_window"
                    ),
                    tickers=batch,
                    start_ts=start_ts,
                    end_ts=target_ts,
                )
            )
    return groups


def _numeric(value: Any) -> float | None:
    parsed = safe_float(value)
    if parsed is None or not math.isfinite(parsed):
        return None
    return parsed


def _close(candle: Mapping[str, Any], field: str) -> float | None:
    parent = candle.get(field)
    if not isinstance(parent, Mapping):
        return None
    # Current responses use fixed-point dollars; the historical response uses
    # legacy numeric `close`. Supporting both is schema compatibility, not a
    # price fallback.
    for name in ("close_dollars", "close"):
        value = _numeric(parent.get(name))
        if value is not None:
            return value
    return None


def _end_ts(candle: Mapping[str, Any]) -> int:
    value = candle.get("end_period_ts")
    if isinstance(value, bool):
        raise SmokeValidationError("invalid boolean candle timestamp")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise SmokeValidationError("candle lacks integer end_period_ts") from exc
    return parsed


def validate_batch_payload(
    payload: Any, group: RequestGroup
) -> dict[str, list[dict[str, Any]]]:
    if not isinstance(payload, Mapping) or not isinstance(payload.get("markets"), list):
        raise SmokeValidationError("batch candlestick response schema changed")
    requested = set(group.tickers)
    found: dict[str, list[dict[str, Any]]] = {}
    for position, item in enumerate(payload["markets"]):
        if not isinstance(item, Mapping):
            raise SmokeValidationError(f"markets[{position}] is not an object")
        ticker = str(item.get("market_ticker") or "")
        candles = item.get("candlesticks")
        if not ticker or ticker not in requested or not isinstance(candles, list):
            raise SmokeValidationError("unexpected market identity or candle schema")
        if ticker in found:
            raise SmokeValidationError(f"duplicate returned market ticker: {ticker}")
        normalized: list[dict[str, Any]] = []
        seen_timestamps: set[int] = set()
        for candle in candles:
            if not isinstance(candle, Mapping):
                raise SmokeValidationError("candlestick is not an object")
            row = dict(candle)
            timestamp = _end_ts(row)
            if timestamp > group.end_ts:
                raise SmokeValidationError("post-target candlestick returned")
            if timestamp < group.start_ts:
                raise SmokeValidationError(
                    "pre-start candle returned while synthetic continuity is disabled"
                )
            if timestamp in seen_timestamps:
                raise SmokeValidationError("duplicate candlestick timestamp")
            seen_timestamps.add(timestamp)
            normalized.append(row)
        found[ticker] = sorted(normalized, key=_end_ts)
    return found


def validate_documented_boundary_semantics(semantics: str) -> None:
    if semantics != CANDLE_TIMESTAMP_SEMANTICS:
        raise SmokeValidationError(
            "candle timestamp is not established as inclusive period end"
        )


def latest_complete_candle(
    candles: Iterable[Mapping[str, Any]], target_ts: int
) -> dict[str, Any] | None:
    """Latest full candle under the pinned inclusive-end convention."""
    validate_documented_boundary_semantics(CANDLE_TIMESTAMP_SEMANTICS)
    eligible = [dict(row) for row in candles if _end_ts(row) <= target_ts]
    return max(eligible, key=_end_ts) if eligible else None


def latest_trade_candle(
    candles: Iterable[Mapping[str, Any]], target_ts: int
) -> dict[str, Any] | None:
    eligible = [
        dict(row)
        for row in candles
        if _end_ts(row) <= target_ts and _close(row, "price") is not None
    ]
    return max(eligible, key=_end_ts) if eligible else None


def _iso_unix(value: int | None) -> str:
    if value is None:
        return ""
    return format_iso_utc(datetime.fromtimestamp(value, tz=timezone.utc))


def _age(target_ts: int, observation_ts: int | None) -> float | None:
    if observation_ts is None or observation_ts > target_ts:
        return None
    return (target_ts - observation_ts) / 60.0


def extract_contract_observation(
    *, ticker: str, candles: Iterable[Mapping[str, Any]], target_ts: int
) -> dict[str, Any]:
    rows = list(candles)
    latest = latest_complete_candle(rows, target_ts)
    trade = latest_trade_candle(rows, target_ts)
    midpoint_reason = ""
    bid = ask = midpoint = spread = None
    quote_ts = _end_ts(latest) if latest is not None else None
    if latest is None:
        midpoint_reason = "no_candle_before_target"
    else:
        bid = _close(latest, "yes_bid")
        ask = _close(latest, "yes_ask")
        if bid is None and ask is None:
            midpoint_reason = "missing_bid_and_ask"
        elif bid is None:
            midpoint_reason = "missing_bid"
        elif ask is None:
            midpoint_reason = "missing_ask"
        elif not (0 <= bid <= ask <= 1):
            raise SmokeValidationError(f"invalid or crossed YES quote for {ticker}")
        else:
            midpoint = (bid + ask) / 2
            spread = ask - bid
    quote_age = _age(target_ts, quote_ts)
    if midpoint is not None and quote_age is not None and quote_age > 60:
        midpoint_reason = "midpoint_too_stale"

    trade_ts = _end_ts(trade) if trade is not None else None
    trade_close = _close(trade, "price") if trade is not None else None
    if trade_close is not None and not (0 <= trade_close <= 1):
        raise SmokeValidationError(f"invalid trade close for {ticker}")
    trade_age = _age(target_ts, trade_ts)
    trade_reason = "" if trade_close is not None else "no_trade"
    if trade_close is not None and trade_age is not None and trade_age > 60:
        trade_reason = "trade_too_stale"

    return {
        "market_ticker": ticker,
        "target_time": _iso_unix(target_ts),
        "candle_count": len(rows),
        "midpoint_status": "available" if midpoint is not None else "unavailable",
        "midpoint_reason": midpoint_reason,
        "yes_bid": bid,
        "yes_ask": ask,
        "midpoint": midpoint,
        "spread": spread,
        "midpoint_observation_time": _iso_unix(quote_ts),
        "midpoint_staleness_minutes": quote_age,
        "midpoint_within_15m": bool(midpoint is not None and quote_age is not None and quote_age <= 15),
        "midpoint_within_60m": bool(midpoint is not None and quote_age is not None and quote_age <= 60),
        "trade_status": "available" if trade_close is not None else "unavailable",
        "trade_reason": trade_reason,
        "trade_close": trade_close,
        "trade_observation_time": _iso_unix(trade_ts),
        "trade_staleness_minutes": trade_age,
        "trade_within_15m": bool(trade_close is not None and trade_age is not None and trade_age <= 15),
        "trade_within_60m": bool(trade_close is not None and trade_age is not None and trade_age <= 60),
        "previous_trade_used": False,
    }


def percentile(values: Sequence[float], probability: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def spread_diagnostics(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    values = [float(row["spread"]) for row in rows if row.get("spread") is not None]
    count = len(values)
    return {
        "observation_count": count,
        "median": percentile(values, 0.5),
        "p75": percentile(values, 0.75),
        "p90": percentile(values, 0.9),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "maximum": max(values) if values else None,
        "fraction_gt_0_02": sum(value > 0.02 for value in values) / count if count else None,
        "fraction_gt_0_05": sum(value > 0.05 for value in values) / count if count else None,
        "fraction_gt_0_10": sum(value > 0.10 for value in values) / count if count else None,
        "fraction_gt_0_20": sum(value > 0.20 for value in values) / count if count else None,
    }


def decision_counts(rows: Iterable[Mapping[str, Any]], field: str) -> dict[str, int]:
    return dict(sorted(Counter(str(row.get(field) or "") for row in rows).items()))


def canonical_report_hash(report: Mapping[str, Any]) -> str:
    return sha256_bytes(canonical_json(dict(report)))
