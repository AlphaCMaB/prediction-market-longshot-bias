"""Pure Kalshi candlestick selection and price extraction."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Iterable, Mapping

from scripts.common.probability_utils import is_valid_probability, probability_bin, safe_float
from scripts.common.time_utils import format_iso_utc, parse_iso_utc


MAIN_STALENESS_MINUTES = 15.0
ROBUSTNESS_STALENESS_MINUTES = 60.0


def _unix_timestamp(value: Any) -> int | None:
    if isinstance(value, datetime):
        parsed = value if value.tzinfo else value.replace(tzinfo=timezone.utc)
        return int(parsed.timestamp())
    if isinstance(value, str) and not value.strip().lstrip("-").isdigit():
        parsed = parse_iso_utc(value)
        return int(parsed.timestamp()) if parsed else None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def select_latest_at_or_before(
    candlesticks: Iterable[Mapping[str, Any]], target: Any
) -> dict[str, Any] | None:
    """Return the latest candle whose end timestamp is not after target."""
    target_ts = _unix_timestamp(target)
    if target_ts is None:
        return None
    eligible: list[tuple[int, dict[str, Any]]] = []
    for source in candlesticks:
        end_ts = _unix_timestamp(source.get("end_period_ts"))
        if end_ts is not None and end_ts <= target_ts:
            eligible.append((end_ts, dict(source)))
    return max(eligible, key=lambda item: item[0])[1] if eligible else None


def _nested_float(candle: Mapping[str, Any], parent: str, names: tuple[str, ...]) -> float | None:
    value = candle.get(parent)
    if not isinstance(value, Mapping):
        return None
    for name in names:
        parsed = safe_float(value.get(name))
        if parsed is not None:
            return parsed
    return None


def extract_price_fields(candle: Mapping[str, Any]) -> dict[str, Any]:
    yes_bid = _nested_float(candle, "yes_bid", ("close_dollars", "close"))
    yes_ask = _nested_float(candle, "yes_ask", ("close_dollars", "close"))
    trade_close = _nested_float(candle, "price", ("close_dollars", "close"))
    previous_trade = _nested_float(candle, "price", ("previous_dollars", "previous"))
    midpoint = (yes_bid + yes_ask) / 2.0 if yes_bid is not None and yes_ask is not None else None

    if midpoint is not None:
        selected, source = midpoint, "yes_bid_ask_midpoint"
    elif trade_close is not None:
        selected, source = trade_close, "trade_close"
    elif previous_trade is not None:
        selected, source = previous_trade, "previous_trade"
    else:
        selected, source = None, ""

    return {
        "yes_bid": yes_bid,
        "yes_ask": yes_ask,
        "yes_midpoint": midpoint,
        "trade_close": trade_close,
        "previous_trade": previous_trade,
        "p_hat": selected,
        "price_source": source,
    }


def staleness_minutes(target: Any, candle_end: Any) -> float | None:
    target_ts = _unix_timestamp(target)
    candle_ts = _unix_timestamp(candle_end)
    if target_ts is None or candle_ts is None or candle_ts > target_ts:
        return None
    return (target_ts - candle_ts) / 60.0


def staleness_bucket(minutes: float | None) -> str:
    if minutes is None:
        return "missing"
    if minutes <= 5:
        return "0-5m"
    if minutes <= 15:
        return "5-15m"
    if minutes <= 60:
        return "15-60m"
    if minutes <= 180:
        return "1-3h"
    if minutes <= 360:
        return "3-6h"
    return ">6h"


def build_snapshot(
    candlestick_rows: Iterable[Mapping[str, Any]],
    target: Any,
    *,
    main_staleness_minutes: float = MAIN_STALENESS_MINUTES,
    robustness_staleness_minutes: float = ROBUSTNESS_STALENESS_MINUTES,
) -> dict[str, Any]:
    """Select and normalize one snapshot with explicit eligibility fields."""
    candle = select_latest_at_or_before(candlestick_rows, target)
    if candle is None:
        return {
            "snapshot_status": "missing",
            "snapshot_reason": "no_candlestick_at_or_before_target",
            "snapshot_time": "",
            "snapshot_staleness_minutes": None,
            "staleness_bucket": "missing",
            "main_specification_eligible": False,
            "robustness_specification_eligible": False,
            "p_hat": None,
            "price_source": "",
            "probability_bin": "missing",
        }

    end_ts = _unix_timestamp(candle.get("end_period_ts"))
    stale = staleness_minutes(target, end_ts)
    prices = extract_price_fields(candle)
    probability = prices["p_hat"]
    valid = is_valid_probability(probability)
    status = "ok" if valid else "unusable"
    reason = "" if valid else ("no_usable_price" if probability is None else "invalid_probability")
    if not valid:
        probability = None

    return {
        "snapshot_status": status,
        "snapshot_reason": reason,
        "snapshot_time": format_iso_utc(datetime.fromtimestamp(end_ts, tz=timezone.utc)) if end_ts is not None else "",
        "snapshot_staleness_minutes": stale,
        "staleness_bucket": staleness_bucket(stale),
        "main_specification_eligible": bool(valid and stale is not None and stale <= main_staleness_minutes),
        "robustness_specification_eligible": bool(valid and stale is not None and stale <= robustness_staleness_minutes),
        **prices,
        "p_hat": probability,
        "probability_bin": probability_bin(probability),
    }
