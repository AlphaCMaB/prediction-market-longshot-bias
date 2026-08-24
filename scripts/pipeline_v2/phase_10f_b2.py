"""Pure routing, sampling, and typed normalization for Phase 10F-B2."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
import hashlib
import math
from typing import Any, Iterable, Mapping, Sequence

from scripts.common.probability_utils import safe_float
from scripts.common.time_utils import format_iso_utc, parse_iso_utc
from scripts.pipeline_v2.kalshi_metadata_cache import canonical_json
from scripts.pipeline_v2.phase_10f_smoke import percentile


SCHEMA_VERSION = "phase-10f-b2-historical-validation-v1"
SAMPLE_SEED = "phase-10f-b2-200-ticker-stratified-v1"
SAMPLE_SIZE = 200
MAX_NETWORK_REQUESTS = 202
HISTORICAL_ROUTE = "historical_per_market"
LIVE_ROUTE = "live_single_ticker_batch"
SECOND_TICKER_QUOTAS = {
    "Crypto": 28,
    "Financials": 23,
    "Climate and Weather": 14,
}


class B2ValidationError(RuntimeError):
    pass


@dataclass(frozen=True)
class TickerCandidate:
    family_id: str
    family_id_source: str
    event_ticker: str
    rule: str
    category: str
    timing_structure: str
    target_time: str
    ticker: str
    family_market_count: int
    settlement_time: str

    @property
    def family_identity(self) -> tuple[str, str]:
        return self.family_id, self.family_id_source

    @property
    def target_ts(self) -> int:
        parsed = parse_iso_utc(self.target_time)
        if parsed is None:
            raise B2ValidationError("sample target is not an exact timestamp")
        return int(parsed.timestamp())


def deterministic_rank(*parts: str) -> str:
    material = "\x00".join((SAMPLE_SEED, *parts))
    return hashlib.sha256(material.encode("utf-8")).hexdigest()


def family_size_stratum(count: int) -> str:
    if count <= 1:
        return "single_contract"
    if count <= 25:
        return "low_contract_count"
    if count <= 100:
        return "medium_contract_count"
    return "high_contract_count"


def select_ticker_sample(
    candidates: Iterable[TickerCandidate],
) -> list[TickerCandidate]:
    by_family: dict[tuple[str, str], list[TickerCandidate]] = {}
    for candidate in candidates:
        by_family.setdefault(candidate.family_identity, []).append(candidate)
    if len(by_family) != 135:
        raise B2ValidationError(
            f"expected 135 network families, found {len(by_family)}"
        )

    selected: list[TickerCandidate] = []
    remaining_by_family: dict[tuple[str, str], list[TickerCandidate]] = {}
    for identity in sorted(by_family):
        ranked = sorted(
            by_family[identity],
            key=lambda row: deterministic_rank(
                "first", row.family_id, row.family_id_source, row.ticker
            ),
        )
        selected.append(ranked[0])
        remaining_by_family[identity] = ranked[1:]

    for category, quota in SECOND_TICKER_QUOTAS.items():
        eligible_families = [
            identity
            for identity, values in remaining_by_family.items()
            if values and values[0].category == category
        ]
        ranked_families = sorted(
            eligible_families,
            key=lambda identity: (
                # Interleave target month and size stratum through the hash
                # while preventing any family from contributing over two rows.
                deterministic_rank(
                    "second-family",
                    remaining_by_family[identity][0].target_time[:7],
                    family_size_stratum(
                        remaining_by_family[identity][0].family_market_count
                    ),
                    *identity,
                ),
                identity,
            ),
        )
        if len(ranked_families) < quota:
            raise B2ValidationError(
                f"insufficient second-ticker families in {category}"
            )
        for identity in ranked_families[:quota]:
            second = min(
                remaining_by_family[identity],
                key=lambda row: deterministic_rank(
                    "second", row.family_id, row.family_id_source, row.ticker
                ),
            )
            selected.append(second)

    if (
        len(selected) != SAMPLE_SIZE
        or len({row.ticker for row in selected}) != SAMPLE_SIZE
    ):
        raise B2ValidationError(
            "deterministic ticker sample is not exactly 200 unique rows"
        )
    if (
        max(
            sum(row.family_identity == identity for row in selected)
            for identity in by_family
        )
        > 2
    ):
        raise B2ValidationError("large family dominates the bounded sample")
    return sorted(selected, key=lambda row: (row.family_identity, row.ticker))


def sample_identity(sample: Sequence[TickerCandidate]) -> str:
    projection = [
        {
            "family_id": row.family_id,
            "family_id_source": row.family_id_source,
            "ticker": row.ticker,
            "target_time": row.target_time,
        }
        for row in sample
    ]
    return hashlib.sha256(canonical_json(projection)).hexdigest()


def route_for_settlement(settlement_time: str, market_cutoff: str) -> str:
    settlement = parse_iso_utc(settlement_time)
    cutoff = parse_iso_utc(market_cutoff)
    if settlement is None:
        raise B2ValidationError("market lacks exact settlement timestamp for routing")
    if cutoff is None:
        raise B2ValidationError("historical cutoff lacks exact market_settled_ts")
    return HISTORICAL_ROUTE if settlement < cutoff else LIVE_ROUTE


def _typed_value(value: Any, *, field: str) -> float | None:
    if value is None:
        return None
    parsed = safe_float(value)
    if parsed is None or not math.isfinite(parsed) or not 0 <= parsed <= 1:
        raise B2ValidationError(f"invalid probability value in {field}")
    return parsed


def _distribution_close(
    value: Any, *, variant: str, field: str, nullable: bool
) -> float | None:
    if not isinstance(value, Mapping):
        raise B2ValidationError(f"{field} is not an object")
    expected = "close" if variant == "historical_legacy_close" else "close_dollars"
    ambiguous = "close_dollars" if expected == "close" else "close"
    if expected not in value or ambiguous in value:
        raise B2ValidationError(f"unknown or ambiguous {variant} schema in {field}")
    result = _typed_value(value.get(expected), field=f"{field}.{expected}")
    if result is None and not nullable:
        raise B2ValidationError(f"required quote close missing in {field}")
    return result


def _live_quote_close(value: Any, *, field: str) -> float | None:
    """Parse a live quote side without accepting a legacy representation."""
    if not isinstance(value, Mapping):
        raise B2ValidationError(f"{field} is not an object")
    if "close_dollars" not in value:
        raise B2ValidationError(f"unknown live quote schema in {field}")
    documented = _typed_value(
        value.get("close_dollars"), field=f"{field}.close_dollars"
    )
    if "close" in value:
        legacy = _typed_value(value.get("close"), field=f"{field}.close")
        if documented != legacy:
            raise B2ValidationError(f"ambiguous live quote schema in {field}")
    return documented


def _live_trade_close(value: Any) -> tuple[float | None, bool, str]:
    """Parse live trade close independently, never using previous-price fields."""
    if not isinstance(value, Mapping):
        return None, False, "trade_schema_unavailable"
    if "close_dollars" not in value or "close" in value:
        return None, False, "trade_schema_unavailable"
    result = _typed_value(value.get("close_dollars"), field="price.close_dollars")
    if result is None:
        return None, False, "no_trade_value"
    return result, True, ""


def normalize_candle(candle: Any, *, route: str) -> dict[str, Any]:
    if not isinstance(candle, Mapping):
        raise B2ValidationError("candlestick is not an object")
    try:
        end_ts = int(candle["end_period_ts"])
    except (KeyError, TypeError, ValueError) as exc:
        raise B2ValidationError("candlestick lacks integer end_period_ts") from exc
    if route == HISTORICAL_ROUTE:
        variant = "historical_legacy_close"
    elif route == LIVE_ROUTE:
        variant = "live_fixed_point_dollars"
    else:
        raise B2ValidationError("unknown candlestick route")
    if route == HISTORICAL_ROUTE:
        bid = _distribution_close(
            candle.get("yes_bid"), variant=variant, field="yes_bid", nullable=True
        )
        ask = _distribution_close(
            candle.get("yes_ask"), variant=variant, field="yes_ask", nullable=True
        )
        trade = _distribution_close(
            candle.get("price"), variant=variant, field="price", nullable=True
        )
        trade_valid = trade is not None
        trade_failure = "" if trade_valid else "no_trade_value"
    else:
        bid = _live_quote_close(candle.get("yes_bid"), field="yes_bid")
        ask = _live_quote_close(candle.get("yes_ask"), field="yes_ask")
        trade, trade_valid, trade_failure = _live_trade_close(candle.get("price"))
    if bid is not None and ask is not None and bid > ask:
        raise B2ValidationError("crossed YES quote")
    midpoint_valid = bid is not None and ask is not None
    if midpoint_valid:
        midpoint_failure = ""
    elif bid is None and ask is None:
        midpoint_failure = "missing_bid_and_ask"
    elif bid is None:
        midpoint_failure = "missing_bid"
    else:
        midpoint_failure = "missing_ask"
    return {
        "end_period_ts": end_ts,
        "yes_bid_close": bid,
        "yes_ask_close": ask,
        "trade_close": trade,
        "midpoint_valid": midpoint_valid,
        "trade_close_valid": trade_valid,
        "midpoint_failure_reason": midpoint_failure,
        "trade_failure_reason": trade_failure,
        "schema_variant": variant,
        "previous_trade_used": False,
    }


def normalize_response(
    payload: Any, *, route: str, ticker: str
) -> list[dict[str, Any]]:
    if not isinstance(payload, Mapping):
        raise B2ValidationError("candlestick response is not an object")
    if route == HISTORICAL_ROUTE:
        if set(payload) != {"ticker", "candlesticks"}:
            raise B2ValidationError("historical response schema changed")
        if payload.get("ticker") != ticker or not isinstance(
            payload.get("candlesticks"), list
        ):
            raise B2ValidationError("historical response ticker/schema mismatch")
        source = payload["candlesticks"]
    elif route == LIVE_ROUTE:
        if set(payload) != {"markets"} or not isinstance(payload.get("markets"), list):
            raise B2ValidationError("live batch response schema changed")
        markets = payload["markets"]
        if not markets:
            return []
        if len(markets) != 1 or markets[0].get("market_ticker") != ticker:
            raise B2ValidationError("live batch returned unexpected market identity")
        source = markets[0].get("candlesticks")
        if not isinstance(source, list):
            raise B2ValidationError("live candlestick list schema changed")
    else:
        raise B2ValidationError("unknown response route")
    rows = [normalize_candle(row, route=route) for row in source]
    timestamps = [row["end_period_ts"] for row in rows]
    if len(timestamps) != len(set(timestamps)):
        raise B2ValidationError("duplicate candle end_period_ts")
    return sorted(rows, key=lambda row: row["end_period_ts"])


def extract_observation(
    candles: Sequence[Mapping[str, Any]], *, target_ts: int
) -> dict[str, Any]:
    if any(int(row["end_period_ts"]) > target_ts for row in candles):
        raise B2ValidationError("post-target candle returned")
    latest = max(candles, key=lambda row: int(row["end_period_ts"]), default=None)
    bid = latest.get("yes_bid_close") if latest else None
    ask = latest.get("yes_ask_close") if latest else None
    quote_ts = int(latest["end_period_ts"]) if latest else None
    quote_age = (target_ts - quote_ts) / 60 if quote_ts is not None else None
    midpoint = (
        (float(bid) + float(ask)) / 2 if bid is not None and ask is not None else None
    )
    spread = float(ask) - float(bid) if midpoint is not None else None
    trades = [
        row
        for row in candles
        if bool(row.get("trade_close_valid")) and row.get("trade_close") is not None
    ]
    trade_row = max(trades, key=lambda row: int(row["end_period_ts"]), default=None)
    unavailable = [
        row
        for row in candles
        if row.get("trade_failure_reason") == "trade_schema_unavailable"
    ]
    latest_unavailable_ts = max(
        (int(row["end_period_ts"]) for row in unavailable), default=None
    )
    trade_ts = int(trade_row["end_period_ts"]) if trade_row else None
    trade_schema_unavailable = bool(
        latest_unavailable_ts is not None
        and (trade_ts is None or latest_unavailable_ts >= trade_ts)
    )
    if trade_schema_unavailable:
        trade = None
        trade_ts = None
        trade_failure_reason = "trade_schema_unavailable"
    else:
        trade = float(trade_row["trade_close"]) if trade_row else None
        trade_failure_reason = "" if trade_row else "no_trade"
    trade_age = (target_ts - trade_ts) / 60 if trade_ts is not None else None
    return {
        "candle_count": len(candles),
        "earliest_end_period_ts": candles[0]["end_period_ts"] if candles else None,
        "latest_end_period_ts": candles[-1]["end_period_ts"] if candles else None,
        "schema_variant": candles[0]["schema_variant"] if candles else "",
        "post_target_candle_count": 0,
        "duplicate_candle_count": 0,
        "missing_bid": bool(latest is not None and bid is None),
        "missing_ask": bool(latest is not None and ask is None),
        "midpoint_valid": bool(latest is not None and latest.get("midpoint_valid")),
        "midpoint_failure_reason": (
            str(latest.get("midpoint_failure_reason") or "")
            if latest is not None
            else "no_pre_target_candle"
        ),
        "yes_bid": bid,
        "yes_ask": ask,
        "midpoint": midpoint,
        "spread": spread,
        "midpoint_observation_time": _iso(quote_ts),
        "midpoint_staleness_minutes": quote_age,
        "midpoint_within_15m": bool(
            midpoint is not None and quote_age is not None and quote_age <= 15
        ),
        "midpoint_within_60m": bool(
            midpoint is not None and quote_age is not None and quote_age <= 60
        ),
        "trade_close": trade,
        "trade_close_valid": bool(trade is not None),
        "trade_failure_reason": trade_failure_reason,
        "trade_observation_time": _iso(trade_ts),
        "trade_staleness_minutes": trade_age,
        "trade_within_15m": bool(
            trade is not None and trade_age is not None and trade_age <= 15
        ),
        "trade_within_60m": bool(
            trade is not None and trade_age is not None and trade_age <= 60
        ),
        "previous_trade_used": False,
    }


def _iso(timestamp: int | None) -> str:
    if timestamp is None:
        return ""
    return format_iso_utc(datetime.fromtimestamp(timestamp, tz=timezone.utc))


def diagnostic_distribution(values: Sequence[float]) -> dict[str, Any]:
    count = len(values)
    return {
        "observation_count": count,
        "median": percentile(values, 0.5),
        "p75": percentile(values, 0.75),
        "p90": percentile(values, 0.90),
        "p95": percentile(values, 0.95),
        "p99": percentile(values, 0.99),
        "maximum": max(values) if values else None,
        **{
            f"fraction_gt_{str(threshold).replace('.', '_')}": (
                sum(value > threshold for value in values) / count if count else None
            )
            for threshold in (0.02, 0.05, 0.10, 0.20)
        },
    }
