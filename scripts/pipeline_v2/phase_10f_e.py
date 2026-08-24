"""Pure outcome-blind classification and weighting helpers for Phase 10F-E."""

from __future__ import annotations

from collections import Counter, defaultdict
import math
from typing import Any, Mapping, Sequence


class PriceFreezeError(RuntimeError):
    pass


def classify_price_observability(row: Mapping[str, Any]) -> dict[str, Any]:
    """Assign explicit, non-adaptive midpoint and trade observability statuses."""
    success = bool(row.get("request_success"))
    candles = int(row.get("candle_count") or 0)
    if not success:
        midpoint_status = trade_status = "api_or_data_failure"
    elif candles == 0:
        midpoint_status = trade_status = "no_pre_target_candle"
    else:
        missing_bid = bool(row.get("missing_bid"))
        missing_ask = bool(row.get("missing_ask"))
        if missing_bid and missing_ask:
            midpoint_status = "missing_bid_and_ask"
        elif missing_bid:
            midpoint_status = "missing_bid"
        elif missing_ask:
            midpoint_status = "missing_ask"
        elif bool(row.get("midpoint_within_15m")):
            midpoint_status = "usable_midpoint_15m"
        elif bool(row.get("midpoint_within_60m")):
            midpoint_status = "usable_midpoint_60m_only"
        elif row.get("midpoint") is not None:
            midpoint_status = "midpoint_too_stale"
        else:
            midpoint_status = "missing_bid_and_ask"

        if bool(row.get("trade_within_15m")):
            trade_status = "usable_trade_15m"
        elif bool(row.get("trade_within_60m")):
            trade_status = "usable_trade_60m_only"
        elif row.get("trade_close") is None:
            trade_status = "no_trade"
        else:
            trade_status = "trade_too_stale"
    return {
        "midpoint_observability_status": midpoint_status,
        "trade_observability_status": trade_status,
        "api_or_data_failure": midpoint_status == "api_or_data_failure",
        "no_pre_target_candle": midpoint_status == "no_pre_target_candle",
        "missing_bid": midpoint_status in {"missing_bid", "missing_bid_and_ask"},
        "missing_ask": midpoint_status in {"missing_ask", "missing_bid_and_ask"},
        "midpoint_too_stale": midpoint_status == "midpoint_too_stale",
        "no_trade": trade_status == "no_trade",
        "trade_too_stale": trade_status == "trade_too_stale",
    }


def kish_ess(weights: Sequence[float]) -> float:
    values = [float(value) for value in weights if float(value) > 0]
    denominator = sum(value * value for value in values)
    if not values or denominator <= 0:
        return 0.0
    return sum(values) ** 2 / denominator


def sample_metrics(rows: Sequence[Mapping[str, Any]], *, flag: str) -> dict[str, Any]:
    usable = [row for row in rows if bool(row.get(flag))]
    families: dict[tuple[str, str], float] = defaultdict(float)
    for row in usable:
        identity = (str(row["family_id"]), str(row["family_id_source"]))
        families[identity] += float(row["family_weight_raw"])
    family_denominator = sum(float(row["family_weight_raw"]) for row in rows)
    contract_denominator = sum(float(row["contract_weight_raw"]) for row in rows)
    return {
        "usable_contracts": len(usable),
        "usable_unique_families": len(families),
        "unweighted_contract_coverage": len(usable) / len(rows) if rows else None,
        "family_target_weighted_coverage": (
            sum(float(row["family_weight_raw"]) for row in usable) / family_denominator
            if family_denominator
            else None
        ),
        "contract_target_weighted_coverage": (
            sum(float(row["contract_weight_raw"]) for row in usable)
            / contract_denominator
            if contract_denominator
            else None
        ),
        "family_weighted_ess": kish_ess(list(families.values())),
        "contract_weighted_ess": kish_ess(
            [float(row["contract_weight_raw"]) for row in usable]
        ),
    }


def grouped_metrics(
    rows: Sequence[Mapping[str, Any]], *, flag: str, field: str
) -> dict[str, Any]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[str(row.get(field) or "[missing]")].append(row)
    return {
        key: {"sampled_contracts": len(group), **sample_metrics(group, flag=flag)}
        for key, group in sorted(groups.items())
    }


def distribution(values: Sequence[float]) -> dict[str, Any]:
    ordered = sorted(float(value) for value in values if math.isfinite(float(value)))

    def percentile(probability: float) -> float | None:
        if not ordered:
            return None
        position = probability * (len(ordered) - 1)
        lower = math.floor(position)
        upper = math.ceil(position)
        if lower == upper:
            return ordered[lower]
        fraction = position - lower
        return ordered[lower] * (1 - fraction) + ordered[upper] * fraction

    return {
        "count": len(ordered),
        "median": percentile(0.50),
        "p75": percentile(0.75),
        "p90": percentile(0.90),
        "p95": percentile(0.95),
        "p99": percentile(0.99),
        "maximum": max(ordered) if ordered else None,
        **{
            f"fraction_gt_{label}": (
                sum(value > threshold for value in ordered) / len(ordered)
                if ordered
                else None
            )
            for threshold, label in (
                (0.02, "0_02"),
                (0.05, "0_05"),
                (0.10, "0_10"),
                (0.20, "0_20"),
            )
        },
    }


def attrition_counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    midpoint = Counter(str(row["midpoint_observability_status"]) for row in rows)
    trade = Counter(str(row["trade_observability_status"]) for row in rows)
    flags = (
        "api_or_data_failure",
        "no_pre_target_candle",
        "missing_bid",
        "missing_ask",
        "midpoint_too_stale",
        "no_trade",
        "trade_too_stale",
    )
    return {
        "midpoint_status_counts": dict(sorted(midpoint.items())),
        "trade_status_counts": dict(sorted(trade.items())),
        "explicit_flag_counts": {
            flag: sum(bool(row.get(flag)) for row in rows) for flag in flags
        },
    }
