"""Pure helpers for the exploratory Phase 10I execution analysis."""

from __future__ import annotations

from collections import defaultdict
from decimal import Decimal, ROUND_CEILING
import hashlib
from typing import Any, Callable, Mapping, Sequence

import numpy as np

from scripts.pipeline_v2.phase_10f_e import kish_ess
from scripts.pipeline_v2.phase_10g_analysis import family_identity


BOOTSTRAP_SEED = "phase-10i-exploratory-family-cluster-bootstrap-v1"
BOOTSTRAP_REPLICATES = 10_000
BOOTSTRAP_BATCH_SIZE = 250
TAIL_GROUPS = ("longshot", "favorite", "combined")
WEIGHT_SYSTEMS = {
    "family_target": "family_weight_raw",
    "contract_target": "contract_weight_raw",
}


class PhaseIAnalysisError(RuntimeError):
    """Raised when the exploratory analysis inputs or calculations are invalid."""


def fee_per_contract(price: float, contracts: int, rate: float) -> float:
    """Published schedule fee, including order-level cent rounding."""
    if contracts <= 0:
        raise ValueError("contracts must be positive")
    if not 0 <= price <= 1:
        raise ValueError("price must be in [0, 1]")
    if rate < 0:
        raise ValueError("fee rate must be nonnegative")
    raw = (
        Decimal(str(rate))
        * Decimal(contracts)
        * Decimal(str(price))
        * (Decimal(1) - Decimal(str(price)))
    )
    total = (raw * Decimal(100)).to_integral_value(rounding=ROUND_CEILING) / Decimal(
        100
    )
    return float(total / Decimal(contracts))


def strategy_group(midpoint: float) -> str | None:
    if midpoint < 0.20:
        return "longshot"
    if midpoint >= 0.80:
        return "favorite"
    return None


def execution_values(row: Mapping[str, Any]) -> dict[str, float | str]:
    """Derive taker and conditional-maker values for one resolved tail row."""
    midpoint = float(row["midpoint"])
    bid = float(row["yes_bid"])
    ask = float(row["yes_ask"])
    outcome = int(row["binary_resolution_outcome"])
    if not (0 <= bid <= midpoint <= ask <= 1):
        raise PhaseIAnalysisError("invalid bid/midpoint/ask ordering")
    if outcome not in {0, 1}:
        raise PhaseIAnalysisError("execution row is not binary resolved")
    group = strategy_group(midpoint)
    if group is None:
        raise PhaseIAnalysisError("execution row is not in a frozen tail")
    if group == "longshot":
        payout = float(1 - outcome)
        taker_entry = 1.0 - bid
        maker_entry = 1.0 - ask
    else:
        payout = float(outcome)
        taker_entry = ask
        maker_entry = bid
    values: dict[str, float | str] = {
        "strategy_group": group,
        "payout": payout,
        "taker_entry": taker_entry,
        "maker_entry": maker_entry,
        "taker_gross_profit": payout - taker_entry,
        "maker_gross_profit": payout - maker_entry,
    }
    for contracts in (1, 100):
        taker_fee = fee_per_contract(taker_entry, contracts, 0.07)
        maker_fee = fee_per_contract(maker_entry, contracts, 0.0175)
        values[f"taker_fee_c{contracts}"] = taker_fee
        values[f"taker_net_profit_c{contracts}"] = payout - taker_entry - taker_fee
        values[f"maker_fee_c{contracts}"] = maker_fee
        values[f"maker_net_profit_c{contracts}"] = payout - maker_entry - maker_fee
    return values


def weighted_execution_summary(
    rows: Sequence[Mapping[str, Any]], *, weight_field: str
) -> dict[str, Any]:
    if not rows:
        raise PhaseIAnalysisError("execution summary has no rows")
    weights = [float(row[weight_field]) for row in rows]
    if any(weight <= 0 for weight in weights):
        raise PhaseIAnalysisError("execution weights must be positive")
    denominator = sum(weights)

    def mean(field: str) -> float:
        return (
            sum(weight * float(row[field]) for weight, row in zip(weights, rows))
            / denominator
        )

    family_weights: dict[tuple[str, str], float] = defaultdict(float)
    for weight, row in zip(weights, rows):
        family_weights[family_identity(row)] += weight
    result = {
        "contracts": len(rows),
        "families": len(family_weights),
        "weight_sum": denominator,
        "contract_weighted_ess": kish_ess(weights),
        "family_aggregated_weight_ess": kish_ess(list(family_weights.values())),
        "weighted_payout_rate": mean("payout"),
        "weighted_taker_entry": mean("taker_entry"),
        "weighted_taker_gross_profit": mean("taker_gross_profit"),
        "weighted_maker_entry": mean("maker_entry"),
        "weighted_maker_gross_profit": mean("maker_gross_profit"),
    }
    for side in ("taker", "maker"):
        for contracts in (1, 100):
            profit_field = f"{side}_net_profit_c{contracts}"
            fee_field = f"{side}_fee_c{contracts}"
            weighted_profit = mean(profit_field)
            weighted_cost = mean(
                "taker_entry" if side == "taker" else "maker_entry"
            ) + mean(fee_field)
            result[f"weighted_{side}_fee_c{contracts}"] = mean(fee_field)
            result[f"weighted_{side}_net_profit_c{contracts}"] = weighted_profit
            result[f"weighted_{side}_capital_return_c{contracts}"] = (
                weighted_profit / weighted_cost if weighted_cost > 0 else None
            )
    result["weighted_taker_gross_capital_return"] = (
        result["weighted_taker_gross_profit"] / result["weighted_taker_entry"]
    )
    result["weighted_maker_gross_capital_return"] = (
        result["weighted_maker_gross_profit"] / result["weighted_maker_entry"]
    )
    return result


def quantile_cutpoints(values: Sequence[float]) -> tuple[float, float, float]:
    if not values:
        raise PhaseIAnalysisError("cannot calculate empty quantiles")
    result = np.quantile(np.asarray(values, dtype=float), [0.25, 0.5, 0.75])
    return tuple(float(value) for value in result)


def quantile_label(value: float, cutpoints: Sequence[float]) -> str:
    if len(cutpoints) != 3:
        raise ValueError("three quartile cutpoints are required")
    if value <= cutpoints[0]:
        return "Q1"
    if value <= cutpoints[1]:
        return "Q2"
    if value <= cutpoints[2]:
        return "Q3"
    return "Q4"


def _interval(values: np.ndarray) -> dict[str, Any]:
    if values.size == 0 or not np.all(np.isfinite(values)):
        raise PhaseIAnalysisError("bootstrap produced non-finite estimates")
    lower, upper = np.quantile(values, [0.025, 0.975], method="linear")
    nonpositive = int(np.count_nonzero(values <= 0))
    nonnegative = int(np.count_nonzero(values >= 0))
    tail = min(
        1.0,
        2 * min(nonpositive + 1, nonnegative + 1) / (values.size + 1),
    )
    return {
        "replicates": int(values.size),
        "ci_method": "two-sided_95_percentile",
        "ci_lower": float(lower),
        "ci_upper": float(upper),
        "two_sided_bootstrap_tail_probability_plus_one": float(tail),
    }


MetricSpec = tuple[Callable[[Mapping[str, Any]], bool], str, str]


def bootstrap_metric_intervals(
    frame_rows: Sequence[Mapping[str, Any]],
    metrics: Mapping[str, MetricSpec],
    *,
    contrasts: Mapping[str, tuple[str, str]] | None = None,
    replicates: int = BOOTSTRAP_REPLICATES,
    batch_size: int = BOOTSTRAP_BATCH_SIZE,
) -> dict[str, Any]:
    """Bootstrap weighted means over the unchanged 5,000-family frame."""
    if replicates <= 0 or batch_size <= 0:
        raise ValueError("bootstrap sizes must be positive")
    family_rows: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in frame_rows:
        family_rows[family_identity(row)].append(row)
    families = sorted(
        family_rows,
        key=lambda identity: int(family_rows[identity][0]["family_sample_index"]),
    )
    if len(families) != 5_000:
        raise PhaseIAnalysisError("bootstrap family frame changed")
    family_index = {identity: index for index, identity in enumerate(families)}
    strata: dict[str, list[int]] = defaultdict(list)
    for identity, index in family_index.items():
        first = family_rows[identity][0]
        strata[f"{first['anchor_month']}|{first['family_size_bin']}"].append(index)

    names = list(metrics)
    contributions = np.zeros((len(families), len(names) * 2), dtype=np.float64)
    contributing_families: dict[str, set[tuple[str, str]]] = {
        name: set() for name in names
    }
    for row in frame_rows:
        family = family_identity(row)
        index = family_index[family]
        for metric_index, (name, (predicate, value_field, weight_field)) in enumerate(
            metrics.items()
        ):
            if not predicate(row):
                continue
            weight = float(row[weight_field])
            value = float(row[value_field])
            contributions[index, 2 * metric_index] += weight * value
            contributions[index, 2 * metric_index + 1] += weight
            contributing_families[name].add(family)
    for name, families_for_metric in contributing_families.items():
        if not families_for_metric:
            raise PhaseIAnalysisError(f"bootstrap metric has no support: {name}")

    output = {name: np.empty(replicates, dtype=float) for name in names}
    contrast_arrays = {
        name: np.empty(replicates, dtype=float) for name in (contrasts or {})
    }
    seed = int.from_bytes(hashlib.sha256(BOOTSTRAP_SEED.encode()).digest()[:8], "big")
    rng = np.random.Generator(np.random.PCG64(seed))
    stratum_indices = [
        np.asarray(indices, dtype=np.int64) for _, indices in sorted(strata.items())
    ]
    for start in range(0, replicates, batch_size):
        stop = min(start + batch_size, replicates)
        size = stop - start
        totals = np.zeros((size, len(names) * 2), dtype=float)
        for indices in stratum_indices:
            counts = rng.multinomial(
                len(indices), np.full(len(indices), 1 / len(indices)), size=size
            )
            totals += counts @ contributions[indices]
        batch_values: dict[str, np.ndarray] = {}
        for metric_index, name in enumerate(names):
            denominator = totals[:, 2 * metric_index + 1]
            if np.any(denominator <= 0):
                raise PhaseIAnalysisError(
                    f"bootstrap replicate lost all support for {name}"
                )
            values = totals[:, 2 * metric_index] / denominator
            output[name][start:stop] = values
            batch_values[name] = values
        for name, (left, right) in (contrasts or {}).items():
            contrast_arrays[name][start:stop] = batch_values[left] - batch_values[right]
    return {
        "seed": BOOTSTRAP_SEED,
        "generator": "numpy.random.PCG64",
        "replicates": replicates,
        "resampling_unit": "family_cluster",
        "strata": "anchor_month_x_family_size_bin",
        "stratum_count": len(strata),
        "intervals": {name: _interval(values) for name, values in output.items()},
        "contrasts": {
            name: _interval(values) for name, values in contrast_arrays.items()
        },
    }
