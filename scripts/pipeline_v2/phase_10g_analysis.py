"""Pure weighted-estimation and bootstrap helpers for Phase 10G."""

from __future__ import annotations

from collections import defaultdict
import hashlib
from typing import Any, Mapping, Sequence

import numpy as np

from scripts.common.probability_utils import probability_bin
from scripts.pipeline_v2.phase_10f_e import kish_ess


BOOTSTRAP_SEED = "phase-10g-stratified-family-cluster-bootstrap-v1"
BOOTSTRAP_REPLICATES = 10_000
BOOTSTRAP_BATCH_SIZE = 250
WEIGHT_SYSTEMS = {
    "family_target": "family_weight_raw",
    "contract_target": "contract_weight_raw",
}
SAMPLE_FLAGS = {
    "primary_midpoint_15m": "midpoint_within_15m",
    "robustness_midpoint_60m": "midpoint_within_60m",
    "robustness_trade_close_15m": "trade_within_15m",
    "robustness_trade_close_60m": "trade_within_60m",
}


class OutcomeAnalysisError(RuntimeError):
    pass


def family_identity(row: Mapping[str, Any]) -> tuple[str, str]:
    return str(row["family_id"]), str(row["family_id_source"])


def _price(row: Mapping[str, Any], sample_name: str) -> float:
    field = "midpoint" if "midpoint" in sample_name else "trade_close"
    value = row.get(field)
    if value is None or not 0 <= float(value) <= 1:
        raise OutcomeAnalysisError(f"{sample_name} has an invalid price")
    return float(value)


def _resolved_rows(
    rows: Sequence[Mapping[str, Any]], *, sample_name: str
) -> list[Mapping[str, Any]]:
    flag = SAMPLE_FLAGS[sample_name]
    return [
        row
        for row in rows
        if bool(row.get(flag)) and row.get("binary_resolution_outcome") in {0, 1}
    ]


def _weight_metrics(
    rows: Sequence[Mapping[str, Any]], weight_field: str
) -> dict[str, Any]:
    weights = [float(row[weight_field]) for row in rows]
    family_weights: dict[tuple[str, str], float] = defaultdict(float)
    for row, weight in zip(rows, weights):
        family_weights[family_identity(row)] += weight
    return {
        "weight_sum": sum(weights),
        "contract_weighted_ess": kish_ess(weights),
        "family_aggregated_weight_ess": kish_ess(list(family_weights.values())),
    }


def weighted_estimate(
    rows: Sequence[Mapping[str, Any]],
    *,
    sample_name: str,
    weight_field: str,
    require_contrast: bool = True,
) -> dict[str, Any]:
    selected = _resolved_rows(rows, sample_name=sample_name)
    if not selected:
        raise OutcomeAnalysisError(f"{sample_name} has no resolved observations")
    weights = [float(row[weight_field]) for row in selected]
    prices = [_price(row, sample_name) for row in selected]
    outcomes = [int(row["binary_resolution_outcome"]) for row in selected]
    denominator = sum(weights)

    def mean(values: Sequence[float]) -> float:
        return (
            sum(weight * value for weight, value in zip(weights, values)) / denominator
        )

    gaps = [outcome - price for outcome, price in zip(outcomes, prices)]
    brier = [(outcome - price) ** 2 for outcome, price in zip(outcomes, prices)]
    result = {
        "resolved_contracts": len(selected),
        "resolved_families": len({family_identity(row) for row in selected}),
        **_weight_metrics(selected, weight_field),
        "weighted_mean_price": mean(prices),
        "weighted_yes_rate": mean(outcomes),
        "weighted_calibration_gap": mean(gaps),
        "weighted_brier_score": mean(brier),
    }
    tails: dict[str, dict[str, Any]] = {}
    for label, predicate in (
        ("longshot_p_lt_0_20", lambda value: value < 0.20),
        ("favorite_p_gte_0_80", lambda value: value >= 0.80),
    ):
        group = [row for row in selected if predicate(_price(row, sample_name))]
        group_weights = [float(row[weight_field]) for row in group]
        group_gaps = [
            int(row["binary_resolution_outcome"]) - _price(row, sample_name)
            for row in group
        ]
        if not group or sum(group_weights) <= 0:
            if not require_contrast:
                result["longshot_favorite_contrast"] = {
                    "definition": "gap(P<0.20) - gap(P>=0.80)",
                    "estimate": None,
                    "components": tails,
                    "available": False,
                }
                return result
            raise OutcomeAnalysisError(f"{sample_name} lacks {label} support")
        tails[label] = {
            "contracts": len(group),
            "families": len({family_identity(row) for row in group}),
            **_weight_metrics(group, weight_field),
            "weighted_calibration_gap": sum(
                weight * gap for weight, gap in zip(group_weights, group_gaps)
            )
            / sum(group_weights),
        }
    result["longshot_favorite_contrast"] = {
        "definition": "gap(P<0.20) - gap(P>=0.80)",
        "estimate": (
            tails["longshot_p_lt_0_20"]["weighted_calibration_gap"]
            - tails["favorite_p_gte_0_80"]["weighted_calibration_gap"]
        ),
        "components": tails,
        "available": True,
    }
    return result


def calibration_bins(
    rows: Sequence[Mapping[str, Any]], *, weight_field: str
) -> list[dict[str, Any]]:
    selected = _resolved_rows(rows, sample_name="primary_midpoint_15m")
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in selected:
        groups[probability_bin(float(row["midpoint"]))].append(row)
    output = []
    for label, group in sorted(groups.items()):
        weights = [float(row[weight_field]) for row in group]
        denominator = sum(weights)
        prices = [float(row["midpoint"]) for row in group]
        outcomes = [int(row["binary_resolution_outcome"]) for row in group]
        gaps = [outcome - price for outcome, price in zip(outcomes, prices)]
        metrics = _weight_metrics(group, weight_field)
        families = len({family_identity(row) for row in group})
        output.append(
            {
                "probability_bin": label,
                "resolved_contracts": len(group),
                "resolved_families": families,
                **metrics,
                "weighted_mean_price": sum(
                    weight * price for weight, price in zip(weights, prices)
                )
                / denominator,
                "weighted_yes_rate": sum(
                    weight * outcome for weight, outcome in zip(weights, outcomes)
                )
                / denominator,
                "weighted_calibration_gap": sum(
                    weight * gap for weight, gap in zip(weights, gaps)
                )
                / denominator,
                "support_gate_passed": (
                    families >= 100 and metrics["family_aggregated_weight_ess"] >= 100
                ),
            }
        )
    if [row["probability_bin"] for row in output] != [
        f"{index / 10:.1f}-{(index + 1) / 10:.1f}" for index in range(10)
    ]:
        raise OutcomeAnalysisError("primary calibration decile coverage changed")
    return output


def _interval(values: np.ndarray) -> dict[str, Any]:
    finite = values[np.isfinite(values)]
    if finite.size != values.size or finite.size == 0:
        raise OutcomeAnalysisError("bootstrap produced a non-finite estimate")
    lower, upper = np.quantile(finite, [0.025, 0.975], method="linear")
    nonpositive = int(np.count_nonzero(finite <= 0))
    nonnegative = int(np.count_nonzero(finite >= 0))
    tail = min(1.0, 2 * min((nonpositive + 1), (nonnegative + 1)) / (finite.size + 1))
    return {
        "replicates": int(finite.size),
        "ci_method": "two-sided_95_percentile",
        "ci_lower": float(lower),
        "ci_upper": float(upper),
        "two_sided_bootstrap_tail_probability_plus_one": float(tail),
    }


def bootstrap_intervals(
    rows: Sequence[Mapping[str, Any]],
    *,
    replicates: int = BOOTSTRAP_REPLICATES,
    batch_size: int = BOOTSTRAP_BATCH_SIZE,
) -> dict[str, Any]:
    """Stratified family-cluster bootstrap for all frozen estimands."""
    if replicates <= 0 or batch_size <= 0:
        raise OutcomeAnalysisError("bootstrap sizes must be positive")
    family_rows: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        family_rows[family_identity(row)].append(row)
    families = sorted(
        family_rows,
        key=lambda identity: int(family_rows[identity][0]["family_sample_index"]),
    )
    if len(families) != 5_000:
        raise OutcomeAnalysisError("bootstrap family frame changed")
    family_index = {identity: index for index, identity in enumerate(families)}
    strata: dict[str, list[int]] = defaultdict(list)
    for identity, index in family_index.items():
        first = family_rows[identity][0]
        strata[f"{first['anchor_month']}|{first['family_size_bin']}"].append(index)

    column_names: list[str] = []
    for sample_name in SAMPLE_FLAGS:
        for weight_name in WEIGHT_SYSTEMS:
            prefix = f"{sample_name}|{weight_name}"
            column_names.extend(
                f"{prefix}|{suffix}"
                for suffix in (
                    "den",
                    "gap_num",
                    "long_den",
                    "long_gap_num",
                    "favorite_den",
                    "favorite_gap_num",
                )
            )
    for label in (f"{index / 10:.1f}-{(index + 1) / 10:.1f}" for index in range(10)):
        column_names.extend(
            (
                f"primary_midpoint_15m|family_target|bin|{label}|den",
                f"primary_midpoint_15m|family_target|bin|{label}|gap_num",
            )
        )
    column_index = {name: index for index, name in enumerate(column_names)}
    contributions = np.zeros((len(families), len(column_names)), dtype=np.float64)

    for row in rows:
        outcome = row.get("binary_resolution_outcome")
        if outcome not in {0, 1}:
            continue
        family = family_index[family_identity(row)]
        for sample_name, flag in SAMPLE_FLAGS.items():
            if not bool(row.get(flag)):
                continue
            price = _price(row, sample_name)
            gap = int(outcome) - price
            for weight_name, weight_field in WEIGHT_SYSTEMS.items():
                weight = float(row[weight_field])
                prefix = f"{sample_name}|{weight_name}"
                contributions[family, column_index[f"{prefix}|den"]] += weight
                contributions[family, column_index[f"{prefix}|gap_num"]] += weight * gap
                if price < 0.20:
                    contributions[family, column_index[f"{prefix}|long_den"]] += weight
                    contributions[family, column_index[f"{prefix}|long_gap_num"]] += (
                        weight * gap
                    )
                if price >= 0.80:
                    contributions[
                        family, column_index[f"{prefix}|favorite_den"]
                    ] += weight
                    contributions[
                        family, column_index[f"{prefix}|favorite_gap_num"]
                    ] += (weight * gap)
            if sample_name == "primary_midpoint_15m":
                label = probability_bin(price)
                prefix = f"{sample_name}|family_target|bin|{label}"
                weight = float(row[WEIGHT_SYSTEMS["family_target"]])
                contributions[family, column_index[f"{prefix}|den"]] += weight
                contributions[family, column_index[f"{prefix}|gap_num"]] += weight * gap

    derived_names = []
    for sample_name in SAMPLE_FLAGS:
        for weight_name in WEIGHT_SYSTEMS:
            derived_names.extend(
                (
                    f"{sample_name}|{weight_name}|gap",
                    f"{sample_name}|{weight_name}|contrast",
                )
            )
    derived_names.extend(
        f"primary_midpoint_15m|family_target|bin|{index / 10:.1f}-{(index + 1) / 10:.1f}|gap"
        for index in range(10)
    )
    bootstrap = {name: np.empty(replicates, dtype=np.float64) for name in derived_names}
    seed = int.from_bytes(hashlib.sha256(BOOTSTRAP_SEED.encode()).digest()[:8], "big")
    rng = np.random.Generator(np.random.PCG64(seed))
    stratum_indices = [
        np.asarray(indices, dtype=np.int64) for _, indices in sorted(strata.items())
    ]

    for start in range(0, replicates, batch_size):
        stop = min(start + batch_size, replicates)
        size = stop - start
        totals = np.zeros((size, len(column_names)), dtype=np.float64)
        for indices in stratum_indices:
            counts = rng.multinomial(
                len(indices), np.full(len(indices), 1 / len(indices)), size=size
            )
            totals += counts @ contributions[indices]
        for sample_name in SAMPLE_FLAGS:
            for weight_name in WEIGHT_SYSTEMS:
                prefix = f"{sample_name}|{weight_name}"
                den = totals[:, column_index[f"{prefix}|den"]]
                gap = totals[:, column_index[f"{prefix}|gap_num"]] / den
                long_gap = (
                    totals[:, column_index[f"{prefix}|long_gap_num"]]
                    / totals[:, column_index[f"{prefix}|long_den"]]
                )
                favorite_gap = (
                    totals[:, column_index[f"{prefix}|favorite_gap_num"]]
                    / totals[:, column_index[f"{prefix}|favorite_den"]]
                )
                bootstrap[f"{prefix}|gap"][start:stop] = gap
                bootstrap[f"{prefix}|contrast"][start:stop] = long_gap - favorite_gap
        for index in range(10):
            label = f"{index / 10:.1f}-{(index + 1) / 10:.1f}"
            prefix = f"primary_midpoint_15m|family_target|bin|{label}"
            bootstrap[f"{prefix}|gap"][start:stop] = (
                totals[:, column_index[f"{prefix}|gap_num"]]
                / totals[:, column_index[f"{prefix}|den"]]
            )
    return {
        "seed": BOOTSTRAP_SEED,
        "generator": "numpy.random.PCG64",
        "replicates": replicates,
        "resampling_unit": "family_cluster",
        "strata": "anchor_month_x_family_size_bin",
        "stratum_count": len(strata),
        "intervals": {name: _interval(values) for name, values in bootstrap.items()},
    }
