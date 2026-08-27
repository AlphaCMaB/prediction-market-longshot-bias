"""Pure helpers for the final outcome-blind Phase 10F sample audit."""

from __future__ import annotations

from collections import Counter, defaultdict
import math
from typing import Any, Callable, Mapping, Sequence

from scripts.common.probability_utils import probability_bin
from scripts.common.time_utils import parse_iso_utc
from scripts.pipeline_v2.phase_10f_e import kish_ess


class FinalAuditError(RuntimeError):
    pass


def _close(left: float, right: float, *, tolerance: float = 1e-12) -> bool:
    return math.isclose(float(left), float(right), rel_tol=tolerance, abs_tol=tolerance)


def validate_sampling_design(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    """Reconstruct every inclusion probability and raw design weight."""
    if len(rows) != 11_573:
        raise FinalAuditError("frozen contract count changed")
    identities = [(str(row["family_id"]), str(row["family_id_source"])) for row in rows]
    tickers = [str(row["ticker"]) for row in rows]
    indices = [int(row["contract_sample_index"]) for row in rows]
    if len(set(tickers)) != len(tickers):
        raise FinalAuditError("duplicate ticker in frozen sample")
    if indices != list(range(1, len(rows) + 1)):
        raise FinalAuditError("contract sample indices changed")
    if len(set(identities)) != 5_000:
        raise FinalAuditError("frozen family count changed")
    per_family = Counter(identities)
    if max(per_family.values()) > 3:
        raise FinalAuditError("three-contract family cap changed")

    for row in rows:
        family = (str(row["family_id"]), str(row["family_id_source"]))
        population_contracts = int(row["eligible_contract_count"])
        sampled_contracts = int(row["sampled_contract_count"])
        population_families = int(row["stratum_family_count"])
        sampled_families = int(row["stratum_sampled_family_count"])
        if per_family[family] != sampled_contracts:
            raise FinalAuditError("within-family sampled count changed")
        pi_family = float(row["pi_family"])
        pi_given_family = float(row["pi_contract_given_family"])
        pi_contract = float(row["pi_contract"])
        expected = (
            sampled_families / population_families,
            sampled_contracts / population_contracts,
        )
        checks = (
            _close(pi_family, expected[0]),
            _close(pi_given_family, expected[1]),
            _close(pi_contract, pi_family * pi_given_family),
            _close(float(row["contract_weight_raw"]), 1 / pi_contract),
            _close(
                float(row["family_weight_raw"]),
                1 / (pi_contract * population_contracts),
            ),
        )
        if not all(checks):
            raise FinalAuditError("inclusion probability or raw weight changed")
    return {
        "passed": True,
        "contracts": len(rows),
        "families": len(set(identities)),
        "duplicate_tickers": len(tickers) - len(set(tickers)),
        "maximum_contracts_per_family": max(per_family.values()),
        "probability_rows_validated": len(rows),
    }


def validate_temporal_and_price_rules(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Recheck anchors, targets, price arithmetic, staleness, and no-fallback rules."""
    midpoint_rows = 0
    trade_rows = 0
    for row in rows:
        anchor = parse_iso_utc(row["verified_anchor_time"])
        target = parse_iso_utc(row["target_time"])
        opened = parse_iso_utc(row["market_open_time"])
        if anchor is None or target is None or opened is None:
            raise FinalAuditError("invalid frozen timestamp")
        if not _close((anchor - target).total_seconds(), 3600):
            raise FinalAuditError("target is not exactly one hour before anchor")
        if opened > target:
            raise FinalAuditError("market opened after frozen target")
        if int(row.get("post_target_candle_count") or 0) != 0:
            raise FinalAuditError("post-target candle entered frozen sample")
        if bool(row.get("previous_trade_used")):
            raise FinalAuditError("previous-price fallback entered frozen sample")

        midpoint = row.get("midpoint")
        bid = row.get("yes_bid")
        ask = row.get("yes_ask")
        midpoint_time = parse_iso_utc(row.get("midpoint_observation_time"))
        if midpoint is not None:
            midpoint_rows += 1
            if bid is None or ask is None or midpoint_time is None:
                raise FinalAuditError("midpoint lacks quote or observation time")
            if not (0 <= float(bid) <= float(ask) <= 1):
                raise FinalAuditError(
                    "YES quote is outside probability bounds or crossed"
                )
            if not _close(float(midpoint), (float(bid) + float(ask)) / 2):
                raise FinalAuditError("midpoint arithmetic changed")
            if not _close(float(row["spread"]), float(ask) - float(bid)):
                raise FinalAuditError("spread arithmetic changed")
            age = (target - midpoint_time).total_seconds() / 60
            if age < 0 or not _close(age, float(row["midpoint_staleness_minutes"])):
                raise FinalAuditError("midpoint timing or staleness changed")
            if bool(row["midpoint_within_15m"]) != (age <= 15):
                raise FinalAuditError("15-minute midpoint flag changed")
            if bool(row["midpoint_within_60m"]) != (age <= 60):
                raise FinalAuditError("60-minute midpoint flag changed")
        elif bool(row.get("midpoint_within_15m")) or bool(
            row.get("midpoint_within_60m")
        ):
            raise FinalAuditError("missing midpoint is marked usable")

        trade = row.get("trade_close")
        trade_time = parse_iso_utc(row.get("trade_observation_time"))
        if trade is not None:
            trade_rows += 1
            if trade_time is None or not 0 <= float(trade) <= 1:
                raise FinalAuditError("trade close lacks a valid value or time")
            age = (target - trade_time).total_seconds() / 60
            if age < 0 or not _close(age, float(row["trade_staleness_minutes"])):
                raise FinalAuditError("trade timing or staleness changed")
            if bool(row["trade_within_15m"]) != (age <= 15):
                raise FinalAuditError("15-minute trade flag changed")
            if bool(row["trade_within_60m"]) != (age <= 60):
                raise FinalAuditError("60-minute trade flag changed")
        elif bool(row.get("trade_within_15m")) or bool(row.get("trade_within_60m")):
            raise FinalAuditError("missing trade is marked usable")
    return {
        "passed": True,
        "rows_validated": len(rows),
        "midpoint_rows_validated": midpoint_rows,
        "trade_rows_validated": trade_rows,
        "post_target_rows": 0,
        "previous_price_fallback_rows": 0,
        "anchor_changes": 0,
    }


def validate_analysis_projection(
    normalized: Sequence[Mapping[str, Any]],
    analysis: Sequence[Mapping[str, Any]],
    *,
    flag: str,
    sample_name: str,
    analysis_fields: Sequence[str],
) -> dict[str, Any]:
    expected = [
        {
            **{field: row.get(field, "") for field in analysis_fields},
            "price_sample_name": sample_name,
        }
        for row in normalized
        if bool(row.get(flag))
    ]
    if list(analysis) != expected:
        raise FinalAuditError(
            f"{sample_name} is not the deterministic frozen projection"
        )
    tickers = [str(row["ticker"]) for row in analysis]
    if len(tickers) != len(set(tickers)):
        raise FinalAuditError(f"{sample_name} contains duplicate tickers")
    return {
        "passed": True,
        "sample_name": sample_name,
        "contracts": len(analysis),
        "families": len(
            {(str(row["family_id"]), str(row["family_id_source"])) for row in analysis}
        ),
        "duplicate_tickers": 0,
    }


def mutually_exclusive_attrition(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    midpoint = Counter(str(row["midpoint_observability_status"]) for row in rows)
    trade = Counter(str(row["trade_observability_status"]) for row in rows)
    if sum(midpoint.values()) != len(rows) or sum(trade.values()) != len(rows):
        raise FinalAuditError("attrition statuses are not exhaustive")
    return {
        "primary_midpoint_15m": {
            "included": midpoint.get("usable_midpoint_15m", 0),
            "excluded": len(rows) - midpoint.get("usable_midpoint_15m", 0),
            "exclusion_reasons": {
                key: value
                for key, value in sorted(midpoint.items())
                if key != "usable_midpoint_15m"
            },
        },
        "robustness_trade_close_15m": {
            "included": trade.get("usable_trade_15m", 0),
            "excluded": len(rows) - trade.get("usable_trade_15m", 0),
            "exclusion_reasons": {
                key: value
                for key, value in sorted(trade.items())
                if key != "usable_trade_15m"
            },
        },
    }


def _support_group(
    rows: Sequence[Mapping[str, Any]], key: Callable[[Mapping[str, Any]], str]
) -> dict[str, Any]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        groups[key(row)].append(row)
    result: dict[str, Any] = {}
    for label, group in sorted(groups.items()):
        family_weights: dict[tuple[str, str], float] = defaultdict(float)
        for row in group:
            family = (str(row["family_id"]), str(row["family_id_source"]))
            family_weights[family] += float(row["family_weight_raw"])
        families = len(family_weights)
        ess = kish_ess(list(family_weights.values()))
        result[label] = {
            "contracts": len(group),
            "families": families,
            "family_weighted_ess": ess,
            "subgroup_gate_passed": families >= 200 and ess >= 150,
        }
    return result


def support_diagnostics(primary: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    bins = _support_group(primary, lambda row: probability_bin(float(row["midpoint"])))
    for values in bins.values():
        values["probability_bin_gate_passed"] = (
            values["families"] >= 100 and values["family_weighted_ess"] >= 100
        )
    return {
        "thresholds": {
            "overall_families": 500,
            "overall_family_weighted_ess": 500,
            "subgroup_families": 200,
            "subgroup_family_weighted_ess": 150,
            "probability_bin_families": 100,
            "probability_bin_family_weighted_ess": 100,
        },
        "category": _support_group(primary, lambda row: str(row["category"])),
        "anchor_month": _support_group(primary, lambda row: str(row["anchor_month"])),
        "family_size_bin": _support_group(
            primary, lambda row: str(row["family_size_bin"])
        ),
        "probability_decile": bins,
    }


def weighted_observability_balance(
    rows: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    """Outcome-blind family-target balance diagnostics for primary observability."""
    observed = [row for row in rows if bool(row["midpoint_within_15m"])]
    missing = [row for row in rows if not bool(row["midpoint_within_15m"])]
    if not observed or not missing:
        raise FinalAuditError("observability balance requires both groups")

    def weighted_mean(group: Sequence[Mapping[str, Any]], field: str) -> float:
        weights = [float(row["family_weight_raw"]) for row in group]
        return sum(
            float(row[field]) * weight for row, weight in zip(group, weights)
        ) / sum(weights)

    def weighted_variance(
        group: Sequence[Mapping[str, Any]], field: str, mean: float
    ) -> float:
        weights = [float(row["family_weight_raw"]) for row in group]
        return sum(
            weight * (float(row[field]) - mean) ** 2
            for row, weight in zip(group, weights)
        ) / sum(weights)

    obs_mean = weighted_mean(observed, "hours_since_market_open")
    miss_mean = weighted_mean(missing, "hours_since_market_open")
    pooled = math.sqrt(
        (
            weighted_variance(observed, "hours_since_market_open", obs_mean)
            + weighted_variance(missing, "hours_since_market_open", miss_mean)
        )
        / 2
    )

    def categorical(
        field: str, transform: Callable[[str], str] = lambda value: value
    ) -> dict[str, Any]:
        labels = sorted({transform(str(row[field])) for row in rows})

        def shares(group: Sequence[Mapping[str, Any]]) -> dict[str, float]:
            total = sum(float(row["family_weight_raw"]) for row in group)
            return {
                label: sum(
                    float(row["family_weight_raw"])
                    for row in group
                    if transform(str(row[field])) == label
                )
                / total
                for label in labels
            }

        obs = shares(observed)
        miss = shares(missing)
        differences = {label: obs[label] - miss[label] for label in labels}
        return {
            "observed_weighted_shares": obs,
            "missing_weighted_shares": miss,
            "share_differences": differences,
            "maximum_absolute_share_difference": max(
                (abs(value) for value in differences.values()), default=0.0
            ),
        }

    return {
        "weight_system": "family_weight_raw",
        "observed_contracts": len(observed),
        "missing_contracts": len(missing),
        "hours_since_market_open": {
            "observed_weighted_mean": obs_mean,
            "missing_weighted_mean": miss_mean,
            "standardized_mean_difference": (
                (obs_mean - miss_mean) / pooled if pooled > 0 else None
            ),
        },
        "anchor_month": categorical("anchor_month"),
        "family_size_bin": categorical("family_size_bin"),
        "target_utc_hour": categorical("target_time", lambda value: value[11:13]),
        "timing_structure": categorical("timing_structure"),
        "contract_position": "not available in the frozen outcome-blind manifest",
        "observation_propensity_correction_applied": False,
    }
