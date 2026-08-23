"""Pure two-stage probability-sampling and weighting helpers for Phase 10F-C."""

from __future__ import annotations

from collections import Counter, defaultdict
from dataclasses import dataclass
import hashlib
import math
from statistics import fmean
from typing import Any, Iterable, Mapping, Sequence


SCHEMA_VERSION = "phase-10f-c-sampling-design-v1"
SAMPLING_SEED = "phase-10f-c-two-stage-srswor-v1"
SIZE_BINS = ("1", "2-5", "6-25", "26-100", "101-400")


class SamplingDesignError(RuntimeError):
    pass


@dataclass(frozen=True)
class FrameFamily:
    family_id: str
    family_id_source: str
    rule: str
    category: str
    anchor_month: str
    contract_count: int
    contract_ids: tuple[str, ...] = ()

    @property
    def identity(self) -> tuple[str, str]:
        return self.family_id, self.family_id_source

    @property
    def size_bin(self) -> str:
        return family_size_bin(self.contract_count)

    @property
    def stratum(self) -> tuple[str, str, str, str]:
        return self.rule, self.category, self.anchor_month, self.size_bin


def family_size_bin(count: int) -> str:
    if count <= 0:
        raise SamplingDesignError("family contract count must be positive")
    if count == 1:
        return "1"
    if count <= 5:
        return "2-5"
    if count <= 25:
        return "6-25"
    if count <= 100:
        return "26-100"
    if count <= 400:
        return "101-400"
    raise SamplingDesignError("family exceeds frozen 400-contract design bound")


def _rank(seed: str, *parts: str) -> str:
    return hashlib.sha256("\x00".join((seed, *parts)).encode("utf-8")).hexdigest()


def allocate_stratified_sample(
    stratum_counts: Mapping[tuple[str, str, str, str], int],
    total_families: int,
    *,
    minimum_per_stratum: int = 2,
) -> dict[tuple[str, str, str, str], int]:
    """Minimum-preserving proportional allocation with deterministic remainders."""
    if total_families <= 0 or minimum_per_stratum < 0:
        raise SamplingDesignError("invalid family allocation settings")
    counts = {key: int(value) for key, value in stratum_counts.items() if int(value) > 0}
    if total_families > sum(counts.values()):
        raise SamplingDesignError("family sample exceeds frame")
    allocation = {
        key: min(value, minimum_per_stratum) for key, value in counts.items()
    }
    if sum(allocation.values()) > total_families:
        raise SamplingDesignError("sample cannot preserve the minimum in every stratum")
    remaining = total_families - sum(allocation.values())
    while remaining:
        capacity = {key: counts[key] - allocation[key] for key in counts}
        total_capacity = sum(capacity.values())
        if total_capacity < remaining or total_capacity <= 0:
            raise SamplingDesignError("stratified allocation exhausted unexpectedly")
        raw = {key: remaining * capacity[key] / total_capacity for key in counts}
        floors = {key: min(capacity[key], math.floor(raw[key])) for key in counts}
        floor_total = sum(floors.values())
        for key, value in floors.items():
            allocation[key] += value
        remaining -= floor_total
        if not remaining:
            break
        ranked = sorted(
            (key for key in counts if allocation[key] < counts[key]),
            key=lambda key: (-(raw[key] - floors[key]), key),
        )
        if not ranked:
            raise SamplingDesignError("no stratum capacity remains")
        for key in ranked[:remaining]:
            allocation[key] += 1
        remaining -= min(remaining, len(ranked))
    if sum(allocation.values()) != total_families:
        raise SamplingDesignError("family allocation does not sum to target")
    return dict(sorted(allocation.items()))


def draw_two_stage_sample(
    families: Sequence[FrameFamily],
    allocation: Mapping[tuple[str, str, str, str], int],
    contract_cap: int,
    *,
    seed: str = SAMPLING_SEED,
) -> list[dict[str, Any]]:
    """Deterministically realize the approved probability design for a supplied frame."""
    if contract_cap <= 0:
        raise SamplingDesignError("contract cap must be positive")
    by_stratum: dict[tuple[str, str, str, str], list[FrameFamily]] = defaultdict(list)
    identities: set[tuple[str, str]] = set()
    all_contracts: set[str] = set()
    for family in families:
        if family.identity in identities:
            raise SamplingDesignError("duplicate family identity")
        identities.add(family.identity)
        if len(family.contract_ids) != family.contract_count:
            raise SamplingDesignError("family contract identities are incomplete")
        if len(set(family.contract_ids)) != len(family.contract_ids):
            raise SamplingDesignError("duplicate contract inside family")
        if all_contracts.intersection(family.contract_ids):
            raise SamplingDesignError("contract appears in multiple families")
        all_contracts.update(family.contract_ids)
        by_stratum[family.stratum].append(family)

    selected: list[dict[str, Any]] = []
    for stratum in sorted(by_stratum):
        frame = sorted(
            by_stratum[stratum],
            key=lambda row: (_rank(seed, "family", *row.identity), row.identity),
        )
        n_h = int(allocation.get(stratum, 0))
        if not 0 <= n_h <= len(frame):
            raise SamplingDesignError("invalid stratum sample size")
        pi_family = n_h / len(frame)
        for family in frame[:n_h]:
            m_i = min(family.contract_count, contract_cap)
            ranked_contracts = sorted(
                family.contract_ids,
                key=lambda ticker: (
                    _rank(seed, "contract", *family.identity, ticker),
                    ticker,
                ),
            )
            pi_conditional = m_i / family.contract_count
            pi_contract = pi_family * pi_conditional
            for ticker in ranked_contracts[:m_i]:
                selected.append(
                    {
                        "family_id": family.family_id,
                        "family_id_source": family.family_id_source,
                        "contract_id": ticker,
                        "rule": family.rule,
                        "category": family.category,
                        "anchor_month": family.anchor_month,
                        "family_size_bin": family.size_bin,
                        "family_contract_count": family.contract_count,
                        "sampled_contract_count_in_family": m_i,
                        "stratum_family_count": len(frame),
                        "stratum_sampled_family_count": n_h,
                        "pi_family": pi_family,
                        "pi_contract_given_family": pi_conditional,
                        "pi_contract": pi_contract,
                        "contract_weight_raw": 1 / pi_contract,
                        "family_weight_raw": 1
                        / (pi_contract * family.contract_count),
                        "sampling_seed": seed,
                    }
                )
    identities = [(row["family_id"], row["family_id_source"], row["contract_id"]) for row in selected]
    if len(identities) != len(set(identities)):
        raise SamplingDesignError("two-stage sample contains duplicates")
    return sorted(
        selected,
        key=lambda row: (
            row["family_id"], row["family_id_source"], row["contract_id"]
        ),
    )


def weighted_mean(
    rows: Iterable[Mapping[str, Any]], *, value_field: str, weight_field: str
) -> float:
    numerator = denominator = 0.0
    for row in rows:
        value = float(row[value_field])
        weight = float(row[weight_field])
        numerator += value * weight
        denominator += weight
    if denominator <= 0:
        raise SamplingDesignError("weighted mean has no positive mass")
    return numerator / denominator


def wilson_interval(successes: int, trials: int, *, z: float = 1.959963984540054) -> tuple[float, float]:
    if not 0 <= successes <= trials or trials <= 0:
        raise SamplingDesignError("invalid binomial counts")
    p = successes / trials
    denominator = 1 + z * z / trials
    center = (p + z * z / (2 * trials)) / denominator
    radius = (
        z
        * math.sqrt(p * (1 - p) / trials + z * z / (4 * trials * trials))
        / denominator
    )
    return max(0.0, center - radius), min(1.0, center + radius)


def design_expectations(
    families: Sequence[FrameFamily],
    allocation: Mapping[tuple[str, str, str, str], int],
    contract_cap: int,
) -> dict[str, Any]:
    by_stratum: dict[tuple[str, str, str, str], list[FrameFamily]] = defaultdict(list)
    for family in families:
        by_stratum[family.stratum].append(family)
    total_family_mass = len(families)
    total_contract_mass = sum(family.contract_count for family in families)
    expected_tickers = 0.0
    expected_by_rule: Counter[str] = Counter()
    expected_by_category: Counter[str] = Counter()
    expected_by_month: Counter[str] = Counter()
    expected_by_size: Counter[str] = Counter()
    family_cluster_weight_squares = 0.0
    family_weight_squares = 0.0
    contract_weight_squares = 0.0
    for stratum, frame in by_stratum.items():
        n_h = allocation[stratum]
        N_h = len(frame)
        pi_h = n_h / N_h
        mean_m = fmean(min(family.contract_count, contract_cap) for family in frame)
        expected = n_h * mean_m
        expected_tickers += expected
        expected_by_rule[stratum[0]] += expected
        expected_by_category[stratum[1]] += expected
        expected_by_month[stratum[2]] += expected
        expected_by_size[stratum[3]] += expected
        family_cluster_weight_squares += n_h / (pi_h * pi_h)
        family_weight_squares += (
            n_h
            / (pi_h * pi_h)
            * fmean(1 / min(family.contract_count, contract_cap) for family in frame)
        )
        contract_weight_squares += (
            n_h
            / (pi_h * pi_h)
            * fmean(
                family.contract_count * family.contract_count
                / min(family.contract_count, contract_cap)
                for family in frame
            )
        )
    return {
        "expected_sampled_tickers": expected_tickers,
        "expected_tickers_by_rule": dict(sorted(expected_by_rule.items())),
        "expected_tickers_by_category": dict(sorted(expected_by_category.items())),
        "expected_tickers_by_anchor_month": dict(sorted(expected_by_month.items())),
        "expected_tickers_by_family_size_bin": dict(sorted(expected_by_size.items())),
        "expected_independent_family_design_ess": total_family_mass**2
        / family_cluster_weight_squares,
        "expected_family_weight_design_ess": total_family_mass**2 / family_weight_squares,
        "expected_contract_weight_design_ess": total_contract_mass**2 / contract_weight_squares,
    }
