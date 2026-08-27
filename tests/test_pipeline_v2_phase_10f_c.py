"""Offline integrity tests for Phase 10F-C probability-sampling design."""

from __future__ import annotations

from collections import Counter

import pytest

from scripts.pipeline_v2.phase_10f_c_design import (
    FrameFamily,
    SamplingDesignError,
    allocate_stratified_sample,
    design_expectations,
    draw_two_stage_sample,
    weighted_mean,
)
from scripts.pipeline_v2.study_rules import validate_research_feature_columns


def family(name: str, size: int, *, rule: str = "PR1", category: str = "Crypto") -> FrameFamily:
    return FrameFamily(
        family_id=name,
        family_id_source="event",
        rule=rule,
        category=category,
        anchor_month="2026-01",
        contract_count=size,
        contract_ids=tuple(f"{name}-{index}" for index in range(size)),
    )


def test_sampling_is_deterministic_under_input_reordering():
    frame = [family("A", 1), family("B", 3), family("C", 5), family("D", 8)]
    counts = Counter(row.stratum for row in frame)
    allocation = allocate_stratified_sample(counts, 3, minimum_per_stratum=0)
    first = draw_two_stage_sample(frame, allocation, 2)
    second = draw_two_stage_sample(list(reversed(frame)), allocation, 2)
    assert first == second


def test_exact_two_stage_probabilities_and_weights():
    frame = [family(f"F{i}", 4) for i in range(4)]
    allocation = {frame[0].stratum: 2}
    rows = draw_two_stage_sample(frame, allocation, 2)
    assert len(rows) == 4
    for row in rows:
        assert row["pi_family"] == 0.5
        assert row["pi_contract_given_family"] == 0.5
        assert row["pi_contract"] == 0.25
        assert row["contract_weight_raw"] == 4
        assert row["family_weight_raw"] == 1


def test_stratum_minimum_and_total_are_preserved():
    counts = {
        ("PR1", "Crypto", "2026-01", "1"): 100,
        ("PR1", "Financials", "2026-01", "2-5"): 1,
        ("PR2", "Sports", "2026-02", "1"): 30,
    }
    allocation = allocate_stratified_sample(counts, 12)
    assert sum(allocation.values()) == 12
    assert allocation[("PR1", "Financials", "2026-01", "2-5")] == 1
    assert all(allocation[key] >= min(2, count) for key, count in counts.items())


def test_census_reconstructs_family_and_contract_population_means():
    frame = [family("small", 1), family("large", 3)]
    allocation = Counter(row.stratum for row in frame)
    rows = draw_two_stage_sample(frame, allocation, 10)
    values = {"small-0": 0.0, "large-0": 0.0, "large-1": 0.0, "large-2": 1.0}
    enriched = [{**row, "z": values[row["contract_id"]]} for row in rows]
    assert weighted_mean(enriched, value_field="z", weight_field="family_weight_raw") == pytest.approx(1 / 6)
    assert weighted_mean(enriched, value_field="z", weight_field="contract_weight_raw") == pytest.approx(1 / 4)


def test_expected_design_ess_and_ticker_count_are_bounded():
    frame = [family(f"A{i}", 1) for i in range(10)] + [family(f"B{i}", 5) for i in range(10)]
    counts = Counter(row.stratum for row in frame)
    allocation = allocate_stratified_sample(counts, 10, minimum_per_stratum=0)
    result = design_expectations(frame, allocation, 3)
    assert 10 <= result["expected_sampled_tickers"] <= 30
    assert result["expected_independent_family_design_ess"] <= 10 + 1e-9
    assert result["expected_contract_weight_design_ess"] <= result["expected_sampled_tickers"] + 1e-9


def test_duplicate_families_and_contracts_fail_closed():
    one = family("A", 2)
    with pytest.raises(SamplingDesignError, match="duplicate family"):
        draw_two_stage_sample([one, one], {one.stratum: 1}, 1)
    other = FrameFamily("B", "event", "PR1", "Crypto", "2026-01", 1, ("A-0",))
    with pytest.raises(SamplingDesignError, match="multiple families"):
        draw_two_stage_sample([one, other], {one.stratum: 2}, 1)


def test_incomplete_contract_identity_and_invalid_allocation_fail_closed():
    broken = FrameFamily("A", "event", "PR1", "Crypto", "2026-01", 2, ("A-0",))
    with pytest.raises(SamplingDesignError, match="incomplete"):
        draw_two_stage_sample([broken], {broken.stratum: 1}, 1)
    with pytest.raises(SamplingDesignError, match="minimum"):
        allocate_stratified_sample({("A", "B", "C", "1"): 5, ("D", "E", "F", "1"): 5}, 1)


def test_sampling_manifest_projection_quarantines_outcomes_and_prices():
    allowed = (
        "family_id", "family_id_source", "contract_id", "rule", "category",
        "anchor_month", "family_size_bin", "family_contract_count",
        "sampled_contract_count_in_family", "stratum_family_count",
        "stratum_sampled_family_count", "pi_family", "pi_contract_given_family",
        "pi_contract", "contract_weight_raw", "family_weight_raw", "sampling_seed",
    )
    validate_research_feature_columns(allowed)
    for forbidden in ("outcome", "result", "settlement_value"):
        with pytest.raises(ValueError, match="quarantined"):
            validate_research_feature_columns((*allowed, forbidden))
