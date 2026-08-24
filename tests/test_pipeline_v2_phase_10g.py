"""Outcome-release and analysis tests for Phase 10G."""

from __future__ import annotations

import csv
import gzip

import pytest

from scripts.pipeline_v2.phase_10g_analysis import (
    bootstrap_intervals,
    calibration_bins,
    weighted_estimate,
)
from scripts.pipeline_v2.run_phase_10f_e import SAMPLE_COMMIT_IDENTITY
from scripts.pipeline_v2.run_phase_10g_outcome_analysis import (
    JOINED_DERIVED_FIELDS,
    MINIMAL_OUTCOME_FIELDS,
    _release_minimal_outcomes,
    _resolution_diagnostics,
    _rows_sha256,
    _validate_joined_scope,
)
from scripts.pipeline_v2.run_phase_10f_e import NORMALIZED_FIELDS


def row(index, price, outcome, **changes):
    result = {
        "family_sample_index": index,
        "family_id": f"F{index}",
        "family_id_source": "event",
        "ticker": f"T{index}",
        "anchor_month": "2026-01",
        "family_size_bin": "1",
        "category": "Sports",
        "timing_structure": "scheduled_event_start",
        "hours_since_market_open": 24,
        "midpoint": price,
        "trade_close": price,
        "midpoint_within_15m": True,
        "midpoint_within_60m": True,
        "midpoint_15m_spread_lte_0_20": True,
        "midpoint_15m_spread_lte_0_10": True,
        "trade_within_15m": True,
        "trade_within_60m": True,
        "family_weight_raw": 1.0,
        "contract_weight_raw": 1.0,
        "binary_resolution_outcome": outcome,
    }
    result.update(changes)
    return result


def test_weighted_estimate_keeps_weight_system_and_contrast_explicit():
    rows = [row(1, 0.1, 0), row(2, 0.9, 1)]
    result = weighted_estimate(
        rows,
        sample_name="primary_midpoint_15m",
        weight_field="family_weight_raw",
    )
    assert result["weighted_mean_price"] == pytest.approx(0.5)
    assert result["weighted_yes_rate"] == pytest.approx(0.5)
    assert result["weighted_calibration_gap"] == pytest.approx(0)
    assert result["longshot_favorite_contrast"]["estimate"] == pytest.approx(-0.2)


def test_calibration_bins_use_all_fixed_deciles_and_support_gate():
    rows = []
    for decile in range(10):
        price = decile / 10 + 0.05
        for offset in range(100):
            rows.append(row(decile * 100 + offset + 1, price, offset % 2))
    bins = calibration_bins(rows, weight_field="family_weight_raw")
    assert len(bins) == 10
    assert bins[0]["probability_bin"] == "0.0-0.1"
    assert bins[-1]["probability_bin"] == "0.9-1.0"
    assert all(item["support_gate_passed"] for item in bins)


def test_stratified_family_cluster_bootstrap_is_deterministic():
    rows = []
    for index in range(1, 5001):
        decile = (index - 1) % 10
        price = decile / 10 + 0.05
        rows.append(row(index, price, index % 2))
    first = bootstrap_intervals(rows, replicates=25, batch_size=10)
    second = bootstrap_intervals(rows, replicates=25, batch_size=10)
    assert first == second
    assert first["replicates"] == 25
    assert first["stratum_count"] == 1
    assert len(first["intervals"]) == 34


def test_minimal_release_discards_settlement_and_extra_source_fields(tmp_path):
    source = tmp_path / "outcomes.csv.gz"
    with gzip.open(source, "wt", newline="") as handle:
        writer = csv.DictWriter(
            handle,
            fieldnames=(
                "ticker",
                "result",
                "settlement_value_dollars",
                "settlement_ts",
                "binary_outcome_status",
            ),
        )
        writer.writeheader()
        writer.writerow(
            {
                "ticker": "A",
                "result": "yes",
                "settlement_value_dollars": "1.0",
                "settlement_ts": "future",
                "binary_outcome_status": "valid_binary_yes",
            }
        )
        writer.writerow({"ticker": "B", "result": ""})
    rows, report = _release_minimal_outcomes(source, ["A", "B"], expected_contracts=2)
    assert tuple(rows[0]) == MINIMAL_OUTCOME_FIELDS
    assert rows == [
        {
            "contract_identifier": "A",
            "frozen_sample_identifier": SAMPLE_COMMIT_IDENTITY,
            "binary_resolution_outcome": 1,
        },
        {
            "contract_identifier": "B",
            "frozen_sample_identifier": SAMPLE_COMMIT_IDENTITY,
            "binary_resolution_outcome": "",
        },
    ]
    assert report["settlement_fields_released"] == 0
    assert report["post_resolution_metadata_fields_released"] == 0


def test_resolution_diagnostics_do_not_filter_or_replace_missing_outcomes():
    rows = [row(1, 0.1, 0), row(2, 0.9, None)]
    report = _resolution_diagnostics(rows)
    assert report["frozen_sample"]["resolved_contracts"] == 1
    assert report["frozen_sample"]["unresolved_contracts"] == 1
    assert (
        report["resolved_vs_unresolved_ex_ante_comparison"][
            "post_outcome_filtering_applied"
        ]
        is False
    )


def test_joined_scope_and_hash_are_deterministic_and_fail_closed():
    joined = {field: "" for field in NORMALIZED_FIELDS}
    joined.update(
        {
            "binary_resolution_outcome": 1,
            "midpoint_15m_spread_lte_0_20": True,
            "midpoint_15m_spread_lte_0_10": True,
        }
    )
    assert set(joined) == set(NORMALIZED_FIELDS) | set(JOINED_DERIVED_FIELDS)
    assert _validate_joined_scope([joined])["passed"] is True
    assert _rows_sha256([joined]) == _rows_sha256([dict(joined)])

    contaminated = dict(joined, settlement_timestamp="future")
    with pytest.raises(Exception, match="schema changed"):
        _validate_joined_scope([contaminated])
