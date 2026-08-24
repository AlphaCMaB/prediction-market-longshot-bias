"""Tests for deterministic Phase 10G paper reporting."""

from __future__ import annotations

import pytest

from scripts.pipeline_v2.build_phase_10g_paper_report import (
    PaperReportError,
    SAMPLE_LABELS,
    SAMPLE_ORDER,
    markdown_table,
    table_2_rows,
    table_3_rows,
)


def result(value=0.01):
    return {
        "resolved_contracts": 100,
        "resolved_families": 80,
        "family_aggregated_weight_ess": 75.0,
        "weighted_calibration_gap": value,
        "weighted_calibration_gap_inference": {"ci_lower": -0.01, "ci_upper": 0.02},
        "longshot_favorite_contrast": {
            "estimate": value,
            "inference": {"ci_lower": -0.02, "ci_upper": 0.03},
        },
    }


def test_markdown_table_is_deterministic_and_escapes_pipes():
    rows = [{"A": "x|y", "B": 1}]
    assert markdown_table(rows, ("A", "B")) == (
        "| A | B |\n| --- | --- |\n| x\\|y | 1 |"
    )


def test_table_2_preserves_all_frozen_specifications_and_secondary_target():
    estimates = {name: {"family_target": result()} for name in SAMPLE_ORDER}
    estimates["primary_midpoint_15m"]["contract_target"] = result(0.02)
    rows = table_2_rows({"analysis_report": {"estimates": estimates}})
    assert [row["Specification"] for row in rows[:-1]] == [
        SAMPLE_LABELS[name] for name in SAMPLE_ORDER
    ]
    assert rows[-1]["Target"] == "Contract"
    assert len(rows) == 7


def test_table_3_requires_all_ten_supported_frozen_bins():
    bins = []
    for index in range(10):
        bins.append(
            {
                "probability_bin": f"{index/10:.1f}-{(index+1)/10:.1f}",
                "weighted_mean_price": index / 10 + 0.05,
                "weighted_yes_rate": index / 10 + 0.05,
                "weighted_calibration_gap": 0.0,
                "weighted_calibration_gap_inference": {
                    "ci_lower": -0.01,
                    "ci_upper": 0.01,
                },
                "resolved_families": 100,
                "family_aggregated_weight_ess": 100.0,
                "support_gate_passed": True,
            }
        )
    data = {"analysis_report": {"primary_calibration_bins": {"family_target": bins}}}
    assert len(table_3_rows(data)) == 10
    bins[-1]["support_gate_passed"] = False
    with pytest.raises(PaperReportError, match="support changed"):
        table_3_rows(data)
