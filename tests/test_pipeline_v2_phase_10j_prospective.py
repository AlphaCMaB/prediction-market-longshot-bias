"""Tests for the Phase 10J prospective collection design."""

from __future__ import annotations

import json

import pytest

from scripts.pipeline_v2 import build_phase_10j_prospective_plan as runner
from scripts.pipeline_v2.phase_10j_prospective import (
    ProspectiveDesignError,
    family_selected,
    forbidden_paths,
    inclusion_record,
    normalize_orderbook,
    planning_rows,
    prospective_counts,
    sample_contracts,
    storage_scenarios,
    summarize_trades,
    validate_pre_target_capture,
)


def test_sampling_probabilities_are_fixed_and_support_roles_are_explicit():
    rows = {row["category"]: row for row in planning_rows()}
    assert rows["Sports"]["family_inclusion_probability"] == pytest.approx(
        0.03828050274361083
    )
    assert (
        rows["Sports"]["family_inclusion_probability_numerator"],
        rows["Sports"]["family_inclusion_probability_denominator"],
    ) == (9125, 238372)
    assert rows["Financials"]["expected_sampled_families"] == pytest.approx(625)
    assert rows["Climate and Weather"]["family_inclusion_probability"] == 1
    assert rows["Climate and Weather"]["expected_inferential_support"] is False


def test_family_draw_and_contract_sample_are_deterministic():
    first = family_selected("Sports", "E1", "event_ticker")
    second = family_selected("Sports", "E1", "event_ticker")
    assert first == second
    contracts = ["M4", "M1", "M3", "M2"]
    sample = sample_contracts("E1", "event_ticker", contracts)
    assert sample == sample_contracts("E1", "event_ticker", reversed(contracts))
    assert len(sample) == 3
    inclusion = inclusion_record("Sports", 4)
    assert inclusion["contract_inclusion_probability_given_family"] == 0.75
    assert inclusion["contract_inclusion_probability"] == pytest.approx(first[2] * 0.75)


def test_sampling_fails_closed_for_unapproved_category_and_bad_roster():
    with pytest.raises(ProspectiveDesignError, match="no approved rule"):
        family_selected("Politics", "E1", "event_ticker")
    with pytest.raises(ProspectiveDesignError, match="duplicate contract"):
        sample_contracts("E1", "event_ticker", ["M1", "M1"])


def test_forbidden_paths_are_recursive_and_value_independent():
    document = {"event": {"markets": [{"result": ""}]}, "safe": "result"}
    assert forbidden_paths(document) == ["event.markets[0].result"]


def test_pre_target_capture_rejects_late_response_for_price_use():
    valid = validate_pre_target_capture(
        "2026-10-01T11:59:50Z",
        "2026-10-01T11:59:51Z",
        "2026-10-01T12:00:00Z",
    )
    late = validate_pre_target_capture(
        "2026-10-01T11:59:50Z",
        "2026-10-01T12:00:01Z",
        "2026-10-01T12:00:00Z",
    )
    assert valid["valid_pre_target"] is True
    assert late["valid_pre_target"] is False


def test_orderbook_derives_executable_asks_depth_and_spread():
    result = normalize_orderbook(
        {
            "yes_dollars": [["0.40", "5.00"], ["0.35", "10.00"]],
            "no_dollars": [["0.55", "2.00"], ["0.50", "20.00"]],
        }
    )
    assert result["yes_bid"] == 0.40
    assert result["yes_ask"] == 0.45
    assert result["yes_spread"] == pytest.approx(0.05)
    assert result["buy_yes_vwap_1"] == 0.45
    assert result["buy_yes_vwap_10"] == pytest.approx(0.49)
    assert result["buy_yes_vwap_100"] is None
    assert result["buy_no_vwap_10"] == pytest.approx(0.625)


def test_orderbook_rejects_crossed_and_changed_schema():
    with pytest.raises(ProspectiveDesignError, match="crossed"):
        normalize_orderbook(
            {"yes_dollars": [["0.60", "1"]], "no_dollars": [["0.50", "1"]]}
        )
    with pytest.raises(ProspectiveDesignError, match="schema changed"):
        normalize_orderbook({"yes": [], "no": []})


def test_trade_summary_enforces_ticker_time_identity_and_binary_prices():
    trades = [
        {
            "trade_id": "T1",
            "ticker": "M1",
            "count_fp": "2.50",
            "yes_price_dollars": "0.40",
            "no_price_dollars": "0.60",
            "created_time": "2026-10-01T11:30:00Z",
        },
        {
            "trade_id": "T2",
            "ticker": "M1",
            "count_fp": "1.00",
            "yes_price_dollars": "0.45",
            "no_price_dollars": "0.55",
            "created_time": "2026-10-01T11:45:00Z",
        },
    ]
    summary = summarize_trades(
        trades,
        ticker="M1",
        min_time="2026-10-01T11:00:00Z",
        max_time="2026-10-01T12:00:00Z",
    )
    assert summary["trade_count_60m"] == 2
    assert summary["traded_contracts_60m"] == 3.5
    assert summary["last_trade_yes_price"] == 0.45
    bad = [{**trades[0], "created_time": "2026-10-01T12:00:01Z"}]
    with pytest.raises(ProspectiveDesignError, match="outside"):
        summarize_trades(
            bad,
            ticker="M1",
            min_time="2026-10-01T11:00:00Z",
            max_time="2026-10-01T12:00:00Z",
        )


def test_storage_and_request_planning_exceeds_current_headroom():
    counts = prospective_counts()
    assert counts["maximum_sampled_contracts"] == 6058
    scenarios = storage_scenarios(counts["maximum_sampled_contracts"])
    assert scenarios[0]["projected_incremental_bytes"] > 20_176_099
    assert (
        scenarios[-1]["projected_incremental_bytes"]
        > scenarios[0]["projected_incremental_bytes"]
    )


def test_capture_schema_has_no_outcome_or_settlement_fields():
    schema = runner._capture_schema()
    assert schema["outcome_fields"] == 0
    assert schema["settlement_fields"] == 0
    assert forbidden_paths(schema) == []


def test_published_storage_snapshot_is_reused_and_guarded(tmp_path):
    current = {
        "used_bytes": 90,
        "max_bytes": 100,
        "remaining_budget_bytes": 10,
        "free_bytes": 1_000,
        "min_free_bytes": 500,
        "free_space_margin_bytes": 500,
    }
    assert runner._recorded_storage(tmp_path, current) == current
    recorded = {**current, "free_bytes": 900, "free_space_margin_bytes": 400}
    (tmp_path / "phase_10j_preflight.json").write_text(
        json.dumps({"storage_snapshot": recorded})
    )
    assert runner._recorded_storage(tmp_path, current) == recorded
    with pytest.raises(RuntimeError, match="guard changed"):
        runner._recorded_storage(tmp_path, {**current, "max_bytes": 101})
