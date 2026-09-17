"""Tests for Phase 10I exploratory execution helpers."""

from __future__ import annotations

import gzip
import hashlib
import json

import pytest

from scripts.pipeline_v2.kalshi_metadata_cache import canonical_json
from scripts.pipeline_v2.phase_10i_analysis import (
    bootstrap_metric_intervals,
    execution_values,
    fee_per_contract,
    quantile_cutpoints,
    quantile_label,
    strategy_group,
    weighted_execution_summary,
)
from scripts.pipeline_v2 import run_phase_10i_exploratory_analysis as runner


def row(index: int, midpoint: float, bid: float, ask: float, outcome: int):
    base = {
        "family_id": f"F{index}",
        "family_id_source": "test",
        "family_sample_index": index,
        "anchor_month": "2026-01",
        "family_size_bin": "1",
        "midpoint": midpoint,
        "yes_bid": bid,
        "yes_ask": ask,
        "binary_resolution_outcome": outcome,
        "family_weight_raw": 1.0,
        "contract_weight_raw": 1.0,
    }
    return {**base, **execution_values(base)}


def test_fee_formula_preserves_order_level_cent_rounding():
    assert fee_per_contract(0.10, 1, 0.07) == pytest.approx(0.01)
    assert fee_per_contract(0.10, 100, 0.07) == pytest.approx(0.0063)
    assert fee_per_contract(0.50, 100, 0.0175) == pytest.approx(0.0044)


def test_strategy_group_uses_frozen_tail_boundaries():
    assert strategy_group(0.1999) == "longshot"
    assert strategy_group(0.20) is None
    assert strategy_group(0.7999) is None
    assert strategy_group(0.80) == "favorite"


def test_execution_values_use_executable_opposing_sides():
    longshot = row(1, 0.10, 0.08, 0.12, 0)
    assert longshot["strategy_group"] == "longshot"
    assert longshot["taker_entry"] == pytest.approx(0.92)
    assert longshot["maker_entry"] == pytest.approx(0.88)
    assert longshot["taker_gross_profit"] == pytest.approx(0.08)

    favorite = row(2, 0.90, 0.88, 0.92, 1)
    assert favorite["strategy_group"] == "favorite"
    assert favorite["taker_entry"] == pytest.approx(0.92)
    assert favorite["maker_entry"] == pytest.approx(0.88)
    assert favorite["taker_gross_profit"] == pytest.approx(0.08)


def test_execution_values_reject_invalid_quote_ordering():
    invalid = {
        "midpoint": 0.10,
        "yes_bid": 0.12,
        "yes_ask": 0.08,
        "binary_resolution_outcome": 0,
    }
    with pytest.raises(RuntimeError, match="ordering"):
        execution_values(invalid)


def test_weighted_summary_uses_ratio_of_aggregate_profit_and_cost():
    first = row(1, 0.10, 0.08, 0.12, 0)
    second = row(2, 0.90, 0.88, 0.92, 0)
    second["family_weight_raw"] = 3.0
    summary = weighted_execution_summary(
        [first, second], weight_field="family_weight_raw"
    )
    expected_profit = (0.08 + 3 * -0.92) / 4
    assert summary["weighted_taker_gross_profit"] == pytest.approx(expected_profit)
    assert summary["weighted_taker_gross_capital_return"] == pytest.approx(
        expected_profit / 0.92
    )
    assert summary["families"] == 2


def test_quartiles_are_deterministic_and_boundary_inclusive():
    cuts = quantile_cutpoints([1, 2, 3, 4, 5])
    assert cuts == pytest.approx((2, 3, 4))
    assert [quantile_label(value, cuts) for value in (2, 3, 4, 5)] == [
        "Q1",
        "Q2",
        "Q3",
        "Q4",
    ]


def test_bootstrap_resamples_the_full_frozen_family_frame():
    rows = []
    for index in range(1, 5001):
        item = row(index, 0.10, 0.08, 0.12, index % 2)
        item["included"] = index <= 1000
        item["metric"] = float(index % 2)
        rows.append(item)
    result = bootstrap_metric_intervals(
        rows,
        {
            "all": (lambda item: bool(item["included"]), "metric", "family_weight_raw"),
            "even": (
                lambda item: bool(item["included"])
                and int(item["family_sample_index"]) % 2 == 0,
                "metric",
                "family_weight_raw",
            ),
        },
        contrasts={"difference": ("all", "even")},
        replicates=20,
        batch_size=5,
    )
    assert result["replicates"] == 20
    assert result["intervals"]["all"]["replicates"] == 20
    assert result["contrasts"]["difference"]["replicates"] == 20


def test_raw_activity_handles_historical_and_live_schema(tmp_path, monkeypatch):
    monkeypatch.setattr(runner, "EXPECTED_CONTRACTS", 2)
    phase_root = tmp_path / "phase"
    raw_root = phase_root / "partitions" / "partition_0001" / "raw"
    raw_root.mkdir(parents=True)
    records = []
    cases = [
        (
            "HIST",
            "/historical/markets/HIST/candlesticks",
            {},
            {
                "ticker": "HIST",
                "candlesticks": [
                    {
                        "end_period_ts": 90,
                        "volume": "2.00",
                        "open_interest": "4.00",
                        "price": {"close": "0.30"},
                    }
                ],
            },
        ),
        (
            "LIVE",
            "/markets/candlesticks",
            {"market_tickers": "LIVE"},
            {
                "markets": [
                    {
                        "market_ticker": "LIVE",
                        "candlesticks": [
                            {
                                "end_period_ts": 100,
                                "volume_fp": "3.50",
                                "open_interest_fp": "8.25",
                                "price": {"close_dollars": "0.40"},
                            }
                        ],
                    }
                ]
            },
        ),
    ]
    for request_index, (ticker, endpoint, extra_params, response) in enumerate(cases):
        request_id = f"request{request_index}"
        request = {
            "endpoint": endpoint,
            "params": {"end_ts": 100, "start_ts": 40, **extra_params},
            "purpose": "sample_price_window",
        }
        document = {
            "request": request,
            "response": response,
            "response_sha256": hashlib.sha256(canonical_json(response)).hexdigest(),
        }
        payload = gzip.compress(json.dumps(document).encode(), mtime=0)
        path = raw_root / f"{request_id}.json.gz"
        path.write_bytes(payload)
        records.append(
            {
                "request_id": request_id,
                "partition_index": 1,
                "raw_path": f"raw/{request_id}.json.gz",
                "raw_sha256": hashlib.sha256(payload).hexdigest(),
                "request": request,
                "success": True,
                "schema_variants": [ticker.casefold()],
            }
        )
    manifest = phase_root / "manifest.jsonl"
    manifest.write_text("".join(json.dumps(record) + "\n" for record in records))
    activity, validation = runner._raw_activity(manifest, phase_root)
    assert activity["HIST"]["pre_target_volume_60m"] == pytest.approx(2.0)
    assert activity["LIVE"]["pre_target_volume_60m"] == pytest.approx(3.5)
    assert activity["LIVE"]["latest_pre_target_open_interest"] == pytest.approx(8.25)
    assert activity["LIVE"]["pre_target_trade_candle_count"] == 1
    assert validation["post_target_candles"] == 0


def test_cross_category_preflight_stops_on_zero_pr1_coverage(tmp_path):
    frame_path = tmp_path / "frame.json"
    acceptance_path = tmp_path / "acceptance.json"
    frame_path.write_text(
        json.dumps(
            {
                "eligible_family_counts": {
                    "by_category": {"Crypto": 1000, "Financials": 800}
                },
                "eligible_contract_counts": {
                    "by_category": {"Crypto": 10000, "Financials": 8000}
                },
                "b2_planning_rates": {
                    "PR1_M_FIXED_CLOCK_SINGLE_EXACT": {
                        "usable_midpoint_15m": {"successes": 0, "trials": 135},
                        "usable_trade_close_15m": {"successes": 0, "trials": 135},
                    }
                },
            }
        )
    )
    acceptance_path.write_text(
        json.dumps(
            {
                "measured_compressed_raw_bytes_per_ticker_request": 100,
                "measured_normalized_bytes_per_ticker": 10,
                "measured_request_commit_and_manifest_bytes_per_ticker": 50,
            }
        )
    )
    report = runner._cross_category_preflight(
        frame_path, acceptance_path, {"remaining_budget_bytes": 10_000_000}
    )
    assert report["hard_stop"]["triggered"] is True
    assert report["sample_drawn"] is False
    assert report["current_historical_source"]["midpoint_15m_successes"] == 0


def test_published_storage_snapshot_is_reused_for_byte_stable_replay(tmp_path):
    current = {
        "used_bytes": 20,
        "max_bytes": 100,
        "remaining_budget_bytes": 80,
        "free_bytes": 1_000,
        "min_free_bytes": 500,
        "free_space_margin_bytes": 500,
    }
    assert runner._report_storage_snapshot(tmp_path, current) == current
    recorded = {**current, "free_bytes": 900, "free_space_margin_bytes": 400}
    (tmp_path / "phase_10i_analysis_report.json").write_text(
        json.dumps({"storage_preflight": recorded})
    )
    assert runner._report_storage_snapshot(tmp_path, current) == recorded


def test_published_storage_snapshot_rejects_guard_change(tmp_path):
    recorded = {
        "used_bytes": 20,
        "max_bytes": 100,
        "remaining_budget_bytes": 80,
        "free_bytes": 1_000,
        "min_free_bytes": 500,
        "free_space_margin_bytes": 500,
    }
    (tmp_path / "phase_10i_analysis_report.json").write_text(
        json.dumps({"storage_preflight": recorded})
    )
    with pytest.raises(RuntimeError, match="guard changed"):
        runner._report_storage_snapshot(tmp_path, {**recorded, "max_bytes": 101})
