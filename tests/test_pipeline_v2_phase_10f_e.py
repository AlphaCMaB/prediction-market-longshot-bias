"""Outcome-blind unit tests for Phase 10F-E price freezing."""

from __future__ import annotations

import pytest

from scripts.pipeline_v2.investigate_phase_10f_e_schema import inspect_live_payload
from scripts.pipeline_v2.phase_10f_e import (
    attrition_counts,
    classify_price_observability,
    distribution,
    sample_metrics,
)
from scripts.pipeline_v2.run_phase_10f_e import (
    ANALYSIS_FIELDS,
    NORMALIZED_FIELDS,
    _partition_identity,
)
from scripts.pipeline_v2.study_rules import validate_research_feature_columns


def base(**changes):
    row = {
        "request_success": True,
        "candle_count": 1,
        "missing_bid": False,
        "missing_ask": False,
        "midpoint": 0.4,
        "midpoint_within_15m": True,
        "midpoint_within_60m": True,
        "trade_close": 0.4,
        "trade_within_15m": True,
        "trade_within_60m": True,
    }
    row.update(changes)
    return row


@pytest.mark.parametrize(
    ("changes", "midpoint", "trade"),
    [
        ({}, "usable_midpoint_15m", "usable_trade_15m"),
        (
            {"midpoint_within_15m": False, "trade_within_15m": False},
            "usable_midpoint_60m_only",
            "usable_trade_60m_only",
        ),
        ({"missing_bid": True}, "missing_bid", "usable_trade_15m"),
        ({"missing_ask": True}, "missing_ask", "usable_trade_15m"),
        (
            {"request_success": False},
            "api_or_data_failure",
            "api_or_data_failure",
        ),
        ({"candle_count": 0}, "no_pre_target_candle", "no_pre_target_candle"),
        (
            {"midpoint_within_15m": False, "midpoint_within_60m": False},
            "midpoint_too_stale",
            "usable_trade_15m",
        ),
        (
            {"trade_close": None, "trade_within_15m": False, "trade_within_60m": False},
            "usable_midpoint_15m",
            "no_trade",
        ),
        (
            {
                "trade_close": None,
                "trade_within_15m": False,
                "trade_within_60m": False,
                "trade_failure_reason": "trade_schema_unavailable",
            },
            "usable_midpoint_15m",
            "trade_schema_unavailable",
        ),
    ],
)
def test_observability_reasons_are_explicit(changes, midpoint, trade):
    result = classify_price_observability(base(**changes))
    assert result["midpoint_observability_status"] == midpoint
    assert result["trade_observability_status"] == trade


def test_family_and_contract_ess_remain_separate():
    rows = [
        {
            "family_id": "A",
            "family_id_source": "event",
            "family_weight_raw": 1.0,
            "contract_weight_raw": 2.0,
            "usable": True,
        },
        {
            "family_id": "A",
            "family_id_source": "event",
            "family_weight_raw": 1.0,
            "contract_weight_raw": 2.0,
            "usable": True,
        },
        {
            "family_id": "B",
            "family_id_source": "event",
            "family_weight_raw": 2.0,
            "contract_weight_raw": 8.0,
            "usable": False,
        },
    ]
    metrics = sample_metrics(rows, flag="usable")
    assert metrics["usable_contracts"] == 2
    assert metrics["usable_unique_families"] == 1
    assert metrics["family_weighted_ess"] == pytest.approx(1.0)
    assert metrics["contract_weighted_ess"] == pytest.approx(2.0)


def test_spread_distribution_uses_frozen_thresholds():
    result = distribution([0.01, 0.03, 0.11, 0.21])
    assert result["median"] == pytest.approx(0.07)
    assert result["fraction_gt_0_02"] == pytest.approx(0.75)
    assert result["fraction_gt_0_10"] == pytest.approx(0.5)
    assert result["fraction_gt_0_20"] == pytest.approx(0.25)


def test_attrition_keeps_midpoint_and_trade_statuses_separate():
    rows = []
    for changes in (
        {},
        {"missing_bid": True},
        {"trade_close": None, "trade_within_15m": False, "trade_within_60m": False},
    ):
        row = base(**changes)
        rows.append({**row, **classify_price_observability(row)})
    counts = attrition_counts(rows)
    assert counts["midpoint_status_counts"]["usable_midpoint_15m"] == 2
    assert counts["midpoint_status_counts"]["missing_bid"] == 1
    assert counts["trade_status_counts"]["no_trade"] == 1


def test_partition_identity_is_ordered_and_sample_pinned():
    rows = [
        {"contract_sample_index": "1", "ticker": "A"},
        {"contract_sample_index": "2", "ticker": "B"},
    ]
    first = _partition_identity(1, rows)
    assert first == _partition_identity(1, list(rows))
    assert first != _partition_identity(2, rows)
    assert first != _partition_identity(1, list(reversed(rows)))


def test_price_freeze_schema_quarantines_outcomes():
    validate_research_feature_columns(NORMALIZED_FIELDS)
    validate_research_feature_columns(ANALYSIS_FIELDS)
    with pytest.raises(ValueError, match="quarantined"):
        validate_research_feature_columns((*ANALYSIS_FIELDS, "outcome"))


def test_investigation_reports_every_requested_live_key_presence():
    payload = {
        "markets": [
            {
                "market_ticker": "A",
                "candlesticks": [
                    {
                        "end_period_ts": 100,
                        "yes_bid": {"close_dollars": "0.40"},
                        "yes_ask": {"close_dollars": "0.50"},
                        "price": {"previous_dollars": "0.30"},
                    }
                ],
            }
        ]
    }
    report = inspect_live_payload(payload, ticker="A", target_ts=100)
    candle = report["per_candle_key_presence"][0]
    assert candle["yes_bid"]["close_dollars"] is True
    assert candle["yes_bid"]["close"] is False
    assert candle["yes_ask"]["close_dollars"] is True
    assert candle["yes_ask"]["close"] is False
    assert candle["price"]["close_dollars"] is False
    assert candle["price"]["close"] is False
    assert candle["price"]["previous_dollars"] is True
    assert candle["price"]["previous"] is False
    assert report["quote_ambiguity_count"] == 0
    assert report["trade_schema_unavailable_candles"] == 1
