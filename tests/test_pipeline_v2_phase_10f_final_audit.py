"""Tests for the deterministic Phase 10F final pre-outcome audit."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone

import pytest

from scripts.pipeline_v2.phase_10f_final_audit import (
    FinalAuditError,
    mutually_exclusive_attrition,
    support_diagnostics,
    validate_analysis_projection,
    validate_sampling_design,
    validate_temporal_and_price_rules,
    weighted_observability_balance,
)


def _design_rows():
    rows = []
    index = 0
    for family_index in range(1, 5001):
        sampled = 3 if family_index <= 1573 else 2
        for contract_index in range(sampled):
            index += 1
            rows.append(
                {
                    "contract_sample_index": index,
                    "family_id": f"F{family_index}",
                    "family_id_source": "event",
                    "ticker": f"T{family_index}-{contract_index}",
                    "eligible_contract_count": sampled,
                    "sampled_contract_count": sampled,
                    "stratum_family_count": 5000,
                    "stratum_sampled_family_count": 5000,
                    "pi_family": 1.0,
                    "pi_contract_given_family": 1.0,
                    "pi_contract": 1.0,
                    "family_weight_raw": 1 / sampled,
                    "contract_weight_raw": 1.0,
                }
            )
    return rows


def _price_row(**changes):
    target = datetime(2026, 1, 2, 11, tzinfo=timezone.utc)
    row = {
        "contract_sample_index": 1,
        "family_id": "F",
        "family_id_source": "event",
        "ticker": "T",
        "category": "Sports",
        "anchor_month": "2026-01",
        "family_size_bin": "2-5",
        "timing_structure": "scheduled_event_start",
        "verified_anchor_time": (target + timedelta(hours=1)).isoformat(),
        "target_time": target.isoformat(),
        "market_open_time": (target - timedelta(days=1)).isoformat(),
        "hours_since_market_open": 24.0,
        "post_target_candle_count": 0,
        "previous_trade_used": False,
        "yes_bid": 0.2,
        "yes_ask": 0.3,
        "midpoint": 0.25,
        "spread": 0.1,
        "midpoint_observation_time": (target - timedelta(minutes=10)).isoformat(),
        "midpoint_staleness_minutes": 10.0,
        "midpoint_within_15m": True,
        "midpoint_within_60m": True,
        "trade_close": 0.24,
        "trade_observation_time": (target - timedelta(minutes=20)).isoformat(),
        "trade_staleness_minutes": 20.0,
        "trade_within_15m": False,
        "trade_within_60m": True,
        "midpoint_observability_status": "usable_midpoint_15m",
        "trade_observability_status": "usable_trade_60m_only",
        "family_weight_raw": 1.0,
    }
    row.update(changes)
    return row


def test_sampling_design_reconstructs_every_probability_and_weight():
    result = validate_sampling_design(_design_rows())
    assert result["contracts"] == 11573
    assert result["families"] == 5000
    assert result["maximum_contracts_per_family"] == 3


def test_sampling_design_fails_closed_on_weight_change():
    rows = _design_rows()
    rows[0]["family_weight_raw"] = 9
    with pytest.raises(FinalAuditError, match="weight changed"):
        validate_sampling_design(rows)


def test_temporal_price_audit_rejects_post_target_and_fallback():
    assert validate_temporal_and_price_rules([_price_row()])["passed"] is True
    with pytest.raises(FinalAuditError, match="post-target"):
        validate_temporal_and_price_rules([_price_row(post_target_candle_count=1)])
    with pytest.raises(FinalAuditError, match="previous-price"):
        validate_temporal_and_price_rules([_price_row(previous_trade_used=True)])


def test_temporal_price_audit_recomputes_midpoint_and_staleness():
    with pytest.raises(FinalAuditError, match="midpoint arithmetic"):
        validate_temporal_and_price_rules([_price_row(midpoint=0.26)])
    with pytest.raises(FinalAuditError, match="15-minute midpoint"):
        validate_temporal_and_price_rules([_price_row(midpoint_within_15m=False)])


def test_analysis_projection_is_exact_and_duplicate_free():
    normalized = [_price_row(), _price_row(ticker="T2", midpoint_within_15m=False)]
    fields = ("price_sample_name", "ticker", "family_id", "family_id_source")
    analysis = [
        {
            "price_sample_name": "primary_midpoint_15m",
            "ticker": "T",
            "family_id": "F",
            "family_id_source": "event",
        }
    ]
    result = validate_analysis_projection(
        normalized,
        analysis,
        flag="midpoint_within_15m",
        sample_name="primary_midpoint_15m",
        analysis_fields=fields,
    )
    assert result["contracts"] == 1
    bad = [{**analysis[0], "ticker": "CHANGED"}]
    with pytest.raises(FinalAuditError, match="deterministic frozen projection"):
        validate_analysis_projection(
            normalized,
            bad,
            flag="midpoint_within_15m",
            sample_name="primary_midpoint_15m",
            analysis_fields=fields,
        )


def test_primary_attrition_explicitly_retains_15_to_60_minute_rows():
    rows = [
        _price_row(),
        _price_row(
            ticker="T2",
            midpoint_observability_status="usable_midpoint_60m_only",
            trade_observability_status="no_trade",
        ),
    ]
    result = mutually_exclusive_attrition(rows)
    assert result["primary_midpoint_15m"]["included"] == 1
    assert result["primary_midpoint_15m"]["exclusion_reasons"] == {
        "usable_midpoint_60m_only": 1
    }


def test_support_diagnostics_apply_frozen_bin_and_subgroup_gates():
    rows = []
    for index in range(100):
        rows.append(
            _price_row(
                ticker=f"T{index}",
                family_id=f"F{index}",
                midpoint=0.05,
                yes_bid=0.04,
                yes_ask=0.06,
                spread=0.02,
            )
        )
    result = support_diagnostics(rows)
    bin_result = result["probability_decile"]["0.0-0.1"]
    assert bin_result["families"] == 100
    assert bin_result["probability_bin_gate_passed"] is True
    assert result["category"]["Sports"]["subgroup_gate_passed"] is False


def test_observability_balance_uses_only_ex_ante_fields_and_frozen_weights():
    rows = [
        _price_row(),
        _price_row(
            ticker="T2",
            family_id="F2",
            midpoint_within_15m=False,
            midpoint_observability_status="usable_midpoint_60m_only",
            hours_since_market_open=48,
        ),
    ]
    result = weighted_observability_balance(rows)
    assert result["observed_contracts"] == result["missing_contracts"] == 1
    assert result["observation_propensity_correction_applied"] is False
    assert result["contract_position"].startswith("not available")
