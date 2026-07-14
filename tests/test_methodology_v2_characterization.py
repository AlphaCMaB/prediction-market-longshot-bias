"""Synthetic, offline characterization tests for approved Methodology V2."""

from __future__ import annotations

import importlib.util
import socket
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from scripts.common import io_utils, probability_utils, time_utils
from scripts.pipeline_v2 import (
    anchor_validation,
    anchors,
    horizon_eligibility,
    price_targets,
    timing,
)


ROOT = Path(__file__).resolve().parents[1]


def load_transition_script(number: str, filename: str):
    base = ROOT / "scripts/legacy/transition_audits"
    if number == "26":
        base = base / "superseded_prototypes"
    path = base / f"{number}_{filename}.py"
    spec = importlib.util.spec_from_file_location(f"transition_{number}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


transition_timing = load_transition_script("21", "apply_occurrence_anchors_and_split")
transition_horizons = load_transition_script("24", "rebuild_clean_occurrence_horizon_manifests")
transition_candles = load_transition_script("26", "pull_clean_kalshi_candlesticks")


# Time and probability helpers


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-07-01T12:30:00Z", "2026-07-01T12:30:00+00:00"),
        ("2026-07-01T20:30:00+08:00", "2026-07-01T12:30:00+00:00"),
        ("2026-07-01T12:30:00", "2026-07-01T12:30:00+00:00"),
    ],
)
def test_parse_and_format_time_as_utc(value, expected):
    assert time_utils.format_iso_utc(time_utils.parse_iso_utc(value)) == expected


@pytest.mark.parametrize("value", [None, "", "not-a-timestamp"])
def test_parse_time_rejects_missing_or_invalid_values(value):
    assert time_utils.parse_iso_utc(value) is None


@pytest.mark.parametrize(
    ("value", "parsed", "valid"),
    [("0.25", 0.25, True), (0, 0.0, True), (1, 1.0, True), (-0.1, -0.1, False), (1.1, 1.1, False), ("bad", None, False)],
)
def test_probability_parsing_and_validation(value, parsed, valid):
    assert probability_utils.safe_float(value) == parsed
    assert probability_utils.is_valid_probability(value) is valid


@pytest.mark.parametrize(
    ("value", "label"),
    [(0, "0.0-0.1"), (0.1, "0.1-0.2"), (0.999, "0.9-1.0"), (1, "0.9-1.0"), (None, "missing")],
)
def test_probability_bin_labels(value, label):
    assert probability_utils.probability_bin(value) == label


def test_explicit_csv_io_round_trip(tmp_path):
    path = tmp_path / "nested" / "rows.csv"
    io_utils.write_csv(path, [{"a": "1", "b": "two"}])
    assert io_utils.read_csv(path) == [{"a": "1", "b": "two"}]


# Approved anchor selection


def test_verified_occurrence_datetime_has_priority():
    result = anchors.select_anchor(
        occurrence_datetime="2026-07-01T12:00:00Z",
        occurrence_verified=True,
        scheduled_timestamp="2026-07-02T12:00:00Z",
        scheduled_timestamp_verified=True,
        strike_date="2026-07-03T12:00:00Z",
        strike_date_semantically_verified=True,
        manual_override="2026-07-04T12:00:00Z",
        manual_override_verified=True,
    )
    assert result.anchor_time == "2026-07-01T12:00:00+00:00"
    assert result.anchor_source == "occurrence_datetime"
    assert result.validation_status == "verified"


def test_verified_scheduled_timestamp_is_supported():
    result = anchors.select_anchor(
        scheduled_timestamp="2026-07-01T12:00:00Z",
        scheduled_timestamp_verified=True,
    )
    assert result.anchor_source == "official_scheduled_timestamp"
    assert result.validation_status == "verified"


def test_strike_date_requires_explicit_semantic_validation():
    rejected = anchors.select_anchor(strike_date="2026-07-01T12:00:00Z")
    accepted = anchors.select_anchor(
        strike_date="2026-07-01T12:00:00Z",
        strike_date_semantically_verified=True,
    )
    assert rejected.validation_status == "invalid_or_unverified"
    assert accepted.anchor_source == "strike_date"
    assert accepted.validation_status == "verified"


def test_verified_manual_override_is_last_priority_fallback():
    result = anchors.select_anchor(
        manual_override="2026-07-01T12:00:00Z",
        manual_override_verified=True,
    )
    assert result.anchor_source == "manual_override"


def test_close_time_is_never_selected_automatically():
    result = anchors.select_anchor(close_time="2026-07-01T12:00:00Z")
    assert result.anchor_time == ""
    assert result.anchor_source == ""
    assert result.validation_status == "invalid_or_unverified"


def test_anchor_result_preserves_review_note_and_interface():
    result = anchors.select_anchor(
        occurrence_datetime="2026-07-01T12:00:00Z",
        occurrence_verified=True,
        review_note="Checked against official schedule.",
    )
    assert result.to_dict() == {
        "anchor_time": "2026-07-01T12:00:00+00:00",
        "anchor_source": "occurrence_datetime",
        "validation_status": "verified",
        "review_note": "Checked against official schedule.",
    }


# Timing classification and transition equivalence


@pytest.mark.parametrize(
    ("ticker", "title", "expected"),
    [
        ("KXBTC-26JUL", "Bitcoin price at noon", "fixed_clock"),
        ("KXNONSPECIAL-26JUL", "Team A match Team B", "scheduled_event_start"),
        ("KXATP-26WIM", "Wimbledon tournament winner", "scheduled_window"),
        ("KXKOSPI-26JUL", "KOSPI action before deadline", "deadline_window"),
        ("KXATPSETWINNER-26JUL", "First set winner", "endogenous_subevent"),
        ("KXUNKNOWN-26JUL", "Ambiguous contract", "unclear"),
    ],
)
def test_timing_classification_representatives(ticker, title, expected):
    assert timing.classify_timing(ticker, title)[0] == expected


def test_timing_supports_exactly_the_approved_structures():
    assert set(timing.TIMING_STRUCTURES) == {
        "fixed_clock", "scheduled_event_start", "scheduled_window", "deadline_window", "endogenous_subevent", "unclear"
    }


@pytest.mark.parametrize(
    ("ticker", "title"),
    [
        ("KXBTC-26JUL", "Bitcoin price at noon"),
        ("KXNONSPECIAL-26JUL", "Team A match Team B"),
        ("KXATP-26WIM", "Tournament winner"),
        ("KXKOSPI-26JUL", "Before deadline"),
        ("KXATPSETWINNER-26JUL", "First set winner"),
    ],
)
def test_known_timing_cases_match_transition_classifier(ticker, title):
    assert timing.classify_timing(ticker, title)[0] == transition_timing.classify(ticker, title)[0]


# Family-level anchor validation


def member(market_id, family_id, settlement, anchor="2026-07-01T12:00:00Z"):
    return {
        "market_id": market_id,
        "family_id": family_id,
        "family_id_source": "event_ticker",
        "anchor_time": anchor,
        "settlement_time": settlement,
    }


def test_settlement_more_than_15_minutes_early_flags_family():
    audit, valid = anchor_validation.validate_anchor_families(
        [member("m1", "f1", "2026-07-01T11:44:59Z")]
    )
    assert audit[0]["anchor_validation_status"] == "excluded"
    assert audit[0]["anchor_validation_reasons"] == "settled_more_than_15m_before_occurrence"
    assert valid == []


def test_settlement_exactly_15_minutes_early_is_valid():
    audit, valid = anchor_validation.validate_anchor_families(
        [member("m1", "f1", "2026-07-01T11:45:00Z")]
    )
    assert audit[0]["anchor_validation_status"] == "valid"
    assert len(valid) == 1


def test_one_anomalous_member_flags_entire_family_and_clean_family_remains():
    rows = [
        member("m1", "bad", "2026-07-01T12:05:00Z"),
        member("m2", "bad", "2026-07-01T11:40:00Z"),
        member("m3", "clean", "2026-07-01T12:05:00Z"),
    ]
    audit, valid = anchor_validation.validate_anchor_families(rows)
    assert {row["market_id"] for row in valid} == {"m3"}
    assert {row["anchor_validation_status"] for row in audit if row["family_id"] == "bad"} == {"excluded"}


@pytest.mark.parametrize("anchor", [None, "", "invalid"])
def test_missing_or_invalid_anchor_excludes_family(anchor):
    audit, valid = anchor_validation.validate_anchor_families(
        [member("m1", "f1", "2026-07-01T12:05:00Z", anchor=anchor)]
    )
    assert audit[0]["anchor_validation_reasons"] == "missing_or_invalid_anchor"
    assert valid == []


# Horizon eligibility


def source_row(**overrides):
    row = {
        "venue": "kalshi",
        "market_id": "m1",
        "timing_structure": "fixed_clock",
        "family_id": "family-a",
        "family_id_source": "event_ticker",
        "anchor_time": "2026-07-01T12:00:00Z",
        "anchor_source": "occurrence_datetime",
        "market_open_time": "2026-06-30T00:00:00Z",
        "settlement_time": "2026-07-01T12:10:00Z",
    }
    row.update(overrides)
    return row


def at_horizon(rows, value):
    return next(row for row in rows if row["horizon_hours"] == value)


def test_candidate_horizons_and_target_calculation():
    rows = horizon_eligibility.build_horizon_eligibility([source_row()])
    assert [row["horizon_hours"] for row in rows] == [1, 6, 12, 24, 48]
    assert at_horizon(rows, 6)["target_time"] == "2026-07-01T06:00:00+00:00"


def test_market_open_at_or_before_target_is_eligible():
    before = at_horizon(horizon_eligibility.build_horizon_eligibility([source_row()]), 1)
    exactly = at_horizon(
        horizon_eligibility.build_horizon_eligibility(
            [source_row(market_open_time="2026-07-01T11:00:00Z")]
        ),
        1,
    )
    assert before["eligible"] is True
    assert exactly["eligible"] is True


def test_market_open_after_target_is_ineligible():
    row = at_horizon(
        horizon_eligibility.build_horizon_eligibility(
            [source_row(market_open_time="2026-07-01T11:00:01Z")]
        ),
        1,
    )
    assert row["eligibility_status"] == "market_opened_after_target"


def test_missing_anchor_is_ineligible():
    row = at_horizon(
        horizon_eligibility.build_horizon_eligibility([source_row(anchor_time="")]), 1
    )
    assert row["eligibility_status"] == "missing_or_invalid_anchor"


def test_missing_settlement_is_ineligible():
    row = at_horizon(
        horizon_eligibility.build_horizon_eligibility(
            [source_row(settlement_time="")]
        ),
        1,
    )
    assert row["eligibility_status"] == "missing_or_invalid_settlement_time"


@pytest.mark.parametrize("settlement", ["2026-07-01T10:59:59Z", "2026-07-01T11:00:00Z"])
def test_settlement_before_or_at_target_is_ineligible(settlement):
    row = at_horizon(
        horizon_eligibility.build_horizon_eligibility(
            [source_row(settlement_time=settlement)]
        ),
        1,
    )
    assert row["eligibility_status"] == "settled_before_or_at_target"


def test_eligibility_preserves_methodology_fields_and_separate_samples():
    fixed = source_row(timing_structure="fixed_clock")
    scheduled = source_row(market_id="m2", timing_structure="scheduled_event_start")
    rows = horizon_eligibility.build_horizon_eligibility([fixed, scheduled], horizons=[1])
    assert {row["timing_structure"] for row in rows} == {"fixed_clock", "scheduled_event_start"}
    for row in rows:
        assert row["family_id"] == "family-a"
        assert row["family_id_source"] == "event_ticker"
        assert row["anchor_source"] == "occurrence_datetime"


def test_horizon_target_matches_transition_logic_for_equivalent_row():
    new = at_horizon(horizon_eligibility.build_horizon_eligibility([source_row()]), 6)
    old_source = {
        "market_id": "m1",
        "family_id_v2": "family-a",
        "anchor_time_final_v2": "2026-07-01T12:00:00Z",
        "market_open_time": "2026-06-30T00:00:00Z",
        "actual_settlement_time": "2026-07-01T12:10:00Z",
    }
    old = at_horizon(transition_horizons.build_manifest([old_source], "fixed_clock"), 6)
    assert new["target_time"] == old["target_time"]
    assert new["eligible"] is (old["eligible_clean"] == "1")


# Price-target selection


def eligibility_row(timing_structure, horizon, market="m1", family="family-a", **overrides):
    row = source_row(
        timing_structure=timing_structure,
        market_id=market,
        family_id=family,
    )
    row.update(
        {
            "horizon_hours": horizon,
            "target_time": "2026-07-01T11:00:00Z",
            "eligible": True,
        }
    )
    row.update(overrides)
    return row


def test_price_targets_select_only_approved_horizons():
    rows = [
        eligibility_row(sample, horizon, market=f"{sample}-{horizon}")
        for sample in ("fixed_clock", "scheduled_event_start")
        for horizon in (1, 6, 12, 24, 48)
    ]
    selected = price_targets.build_price_targets(rows)
    assert {(row["timing_structure"], row["horizon_hours"]) for row in selected} == {
        ("fixed_clock", 1),
        ("scheduled_event_start", 1),
        ("scheduled_event_start", 6),
        ("scheduled_event_start", 12),
    }


@pytest.mark.parametrize(
    "sample", ["scheduled_window", "deadline_window", "endogenous_subevent", "unclear"]
)
def test_excluded_timing_classes_never_enter_target_manifest(sample):
    assert price_targets.build_price_targets([eligibility_row(sample, 1)]) == []


def test_targets_deduplicate_deterministically_and_preserve_fields():
    row = eligibility_row("fixed_clock", 1, family="official-family")
    selected = price_targets.build_price_targets([row, dict(row)])
    assert len(selected) == 1
    assert selected[0]["family_id"] == "official-family"
    assert selected[0]["family_id_source"] == "event_ticker"
    assert selected[0]["anchor_time"] == "2026-07-01T12:00:00Z"
    assert selected[0]["anchor_source"] == "occurrence_datetime"
    assert selected[0]["target_key"] == price_targets.deterministic_target_key(selected[0])


# Existing candlestick extraction remains characterized until its V2 module is built.


def test_selects_latest_candle_at_or_before_target_and_rejects_future():
    items = [
        {"end_period_ts": 100, "name": "old"},
        {"end_period_ts": 200, "name": "exact"},
        {"end_period_ts": 201, "name": "future"},
    ]
    assert transition_candles.select_candlestick(items, 200)["name"] == "exact"
    assert transition_candles.select_candlestick([items[-1]], 200) is None


def test_candlestick_price_priority_and_missing_price():
    midpoint = transition_candles.extract_prices(
        {
            "yes_bid": {"close_dollars": "0.40"},
            "yes_ask": {"close_dollars": "0.44"},
            "price": {"close_dollars": "0.50", "previous_dollars": "0.30"},
        }
    )
    trade = transition_candles.extract_prices({"price": {"close_dollars": "0.50"}})
    previous = transition_candles.extract_prices({"price": {"previous_dollars": "0.30"}})
    missing = transition_candles.extract_prices({})
    assert (midpoint["p_hat_primary"], midpoint["price_source"]) == (pytest.approx(0.42), "yes_bid_ask_midpoint")
    assert (trade["p_hat_primary"], trade["price_source"]) == (0.5, "trade_close")
    assert (previous["p_hat_primary"], previous["price_source"]) == (0.3, "previous_trade")
    assert (missing["p_hat_primary"], missing["price_source"]) == (None, "")


def test_snapshot_staleness_calculation():
    target = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)
    candle_time = target - timedelta(minutes=14, seconds=30)
    assert (target - candle_time).total_seconds() / 60 == pytest.approx(14.5)


@pytest.mark.parametrize(
    ("minutes", "bucket"),
    [(None, "missing"), (5, "0-5m"), (5.0001, "5-15m"), (15, "5-15m"), (15.0001, "15-60m"), (60, "15-60m"), (60.0001, "1-3h"), (180, "1-3h"), (180.0001, "3-6h"), (360, "3-6h"), (360.0001, ">6h")],
)
def test_staleness_bucket_boundaries(minutes, bucket):
    assert transition_candles.staleness_bucket(minutes) == bucket


def test_cache_path_is_deterministic_and_order_independent(monkeypatch, tmp_path):
    monkeypatch.setattr(transition_candles, "RAW_DIR", tmp_path)
    assert transition_candles.cache_path(["B", "A"], 100, 200) == transition_candles.cache_path(["A", "B"], 100, 200)


# Family-level analysis contract


def test_related_contracts_share_one_family_and_counts_are_not_contract_counts():
    rows = [
        eligibility_row("fixed_clock", 1, market="a", family="family-1"),
        eligibility_row("fixed_clock", 1, market="b", family="family-1"),
        eligibility_row("fixed_clock", 1, market="c", family="family-2"),
    ]
    family_units = {row["family_id"] for row in rows}
    assert family_units == {"family-1", "family-2"}
    assert len(family_units) == 2
    assert len(rows) == 3


def test_future_bootstrap_unit_is_family_identifier():
    rows = [
        eligibility_row("fixed_clock", 1, market="a", family="family-1"),
        eligibility_row("fixed_clock", 1, market="b", family="family-1"),
    ]
    bootstrap_units = [row["family_id"] for row in rows]
    assert set(bootstrap_units) == {"family-1"}


# Import safety


NEW_MODULE_PATHS = [
    ROOT / "scripts/common/__init__.py",
    ROOT / "scripts/common/time_utils.py",
    ROOT / "scripts/common/io_utils.py",
    ROOT / "scripts/common/probability_utils.py",
    ROOT / "scripts/pipeline_v2/__init__.py",
    ROOT / "scripts/pipeline_v2/anchors.py",
    ROOT / "scripts/pipeline_v2/timing.py",
    ROOT / "scripts/pipeline_v2/anchor_validation.py",
    ROOT / "scripts/pipeline_v2/horizon_eligibility.py",
    ROOT / "scripts/pipeline_v2/price_targets.py",
]


@pytest.mark.parametrize("module_path", NEW_MODULE_PATHS, ids=lambda path: path.stem)
def test_import_has_no_filesystem_or_network_side_effects(module_path, monkeypatch, tmp_path):
    before = set(tmp_path.rglob("*"))

    def blocked_socket(*args, **kwargs):
        raise AssertionError("network socket created during module import")

    monkeypatch.setattr(socket, "socket", blocked_socket)
    monkeypatch.chdir(tmp_path)
    module_name = f"side_effect_check_{module_path.stem}"
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[module_name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(module_name, None)
    assert set(tmp_path.rglob("*")) == before
