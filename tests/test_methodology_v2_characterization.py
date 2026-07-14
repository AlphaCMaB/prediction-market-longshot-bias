"""Characterization tests for the approved Methodology V2 behavior.

The numbered transition scripts are loaded by path because their filenames are
not valid Python module names. Imports are side-effect free: their network and
file-writing work is guarded by ``main()`` or explicit function calls.

Strict xfails identify approved behavior that the transition scripts do not yet
implement. They are executable documentation for the later consolidation.
"""

from __future__ import annotations

import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest


ROOT = Path(__file__).resolve().parents[1]


def load_script(number: str, filename: str):
    path = ROOT / "scripts" / f"{number}_{filename}.py"
    spec = importlib.util.spec_from_file_location(f"characterize_{number}", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


anchor_metadata = load_script("20", "pull_kalshi_event_anchor_metadata")
timing = load_script("21", "apply_occurrence_anchors_and_split")
anomalies = load_script("23", "audit_occurrence_anchor_anomalies")
horizons = load_script("24", "rebuild_clean_occurrence_horizon_manifests")
targets = load_script("25", "build_clean_price_target_manifest")
candles = load_script("26", "pull_clean_kalshi_candlesticks")


# Timestamp parsing


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("2026-07-01T12:30:00Z", "2026-07-01T12:30:00+00:00"),
        ("2026-07-01T20:30:00+08:00", "2026-07-01T12:30:00+00:00"),
        ("2026-07-01T12:30:00", "2026-07-01T12:30:00+00:00"),
    ],
)
def test_parse_time_normalizes_to_utc(value, expected):
    assert horizons.parse_time(value).isoformat() == expected


@pytest.mark.parametrize("value", [None, "", "not-a-timestamp"])
def test_parse_time_rejects_missing_or_invalid_values(value):
    assert horizons.parse_time(value) is None


# Anchor selection


def test_shared_occurrence_datetime_has_priority_over_other_metadata():
    event = {"strike_date": "2026-07-02T00:00:00Z", "strike_period": "day"}
    markets = [
        {
            "occurrence_datetime": "2026-07-01T12:00:00Z",
            "close_time": "2026-07-03T00:00:00Z",
        },
        {"occurrence_datetime": "2026-07-01T12:00:00Z"},
    ]
    anchor, source, confidence, _ = anchor_metadata.choose_anchor(event, markets)
    assert anchor == "2026-07-01T12:00:00+00:00"
    assert source == "market_occurrence_datetime"
    assert confidence == "high"


def test_close_time_is_never_selected_automatically():
    anchor, source, confidence, _ = anchor_metadata.choose_anchor(
        {}, [{"close_time": "2026-07-01T12:00:00Z"}]
    )
    assert (anchor, source, confidence) == ("", "", "none")


def test_unverified_strike_date_is_not_usable_by_v2_split():
    anchor, source, confidence, _ = anchor_metadata.choose_anchor(
        {"strike_date": "2026-07-01T12:00:00Z"}, [{}]
    )
    usable = bool(
        anchor
        and source == "market_occurrence_datetime"
        and confidence == "high"
    )
    assert usable is False


@pytest.mark.xfail(
    strict=True,
    reason="No production V2 function yet accepts a manually verified scheduled timestamp.",
)
def test_semantically_verified_scheduled_timestamp_can_be_selected():
    assert hasattr(anchor_metadata, "select_verified_scheduled_timestamp")


@pytest.mark.xfail(
    strict=True,
    reason="strike_date validation status is not yet an input to choose_anchor().",
)
def test_verified_strike_date_requires_explicit_semantic_validation():
    event = {
        "strike_date": "2026-07-01T12:00:00Z",
        "strike_date_semantically_validated": True,
    }
    _, _, confidence, _ = anchor_metadata.choose_anchor(event, [{}])
    assert confidence == "high"


# Timing classification


@pytest.mark.parametrize(
    ("ticker", "title", "expected"),
    [
        ("KXBTC-26JUL", "Bitcoin price at noon", "fixed_clock"),
        ("KXNONSPECIAL-26JUL", "Team A match Team B", "scheduled_event_start"),
        ("KXATP-26WIM", "Wimbledon tournament winner", "scheduled_window"),
        ("KXKOSPI-26JUL", "KOSPI action before deadline", "deadline_window"),
        ("KXATPSETWINNER-26JUL", "First set winner", "endogenous_subevent"),
    ],
)
def test_current_timing_classification_representatives(ticker, title, expected):
    assert timing.classify(ticker, title)[0] == expected


@pytest.mark.xfail(
    strict=True,
    reason="The transition classifier defaults unmatched markets to scheduled_event_start.",
)
def test_ambiguous_timing_is_classified_unclear():
    assert timing.classify("KXUNKNOWN-26JUL", "Ambiguous contract")[0] == "unclear"


# Occurrence-anchor anomaly auditing


def market_row(market_id, family_id, settlement, anchor="2026-07-01T12:00:00Z"):
    return {
        "market_id": market_id,
        "family_id_v2": family_id,
        "anchor_time_final_v2": anchor,
        "actual_settlement_time": settlement,
        "market_open_time": "2026-06-30T00:00:00Z",
    }


def test_settlement_more_than_15_minutes_early_flags_family():
    audit, clean, _ = anomalies.audit_rows(
        [market_row("m1", "f1", "2026-07-01T11:44:59Z")], "fixed_clock"
    )
    assert audit[0]["family_decision"] == "exclude_pending_manual_review"
    assert "settled_more_than_15m_before_occurrence" in audit[0]["family_reasons"]
    assert clean == []


def test_settlement_exactly_15_minutes_early_is_within_tolerance():
    audit, clean, _ = anomalies.audit_rows(
        [market_row("m1", "f1", "2026-07-01T11:45:00Z")], "fixed_clock"
    )
    assert audit[0]["family_decision"] == "keep_candidate"
    assert len(clean) == 1


def test_one_anomalous_member_excludes_entire_family_but_not_clean_family():
    rows = [
        market_row("m1", "bad-family", "2026-07-01T12:05:00Z"),
        market_row("m2", "bad-family", "2026-07-01T11:40:00Z"),
        market_row("m3", "clean-family", "2026-07-01T12:05:00Z"),
    ]
    audit, clean, stats = anomalies.audit_rows(rows, "scheduled_event_start")
    assert {r["market_id"] for r in clean} == {"m3"}
    assert {r["family_decision"] for r in audit if r["family_id_v2"] == "bad-family"} == {
        "exclude_pending_manual_review"
    }
    assert stats["clean_family_count"] == 1


# Horizon eligibility and family counts


def eligible_source(**overrides):
    row = {
        "market_id": "m1",
        "family_id_v2": "family-a",
        "market_open_time": "2026-06-30T00:00:00Z",
        "anchor_time_final_v2": "2026-07-01T12:00:00Z",
        "actual_settlement_time": "2026-07-01T12:10:00Z",
    }
    row.update(overrides)
    return row


def manifest_at(rows, horizon):
    return next(r for r in rows if r["horizon_hours"] == horizon)


def test_target_equals_anchor_minus_horizon_and_open_before_target_is_eligible():
    row = manifest_at(horizons.build_manifest([eligible_source()], "fixed_clock"), 6)
    assert row["target_time"] == "2026-07-01T06:00:00+00:00"
    assert row["eligible_clean"] == "1"


def test_open_after_target_is_ineligible():
    source = eligible_source(market_open_time="2026-07-01T11:30:00Z")
    row = manifest_at(horizons.build_manifest([source], "fixed_clock"), 1)
    assert row["eligibility_status_clean"] == "market_opened_after_target"


def test_missing_anchor_is_ineligible():
    row = manifest_at(
        horizons.build_manifest([eligible_source(anchor_time_final_v2="")], "fixed_clock"),
        1,
    )
    assert row["eligibility_status_clean"] == "missing_occurrence_anchor"


@pytest.mark.parametrize("settlement", ["2026-07-01T10:59:59Z", "2026-07-01T11:00:00Z"])
def test_settlement_before_or_at_target_is_ineligible(settlement):
    source = eligible_source(actual_settlement_time=settlement)
    row = manifest_at(horizons.build_manifest([source], "fixed_clock"), 1)
    assert row["eligibility_status_clean"] == "settled_before_or_at_target"


def test_sample_types_remain_separate():
    fixed = horizons.build_manifest([eligible_source()], "fixed_clock")
    scheduled = horizons.build_manifest([eligible_source()], "scheduled_event_start")
    assert {r["sample_type"] for r in fixed} == {"fixed_clock"}
    assert {r["sample_type"] for r in scheduled} == {"scheduled_event_start"}


def test_family_summary_counts_families_separately_from_contracts():
    rows = horizons.build_manifest(
        [eligible_source(market_id="m1"), eligible_source(market_id="m2")],
        "fixed_clock",
    )
    stats = horizons.summarize(rows)
    assert stats["contract_counts"][1] == 2
    assert stats["family_sets"][1] == {"family-a"}


# Price-target manifest


def target_row(sample, horizon, market="m1", family="family-a", **overrides):
    row = {
        "venue": "kalshi",
        "market_id": market,
        "family_id_analysis": family,
        "sample_type": sample,
        "horizon_hours": str(horizon),
        "target_time": f"2026-07-01T{12 - min(horizon, 12):02d}:00:00+00:00",
        "eligible_clean": "1",
    }
    row.update(overrides)
    return row


def test_price_targets_select_only_approved_horizons():
    fixed = [target_row("fixed_clock", h, market=f"f{h}") for h in [1, 6, 12, 24, 48]]
    scheduled = [
        target_row("scheduled_event_start", h, market=f"s{h}")
        for h in [1, 6, 12, 24, 48]
    ]
    result = targets.build_targets(fixed, scheduled)
    assert {(r["analysis_sample"], int(r["horizon_hours"])) for r in result} == {
        ("fixed_clock", 1),
        ("scheduled_event_start", 1),
        ("scheduled_event_start", 6),
        ("scheduled_event_start", 12),
    }


@pytest.mark.parametrize(
    "sample",
    ["scheduled_window", "deadline_window", "endogenous_subevent", "unclear"],
)
def test_excluded_timing_classes_never_enter_targets(sample):
    assert targets.build_targets([target_row(sample, 1)], []) == []


def test_duplicate_target_keys_are_removed_and_family_is_retained():
    duplicate = target_row("fixed_clock", 1, family="official-event-1")
    result = targets.build_targets([duplicate, dict(duplicate)], [])
    assert len(result) == 1
    assert result[0]["family_id_analysis"] == "official-event-1"


# Candlestick extraction


def test_selects_latest_candle_at_or_before_target_and_rejects_later_candles():
    items = [
        {"end_period_ts": 100, "name": "old"},
        {"end_period_ts": 200, "name": "exact"},
        {"end_period_ts": 201, "name": "future"},
    ]
    assert candles.select_candlestick(items, 200)["name"] == "exact"
    assert candles.select_candlestick([items[-1]], 200) is None


def test_price_priority_midpoint_then_trade_then_previous_then_missing():
    midpoint = candles.extract_prices(
        {
            "yes_bid": {"close_dollars": "0.40"},
            "yes_ask": {"close_dollars": "0.44"},
            "price": {"close_dollars": "0.50", "previous_dollars": "0.30"},
        }
    )
    trade = candles.extract_prices({"price": {"close_dollars": "0.50"}})
    previous = candles.extract_prices({"price": {"previous_dollars": "0.30"}})
    missing = candles.extract_prices({})
    assert (midpoint["p_hat_primary"], midpoint["price_source"]) == (
        pytest.approx(0.42),
        "yes_bid_ask_midpoint",
    )
    assert (trade["p_hat_primary"], trade["price_source"]) == (0.5, "trade_close")
    assert (previous["p_hat_primary"], previous["price_source"]) == (0.3, "previous_trade")
    assert (missing["p_hat_primary"], missing["price_source"]) == (None, "")


def test_staleness_is_target_minus_selected_candle_time():
    target = datetime(2026, 7, 1, 12, 0, tzinfo=timezone.utc)
    candle_time = target - timedelta(minutes=14, seconds=30)
    staleness = (target - candle_time).total_seconds() / 60.0
    assert staleness == pytest.approx(14.5)


@pytest.mark.parametrize(
    ("minutes", "bucket"),
    [
        (None, "missing"),
        (5, "0-5m"),
        (5.0001, "5-15m"),
        (15, "5-15m"),
        (15.0001, "15-60m"),
        (60, "15-60m"),
        (60.0001, "1-3h"),
        (180, "1-3h"),
        (180.0001, "3-6h"),
        (360, "3-6h"),
        (360.0001, ">6h"),
    ],
)
def test_staleness_bucket_boundaries(minutes, bucket):
    assert candles.staleness_bucket(minutes) == bucket


def test_cache_path_is_deterministic_and_ticker_order_independent(monkeypatch, tmp_path):
    monkeypatch.setattr(candles, "RAW_DIR", tmp_path)
    first = candles.cache_path(["B", "A"], 100, 200)
    second = candles.cache_path(["A", "B"], 100, 200)
    assert first == second
    assert first.parent == tmp_path


# Family-level behavior


def test_related_contracts_share_official_family_identifier():
    rows = [
        {"market_id": "event-A", "family_id_v2": "kalshi_event::EVENT"},
        {"market_id": "event-B", "family_id_v2": "kalshi_event::EVENT"},
    ]
    assert {anomalies.get_family_id(row) for row in rows} == {"kalshi_event::EVENT"}


def test_family_identifier_is_the_characterized_resampling_unit():
    contracts = [
        {"market_id": "a", "family_id_analysis": "family-1"},
        {"market_id": "b", "family_id_analysis": "family-1"},
        {"market_id": "c", "family_id_analysis": "family-2"},
    ]
    bootstrap_units = {candles.family_id(row) for row in contracts}
    assert bootstrap_units == {"family-1", "family-2"}
    assert len(bootstrap_units) != len(contracts)
