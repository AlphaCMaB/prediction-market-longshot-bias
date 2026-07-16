from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.pipeline_v2.anchor_evidence import (
    build_anchor_evidence,
    parse_candidate_value,
)
from scripts.pipeline_v2.study_rules import load_study_rules


CONFIG = Path(__file__).parents[1] / "configs" / "pipeline_v2.toml"


def market(**overrides):
    row = {
        "ticker": "M1", "family_id": "F1", "family_id_source": "source_a",
        "event_ticker": "E1", "title": "Market", "market_open_time": "2025-06-01T00:00:00Z",
        "occurrence_datetime": "2025-08-01T12:00:00-04:00",
    }
    row.update(overrides)
    return row


def event(**overrides):
    row = {
        "event_ticker": "E1", "title": "Event", "category": "Sports",
        "strike_date": "2025-08-01",
    }
    row.update(overrides)
    return row


def milestone(**overrides):
    row = {
        "event_ticker": "E1", "milestone_id": "MS1", "milestone_title": "Start",
        "milestone_start_date": "2025-08-01T16:00:00Z",
        "milestone_end_date": "2025-08-01T18:00:00Z",
        "association_type": "primary_event_tickers",
    }
    row.update(overrides)
    return row


def build(markets=None, events=None, milestones=None):
    return build_anchor_evidence(
        markets if markets is not None else [market()],
        events if events is not None else [event()],
        milestones if milestones is not None else [milestone()],
        load_study_rules(CONFIG),
    )


def test_candidate_timestamp_and_date_precision():
    exact = parse_candidate_value("2025-08-01T12:00:00-04:00", allow_date_only=False)
    assert exact.candidate_time_utc == "2025-08-01T16:00:00Z"
    assert exact.precision == "exact_timestamp"
    date_only = parse_candidate_value("2025-08-01", allow_date_only=True)
    assert date_only.candidate_date == "2025-08-01"
    assert date_only.candidate_time_utc == ""
    assert date_only.precision == "date_only"


@pytest.mark.parametrize(
    "value",
    ["bad", "2025-08-01T12:00:00", "2025-02-30", " 2025-08-01", "2025-08-01T12:00:00Z "],
)
def test_invalid_values_are_not_repaired(value):
    parsed = parse_candidate_value(value, allow_date_only=True)
    assert not parsed.valid
    assert parsed.issue == "invalid_candidate_value"


def test_year_one_is_sentinel_not_candidate():
    parsed = parse_candidate_value("0001-01-01T00:00:00Z", allow_date_only=False)
    assert not parsed.valid
    assert parsed.issue == "sentinel_timestamp"
    built = build(markets=[market(occurrence_datetime="0001-01-01T00:00:00Z")])
    assert all(row["candidate_source_type"] != "market_occurrence_datetime" for row in built.evidence_rows)
    assert built.statistics["sentinel_timestamp_count"] == 1


@pytest.mark.parametrize(
    "value",
    [
        "0001-01-01T00:00:00Z",
        "0001-01-01T00:00:00+01:00",
        "0001-01-01T00:00:00-01:00",
        "0001-12-31T23:59:59+14:00",
    ],
)
def test_aware_year_one_offsets_are_sentinels_without_conversion_crash(value):
    parsed = parse_candidate_value(value, allow_date_only=False)
    assert not parsed.valid
    assert parsed.issue == "sentinel_timestamp"


@pytest.mark.parametrize(
    "value", ["0001-01-01T00:00:00", "0000-01-01T00:00:00Z", "9999-12-31T23:59:59-14:00"]
)
def test_naive_year_one_and_range_invalid_values_are_invalid(value):
    parsed = parse_candidate_value(value, allow_date_only=False)
    assert not parsed.valid
    assert parsed.issue == "invalid_candidate_value"


def test_ordinary_offset_timestamp_still_normalizes():
    parsed = parse_candidate_value("2025-08-01T12:00:00+01:00", allow_date_only=False)
    assert parsed.candidate_time_utc == "2025-08-01T11:00:00Z"


def test_all_three_allowed_candidate_sources_and_window_status():
    built = build()
    by_type = {row["candidate_source_type"]: row for row in built.evidence_rows}
    assert set(by_type) == {
        "market_occurrence_datetime", "event_strike_date",
        "event_milestone_start_date",
    }
    assert by_type["market_occurrence_datetime"]["candidate_time_utc"] == "2025-08-01T16:00:00Z"
    assert by_type["event_strike_date"]["analysis_window_status"] == "date_only_unknown"
    assert by_type["event_milestone_start_date"]["analysis_window_status"] == "inside_analysis_window"
    assert {row["review_status"] for row in built.evidence_rows} == {"needs_review"}


def test_forbidden_and_update_timestamps_never_become_candidates():
    built = build(
        markets=[market(
            occurrence_datetime="", market_open_time="2025-08-01T01:00:00Z",
            close_time="2025-08-01T02:00:00Z", expiration_time="2025-08-01T03:00:00Z",
            diagnostic_settlement_ts="2025-08-01T04:00:00Z",
        )],
        events=[event(strike_date="", last_updated_ts="2025-08-01T05:00:00Z")],
        milestones=[milestone(
            milestone_start_date="", milestone_end_date="2025-08-01T06:00:00Z",
            milestone_last_updated_ts="2025-08-01T07:00:00Z",
        )],
    )
    assert built.evidence_rows == ()
    assert built.family_rows[0]["review_reason"] == "no_candidate_anchor_evidence"


def test_composite_family_namespaces_are_independent():
    built = build(
        markets=[
            market(ticker="A", family_id_source="source_a"),
            market(ticker="B", family_id_source="source_b", occurrence_datetime="2025-09-01T00:00:00Z"),
        ]
    )
    assert built.statistics["family_count"] == 2
    assert {(row["family_id"], row["family_id_source"]) for row in built.family_rows} == {
        ("F1", "source_a"), ("F1", "source_b")
    }
    assert all(
        row["family_id_source"] in {"source_a", "source_b"}
        for row in built.evidence_rows
    )


def test_multiple_times_and_event_tickers_are_flagged():
    built = build(
        markets=[
            market(ticker="A", event_ticker="E1"),
            market(ticker="B", event_ticker="E2", occurrence_datetime="2025-09-01T00:00:00Z"),
        ],
        events=[event(), event(event_ticker="E2", strike_date="")],
        milestones=[],
    )
    review = built.family_rows[0]
    assert review["has_multiple_event_tickers"] == "true"
    assert review["has_conflicting_exact_candidate_times"] == "true"
    assert review["review_reason"] == "multiple_event_tickers"
    assert json.loads(review["event_tickers_json"]) == ["E1", "E2"]


def test_missing_event_metadata_preserves_family():
    built = build(events=[], milestones=[])
    assert len(built.family_rows) == 1
    assert built.family_rows[0]["missing_event_metadata"] == "true"
    assert built.family_rows[0]["review_reason"] == "missing_event_metadata"
    assert len(built.decision_rows) == 1


def test_milestone_evidence_remains_traceable_when_event_metadata_is_missing():
    built = build(events=[], milestones=[milestone()])
    assert built.family_rows[0]["missing_event_metadata"] == "true"
    assert any(
        row["candidate_source_type"] == "event_milestone_start_date"
        for row in built.evidence_rows
    )


def test_repeated_equivalent_candidate_is_deduplicated():
    built = build(markets=[market(ticker="M1"), market(ticker="M1")])
    occurrence = [
        row for row in built.evidence_rows
        if row["candidate_source_type"] == "market_occurrence_datetime"
    ]
    assert len(occurrence) == 1


def test_same_candidate_id_with_conflicting_title_fails():
    with pytest.raises(ValueError, match="conflicting candidate duplicate"):
        build(markets=[market(title="One"), market(title="Two")])


def test_same_candidate_id_with_conflicting_context_fails():
    with pytest.raises(ValueError, match="conflicting candidate duplicate"):
        build(markets=[market(subtitle="One"), market(subtitle="Two")])


def test_event_ticker_participates_in_candidate_identity():
    built = build(
        markets=[
            market(ticker="SHARED", event_ticker="E1"),
            market(ticker="SHARED", event_ticker="E2"),
        ],
        events=[event(), event(event_ticker="E2")],
        milestones=[],
    )
    occurrence = [
        row for row in built.evidence_rows
        if row["candidate_source_type"] == "market_occurrence_datetime"
    ]
    assert len(occurrence) == 2
    assert len({row["candidate_id"] for row in occurrence}) == 2


def test_decision_rows_are_unverified_and_blank():
    row = build().decision_rows[0]
    assert row["verification_status"] == "needs_review"
    assert row["verified_anchor_time"] == ""
    assert row["verified_anchor_source"] == ""
    assert row["timing_structure"] == ""
    assert row["evidence_reference"] == ""


def test_analysis_window_boundaries_are_descriptive_only():
    built = build(
        markets=[
            market(ticker="A", occurrence_datetime="2025-06-30T23:59:59Z"),
            market(ticker="B", occurrence_datetime="2025-07-01T00:00:00Z"),
            market(ticker="C", occurrence_datetime="2026-07-01T00:00:00Z"),
        ]
    )
    statuses = {
        row["candidate_original_value"]: row["analysis_window_status"]
        for row in built.evidence_rows
        if row["candidate_source_type"] == "market_occurrence_datetime"
    }
    assert statuses["2025-06-30T23:59:59Z"] == "before_analysis_window"
    assert statuses["2025-07-01T00:00:00Z"] == "inside_analysis_window"
    assert statuses["2026-07-01T00:00:00Z"] == "at_or_after_analysis_window"
    assert all(row["review_status"] == "needs_review" for row in built.evidence_rows)


def test_conflicting_duplicate_milestone_fails():
    with pytest.raises(ValueError, match="conflicting duplicate milestone"):
        build(milestones=[milestone(), milestone(milestone_title="Different")])
