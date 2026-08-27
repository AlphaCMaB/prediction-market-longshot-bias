from __future__ import annotations

import csv
import hashlib
from pathlib import Path

import pytest

import scripts.pipeline_v2.build_kalshi_anchor_evidence as evidence_builder
from scripts.common.io_utils import write_csv
from scripts.pipeline_v2.anchor_evidence import (
    ANCHOR_EVIDENCE_FIELDS,
    ANCHOR_FAMILY_REVIEW_FIELDS,
    DECISION_TEMPLATE_FIELDS,
)
from scripts.pipeline_v2.apply_anchor_verification import (
    apply_verification,
    validate_decisions,
)
from scripts.pipeline_v2.build_kalshi_anchor_evidence import run
from scripts.pipeline_v2.build_occurrence_anchors import (
    run as run_occurrence_anchors,
)


CONFIG = Path(__file__).parents[1] / "configs" / "pipeline_v2.toml"
MARKET_FIELDS = (
    "ticker",
    "family_id",
    "family_id_source",
    "event_ticker",
    "title",
    "subtitle",
    "rules_primary",
    "rules_secondary",
    "market_open_time",
    "occurrence_datetime",
    "diagnostic_settlement_ts",
    "close_time",
    "expiration_time",
)
EVENT_FIELDS = (
    "event_ticker",
    "series_ticker",
    "title",
    "sub_title",
    "category",
    "strike_date",
    "strike_period",
    "mutually_exclusive",
    "settlement_sources_json",
    "product_metadata_json",
    "last_updated_ts",
)
MILESTONE_FIELDS = (
    "event_ticker",
    "milestone_id",
    "milestone_category",
    "milestone_type",
    "milestone_title",
    "milestone_start_date",
    "milestone_end_date",
    "milestone_source_id",
    "milestone_source_ids_json",
    "milestone_details_json",
    "milestone_last_updated_ts",
    "association_type",
)


def rows(path):
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def fixtures(root: Path, *, reverse=False, retrospective="one"):
    markets = [
        {
            "ticker": "M1",
            "family_id": "F1",
            "family_id_source": "source_a",
            "event_ticker": "E1",
            "title": "Market 1",
            "subtitle": "",
            "rules_primary": "Rules",
            "rules_secondary": "",
            "market_open_time": "2025-06-01T00:00:00Z",
            "occurrence_datetime": "2025-08-01T12:00:00Z",
            "diagnostic_settlement_ts": f"2026-01-0{1 if retrospective == 'one' else 2}T00:00:00Z",
            "close_time": retrospective,
            "expiration_time": retrospective,
        },
        {
            "ticker": "M2",
            "family_id": "F1",
            "family_id_source": "source_b",
            "event_ticker": "E2",
            "title": "Market 2",
            "subtitle": "",
            "rules_primary": "Rules",
            "rules_secondary": "",
            "market_open_time": "2025-06-02T00:00:00Z",
            "occurrence_datetime": "",
            "diagnostic_settlement_ts": retrospective,
            "close_time": retrospective,
            "expiration_time": retrospective,
        },
    ]
    events = [
        {
            "event_ticker": "E1",
            "series_ticker": "S1",
            "title": "Event 1",
            "sub_title": "",
            "category": "Sports",
            "strike_date": "2025-08-01",
            "strike_period": "",
            "mutually_exclusive": "",
            "settlement_sources_json": "[]",
            "product_metadata_json": "{}",
            "last_updated_ts": retrospective,
        },
        {
            "event_ticker": "E2",
            "series_ticker": "S2",
            "title": "Event 2",
            "sub_title": "",
            "category": "Economics",
            "strike_date": "",
            "strike_period": "",
            "mutually_exclusive": "",
            "settlement_sources_json": "[]",
            "product_metadata_json": "{}",
            "last_updated_ts": retrospective,
        },
    ]
    milestones = [
        {
            "event_ticker": "E1",
            "milestone_id": "MS1",
            "milestone_category": "event",
            "milestone_type": "start",
            "milestone_title": "Start",
            "milestone_start_date": "2025-08-01T12:00:00Z",
            "milestone_end_date": "2025-08-01T13:00:00Z",
            "milestone_source_id": "",
            "milestone_source_ids_json": "[]",
            "milestone_details_json": "{}",
            "milestone_last_updated_ts": retrospective,
            "association_type": "both",
        }
    ]
    if reverse:
        markets.reverse()
        events.reverse()
    paths = (root / "markets.csv", root / "events.csv", root / "milestones.csv")
    write_csv(paths[0], markets, fieldnames=MARKET_FIELDS)
    write_csv(paths[1], events, fieldnames=EVENT_FIELDS)
    write_csv(paths[2], milestones, fieldnames=MILESTONE_FIELDS)
    return paths


def test_outputs_schemas_hashes_and_composite_counts(tmp_path):
    inputs = fixtures(tmp_path)
    output = tmp_path / "output"
    report = run(*inputs, output, config_path=CONFIG)
    assert tuple(rows(output / "anchor_evidence.csv")[0]) == ANCHOR_EVIDENCE_FIELDS
    assert (
        tuple(rows(output / "anchor_family_review.csv")[0])
        == ANCHOR_FAMILY_REVIEW_FIELDS
    )
    assert (
        tuple(rows(output / "anchor_verification_decisions_template.csv")[0])
        == DECISION_TEMPLATE_FIELDS
    )
    assert report["family_count"] == 2
    for name, digest in report["output_hashes"].items():
        assert hashlib.sha256((output / name).read_bytes()).hexdigest() == digest


def test_template_is_accepted_and_leaves_all_families_unverified(tmp_path):
    inputs = fixtures(tmp_path)
    output = tmp_path / "output"
    run(*inputs, output, config_path=CONFIG)
    decision_rows = rows(output / "anchor_verification_decisions_template.csv")
    decisions = validate_decisions(decision_rows, DECISION_TEMPLATE_FIELDS)
    applied = apply_verification(rows(inputs[0]), decisions)
    assert {row["verification_status"] for row in applied} == {"needs_review"}
    assert all(not row["verified_anchor_time"] for row in applied)
    applied_path = tmp_path / "applied-unedited.csv"
    applied_fields = tuple(
        dict.fromkeys(
            (
                *MARKET_FIELDS,
                *(
                    "verification_status",
                    "verified_anchor_time",
                    "verified_anchor_source",
                    "timing_structure_reviewed",
                    "evidence_reference",
                    "review_note",
                ),
            )
        )
    )
    write_csv(applied_path, applied, fieldnames=applied_fields)
    anchors_path = tmp_path / "anchors-unedited.csv"
    run_occurrence_anchors(applied_path, anchors_path)
    assert all(
        row["validation_status"] == "invalid_or_unverified"
        for row in rows(anchors_path)
    )
    edited = [dict(row) for row in decision_rows]
    edited[0].update(
        {
            "verification_status": "verified_manual",
            "verified_anchor_time": "2025-08-01T12:00:00Z",
            "verified_anchor_source": "manual_override",
            "timing_structure": "scheduled_event_start",
            "evidence_reference": "candidate:test",
        }
    )
    verified = apply_verification(
        rows(inputs[0]), validate_decisions(edited, DECISION_TEMPLATE_FIELDS)
    )
    verified_identities = {
        (row["family_id"], row["family_id_source"])
        for row in verified
        if row["verification_status"] == "verified_manual"
    }
    assert len(verified_identities) == 1
    verified_path = tmp_path / "applied-verified.csv"
    write_csv(verified_path, verified, fieldnames=applied_fields)
    verified_anchors_path = tmp_path / "anchors-verified.csv"
    run_occurrence_anchors(verified_path, verified_anchors_path)
    anchored = [
        row
        for row in rows(verified_anchors_path)
        if row["validation_status"] == "verified"
    ]
    assert len({(row["family_id"], row["family_id_source"]) for row in anchored}) == 1


def test_input_order_and_retrospective_updates_do_not_change_outputs(tmp_path):
    first_inputs = fixtures(tmp_path / "first", retrospective="one")
    second_inputs = fixtures(tmp_path / "second", reverse=True, retrospective="two")
    first = tmp_path / "first-out"
    second = tmp_path / "second-out"
    run(*first_inputs, first, config_path=CONFIG)
    run(*second_inputs, second, config_path=CONFIG)
    for name in (
        "anchor_evidence.csv",
        "anchor_family_review.csv",
        "anchor_verification_decisions_template.csv",
        "anchor_evidence_report.json",
    ):
        assert (first / name).read_bytes() == (second / name).read_bytes()


@pytest.mark.parametrize(
    "end_value",
    ["", "malformed", "0001-01-01T00:00:00Z", "2026-12-31T23:59:59Z"],
)
def test_milestone_end_date_is_fully_ignored(tmp_path, end_value):
    baseline_inputs = fixtures(tmp_path / "baseline")
    changed_inputs = fixtures(tmp_path / "changed")
    changed = rows(changed_inputs[2])
    changed[0]["milestone_end_date"] = end_value
    write_csv(changed_inputs[2], changed, fieldnames=MILESTONE_FIELDS)
    baseline_out = tmp_path / "baseline-out"
    changed_out = tmp_path / "changed-out"
    run(*baseline_inputs, baseline_out, config_path=CONFIG)
    run(*changed_inputs, changed_out, config_path=CONFIG)
    for name in (
        "anchor_evidence.csv",
        "anchor_family_review.csv",
        "anchor_verification_decisions_template.csv",
        "anchor_evidence_report.json",
    ):
        assert (baseline_out / name).read_bytes() == (changed_out / name).read_bytes()


def test_conflicting_candidate_duplicate_publishes_no_outputs(tmp_path):
    inputs = fixtures(tmp_path)
    market_rows = rows(inputs[0])
    market_rows.append({**market_rows[0], "title": "Conflicting title"})
    write_csv(inputs[0], market_rows, fieldnames=MARKET_FIELDS)
    output = tmp_path / "out"
    with pytest.raises(ValueError, match="conflicting candidate duplicate"):
        run(*inputs, output, config_path=CONFIG)
    assert not output.exists()


def test_equivalent_duplicate_candidate_is_order_independent(tmp_path):
    first_inputs = fixtures(tmp_path / "first")
    second_inputs = fixtures(tmp_path / "second")
    for inputs, reverse in ((first_inputs, False), (second_inputs, True)):
        market_rows = rows(inputs[0])
        duplicate = dict(market_rows[0])
        combined = [*market_rows, duplicate]
        if reverse:
            combined.reverse()
        write_csv(inputs[0], combined, fieldnames=MARKET_FIELDS)
    first_out = tmp_path / "first-out"
    second_out = tmp_path / "second-out"
    run(*first_inputs, first_out, config_path=CONFIG)
    run(*second_inputs, second_out, config_path=CONFIG)
    for name in (
        "anchor_evidence.csv",
        "anchor_family_review.csv",
        "anchor_verification_decisions_template.csv",
        "anchor_evidence_report.json",
    ):
        assert (first_out / name).read_bytes() == (second_out / name).read_bytes()


def test_dry_run_writes_nothing_and_limit_is_incomplete(tmp_path):
    inputs = fixtures(tmp_path)
    dry = tmp_path / "dry"
    report = run(*inputs, dry, config_path=CONFIG, limit_families=1, dry_run=True)
    assert not dry.exists()
    assert report["families_before_limit"] == 2
    assert report["families_after_limit"] == 1
    assert report["limited_run"] is True
    assert report["universe_complete"] is False


def test_nontruncating_limit_is_recorded_but_complete(tmp_path):
    inputs = fixtures(tmp_path)
    report = run(*inputs, tmp_path / "out", config_path=CONFIG, limit_families=10)
    assert report["limited_run"] is True
    assert report["requested_limit"] == 10
    assert report["universe_complete"] is True


def test_empty_header_only_inputs_succeed(tmp_path):
    markets = tmp_path / "markets.csv"
    events = tmp_path / "events.csv"
    milestones = tmp_path / "milestones.csv"
    write_csv(markets, [], fieldnames=MARKET_FIELDS)
    write_csv(events, [], fieldnames=EVENT_FIELDS)
    write_csv(milestones, [], fieldnames=MILESTONE_FIELDS)
    output = tmp_path / "out"
    report = run(markets, events, milestones, output, config_path=CONFIG)
    assert report["family_count"] == 0
    assert rows(output / "anchor_evidence.csv") == []
    assert rows(output / "anchor_family_review.csv") == []
    assert rows(output / "anchor_verification_decisions_template.csv") == []


def test_malformed_headers_and_unexpected_events_fail(tmp_path):
    inputs = fixtures(tmp_path)
    bad = tmp_path / "bad.csv"
    write_csv(bad, [], fieldnames=("wrong",))
    with pytest.raises(ValueError, match="missing required columns"):
        run(bad, inputs[1], inputs[2], tmp_path / "bad-out", config_path=CONFIG)
    event_rows = rows(inputs[1])
    event_rows.append({**event_rows[0], "event_ticker": "UNEXPECTED"})
    write_csv(inputs[1], event_rows, fieldnames=EVENT_FIELDS)
    with pytest.raises(ValueError, match="unexpected event metadata"):
        run(*inputs, tmp_path / "unexpected", config_path=CONFIG)


def test_outcome_columns_are_rejected(tmp_path):
    inputs = fixtures(tmp_path)
    market_rows = rows(inputs[0])
    for row in market_rows:
        row["result"] = "yes"
    write_csv(inputs[0], market_rows, fieldnames=(*MARKET_FIELDS, "result"))
    with pytest.raises(ValueError, match="quarantined outcome"):
        run(*inputs, tmp_path / "out", config_path=CONFIG)


def test_no_automatic_verification_language_or_values(tmp_path):
    inputs = fixtures(tmp_path)
    output = tmp_path / "out"
    run(*inputs, output, config_path=CONFIG)
    combined = b"".join(
        (output / name).read_bytes()
        for name in (
            "anchor_evidence.csv",
            "anchor_family_review.csv",
            "anchor_verification_decisions_template.csv",
        )
    )
    assert b"verified_automatic" not in combined
    assert b"verified_manual" not in combined
    assert {
        row["verification_status"]
        for row in rows(output / "anchor_verification_decisions_template.csv")
    } == {"needs_review"}


def test_streaming_compaction_preserves_family_counts_and_deduplicates_candidates(
    tmp_path, monkeypatch
):
    inputs = fixtures(tmp_path)
    market_rows = rows(inputs[0])
    market_rows[1].update(
        {
            "family_id_source": "source_a",
            "event_ticker": "E1",
            "occurrence_datetime": market_rows[0]["occurrence_datetime"],
        }
    )
    write_csv(inputs[0], market_rows, fieldnames=MARKET_FIELDS)
    write_csv(inputs[1], rows(inputs[1])[:1], fieldnames=EVENT_FIELDS)
    monkeypatch.setattr(evidence_builder, "STREAMING_MARKET_THRESHOLD_BYTES", 0)
    output = tmp_path / "out"
    report = run(*inputs, output, config_path=CONFIG)
    occurrence = [
        row
        for row in rows(output / "anchor_evidence.csv")
        if row["candidate_source_type"] == "market_occurrence_datetime"
    ]
    assert report["streaming_market_compaction"] is True
    assert report["market_count"] == 2
    assert report["family_count"] == 1
    assert len(occurrence) == 1
    assert occurrence[0]["supporting_source_count"] == "2"
    assert rows(output / "anchor_family_review.csv")[0]["market_count"] == "2"


def test_existing_output_rerun_is_no_write_and_hash_validated(tmp_path):
    inputs = fixtures(tmp_path)
    output = tmp_path / "out"
    first = run(*inputs, output, config_path=CONFIG)
    before = {
        path.name: (path.stat().st_size, path.stat().st_mtime_ns)
        for path in output.iterdir()
    }
    second = run(*inputs, output, config_path=CONFIG)
    after = {
        path.name: (path.stat().st_size, path.stat().st_mtime_ns)
        for path in output.iterdir()
    }
    assert first == second
    assert before == after


def test_expected_input_hash_and_namespace_guard_fail_before_publication(tmp_path):
    inputs = fixtures(tmp_path)
    with pytest.raises(ValueError, match="SHA-256 mismatch"):
        run(
            *inputs,
            tmp_path / "hash-out",
            config_path=CONFIG,
            expected_market_sha256="0" * 64,
        )
    assert not (tmp_path / "hash-out").exists()
    with pytest.raises(ValueError, match="namespace ceiling"):
        run(
            *inputs,
            tmp_path / "budget-out",
            config_path=CONFIG,
            guard_root=tmp_path,
            max_generated_bytes=1,
        )
    assert not (tmp_path / "budget-out").exists()


def test_report_contains_required_descriptive_diagnostics(tmp_path):
    inputs = fixtures(tmp_path)
    report = run(*inputs, tmp_path / "out", config_path=CONFIG)
    assert report["family_with_candidate_count"] == 1
    assert report["family_with_no_candidate_count"] == 1
    assert report["analysis_window_coverage"] == {
        "inside_candidate_count": 2,
        "outside_candidate_count": 0,
        "overlapping_candidate_count": 1,
        "detailed_status_counts": {
            "date_only_overlaps_analysis_window": 1,
            "inside_analysis_window": 2,
        },
    }
    assert report["family_review_status_counts"] == {"needs_review": 2}
    assert report["candidate_review_status_counts"] == {"needs_review": 3}
    assert report["decision_verification_status_counts"] == {"needs_review": 2}
    assert report["anchors_verified"] == 0
    assert report["outcomes_merged"] is False
    assert report["network_requests"] == 0
