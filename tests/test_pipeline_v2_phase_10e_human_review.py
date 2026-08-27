"""Offline tests for the compact Phase 10E human-review interface."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pytest

from scripts.pipeline_v2.phase_10e_human_review import (
    HUMAN_DECISION_FIELDS,
    atomic_save_human_decisions,
    atomic_write_json,
    build_final_report,
    build_human_decision,
    load_human_decisions,
    load_review_subset,
    validate_human_decision,
    verification_projection,
)
from scripts.pipeline_v2.review_phase_10e_human import (
    render_case,
    run_interactive,
)


def subset_row(number=1, tier="tier_1", **updates):
    timestamp = "2025-08-01T12:00:00Z"
    row = {
        "audit_id": f"P10E-{number:04d}",
        "family_id": f"FAMILY-{number}",
        "family_id_source": "kalshi_event_ticker",
        "proposed_tier": tier,
        "proposed_rule": (
            "PR1_FIXED_CLOCK_SINGLE_EXACT"
            if tier == "tier_1"
            else (
                "PR2_SCHEDULED_START_SINGLE_MILESTONE"
                if tier == "tier_2"
                else "NONE_MANUAL_REVIEW"
            )
        ),
        "category": "Crypto" if tier == "tier_1" else "Sports",
        "family_title": "Will the measurement exceed 100 at noon?",
        "event_title": "Measurement at noon",
        "event_sub_title": "",
        "analysis_window_status": "inside_analysis_window",
        "evidence_pattern": "single_exact_candidate",
        "semantic_agreement": "exact_informative_token_set",
        "candidate_count": "1",
        "unique_exact_time_count": "1",
        "sampling_weight": "100",
        "reviewer_decision": "recommend_rule_case",
        "recommended_verification_status": "needs_review",
        "ambiguity_flags_json": "[]",
        "confidence": "high",
        "human_subset_reason": "deterministic_50_per_rule_tier",
        "candidates_json": json.dumps(
            [
                {
                    "candidate_id": f"C-{number}",
                    "candidate_source_type": "event_strike_date",
                    "candidate_original_value": timestamp,
                    "candidate_time_utc": timestamp,
                    "candidate_date": "",
                    "candidate_precision": "exact_timestamp",
                    "potential_verified_anchor_source": "validated_strike_date",
                    "candidate_title": "Measurement at noon",
                    "evidence_reference": f"candidate:C-{number}",
                    "supporting_source_count": 1,
                    "analysis_window_status": "inside_analysis_window",
                    "safe_evidence_context": {
                        "category": "Crypto",
                        "event_title": "Measurement at noon",
                        "series_ticker": "KXTEST",
                        "sub_title": "",
                    },
                }
            ],
            separators=(",", ":"),
        ),
    }
    row.update(updates)
    return row


def decision(row, value="approve_candidate", **updates):
    result = build_human_decision(
        row,
        human_decision=value,
        timing_structure="fixed_clock",
        candidate_relevance="yes" if value == "approve_candidate" else "uncertain",
        confidence="high",
        ambiguity_flags=(),
        rationale="" if value == "approve_candidate" else "Evidence remains ambiguous.",
    )
    result.update(updates)
    return result


def write_csv(path: Path, rows):
    rows = list(rows)
    fields = tuple(rows[0])
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def test_loads_hash_pinned_outcome_blind_subset_without_mutation(tmp_path):
    rows = [subset_row(number) for number in range(1, 166)]
    path = tmp_path / "subset.csv"
    write_csv(path, rows)
    before = (path.read_bytes(), path.stat().st_mtime_ns)
    expected_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    loaded = load_review_subset(path, expected_subset_sha256=expected_hash)
    assert len(loaded) == 165
    assert before == (path.read_bytes(), path.stat().st_mtime_ns)


def test_nested_non_safelisted_or_outcome_key_fails_closed(tmp_path):
    rows = [subset_row(number) for number in range(1, 166)]
    candidates = json.loads(rows[0]["candidates_json"])
    candidates[0]["safe_evidence_context"]["settlement_value"] = "yes"
    rows[0]["candidates_json"] = json.dumps(candidates)
    path = tmp_path / "subset.csv"
    write_csv(path, rows)
    expected_hash = hashlib.sha256(path.read_bytes()).hexdigest()
    with pytest.raises(ValueError, match="quarantined|non-safelisted"):
        load_review_subset(path, expected_subset_sha256=expected_hash)


def test_human_recommendation_projects_to_exact_unverified_schema():
    packet = subset_row()
    row = validate_human_decision(
        decision(packet), subset_by_id={packet["audit_id"]: packet}
    )
    projected = verification_projection([row])
    assert tuple(projected[0]) == (
        "family_id",
        "family_id_source",
        "verification_status",
        "verified_anchor_time",
        "verified_anchor_source",
        "timing_structure",
        "evidence_reference",
        "review_note",
    )
    assert projected[0]["verification_status"] == "needs_review"
    assert projected[0]["verified_anchor_time"] == ""
    assert projected[0]["verified_anchor_source"] == ""
    assert projected[0]["timing_structure"] == ""


def test_rejection_and_uncertainty_require_rationale():
    packet = subset_row()
    for value in ("reject", "uncertain"):
        row = decision(packet, value, concise_rationale="short")
        with pytest.raises(ValueError, match="short rationale"):
            validate_human_decision(row, subset_by_id={packet["audit_id"]: packet})


def test_atomic_save_resume_and_external_conflict_guard(tmp_path):
    subset = [subset_row()]
    path = tmp_path / "guard" / "decisions.csv"
    first_hash = atomic_save_human_decisions(
        path,
        [decision(subset[0])],
        subset_rows=subset,
        guard_root=tmp_path / "guard",
        expected_existing_sha256=None,
        max_generated_bytes=1_000_000,
        min_free_bytes=1,
    )
    loaded, loaded_hash = load_human_decisions(path, subset_rows=subset)
    assert loaded_hash == first_hash
    assert loaded[0]["human_decision"] == "approve_candidate"
    path.write_text(path.read_text() + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="changed since it was loaded"):
        atomic_save_human_decisions(
            path,
            loaded,
            subset_rows=subset,
            guard_root=tmp_path / "guard",
            expected_existing_sha256=loaded_hash,
            max_generated_bytes=1_000_000,
            min_free_bytes=1,
        )


def test_atomic_save_fails_before_namespace_ceiling(tmp_path):
    subset = [subset_row()]
    guard = tmp_path / "guard"
    guard.mkdir()
    with pytest.raises(ValueError, match="namespace ceiling"):
        atomic_save_human_decisions(
            guard / "decisions.csv",
            [decision(subset[0])],
            subset_rows=subset,
            guard_root=guard,
            expected_existing_sha256=None,
            max_generated_bytes=1,
            min_free_bytes=1,
        )
    assert not (guard / "decisions.csv").exists()


def test_interface_hides_ai_decision_and_autosaves_interruption(tmp_path):
    subset = [subset_row(1), subset_row(2)]
    rendered = render_case(subset[0], position=1, total=2, completed=0)
    assert "recommend_rule_case" not in rendered
    assert "sampling_weight" not in rendered
    responses = iter(["a", "1", "y", "h", "", "", "q"])
    decisions_path = tmp_path / "guard" / "decisions.csv"
    rows, complete = run_interactive(
        subset,
        [],
        decisions_path=decisions_path,
        report_path=tmp_path / "guard" / "report.json",
        guard_root=tmp_path / "guard",
        existing_sha256=None,
        input_fn=lambda _: next(responses),
        output_fn=lambda _: None,
    )
    assert complete is False
    assert len(rows) == 1
    loaded, _ = load_human_decisions(decisions_path, subset_rows=subset)
    assert loaded == rows


def test_complete_review_report_separates_rules_and_verifies_no_application():
    subset = []
    for number in range(1, 166):
        tier = "tier_1" if number <= 96 else "tier_2" if number <= 155 else "tier_3"
        row = subset_row(number, tier=tier)
        if number in {1, 97}:
            row["human_subset_reason"] = "ambiguity_flag|case_requires_human_review"
            row["ambiguity_flags_json"] = '["insufficient_evidence"]'
        subset.append(row)
    decisions = []
    for row in subset:
        value = (
            "reject"
            if row["audit_id"] in {"P10E-0001", "P10E-0097"}
            else "approve_candidate"
        )
        decisions.append(decision(row, value))
    report = build_final_report(subset, decisions)
    assert report["reviewed_case_count"] == 165
    assert report["anchors_verified"] == 0
    assert report["rules_approved"] == 0
    assert report["actual_verification_status_counts"] == {"needs_review": 165}
    assert report["outcomes_accessed"] is False
    assert report["network_requests"] == 0
    assert set(report["human_review_statistics"]) == {
        "PR1_FIXED_CLOCK_SINGLE_EXACT",
        "PR2_SCHEDULED_START_SINGLE_MILESTONE",
    }
    for values in report["human_review_statistics"].values():
        assert "weighted_human_approval_rate" in values
        assert "weighted_confirmed_false_positive_rate" in values
        assert "weighted_human_uncertainty_rate" in values
    assert set(report["rule_status"].values()) == {"not_approved"}


def test_saved_file_uses_exact_human_decision_schema(tmp_path):
    subset = [subset_row()]
    path = tmp_path / "guard" / "decisions.csv"
    atomic_save_human_decisions(
        path,
        [decision(subset[0])],
        subset_rows=subset,
        guard_root=tmp_path / "guard",
        expected_existing_sha256=None,
        max_generated_bytes=1_000_000,
        min_free_bytes=1,
    )
    with path.open(newline="", encoding="utf-8") as handle:
        assert tuple(csv.DictReader(handle).fieldnames or ()) == HUMAN_DECISION_FIELDS


def test_final_report_publication_is_idempotent_and_conflict_safe(tmp_path):
    guard = tmp_path / "guard"
    path = guard / "report.json"
    first = atomic_write_json(
        path,
        {"anchors_verified": 0, "rule_status": "not_approved"},
        guard_root=guard,
        max_generated_bytes=1_000_000,
        min_free_bytes=1,
    )
    before = (path.read_bytes(), path.stat().st_mtime_ns)
    second = atomic_write_json(
        path,
        {"anchors_verified": 0, "rule_status": "not_approved"},
        guard_root=guard,
        max_generated_bytes=1_000_000,
        min_free_bytes=1,
    )
    assert first == second
    assert before == (path.read_bytes(), path.stat().st_mtime_ns)
    with pytest.raises(ValueError, match="conflicts"):
        atomic_write_json(
            path,
            {"anchors_verified": 1},
            guard_root=guard,
            max_generated_bytes=1_000_000,
            min_free_bytes=1,
        )
