"""Offline tests for the Phase 10E AI-assisted import and fresh validation."""

from __future__ import annotations

import csv
import hashlib
import json
from pathlib import Path

import pytest

from scripts.pipeline_v2.phase_10e_ai_assisted_review import (
    ANNOTATION_HEADERS,
    AI_ASSISTED_DECISION_FIELDS,
    build_ai_assisted_diagnostics,
    build_independent_validation_design,
    import_ai_assisted_annotations,
)
from scripts.pipeline_v2.phase_10e_independent_validation import (
    INDEPENDENT_DECISION_FIELDS,
    atomic_save_independent_decisions,
    build_independent_decision,
    build_independent_validation_report,
    load_independent_decisions,
    load_validation_sources,
    validate_independent_decision,
)
from scripts.pipeline_v2.review_phase_10e_independent_validation import render_case


REJECT_NUMBERS = {27, 87, 101, 112, 160}
UNCERTAIN_NUMBERS = {1, 2, 3, 4, 5, 6, 54, 58, 59, 67, 138}


def candidate_json(number: int) -> str:
    timestamp = "2025-08-01T12:00:00Z"
    return json.dumps(
        [
            {
                "candidate_id": f"C-{number}",
                "candidate_source_type": "event_strike_date",
                "candidate_original_value": timestamp,
                "candidate_time_utc": timestamp,
                "candidate_date": "",
                "candidate_precision": "exact_timestamp",
                "potential_verified_anchor_source": "validated_strike_date",
                "candidate_title": "Test event",
                "evidence_reference": f"candidate:C-{number}",
                "supporting_source_count": 1,
                "analysis_window_status": "inside_analysis_window",
                "safe_evidence_context": {
                    "category": "Crypto",
                    "event_title": "Test event",
                    "series_ticker": "KXTEST",
                    "sub_title": "",
                },
            }
        ],
        separators=(",", ":"),
    )


def subset_rows() -> list[dict[str, str]]:
    rows = []
    for number in range(1, 166):
        tier = "tier_1" if number <= 96 else "tier_2" if number <= 155 else "tier_3"
        rule = {
            "tier_1": "PR1_FIXED_CLOCK_SINGLE_EXACT",
            "tier_2": "PR2_SCHEDULED_START_SINGLE_MILESTONE",
            "tier_3": "NONE_MANUAL_REVIEW",
        }[tier]
        rows.append(
            {
                "audit_id": f"P10E-{number:04d}",
                "family_id": f"FAMILY-{number}",
                "family_id_source": "kalshi_event_ticker",
                "proposed_tier": tier,
                "proposed_rule": rule,
                "category": "Crypto" if tier == "tier_1" else "Sports",
                "family_title": "Test family",
                "event_title": "Test event",
                "event_sub_title": "",
                "analysis_window_status": "inside_analysis_window",
                "evidence_pattern": "single_exact_candidate",
                "semantic_agreement": "exact_informative_token_set",
                "candidate_count": "1",
                "unique_exact_time_count": "1",
                "sampling_weight": "2",
                "reviewer_decision": "recommend_rule_case",
                "recommended_verification_status": "needs_review",
                "ambiguity_flags_json": "[]",
                "confidence": "high",
                "human_review_required": "false",
                "human_subset_reason": "deterministic_50_per_rule_tier",
                "candidates_json": candidate_json(number),
            }
        )
    return rows


def annotation_rows(subset: list[dict[str, str]]) -> list[dict[str, str]]:
    rows = []
    for number, packet in enumerate(subset, 1):
        decision = (
            "R"
            if number in REJECT_NUMBERS
            else "U" if number in UNCERTAIN_NUMBERS else "A"
        )
        flags = "deadline_or_window_not_fixed_clock" if number == 87 else ""
        rows.append(
            {
                "review_number": str(number),
                "audit_id": packet["audit_id"],
                "human_decision": decision,
                "timing_structure": (
                    "fixed_clock"
                    if decision == "A" and packet["proposed_tier"] == "tier_1"
                    else (
                        "scheduled_event_start"
                        if decision == "A" and packet["proposed_tier"] == "tier_2"
                        else "neither/uncertain"
                    )
                ),
                "candidate_relevant": (
                    "yes"
                    if decision == "A"
                    else "no" if decision == "R" else "uncertain"
                ),
                "confidence": "high" if decision == "A" else "medium",
                "ambiguity_flags": flags,
                "rationale": (
                    "Candidate matches ex-ante evidence."
                    if decision == "A"
                    else "Evidence is not sufficient for approval."
                ),
                "review_label": "AI-assisted outcome-blind review",
            }
        )
    return rows


def write_csv(path: Path, rows, fields) -> str:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)
    return hashlib.sha256(path.read_bytes()).hexdigest()


def imported_fixture(tmp_path):
    subset = subset_rows()
    path = tmp_path / "annotations.csv"
    digest = write_csv(path, annotation_rows(subset), ANNOTATION_HEADERS)
    imported = import_ai_assisted_annotations(
        path, subset, expected_annotation_sha256=digest
    )
    return subset, path, digest, imported


def test_compact_import_validates_counts_corrections_and_unverified_projection(
    tmp_path,
):
    subset, _, digest, imported = imported_fixture(tmp_path)
    assert len(imported) == len({row["audit_id"] for row in imported}) == 165
    assert (
        sum(row["ai_assisted_decision"] == "approve_candidate" for row in imported)
        == 149
    )
    assert sum(row["ai_assisted_decision"] == "reject" for row in imported) == 5
    assert sum(row["ai_assisted_decision"] == "uncertain" for row in imported) == 11
    assert all(row["source_annotation_sha256"] == digest for row in imported)
    assert all(row["verification_status"] == "needs_review" for row in imported)
    assert all(not row["verified_anchor_time"] for row in imported)
    assert all(not row["verified_anchor_source"] for row in imported)
    assert tuple(imported[0]) == AI_ASSISTED_DECISION_FIELDS
    assert len(subset) == 165


@pytest.mark.parametrize(
    "failure", ["hash", "duplicate", "missing", "vocabulary", "label", "count"]
)
def test_compact_import_fails_closed_on_mismatch(tmp_path, failure):
    subset = subset_rows()
    rows = annotation_rows(subset)
    if failure == "duplicate":
        rows[-1]["audit_id"] = rows[0]["audit_id"]
    elif failure == "missing":
        rows.pop()
    elif failure == "vocabulary":
        rows[0]["confidence"] = "certain"
    elif failure == "label":
        rows[0]["review_label"] = "independent human review"
    elif failure == "count":
        rows[7]["human_decision"] = "U"
    path = tmp_path / "annotations.csv"
    digest = write_csv(path, rows, ANNOTATION_HEADERS)
    expected = "0" * 64 if failure == "hash" else digest
    with pytest.raises(ValueError):
        import_ai_assisted_annotations(
            path, subset, expected_annotation_sha256=expected
        )


def design_fixture(tmp_path):
    subset, _, _, imported = imported_fixture(tmp_path)
    first_review = {
        row["audit_id"]: {
            **row,
            "confidence": "high",
            "reviewer_decision": "recommend_rule_case",
            "ambiguity_flags_json": "[]",
            "human_review_required": "false",
        }
        for row in subset
    }
    _, analysis = build_ai_assisted_diagnostics(subset, imported, first_review)
    return imported, build_independent_validation_design(analysis, per_rule=50)


def test_diagnostics_and_independent_design_are_weighted_blinded_and_deterministic(
    tmp_path,
):
    imported, (packets, manifests, report) = design_fixture(tmp_path)
    _, (packets_2, manifests_2, report_2) = design_fixture(tmp_path / "again")
    assert (packets, manifests, report) == (packets_2, manifests_2, report_2)
    assert len(packets) == 100
    assert sum(row["proposed_rule"].startswith("PR1") for row in packets) == 50
    assert sum(row["proposed_rule"].startswith("PR2") for row in packets) == 50
    assert all(not row["independent_human_decision"] for row in packets)
    assert all("ai_assisted" not in key for key in packets[0])
    assert report["tier_3_excluded_from_rule_inference"] is True
    assert report["prior_ai_assisted_decisions_in_packet"] == 0
    assert all(
        float(row["independent_validation_analysis_weight"]) > 0 for row in manifests
    )
    assert all(
        float(row["ai_assisted_subset_inclusion_probability"])
        == pytest.approx(1.0 / 3.0)
        for row in manifests
    )
    assert len(imported) == 165


def validation_sources(tmp_path):
    imported, (packets, manifests, _) = design_fixture(tmp_path)
    packet_path = tmp_path / "packet.csv"
    manifest_path = tmp_path / "manifest.csv"
    from scripts.pipeline_v2.phase_10e_ai_assisted_review import (
        INDEPENDENT_MANIFEST_FIELDS,
        INDEPENDENT_PACKET_FIELDS,
    )

    packet_hash = write_csv(packet_path, packets, INDEPENDENT_PACKET_FIELDS)
    manifest_hash = write_csv(manifest_path, manifests, INDEPENDENT_MANIFEST_FIELDS)
    loaded, manifest_by_id = load_validation_sources(
        packet_path,
        manifest_path,
        expected_packet_sha256=packet_hash,
        expected_manifest_sha256=manifest_hash,
    )
    return imported, loaded, manifest_by_id, packet_hash, manifest_hash


def human_decision(packet, packet_hash, manifest_hash, value="approve_candidate"):
    return build_independent_decision(
        packet,
        packet_sha256=packet_hash,
        manifest_sha256=manifest_hash,
        decision=value,
        timing_structure=(
            "fixed_clock"
            if packet["proposed_tier"] == "tier_1"
            else "scheduled_event_start"
        ),
        candidate_relevance="yes" if value == "approve_candidate" else "uncertain",
        confidence="high",
        ambiguity_flags=(),
        rationale="" if value == "approve_candidate" else "Evidence remains ambiguous.",
    )


def test_fresh_packet_hashes_schema_and_prior_decision_guard(tmp_path):
    _, packets, _, packet_hash, manifest_hash = validation_sources(tmp_path)
    decision = human_decision(packets[0], packet_hash, manifest_hash)
    validated = validate_independent_decision(
        decision,
        packet_by_validation_id={row["validation_id"]: row for row in packets},
        packet_sha256=packet_hash,
        manifest_sha256=manifest_hash,
    )
    assert tuple(validated) == INDEPENDENT_DECISION_FIELDS
    assert validated["verification_status"] == "needs_review"
    tampered = dict(packets[0], independent_human_decision="approve_candidate")
    packet_path = tmp_path / "tampered.csv"
    from scripts.pipeline_v2.phase_10e_ai_assisted_review import (
        INDEPENDENT_PACKET_FIELDS,
    )

    tampered_rows = [tampered, *packets[1:]]
    tampered_hash = write_csv(packet_path, tampered_rows, INDEPENDENT_PACKET_FIELDS)
    with pytest.raises(ValueError, match="prior decision"):
        load_validation_sources(
            packet_path,
            tmp_path / "manifest.csv",
            expected_packet_sha256=tampered_hash,
            expected_manifest_sha256=manifest_hash,
        )


def test_independent_autosave_resume_and_interface_hides_ai(tmp_path):
    _, packets, _, packet_hash, manifest_hash = validation_sources(tmp_path)
    rendered = render_case(packets[0], position=1, total=100, completed=0)
    assert "approve_candidate" not in rendered
    assert "ai_assisted_decision" not in rendered
    assert "sampling_weight" not in rendered
    decisions_path = tmp_path / "guard" / "decisions.csv"
    first = human_decision(packets[0], packet_hash, manifest_hash)
    digest = atomic_save_independent_decisions(
        decisions_path,
        [first],
        packets=packets,
        packet_sha256=packet_hash,
        manifest_sha256=manifest_hash,
        guard_root=tmp_path / "guard",
        expected_existing_sha256=None,
        max_generated_bytes=1_000_000,
        min_free_bytes=1,
    )
    loaded, loaded_hash = load_independent_decisions(
        decisions_path,
        packets=packets,
        packet_sha256=packet_hash,
        manifest_sha256=manifest_hash,
    )
    assert loaded == [first]
    assert loaded_hash == digest
    decisions_path.write_text(decisions_path.read_text() + "\n", encoding="utf-8")
    with pytest.raises(ValueError, match="changed since they were loaded"):
        atomic_save_independent_decisions(
            decisions_path,
            loaded,
            packets=packets,
            packet_sha256=packet_hash,
            manifest_sha256=manifest_hash,
            guard_root=tmp_path / "guard",
            expected_existing_sha256=loaded_hash,
            max_generated_bytes=1_000_000,
            min_free_bytes=1,
        )


def test_complete_independent_report_separates_human_and_ai_assisted(tmp_path):
    imported, packets, manifests, packet_hash, manifest_hash = validation_sources(
        tmp_path
    )
    decisions = [
        human_decision(
            packet,
            packet_hash,
            manifest_hash,
            "reject" if number in {1, 51} else "approve_candidate",
        )
        for number, packet in enumerate(packets, 1)
    ]
    report = build_independent_validation_report(
        packets,
        manifests,
        decisions,
        {row["audit_id"]: row for row in imported},
        packet_sha256=packet_hash,
        manifest_sha256=manifest_hash,
    )
    assert report["review_type"] == "fresh_independent_human_outcome_blind_validation"
    assert report["anchors_verified"] == report["rules_approved"] == 0
    assert report["verification_status_counts"] == {"needs_review": 100}
    assert set(report["rule_specific"]) == {
        "PR1_FIXED_CLOCK_SINGLE_EXACT",
        "PR2_SCHEDULED_START_SINGLE_MILESTONE",
    }
    for values in report["rule_specific"].values():
        assert "weighted_human_approval_rate" in values
        assert "weighted_confirmed_false_positive_rate" in values
        assert "weighted_ai_assisted_human_disagreement_rate" in values
