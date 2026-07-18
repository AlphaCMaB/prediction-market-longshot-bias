from __future__ import annotations

import csv
import json

from scripts.pipeline_v2.build_phase_10e_first_review import (
    FIRST_REVIEW_FIELDS,
    REQUIRED_OUTPUTS,
    run,
)
from scripts.pipeline_v2.build_phase_10e_verification_design import PACKET_FIELDS
from scripts.pipeline_v2.phase_10e_first_review import review_case


def candidate(source, timestamp, *, title="Event", context=None):
    return {
        "candidate_id": "candidate-1",
        "candidate_source_type": source,
        "candidate_original_value": timestamp,
        "candidate_time_utc": timestamp,
        "candidate_date": "",
        "candidate_precision": "exact_timestamp",
        "potential_verified_anchor_source": "validated_strike_date",
        "candidate_title": title,
        "evidence_reference": "event:E1",
        "supporting_source_count": 1,
        "analysis_window_status": "inside_analysis_window",
        "safe_evidence_context": context or {},
    }


def packet_row(audit_id="P10E-0001", tier="tier_1", **overrides):
    timestamp = overrides.pop("proposed_candidate_time", "2025-08-01T12:00:00Z")
    source = overrides.pop("proposed_candidate_source_type", "event_strike_date")
    candidates = overrides.pop(
        "candidates_json",
        json.dumps([candidate(source, timestamp)], separators=(",", ":")),
    )
    base = {
        "audit_id": audit_id,
        "proposed_tier": tier,
        "proposed_rule": "PR1" if tier == "tier_1" else "PR2",
        "tier_reason": "test",
        "proposed_timing_structure": (
            "fixed_clock" if tier == "tier_1" else "scheduled_event_start"
        ),
        "semantic_agreement": "exact_informative_token_set",
        "audit_stratum": "test",
        "stratum_family_count": "150",
        "stratum_sample_count": "150",
        "sampling_weight": "1.0",
        "family_id": f"FAMILY-{audit_id}",
        "family_id_source": "kalshi_event_ticker",
        "category": "Crypto" if tier == "tier_1" else "Sports",
        "event_ticker": f"EVENT-{audit_id}",
        "series_ticker": "KXBTC" if tier == "tier_1" else "KXGAME",
        "family_title": (
            "Bitcoin price at noon" if tier == "tier_1" else "Atlanta vs Boston winner"
        ),
        "event_title": (
            "Bitcoin price at noon" if tier == "tier_1" else "Atlanta vs Boston"
        ),
        "event_sub_title": "On Aug 1, 2025 at noon",
        "market_count": "1",
        "candidate_count": "1" if tier != "tier_3" else "0",
        "unique_exact_time_count": "1" if tier != "tier_3" else "0",
        "source_combination": "event_strike" if tier != "tier_3" else "none",
        "evidence_pattern": (
            "single_exact_candidate" if tier != "tier_3" else "no_candidate"
        ),
        "proposed_candidate_id": "candidate-1" if tier != "tier_3" else "",
        "proposed_candidate_time": timestamp if tier != "tier_3" else "",
        "proposed_candidate_source_type": source if tier != "tier_3" else "",
        "proposed_verified_anchor_source": (
            "validated_strike_date" if tier != "tier_3" else ""
        ),
        "analysis_window_status": (
            "inside_analysis_window" if tier != "tier_3" else "not_applicable"
        ),
        "candidates_json": candidates if tier != "tier_3" else "[]",
        "reviewer_instruction": "outcome blind",
    }
    base.update(overrides)
    return base


def write_packet(path, rows):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=PACKET_FIELDS, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def read_rows(path):
    with path.open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_tier_one_review_flags_short_deadline_and_publication_cases():
    approved = review_case(packet_row())
    assert approved.reviewer_decision == "recommend_rule_case"
    assert approved.recommended_verification_status == "needs_review"

    short = review_case(
        packet_row(
            family_title="BTC price up in next 15 mins",
            event_title="BTC Up or Down - 15 minutes",
        )
    )
    assert short.reviewer_decision == "uncertain_human_review"
    assert "recurring_intraday_one_hour_preexistence_risk" in short.ambiguity_flags

    deadline = review_case(packet_row(family_title="Will BTC be above X by Dec 31?"))
    assert deadline.reviewer_decision == "recommend_reject"

    milestone = candidate(
        "event_milestone_start_date",
        "2025-08-01T12:00:00Z",
        title="Annual report publication",
        context={"milestone_title": "Annual report publication"},
    )
    publication = review_case(
        packet_row(
            proposed_candidate_source_type="event_milestone_start_date",
            candidates_json=json.dumps([milestone]),
            event_title="Annual report publication",
        )
    )
    assert publication.reviewer_decision == "recommend_reject"
    assert "publication_or_result_timing" in publication.ambiguity_flags


def test_tier_two_review_approves_clear_start_and_flags_subevents_and_date_mismatch():
    timestamp = "2025-08-01T12:00:00Z"
    good_candidate = candidate(
        "event_milestone_start_date",
        timestamp,
        title="Atlanta vs Boston",
        context={"milestone_title": "Atlanta vs Boston"},
    )
    good = packet_row(
        tier="tier_2",
        family_id="KXGAME-25AUG01",
        proposed_candidate_source_type="event_milestone_start_date",
        candidates_json=json.dumps([good_candidate]),
    )
    assert review_case(good).reviewer_decision == "recommend_rule_case"

    subevent = dict(
        good, family_id="KXATPSET-25AUG01", family_title="Atlanta vs Boston set 2"
    )
    assert review_case(subevent).reviewer_decision == "uncertain_human_review"

    mismatch_candidate = candidate(
        "event_milestone_start_date",
        "2025-10-01T12:00:00Z",
        title="Atlanta vs Boston",
    )
    mismatch = dict(
        good,
        candidates_json=json.dumps([mismatch_candidate]),
        proposed_candidate_time="2025-10-01T12:00:00Z",
    )
    assert review_case(mismatch).reviewer_decision == "recommend_reject"


def test_tier_three_remains_quarantined_without_routine_human_review():
    review = review_case(
        packet_row(tier="tier_3", tier_reason="no_candidate_anchor_evidence")
    )
    assert review.reviewer_decision == "quarantine_tier_3"
    assert review.recommended_verification_status == "needs_review"
    assert review.human_review_required is False


def test_builds_450_recommendations_and_compact_human_subset(tmp_path):
    rows = []
    for tier, start in (("tier_1", 1), ("tier_2", 151), ("tier_3", 301)):
        for number in range(start, start + 150):
            row = packet_row(f"P10E-{number:04d}", tier=tier)
            if tier == "tier_2":
                row.update(
                    {
                        "family_id": f"KXGAME-25AUG{(number % 28) + 1:02d}-{number}",
                        "proposed_candidate_source_type": "event_milestone_start_date",
                    }
                )
                row["candidates_json"] = json.dumps(
                    [
                        candidate(
                            "event_milestone_start_date",
                            row["proposed_candidate_time"],
                            title="Atlanta vs Boston",
                        )
                    ]
                )
            rows.append(row)
    packet = tmp_path / "packet.csv"
    write_packet(packet, rows)
    output = tmp_path / "guard" / "review"
    report = run(
        packet,
        output,
        guard_root=tmp_path / "guard",
        max_generated_bytes=10_000_000,
        min_free_bytes=1,
    )
    assert report["reviewed_family_count"] == 450
    assert report["anchors_verified"] == 0
    assert report["verification_status_counts"] == {"needs_review": 450}
    assert set(path.name for path in output.iterdir()) == set(REQUIRED_OUTPUTS)
    reviews = read_rows(output / "phase_10e_first_review.csv")
    assert tuple(reviews[0]) == FIRST_REVIEW_FIELDS
    assert all(
        row["recommended_verification_status"] == "needs_review" for row in reviews
    )
    assert report["human_review_statistics"]["deterministic_tier_1_count"] == 50
    assert report["human_review_statistics"]["deterministic_tier_2_count"] == 50
    assert report["human_review_statistics"]["tier_3_diagnostic_count"] == 10

    before = {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in output.iterdir()
    }
    assert (
        run(
            packet,
            output,
            guard_root=tmp_path / "guard",
            max_generated_bytes=10_000_000,
            min_free_bytes=1,
        )
        == report
    )
    after = {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in output.iterdir()
    }
    assert before == after
