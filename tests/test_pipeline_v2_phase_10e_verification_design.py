from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from scripts.pipeline_v2.anchor_evidence import (
    ANCHOR_EVIDENCE_FIELDS,
    ANCHOR_FAMILY_REVIEW_FIELDS,
    DECISION_TEMPLATE_FIELDS,
)
from scripts.pipeline_v2.build_phase_10e_verification_design import (
    PACKET_FIELDS,
    REQUIRED_OUTPUTS,
    run,
)
from scripts.pipeline_v2.phase_10e_verification_design import (
    assign_tier,
    evidence_pattern,
    safe_candidate_projection,
    stratified_sample,
    title_agreement,
)


CONFIG = Path(__file__).parents[1] / "configs" / "pipeline_v2.toml"


def write_rows(path, fields, rows):
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        writer.writerows(rows)


def read_rows(path):
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def family(
    family_id,
    *,
    category,
    title,
    candidate_count,
    occurrence=0,
    strike=0,
    milestone=0,
    times=(),
):
    return {
        "family_id": family_id,
        "family_id_source": "kalshi_event_ticker",
        "market_count": "1",
        "event_tickers_json": json.dumps([family_id]),
        "representative_title": title,
        "category": category,
        "first_market_open_time": "2025-01-01T00:00:00Z",
        "candidate_count": str(candidate_count),
        "exact_timestamp_candidate_count": str(candidate_count),
        "date_only_candidate_count": "0",
        "occurrence_candidate_count": str(occurrence),
        "strike_date_candidate_count": str(strike),
        "milestone_start_candidate_count": str(milestone),
        "distinct_exact_candidate_times_json": json.dumps(list(times)),
        "has_conflicting_exact_candidate_times": "true" if len(times) > 1 else "false",
        "has_multiple_event_tickers": "false",
        "missing_event_metadata": "false",
        "invalid_candidate_value_count": "0",
        "sentinel_timestamp_count": "0",
        "review_status": "needs_review",
        "review_reason": "candidate_evidence_requires_review",
        "candidate_ids_json": "[]",
    }


def candidate(family_id, source, timestamp, *, title, context):
    potential = {
        "event_strike_date": "validated_strike_date",
        "event_milestone_start_date": "verified_official_scheduled_timestamp",
        "market_occurrence_datetime": "verified_occurrence_datetime",
    }[source]
    return {
        "family_id": family_id,
        "family_id_source": "kalshi_event_ticker",
        "event_ticker": family_id,
        "candidate_id": f"candidate-{family_id}",
        "candidate_source_type": source,
        "candidate_original_value": timestamp,
        "candidate_time_utc": timestamp,
        "candidate_date": "",
        "candidate_precision": "exact_timestamp",
        "potential_verified_anchor_source": potential,
        "candidate_title": title,
        "evidence_reference": f"event:{family_id}",
        "supporting_source_count": "1",
        "evidence_context_json": json.dumps(context, sort_keys=True),
        "analysis_window_status": "inside_analysis_window",
        "review_status": "needs_review",
    }


def event(family_id, series, title, category):
    return {
        "event_ticker": family_id,
        "series_ticker": series,
        "title": title,
        "sub_title": "Scheduled event",
        "category": category,
    }


def test_evidence_patterns_and_title_agreement_are_conservative():
    no_candidate = family("NONE", category="World", title="No date", candidate_count=0)
    conflict = family(
        "CONFLICT",
        category="Sports",
        title="A vs B",
        candidate_count=2,
        occurrence=1,
        milestone=1,
        times=("2025-08-01T00:00:00Z", "2025-08-01T01:00:00Z"),
    )
    assert evidence_pattern(no_candidate) == "no_candidate"
    assert evidence_pattern(conflict) == "multiple_distinct_exact_times"
    assert title_agreement("Atlanta vs Boston", "Boston at Atlanta")[0] is True
    assert title_agreement("JAY-Z album release", "Ed Sheeran new single")[0] is False


def test_tier_assignments_are_proposals_only():
    timestamp = "2025-08-01T12:00:00Z"
    tier_one_family = family(
        "BTC-25AUG",
        category="Crypto",
        title="Bitcoin price at noon",
        candidate_count=1,
        strike=1,
        times=(timestamp,),
    )
    tier_one_candidate = candidate(
        "BTC-25AUG",
        "event_strike_date",
        timestamp,
        title="Bitcoin price at noon",
        context={"event_title": "Bitcoin price at noon", "series_ticker": "KXBTC"},
    )
    assignment = assign_tier(
        tier_one_family,
        event("BTC-25AUG", "KXBTC", "Bitcoin price at noon", "Crypto"),
        single_candidate=tier_one_candidate,
        milestone_candidate=None,
    )
    assert assignment.tier == "tier_1"
    assert assignment.proposed_rule.startswith("PR1_")

    tier_two_family = family(
        "GAME",
        category="Sports",
        title="Atlanta vs Boston winner",
        candidate_count=1,
        milestone=1,
        times=(timestamp,),
    )
    milestone = candidate(
        "GAME",
        "event_milestone_start_date",
        timestamp,
        title="Atlanta vs Boston",
        context={"milestone_title": "Boston at Atlanta", "association_type": "both"},
    )
    assignment = assign_tier(
        tier_two_family,
        event("GAME", "KXNBAGAME", "Atlanta vs Boston", "Sports"),
        single_candidate=milestone,
        milestone_candidate=milestone,
    )
    assert assignment.tier == "tier_2"

    subevent = assign_tier(
        tier_two_family,
        event("GAME", "KXATPSETWINNER", "Atlanta vs Boston: Set 2 Winner", "Sports"),
        single_candidate=milestone,
        milestone_candidate=milestone,
    )
    assert subevent.tier == "tier_3"
    assert "endogenous_subevent" in subevent.reason


def test_packet_projection_removes_nonapproved_context():
    row = candidate(
        "E1",
        "event_strike_date",
        "2025-08-01T12:00:00Z",
        title="Event",
        context={
            "category": "Economics",
            "event_title": "Event",
            "settlement_sources_json": "must not enter packet",
            "last_updated_ts": "must not enter packet",
        },
    )
    projected = safe_candidate_projection(row)
    assert projected["safe_evidence_context"] == {
        "category": "Economics",
        "event_title": "Event",
    }
    assert "verification_status" not in projected


def test_stratified_sample_is_deterministic_and_weighted():
    identities = {(f"F{i}", "source") for i in range(10)}
    strata = {identity: f"s{int(identity[0][1:]) % 2}" for identity in identities}
    first = stratified_sample(identities, tier="tier_1", strata=strata, sample_size=4)
    second = stratified_sample(
        reversed(sorted(identities)), tier="tier_1", strata=strata, sample_size=4
    )
    assert first == second
    assert len(first) == 4
    assert {row[1] for row in first} == {"s0", "s1"}
    assert all(row[4] == pytest.approx(2.5) for row in first)


def test_builds_complete_outcome_blind_packet_and_reruns_without_changes(tmp_path):
    timestamp = "2025-08-01T12:00:00Z"
    families = [
        family(
            "BTC-25AUG",
            category="Crypto",
            title="Bitcoin price at noon",
            candidate_count=1,
            strike=1,
            times=(timestamp,),
        ),
        family(
            "GAME",
            category="Sports",
            title="Atlanta vs Boston winner",
            candidate_count=1,
            milestone=1,
            times=(timestamp,),
        ),
        family("NONE", category="World", title="No timing evidence", candidate_count=0),
    ]
    evidence = [
        candidate(
            "BTC-25AUG",
            "event_strike_date",
            timestamp,
            title="Bitcoin price at noon",
            context={"event_title": "Bitcoin price at noon", "series_ticker": "KXBTC"},
        ),
        candidate(
            "GAME",
            "event_milestone_start_date",
            timestamp,
            title="Atlanta vs Boston",
            context={
                "milestone_title": "Boston at Atlanta",
                "association_type": "both",
            },
        ),
    ]
    events = [
        event("BTC-25AUG", "KXBTC", "Bitcoin price at noon", "Crypto"),
        event("GAME", "KXNBAGAME", "Atlanta vs Boston", "Sports"),
        event("NONE", "KXNONE", "No timing evidence", "World"),
    ]
    family_path = tmp_path / "family.csv"
    evidence_path = tmp_path / "evidence.csv"
    event_path = tmp_path / "events.csv"
    output = tmp_path / "guard" / "phase_10e"
    write_rows(family_path, ANCHOR_FAMILY_REVIEW_FIELDS, families)
    write_rows(evidence_path, ANCHOR_EVIDENCE_FIELDS, evidence)
    write_rows(event_path, tuple(events[0]), events)
    report = run(
        family_path,
        evidence_path,
        event_path,
        output,
        config_path=CONFIG,
        guard_root=tmp_path / "guard",
        audit_per_tier=1,
        max_generated_bytes=10_000_000,
        min_free_bytes=1,
    )
    assert report["tier_counts"] == {"tier_1": 1, "tier_2": 1, "tier_3": 1}
    assert report["anchors_verified"] == 0
    assert report["outcomes_merged"] is False
    assert {path.name for path in output.iterdir()} == set(REQUIRED_OUTPUTS)
    packet = read_rows(output / "phase_10e_audit_review_packet.csv")
    assert tuple(packet[0]) == PACKET_FIELDS
    assert len(packet) == 3
    decisions = read_rows(output / "phase_10e_audit_decisions_template.csv")
    assert tuple(decisions[0]) == DECISION_TEMPLATE_FIELDS
    assert {row["verification_status"] for row in decisions} == {"needs_review"}
    assert all(not row["verified_anchor_time"] for row in decisions)
    before = {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in output.iterdir()
    }
    rerun = run(
        family_path,
        evidence_path,
        event_path,
        output,
        config_path=CONFIG,
        guard_root=tmp_path / "guard",
        audit_per_tier=1,
        max_generated_bytes=10_000_000,
        min_free_bytes=1,
    )
    after = {
        path.name: (path.read_bytes(), path.stat().st_mtime_ns)
        for path in output.iterdir()
    }
    assert rerun == report
    assert before == after
