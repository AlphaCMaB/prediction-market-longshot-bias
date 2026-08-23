import json
from pathlib import Path

from scripts.pipeline_v2.apply_phase_10e_approved_rules import (
    OUTPUT_FILES,
    _preserve_publication_snapshot,
    estimate_storage,
)
from scripts.pipeline_v2.phase_10e_approved_rules import (
    PR1,
    PR2,
    RULE_SPECIFICATION_SHA256,
    classify_pr1,
    classify_pr2,
)


def candidate(**updates):
    row = {
        "candidate_source_type": "event_strike_date",
        "candidate_precision": "exact_timestamp",
        "candidate_time_utc": "2025-08-02T16:00:00Z",
        "potential_verified_anchor_source": "validated_strike_date",
        "candidate_title": "Bitcoin price at noon ET",
        "evidence_context_json": json.dumps(
            {"event_title": "Bitcoin price at noon ET"}
        ),
    }
    row.update(updates)
    return row


def family(title="Bitcoin price at noon ET", family_id="KXBTC-25AUG02"):
    return {"family_id": family_id, "representative_title": title}


def event(title="Bitcoin price at noon ET", sub_title="", series="KXBTC"):
    return {"title": title, "sub_title": sub_title, "series_ticker": series}


def milestone(**updates):
    row = candidate(
        candidate_source_type="event_milestone_start_date",
        potential_verified_anchor_source="verified_official_scheduled_timestamp",
        candidate_title="Yankees vs Red Sox",
        evidence_context_json=json.dumps({"milestone_title": "Yankees vs Red Sox"}),
    )
    row.update(updates)
    return row


def test_pr1_allows_short_duration_anchor_without_testing_horizon():
    result = classify_pr1(
        family("Bitcoin price in the next 15 minutes"),
        event("Bitcoin price", "12:00–12:15 ET"),
        candidate(candidate_title="Bitcoin price at 12:15 ET"),
    )
    assert result.approved is True
    assert result.rule == PR1


def test_pr1_excludes_deadline_or_window():
    result = classify_pr1(
        family("How high will Bitcoin get by August 2?"), event(), candidate()
    )
    assert result.approved is False
    assert "deadline_or_window_not_fixed_clock" in result.reasons


def test_pr1_allows_exact_official_benchmark_settlement_exception():
    result = classify_pr1(
        family("What will the WTI settlement price be?", "KXWTI-25AUG02"),
        event("WTI crude oil official settlement price", series="KXWTI"),
        candidate(candidate_title="WTI daily settlement price"),
    )
    assert result.approved is True


def test_pr1_excludes_publication_and_large_ticker_date_mismatch():
    result = classify_pr1(
        family("CPI publication time", "KXCPI-25AUG02"),
        event("CPI data release"),
        candidate(candidate_time_utc="2025-08-08T12:30:00Z"),
    )
    assert set(result.reasons) >= {
        "publication_time_not_contract_defined_event",
        "ticker_candidate_date_mismatch",
    }


def test_pr2_allows_one_day_utc_rollover_and_matching_official_start():
    result = classify_pr2(
        family("Yankees vs Red Sox winner", "KXMLB-25AUG02"),
        event("Yankees vs Red Sox"),
        milestone(candidate_time_utc="2025-08-03T00:10:00Z"),
    )
    assert result.approved is True
    assert result.rule == PR2


def test_pr2_excludes_endogenous_or_partial_subevent():
    result = classify_pr2(
        family("First touchdown in the first half", "KXNFL-25AUG02"),
        event("Cowboys vs Eagles"),
        milestone(
            candidate_title="Cowboys vs Eagles",
            evidence_context_json=json.dumps({"milestone_title": "Cowboys vs Eagles"}),
        ),
    )
    assert result.approved is False
    assert set(result.reasons) >= {"endogenous_subevent", "partial_event_scope"}


def test_pr2_excludes_first_goalscorer_first_five_innings_and_subminute_time():
    goalscorer = classify_pr2(
        family("Alex Scott: First Goalscorer", "KXEPL-25AUG02"),
        event("Burnley vs Bournemouth"),
        milestone(
            candidate_title="Burnley vs Bournemouth",
            evidence_context_json=json.dumps(
                {"milestone_title": "Burnley vs Bournemouth"}
            ),
        ),
    )
    first_five = classify_pr2(
        family("Philadelphia wins first 5 innings", "KXMLB-25AUG02"),
        event("Texas vs Philadelphia"),
        milestone(
            candidate_title="Texas vs Philadelphia",
            evidence_context_json=json.dumps(
                {"milestone_title": "Texas vs Philadelphia"}
            ),
        ),
    )
    subminute = classify_pr2(
        family("Player A vs Player B winner", "KXATP-25AUG02"),
        event("Player A vs Player B"),
        milestone(
            candidate_time_utc="2025-08-02T16:00:01.25Z",
            candidate_title="Player A vs Player B",
            evidence_context_json=json.dumps(
                {"milestone_title": "Player A vs Player B"}
            ),
        ),
    )
    assert "endogenous_subevent" in goalscorer.reasons
    assert "partial_event_scope" in first_five.reasons
    assert "subminute_timestamp_not_predetermined_schedule" in subminute.reasons


def test_pr2_excludes_set_map_and_semantic_mismatch():
    result = classify_pr2(
        family("Map 2 winner", "KXCS2-25AUG02"),
        event("Falcons vs Liquid"),
        milestone(
            candidate_title="Arsenal vs Chelsea",
            evidence_context_json=json.dumps({"milestone_title": "Arsenal vs Chelsea"}),
        ),
    )
    assert result.approved is False
    assert "set_map_or_series_scope_not_independently_scheduled" in result.reasons
    assert "title_event_milestone_semantic_mismatch" in result.reasons


def test_rule_specification_fingerprint_and_preflight_estimate_are_stable():
    assert len(RULE_SPECIFICATION_SHA256) == 64
    estimate = estimate_storage(427090, 196410)
    assert estimate == {
        "projected_decisions_bytes": 38438100,
        "projected_verified_anchors_bytes": 19641000,
        "projected_exclusions_bytes": 10802550,
        "projected_reports_bytes": 2097152,
        "projected_publication_bytes": 70978802,
        "projected_atomic_peak_incremental_bytes": 70978802,
    }


def test_deterministic_rerun_preserves_publication_time_disk_snapshot(tmp_path: Path):
    root = tmp_path / "published"
    root.mkdir()
    existing = {
        "input_hashes": {"family": "abc"},
        "rule_specification_sha256": RULE_SPECIFICATION_SHA256,
        "study_rules_fingerprint": "rules",
        "storage_before": {"used_bytes": 10, "free_bytes": 90},
        "projected_namespace_bytes": 20,
        "projected_free_bytes": 80,
    }
    (root / OUTPUT_FILES[3]).write_text(json.dumps(existing))
    current = {
        **existing,
        "storage_before": {"used_bytes": 30, "free_bytes": 70},
        "projected_namespace_bytes": 40,
        "projected_free_bytes": 60,
    }
    assert (
        _preserve_publication_snapshot(current, root, rules_fingerprint="rules")
        == existing
    )
