"""Offline characterization of the explicit family anchor-verification handoff."""

from __future__ import annotations

import importlib
from pathlib import Path

import pytest

from scripts.common.io_utils import read_csv, read_csv_with_header, write_csv
from scripts.pipeline_v2 import (
    apply_anchor_verification, build_horizon_manifest, build_occurrence_anchors,
    build_price_target_manifest, classify_timing, validate_anchors,
)


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "configs/pipeline_v2.toml"


def market(**updates):
    row = {
        "ticker": "T1", "event_ticker": "F1", "family_id": "F1",
        "family_id_source": "kalshi_event_ticker", "title": "Bitcoin price at noon",
        "open_time": "2025-06-01T00:00:00Z",
        "occurrence_datetime": "2025-07-10T12:00:00Z",
        "diagnostic_settlement_ts": "2025-07-10T12:10:00Z",
    }
    row.update(updates)
    return row


def decision(**updates):
    row = {
        "family_id": "F1", "family_id_source": "kalshi_event_ticker",
        "verification_status": "verified_manual",
        "verified_anchor_time": "2025-07-10T12:00:00Z",
        "verified_anchor_source": "manual_override",
        "timing_structure": "fixed_clock", "evidence_reference": "official rules",
        "review_note": "reviewed synthetic decision",
    }
    row.update(updates)
    return row


def verified_rows(status="verified_manual", **updates):
    decisions = apply_anchor_verification.validate_decisions([decision(
        verification_status=status, **updates
    )])
    return apply_anchor_verification.apply_verification([market()], decisions)


def test_unverified_api_timestamp_is_not_a_verified_anchor():
    normalized = build_occurrence_anchors.normalize_market_metadata_rows([market()])
    anchored = build_occurrence_anchors.build_rows(normalized)[0]
    assert anchored["validation_status"] == "invalid_or_unverified"
    assert anchored["anchor_time"] == ""


@pytest.mark.parametrize("status", ["verified_manual", "verified_automatic"])
def test_only_explicit_verified_statuses_create_valid_anchor(status):
    anchored = build_occurrence_anchors.build_rows(
        build_occurrence_anchors.normalize_market_metadata_rows(verified_rows(status))
    )[0]
    assert anchored["validation_status"] == "verified"
    assert anchored["anchor_time"] == "2025-07-10T12:00:00+00:00"
    assert anchored["anchor_source"] == "manual_override"


@pytest.mark.parametrize("status", ["needs_review", "rejected"])
def test_nonverified_statuses_remain_ineligible(status):
    rows = verified_rows(
        status, verified_anchor_time="", verified_anchor_source="", timing_structure=""
    )
    anchored = build_occurrence_anchors.build_rows(
        build_occurrence_anchors.normalize_market_metadata_rows(rows)
    )[0]
    assert anchored["validation_status"] == "invalid_or_unverified"
    assert anchored["anchor_time"] == ""


def test_unmatched_family_becomes_needs_review():
    output = apply_anchor_verification.apply_verification([market()], {})[0]
    assert output["verification_status"] == "needs_review"
    assert output["verified_anchor_time"] == ""


def test_same_family_id_with_different_source_does_not_match_or_overwrite():
    decisions = apply_anchor_verification.validate_decisions([decision()])
    output = apply_anchor_verification.apply_verification([
        market(family_id_source="another_source")
    ], decisions)[0]
    assert output["verification_status"] == "needs_review"
    assert output["family_id_source"] == "another_source"


def test_same_family_id_in_two_namespaces_remains_independent():
    decisions = apply_anchor_verification.validate_decisions([
        decision(),
        decision(
            family_id_source="manual_namespace", verification_status="rejected",
            verified_anchor_time="", verified_anchor_source="", timing_structure="",
        ),
    ])
    output = apply_anchor_verification.apply_verification([
        market(ticker="A"),
        market(ticker="B", family_id_source="manual_namespace"),
    ], decisions)
    assert {(row["ticker"], row["verification_status"]) for row in output} == {
        ("A", "verified_manual"), ("B", "rejected")
    }


def test_verified_decision_requires_time_timing_and_allowed_source():
    with pytest.raises(ValueError, match="verified_anchor_time"):
        apply_anchor_verification.validate_decisions([decision(verified_anchor_time="")])
    with pytest.raises(ValueError, match="timing"):
        apply_anchor_verification.validate_decisions([decision(timing_structure="unclear")])
    with pytest.raises(ValueError, match="anchor_source"):
        apply_anchor_verification.validate_decisions([decision(verified_anchor_source="close_time")])


@pytest.mark.parametrize("source", [
    "settlement_ts", "diagnostic_settlement_ts", "close_time", "expiration_time",
    "result", "settlement_value", "settlement_value_dollars",
])
def test_retrospective_and_outcome_fields_are_disallowed_anchor_sources(source):
    with pytest.raises(ValueError, match="anchor_source"):
        apply_anchor_verification.validate_decisions([decision(verified_anchor_source=source)])


def test_duplicate_family_decisions_fail():
    with pytest.raises(ValueError, match="duplicate or contradictory"):
        apply_anchor_verification.validate_decisions([decision(), decision(review_note="other")])


def test_deterministic_order_uses_source_then_family_then_market():
    rows = apply_anchor_verification.apply_verification([
        market(ticker="Z", family_id="A", family_id_source="z-source"),
        market(ticker="B", family_id="B", family_id_source="a-source"),
        market(ticker="A", family_id="B", family_id_source="a-source"),
    ], {})
    assert [(row["family_id_source"], row["family_id"], row["ticker"]) for row in rows] == [
        ("a-source", "B", "A"), ("a-source", "B", "B"), ("z-source", "A", "Z")
    ]


def test_missing_market_family_identity_fails_without_guessing():
    with pytest.raises(ValueError, match="requires family_id"):
        apply_anchor_verification.apply_verification([
            market(family_id_source="")
        ], {})


@pytest.mark.parametrize("which", ["markets", "decisions"])
def test_outcome_columns_are_rejected(tmp_path, which):
    markets = [market()]
    decisions = [decision()]
    if which == "markets":
        markets[0]["result"] = "yes"
    else:
        decisions[0]["result"] = "yes"
    market_path, decision_path = tmp_path / "markets.csv", tmp_path / "decisions.csv"
    write_csv(market_path, markets)
    write_csv(decision_path, decisions)
    with pytest.raises(ValueError, match="quarantined"):
        apply_anchor_verification.run(
            market_path, decision_path, tmp_path / "out.csv", config_path=CONFIG
        )


def test_diagnostic_and_outcome_values_do_not_change_verification():
    decisions = apply_anchor_verification.validate_decisions([decision()])
    first = apply_anchor_verification.apply_verification([market()], decisions)[0]
    changed = apply_anchor_verification.apply_verification([market(
        diagnostic_settlement_ts="1900-01-01Z"
    )], decisions)[0]
    for field in (
        "verification_status", "verified_anchor_time", "verified_anchor_source",
        "timing_structure_reviewed",
    ):
        assert first[field] == changed[field]


def test_dry_run_writes_nothing(tmp_path):
    market_path, decision_path = tmp_path / "markets.csv", tmp_path / "decisions.csv"
    write_csv(market_path, [market()])
    write_csv(decision_path, [decision()])
    output = tmp_path / "output" / "verified.csv"
    apply_anchor_verification.run(
        market_path, decision_path, output, config_path=CONFIG, dry_run=True
    )
    assert not output.parent.exists()


def test_import_is_side_effect_free(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    import scripts.pipeline_v2.apply_anchor_verification as module
    importlib.reload(module)
    assert list(tmp_path.iterdir()) == []


def test_diagnostic_settlement_is_stripped_from_anchor_and_timing_serialization(tmp_path):
    decisions = apply_anchor_verification.validate_decisions([decision()])
    rendered = []
    for number, diagnostic in enumerate(("2025-07-10T12:10:00Z", "1900-01-01Z")):
        verified = apply_anchor_verification.apply_verification([
            market(diagnostic_settlement_ts=diagnostic)
        ], decisions)
        normalized = build_occurrence_anchors.normalize_market_metadata_rows(verified)
        anchors = build_occurrence_anchors.build_rows(normalized)
        timings = classify_timing.build_rows(anchors)
        anchor_path = tmp_path / f"anchors-{number}.csv"
        timing_path = tmp_path / f"timing-{number}.csv"
        write_csv(anchor_path, anchors)
        write_csv(timing_path, timings)
        rendered.append((anchor_path.read_bytes(), timing_path.read_bytes()))
        assert "diagnostic_settlement_ts" not in anchors[0]
        assert "diagnostic_settlement_ts" not in timings[0]
    assert rendered[0] == rendered[1]


def _run_stages(tmp_path, markets, decisions, suffix):
    base = tmp_path / suffix
    market_path, decision_path = base / "markets.csv", base / "decisions.csv"
    verified, anchors, timing = base / "verified.csv", base / "anchors.csv", base / "timing.csv"
    audit, clean, excluded = base / "audit.csv", base / "clean.csv", base / "excluded.csv"
    horizons, targets, universe = base / "horizons.csv", base / "targets.csv", base / "universe.csv"
    write_csv(market_path, markets)
    write_csv(decision_path, decisions, fieldnames=apply_anchor_verification.DECISION_FIELDS)
    apply_anchor_verification.run(market_path, decision_path, verified, config_path=CONFIG)
    build_occurrence_anchors.run(verified, anchors)
    classify_timing.run(anchors, timing)
    validate_anchors.run(timing, audit, clean, excluded, config_path=CONFIG)
    horizon_summary = build_horizon_manifest.run(clean, horizons, config_path=CONFIG)
    target_summary = build_price_target_manifest.run(
        horizons, targets, universe, config_path=CONFIG
    )
    return {
        "clean": clean, "horizons": horizons, "targets": targets, "universe": universe,
        "horizon_summary": horizon_summary, "target_summary": target_summary,
    }


def test_all_unverified_serialized_pipeline_produces_valid_empty_manifests(tmp_path):
    nonmatching = decision(family_id="OTHER")
    first = _run_stages(tmp_path, [market()], [nonmatching], "first")
    second = _run_stages(tmp_path, [market()], [nonmatching], "second")
    assert read_csv(first["clean"]) == []
    assert read_csv(first["horizons"]) == []
    assert read_csv(first["targets"]) == []
    assert read_csv(first["universe"]) == []
    assert first["horizon_summary"]["contract_count"] == 0
    assert first["horizon_summary"]["family_count"] == 0
    assert first["horizon_summary"]["eligible_count"] == 0
    assert first["target_summary"]["contract_count"] == 0
    assert first["target_summary"]["family_count"] == 0
    for name in ("horizons", "targets", "universe"):
        assert read_csv_with_header(first[name])[1]
        assert first[name].read_bytes() == second[name].read_bytes()
    assert read_csv_with_header(first["horizons"])[1] == build_horizon_manifest.HORIZON_OUTPUT_FIELDS
    assert read_csv_with_header(first["targets"])[1] == build_price_target_manifest.TARGET_OUTPUT_FIELDS
    assert read_csv_with_header(first["universe"])[1] == build_price_target_manifest.UNIVERSE_OUTPUT_FIELDS


def test_header_only_malformed_clean_schema_still_fails(tmp_path):
    malformed = tmp_path / "malformed.csv"
    write_csv(malformed, [], fieldnames=["market_id"])
    with pytest.raises(ValueError, match="Missing required columns"):
        build_horizon_manifest.run(malformed, tmp_path / "horizons.csv", config_path=CONFIG)


def test_mixed_serialized_pipeline_selects_only_verified_family(tmp_path):
    markets = [
        market(ticker="VERIFIED", family_id="FV", event_ticker="FV"),
        market(ticker="UNMATCHED", family_id="FU", event_ticker="FU"),
        market(ticker="REJECTED", family_id="FR", event_ticker="FR"),
    ]
    decisions = [
        decision(family_id="FV"),
        decision(
            family_id="FR", verification_status="rejected", verified_anchor_time="",
            verified_anchor_source="", timing_structure="",
        ),
    ]
    result = _run_stages(tmp_path, markets, decisions, "mixed")
    assert {row["market_id"] for row in read_csv(result["targets"])} == {"VERIFIED"}


def test_serialized_validation_keeps_same_id_namespaces_isolated(tmp_path):
    markets = [
        market(ticker="VALID", family_id="F1", family_id_source="source_a"),
        market(ticker="INVALID", family_id="F1", family_id_source="source_b"),
    ]
    decisions = [
        decision(family_id="F1", family_id_source="source_a"),
        decision(
            family_id="F1", family_id_source="source_b", verification_status="rejected",
            verified_anchor_time="", verified_anchor_source="", timing_structure="",
        ),
    ]
    result = _run_stages(tmp_path, markets, decisions, "namespaces")
    targets = read_csv(result["targets"])
    assert {(row["market_id"], row["family_id_source"]) for row in targets} == {
        ("VALID", "source_a")
    }
    assert result["horizon_summary"]["input_families"] == 1


def test_three_stage_summaries_count_composite_families_and_statuses(tmp_path):
    markets = [
        market(ticker="A1", family_id="F1", family_id_source="source_a"),
        market(ticker="A2", family_id="F1", family_id_source="source_a"),
        market(ticker="B", family_id="F1", family_id_source="source_b"),
        market(ticker="C", family_id="F2", family_id_source="source_a"),
    ]
    decisions = [
        decision(family_id="F1", family_id_source="source_a"),
        decision(
            family_id="F1", family_id_source="source_b", verification_status="needs_review",
            verified_anchor_time="", verified_anchor_source="", timing_structure="",
        ),
        decision(family_id="F2", family_id_source="source_a"),
    ]
    market_path, decision_path = tmp_path / "markets.csv", tmp_path / "decisions.csv"
    verified, anchors, timing = tmp_path / "verified.csv", tmp_path / "anchors.csv", tmp_path / "timing.csv"
    write_csv(market_path, markets)
    write_csv(decision_path, decisions)
    verification_summary = apply_anchor_verification.run(
        market_path, decision_path, verified, config_path=CONFIG
    )
    anchor_summary = build_occurrence_anchors.run(verified, anchors)
    timing_summary = classify_timing.run(anchors, timing)
    assert verification_summary["family_count"] == 3
    assert verification_summary["verified_family_count"] == 2
    assert verification_summary["needs_review_family_count"] == 1
    assert verification_summary["rejected_family_count"] == 0
    assert anchor_summary["family_count"] == 3
    assert timing_summary["family_count"] == 3
    assert sum(timing_summary["timing_family_counts"].values()) == 3


def test_three_stage_empty_summaries_report_zero_families(tmp_path):
    market_path, decision_path = tmp_path / "markets.csv", tmp_path / "decisions.csv"
    verified, anchors = tmp_path / "verified.csv", tmp_path / "anchors.csv"
    write_csv(market_path, [], fieldnames=market().keys())
    write_csv(decision_path, [], fieldnames=apply_anchor_verification.DECISION_FIELDS)
    verification_summary = apply_anchor_verification.run(
        market_path, decision_path, verified, config_path=CONFIG
    )
    anchor_summary = build_occurrence_anchors.run(verified, anchors)
    timing_summary = classify_timing.run(anchors, tmp_path / "timing.csv")
    assert verification_summary["family_count"] == 0
    assert verification_summary["verified_family_count"] == 0
    assert anchor_summary["family_count"] == 0
    assert timing_summary["family_count"] == 0
