"""Offline tests for runnable Methodology V2 metadata and manifest stages."""

from __future__ import annotations

import importlib.util
import socket
import sys
from pathlib import Path

import pytest

from scripts.common.io_utils import read_csv, write_csv
from scripts.pipeline_v2 import build_occurrence_anchors, classify_timing
from scripts.pipeline_v2 import validate_anchors, build_horizon_manifest
from scripts.pipeline_v2 import build_price_target_manifest
from scripts.pipeline_v2.config import load_config


ROOT = Path(__file__).resolve().parents[1]


def write_config(path: Path, *, tolerance=15):
    path.write_text(
        "candidate_horizons_hours = [1, 6, 12, 24, 48]\n"
        "fixed_clock_selected_horizons_hours = [1]\n"
        "scheduled_event_start_selected_horizons_hours = [1, 6, 12]\n"
        "main_staleness_minutes = 15\n"
        "robustness_staleness_minutes = 60\n"
        f"early_settlement_tolerance_minutes = {tolerance}\n",
        encoding="utf-8",
    )


def raw_row(market_id="M1", family="F1", **overrides):
    row = {
        "venue": "kalshi",
        "market_id": market_id,
        "family_id": family,
        "family_id_source": "event_ticker",
        "event_ticker": "KXBTC-TEST",
        "title": "Bitcoin price at noon",
        "occurrence_datetime": "2026-07-01T12:00:00Z",
        "occurrence_datetime_verified": "1",
        "verified_scheduled_timestamp": "2026-07-02T12:00:00Z",
        "verified_scheduled_timestamp_validated": "1",
        "strike_date": "2026-07-03T12:00:00Z",
        "strike_date_semantically_verified": "1",
        "manual_override_time": "2026-07-04T12:00:00Z",
        "manual_override_verified": "1",
        "close_time": "2026-07-05T12:00:00Z",
        "market_open_time": "2026-06-29T00:00:00Z",
        "settlement_time": "2026-07-01T12:10:00Z",
        "review_note": "synthetic fixture",
    }
    row.update(overrides)
    return row


def test_config_loading_and_validation(tmp_path):
    valid = tmp_path / "valid.toml"
    write_config(valid)
    config = load_config(valid)
    assert config.candidate_horizons_hours == (1, 6, 12, 24, 48)
    assert config.selected_horizons == {
        "fixed_clock": (1,), "scheduled_event_start": (1, 6, 12)
    }
    invalid = tmp_path / "invalid.toml"
    invalid.write_text("candidate_horizons_hours = [1]\n", encoding="utf-8")
    with pytest.raises(ValueError, match="Missing required configuration values"):
        load_config(invalid)


def test_missing_required_columns_fail_clearly(tmp_path):
    path = tmp_path / "bad.csv"
    write_csv(path, [{"market_id": "M"}])
    with pytest.raises(ValueError, match="family_id, family_id_source"):
        build_occurrence_anchors.run(path, tmp_path / "out.csv")


def test_anchor_priority_and_close_time_is_diagnostic_only():
    output = build_occurrence_anchors.build_rows([raw_row()])[0]
    assert output["anchor_source"] == "occurrence_datetime"
    assert output["anchor_time"] == "2026-07-01T12:00:00+00:00"
    assert output["close_time"] == "2026-07-05T12:00:00Z"
    only_close = raw_row(
        occurrence_datetime="", occurrence_datetime_verified="0",
        verified_scheduled_timestamp="", verified_scheduled_timestamp_validated="0",
        strike_date="", strike_date_semantically_verified="0",
        manual_override_time="", manual_override_verified="0",
    )
    rejected = build_occurrence_anchors.build_rows([only_close])[0]
    assert rejected["anchor_time"] == ""
    assert rejected["validation_status"] == "invalid_or_unverified"


def test_unmatched_timing_becomes_unclear():
    anchored = build_occurrence_anchors.build_rows([
        raw_row(event_ticker="KXUNKNOWN", title="Ambiguous contract")
    ])
    assert classify_timing.build_rows(anchored)[0]["timing_structure"] == "unclear"


def test_family_anomaly_exclusion_and_exact_tolerance(tmp_path):
    config_path = tmp_path / "config.toml"
    write_config(config_path)
    rows = classify_timing.build_rows(build_occurrence_anchors.build_rows([
        raw_row("BAD1", "BAD", settlement_time="2026-07-01T12:01:00Z"),
        raw_row("BAD2", "BAD", settlement_time="2026-07-01T11:44:59Z"),
        raw_row("EDGE", "EDGE", settlement_time="2026-07-01T11:45:00Z"),
    ]))
    input_path = tmp_path / "classified.csv"
    write_csv(input_path, rows)
    summary = validate_anchors.run(
        input_path, tmp_path / "audit.csv", tmp_path / "clean.csv",
        tmp_path / "excluded.csv", config_path=config_path,
    )
    assert summary["clean_families"] == 1
    assert {row["market_id"] for row in read_csv(tmp_path / "clean.csv")} == {"EDGE"}
    assert {row["family_id"] for row in read_csv(tmp_path / "excluded.csv")} == {"BAD"}


def test_horizon_construction_and_ineligibility_reasons(tmp_path):
    config_path = tmp_path / "config.toml"
    write_config(config_path)
    rows = [
        {**raw_row("OK"), "timing_structure": "fixed_clock", "anchor_time": "2026-07-01T12:00:00Z", "anchor_source": "occurrence_datetime", "validation_status": "verified"},
        {**raw_row("LATE", market_open_time="2026-07-01T11:30:00Z"), "timing_structure": "fixed_clock", "anchor_time": "2026-07-01T12:00:00Z", "anchor_source": "occurrence_datetime", "validation_status": "verified"},
        {**raw_row("SETTLED", settlement_time="2026-07-01T11:00:00Z"), "timing_structure": "fixed_clock", "anchor_time": "2026-07-01T12:00:00Z", "anchor_source": "occurrence_datetime", "validation_status": "verified"},
    ]
    input_path = tmp_path / "clean.csv"
    output_path = tmp_path / "horizons.csv"
    write_csv(input_path, rows)
    build_horizon_manifest.run(input_path, output_path, config_path=config_path)
    output = read_csv(output_path)
    assert {int(row["horizon_hours"]) for row in output} == {1, 6, 12, 24, 48}
    one_hour = {row["market_id"]: row for row in output if row["horizon_hours"] == "1"}
    assert one_hour["OK"]["target_time"] == "2026-07-01T11:00:00+00:00"
    assert one_hour["LATE"]["eligibility_status"] == "market_opened_after_target"
    assert one_hour["SETTLED"]["eligibility_status"] == "settled_before_or_at_target"


def test_selected_horizons_deduplication_and_family_counts(tmp_path):
    config_path = tmp_path / "config.toml"
    write_config(config_path)
    base = {
        "venue": "kalshi", "market_id": "M", "family_id": "F",
        "family_id_source": "event_ticker", "anchor_time": "2026-07-01T12:00:00Z",
        "anchor_source": "occurrence_datetime", "validation_status": "verified",
        "target_time": "2026-07-01T11:00:00Z", "eligible": "1",
    }
    rows = [
        {**base, "timing_structure": timing, "horizon_hours": str(horizon)}
        for timing in ["fixed_clock", "scheduled_event_start", "unclear"]
        for horizon in [1, 6, 12, 24, 48]
    ]
    rows.append(dict(rows[0]))
    input_path = tmp_path / "horizons.csv"
    write_csv(input_path, rows)
    summary = build_price_target_manifest.run(
        input_path, tmp_path / "targets.csv", tmp_path / "universe.csv",
        config_path=config_path,
    )
    targets = read_csv(tmp_path / "targets.csv")
    assert {(row["timing_structure"], int(row["horizon_hours"])) for row in targets} == {
        ("fixed_clock", 1), ("scheduled_event_start", 1),
        ("scheduled_event_start", 6), ("scheduled_event_start", 12),
    }
    assert summary == {"target_rows": 4, "unique_markets": 1, "unique_families": 1}


def test_family_counts_are_separate_from_contract_counts():
    rows = [
        {"timing_structure": "fixed_clock", "horizon_hours": 1, "eligibility_status": "eligible", "family_id": "F"},
        {"timing_structure": "fixed_clock", "horizon_hours": 1, "eligibility_status": "eligible", "family_id": "F"},
    ]
    summary = build_horizon_manifest.summarize(rows)[0]
    assert summary["contract_count"] == 2
    assert summary["family_count"] == 1


def test_all_stage_dry_runs_write_no_files(tmp_path):
    config_path = tmp_path / "config.toml"
    write_config(config_path)
    raw_path = tmp_path / "raw.csv"
    write_csv(raw_path, [raw_row()])
    outputs = [tmp_path / name for name in ["a.csv", "b.csv", "c.csv", "d.csv", "e.csv", "f.csv", "g.csv", "r.json"]]
    build_occurrence_anchors.run(raw_path, outputs[0], dry_run=True)
    anchored = build_occurrence_anchors.build_rows([raw_row()])
    anchored_path = tmp_path / "anchored_input.csv"; write_csv(anchored_path, anchored)
    classify_timing.run(anchored_path, outputs[1], dry_run=True)
    classified = classify_timing.build_rows(anchored)
    classified_path = tmp_path / "classified_input.csv"; write_csv(classified_path, classified)
    validate_anchors.run(classified_path, outputs[2], outputs[3], outputs[4], config_path=config_path, dry_run=True)
    build_horizon_manifest.run(classified_path, outputs[5], config_path=config_path, dry_run=True)
    horizon_rows = __import__("scripts.pipeline_v2.horizon_eligibility", fromlist=["build_horizon_eligibility"]).build_horizon_eligibility(classified)
    horizon_path = tmp_path / "horizon_input.csv"; write_csv(horizon_path, horizon_rows)
    build_price_target_manifest.run(horizon_path, outputs[6], tmp_path / "u.csv", report_output=outputs[7], config_path=config_path, dry_run=True)
    assert not any(path.exists() for path in outputs)
    assert not (tmp_path / "u.csv").exists()


def test_small_end_to_end_offline_fixture(tmp_path):
    config_path = tmp_path / "config.toml"; write_config(config_path)
    raw_path = tmp_path / "raw.csv"
    write_csv(raw_path, [
        raw_row("FIXED", "F-FIXED"),
        raw_row("MATCH", "F-MATCH", event_ticker="KXMATCH", title="Team A match Team B"),
    ])
    anchor_path = tmp_path / "anchors.csv"
    timing_path = tmp_path / "timing.csv"
    audit_path, clean_path, excluded_path = tmp_path / "audit.csv", tmp_path / "clean.csv", tmp_path / "excluded.csv"
    horizon_path, target_path, universe_path = tmp_path / "horizons.csv", tmp_path / "targets.csv", tmp_path / "universe.csv"
    build_occurrence_anchors.run(raw_path, anchor_path)
    classify_timing.run(anchor_path, timing_path)
    validate_anchors.run(timing_path, audit_path, clean_path, excluded_path, config_path=config_path)
    build_horizon_manifest.run(clean_path, horizon_path, config_path=config_path)
    summary = build_price_target_manifest.run(horizon_path, target_path, universe_path, config_path=config_path)
    assert summary == {"target_rows": 4, "unique_markets": 2, "unique_families": 2}
    assert len(read_csv(target_path)) == 4
    assert len(read_csv(universe_path)) == 2


NEW_STAGES = [
    ROOT / "scripts/pipeline_v2/config.py",
    ROOT / "scripts/pipeline_v2/build_occurrence_anchors.py",
    ROOT / "scripts/pipeline_v2/classify_timing.py",
    ROOT / "scripts/pipeline_v2/validate_anchors.py",
    ROOT / "scripts/pipeline_v2/build_horizon_manifest.py",
    ROOT / "scripts/pipeline_v2/build_price_target_manifest.py",
]


@pytest.mark.parametrize("module_path", NEW_STAGES, ids=lambda path: path.stem)
def test_stage_import_has_no_side_effects(module_path, monkeypatch, tmp_path):
    before = set(tmp_path.rglob("*"))
    monkeypatch.setattr(socket, "socket", lambda *a, **k: (_ for _ in ()).throw(AssertionError("network during import")))
    monkeypatch.chdir(tmp_path)
    name = f"phase6_import_{module_path.stem}"
    spec = importlib.util.spec_from_file_location(name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(name, None)
    assert set(tmp_path.rglob("*")) == before
