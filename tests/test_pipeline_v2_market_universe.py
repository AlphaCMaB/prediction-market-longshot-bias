from __future__ import annotations

import csv
import hashlib
import importlib
import json
from pathlib import Path

import pytest

from scripts.pipeline_v2.kalshi_metadata_cache import canonical_json, sha256_json
from scripts.pipeline_v2.kalshi_metadata_consolidation import payload_sha256
from scripts.pipeline_v2.prepare_kalshi_market_universe import (
    METADATA_FIELDS,
    deduplicate_markets,
    prepare_universe,
    run,
)
from scripts.pipeline_v2.study_rules import load_study_rules
from scripts.pipeline_v2.build_occurrence_anchors import normalize_market_metadata_rows
from scripts.pipeline_v2 import (
    anchor_validation, apply_anchor_verification, build_horizon_manifest,
    build_occurrence_anchors, build_price_target_manifest, classify_timing,
    validate_anchors,
)
from scripts.common.io_utils import read_csv, write_csv
from scripts.pipeline_v2.horizon_eligibility import build_horizon_eligibility
from scripts.pipeline_v2.price_targets import build_price_targets


CONFIG = Path(__file__).resolve().parents[1] / "configs/pipeline_v2.toml"


def _hash(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _jsonl(rows):
    return b"".join(canonical_json(row) + b"\n" for row in rows)


def _market(ticker="T1", result="yes", value="1.00", **updates):
    row = {
        "ticker": ticker,
        "event_ticker": "EV1",
        "title": "Will it happen?",
        "open_time": "2025-06-01T00:00:00Z",
        "close_time": "2025-07-10T00:00:00Z",
        "expiration_time": "2025-07-11T00:00:00Z",
        "occurrence_datetime": "2025-07-09T12:00:00Z",
        "updated_time": "2025-07-11T01:00:00Z",
        "settlement_ts": "2025-07-11T00:30:00Z",
        "result": result,
        "settlement_value_dollars": value,
    }
    row.update(updates)
    return row


def _acquisition(root: Path, records: list[dict], *, page_records=None) -> dict:
    raw = root / "raw"
    month = "2025-07"
    page = raw / month / "live_pages" / "page.json"
    monthly = raw / month / "settled_markets_test.jsonl"
    provenance = raw / month / "settled_markets_provenance_test.jsonl"
    page.parent.mkdir(parents=True)
    response = {"markets": page_records if page_records is not None else records, "cursor": ""}
    response_hash = sha256_json(response)
    wrapper = {"response": response, "response_sha256": response_hash}
    page_bytes = canonical_json(wrapper) + b"\n"
    page.write_bytes(page_bytes)
    monthly_bytes = _jsonl(records)
    monthly.write_bytes(monthly_bytes)
    monthly_hash = _hash(monthly_bytes)
    source = {
        "endpoint_tier": "live", "endpoint_path": "/trade-api/v2/markets",
        "immutable_page_path": str(page), "page_file_sha256": _hash(page_bytes),
        "page_response_sha256": response_hash, "request_id": "request-1", "page_number": 1,
        "request_cursor_hash": "first", "response_cursor_hash": "terminal",
        "cutoff_snapshot_identity": "cutoff-1", "month": month,
        "range_start_utc": "2025-07-01T00:00:00Z",
        "range_end_utc_exclusive": "2025-08-01T00:00:00Z",
        "acquisition_status": "fetched",
    }
    prov_rows = []
    for record in records:
        digest = payload_sha256(record)
        prov_rows.append({
            "month": month, "output_record_id": f"{record['ticker']}|{digest}",
            "ticker": record["ticker"], "selected_payload_sha256": digest,
            "monthly_output_artifact": {
                "path": str(monthly), "sha256": monthly_hash, "source_set_hash": "test",
            },
            "source_associations": [{"payload_sha256": digest, "selected_payload": True, **source}],
        })
    provenance_bytes = _jsonl(prov_rows)
    provenance.write_bytes(provenance_bytes)
    artifacts = [
        {"kind": "monthly_consolidation", "month": month, "path": str(monthly),
         "sha256": monthly_hash, "source_set_hash": "test"},
        {"kind": "record_provenance", "month": month, "path": str(provenance),
         "sha256": _hash(provenance_bytes), "source_set_hash": "test"},
    ]
    page_entry = {
        **source, "row_count": len(response["markets"]), "terminal_page": True,
        "cache_status": "fetched",
    }
    plan = [{key: source[key] for key in (
        "endpoint_tier", "endpoint_path", "month", "range_start_utc", "range_end_utc_exclusive"
    )}]
    commit = {
        "schema_version": 1, "run_id": "run-test", "selected_months": [month],
        "cutoff_snapshot_id": "cutoff-1",
        "date_range": {"start_utc": source["range_start_utc"], "end_utc_exclusive": source["range_end_utc_exclusive"]},
        "effective_configuration": {"endpoint_routing_plan": plan},
        "source_pages": [page_entry], "artifacts": artifacts,
    }
    commit_dir = raw / "run_commits"
    commit_dir.mkdir()
    (commit_dir / "run_run-test.json").write_bytes(canonical_json(commit) + b"\n")
    return commit


def _csv_header(path: Path):
    with path.open(newline="", encoding="utf-8") as handle:
        return next(csv.reader(handle))


def test_prepare_outputs_exact_quarantine_and_report(tmp_path):
    _acquisition(tmp_path, [_market(), _market("T2", "scalar", "0.37")])
    report = run(tmp_path / "raw", tmp_path / "out", config_path=CONFIG)
    assert sorted(path.name for path in (tmp_path / "out").iterdir()) == [
        "event_tickers.csv", "market_metadata.csv", "market_outcomes.csv",
        "market_source_provenance.jsonl", "universe_report.json",
    ]
    metadata_header = _csv_header(tmp_path / "out" / "market_metadata.csv")
    assert metadata_header == list(METADATA_FIELDS)
    assert not {"result", "settlement_value", "settlement_value_dollars"} & set(metadata_header)
    assert "diagnostic_settlement_ts" in metadata_header
    outcomes = list(csv.DictReader((tmp_path / "out" / "market_outcomes.csv").open()))
    assert [row["binary_outcome_status"] for row in outcomes] == ["valid_binary_yes", "invalid_binary_result"]
    assert report["outcome_quarantine_enabled"] is True
    assert report["invalid_binary_result_count"] == 1
    events_header = _csv_header(tmp_path / "out" / "event_tickers.csv")
    assert events_header == ["event_ticker", "contract_count", "first_open_time"]


def test_result_and_value_invariance_end_to_end(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    _acquisition(first, [_market(result="yes", value="1.00")])
    _acquisition(second, [_market(result="no", value="0.00")])
    run(first / "raw", first / "out", config_path=CONFIG)
    run(second / "raw", second / "out", config_path=CONFIG)
    for name in ("market_metadata.csv", "event_tickers.csv", "market_source_provenance.jsonl"):
        assert (first / "out" / name).read_bytes() == (second / "out" / name).read_bytes()
    assert (first / "out" / "market_outcomes.csv").read_bytes() != (second / "out" / "market_outcomes.csv").read_bytes()
    first_report = json.loads((first / "out" / "universe_report.json").read_text())
    second_report = json.loads((second / "out" / "universe_report.json").read_text())
    assert first_report["study_rules_fingerprint"] == second_report["study_rules_fingerprint"]
    assert first_report["binary_yes_count"] == 1 and second_report["binary_no_count"] == 1


@pytest.mark.parametrize("metadata_change", [
    {"title": "Different research title"},
    {"occurrence_datetime": "2025-07-10T12:00:00Z"},
    {"open_time": "2025-05-01T00:00:00Z"},
])
def test_research_provenance_hash_ignores_outcomes_but_changes_with_metadata(metadata_change):
    rules = load_study_rules(CONFIG)
    source = {"source_associations": [{
        "request_id": "r", "page_file_sha256": "raw-a",
        "page_response_sha256": "raw-b",
    }]}
    base = prepare_universe([(_market(), source)], rules)[0]
    outcome_changed = prepare_universe([(_market(
        result="no", value="0.00", settlement_ts="2026-01-01T00:00:00Z"
    ), source)], rules)[0]
    metadata_changed = prepare_universe([(_market(**metadata_change), source)], rules)[0]
    assert base["market_source_provenance.jsonl"] == outcome_changed["market_source_provenance.jsonl"]
    assert base["market_source_provenance.jsonl"] != metadata_changed["market_source_provenance.jsonl"]
    assert b"page_file_sha256" not in base["market_source_provenance.jsonl"]
    assert b"page_response_sha256" not in base["market_source_provenance.jsonl"]


def test_event_tickers_are_invariant_to_all_settlement_times():
    rules = load_study_rules(CONFIG)
    source = {"source_associations": [{"request_id": "r"}]}
    first = prepare_universe([
        (_market("T1", settlement_ts="2025-07-02T00:00:00Z"), source),
        (_market("T2", settlement_ts="2025-07-03T00:00:00Z"), source),
    ], rules)[0]
    changed = prepare_universe([
        (_market("T1", settlement_ts="2026-05-02T00:00:00Z"), source),
        (_market("T2", settlement_ts="2026-06-03T00:00:00Z"), source),
    ], rules)[0]
    assert first["event_tickers.csv"] == changed["event_tickers.csv"]


def test_close_expiration_and_diagnostic_changes_do_not_select_anchor():
    rules = load_study_rules(CONFIG)
    pairs = [(_market(close_time="2030-01-01Z", expiration_time="2031-01-01Z"), {
        "source_associations": [{"request_id": "x"}]
    })]
    contents, _ = prepare_universe(pairs, rules)
    text = contents["market_metadata.csv"].decode()
    assert "anchor_time" not in text and "anchor_source" not in text
    assert rules.study_window.allowed_timing_structures == ("fixed_clock", "scheduled_event_start")


def test_metadata_identity_is_compatible_with_anchor_stage_without_outcomes():
    row = {field: "" for field in METADATA_FIELDS}
    row.update({"ticker": "T1", "event_ticker": "EV1", "diagnostic_settlement_ts": "2025-07-01Z"})
    normalized = normalize_market_metadata_rows([row])[0]
    assert normalized["market_id"] == "T1"
    assert normalized["family_id"] == "EV1"
    assert normalized["family_id_source"] == "kalshi_event_ticker"
    assert "result" not in normalized


def test_invalid_and_missing_results_are_preserved():
    rules = load_study_rules(CONFIG)
    pairs = [
        (_market("A", "void"), {"source_associations": [{"request_id": "a"}]}),
        (_market("B", None), {"source_associations": [{"request_id": "b"}]}),
    ]
    contents, summary = prepare_universe(pairs, rules)
    rows = list(csv.DictReader(contents["market_outcomes.csv"].decode().splitlines()))
    assert rows[0]["result"] == "void"
    assert rows[0]["binary_outcome_status"] == "invalid_binary_result"
    assert rows[1]["binary_outcome_status"] == "missing_result"
    assert summary["invalid_binary_result_count"] == summary["missing_result_count"] == 1


def test_dedup_identical_sources_and_unresolved_conflict():
    record = _market()
    selected = deduplicate_markets([
        (record, {"source_associations": [{"request_id": "a"}]}),
        (record, {"source_associations": [{"request_id": "b"}]}),
    ])
    assert [item["request_id"] for item in selected[0][1]["source_associations"]] == ["a", "b"]
    with pytest.raises(Exception, match="unresolved"):
        deduplicate_markets([
            (_market(result="yes", updated_time="bad"), {"source_associations": [{"request_id": "a"}]}),
            (_market(result="no", updated_time="also-bad"), {"source_associations": [{"request_id": "b"}]}),
        ])


def test_invalid_commit_and_missing_provenance_fail(tmp_path):
    commit = _acquisition(tmp_path, [_market()])
    Path(commit["artifacts"][1]["path"]).unlink()
    with pytest.raises(ValueError, match="no completed valid"):
        run(tmp_path / "raw", tmp_path / "out", config_path=CONFIG)
    assert not (tmp_path / "out").exists()


def test_orphans_ignored_and_dry_run_writes_nothing(tmp_path):
    _acquisition(tmp_path, [_market()])
    orphan = tmp_path / "raw" / "2099-01" / "orphan.jsonl"
    orphan.parent.mkdir()
    orphan.write_text('{"ticker":"ORPHAN"}\n')
    output = tmp_path / "out"
    report = run(tmp_path / "raw", output, config_path=CONFIG, dry_run=True)
    assert report["contract_count"] == 1
    assert not output.exists()


def test_identical_rerun_is_byte_identical(tmp_path):
    _acquisition(tmp_path, [_market()])
    run(tmp_path / "raw", tmp_path / "out", config_path=CONFIG)
    before = {path.name: path.read_bytes() for path in (tmp_path / "out").iterdir()}
    run(tmp_path / "raw", tmp_path / "out", config_path=CONFIG)
    assert before == {path.name: path.read_bytes() for path in (tmp_path / "out").iterdir()}


def test_limit_marks_truncated_universe_incomplete_and_reconciles_counts(tmp_path, capsys):
    _acquisition(tmp_path, [_market("T1"), _market("T2")])
    report = run(tmp_path / "raw", tmp_path / "out", config_path=CONFIG, limit=1)
    assert report["limited_run"] is True
    assert report["requested_limit"] == 1
    assert report["universe_complete"] is False
    assert report["pre_limit_contract_count"] == 2
    assert report["output_contract_count"] == 1
    assert report["omitted_contract_count"] == 1
    assert report["full_input_contract_count"] == 2
    assert "universe is incomplete" in capsys.readouterr().err
    before = {path.name: path.read_bytes() for path in (tmp_path / "out").iterdir()}
    run(tmp_path / "raw", tmp_path / "out", config_path=CONFIG, limit=1)
    assert before == {path.name: path.read_bytes() for path in (tmp_path / "out").iterdir()}


def test_no_limit_is_complete_and_nontruncating_limit_is_recorded(tmp_path):
    first = tmp_path / "first"
    second = tmp_path / "second"
    _acquisition(first, [_market()])
    _acquisition(second, [_market()])
    complete = run(first / "raw", first / "out", config_path=CONFIG)
    bounded = run(second / "raw", second / "out", config_path=CONFIG, limit=5)
    assert complete["limited_run"] is False
    assert complete["requested_limit"] is None
    assert complete["universe_complete"] is True
    assert complete["omitted_contract_count"] == 0
    assert bounded["limited_run"] is True
    assert bounded["requested_limit"] == 5
    assert bounded["universe_complete"] is True
    assert bounded["omitted_contract_count"] == 0


def test_import_has_no_filesystem_or_network_side_effect(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    import scripts.pipeline_v2.prepare_kalshi_market_universe as module
    importlib.reload(module)
    assert list(tmp_path.iterdir()) == []


def _research_pipeline(metadata_bytes: bytes):
    rows = list(csv.DictReader(metadata_bytes.decode().splitlines()))
    decisions = apply_anchor_verification.validate_decisions([{
        "family_id": "EV1", "family_id_source": "kalshi_event_ticker",
        "verification_status": "verified_manual",
        "verified_anchor_time": "2025-07-09T12:00:00Z",
        "verified_anchor_source": "manual_override", "timing_structure": "fixed_clock",
        "evidence_reference": "synthetic official schedule", "review_note": "reviewed",
    }])
    verified = apply_anchor_verification.apply_verification(rows, decisions)
    normalized = normalize_market_metadata_rows(verified)
    anchors = build_occurrence_anchors.build_rows(normalized)
    timed = classify_timing.build_rows(anchors)
    audit, clean = anchor_validation.validate_anchor_families(timed)
    horizons = build_horizon_eligibility(clean)
    targets = build_price_targets(horizons, study_rules=load_study_rules(CONFIG))
    decisions = [{key: row.get(key) for key in (
        "market_id", "anchor_time", "anchor_source", "validation_status"
    )} for row in anchors]
    classifications = [{key: row.get(key) for key in (
        "market_id", "timing_structure", "timing_classification_reason"
    )} for row in timed]
    statuses = [{key: row.get(key) for key in (
        "market_id", "anchor_validation_status", "anchor_validation_reasons"
    )} for row in audit]
    return tuple(canonical_json(value) for value in (decisions, classifications, statuses, horizons, targets)), audit


def test_exact_metadata_schema_reaches_targets_and_is_lookahead_invariant():
    rules = load_study_rules(CONFIG)
    source = {"source_associations": [{"request_id": "stable"}]}
    first, _ = prepare_universe([(_market(result="yes", value="1.00"), source)], rules)
    changed, _ = prepare_universe([(_market(
        result="no", value="0.00", settlement_ts="2025-06-01T00:00:00Z"
    ), source)], rules)
    assert list(csv.DictReader(first["market_metadata.csv"].decode().splitlines()))[0].keys() == set(METADATA_FIELDS)
    first_outputs, first_audit = _research_pipeline(first["market_metadata.csv"])
    changed_outputs, changed_audit = _research_pipeline(changed["market_metadata.csv"])
    assert first_outputs == changed_outputs
    assert json.loads(first_outputs[-1])
    assert first_audit[0]["diagnostic_early_settlement_flag"] is False
    assert changed_audit[0]["diagnostic_early_settlement_flag"] is False
    assert "diagnostic_settlement_ts" not in changed_audit[0]


@pytest.mark.parametrize("diagnostic", ["", "2025-06-01T00:00:00Z", "2026-02-01T00:00:00Z"])
def test_diagnostic_settlement_variants_leave_horizons_and_targets_unchanged(diagnostic):
    base = _market()
    reference = _research_pipeline(prepare_universe(
        [(base, {"source_associations": [{"request_id": "x"}]})], load_study_rules(CONFIG)
    )[0]["market_metadata.csv"])[0]
    variant = _market(settlement_ts=diagnostic)
    changed = _research_pipeline(prepare_universe(
        [(variant, {"source_associations": [{"request_id": "x"}]})], load_study_rules(CONFIG)
    )[0]["market_metadata.csv"])[0]
    assert reference == changed


def _run_serialized_research_pipeline(root: Path, raw_root: Path):
    universe = root / "universe"
    run(raw_root, universe, config_path=CONFIG)
    decisions_path = root / "decisions.csv"
    write_csv(decisions_path, [{
        "family_id": "EV1", "family_id_source": "kalshi_event_ticker",
        "verification_status": "verified_manual",
        "verified_anchor_time": "2025-07-09T12:00:00Z",
        "verified_anchor_source": "manual_override", "timing_structure": "fixed_clock",
        "evidence_reference": "synthetic official schedule", "review_note": "reviewed",
    }])
    verified = root / "verified.csv"
    anchors = root / "anchors.csv"
    timing = root / "timing.csv"
    audit, clean, excluded = root / "audit.csv", root / "clean.csv", root / "excluded.csv"
    horizons, targets, market_universe = root / "horizons.csv", root / "targets.csv", root / "targets-universe.csv"
    apply_anchor_verification.run(
        universe / "market_metadata.csv", decisions_path, verified, config_path=CONFIG
    )
    build_occurrence_anchors.run(verified, anchors)
    classify_timing.run(anchors, timing)
    validate_anchors.run(timing, audit, clean, excluded, config_path=CONFIG)
    build_horizon_manifest.run(clean, horizons, config_path=CONFIG)
    build_price_target_manifest.run(
        horizons, targets, market_universe, config_path=CONFIG
    )
    return {name: path for name, path in {
        "metadata": universe / "market_metadata.csv", "verified": verified,
        "anchors": anchors, "timing": timing, "clean": clean,
        "horizons": horizons, "targets": targets,
    }.items()}


def test_full_serialized_pipeline_requires_handoff_and_is_outcome_invariant(tmp_path):
    first_root, second_root = tmp_path / "first", tmp_path / "second"
    _acquisition(first_root, [_market(result="yes", value="1.00")])
    _acquisition(second_root, [_market(
        result="no", value="0.00", settlement_ts="2025-07-12T00:30:00Z"
    )])

    # The raw metadata alone has candidate evidence, not verification.
    raw_contents, _ = prepare_universe([
        (_market(), {"source_associations": [{"request_id": "x"}]})
    ], load_study_rules(CONFIG))
    raw_rows = list(csv.DictReader(raw_contents["market_metadata.csv"].decode().splitlines()))
    unverified = build_occurrence_anchors.build_rows(
        build_occurrence_anchors.normalize_market_metadata_rows(raw_rows)
    )
    assert unverified[0]["validation_status"] == "invalid_or_unverified"

    first = _run_serialized_research_pipeline(first_root / "work", first_root / "raw")
    second = _run_serialized_research_pipeline(second_root / "work", second_root / "raw")
    assert read_csv(first["targets"])
    for name in ("anchors", "timing", "horizons", "targets"):
        assert first[name].read_bytes() == second[name].read_bytes()
        assert b"diagnostic_settlement_ts" not in first[name].read_bytes()
