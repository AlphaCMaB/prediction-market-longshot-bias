from __future__ import annotations

import importlib
import json
from pathlib import Path

import pytest

from scripts.pipeline_v2.study_rules import (
    STUDY_RULES_SCHEMA_VERSION,
    candidate_anchor_status,
    field_may_be_anchor,
    field_verified_by_default,
    load_study_rules,
    study_rules_from_mapping,
    validate_research_feature_columns,
)


CONFIG = Path("configs/pipeline_v2.toml")


def test_configured_rules_and_stable_fingerprint():
    first = load_study_rules(CONFIG)
    second = load_study_rules(CONFIG)
    assert first.schema_version == STUDY_RULES_SCHEMA_VERSION
    assert first.study_window.analysis_anchor_start_utc == "2025-07-01T00:00:00Z"
    assert first.study_window.analysis_anchor_end_utc_exclusive == "2026-07-01T00:00:00Z"
    assert first.study_window.allowed_timing_structures == ("fixed_clock", "scheduled_event_start")
    assert first.fingerprint == second.fingerprint
    assert len(first.fingerprint) == 64


@pytest.mark.parametrize(
    "field",
    ["settlement_ts", "diagnostic_settlement_ts", "settlement_time", "close_time",
     "expiration_time", "result", "settlement_value", "settlement-value-dollars"],
)
def test_retrospective_and_outcome_fields_can_never_be_anchors(field):
    assert not field_may_be_anchor(field, load_study_rules(CONFIG))


def test_raw_candidate_fields_are_unverified_by_default():
    rules = load_study_rules(CONFIG)
    assert not field_verified_by_default("occurrence_datetime", rules)
    assert not field_verified_by_default("event_strike_date", rules)
    assert candidate_anchor_status("occurrence_datetime", rules) == "unverified_candidate"
    assert candidate_anchor_status("diagnostic_settlement_ts", rules) == "forbidden"


@pytest.mark.parametrize(
    "column", ["result", "Outcome", "settlement-value", "SETTLEMENT VALUE DOLLARS",
               "resolved_yes", "resolvedNo", "binary_label", "label", "target"],
)
def test_research_input_rejects_normalized_outcome_columns(column):
    with pytest.raises(ValueError, match="quarantined"):
        validate_research_feature_columns(["ticker", column])


def test_research_input_accepts_metadata_and_avoids_false_positives():
    validate_research_feature_columns(
        ["ticker", "diagnostic_settlement_ts", "target_time", "price_target_time",
         "resulting_title", "outcome_description_note", "settlement_source",
         "binary_outcome_description", "result_notes", "target_market_id"]
    )


@pytest.mark.parametrize("column", [
    "binary_outcome", "binaryOutcome", "binary outcome", "binary-result",
    "BinaryResult", "binary_label", "finalOutcome", "final_result",
    "outcomeLabel", "result_label", "resolvedOutcome",
])
def test_normalized_binary_and_final_outcome_columns_are_rejected(column):
    with pytest.raises(ValueError, match="quarantined"):
        validate_research_feature_columns([column])


def test_rule_validation_rejects_invalid_window_and_vocab():
    base = {
        "study_window": {
            "analysis_anchor_start_utc": "2026-01-01Z",
            "analysis_anchor_end_utc_exclusive": "2025-01-01Z",
            "allowed_timing_structures": ["fixed_clock"],
            "allowed_binary_results": ["yes", "no"],
        },
        "anchor_verification": {
            "api_occurrence_datetime_is_verified_by_default": False,
            "event_strike_date_is_verified_by_default": False,
            "close_time_may_be_anchor": False,
            "expiration_time_may_be_anchor": False,
            "settlement_time_may_be_anchor": False,
        },
    }
    with pytest.raises(ValueError, match="half-open"):
        study_rules_from_mapping(base)


def _rule_mapping(*, start="2025-07-01T00:00:00Z", end="2026-07-01T00:00:00Z",
                  timings=None, results=None):
    return {
        "study_window": {
            "analysis_anchor_start_utc": start,
            "analysis_anchor_end_utc_exclusive": end,
            "allowed_timing_structures": timings or ["fixed_clock", "scheduled_event_start"],
            "allowed_binary_results": results or ["yes", "no"],
        },
        "anchor_verification": {
            "api_occurrence_datetime_is_verified_by_default": False,
            "event_strike_date_is_verified_by_default": False,
            "close_time_may_be_anchor": False,
            "expiration_time_may_be_anchor": False,
            "settlement_time_may_be_anchor": False,
        },
    }


def test_reversed_vocabularies_have_identical_rules_json_and_fingerprint():
    normal = study_rules_from_mapping(_rule_mapping())
    reversed_rules = study_rules_from_mapping(_rule_mapping(
        timings=["scheduled_event_start", "fixed_clock"], results=["no", "yes"]
    ))
    assert normal == reversed_rules
    assert normal.canonical_mapping() == reversed_rules.canonical_mapping()
    assert json.dumps(normal.canonical_mapping(), sort_keys=True, separators=(",", ":")).encode() == json.dumps(
        reversed_rules.canonical_mapping(), sort_keys=True, separators=(",", ":")
    ).encode()
    assert normal.fingerprint == reversed_rules.fingerprint


@pytest.mark.parametrize("field,value", [
    ("start", "2025-07-02T00:00:00Z"),
    ("end", "2026-06-30T00:00:00Z"),
])
def test_any_different_window_is_rejected(field, value):
    kwargs = {field: value}
    with pytest.raises(ValueError, match="frozen"):
        study_rules_from_mapping(_rule_mapping(**kwargs))


@pytest.mark.parametrize("timings", [
    ["fixed_clock"],
    ["fixed_clock", "scheduled_event_start", "unclear"],
])
def test_added_or_removed_timing_is_rejected(timings):
    with pytest.raises(ValueError, match="timing structures"):
        study_rules_from_mapping(_rule_mapping(timings=timings))


@pytest.mark.parametrize("results", [["yes"], ["yes", "no", "void"]])
def test_added_or_removed_result_is_rejected(results):
    with pytest.raises(ValueError, match="binary results"):
        study_rules_from_mapping(_rule_mapping(results=results))


def test_import_is_side_effect_free(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    before = list(tmp_path.iterdir())
    import scripts.pipeline_v2.study_rules as module
    importlib.reload(module)
    assert list(tmp_path.iterdir()) == before
