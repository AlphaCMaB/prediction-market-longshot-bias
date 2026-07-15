"""Immutable, look-ahead-safe Methodology V2 study rules."""

from __future__ import annotations

from dataclasses import asdict, dataclass
import hashlib
import json
from pathlib import Path
import re
import tomllib
from typing import Any, Iterable, Mapping

from scripts.common.time_utils import format_iso_utc, parse_iso_utc


STUDY_RULES_SCHEMA_VERSION = "1.0"
FROZEN_ANALYSIS_ANCHOR_START_UTC = "2025-07-01T00:00:00Z"
FROZEN_ANALYSIS_ANCHOR_END_UTC_EXCLUSIVE = "2026-07-01T00:00:00Z"
FROZEN_TIMING_STRUCTURES = ("fixed_clock", "scheduled_event_start")
FROZEN_BINARY_RESULTS = ("yes", "no")
_FORBIDDEN_RESEARCH_COLUMNS = frozenset(
    {
        "binaryoutcome", "binaryresult", "binarylabel", "outcome", "result",
        "label", "target", "settlementvalue", "settlementvaluedollars",
        "resolvedyes", "resolvedno", "resolvedoutcome", "finaloutcome",
        "finalresult", "outcomelabel", "resultlabel",
    }
)
_NEVER_ANCHOR_FIELDS = frozenset(
    {
        "settlementts", "settlementtime", "diagnosticsettlementts", "closetime",
        "expirationtime", "result", "outcome", "settlementvalue",
        "settlementvaluedollars",
    }
)


def canonical_field_name(value: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(value).casefold())


@dataclass(frozen=True)
class StudyWindow:
    analysis_anchor_start_utc: str
    analysis_anchor_end_utc_exclusive: str
    allowed_timing_structures: tuple[str, ...]
    allowed_binary_results: tuple[str, ...]


@dataclass(frozen=True)
class AnchorVerificationRules:
    api_occurrence_datetime_is_verified_by_default: bool
    event_strike_date_is_verified_by_default: bool
    close_time_may_be_anchor: bool
    expiration_time_may_be_anchor: bool
    settlement_time_may_be_anchor: bool


@dataclass(frozen=True)
class StudyRules:
    schema_version: str
    study_window: StudyWindow
    anchor_verification: AnchorVerificationRules

    def canonical_mapping(self) -> dict[str, Any]:
        return asdict(self)

    @property
    def fingerprint(self) -> str:
        payload = json.dumps(
            self.canonical_mapping(), sort_keys=True, separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return hashlib.sha256(payload).hexdigest()


def _required_mapping(value: Any, section: str) -> Mapping[str, Any]:
    if not isinstance(value, Mapping):
        raise ValueError(f"configuration lacks [{section}]")
    return value


def study_rules_from_mapping(config: Mapping[str, Any]) -> StudyRules:
    window = _required_mapping(config.get("study_window"), "study_window")
    verification = _required_mapping(config.get("anchor_verification"), "anchor_verification")
    start = parse_iso_utc(window.get("analysis_anchor_start_utc"))
    end = parse_iso_utc(window.get("analysis_anchor_end_utc_exclusive"))
    if start is None or end is None or start >= end:
        raise ValueError("study window must be a valid nonempty half-open UTC interval")
    normalized_start = format_iso_utc(start).replace("+00:00", "Z")
    normalized_end = format_iso_utc(end).replace("+00:00", "Z")
    if (
        normalized_start != FROZEN_ANALYSIS_ANCHOR_START_UTC
        or normalized_end != FROZEN_ANALYSIS_ANCHOR_END_UTC_EXCLUSIVE
    ):
        raise ValueError("analysis anchor window differs from the frozen Phase 9A window")
    supplied_timings = tuple(str(item).strip() for item in window.get("allowed_timing_structures", ()))
    if len(supplied_timings) != len(FROZEN_TIMING_STRUCTURES) or set(supplied_timings) != set(FROZEN_TIMING_STRUCTURES):
        raise ValueError("allowed timing structures must exactly match the frozen Phase 9A vocabulary")
    timings = FROZEN_TIMING_STRUCTURES
    supplied_binary = tuple(str(item).strip().casefold() for item in window.get("allowed_binary_results", ()))
    if len(supplied_binary) != len(FROZEN_BINARY_RESULTS) or set(supplied_binary) != set(FROZEN_BINARY_RESULTS):
        raise ValueError("allowed binary results must exactly match the frozen Phase 9A vocabulary")
    binary = FROZEN_BINARY_RESULTS
    required_flags = (
        "api_occurrence_datetime_is_verified_by_default",
        "event_strike_date_is_verified_by_default",
        "close_time_may_be_anchor",
        "expiration_time_may_be_anchor",
        "settlement_time_may_be_anchor",
    )
    if any(not isinstance(verification.get(name), bool) for name in required_flags):
        raise ValueError("anchor verification settings must be explicit booleans")
    if any(bool(verification[name]) for name in required_flags):
        raise ValueError("Phase 9A anchor defaults and retrospective fields must remain false")
    return StudyRules(
        schema_version=STUDY_RULES_SCHEMA_VERSION,
        study_window=StudyWindow(
            normalized_start,
            normalized_end,
            timings,
            binary,
        ),
        anchor_verification=AnchorVerificationRules(**{name: verification[name] for name in required_flags}),
    )


def load_study_rules(path: str | Path) -> StudyRules:
    with Path(path).open("rb") as handle:
        return study_rules_from_mapping(tomllib.load(handle))


def field_may_be_anchor(field_name: Any, rules: StudyRules | None = None) -> bool:
    del rules
    return canonical_field_name(field_name) not in _NEVER_ANCHOR_FIELDS


def field_verified_by_default(field_name: Any, rules: StudyRules) -> bool:
    name = canonical_field_name(field_name)
    if name in _NEVER_ANCHOR_FIELDS:
        return False
    if name == "occurrencedatetime":
        return rules.anchor_verification.api_occurrence_datetime_is_verified_by_default
    if name in {"strike_date", "strikedate", "eventstrikedate"}:
        return rules.anchor_verification.event_strike_date_is_verified_by_default
    return False


def candidate_anchor_status(field_name: Any, rules: StudyRules) -> str:
    if not field_may_be_anchor(field_name, rules):
        return "forbidden"
    return "verified_by_default" if field_verified_by_default(field_name, rules) else "unverified_candidate"


def analysis_window_bounds(rules: StudyRules | None = None):
    start_value = rules.study_window.analysis_anchor_start_utc if rules else FROZEN_ANALYSIS_ANCHOR_START_UTC
    end_value = rules.study_window.analysis_anchor_end_utc_exclusive if rules else FROZEN_ANALYSIS_ANCHOR_END_UTC_EXCLUSIVE
    start = parse_iso_utc(start_value)
    end = parse_iso_utc(end_value)
    if start is None or end is None:
        raise ValueError("validated study rules contain an invalid analysis window")
    return start, end


def analysis_anchor_window_status(anchor_time: Any, rules: StudyRules | None = None) -> str:
    anchor = parse_iso_utc(anchor_time)
    if anchor is None:
        return "missing_or_invalid_anchor"
    start, end = analysis_window_bounds(rules)
    if anchor < start:
        return "anchor_before_analysis_window"
    if anchor >= end:
        return "anchor_at_or_after_analysis_window"
    return "within_analysis_window"


def forbidden_research_columns(columns: Iterable[Any]) -> tuple[str, ...]:
    return tuple(sorted(str(column) for column in columns if canonical_field_name(column) in _FORBIDDEN_RESEARCH_COLUMNS))


def validate_research_feature_columns(columns: Iterable[Any]) -> None:
    forbidden = forbidden_research_columns(columns)
    if forbidden:
        raise ValueError("research-feature input contains quarantined outcome columns: " + ", ".join(forbidden))
