"""Pure candidate-horizon construction and eligibility checks."""

from __future__ import annotations

from datetime import timedelta
from typing import Iterable, Mapping, Any

from scripts.common.time_utils import format_iso_utc, parse_iso_utc
from scripts.pipeline_v2.study_rules import StudyRules, analysis_anchor_window_status


CANDIDATE_HORIZONS = (1, 6, 12, 24, 48)
ALLOWED_TIMING_STRUCTURES = frozenset({"fixed_clock", "scheduled_event_start"})
AUDIT_ONLY_FIELDS = frozenset({
    "diagnostic_settlement_ts", "diagnostic_early_settlement_flag",
    "diagnostic_early_settlement_minutes", "diagnostic_early_settlement_reason",
    "settlement_time", "settlement_ts", "close_time", "expiration_time",
})
PRESERVED_FIELDS = (
    "timing_structure", "family_id", "family_id_source", "anchor_time", "anchor_source",
)


def build_horizon_eligibility(
    rows: Iterable[Mapping[str, Any]],
    *,
    horizons: Iterable[int] = CANDIDATE_HORIZONS,
    study_rules: StudyRules | None = None,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    normalized_horizons = tuple(int(value) for value in horizons)

    for source in rows:
        row = dict(source)
        anchor = parse_iso_utc(row.get("anchor_time"))
        opened = parse_iso_utc(row.get("market_open_time"))
        timing_structure = str(row.get("timing_structure") or "")
        anchor_verified = str(row.get("validation_status") or "").strip().casefold() == "verified"
        family_valid = str(row.get("anchor_validation_status") or "valid").strip().casefold() == "valid"
        window_status = analysis_anchor_window_status(row.get("anchor_time"), study_rules)

        for horizon in normalized_horizons:
            target = anchor - timedelta(hours=horizon) if anchor else None
            if timing_structure not in ALLOWED_TIMING_STRUCTURES:
                status = "timing_structure_not_allowed"
            elif anchor is None:
                status = "missing_or_invalid_anchor"
            elif window_status != "within_analysis_window":
                status = window_status
            elif not anchor_verified:
                status = "anchor_not_verified"
            elif not family_valid:
                status = "anchor_family_not_valid"
            elif opened is None:
                status = "missing_or_invalid_open_time"
            elif opened > target:
                status = "market_opened_after_target"
            else:
                status = "eligible"

            output = {key: value for key, value in row.items() if key not in AUDIT_ONLY_FIELDS}
            output.update(
                {
                    "horizon_hours": horizon,
                    "target_time": format_iso_utc(target),
                    "eligibility_status": status,
                    "eligible": status == "eligible",
                }
            )
            result.append(output)

    return result
