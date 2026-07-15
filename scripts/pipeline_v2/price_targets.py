"""Pure selection of approved Methodology V2 price targets."""

from __future__ import annotations

from typing import Iterable, Mapping, Any

from scripts.common.time_utils import format_iso_utc, parse_iso_utc
from scripts.pipeline_v2.study_rules import analysis_anchor_window_status
from scripts.pipeline_v2.study_rules import StudyRules


SELECTED_HORIZONS = {
    "fixed_clock": frozenset({1}),
    "scheduled_event_start": frozenset({1, 6, 12}),
}


def deterministic_target_key(row: Mapping[str, Any]) -> str:
    target = format_iso_utc(parse_iso_utc(row.get("target_time")))
    return "|".join(
        (
            str(row.get("venue") or ""),
            str(row.get("market_id") or row.get("ticker") or ""),
            str(row.get("timing_structure") or ""),
            str(int(row.get("horizon_hours"))),
            target,
        )
    )


def build_price_targets(
    rows: Iterable[Mapping[str, Any]],
    *,
    study_rules: StudyRules,
    selected_horizons: Mapping[str, Iterable[int]] | None = None,
) -> list[dict[str, Any]]:
    horizon_policy = (
        {key: frozenset(int(value) for value in values) for key, values in selected_horizons.items()}
        if selected_horizons is not None
        else SELECTED_HORIZONS
    )
    selected: dict[str, dict[str, Any]] = {}
    for source in rows:
        row = dict(source)
        timing_structure = str(row.get("timing_structure") or "")
        try:
            horizon = int(row.get("horizon_hours"))
        except (TypeError, ValueError):
            continue

        eligible = str(row.get("eligible") or "").strip().lower() in {"1", "true", "yes"}
        if not eligible or horizon not in horizon_policy.get(timing_structure, frozenset()):
            continue
        if parse_iso_utc(row.get("target_time")) is None:
            continue
        if analysis_anchor_window_status(row.get("anchor_time"), study_rules) != "within_analysis_window":
            continue

        output = dict(row)
        output["horizon_hours"] = horizon
        output["target_time"] = format_iso_utc(parse_iso_utc(row.get("target_time")))
        output["target_key"] = deterministic_target_key(output)
        selected[output["target_key"]] = output

    return sorted(
        selected.values(),
        key=lambda row: (
            row["timing_structure"],
            row["horizon_hours"],
            str(row.get("family_id_source") or ""),
            str(row.get("family_id") or ""),
            str(row.get("market_id") or row.get("ticker") or ""),
        ),
    )
