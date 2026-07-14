"""Pure candidate-horizon construction and eligibility checks."""

from __future__ import annotations

from datetime import timedelta
from typing import Iterable, Mapping, Any

from scripts.common.time_utils import format_iso_utc, parse_iso_utc


CANDIDATE_HORIZONS = (1, 6, 12, 24, 48)
PRESERVED_FIELDS = (
    "timing_structure", "family_id", "family_id_source", "anchor_time", "anchor_source",
)


def build_horizon_eligibility(
    rows: Iterable[Mapping[str, Any]],
    *,
    horizons: Iterable[int] = CANDIDATE_HORIZONS,
) -> list[dict[str, Any]]:
    result: list[dict[str, Any]] = []
    normalized_horizons = tuple(int(value) for value in horizons)

    for source in rows:
        row = dict(source)
        anchor = parse_iso_utc(row.get("anchor_time"))
        opened = parse_iso_utc(row.get("market_open_time"))
        settlement = parse_iso_utc(row.get("settlement_time"))

        for horizon in normalized_horizons:
            target = anchor - timedelta(hours=horizon) if anchor else None
            if anchor is None:
                status = "missing_or_invalid_anchor"
            elif opened is None:
                status = "missing_or_invalid_open_time"
            elif opened > target:
                status = "market_opened_after_target"
            elif settlement is None:
                status = "missing_or_invalid_settlement_time"
            elif settlement <= target:
                status = "settled_before_or_at_target"
            else:
                status = "eligible"

            output = dict(row)
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
