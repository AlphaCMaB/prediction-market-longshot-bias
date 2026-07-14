"""Pure family-level occurrence-anchor validation."""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Mapping, Any

from scripts.common.time_utils import parse_iso_utc


EARLY_SETTLEMENT_TOLERANCE_MINUTES = 15.0


def _family_id(row: Mapping[str, Any]) -> str:
    return str(row.get("family_id") or "")


def validate_anchor_families(
    rows: Iterable[Mapping[str, Any]],
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Annotate rows and return ``(audit_rows, valid_rows)``.

    Any invalid member excludes its entire family.
    """
    materialized = [dict(row) for row in rows]
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in materialized:
        grouped[_family_id(row)].append(row)

    family_results: dict[str, tuple[str, tuple[str, ...]]] = {}
    for family_id, members in grouped.items():
        reasons: set[str] = set()
        if not family_id:
            reasons.add("missing_family_id")

        for row in members:
            anchor = parse_iso_utc(row.get("anchor_time"))
            settlement = parse_iso_utc(row.get("settlement_time"))
            if anchor is None:
                reasons.add("missing_or_invalid_anchor")
                continue
            if settlement is not None:
                offset_minutes = (settlement - anchor).total_seconds() / 60.0
                if offset_minutes < -EARLY_SETTLEMENT_TOLERANCE_MINUTES:
                    reasons.add("settled_more_than_15m_before_occurrence")

        status = "excluded" if reasons else "valid"
        family_results[family_id] = status, tuple(sorted(reasons))

    audit: list[dict[str, Any]] = []
    valid: list[dict[str, Any]] = []
    for row in materialized:
        status, reasons = family_results[_family_id(row)]
        output = dict(row)
        output["anchor_validation_status"] = status
        output["anchor_validation_reasons"] = " || ".join(reasons)
        audit.append(output)
        if status == "valid":
            valid.append(output)

    return audit, valid
