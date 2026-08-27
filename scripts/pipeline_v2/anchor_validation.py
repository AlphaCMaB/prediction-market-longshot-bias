"""Pure family-level occurrence-anchor validation."""

from __future__ import annotations

from collections import defaultdict
from typing import Iterable, Mapping, Any

from scripts.common.time_utils import parse_iso_utc


EARLY_SETTLEMENT_TOLERANCE_MINUTES = 15.0


def _family_identity(row: Mapping[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("family_id") or "").strip(),
        str(row.get("family_id_source") or "").strip(),
    )


def validate_anchor_families(
    rows: Iterable[Mapping[str, Any]],
    *,
    early_settlement_tolerance_minutes: float = EARLY_SETTLEMENT_TOLERANCE_MINUTES,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    """Annotate rows and return ``(audit_rows, valid_rows)``.

    Invalid anchors exclude their family. Retrospective settlement diagnostics
    are annotated per row but never affect research eligibility.
    """
    materialized = [dict(row) for row in rows]
    grouped: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    for row in materialized:
        grouped[_family_identity(row)].append(row)

    family_results: dict[tuple[str, str], tuple[str, tuple[str, ...]]] = {}
    for identity, members in grouped.items():
        family_id, family_id_source = identity
        reasons: set[str] = set()
        if not family_id:
            reasons.add("missing_family_id")
        if not family_id_source:
            reasons.add("missing_family_id_source")

        for row in members:
            anchor = parse_iso_utc(row.get("anchor_time"))
            if anchor is None:
                reasons.add("missing_or_invalid_anchor")

        status = "excluded" if reasons else "valid"
        family_results[identity] = status, tuple(sorted(reasons))

    audit: list[dict[str, Any]] = []
    valid: list[dict[str, Any]] = []
    for row in materialized:
        status, reasons = family_results[_family_identity(row)]
        output = dict(row)
        output["anchor_validation_status"] = status
        output["anchor_validation_reasons"] = " || ".join(reasons)
        anchor = parse_iso_utc(output.get("anchor_time"))
        settlement = parse_iso_utc(output.get("diagnostic_settlement_ts"))
        if anchor is not None and settlement is not None:
            early_minutes = (anchor - settlement).total_seconds() / 60.0
            flagged = early_minutes > float(early_settlement_tolerance_minutes)
            output["diagnostic_early_settlement_flag"] = flagged
            output["diagnostic_early_settlement_minutes"] = max(0.0, early_minutes)
            output["diagnostic_early_settlement_reason"] = (
                "diagnostic_settlement_more_than_tolerance_before_anchor" if flagged else "within_tolerance_or_after_anchor"
            )
        else:
            output["diagnostic_early_settlement_flag"] = False
            output["diagnostic_early_settlement_minutes"] = ""
            output["diagnostic_early_settlement_reason"] = "missing_or_invalid_diagnostic_value"
        audit.append(output)
        if status == "valid":
            valid.append(output)

    order = lambda row: (
        str(row.get("family_id_source") or ""), str(row.get("family_id") or ""),
        str(row.get("market_id") or row.get("ticker") or ""),
    )
    return sorted(audit, key=order), sorted(valid, key=order)
