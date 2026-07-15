"""Apply reviewed family anchor decisions to quarantined Kalshi metadata.

Invoke as ``python -m scripts.pipeline_v2.apply_anchor_verification``.
"""

from __future__ import annotations

import argparse
import json
from pathlib import Path
import sys
from typing import Any, Iterable, Mapping

from scripts.common.io_utils import read_csv_with_header, write_csv
from scripts.common.time_utils import format_iso_utc, parse_iso_utc
from scripts.pipeline_v2.study_rules import load_study_rules, validate_research_feature_columns


DECISION_FIELDS = (
    "family_id", "family_id_source", "verification_status", "verified_anchor_time",
    "verified_anchor_source", "timing_structure", "evidence_reference", "review_note",
)
VERIFICATION_STATUSES = frozenset(
    {"verified_automatic", "verified_manual", "needs_review", "rejected"}
)
VERIFIED_STATUSES = frozenset({"verified_automatic", "verified_manual"})
VERIFIED_ANCHOR_SOURCES = frozenset(
    {
        "verified_occurrence_datetime", "verified_official_scheduled_timestamp",
        "validated_strike_date", "manual_override",
    }
)
ALLOWED_TIMING_STRUCTURES = frozenset({"fixed_clock", "scheduled_event_start"})


def family_identity(row: Mapping[str, Any]) -> tuple[str, str] | None:
    identity = _family_identity(row)
    return identity if all(identity) else None


def count_family_identities(
    rows: Iterable[Mapping[str, Any]], *, status: str | None = None,
) -> int:
    return len({
        identity for row in rows
        if (status is None or str(row.get("verification_status") or "") == status)
        and (identity := family_identity(row)) is not None
    })


def _family_identity(row: Mapping[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("family_id") or "").strip(),
        str(row.get("family_id_source") or "").strip(),
    )


def validate_decisions(
    rows: list[dict[str, Any]], columns: Iterable[str] = DECISION_FIELDS,
) -> dict[tuple[str, str], dict[str, Any]]:
    columns = set(columns)
    validate_research_feature_columns(columns)
    if columns != set(DECISION_FIELDS):
        missing = sorted(set(DECISION_FIELDS) - columns)
        extra = sorted(columns - set(DECISION_FIELDS))
        raise ValueError(f"decision schema mismatch; missing={missing}; extra={extra}")
    decisions: dict[tuple[str, str], dict[str, Any]] = {}
    for source in rows:
        row = dict(source)
        family_id = str(row.get("family_id") or "").strip()
        if not family_id:
            raise ValueError("decision family_id is required")
        family_id_source = str(row.get("family_id_source") or "").strip()
        if not family_id_source:
            raise ValueError(f"decision family_id_source is required for family {family_id!r}")
        identity = (family_id, family_id_source)
        if identity in decisions:
            raise ValueError(f"duplicate or contradictory decision for family identity {identity!r}")
        status = str(row.get("verification_status") or "").strip()
        anchor_source = str(row.get("verified_anchor_source") or "").strip()
        timing = str(row.get("timing_structure") or "").strip()
        if status not in VERIFICATION_STATUSES:
            raise ValueError(f"unsupported verification_status for family {family_id!r}")
        if anchor_source and anchor_source not in VERIFIED_ANCHOR_SOURCES:
            raise ValueError(f"disallowed verified_anchor_source for family {family_id!r}")
        if timing and timing not in ALLOWED_TIMING_STRUCTURES:
            raise ValueError(f"disallowed timing_structure for family {family_id!r}")
        if status in VERIFIED_STATUSES:
            if parse_iso_utc(row.get("verified_anchor_time")) is None:
                raise ValueError(f"verified family {family_id!r} requires verified_anchor_time")
            if anchor_source not in VERIFIED_ANCHOR_SOURCES:
                raise ValueError(f"verified family {family_id!r} requires an allowed anchor source")
            if timing not in ALLOWED_TIMING_STRUCTURES:
                raise ValueError(f"verified family {family_id!r} requires an allowed timing structure")
        decisions[identity] = row
    return decisions


def apply_verification(
    markets: Iterable[Mapping[str, Any]],
    decisions: Mapping[tuple[str, str], Mapping[str, Any]],
) -> list[dict[str, Any]]:
    output = []
    for source in markets:
        row = dict(source)
        family_id, family_id_source = _family_identity(row)
        if not family_id or not family_id_source:
            raise ValueError("market row requires family_id and family_id_source")
        decision = decisions.get((family_id, family_id_source))
        if decision is None:
            row.update(
                {
                    "family_id_source": family_id_source,
                    "verification_status": "needs_review",
                    "verified_anchor_time": "",
                    "verified_anchor_source": "",
                    "timing_structure_reviewed": "",
                    "evidence_reference": "",
                    "review_note": "No family verification decision supplied.",
                }
            )
        else:
            status = str(decision["verification_status"])
            verified = status in VERIFIED_STATUSES
            row.update(
                {
                    "family_id_source": family_id_source,
                    "verification_status": status,
                    "verified_anchor_time": format_iso_utc(
                        parse_iso_utc(decision["verified_anchor_time"])
                    ) if verified else "",
                    "verified_anchor_source": str(decision["verified_anchor_source"]) if verified else "",
                    "timing_structure_reviewed": str(decision["timing_structure"]) if verified else "",
                    "evidence_reference": str(decision["evidence_reference"]),
                    "review_note": str(decision["review_note"]),
                }
            )
        output.append(row)
    return sorted(output, key=lambda row: (
        str(row["family_id_source"]), str(row["family_id"]),
        str(row.get("ticker") or row.get("market_id") or ""),
    ))


def run(
    markets_path: Path, decisions_path: Path, output_path: Path, *,
    config_path: Path, limit: int | None = None, dry_run: bool = False,
) -> dict[str, Any]:
    load_study_rules(config_path)
    markets, market_columns = read_csv_with_header(markets_path)
    decisions_rows, decision_columns = read_csv_with_header(decisions_path)
    validate_research_feature_columns(market_columns)
    decisions = validate_decisions(decisions_rows, decision_columns)
    if limit is not None:
        if limit < 0:
            raise ValueError("--limit must be nonnegative")
        markets = markets[:limit]
    output = apply_verification(markets, decisions)
    summary = {
        "rows": len(output),
        "families": count_family_identities(output),
        "family_count": count_family_identities(output),
        "verified_family_count": len({
            identity for row in output
            if row["verification_status"] in VERIFIED_STATUSES
            and (identity := family_identity(row)) is not None
        }),
        "needs_review_family_count": count_family_identities(output, status="needs_review"),
        "rejected_family_count": count_family_identities(output, status="rejected"),
        "status_counts": {
            status: sum(row["verification_status"] == status for row in output)
            for status in sorted(VERIFICATION_STATUSES)
        },
    }
    print(json.dumps(summary, sort_keys=True))
    if not dry_run:
        output_fields = tuple(dict.fromkeys((*market_columns, *(
            "family_id", "family_id_source", "verification_status", "verified_anchor_time",
            "verified_anchor_source", "timing_structure_reviewed", "evidence_reference",
            "review_note",
        ))))
        write_csv(output_path, output, fieldnames=output_fields)
    return summary


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--markets", required=True, type=Path)
    parser.add_argument("--decisions", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        run(
            args.markets, args.decisions, args.output, config_path=args.config,
            limit=args.limit, dry_run=args.dry_run,
        )
    except (FileNotFoundError, ValueError) as exc:
        print(f"Invalid input: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
