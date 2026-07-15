"""Build anchors; invoke as ``python -m scripts.pipeline_v2.build_occurrence_anchors``."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from scripts.common.io_utils import read_csv_with_header, write_csv
from scripts.common.time_utils import format_iso_utc, parse_iso_utc
from scripts.pipeline_v2.anchors import AnchorSelection
from scripts.pipeline_v2.apply_anchor_verification import (
    VERIFIED_ANCHOR_SOURCES, VERIFIED_STATUSES, count_family_identities,
)
from scripts.pipeline_v2.study_rules import validate_research_feature_columns


REQUIRED_COLUMNS = {"market_id", "family_id", "family_id_source"}
RESEARCH_INPUT_FIELDS = (
    "venue", "ticker", "market_id", "event_ticker", "family_id", "family_id_source",
    "title", "subtitle", "yes_sub_title", "no_sub_title", "rules_primary",
    "rules_secondary", "market_type", "open_time", "market_open_time",
    "occurrence_datetime", "updated_time", "strike_date", "strike_type",
    "floor_strike", "cap_strike", "custom_strike", "can_close_early",
    "early_close_condition", "verified_scheduled_timestamp",
    "occurrence_datetime_verified", "verified_scheduled_timestamp_validated",
    "strike_date_semantically_verified", "manual_override_time",
    "manual_override_verified", "verified_anchor_time", "verified_anchor_source",
    "verification_status", "timing_structure_reviewed", "evidence_reference",
    "review_note",
)
ANCHOR_OUTPUT_FIELDS = (
    *RESEARCH_INPUT_FIELDS, "anchor_time", "anchor_source", "validation_status",
)


def validate_columns(
    rows: list[dict[str, Any]], required=REQUIRED_COLUMNS,
    *, available_columns: Iterable[str] | None = None,
) -> None:
    available = set(available_columns) if available_columns is not None else (set(rows[0]) if rows else set())
    validate_research_feature_columns(available)
    missing = sorted(set(required) - available)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def normalize_market_metadata_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Map the quarantined Kalshi metadata identity onto existing stage fields."""
    normalized = []
    for source in rows:
        row = dict(source)
        row["market_id"] = row.get("market_id") or row.get("ticker") or ""
        row["market_open_time"] = row.get("market_open_time") or row.get("open_time") or ""
        if not row.get("family_id") and row.get("event_ticker"):
            row["family_id"] = row["event_ticker"]
            row["family_id_source"] = "kalshi_event_ticker"
        normalized.append(row)
    return normalized


def build_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for source in rows:
        row = {field: source.get(field, "") for field in RESEARCH_INPUT_FIELDS}
        verification_status = str(row.get("verification_status") or "").strip()
        verified_time = parse_iso_utc(row.get("verified_anchor_time"))
        verified_source = str(row.get("verified_anchor_source") or "").strip()
        if (
            verification_status in VERIFIED_STATUSES
            and verified_time is not None
            and verified_source in VERIFIED_ANCHOR_SOURCES
        ):
            selection = AnchorSelection(
                format_iso_utc(verified_time), verified_source, "verified",
                str(row.get("review_note") or "Verified family decision."),
            )
        else:
            selection = AnchorSelection(
                "", "", "invalid_or_unverified",
                str(row.get("review_note") or (
                    f"Family status is {verification_status}." if verification_status
                    else "No explicit family verification decision."
                )),
            )
        row.update(selection.to_dict())
        result.append(row)
    return sorted(result, key=lambda row: (
        str(row["family_id_source"]), str(row["family_id"]), str(row["market_id"]),
    ))


def run(input_path: Path, output_path: Path, *, limit: int | None = None, dry_run: bool = False) -> dict[str, int]:
    rows, columns = read_csv_with_header(input_path)
    rows = normalize_market_metadata_rows(rows)
    available = set(columns)
    if "ticker" in available:
        available.add("market_id")
    if "event_ticker" in available:
        available.update({"family_id", "family_id_source"})
    validate_columns(rows, available_columns=available)
    if limit is not None:
        rows = rows[:limit]
    output = build_rows(rows)
    summary = {
        "rows": len(output), "families": count_family_identities(output),
        "family_count": count_family_identities(output),
    }
    print(json.dumps(summary, sort_keys=True))
    if not dry_run:
        write_csv(output_path, output, fieldnames=ANCHOR_OUTPUT_FIELDS)
    return summary


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv=None) -> None:
    args = parse_args(argv)
    try:
        run(args.input, args.output, limit=args.limit, dry_run=args.dry_run)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(f"Invalid input: {exc}") from exc


if __name__ == "__main__":
    main()
