"""Classify timing; invoke as ``python -m scripts.pipeline_v2.classify_timing``."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any, Iterable, Mapping

from scripts.common.io_utils import read_csv_with_header, write_csv
from scripts.pipeline_v2.apply_anchor_verification import count_family_identities, family_identity
from scripts.pipeline_v2.build_occurrence_anchors import ANCHOR_OUTPUT_FIELDS, validate_columns
from scripts.pipeline_v2.timing import classify_timing


REQUIRED_COLUMNS = {"market_id", "family_id", "family_id_source", "anchor_time", "anchor_source", "validation_status"}
TIMING_OUTPUT_FIELDS = (
    *ANCHOR_OUTPUT_FIELDS, "timing_structure", "timing_classification_reason",
    "timing_classification_confidence",
)


def build_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for source in rows:
        row = {field: source.get(field, "") for field in ANCHOR_OUTPUT_FIELDS}
        explicit = str(row.get("timing_structure_reviewed") or "").strip() or None
        structure, reason = classify_timing(
            str(row.get("event_ticker") or row.get("ticker") or ""),
            str(row.get("title") or ""),
            explicit_timing_structure=explicit,
        )
        row["timing_structure"] = structure
        row["timing_classification_reason"] = str(row.get("timing_classification_reason") or reason)
        row["timing_classification_confidence"] = str(row.get("timing_classification_confidence") or ("reviewed" if explicit else "heuristic"))
        result.append(row)
    return sorted(result, key=lambda row: (
        str(row["timing_structure"]), str(row["family_id_source"]),
        str(row["family_id"]), str(row["market_id"]),
    ))


def run(input_path: Path, output_path: Path, *, limit=None, dry_run=False) -> dict[str, Any]:
    rows, columns = read_csv_with_header(input_path)
    validate_columns(rows, REQUIRED_COLUMNS, available_columns=columns)
    if limit is not None:
        rows = rows[:limit]
    output = build_rows(rows)
    summary = {
        "rows": len(output), "families": count_family_identities(output),
        "family_count": count_family_identities(output),
        "timing_counts": dict(Counter(row["timing_structure"] for row in output)),
        "timing_family_counts": {
            timing: len({identity for row in output if row["timing_structure"] == timing and (identity := family_identity(row)) is not None})
            for timing in sorted({row["timing_structure"] for row in output})
        },
    }
    print(json.dumps(summary, sort_keys=True))
    if not dry_run:
        write_csv(output_path, output, fieldnames=TIMING_OUTPUT_FIELDS)
    return summary


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--config", type=Path)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        run(args.input, args.output, limit=args.limit, dry_run=args.dry_run)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(f"Invalid input: {exc}") from exc


if __name__ == "__main__":
    main()
