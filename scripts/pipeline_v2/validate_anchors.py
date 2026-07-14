"""Apply configured family-level occurrence-anchor anomaly validation."""

from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Any

from scripts.common.io_utils import read_csv, write_csv
from scripts.pipeline_v2.anchor_validation import validate_anchor_families
from scripts.pipeline_v2.build_occurrence_anchors import validate_columns
from scripts.pipeline_v2.config import load_config


DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "configs/pipeline_v2.toml"
REQUIRED_COLUMNS = {
    "market_id", "family_id", "family_id_source", "timing_structure",
    "anchor_time", "anchor_source", "validation_status", "settlement_time",
}


def run(input_path: Path, audit_output: Path, clean_output: Path, excluded_output: Path, *, config_path: Path = DEFAULT_CONFIG, limit: int | None = None, dry_run: bool = False) -> dict[str, Any]:
    config = load_config(config_path)
    rows = read_csv(input_path)
    validate_columns(rows, REQUIRED_COLUMNS)
    if limit is not None:
        rows = rows[:limit]
    audit, clean = validate_anchor_families(rows, early_settlement_tolerance_minutes=config.early_settlement_tolerance_minutes)
    excluded = [row for row in audit if row["anchor_validation_status"] != "valid"]
    summary = {
        "audit_rows": len(audit), "clean_rows": len(clean), "excluded_rows": len(excluded),
        "input_families": len({row["family_id"] for row in rows}),
        "clean_families": len({row["family_id"] for row in clean}),
        "status_counts": dict(Counter(row["anchor_validation_status"] for row in audit)),
    }
    print(json.dumps(summary, sort_keys=True))
    if not dry_run:
        fields = list(audit[0]) if audit else sorted(REQUIRED_COLUMNS)
        write_csv(audit_output, audit, fieldnames=fields)
        write_csv(clean_output, clean, fieldnames=fields)
        write_csv(excluded_output, excluded, fieldnames=fields)
    return summary


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--audit-output", required=True, type=Path)
    parser.add_argument("--clean-output", required=True, type=Path)
    parser.add_argument("--excluded-output", required=True, type=Path)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        run(args.input, args.audit_output, args.clean_output, args.excluded_output, config_path=args.config, limit=args.limit, dry_run=args.dry_run)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(f"Invalid input: {exc}") from exc


if __name__ == "__main__":
    main()
