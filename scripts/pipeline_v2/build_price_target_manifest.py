"""Build targets; invoke as ``python -m scripts.pipeline_v2.build_price_target_manifest``."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from scripts.common.io_utils import read_csv_with_header, write_csv
from scripts.pipeline_v2.build_occurrence_anchors import validate_columns
from scripts.pipeline_v2.config import load_config
from scripts.pipeline_v2.price_targets import build_price_targets
from scripts.pipeline_v2.study_rules import load_study_rules
from scripts.pipeline_v2.build_horizon_manifest import HORIZON_OUTPUT_FIELDS


DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "configs/pipeline_v2.toml"
REQUIRED_COLUMNS = {
    "market_id", "family_id", "family_id_source", "timing_structure",
    "anchor_time", "anchor_source", "validation_status", "horizon_hours",
    "target_time", "eligible",
}
TARGET_OUTPUT_FIELDS = (*HORIZON_OUTPUT_FIELDS, "target_key")
UNIVERSE_OUTPUT_FIELDS = (
    "venue", "market_id", "family_id", "family_id_source", "timing_structures",
    "horizons_hours", "target_count", "earliest_target_time", "latest_target_time",
)


def build_market_universe(targets: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped = defaultdict(list)
    for row in targets:
        grouped[(str(row.get("venue") or ""), str(row["market_id"]))].append(row)
    result = []
    for (venue, market_id), members in sorted(grouped.items()):
        first = members[0]
        result.append({
            "venue": venue,
            "market_id": market_id,
            "family_id": first["family_id"],
            "family_id_source": first["family_id_source"],
            "timing_structures": " || ".join(sorted({str(row["timing_structure"]) for row in members})),
            "horizons_hours": " || ".join(str(value) for value in sorted({int(row["horizon_hours"]) for row in members})),
            "target_count": len(members),
            "earliest_target_time": min(str(row["target_time"]) for row in members),
            "latest_target_time": max(str(row["target_time"]) for row in members),
        })
    return result


def run(input_path: Path, target_output: Path, universe_output: Path, *, report_output: Path | None = None, config_path: Path = DEFAULT_CONFIG, limit=None, dry_run=False) -> dict[str, Any]:
    config = load_config(config_path)
    study_rules = load_study_rules(config_path)
    rows, columns = read_csv_with_header(input_path)
    validate_columns(rows, REQUIRED_COLUMNS, available_columns=columns)
    if limit is not None:
        rows = rows[:limit]
    targets = build_price_targets(
        rows, study_rules=study_rules, selected_horizons=config.selected_horizons
    )
    universe = build_market_universe(targets)
    summary = {
        "target_rows": len(targets), "unique_markets": len(universe),
        "unique_families": len({(row["family_id"], row["family_id_source"]) for row in targets}),
        "contract_count": len({row["market_id"] for row in targets}),
        "family_count": len({(row["family_id"], row["family_id_source"]) for row in targets}),
    }
    print(json.dumps(summary, sort_keys=True))
    if not dry_run:
        write_csv(target_output, targets, fieldnames=TARGET_OUTPUT_FIELDS)
        write_csv(universe_output, universe, fieldnames=UNIVERSE_OUTPUT_FIELDS)
        if report_output is not None:
            report_output.parent.mkdir(parents=True, exist_ok=True)
            report_output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--target-output", required=True, type=Path)
    parser.add_argument("--universe-output", required=True, type=Path)
    parser.add_argument("--report-output", type=Path)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        run(args.input, args.target_output, args.universe_output, report_output=args.report_output, config_path=args.config, limit=args.limit, dry_run=args.dry_run)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(f"Invalid input: {exc}") from exc


if __name__ == "__main__":
    main()
