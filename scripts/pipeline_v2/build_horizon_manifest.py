"""Build horizons; invoke as ``python -m scripts.pipeline_v2.build_horizon_manifest``."""

from __future__ import annotations

import argparse
import json
from collections import defaultdict
from pathlib import Path
from typing import Any

from scripts.common.io_utils import read_csv, write_csv
from scripts.pipeline_v2.build_occurrence_anchors import validate_columns
from scripts.pipeline_v2.config import load_config
from scripts.pipeline_v2.horizon_eligibility import build_horizon_eligibility


DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "configs/pipeline_v2.toml"
REQUIRED_COLUMNS = {
    "market_id", "family_id", "family_id_source", "timing_structure",
    "anchor_time", "anchor_source", "validation_status", "market_open_time",
    "settlement_time",
}


def summarize(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    groups = defaultdict(list)
    for row in rows:
        key = (str(row["timing_structure"]), int(row["horizon_hours"]), str(row["eligibility_status"]))
        groups[key].append(row)
    return [
        {
            "timing_structure": key[0], "horizon_hours": key[1],
            "eligibility_status": key[2], "contract_count": len(members),
            "family_count": len({row["family_id"] for row in members}),
        }
        for key, members in sorted(groups.items())
    ]


def run(input_path: Path, output_path: Path, *, config_path: Path = DEFAULT_CONFIG, limit=None, dry_run=False) -> dict[str, Any]:
    config = load_config(config_path)
    rows = read_csv(input_path)
    validate_columns(rows, REQUIRED_COLUMNS)
    if limit is not None:
        rows = rows[:limit]
    output = build_horizon_eligibility(rows, horizons=config.candidate_horizons_hours)
    output.sort(key=lambda row: (str(row["timing_structure"]), int(row["horizon_hours"]), str(row["family_id"]), str(row["market_id"])))
    summary = {
        "rows": len(output), "input_contracts": len(rows),
        "input_families": len({row["family_id"] for row in rows}),
        "groups": summarize(output),
    }
    print(json.dumps(summary, sort_keys=True))
    if not dry_run:
        write_csv(output_path, output)
    return summary


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        run(args.input, args.output, config_path=args.config, limit=args.limit, dry_run=args.dry_run)
    except (FileNotFoundError, ValueError) as exc:
        raise SystemExit(f"Invalid input: {exc}") from exc


if __name__ == "__main__":
    main()
