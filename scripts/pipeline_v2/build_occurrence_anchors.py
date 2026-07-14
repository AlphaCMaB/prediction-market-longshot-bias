"""Build verified ex-ante occurrence anchors from downloaded metadata CSV."""

from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from scripts.common.io_utils import read_csv, write_csv
from scripts.pipeline_v2.anchors import select_anchor


REQUIRED_COLUMNS = {"market_id", "family_id", "family_id_source"}


def _truthy(value: Any) -> bool:
    return str(value or "").strip().lower() in {"1", "true", "yes", "verified"}


def validate_columns(rows: list[dict[str, Any]], required=REQUIRED_COLUMNS) -> None:
    available = set(rows[0]) if rows else set()
    missing = sorted(set(required) - available)
    if missing:
        raise ValueError(f"Missing required columns: {', '.join(missing)}")


def build_rows(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, Any]]:
    result = []
    for source in rows:
        row = dict(source)
        selection = select_anchor(
            occurrence_datetime=row.get("occurrence_datetime"),
            occurrence_verified=_truthy(row.get("occurrence_datetime_verified")),
            scheduled_timestamp=row.get("verified_scheduled_timestamp"),
            scheduled_timestamp_verified=_truthy(row.get("verified_scheduled_timestamp_validated")),
            strike_date=row.get("strike_date"),
            strike_date_semantically_verified=_truthy(row.get("strike_date_semantically_verified")),
            manual_override=row.get("manual_override_time"),
            manual_override_verified=_truthy(row.get("manual_override_verified")),
            close_time=row.get("close_time"),
            review_note=str(row.get("review_note") or ""),
        )
        row.update(selection.to_dict())
        result.append(row)
    return sorted(result, key=lambda row: (str(row["family_id"]), str(row["market_id"])))


def run(input_path: Path, output_path: Path, *, limit: int | None = None, dry_run: bool = False) -> dict[str, int]:
    rows = read_csv(input_path)
    validate_columns(rows)
    if limit is not None:
        rows = rows[:limit]
    output = build_rows(rows)
    summary = {"rows": len(output), "families": len({row["family_id"] for row in output})}
    print(json.dumps(summary, sort_keys=True))
    if not dry_run:
        write_csv(output_path, output)
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
