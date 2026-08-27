"""
24_rebuild_clean_occurrence_horizon_manifests.py

Rebuild horizon eligibility after removing families with suspicious
pre-occurrence settlement.

Inputs:
    data/processed/markets_fixed_clock_clean_candidates.csv
    data/processed/markets_scheduled_event_start_clean_candidates.csv

Outputs:
    data/processed/fixed_clock_horizon_manifest_clean.csv
    data/processed/scheduled_event_start_horizon_manifest_clean.csv
    outputs/clean_occurrence_horizon_eligibility_report.md

No API calls are made.
"""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


FIXED_INPUT = Path(
    "data/processed/markets_fixed_clock_clean_candidates.csv"
)
SCHEDULED_INPUT = Path(
    "data/processed/markets_scheduled_event_start_clean_candidates.csv"
)

FIXED_OUTPUT = Path(
    "data/processed/fixed_clock_horizon_manifest_clean.csv"
)
SCHEDULED_OUTPUT = Path(
    "data/processed/scheduled_event_start_horizon_manifest_clean.csv"
)

REPORT_PATH = Path(
    "outputs/clean_occurrence_horizon_eligibility_report.md"
)

HORIZONS = [1, 6, 12, 24, 48]


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")

    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        path.write_text("", encoding="utf-8")
        print(f"Saved empty file: {path}")
        return

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=list(rows[0].keys()),
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved: {path} ({len(rows)} rows)")


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None

    text = str(value).strip().replace("Z", "+00:00")

    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def iso(dt: datetime | None) -> str:
    return dt.isoformat() if dt else ""


def family_id(row: dict) -> str:
    return str(
        row.get("family_id_v2")
        or row.get("family_id")
        or row.get("event_ticker_joined")
        or row.get("market_id")
        or ""
    )


def build_manifest(
    rows: list[dict],
    sample_type: str,
) -> list[dict]:
    result = []

    for row in rows:
        open_time = parse_time(row.get("market_open_time"))
        anchor_time = parse_time(
            row.get("anchor_time_final_v2")
            or row.get("occurrence_anchor_time")
        )
        settlement_time = parse_time(
            row.get("actual_settlement_time")
        )

        for horizon in HORIZONS:
            target_time = (
                anchor_time - timedelta(hours=horizon)
                if anchor_time
                else None
            )

            if anchor_time is None:
                status = "missing_occurrence_anchor"
            elif open_time is None:
                status = "missing_open_time"
            elif open_time > target_time:
                status = "market_opened_after_target"
            elif (
                settlement_time is not None
                and settlement_time <= target_time
            ):
                status = "settled_before_or_at_target"
            else:
                status = "eligible"

            output = dict(row)
            output.update(
                {
                    "sample_type": sample_type,
                    "horizon_hours": horizon,
                    "target_time": iso(target_time),
                    "eligibility_status_clean": status,
                    "eligible_clean": (
                        "1" if status == "eligible" else "0"
                    ),
                    "family_id_analysis": family_id(row),
                }
            )

            result.append(output)

    return result


def summarize(rows: list[dict]) -> dict:
    status_counts = Counter(
        (
            int(row["horizon_hours"]),
            row["eligibility_status_clean"],
        )
        for row in rows
    )

    contract_counts = Counter()
    family_sets = defaultdict(set)

    for row in rows:
        if row["eligible_clean"] != "1":
            continue

        horizon = int(row["horizon_hours"])
        contract_counts[horizon] += 1
        family_sets[horizon].add(row["family_id_analysis"])

    return {
        "status_counts": status_counts,
        "contract_counts": contract_counts,
        "family_sets": family_sets,
    }


def add_section(
    lines: list[str],
    title: str,
    source_rows: list[dict],
    manifest_rows: list[dict],
) -> None:
    stats = summarize(manifest_rows)

    lines.extend(
        [
            f"## {title}",
            "",
            f"- Clean source contracts: {len(source_rows)}",
            f"- Clean source families: "
            f"{len({family_id(row) for row in source_rows})}",
            "",
        ]
    )

    for horizon in HORIZONS:
        lines.extend(
            [
                f"### {horizon}h",
                "",
                f"- Eligible contracts: "
                f"{stats['contract_counts'][horizon]}",
                f"- Eligible families: "
                f"{len(stats['family_sets'][horizon])}",
            ]
        )

        statuses = sorted(
            {
                status
                for h, status in stats["status_counts"]
                if h == horizon and status != "eligible"
            }
        )

        for status in statuses:
            lines.append(
                f"- {status}: "
                f"{stats['status_counts'][(horizon, status)]}"
            )

        lines.append("")


def main() -> None:
    fixed_rows = read_csv(FIXED_INPUT)
    scheduled_rows = read_csv(SCHEDULED_INPUT)

    fixed_manifest = build_manifest(
        fixed_rows,
        "fixed_clock_clean",
    )
    scheduled_manifest = build_manifest(
        scheduled_rows,
        "scheduled_event_start_clean",
    )

    write_csv(FIXED_OUTPUT, fixed_manifest)
    write_csv(SCHEDULED_OUTPUT, scheduled_manifest)

    lines = [
        "# Clean Occurrence-Anchor Horizon Eligibility",
        "",
        "Families with suspicious pre-occurrence settlement have "
        "already been removed.",
        "",
    ]

    add_section(
        lines,
        "Strict fixed-clock sample",
        fixed_rows,
        fixed_manifest,
    )
    add_section(
        lines,
        "Clean scheduled-event-start sample",
        scheduled_rows,
        scheduled_manifest,
    )

    lines.extend(
        [
            "## Decision guide",
            "",
            "- Prefer horizons with at least 30 independent families.",
            "- Treat 20-29 families as exploratory.",
            "- Treat fewer than 20 families as descriptive only.",
            "- Continue to de-cluster or bootstrap by family.",
        ]
    )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )

    print(f"Saved: {REPORT_PATH}")


if __name__ == "__main__":
    main()
