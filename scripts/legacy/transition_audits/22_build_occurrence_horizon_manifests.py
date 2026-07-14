"""
22_build_occurrence_horizon_manifests.py

Build horizon-eligibility manifests using corrected occurrence anchors.

Inputs:
    data/processed/markets_fixed_clock_final.csv
    data/processed/markets_scheduled_event_start_final.csv

Outputs:
    data/processed/fixed_clock_horizon_manifest.csv
    data/processed/scheduled_event_start_horizon_manifest.csv
    outputs/occurrence_horizon_eligibility_report.md
"""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


FIXED_INPUT = Path(
    "data/processed/markets_fixed_clock_final.csv"
)
SCHEDULED_INPUT = Path(
    "data/processed/markets_scheduled_event_start_final.csv"
)

FIXED_OUTPUT = Path(
    "data/processed/fixed_clock_horizon_manifest.csv"
)
SCHEDULED_OUTPUT = Path(
    "data/processed/scheduled_event_start_horizon_manifest.csv"
)

REPORT_PATH = Path(
    "outputs/occurrence_horizon_eligibility_report.md"
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
                    "eligibility_status": status,
                    "eligible": (
                        "1" if status == "eligible" else "0"
                    ),
                    "family_id_analysis": family_id(row),
                    "open_to_anchor_hours_v2": (
                        round(
                            (
                                anchor_time - open_time
                            ).total_seconds()
                            / 3600.0,
                            3,
                        )
                        if open_time and anchor_time
                        else ""
                    ),
                    "settlement_minus_anchor_hours_v2": (
                        round(
                            (
                                settlement_time - anchor_time
                            ).total_seconds()
                            / 3600.0,
                            3,
                        )
                        if settlement_time and anchor_time
                        else ""
                    ),
                }
            )

            result.append(output)

    return result


def summarize(rows: list[dict]) -> dict:
    statuses = Counter(
        (
            int(row["horizon_hours"]),
            row["eligibility_status"],
        )
        for row in rows
    )

    contracts = Counter()
    families = defaultdict(set)

    for row in rows:
        if row["eligible"] != "1":
            continue

        horizon = int(row["horizon_hours"])
        contracts[horizon] += 1
        families[horizon].add(row["family_id_analysis"])

    return {
        "statuses": statuses,
        "contracts": contracts,
        "families": families,
    }


def add_report_section(
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
            f"- Source contracts: {len(source_rows)}",
            f"- Source families: "
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
                f"{stats['contracts'][horizon]}",
                f"- Eligible families: "
                f"{len(stats['families'][horizon])}",
            ]
        )

        for status in sorted(
            {
                status
                for h, status in stats["statuses"]
                if h == horizon and status != "eligible"
            }
        ):
            lines.append(
                f"- {status}: "
                f"{stats['statuses'][(horizon, status)]}"
            )

        lines.append("")


def main() -> None:
    fixed_rows = read_csv(FIXED_INPUT)
    scheduled_rows = read_csv(SCHEDULED_INPUT)

    fixed_manifest = build_manifest(
        fixed_rows,
        "fixed_clock",
    )
    scheduled_manifest = build_manifest(
        scheduled_rows,
        "scheduled_event_start",
    )

    write_csv(FIXED_OUTPUT, fixed_manifest)
    write_csv(SCHEDULED_OUTPUT, scheduled_manifest)

    lines = [
        "# Occurrence-Anchor Horizon Eligibility",
        "",
        "This report uses corrected event-level occurrence anchors, "
        "not realized close or settlement timestamps.",
        "",
    ]

    add_report_section(
        lines,
        "Strict fixed-clock sample",
        fixed_rows,
        fixed_manifest,
    )
    add_report_section(
        lines,
        "Scheduled-event-start sample",
        scheduled_rows,
        scheduled_manifest,
    )

    lines.extend(
        [
            "## Interpretation",
            "",
            "- `fixed_clock` is the strict primary sample.",
            "- `scheduled_event_start` is a separate pre-event sample.",
            "- Contract counts may greatly exceed family counts.",
            "- Do not run bin-level inference until eligible family "
            "counts are checked by horizon.",
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
