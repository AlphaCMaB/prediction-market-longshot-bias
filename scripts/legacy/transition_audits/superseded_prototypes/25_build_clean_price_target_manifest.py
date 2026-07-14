"""
25_build_clean_price_target_manifest.py

Build the final target-time manifest for price extraction.

Inputs:
    data/processed/fixed_clock_horizon_manifest_clean.csv
    data/processed/scheduled_event_start_horizon_manifest_clean.csv

Selected analyses:
    fixed_clock:
        1h only

    scheduled_event_start:
        1h, 6h, 12h

Outputs:
    data/processed/price_snapshot_targets_clean.csv
    data/processed/price_history_market_universe_clean.csv
    outputs/price_target_manifest_report.md

No API calls are made.
"""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


FIXED_INPUT = Path(
    "data/processed/fixed_clock_horizon_manifest_clean.csv"
)
SCHEDULED_INPUT = Path(
    "data/processed/scheduled_event_start_horizon_manifest_clean.csv"
)

TARGET_OUTPUT = Path(
    "data/processed/price_snapshot_targets_clean.csv"
)
UNIVERSE_OUTPUT = Path(
    "data/processed/price_history_market_universe_clean.csv"
)
REPORT_PATH = Path(
    "outputs/price_target_manifest_report.md"
)

FIXED_HORIZONS = {1}
SCHEDULED_HORIZONS = {1, 6, 12}


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

    fieldnames = list(rows[0].keys())

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
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


def get_market_id(row: dict) -> str:
    return str(
        row.get("market_id")
        or row.get("ticker")
        or row.get("id")
        or ""
    )


def get_family_id(row: dict) -> str:
    return str(
        row.get("family_id_analysis")
        or row.get("family_id_v2")
        or row.get("family_id")
        or row.get("event_ticker_joined")
        or get_market_id(row)
    )


def get_sample_type(row: dict) -> str:
    sample_type = str(
        row.get("sample_type")
        or row.get("timing_structure")
        or ""
    )

    if sample_type.startswith("fixed_clock"):
        return "fixed_clock"

    if sample_type.startswith("scheduled_event_start"):
        return "scheduled_event_start"

    return sample_type


def is_selected(row: dict) -> bool:
    eligible = (
        row.get("eligible_clean") == "1"
        or row.get("eligible") == "1"
    )

    if not eligible:
        return False

    try:
        horizon = int(row.get("horizon_hours", ""))
    except Exception:
        return False

    sample_type = get_sample_type(row)

    if sample_type == "fixed_clock":
        return horizon in FIXED_HORIZONS

    if sample_type == "scheduled_event_start":
        return horizon in SCHEDULED_HORIZONS

    return False


def build_targets(
    fixed_rows: list[dict],
    scheduled_rows: list[dict],
) -> list[dict]:
    selected = []

    for row in fixed_rows + scheduled_rows:
        if not is_selected(row):
            continue

        output = dict(row)
        output["analysis_sample"] = get_sample_type(row)
        output["market_id_analysis"] = get_market_id(row)
        output["family_id_analysis"] = get_family_id(row)
        output["target_key"] = "|".join(
            [
                output.get("venue", ""),
                output["market_id_analysis"],
                output["analysis_sample"],
                str(output.get("horizon_hours", "")),
                str(output.get("target_time", "")),
            ]
        )

        selected.append(output)

    deduped = {}

    for row in selected:
        deduped[row["target_key"]] = row

    result = list(deduped.values())

    result.sort(
        key=lambda row: (
            row["analysis_sample"],
            int(row["horizon_hours"]),
            row["family_id_analysis"],
            row["market_id_analysis"],
        )
    )

    return result


def build_universe(targets: list[dict]) -> list[dict]:
    groups = defaultdict(list)

    for row in targets:
        key = (
            row.get("venue", ""),
            row["market_id_analysis"],
        )
        groups[key].append(row)

    universe = []

    for (venue, market_id), rows in sorted(groups.items()):
        target_times = [
            parse_time(row.get("target_time"))
            for row in rows
        ]
        target_times = [
            value for value in target_times
            if value is not None
        ]

        samples = sorted(
            {row["analysis_sample"] for row in rows}
        )
        horizons = sorted(
            {int(row["horizon_hours"]) for row in rows}
        )
        families = sorted(
            {row["family_id_analysis"] for row in rows}
        )

        representative = rows[0]

        universe.append(
            {
                "venue": venue,
                "market_id": market_id,
                "ticker": representative.get("ticker", ""),
                "title": representative.get("title", ""),
                "family_id": " || ".join(families),
                "analysis_samples": " || ".join(samples),
                "horizons_hours": " || ".join(
                    str(value) for value in horizons
                ),
                "target_count": len(rows),
                "earliest_target_time": (
                    min(target_times).isoformat()
                    if target_times
                    else ""
                ),
                "latest_target_time": (
                    max(target_times).isoformat()
                    if target_times
                    else ""
                ),
                "anchor_time": representative.get(
                    "anchor_time_final_v2",
                    representative.get(
                        "occurrence_anchor_time",
                        "",
                    ),
                ),
                "actual_settlement_time": representative.get(
                    "actual_settlement_time",
                    "",
                ),
            }
        )

    return universe


def write_report(
    targets: list[dict],
    universe: list[dict],
) -> None:
    counts = Counter(
        (
            row["analysis_sample"],
            int(row["horizon_hours"]),
        )
        for row in targets
    )

    families = defaultdict(set)
    markets = defaultdict(set)

    for row in targets:
        key = (
            row["analysis_sample"],
            int(row["horizon_hours"]),
        )
        families[key].add(row["family_id_analysis"])
        markets[key].add(row["market_id_analysis"])

    lines = [
        "# Clean Price Target Manifest",
        "",
        "This manifest contains only the selected clean analyses:",
        "",
        "- fixed-clock: 1h",
        "- scheduled-event-start: 1h, 6h, 12h",
        "",
        f"- Target rows: {len(targets)}",
        f"- Unique markets requiring history: {len(universe)}",
        "",
        "## Counts by sample and horizon",
        "",
    ]

    for key in sorted(counts):
        sample, horizon = key

        lines.extend(
            [
                f"### {sample} / {horizon}h",
                "",
                f"- Target rows: {counts[key]}",
                f"- Unique markets: {len(markets[key])}",
                f"- Unique families: {len(families[key])}",
                "",
            ]
        )

    lines.extend(
        [
            "## Next methodological checks",
            "",
            "- Pull or reuse one price history per unique market.",
            "- Select the latest valid price at or before each target time.",
            "- Record snapshot staleness in minutes.",
            "- Count independent families within each probability bin.",
            "- Bootstrap and perform leave-one-family-out checks by family.",
            "- Do not run the Kelly analysis until calibration survives "
            "those checks.",
            "",
            f"- Target manifest: `{TARGET_OUTPUT}`",
            f"- Market universe: `{UNIVERSE_OUTPUT}`",
        ]
    )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )

    print(f"Saved: {REPORT_PATH}")


def main() -> None:
    fixed_rows = read_csv(FIXED_INPUT)
    scheduled_rows = read_csv(SCHEDULED_INPUT)

    targets = build_targets(
        fixed_rows,
        scheduled_rows,
    )
    universe = build_universe(targets)

    write_csv(TARGET_OUTPUT, targets)
    write_csv(UNIVERSE_OUTPUT, universe)
    write_report(targets, universe)


if __name__ == "__main__":
    main()
