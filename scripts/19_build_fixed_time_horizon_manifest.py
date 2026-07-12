"""
19_build_fixed_time_horizon_manifest.py

Build a pre-pull eligibility manifest for the validated fixed-time sample.

Input:
    data/processed/markets_scheduled_absolute_final.csv

Outputs:
    data/processed/fixed_time_horizon_manifest.csv
    outputs/fixed_time_horizon_eligibility_report.md

A contract-horizon row is eligible when:
    1. market_open_time and anchor_time_final are present;
    2. the market was already open at the target snapshot;
    3. the market had not already settled before the target snapshot.

The target snapshot is:
    target_time = anchor_time_final - horizon_hours

This script does not call any API.
"""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path


INPUT_PATH = Path(
    "data/processed/markets_scheduled_absolute_final.csv"
)
# MANIFEST_PATH = Path(
#     "data/processed/fixed_time_horizon_manifest.csv"
# )
# REPORT_PATH = Path(
#     "outputs/fixed_time_horizon_eligibility_report.md"
# )

MANIFEST_PATH = Path(
    "data/processed/fixed_time_horizon_manifest_short.csv"
)

REPORT_PATH = Path(
    "outputs/fixed_time_horizon_eligibility_short_report.md"
)
#HORIZONS = [24, 48, 168]
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
        return

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=list(rows[0].keys()),
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None

    text = str(value).strip().replace("Z", "+00:00")

    if len(text) == 10:
        text += "T00:00:00+00:00"

    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def iso(dt: datetime | None) -> str:
    return dt.isoformat() if dt else ""


def build_manifest(rows: list[dict]) -> list[dict]:
    manifest = []

    for row in rows:
        open_time = parse_time(row.get("market_open_time"))
        anchor_time = parse_time(row.get("anchor_time_final"))
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
                status = "missing_anchor_time"

            elif open_time is None:
                status = "missing_open_time"

            elif target_time is None:
                status = "missing_target_time"

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
                    "horizon_hours": horizon,
                    "target_time": iso(target_time),
                    "eligibility_status": status,
                    "eligible": "1" if status == "eligible" else "0",
                    "open_to_anchor_hours": (
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
                    "settlement_minus_anchor_hours": (
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

            manifest.append(output)

    return manifest


def write_report(
    source_rows: list[dict],
    manifest: list[dict],
) -> None:
    overall = Counter(
        row["eligibility_status"] for row in manifest
    )

    by_horizon = Counter(
        (
            row["horizon_hours"],
            row["eligibility_status"],
        )
        for row in manifest
    )

    by_venue_horizon = Counter(
        (
            row.get("venue", ""),
            row["horizon_hours"],
            row["eligibility_status"],
        )
        for row in manifest
    )

    eligible_families = defaultdict(set)

    for row in manifest:
        if row["eligible"] == "1":
            eligible_families[
                (
                    row.get("venue", ""),
                    row["horizon_hours"],
                )
            ].add(
                row.get(
                    "family_id_v2",
                    row.get("family_id", ""),
                )
            )

    lines = [
        "# Fixed-Time Horizon Eligibility Report",
        "",
        "This report is computed before any new price-history pull.",
        "",
        f"- Fixed-time contracts: {len(source_rows)}",
        f"- Contract-horizon rows: {len(manifest)}",
        "",
        "## Overall eligibility status",
        "",
    ]

    for status, count in sorted(overall.items()):
        lines.append(f"- {status}: {count}")

    lines.extend(["", "## By horizon", ""])

    for horizon in HORIZONS:
        lines.append(f"### {horizon}h")
        lines.append("")

        statuses = sorted(
            {
                status
                for h, status in by_horizon
                if h == horizon
            }
        )

        for status in statuses:
            lines.append(
                f"- {status}: "
                f"{by_horizon[(horizon, status)]}"
            )

        lines.append("")

    lines.extend(
        [
            "## Eligible contracts and families by venue",
            "",
        ]
    )

    venues = sorted(
        {row.get("venue", "") for row in manifest}
    )

    for venue in venues:
        lines.append(f"### {venue}")
        lines.append("")

        for horizon in HORIZONS:
            contracts = by_venue_horizon[
                (venue, horizon, "eligible")
            ]
            families = len(
                eligible_families[(venue, horizon)]
            )

            lines.append(
                f"- {horizon}h: "
                f"{contracts} contracts, "
                f"{families} unique families"
            )

        lines.append("")

    lines.extend(
        [
            "## Interpretation",
            "",
            "Contract counts can greatly exceed independent event-family "
            "counts. Downstream calibration and bootstrap analysis should "
            "continue to de-cluster by family.",
            "",
            "A large fixed-time metadata sample does not automatically "
            "imply a large usable 24h/48h/168h price sample. Only rows "
            "marked `eligible` should proceed to price extraction.",
        ]
    )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def main() -> None:
    rows = read_csv(INPUT_PATH)
    manifest = build_manifest(rows)

    write_csv(MANIFEST_PATH, manifest)
    write_report(rows, manifest)

    eligible = sum(
        row["eligible"] == "1" for row in manifest
    )

    print(f"Fixed-time contracts: {len(rows)}")
    print(f"Contract-horizon rows: {len(manifest)}")
    print(f"Eligible contract-horizon rows: {eligible}")
    print(f"Saved: {MANIFEST_PATH}")
    print(f"Saved: {REPORT_PATH}")


if __name__ == "__main__":
    main()
