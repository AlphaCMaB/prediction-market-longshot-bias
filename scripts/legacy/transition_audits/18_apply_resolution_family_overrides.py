"""
18_apply_resolution_family_overrides.py

Inputs:
    data/processed/resolution_anchor_contract_audit_v2.csv
    data/manual/resolution_family_overrides.csv

Outputs:
    data/processed/markets_scheduled_absolute_final.csv
    data/processed/markets_scheduled_absolute_pending_verification.csv
    data/processed/markets_deadline_window_final.csv
    data/processed/markets_trigger_relative_final.csv
    data/processed/markets_excluded_anchor_final.csv
    data/processed/resolution_anchor_contract_final.csv
    outputs/resolution_anchor_final_report.md
"""

from __future__ import annotations

import csv
from collections import Counter
from pathlib import Path


CONTRACT_AUDIT_PATH = Path(
    "data/processed/resolution_anchor_contract_audit_v2.csv"
)
OVERRIDES_PATH = Path(
    "data/manual/resolution_family_overrides.csv"
)

PROCESSED_DIR = Path("data/processed")
OUTPUTS_DIR = Path("outputs")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

FINAL_ALL_PATH = PROCESSED_DIR / "resolution_anchor_contract_final.csv"
FIXED_PATH = PROCESSED_DIR / "markets_scheduled_absolute_final.csv"
PENDING_PATH = (
    PROCESSED_DIR
    / "markets_scheduled_absolute_pending_verification.csv"
)
DEADLINE_PATH = PROCESSED_DIR / "markets_deadline_window_final.csv"
TRIGGER_PATH = PROCESSED_DIR / "markets_trigger_relative_final.csv"
EXCLUDED_PATH = PROCESSED_DIR / "markets_excluded_anchor_final.csv"
REPORT_PATH = OUTPUTS_DIR / "resolution_anchor_final_report.md"


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")

    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict]) -> None:
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


def main() -> None:
    contracts = read_csv(CONTRACT_AUDIT_PATH)
    overrides = read_csv(OVERRIDES_PATH)

    override_by_family = {
        row["family_id"]: row
        for row in overrides
        if row.get("family_id")
    }

    final_rows = []

    for contract in contracts:
        family_id = contract.get("family_id_v2", "")
        override = override_by_family.get(family_id)

        if override is None:
            review_status = "missing_family_override"
            final_type = "exclude"
            manual_time = ""
            note = "No reviewed family override was found."
        else:
            review_status = override.get("review_status", "")
            final_type = override.get("manual_anchor_type", "")
            manual_time = override.get("manual_anchor_time", "")
            note = override.get("review_note", "")

        if manual_time:
            anchor_time_final = manual_time
            anchor_time_source_final = "manual_anchor_time"
        elif final_type in {"scheduled_absolute", "deadline_window"}:
            anchor_time_final = contract.get("scheduled_end_time", "")
            anchor_time_source_final = contract.get(
                "scheduled_end_source",
                "contract_scheduled_end_time",
            )
        else:
            anchor_time_final = ""
            anchor_time_source_final = ""

        primary_fixed_eligible = (
            review_status == "reviewed_include_fixed"
            and final_type == "scheduled_absolute"
            and bool(anchor_time_final)
        )

        pending_verification = (
            review_status == "reviewed_needs_anchor_verification"
            and final_type == "scheduled_absolute"
        )

        final_row = dict(contract)
        final_row.update(
            {
                "review_status_final": review_status,
                "anchor_type_final": final_type,
                "anchor_time_final": anchor_time_final,
                "anchor_time_source_final": anchor_time_source_final,
                "primary_fixed_eligible": (
                    "1" if primary_fixed_eligible else "0"
                ),
                "pending_anchor_verification": (
                    "1" if pending_verification else "0"
                ),
                "final_review_note": note,
            }
        )

        final_rows.append(final_row)

    fixed_rows = [
        row for row in final_rows
        if row["primary_fixed_eligible"] == "1"
    ]
    pending_rows = [
        row for row in final_rows
        if row["pending_anchor_verification"] == "1"
    ]
    deadline_rows = [
        row for row in final_rows
        if row["review_status_final"] == "reviewed_separate_deadline"
    ]
    trigger_rows = [
        row for row in final_rows
        if row["review_status_final"] == "reviewed_separate_trigger"
    ]
    excluded_rows = [
        row for row in final_rows
        if row["review_status_final"] in {
            "reviewed_exclude_unclear",
            "missing_family_override",
        }
    ]

    write_csv(FINAL_ALL_PATH, final_rows)
    write_csv(FIXED_PATH, fixed_rows)
    write_csv(PENDING_PATH, pending_rows)
    write_csv(DEADLINE_PATH, deadline_rows)
    write_csv(TRIGGER_PATH, trigger_rows)
    write_csv(EXCLUDED_PATH, excluded_rows)

    status_counts = Counter(
        row["review_status_final"] for row in final_rows
    )
    venue_counts = Counter(
        (row.get("venue", ""), row["review_status_final"])
        for row in final_rows
    )

    lines = [
        "# Final Resolution Anchor Split",
        "",
        f"- Total contracts: {len(final_rows)}",
        f"- Primary fixed-time contracts: {len(fixed_rows)}",
        f"- Pending fixed-time verification: {len(pending_rows)}",
        f"- Deadline-window contracts: {len(deadline_rows)}",
        f"- Trigger-relative contracts: {len(trigger_rows)}",
        f"- Excluded contracts: {len(excluded_rows)}",
        "",
        "## Contract counts by review status",
        "",
    ]

    for status, count in sorted(status_counts.items()):
        lines.append(f"- {status}: {count}")

    lines.extend(["", "## Venue and status", ""])

    for (venue, status), count in sorted(venue_counts.items()):
        lines.append(f"- {venue} / {status}: {count}")

    lines.extend(
        [
            "",
            "## Primary analysis rule",
            "",
            "Only rows in `markets_scheduled_absolute_final.csv` "
            "should enter the main 24h/48h/168h fixed-time analysis.",
            "",
            "Rows in the pending-verification file remain excluded "
            "until their exact ex-ante anchor is checked against the "
            "contract rules.",
        ]
    )

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved: {REPORT_PATH}")


if __name__ == "__main__":
    main()
