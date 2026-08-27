"""
23_audit_occurrence_anchor_anomalies.py

Audit whether markets settle before their purported occurrence anchor.

Inputs:
    data/processed/markets_fixed_clock_final.csv
    data/processed/markets_scheduled_event_start_final.csv

Outputs:
    data/processed/fixed_clock_occurrence_audit.csv
    data/processed/scheduled_event_occurrence_audit.csv
    data/processed/markets_fixed_clock_clean_candidates.csv
    data/processed/markets_scheduled_event_start_clean_candidates.csv
    outputs/occurrence_anchor_anomaly_report.md

Conservative family-level rule:
    Exclude an entire family when any contract in that family settled more
    than 15 minutes before the occurrence anchor.

Why:
    Such a pattern suggests cancellation, rescheduling, stale occurrence
    metadata, or an occurrence timestamp that is not the actual scheduled
    event start / fixed observation time.

No API calls are made.
"""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path


FIXED_INPUT = Path(
    "data/processed/markets_fixed_clock_final.csv"
)
SCHEDULED_INPUT = Path(
    "data/processed/markets_scheduled_event_start_final.csv"
)

FIXED_AUDIT = Path(
    "data/processed/fixed_clock_occurrence_audit.csv"
)
SCHEDULED_AUDIT = Path(
    "data/processed/scheduled_event_occurrence_audit.csv"
)

FIXED_CLEAN = Path(
    "data/processed/markets_fixed_clock_clean_candidates.csv"
)
SCHEDULED_CLEAN = Path(
    "data/processed/markets_scheduled_event_start_clean_candidates.csv"
)

REPORT_PATH = Path(
    "outputs/occurrence_anchor_anomaly_report.md"
)

EARLY_SETTLEMENT_TOLERANCE_HOURS = 0.25


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


def get_family_id(row: dict) -> str:
    return str(
        row.get("family_id_v2")
        or row.get("family_id")
        or row.get("event_ticker_joined")
        or row.get("market_id")
        or ""
    )


def audit_rows(
    rows: list[dict],
    sample_type: str,
) -> tuple[list[dict], list[dict], dict]:
    family_rows = defaultdict(list)

    for row in rows:
        family_rows[get_family_id(row)].append(row)

    family_decisions = {}
    family_details = {}

    for family_id, members in family_rows.items():
        reasons = set()
        settlement_offsets = []
        open_leads = []

        for row in members:
            anchor = parse_time(
                row.get("anchor_time_final_v2")
                or row.get("occurrence_anchor_time")
            )
            settlement = parse_time(
                row.get("actual_settlement_time")
            )
            market_open = parse_time(
                row.get("market_open_time")
            )

            if anchor is None:
                reasons.add("missing_occurrence_anchor")

            if anchor and market_open:
                open_lead = (
                    anchor - market_open
                ).total_seconds() / 3600.0
                open_leads.append(open_lead)

                if open_lead < 0:
                    reasons.add("market_opened_after_occurrence")

            if anchor and settlement:
                settlement_offset = (
                    settlement - anchor
                ).total_seconds() / 3600.0
                settlement_offsets.append(settlement_offset)

                if (
                    settlement_offset
                    < -EARLY_SETTLEMENT_TOLERANCE_HOURS
                ):
                    reasons.add(
                        "settled_more_than_15m_before_occurrence"
                    )

        if reasons:
            decision = "exclude_pending_manual_review"
        else:
            decision = "keep_candidate"

        family_decisions[family_id] = decision
        family_details[family_id] = {
            "family_decision": decision,
            "family_reasons": " || ".join(sorted(reasons)),
            "family_contract_count": len(members),
            "family_min_settlement_offset_hours": (
                round(min(settlement_offsets), 6)
                if settlement_offsets
                else ""
            ),
            "family_max_settlement_offset_hours": (
                round(max(settlement_offsets), 6)
                if settlement_offsets
                else ""
            ),
            "family_min_open_lead_hours": (
                round(min(open_leads), 6)
                if open_leads
                else ""
            ),
            "family_max_open_lead_hours": (
                round(max(open_leads), 6)
                if open_leads
                else ""
            ),
        }

    audit = []
    clean = []

    for row in rows:
        family_id = get_family_id(row)
        anchor = parse_time(
            row.get("anchor_time_final_v2")
            or row.get("occurrence_anchor_time")
        )
        settlement = parse_time(
            row.get("actual_settlement_time")
        )
        market_open = parse_time(
            row.get("market_open_time")
        )
        previous_anchor = parse_time(
            row.get("anchor_time_previous")
        )

        output = dict(row)
        output.update(family_details[family_id])
        output["sample_type_audit"] = sample_type

        output["settlement_minus_occurrence_hours"] = (
            round(
                (
                    settlement - anchor
                ).total_seconds() / 3600.0,
                6,
            )
            if settlement and anchor
            else ""
        )

        output["open_to_occurrence_hours"] = (
            round(
                (
                    anchor - market_open
                ).total_seconds() / 3600.0,
                6,
            )
            if market_open and anchor
            else ""
        )

        output["previous_anchor_minus_occurrence_hours"] = (
            round(
                (
                    previous_anchor - anchor
                ).total_seconds() / 3600.0,
                6,
            )
            if previous_anchor and anchor
            else ""
        )

        audit.append(output)

        if family_decisions[family_id] == "keep_candidate":
            clean.append(output)

    decision_counts = Counter(family_decisions.values())
    reason_counts = Counter()

    for details in family_details.values():
        reasons = details["family_reasons"]

        if not reasons:
            reason_counts["none"] += 1
        else:
            for reason in reasons.split(" || "):
                reason_counts[reason] += 1

    stats = {
        "contract_count": len(rows),
        "family_count": len(family_rows),
        "clean_contract_count": len(clean),
        "clean_family_count": len(
            {
                get_family_id(row)
                for row in clean
            }
        ),
        "decision_counts": decision_counts,
        "reason_counts": reason_counts,
    }

    return audit, clean, stats


def add_report_section(
    lines: list[str],
    title: str,
    stats: dict,
) -> None:
    lines.extend(
        [
            f"## {title}",
            "",
            f"- Input contracts: {stats['contract_count']}",
            f"- Input families: {stats['family_count']}",
            f"- Clean candidate contracts: "
            f"{stats['clean_contract_count']}",
            f"- Clean candidate families: "
            f"{stats['clean_family_count']}",
            "",
            "### Family decisions",
            "",
        ]
    )

    for key, value in sorted(
        stats["decision_counts"].items()
    ):
        lines.append(f"- {key}: {value}")

    lines.extend(["", "### Family-level flags", ""])

    for key, value in sorted(
        stats["reason_counts"].items()
    ):
        lines.append(f"- {key}: {value}")

    lines.append("")


def main() -> None:
    fixed_rows = read_csv(FIXED_INPUT)
    scheduled_rows = read_csv(SCHEDULED_INPUT)

    fixed_audit, fixed_clean, fixed_stats = audit_rows(
        fixed_rows,
        "fixed_clock",
    )
    scheduled_audit, scheduled_clean, scheduled_stats = (
        audit_rows(
            scheduled_rows,
            "scheduled_event_start",
        )
    )

    write_csv(FIXED_AUDIT, fixed_audit)
    write_csv(SCHEDULED_AUDIT, scheduled_audit)
    write_csv(FIXED_CLEAN, fixed_clean)
    write_csv(SCHEDULED_CLEAN, scheduled_clean)

    lines = [
        "# Occurrence Anchor Anomaly Audit",
        "",
        "An entire family is flagged when any member settles more "
        "than 15 minutes before its occurrence anchor.",
        "",
    ]

    add_report_section(
        lines,
        "Strict fixed-clock sample",
        fixed_stats,
    )
    add_report_section(
        lines,
        "Scheduled-event-start sample",
        scheduled_stats,
    )

    lines.extend(
        [
            "## Interpretation",
            "",
            "- Pre-occurrence settlement can indicate cancellation, "
            "rescheduling, stale occurrence metadata, or the wrong "
            "semantic interpretation of occurrence_datetime.",
            "- Flagged families remain excluded until manually verified.",
            "- After this audit, horizon eligibility should be rebuilt "
            "from the two clean-candidate files.",
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
