"""
17_build_resolution_anchor_table.py

Purpose:
    Audit how each market's forecasting anchor should be defined.

This script separates markets into three groups:

1. scheduled_event
   The uncertainty is tied to a known event/date, such as:
   - a sports match
   - an election
   - weather on a specified day
   - a closing/settlement price on a specified date

2. deadline_window
   The event may happen at an unknown time before a known deadline, such as:
   - "Will Harvard announce X by Dec 31?"
   - "Will a company launch before June?"
   - "Will Trump say X before a date?"

3. unclear
   The available metadata/title is not sufficient for a reliable automatic
   classification. These markets should be manually reviewed or excluded.

Inputs:
    data/processed/markets_metadata_clean.csv
    data/raw/polymarket/polymarket_recent_closed_markets.jsonl
    data/raw/kalshi/kalshi_recent_settled_markets.jsonl

Outputs:
    data/processed/market_resolution_anchor_audit.csv
    data/processed/markets_scheduled_event.csv
    data/processed/markets_deadline_window.csv
    data/processed/markets_anchor_unclear.csv
    outputs/resolution_anchor_audit_report.md

Important:
    This script does NOT call APIs.
    The classification is heuristic and should be manually reviewed before the
    final paper.
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


CLEAN_PATH = Path("data/processed/markets_metadata_clean.csv")
POLY_RAW_PATH = Path(
    "data/raw/polymarket/polymarket_recent_closed_markets.jsonl"
)
KALSHI_RAW_PATH = Path(
    "data/raw/kalshi/kalshi_recent_settled_markets.jsonl"
)

PROCESSED_DIR = Path("data/processed")
OUTPUTS_DIR = Path("outputs")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

AUDIT_PATH = PROCESSED_DIR / "market_resolution_anchor_audit.csv"
SCHEDULED_PATH = PROCESSED_DIR / "markets_scheduled_event.csv"
DEADLINE_PATH = PROCESSED_DIR / "markets_deadline_window.csv"
UNCLEAR_PATH = PROCESSED_DIR / "markets_anchor_unclear.csv"
REPORT_PATH = OUTPUTS_DIR / "resolution_anchor_audit_report.md"


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")

    with path.open("r", newline="", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def read_jsonl(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")

    rows = []

    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))

    return rows


def write_csv(path: Path, rows: list[dict]) -> None:
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


def first_nonempty(*values: Any) -> str:
    for value in values:
        if value is not None and str(value).strip():
            return str(value).strip()

    return ""


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None

    value = str(value).strip().replace("Z", "+00:00")

    if len(value) == 10:
        value += "T00:00:00+00:00"

    try:
        dt = datetime.fromisoformat(value)
    except Exception:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def iso_or_blank(value: str | None) -> str:
    dt = parse_time(value)
    return dt.isoformat() if dt else ""


def hours_between(later_value: str, earlier_value: str) -> float | str:
    later = parse_time(later_value)
    earlier = parse_time(earlier_value)

    if later is None or earlier is None:
        return ""

    return (later - earlier).total_seconds() / 3600.0


def clean_text(value: str) -> str:
    value = str(value or "").lower()
    value = value.replace("&", " and ")
    value = re.sub(r"[^a-z0-9\s$%°:/.-]", " ", value)
    value = re.sub(r"\s+", " ", value).strip()
    return value


def classify_anchor(title: str, venue: str) -> tuple[str, str, str]:
    text = clean_text(title)

    fixed_patterns = [
        r"\bon\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)",
        r"\bon\s+\w+\s+\d{1,2}",
        r"\bmatch\b",
        r"\bgame\b",
        r"\bset\s+\d+\b",
        r"\bmap\s+\d+\b",
        r"\btournament\b",
        r"\belection\b",
        r"\bhigh temp\b",
        r"\blow temp\b",
        r"\btemperature\b",
        r"\bweather\b",
        r"\bsettlement price\b",
        r"\bclosing price\b",
        r"\bclose above\b",
        r"\bclose below\b",
        r"\bfomc\b",
        r"\bcpi\b",
        r"\bgdp\b",
        r"\bunemployment\b",
        r"\bearnings\b",
    ]

    deadline_patterns = [
        r"\bby\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)",
        r"\bbefore\s+(jan|feb|mar|apr|may|jun|jul|aug|sep|sept|oct|nov|dec)",
        r"\bby\s+\w+\s+\d{1,2}",
        r"\bbefore\s+\w+\s+\d{1,2}",
        r"\bby the end of\b",
        r"\bbefore the end of\b",
        r"\bannounce\b",
        r"\bannouncement\b",
        r"\blaunch\b",
        r"\brelease\b",
        r"\bresign\b",
        r"\bstep down\b",
        r"\bsay\b",
        r"\bmention\b",
        r"\bapprove\b",
        r"\bsign\b",
        r"\bfile\b",
        r"\bipo\b",
        r"\blist\b",
        r"\btge\b",
        r"\btoken generation event\b",
        r"\breach an agreement\b",
        r"\bceasefire\b",
        r"\bmou\b",
    ]

    for pattern in fixed_patterns:
        if re.search(pattern, text):
            return (
                "scheduled_event",
                "high",
                f"matched scheduled-event pattern: {pattern}",
            )

    for pattern in deadline_patterns:
        if re.search(pattern, text):
            return (
                "deadline_window",
                "high",
                f"matched deadline-window pattern: {pattern}",
            )

    if venue == "kalshi":
        return (
            "unclear",
            "medium",
            "Kalshi market with no decisive title pattern",
        )

    return (
        "unclear",
        "low",
        "no decisive automatic pattern",
    )


def build_raw_lookups(
    poly_rows: list[dict],
    kalshi_rows: list[dict],
) -> tuple[dict[str, dict], dict[str, dict]]:
    poly_lookup = {}

    for row in poly_rows:
        market_id = first_nonempty(row.get("id"), row.get("conditionId"))
        if market_id:
            poly_lookup[market_id] = row

    kalshi_lookup = {}

    for row in kalshi_rows:
        ticker = first_nonempty(row.get("ticker"))
        if ticker:
            kalshi_lookup[ticker] = row

    return poly_lookup, kalshi_lookup


def enrich_row(
    clean_row: dict,
    poly_lookup: dict[str, dict],
    kalshi_lookup: dict[str, dict],
) -> dict:
    venue = clean_row.get("venue", "")
    market_id = clean_row.get("market_id", "")
    title = clean_row.get("title", "")

    raw = {}

    if venue == "polymarket":
        raw = poly_lookup.get(market_id, {})

        actual_settlement_time = first_nonempty(
            raw.get("closedTime"),
            clean_row.get("resolution_time"),
        )

        scheduled_end_time = first_nonempty(
            raw.get("endDateIso"),
            raw.get("endDate"),
        )

        market_open_time = first_nonempty(
            raw.get("startDate"),
            raw.get("createdAt"),
        )

        settlement_source = (
            "polymarket_closedTime"
            if raw.get("closedTime")
            else "clean_resolution_time"
        )

        scheduled_source = (
            "polymarket_endDateIso"
            if raw.get("endDateIso")
            else (
                "polymarket_endDate"
                if raw.get("endDate")
                else ""
            )
        )

    elif venue == "kalshi":
        ticker = first_nonempty(
            clean_row.get("ticker"),
            market_id,
        )
        raw = kalshi_lookup.get(ticker, {})

        actual_settlement_time = first_nonempty(
            raw.get("settlement_ts"),
            clean_row.get("resolution_time"),
        )

        scheduled_end_time = first_nonempty(
            raw.get("close_time"),
            raw.get("expiration_time"),
        )

        market_open_time = first_nonempty(
            raw.get("open_time"),
        )

        settlement_source = (
            "kalshi_settlement_ts"
            if raw.get("settlement_ts")
            else "clean_resolution_time"
        )

        scheduled_source = (
            "kalshi_close_time"
            if raw.get("close_time")
            else (
                "kalshi_expiration_time"
                if raw.get("expiration_time")
                else ""
            )
        )

    else:
        actual_settlement_time = clean_row.get("resolution_time", "")
        scheduled_end_time = ""
        market_open_time = ""
        settlement_source = "clean_resolution_time"
        scheduled_source = ""

    anchor_type, confidence, reason = classify_anchor(title, venue)

    scheduled_iso = iso_or_blank(scheduled_end_time)
    settlement_iso = iso_or_blank(actual_settlement_time)
    open_iso = iso_or_blank(market_open_time)

    if not scheduled_iso:
        anchor_type = "unclear"
        confidence = "low"
        reason = "missing scheduled end/deadline timestamp"

    forecast_anchor_time = (
        scheduled_iso
        if anchor_type in {"scheduled_event", "deadline_window"}
        else ""
    )

    settlement_delay = hours_between(
        settlement_iso,
        scheduled_iso,
    )

    result = dict(clean_row)

    result.update(
        {
            "market_open_time": open_iso,
            "scheduled_end_time": scheduled_iso,
            "actual_settlement_time": settlement_iso,
            "forecast_anchor_time": forecast_anchor_time,
            "anchor_type": anchor_type,
            "anchor_confidence": confidence,
            "anchor_reason": reason,
            "scheduled_end_source": scheduled_source,
            "settlement_time_source": settlement_source,
            "settlement_delay_hours": settlement_delay,
            "main_fixed_time_eligible": (
                "1"
                if anchor_type == "scheduled_event"
                and bool(forecast_anchor_time)
                else "0"
            ),
            "deadline_window_eligible": (
                "1"
                if anchor_type == "deadline_window"
                and bool(forecast_anchor_time)
                else "0"
            ),
        }
    )

    return result


def write_report(rows: list[dict]) -> None:
    type_counts = Counter(row["anchor_type"] for row in rows)
    venue_type_counts = Counter(
        (row["venue"], row["anchor_type"])
        for row in rows
    )
    confidence_counts = Counter(
        row["anchor_confidence"]
        for row in rows
    )

    lines = []

    lines.append("# Resolution Anchor Audit Report")
    lines.append("")
    lines.append(
        "This report separates scheduled-event markets from "
        "deadline-window and unclear markets."
    )
    lines.append("")
    lines.append(
        "The main forecasting analysis should focus on "
        "`scheduled_event` markets."
    )
    lines.append("")
    lines.append(
        "`deadline_window` markets should be analyzed separately using "
        "their known deadline, not their realized early settlement time."
    )
    lines.append("")
    lines.append("## Overall")
    lines.append("")
    lines.append(f"- Total markets: {len(rows)}")

    for anchor_type, count in sorted(type_counts.items()):
        lines.append(f"- {anchor_type}: {count}")

    lines.append("")
    lines.append("## By venue and anchor type")
    lines.append("")

    for (venue, anchor_type), count in sorted(
        venue_type_counts.items()
    ):
        lines.append(f"- {venue} / {anchor_type}: {count}")

    lines.append("")
    lines.append("## Confidence")
    lines.append("")

    for confidence, count in sorted(confidence_counts.items()):
        lines.append(f"- {confidence}: {count}")

    lines.append("")
    lines.append("## Scheduled-event examples")
    lines.append("")

    examples = [
        row for row in rows
        if row["anchor_type"] == "scheduled_event"
    ][:20]

    for row in examples:
        lines.append(
            f"- {row['venue']} | {row['market_id']} | "
            f"anchor={row['forecast_anchor_time']} | "
            f"{row['title']}"
        )

    lines.append("")
    lines.append("## Deadline-window examples")
    lines.append("")

    examples = [
        row for row in rows
        if row["anchor_type"] == "deadline_window"
    ][:20]

    for row in examples:
        lines.append(
            f"- {row['venue']} | {row['market_id']} | "
            f"deadline={row['forecast_anchor_time']} | "
            f"{row['title']}"
        )

    lines.append("")
    lines.append("## Unclear examples requiring review")
    lines.append("")

    examples = [
        row for row in rows
        if row["anchor_type"] == "unclear"
    ][:30]

    for row in examples:
        lines.append(
            f"- {row['venue']} | {row['market_id']} | "
            f"{row['title']}"
        )

    lines.append("")
    lines.append("## Next methodology step")
    lines.append("")
    lines.append(
        "1. Re-run the primary 24h/48h/168h analysis using only "
        "`markets_scheduled_event.csv`."
    )
    lines.append(
        "2. Analyze `markets_deadline_window.csv` separately, anchored "
        "to the known deadline."
    )
    lines.append(
        "3. Exclude or manually classify `markets_anchor_unclear.csv`."
    )

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved: {REPORT_PATH}")


def main() -> None:
    print("=" * 80)
    print("Build resolution anchor audit table")
    print("No API calls.")

    clean_rows = read_csv(CLEAN_PATH)
    poly_rows = read_jsonl(POLY_RAW_PATH)
    kalshi_rows = read_jsonl(KALSHI_RAW_PATH)

    poly_lookup, kalshi_lookup = build_raw_lookups(
        poly_rows,
        kalshi_rows,
    )

    rows = [
        enrich_row(
            clean_row=row,
            poly_lookup=poly_lookup,
            kalshi_lookup=kalshi_lookup,
        )
        for row in clean_rows
    ]

    scheduled_rows = [
        row for row in rows
        if row["anchor_type"] == "scheduled_event"
    ]

    deadline_rows = [
        row for row in rows
        if row["anchor_type"] == "deadline_window"
    ]

    unclear_rows = [
        row for row in rows
        if row["anchor_type"] == "unclear"
    ]

    write_csv(AUDIT_PATH, rows)
    write_csv(SCHEDULED_PATH, scheduled_rows)
    write_csv(DEADLINE_PATH, deadline_rows)
    write_csv(UNCLEAR_PATH, unclear_rows)
    write_report(rows)

    print("")
    print("=" * 80)
    print("Resolution anchor audit complete")
    print("Scheduled-event markets:", len(scheduled_rows))
    print("Deadline-window markets:", len(deadline_rows))
    print("Unclear markets:", len(unclear_rows))
    print("Report:", REPORT_PATH)


if __name__ == "__main__":
    main()
