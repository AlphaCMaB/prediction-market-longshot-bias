"""
17_build_resolution_anchor_review_v2.py

Conservative resolution-anchor audit for prediction-market research.

This script DOES NOT build the final analysis sample yet. It creates:

1. A contract-level audit table.
2. A family-level review sheet.
3. A blank manual-override template.

Four anchor classes:
    scheduled_absolute
        The uncertainty is tied to a date/time known ex ante.

    deadline_window
        The event can occur at an unknown time before a known deadline.

    trigger_relative
        The evaluation time depends on another event whose timing is unknown,
        such as "one day after launch" or "next opponent".

    unclear
        The available title/metadata is insufficient.

The classifier is intentionally conservative. A contract enters the future
fixed-time sample only when:
    anchor_type_auto == "scheduled_absolute"
    and anchor_validated_auto == "1"

Inputs:
    data/processed/markets_metadata_clean.csv
    data/raw/polymarket/polymarket_recent_closed_markets.jsonl
    data/raw/kalshi/kalshi_recent_settled_markets.jsonl

Outputs:
    data/processed/resolution_anchor_contract_audit_v2.csv
    data/manual/resolution_family_review_v2.csv
    data/manual/resolution_family_overrides_template.csv
    outputs/resolution_anchor_audit_v2_report.md
"""

from __future__ import annotations

import csv
import json
import re
from collections import Counter, defaultdict
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
MANUAL_DIR = Path("data/manual")
OUTPUTS_DIR = Path("outputs")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
MANUAL_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

CONTRACT_AUDIT_PATH = (
    PROCESSED_DIR / "resolution_anchor_contract_audit_v2.csv"
)
FAMILY_REVIEW_PATH = (
    MANUAL_DIR / "resolution_family_review_v2.csv"
)
OVERRIDE_TEMPLATE_PATH = (
    MANUAL_DIR / "resolution_family_overrides_template.csv"
)
REPORT_PATH = OUTPUTS_DIR / "resolution_anchor_audit_v2_report.md"


MONTHS = {
    "january": 1,
    "jan": 1,
    "february": 2,
    "feb": 2,
    "march": 3,
    "mar": 3,
    "april": 4,
    "apr": 4,
    "may": 5,
    "june": 6,
    "jun": 6,
    "july": 7,
    "jul": 7,
    "august": 8,
    "aug": 8,
    "september": 9,
    "sept": 9,
    "sep": 9,
    "october": 10,
    "oct": 10,
    "november": 11,
    "nov": 11,
    "december": 12,
    "dec": 12,
}


TRIGGER_RELATIVE_PATTERNS = [
    r"\bone day after\b",
    r"\b\d+\s+(hour|hours|day|days|week|weeks)\s+after\b",
    r"\bafter launch\b",
    r"\bafter (the )?ipo\b",
    r"\bafter (the )?tge\b",
    r"\bafter token generation\b",
    r"\bafter listing\b",
    r"\bonce .* launch",
    r"\bnext opponent\b",
    r"\bfight .* next\b",
    r"\bplay .* next\b",
    r"\bnext to\b",
    r"\bfirst .* after\b",
]

DEADLINE_PATTERNS = [
    r"\bby\s+(the\s+)?end\s+of\b",
    r"\bbefore\s+(the\s+)?end\s+of\b",
    r"\bby\s+(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sept|sep|october|oct|november|nov|december|dec)\b",
    r"\bbefore\s+(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sept|sep|october|oct|november|nov|december|dec)\b",
    r"\buntil\s+(january|jan|february|feb|march|mar|april|apr|may|june|jun|july|jul|august|aug|september|sept|sep|october|oct|november|nov|december|dec)\b",
    r"\bduring\s+20\d{2}\b",
    r"\bin\s+20\d{2}\b",
]

SCHEDULED_KEYWORD_PATTERNS = [
    r"\bgame\b",
    r"\bmatch\b",
    r"\bset\s+\d+\b",
    r"\bmap\s+\d+\b",
    r"\bby-election\b",
    r"\belection\b",
    r"\btemperature\b",
    r"\bhigh temp\b",
    r"\blow temp\b",
    r"\bweather\b",
    r"\bclosing price\b",
    r"\bsettlement price\b",
    r"\bclose above\b",
    r"\bclose below\b",
    r"\bcpi\b",
    r"\bgdp\b",
    r"\bfomc\b",
    r"\bunemployment\b",
    r"\bearnings\b",
]


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


def iso_or_blank(value: str | None) -> str:
    dt = parse_time(value)
    return dt.isoformat() if dt else ""


def normalize_text(value: str) -> str:
    text = str(value or "").lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^\w\s$%°:/.-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def normalize_family_fallback(title: str) -> str:
    """
    Conservative fallback family key.

    This groups obvious threshold variants such as:
        "Espresso FDV above $100M one day after launch"
        "Espresso FDV above $500M one day after launch"

    It will not perfectly group candidate-name markets. Structured event
    metadata is preferred whenever available.
    """
    text = normalize_text(title)

    text = re.sub(
        r"\$?\d+(?:\.\d+)?\s*(k|m|b|t|million|billion|trillion)?",
        "<number>",
        text,
    )
    text = re.sub(r"\b\d{1,2}:\d{2}\b", "<time>", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text[:220]


def get_poly_event_info(raw: dict) -> tuple[str, str]:
    events = raw.get("events")

    if isinstance(events, list) and events:
        event = events[0]

        if isinstance(event, dict):
            event_id = first_nonempty(
                event.get("id"),
                event.get("slug"),
                event.get("ticker"),
            )
            event_title = first_nonempty(
                event.get("title"),
                event.get("question"),
                event.get("slug"),
            )
            return event_id, event_title

    event_id = first_nonempty(
        raw.get("eventId"),
        raw.get("event_id"),
        raw.get("eventSlug"),
    )
    event_title = first_nonempty(
        raw.get("eventTitle"),
        raw.get("event_title"),
    )

    return event_id, event_title


def build_raw_lookups(
    poly_rows: list[dict],
    kalshi_rows: list[dict],
) -> tuple[dict[str, dict], dict[str, dict]]:
    poly_lookup = {}

    for row in poly_rows:
        keys = [
            first_nonempty(row.get("id")),
            first_nonempty(row.get("conditionId")),
            first_nonempty(row.get("condition_id")),
        ]

        for key in keys:
            if key:
                poly_lookup[key] = row

    kalshi_lookup = {}

    for row in kalshi_rows:
        ticker = first_nonempty(row.get("ticker"))

        if ticker:
            kalshi_lookup[ticker] = row

    return poly_lookup, kalshi_lookup


def extract_title_date(
    title: str,
    metadata_time: str,
) -> tuple[str, str]:
    """
    Extracts a date such as "January 2" or "January 2, 2027".

    If the title omits the year, use the metadata anchor's year only for
    consistency checking. The extracted title date is not automatically used
    as the final anchor.
    """
    text = normalize_text(title)

    month_pattern = (
        r"\b("
        + "|".join(sorted(MONTHS.keys(), key=len, reverse=True))
        + r")\s+(\d{1,2})(?:st|nd|rd|th)?"
        + r"(?:[\s,]+(20\d{2}))?\b"
    )

    match = re.search(month_pattern, text)

    if not match:
        return "", ""

    month_name = match.group(1)
    day = int(match.group(2))
    year_text = match.group(3)

    metadata_dt = parse_time(metadata_time)

    if year_text:
        year = int(year_text)
        year_source = "title"
    elif metadata_dt:
        year = metadata_dt.year
        year_source = "metadata_year"
    else:
        return "", ""

    try:
        dt = datetime(
            year,
            MONTHS[month_name],
            day,
            tzinfo=timezone.utc,
        )
    except ValueError:
        return "", ""

    return dt.isoformat(), year_source


def date_difference_hours(value_a: str, value_b: str) -> float | None:
    dt_a = parse_time(value_a)
    dt_b = parse_time(value_b)

    if dt_a is None or dt_b is None:
        return None

    return abs((dt_a - dt_b).total_seconds()) / 3600.0


def classify_contract(
    title: str,
    scheduled_end_time: str,
) -> dict:
    text = normalize_text(title)

    for pattern in TRIGGER_RELATIVE_PATTERNS:
        if re.search(pattern, text):
            return {
                "anchor_type_auto": "trigger_relative",
                "anchor_confidence_auto": "high",
                "anchor_reason_auto": (
                    f"matched trigger-relative pattern: {pattern}"
                ),
                "anchor_validated_auto": "0",
                "title_date": "",
                "title_date_year_source": "",
                "date_alignment_hours": "",
            }

    for pattern in DEADLINE_PATTERNS:
        if re.search(pattern, text):
            title_date, year_source = extract_title_date(
                title,
                scheduled_end_time,
            )

            return {
                "anchor_type_auto": "deadline_window",
                "anchor_confidence_auto": "medium",
                "anchor_reason_auto": (
                    f"matched deadline-window pattern: {pattern}"
                ),
                "anchor_validated_auto": "0",
                "title_date": title_date,
                "title_date_year_source": year_source,
                "date_alignment_hours": "",
            }

    title_date, year_source = extract_title_date(
        title,
        scheduled_end_time,
    )

    scheduled_keyword = ""

    for pattern in SCHEDULED_KEYWORD_PATTERNS:
        if re.search(pattern, text):
            scheduled_keyword = pattern
            break

    if title_date:
        alignment = date_difference_hours(
            title_date,
            scheduled_end_time,
        )

        if alignment is not None and alignment <= 36:
            return {
                "anchor_type_auto": "scheduled_absolute",
                "anchor_confidence_auto": "high",
                "anchor_reason_auto": (
                    "explicit title date agrees with metadata "
                    "within 36 hours"
                ),
                "anchor_validated_auto": "1",
                "title_date": title_date,
                "title_date_year_source": year_source,
                "date_alignment_hours": round(alignment, 3),
            }

        return {
            "anchor_type_auto": "scheduled_absolute",
            "anchor_confidence_auto": "low",
            "anchor_reason_auto": (
                "explicit title date conflicts with or cannot be "
                "matched to metadata"
            ),
            "anchor_validated_auto": "0",
            "title_date": title_date,
            "title_date_year_source": year_source,
            "date_alignment_hours": (
                round(alignment, 3)
                if alignment is not None
                else ""
            ),
        }

    if scheduled_keyword:
        return {
            "anchor_type_auto": "scheduled_absolute",
            "anchor_confidence_auto": "medium",
            "anchor_reason_auto": (
                "scheduled-event keyword found, but no exact title date "
                "was available for automatic validation"
            ),
            "anchor_validated_auto": "0",
            "title_date": "",
            "title_date_year_source": "",
            "date_alignment_hours": "",
        }

    return {
        "anchor_type_auto": "unclear",
        "anchor_confidence_auto": "low",
        "anchor_reason_auto": "no reliable automatic anchor rule",
        "anchor_validated_auto": "0",
        "title_date": "",
        "title_date_year_source": "",
        "date_alignment_hours": "",
    }


def enrich_contract(
    clean_row: dict,
    poly_lookup: dict[str, dict],
    kalshi_lookup: dict[str, dict],
) -> dict:
    venue = first_nonempty(clean_row.get("venue"))
    market_id = first_nonempty(clean_row.get("market_id"))
    title = first_nonempty(clean_row.get("title"))

    raw = {}
    structured_family_key = ""
    structured_family_title = ""

    if venue == "polymarket":
        raw = poly_lookup.get(market_id, {})

        event_id, event_title = get_poly_event_info(raw)

        if event_id:
            structured_family_key = f"polymarket_event::{event_id}"
            structured_family_title = event_title

        scheduled_end_raw = first_nonempty(
            raw.get("endDateIso"),
            raw.get("endDate"),
        )
        settlement_raw = first_nonempty(
            raw.get("closedTime"),
            clean_row.get("resolution_time"),
        )
        open_raw = first_nonempty(
            raw.get("startDate"),
            raw.get("createdAt"),
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

        settlement_source = (
            "polymarket_closedTime"
            if raw.get("closedTime")
            else "clean_resolution_time"
        )

    elif venue == "kalshi":
        raw = kalshi_lookup.get(market_id, {})

        event_ticker = first_nonempty(
            raw.get("event_ticker"),
            raw.get("eventTicker"),
            raw.get("series_ticker"),
            raw.get("seriesTicker"),
        )

        if event_ticker:
            structured_family_key = f"kalshi_event::{event_ticker}"
            structured_family_title = first_nonempty(
                raw.get("event_title"),
                raw.get("series_title"),
                raw.get("title"),
            )

        scheduled_end_raw = first_nonempty(
            raw.get("close_time"),
            raw.get("expiration_time"),
        )
        settlement_raw = first_nonempty(
            raw.get("settlement_ts"),
            clean_row.get("resolution_time"),
        )
        open_raw = first_nonempty(raw.get("open_time"))

        scheduled_source = (
            "kalshi_close_time"
            if raw.get("close_time")
            else (
                "kalshi_expiration_time"
                if raw.get("expiration_time")
                else ""
            )
        )

        settlement_source = (
            "kalshi_settlement_ts"
            if raw.get("settlement_ts")
            else "clean_resolution_time"
        )

    else:
        scheduled_end_raw = ""
        settlement_raw = first_nonempty(
            clean_row.get("resolution_time")
        )
        open_raw = ""
        scheduled_source = ""
        settlement_source = "clean_resolution_time"

    scheduled_end_time = iso_or_blank(scheduled_end_raw)
    actual_settlement_time = iso_or_blank(settlement_raw)
    market_open_time = iso_or_blank(open_raw)

    if structured_family_key:
        family_id = structured_family_key
        family_source = "structured_event_metadata"
    else:
        family_id = (
            f"{venue}_fallback::{normalize_family_fallback(title)}"
        )
        family_source = "normalized_title_fallback"

    classification = classify_contract(
        title=title,
        scheduled_end_time=scheduled_end_time,
    )

    if not scheduled_end_time:
        classification["anchor_validated_auto"] = "0"
        classification["anchor_confidence_auto"] = "low"
        classification["anchor_reason_auto"] += (
            "; scheduled metadata timestamp missing"
        )

    result = dict(clean_row)

    result.update(
        {
            "family_id_v2": family_id,
            "family_source_v2": family_source,
            "family_title_v2": structured_family_title,
            "market_open_time": market_open_time,
            "scheduled_end_time": scheduled_end_time,
            "actual_settlement_time": actual_settlement_time,
            "scheduled_end_source": scheduled_source,
            "settlement_time_source": settlement_source,
            **classification,
        }
    )

    return result


def choose_family_type(rows: list[dict]) -> tuple[str, str]:
    types = Counter(row["anchor_type_auto"] for row in rows)

    if len(types) == 1:
        return next(iter(types)), "all contracts agree"

    top_type, top_count = types.most_common(1)[0]

    if top_count / len(rows) >= 0.8:
        return (
            top_type,
            f"majority rule: {top_count}/{len(rows)} contracts",
        )

    return "mixed_or_unclear", f"mixed contract labels: {dict(types)}"


def aggregate_families(contract_rows: list[dict]) -> list[dict]:
    groups: dict[str, list[dict]] = defaultdict(list)

    for row in contract_rows:
        groups[row["family_id_v2"]].append(row)

    family_rows = []

    for family_id, rows in sorted(groups.items()):
        venue = rows[0].get("venue", "")
        family_type, family_type_reason = choose_family_type(rows)

        validated_count = sum(
            row["anchor_validated_auto"] == "1"
            for row in rows
        )

        scheduled_times = sorted(
            {
                row["scheduled_end_time"]
                for row in rows
                if row["scheduled_end_time"]
            }
        )

        example_titles = " || ".join(
            row.get("title", "")
            for row in rows[:5]
        )

        if (
            family_type == "scheduled_absolute"
            and validated_count == len(rows)
        ):
            recommended_review_status = "auto_accept"
        elif family_type in {
            "trigger_relative",
            "deadline_window",
        }:
            recommended_review_status = "separate_analysis"
        else:
            recommended_review_status = "manual_review"

        family_rows.append(
            {
                "family_id": family_id,
                "venue": venue,
                "family_source": rows[0]["family_source_v2"],
                "family_title": rows[0]["family_title_v2"],
                "contract_count": len(rows),
                "anchor_type_auto": family_type,
                "family_type_reason": family_type_reason,
                "validated_contract_count": validated_count,
                "all_contracts_auto_validated": (
                    "1" if validated_count == len(rows) else "0"
                ),
                "scheduled_time_count": len(scheduled_times),
                "scheduled_time_examples": " || ".join(
                    scheduled_times[:5]
                ),
                "example_titles": example_titles,
                "recommended_review_status": (
                    recommended_review_status
                ),
                "review_status": "",
                "manual_anchor_type": "",
                "manual_anchor_time": "",
                "review_note": "",
            }
        )

    return family_rows


def build_override_template(
    family_rows: list[dict],
) -> list[dict]:
    result = []

    for row in family_rows:
        result.append(
            {
                "family_id": row["family_id"],
                "review_status": "",
                "manual_anchor_type": "",
                "manual_anchor_time": "",
                "review_note": "",
            }
        )

    return result


def write_report(
    contract_rows: list[dict],
    family_rows: list[dict],
) -> None:
    contract_type_counts = Counter(
        row["anchor_type_auto"]
        for row in contract_rows
    )
    family_type_counts = Counter(
        row["anchor_type_auto"]
        for row in family_rows
    )
    review_counts = Counter(
        row["recommended_review_status"]
        for row in family_rows
    )

    validated_contracts = sum(
        row["anchor_validated_auto"] == "1"
        for row in contract_rows
    )

    lines = [
        "# Resolution Anchor Audit V2",
        "",
        "This version uses four anchor classes and applies a "
        "conservative automatic-validation rule.",
        "",
        "## Contract-level summary",
        "",
        f"- Total contracts: {len(contract_rows)}",
        f"- Automatically validated scheduled contracts: "
        f"{validated_contracts}",
    ]

    for anchor_type, count in sorted(contract_type_counts.items()):
        lines.append(f"- {anchor_type}: {count}")

    lines.extend(
        [
            "",
            "## Family-level summary",
            "",
            f"- Total families: {len(family_rows)}",
        ]
    )

    for anchor_type, count in sorted(family_type_counts.items()):
        lines.append(f"- {anchor_type}: {count}")

    lines.extend(
        [
            "",
            "## Recommended review status",
            "",
        ]
    )

    for status, count in sorted(review_counts.items()):
        lines.append(f"- {status}: {count}")

    lines.extend(
        [
            "",
            "## Important rule",
            "",
            "Do not build the fixed-time sample from every contract "
            "labeled `scheduled_absolute`.",
            "",
            "The future primary sample should require:",
            "",
            "```text",
            "anchor_type_final == scheduled_absolute",
            "and",
            "anchor_validated_final == 1",
            "```",
            "",
            "## Files",
            "",
            f"- Contract audit: `{CONTRACT_AUDIT_PATH}`",
            f"- Family review sheet: `{FAMILY_REVIEW_PATH}`",
            f"- Override template: `{OVERRIDE_TEMPLATE_PATH}`",
            "",
            "## Review order",
            "",
            "1. Inspect `manual_review` families.",
            "2. Audit a random sample of `auto_accept` families.",
            "3. Keep `deadline_window` and `trigger_relative` out of "
            "the primary fixed-time analysis.",
        ]
    )

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved: {REPORT_PATH}")


def main() -> None:
    print("=" * 80)
    print("Resolution anchor audit V2")
    print("No API calls.")

    clean_rows = read_csv(CLEAN_PATH)
    poly_rows = read_jsonl(POLY_RAW_PATH)
    kalshi_rows = read_jsonl(KALSHI_RAW_PATH)

    poly_lookup, kalshi_lookup = build_raw_lookups(
        poly_rows,
        kalshi_rows,
    )

    contract_rows = [
        enrich_contract(
            clean_row=row,
            poly_lookup=poly_lookup,
            kalshi_lookup=kalshi_lookup,
        )
        for row in clean_rows
    ]

    family_rows = aggregate_families(contract_rows)
    override_rows = build_override_template(family_rows)

    write_csv(CONTRACT_AUDIT_PATH, contract_rows)
    write_csv(FAMILY_REVIEW_PATH, family_rows)
    write_csv(OVERRIDE_TEMPLATE_PATH, override_rows)
    write_report(contract_rows, family_rows)

    print("")
    print("=" * 80)
    print("Audit complete")
    print("Contracts:", len(contract_rows))
    print("Families:", len(family_rows))
    print("Report:", REPORT_PATH)


if __name__ == "__main__":
    main()
