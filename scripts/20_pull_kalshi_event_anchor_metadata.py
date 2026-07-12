"""
20_pull_kalshi_event_anchor_metadata.py

Fetch event-level metadata for Kalshi families currently in the fixed-time
candidate sample. The script audits `occurrence_datetime`, `strike_date`,
`strike_period`, early-close flags, and within-family close-time variation.

Input:
    data/processed/markets_scheduled_absolute_final.csv

Outputs:
    data/raw/kalshi/events/<event_ticker>.json
    data/processed/kalshi_event_anchor_metadata.csv
    outputs/kalshi_event_anchor_metadata_report.md
"""

from __future__ import annotations

import csv
import json
import time
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

import requests


INPUT_PATH = Path("data/processed/markets_scheduled_absolute_final.csv")
RAW_DIR = Path("data/raw/kalshi/events")
OUTPUT_CSV = Path("data/processed/kalshi_event_anchor_metadata.csv")
REPORT_PATH = Path("outputs/kalshi_event_anchor_metadata_report.md")
BASE_URL = "https://external-api.kalshi.com/trade-api/v2/events"

MAX_RETRIES = 4
TIMEOUT_SECONDS = 30
SLEEP_SECONDS = 0.15


def read_csv(path):
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")

    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(path, rows):
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


def parse_time(value):
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


def normalize_time(value):
    dt = parse_time(value)
    return dt.isoformat() if dt else ""


def unique_times(values):
    return sorted(
        {
            normalize_time(value)
            for value in values
            if normalize_time(value)
        }
    )


def spread_hours(values):
    parsed = [parse_time(value) for value in values]
    parsed = [value for value in parsed if value is not None]

    if not parsed:
        return ""

    return round(
        (max(parsed) - min(parsed)).total_seconds() / 3600.0,
        6,
    )


def event_ticker_from_row(row):
    family_id = str(
        row.get("family_id_v2")
        or row.get("family_id")
        or ""
    )

    prefix = "kalshi_event::"

    if family_id.startswith(prefix):
        return family_id[len(prefix):]

    return str(row.get("event_ticker") or "").strip()


def safe_name(value):
    return "".join(
        char if char.isalnum() or char in "-_." else "_"
        for char in value
    )


def fetch_event(session, event_ticker):
    path = RAW_DIR / f"{safe_name(event_ticker)}.json"

    if path.exists():
        with path.open(encoding="utf-8") as f:
            return json.load(f)

    url = f"{BASE_URL}/{event_ticker}"
    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                url,
                params={"with_nested_markets": "true"},
                timeout=TIMEOUT_SECONDS,
            )

            if response.status_code == 429:
                wait = min(2 ** attempt, 20)
                print(f"Rate limited; sleeping {wait}s")
                time.sleep(wait)
                continue

            response.raise_for_status()
            payload = response.json()

            with path.open("w", encoding="utf-8") as f:
                json.dump(
                    payload,
                    f,
                    ensure_ascii=False,
                    indent=2,
                )

            time.sleep(SLEEP_SECONDS)
            return payload

        except Exception as exc:
            last_error = exc

            if attempt < MAX_RETRIES:
                wait = min(2 ** attempt, 20)
                print(
                    f"Retry {attempt}/{MAX_RETRIES} for "
                    f"{event_ticker}: {exc}"
                )
                time.sleep(wait)

    raise RuntimeError(
        f"Failed to fetch {event_ticker}: {last_error}"
    )


def get_markets(payload):
    event = payload.get("event") or {}
    nested = event.get("markets")

    if isinstance(nested, list) and nested:
        return nested

    markets = payload.get("markets")
    return markets if isinstance(markets, list) else []


def choose_anchor(event, markets):
    occurrence_values = unique_times(
        [market.get("occurrence_datetime") for market in markets]
    )
    strike_date = normalize_time(event.get("strike_date"))
    strike_period = str(event.get("strike_period") or "").strip()

    if len(occurrence_values) == 1:
        return (
            occurrence_values[0],
            "market_occurrence_datetime",
            "high",
            "All markets share one occurrence_datetime.",
        )

    if len(occurrence_values) > 1:
        return (
            strike_date,
            "event_strike_date" if strike_date else "",
            "manual_review",
            "Multiple occurrence_datetime values exist.",
        )

    if strike_date:
        return (
            strike_date,
            "event_strike_date",
            "medium" if strike_period else "manual_review",
            "No occurrence_datetime; strike_date is a candidate.",
        )

    return (
        "",
        "",
        "none",
        "No occurrence_datetime or strike_date.",
    )


def summarize(event_ticker, payload):
    event = payload.get("event") or {}
    markets = get_markets(payload)

    occurrence_values = unique_times(
        [market.get("occurrence_datetime") for market in markets]
    )
    close_values = unique_times(
        [market.get("close_time") for market in markets]
    )
    expected_values = unique_times(
        [
            market.get("expected_expiration_time")
            for market in markets
        ]
    )
    expiration_values = unique_times(
        [market.get("expiration_time") for market in markets]
    )

    early_conditions = []
    can_close_early = []

    for market in markets:
        if market.get("can_close_early") is not None:
            can_close_early.append(
                bool(market.get("can_close_early"))
            )

        condition = str(
            market.get("early_close_condition") or ""
        ).strip()

        if condition and condition not in early_conditions:
            early_conditions.append(condition)

    anchor, source, confidence, reason = choose_anchor(
        event,
        markets,
    )

    return {
        "event_ticker": event_ticker,
        "event_title": str(event.get("title") or ""),
        "category": str(event.get("category") or ""),
        "series_ticker": str(event.get("series_ticker") or ""),
        "strike_date": normalize_time(event.get("strike_date")),
        "strike_period": str(event.get("strike_period") or ""),
        "market_count": len(markets),
        "occurrence_datetime_count": len(occurrence_values),
        "occurrence_datetime_values": " || ".join(
            occurrence_values[:20]
        ),
        "occurrence_datetime_spread_hours": spread_hours(
            occurrence_values
        ),
        "close_time_count": len(close_values),
        "close_time_values": " || ".join(close_values[:20]),
        "close_time_spread_hours": spread_hours(close_values),
        "expected_expiration_time_values": " || ".join(
            expected_values[:20]
        ),
        "expiration_time_values": " || ".join(
            expiration_values[:20]
        ),
        "any_can_close_early": (
            "1" if any(can_close_early) else "0"
        ),
        "all_can_close_early": (
            "1"
            if can_close_early and all(can_close_early)
            else "0"
        ),
        "early_close_condition_examples": " || ".join(
            early_conditions[:10]
        ),
        "candidate_anchor_time": anchor,
        "candidate_anchor_source": source,
        "candidate_anchor_confidence": confidence,
        "candidate_anchor_reason": reason,
        "fetch_status": "ok",
        "fetch_error": "",
    }


def write_report(rows):
    confidence_counts = Counter(
        row["candidate_anchor_confidence"] for row in rows
    )
    source_counts = Counter(
        row["candidate_anchor_source"] or "none"
        for row in rows
    )

    occurrence_counts = Counter()

    for row in rows:
        count = int(row["occurrence_datetime_count"])

        if count == 0:
            occurrence_counts["none"] += 1
        elif count == 1:
            occurrence_counts["one"] += 1
        else:
            occurrence_counts["multiple"] += 1

    lines = [
        "# Kalshi Event Anchor Metadata Report",
        "",
        f"- Events fetched: {len(rows)}",
        "",
        "## Candidate-anchor confidence",
        "",
    ]

    for key, value in sorted(confidence_counts.items()):
        lines.append(f"- {key}: {value}")

    lines.extend(["", "## Candidate-anchor source", ""])

    for key, value in sorted(source_counts.items()):
        lines.append(f"- {key}: {value}")

    lines.extend(
        ["", "## occurrence_datetime availability", ""]
    )

    for key, value in sorted(occurrence_counts.items()):
        lines.append(f"- {key}: {value}")

    lines.extend(
        [
            "",
            "## Methodological rule",
            "",
            "- Do not use `close_time` as the preferred anchor.",
            "- A shared `occurrence_datetime` is the strongest "
            "automatic candidate.",
            "- `strike_date` remains a review candidate.",
            "- Multiple occurrence times require semantic review.",
            "",
            f"- CSV: `{OUTPUT_CSV}`",
            f"- Raw event files: `{RAW_DIR}`",
        ]
    )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")


def main():
    source_rows = read_csv(INPUT_PATH)

    event_tickers = sorted(
        {
            event_ticker_from_row(row)
            for row in source_rows
            if row.get("venue") == "kalshi"
            and event_ticker_from_row(row)
        }
    )

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    print(f"Unique Kalshi events: {len(event_tickers)}")

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "prediction-market-longshot-bias-anchor-audit"
            )
        }
    )

    rows = []

    for index, event_ticker in enumerate(
        event_tickers,
        start=1,
    ):
        print(f"[{index}/{len(event_tickers)}] {event_ticker}")

        try:
            payload = fetch_event(session, event_ticker)
            rows.append(summarize(event_ticker, payload))
        except Exception as exc:
            rows.append(
                {
                    "event_ticker": event_ticker,
                    "event_title": "",
                    "category": "",
                    "series_ticker": "",
                    "strike_date": "",
                    "strike_period": "",
                    "market_count": 0,
                    "occurrence_datetime_count": 0,
                    "occurrence_datetime_values": "",
                    "occurrence_datetime_spread_hours": "",
                    "close_time_count": 0,
                    "close_time_values": "",
                    "close_time_spread_hours": "",
                    "expected_expiration_time_values": "",
                    "expiration_time_values": "",
                    "any_can_close_early": "",
                    "all_can_close_early": "",
                    "early_close_condition_examples": "",
                    "candidate_anchor_time": "",
                    "candidate_anchor_source": "",
                    "candidate_anchor_confidence": "fetch_error",
                    "candidate_anchor_reason": "",
                    "fetch_status": "error",
                    "fetch_error": str(exc),
                }
            )

    write_csv(OUTPUT_CSV, rows)
    write_report(rows)

    print(f"Saved: {OUTPUT_CSV}")
    print(f"Saved: {REPORT_PATH}")


if __name__ == "__main__":
    main()
