"""
08_filter_kalshi_48h_candidates.py

Find Kalshi markets that were open at least 48 hours before settlement.

Input:
    data/raw/kalshi/kalshi_recent_settled_markets.jsonl

Outputs:
    data/processed/kalshi_48h_candidates.csv
    outputs/kalshi_48h_candidate_report.md
"""

from __future__ import annotations

import csv
import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

RAW_KALSHI_PATH = Path("data/raw/kalshi/kalshi_recent_settled_markets.jsonl")
PROCESSED_DIR = Path("data/processed")
OUTPUTS_DIR = Path("outputs")
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = PROCESSED_DIR / "kalshi_48h_candidates.csv"
REPORT_PATH = OUTPUTS_DIR / "kalshi_48h_candidate_report.md"
MIN_HOURS_OPEN_BEFORE_SETTLEMENT = 48.0


def read_jsonl(path: Path) -> list[dict]:
    rows = []
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None
    value = str(value).strip()
    if len(value) == 10:
        value = value + "T00:00:00+00:00"
    value = value.replace("Z", "+00:00")
    if value.endswith("+00"):
        value = value + ":00"
    try:
        dt = datetime.fromisoformat(value)
    except Exception:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def hours_between(later: datetime, earlier: datetime) -> float:
    return (later - earlier).total_seconds() / 3600.0


def make_candidate_row(market: dict, hours_open_before_settlement: float) -> dict:
    ticker = str(market.get("ticker") or "")
    event_ticker = str(market.get("event_ticker") or "")
    series_ticker = event_ticker.split("-")[0] if event_ticker else ""

    settlement_value = to_float(market.get("settlement_value_dollars"))
    if settlement_value in {0.0, 1.0}:
        outcome = int(settlement_value)
        final_no_price = 1.0 - settlement_value
    else:
        outcome = ""
        final_no_price = ""

    open_time = parse_time(market.get("open_time"))
    settlement_time = parse_time(market.get("settlement_ts"))

    return {
        "venue": "kalshi",
        "market_id": ticker,
        "event_id": event_ticker,
        "ticker": ticker,
        "title": str(market.get("title") or ""),
        "category": series_ticker or "unknown",
        "resolution_time": settlement_time.isoformat() if settlement_time else "",
        "outcome": outcome,
        "yes_token_id": "",
        "no_token_id": "",
        "kalshi_event_ticker": event_ticker,
        "kalshi_series_ticker": series_ticker,
        "volume": to_float(market.get("volume_fp")),
        "liquidity": to_float(market.get("liquidity_dollars")),
        "final_yes_price": settlement_value,
        "final_no_price": final_no_price,
        "raw_status": str(market.get("status") or ""),
        "open_time": open_time.isoformat() if open_time else "",
        "settlement_ts": settlement_time.isoformat() if settlement_time else "",
        "hours_open_before_settlement": hours_open_before_settlement,
    }


def main() -> None:
    print("=" * 80)
    print("Filter Kalshi 48h candidates")
    print("No API calls.")

    raw_rows = read_jsonl(RAW_KALSHI_PATH)
    candidates = []
    drop_reasons = Counter()
    series_counts = Counter()

    for market in raw_rows:
        ticker = str(market.get("ticker") or "")
        event_ticker = str(market.get("event_ticker") or "")

        if not ticker:
            drop_reasons["missing_ticker"] += 1
            continue
        if ticker.startswith("KXMVE") or event_ticker.startswith("KXMVE"):
            drop_reasons["multivariate_market"] += 1
            continue
        if market.get("market_type") != "binary":
            drop_reasons["not_binary"] += 1
            continue
        if market.get("status") not in {"settled", "finalized"}:
            drop_reasons["not_finalized"] += 1
            continue

        settlement_value = to_float(market.get("settlement_value_dollars"))
        if settlement_value not in {0.0, 1.0}:
            drop_reasons["non_binary_settlement"] += 1
            continue

        open_time = parse_time(market.get("open_time"))
        settlement_time = parse_time(market.get("settlement_ts"))
        if open_time is None:
            drop_reasons["missing_open_time"] += 1
            continue
        if settlement_time is None:
            drop_reasons["missing_settlement_time"] += 1
            continue

        hours_open = hours_between(settlement_time, open_time)
        if hours_open < MIN_HOURS_OPEN_BEFORE_SETTLEMENT:
            drop_reasons["open_less_than_48h_before_settlement"] += 1
            continue

        row = make_candidate_row(market, hours_open)
        candidates.append(row)
        series_counts[row["kalshi_series_ticker"]] += 1

    fieldnames = [
        "venue", "market_id", "event_id", "ticker", "title", "category",
        "resolution_time", "outcome", "yes_token_id", "no_token_id",
        "kalshi_event_ticker", "kalshi_series_ticker", "volume", "liquidity",
        "final_yes_price", "final_no_price", "raw_status", "open_time",
        "settlement_ts", "hours_open_before_settlement",
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(candidates)

    lines = []
    lines.append("# Kalshi 48h Candidate Report")
    lines.append("")
    lines.append("This report filters Kalshi markets to those open at least 48 hours before settlement.")
    lines.append("")
    lines.append("No API calls were made in this step.")
    lines.append("")
    lines.append("## Summary")
    lines.append("")
    lines.append(f"- Raw Kalshi rows: {len(raw_rows)}")
    lines.append(f"- 48h candidate rows: {len(candidates)}")
    lines.append(f"- Minimum hours open before settlement: {MIN_HOURS_OPEN_BEFORE_SETTLEMENT}")
    lines.append("")
    lines.append("## Drop reasons")
    lines.append("")
    if not drop_reasons:
        lines.append("- None")
    else:
        for reason, count in sorted(drop_reasons.items()):
            lines.append(f"- {reason}: {count}")

    lines.append("")
    lines.append("## Top Kalshi series among candidates")
    lines.append("")
    if not series_counts:
        lines.append("- None")
    else:
        for series, count in series_counts.most_common(30):
            lines.append(f"- {series}: {count}")

    lines.append("")
    lines.append("## Example candidates")
    lines.append("")
    if not candidates:
        lines.append("- None")
    else:
        for row in candidates[:20]:
            lines.append(
                f"- {row['market_id']} | "
                f"hours_open={float(row['hours_open_before_settlement']):.2f} | "
                f"{row['title']}"
            )

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")

    print(f"Saved: {OUT_CSV}")
    print(f"Saved: {REPORT_PATH}")
    print("")
    print("Raw Kalshi rows:", len(raw_rows))
    print("48h candidates:", len(candidates))
    print("Drop reasons:", dict(drop_reasons))


if __name__ == "__main__":
    main()
