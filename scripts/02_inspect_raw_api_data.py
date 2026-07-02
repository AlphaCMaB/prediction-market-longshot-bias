"""
02_inspect_raw_api_data.py

Purpose:
    Inspect raw Polymarket and Kalshi API data.

This script does NOT:
    - clean data
    - pull price history
    - calculate p_hat
    - calculate Brier score

It only summarizes what fields exist and previews important columns.
"""

from __future__ import annotations

import json
from collections import Counter
from pathlib import Path


POLYMARKET_PATH = Path("data/raw/polymarket/polymarket_recent_closed_markets.jsonl")
KALSHI_PATH = Path("data/raw/kalshi/kalshi_recent_settled_markets.jsonl")


def read_jsonl(path: Path) -> list[dict]:
    rows = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                rows.append(json.loads(line))
    return rows


def field_coverage(rows: list[dict]) -> Counter:
    counter = Counter()
    for row in rows:
        for key, value in row.items():
            if value not in [None, "", [], {}]:
                counter[key] += 1
    return counter


def inspect_polymarket(rows: list[dict]) -> None:
    print("\n" + "=" * 80)
    print("POLYMARKET")
    print("Rows:", len(rows))

    coverage = field_coverage(rows)

    print("\nTop fields by non-empty count:")
    for field, count in coverage.most_common(30):
        print(f"{field}: {count}")

    print("\nImportant field coverage:")
    important = [
        "id",
        "question",
        "category",
        "closed",
        "closedTime",
        "umaEndDate",
        "endDate",
        "endDateIso",
        "outcomes",
        "outcomePrices",
        "clobTokenIds",
        "volume",
        "volumeNum",
        "liquidity",
        "liquidityNum",
        "umaResolutionStatus",
    ]

    for field in important:
        print(f"{field}: {coverage.get(field, 0)} / {len(rows)}")

    print("\nFirst 5 market previews:")
    for row in rows[:5]:
        print("-" * 80)
        print("id:", row.get("id"))
        print("question:", row.get("question"))
        print("closedTime:", row.get("closedTime"))
        print("umaEndDate:", row.get("umaEndDate"))
        print("endDate:", row.get("endDate"))
        print("category:", row.get("category"))
        print("outcomes:", row.get("outcomes"))
        print("outcomePrices:", row.get("outcomePrices"))
        print("clobTokenIds:", row.get("clobTokenIds"))
        print("volumeNum:", row.get("volumeNum"))


def inspect_kalshi(rows: list[dict]) -> None:
    print("\n" + "=" * 80)
    print("KALSHI")
    print("Rows:", len(rows))

    coverage = field_coverage(rows)

    print("\nTop fields by non-empty count:")
    for field, count in coverage.most_common(30):
        print(f"{field}: {count}")

    print("\nImportant field coverage:")
    important = [
        "ticker",
        "event_ticker",
        "title",
        "market_type",
        "status",
        "result",
        "settlement_ts",
        "settlement_value_dollars",
        "close_time",
        "expiration_time",
        "yes_bid_dollars",
        "yes_ask_dollars",
        "last_price_dollars",
        "volume_fp",
        "liquidity_dollars",
    ]

    for field in important:
        print(f"{field}: {coverage.get(field, 0)} / {len(rows)}")

    print("\nFirst 5 market previews:")
    for row in rows[:5]:
        print("-" * 80)
        print("ticker:", row.get("ticker"))
        print("event_ticker:", row.get("event_ticker"))
        print("title:", row.get("title"))
        print("market_type:", row.get("market_type"))
        print("status:", row.get("status"))
        print("result:", row.get("result"))
        print("settlement_ts:", row.get("settlement_ts"))
        print("settlement_value_dollars:", row.get("settlement_value_dollars"))
        print("last_price_dollars:", row.get("last_price_dollars"))
        print("volume_fp:", row.get("volume_fp"))
        print("liquidity_dollars:", row.get("liquidity_dollars"))


def main() -> None:
    polymarket_rows = read_jsonl(POLYMARKET_PATH)
    kalshi_rows = read_jsonl(KALSHI_PATH)

    inspect_polymarket(polymarket_rows)
    inspect_kalshi(kalshi_rows)

    print("\n" + "=" * 80)
    print("Inspection complete.")
    print("Next step: build cleaning script after checking field coverage.")


if __name__ == "__main__":
    main()