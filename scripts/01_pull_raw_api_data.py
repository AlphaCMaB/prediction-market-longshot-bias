"""
01_pull_raw_api_data.py

Pull real raw market metadata from Polymarket and Kalshi APIs.

This script:
    - uses real API calls
    - saves raw JSON/JSONL files
    - does NOT use local/sample data
    - does NOT clean data
    - does NOT calculate Brier scores

Outputs:
    data/raw/polymarket/polymarket_raw_pages.json
    data/raw/polymarket/polymarket_all_pulled_markets.jsonl
    data/raw/polymarket/polymarket_recent_closed_markets.jsonl

    data/raw/kalshi/kalshi_raw_pages.json
    data/raw/kalshi/kalshi_all_pulled_markets.jsonl
    data/raw/kalshi/kalshi_recent_settled_markets.jsonl
"""

from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


WINDOW_DAYS = 180

POLYMARKET_LIMIT = 100
POLYMARKET_MAX_PAGES = 25

KALSHI_LIMIT = 100
KALSHI_MAX_PAGES = 50

RAW_DIR = Path("data/raw")
POLYMARKET_RAW_DIR = RAW_DIR / "polymarket"
KALSHI_RAW_DIR = RAW_DIR / "kalshi"

POLYMARKET_RAW_DIR.mkdir(parents=True, exist_ok=True)
KALSHI_RAW_DIR.mkdir(parents=True, exist_ok=True)

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


def save_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Saved JSON: {path}")


def save_jsonl(path: Path, rows: list[dict]) -> None:
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    print(f"Saved JSONL: {path} ({len(rows)} rows)")


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None

    value = str(value).strip()

    # Example: "2028-01-01"
    if len(value) == 10:
        value = value + "T00:00:00+00:00"

    # Example: "2026-03-19T23:20:15Z"
    value = value.replace("Z", "+00:00")

    # Example: "2026-03-19 23:20:15+00"
    if value.endswith("+00"):
        value = value + ":00"

    try:
        dt = datetime.fromisoformat(value)
    except Exception:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def get_polymarket_resolution_time(market: dict) -> datetime | None:
    """
    For Polymarket, endDate can be the original rule deadline.
    closedTime / umaEndDate is usually closer to actual resolution time.
    """
    return (
        parse_time(market.get("closedTime"))
        or parse_time(market.get("umaEndDate"))
        or parse_time(market.get("endDateIso"))
        or parse_time(market.get("endDate"))
    )


def get_kalshi_resolution_time(market: dict) -> datetime | None:
    return (
        parse_time(market.get("settlement_ts"))
        or parse_time(market.get("close_time"))
        or parse_time(market.get("expiration_time"))
    )


def dedupe_by_key(rows: list[dict], key: str) -> list[dict]:
    seen = set()
    deduped = []

    for row in rows:
        value = row.get(key)

        if value is None:
            deduped.append(row)
            continue

        if value in seen:
            continue

        seen.add(value)
        deduped.append(row)

    return deduped


def pull_polymarket_closed_markets(
    window_days: int = WINDOW_DAYS,
    limit: int = POLYMARKET_LIMIT,
    max_pages: int = POLYMARKET_MAX_PAGES,
) -> list[dict]:
    print("\n" + "=" * 80)
    print("Pulling Polymarket closed markets")

    base_url = "https://gamma-api.polymarket.com/markets"
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)

    all_raw_pages: list[dict] = []
    all_pulled_markets: list[dict] = []
    recent_closed_markets: list[dict] = []

    for page in range(max_pages):
        offset = page * limit

        params = {
            "closed": "true",
            "limit": limit,
            "offset": offset,
            "order": "endDate",
            "ascending": "false",
        }

        print("\n" + "-" * 80)
        print(f"Polymarket page {page + 1}")
        print(f"Offset: {offset}")

        response = requests.get(
            base_url,
            params=params,
            headers=HEADERS,
            timeout=30,
        )

        print("Status:", response.status_code)
        print("URL:", response.url)

        if response.status_code == 422:
            print("Polymarket returned 422. Stopping Polymarket pagination gracefully.")
            print("Response preview:")
            print(response.text[:500])
            break

        response.raise_for_status()
        data = response.json()

        if not isinstance(data, list):
            print("Unexpected Polymarket response shape:", type(data))
            print("Response preview:")
            print(str(data)[:500])
            break

        if not data:
            print("No more Polymarket markets returned.")
            break

        all_raw_pages.append(
            {
                "page": page + 1,
                "offset": offset,
                "url": response.url,
                "data": data,
            }
        )

        all_pulled_markets.extend(data)

        page_recent_count = 0
        parsed_times: list[datetime] = []

        for market in data:
            resolution_time = get_polymarket_resolution_time(market)

            if resolution_time:
                parsed_times.append(resolution_time)

            if resolution_time and resolution_time >= cutoff:
                recent_closed_markets.append(market)
                page_recent_count += 1

        print(f"Markets returned on page: {len(data)}")
        print(f"Recent markets on page: {page_recent_count}")

        if parsed_times:
            print("First parsed resolution time:", parsed_times[0].isoformat())
            print("Last parsed resolution time:", parsed_times[-1].isoformat())

        time.sleep(0.25)

    all_pulled_markets = dedupe_by_key(all_pulled_markets, "id")
    recent_closed_markets = dedupe_by_key(recent_closed_markets, "id")

    save_json(
        POLYMARKET_RAW_DIR / "polymarket_raw_pages.json",
        all_raw_pages,
    )

    save_jsonl(
        POLYMARKET_RAW_DIR / "polymarket_all_pulled_markets.jsonl",
        all_pulled_markets,
    )

    save_jsonl(
        POLYMARKET_RAW_DIR / "polymarket_recent_closed_markets.jsonl",
        recent_closed_markets,
    )

    print("\nPolymarket pull summary")
    print("All pulled Polymarket markets:", len(all_pulled_markets))
    print("Recent closed Polymarket markets:", len(recent_closed_markets))

    return recent_closed_markets


def pull_kalshi_settled_markets(
    window_days: int = WINDOW_DAYS,
    limit: int = KALSHI_LIMIT,
    max_pages: int = KALSHI_MAX_PAGES,
) -> list[dict]:
    print("\n" + "=" * 80)
    print("Pulling Kalshi settled markets")

    base_url = "https://external-api.kalshi.com/trade-api/v2/markets"
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)

    now_ts = int(datetime.now(timezone.utc).timestamp())
    cutoff_ts = int(cutoff.timestamp())

    all_raw_pages: list[dict] = []
    all_pulled_markets: list[dict] = []
    recent_settled_markets: list[dict] = []

    cursor = None

    for page in range(max_pages):
        params = {
            "status": "settled",
            "limit": limit,
            "mve_filter": "exclude",
            "min_settled_ts": cutoff_ts,
            "max_settled_ts": now_ts,
        }

        if cursor:
            params["cursor"] = cursor

        print("\n" + "-" * 80)
        print(f"Kalshi page {page + 1}")

        response = requests.get(
            base_url,
            params=params,
            headers=HEADERS,
            timeout=30,
        )

        print("Status:", response.status_code)
        print("URL:", response.url)

        if response.status_code == 422:
            print("Kalshi returned 422 with timestamp filters.")
            print("Retrying this page without min_settled_ts / max_settled_ts...")

            fallback_params = {
                "status": "settled",
                "limit": limit,
                "mve_filter": "exclude",
            }

            if cursor:
                fallback_params["cursor"] = cursor

            response = requests.get(
                base_url,
                params=fallback_params,
                headers=HEADERS,
                timeout=30,
            )

            print("Fallback status:", response.status_code)
            print("Fallback URL:", response.url)

            if response.status_code == 422:
                print("Kalshi still returned 422. Stopping Kalshi pagination gracefully.")
                print("Response preview:")
                print(response.text[:500])
                break

        response.raise_for_status()
        data = response.json()

        if not isinstance(data, dict):
            print("Unexpected Kalshi response shape:", type(data))
            print("Response preview:")
            print(str(data)[:500])
            break

        markets = data.get("markets", [])

        if not markets:
            print("No more Kalshi markets returned.")
            break

        all_raw_pages.append(
            {
                "page": page + 1,
                "url": response.url,
                "data": data,
            }
        )

        all_pulled_markets.extend(markets)

        page_recent_count = 0
        parsed_times: list[datetime] = []

        for market in markets:
            resolution_time = get_kalshi_resolution_time(market)

            if resolution_time:
                parsed_times.append(resolution_time)

            if resolution_time and resolution_time >= cutoff:
                recent_settled_markets.append(market)
                page_recent_count += 1

        print(f"Markets returned on page: {len(markets)}")
        print(f"Recent markets on page: {page_recent_count}")

        if parsed_times:
            print("First parsed settlement time:", parsed_times[0].isoformat())
            print("Last parsed settlement time:", parsed_times[-1].isoformat())

        cursor = data.get("cursor")

        if not cursor:
            print("No Kalshi cursor returned. Stopping.")
            break

        time.sleep(0.25)

    all_pulled_markets = dedupe_by_key(all_pulled_markets, "ticker")
    recent_settled_markets = dedupe_by_key(recent_settled_markets, "ticker")

    save_json(
        KALSHI_RAW_DIR / "kalshi_raw_pages.json",
        all_raw_pages,
    )

    save_jsonl(
        KALSHI_RAW_DIR / "kalshi_all_pulled_markets.jsonl",
        all_pulled_markets,
    )

    save_jsonl(
        KALSHI_RAW_DIR / "kalshi_recent_settled_markets.jsonl",
        recent_settled_markets,
    )

    print("\nKalshi pull summary")
    print("All pulled Kalshi markets:", len(all_pulled_markets))
    print("Recent settled Kalshi markets:", len(recent_settled_markets))

    return recent_settled_markets


def main() -> None:
    print("=" * 80)
    print("Raw API pull script")
    print("No local data. No cleaning. No Brier score.")
    print(f"Window days: {WINDOW_DAYS}")

    polymarket_recent = pull_polymarket_closed_markets()
    kalshi_recent = pull_kalshi_settled_markets()

    print("\n" + "=" * 80)
    print("Raw API pull complete")
    print("Recent Polymarket closed markets:", len(polymarket_recent))
    print("Recent Kalshi settled markets:", len(kalshi_recent))

    print("\nFiles created:")
    print("  data/raw/polymarket/polymarket_raw_pages.json")
    print("  data/raw/polymarket/polymarket_all_pulled_markets.jsonl")
    print("  data/raw/polymarket/polymarket_recent_closed_markets.jsonl")
    print("  data/raw/kalshi/kalshi_raw_pages.json")
    print("  data/raw/kalshi/kalshi_all_pulled_markets.jsonl")
    print("  data/raw/kalshi/kalshi_recent_settled_markets.jsonl")

    print("\nNext step after this works:")
    print("  inspect raw data shape first, then build a cleaning script.")
    print("  Do NOT compute Brier score until raw data and price history are verified.")


if __name__ == "__main__":
    main()
