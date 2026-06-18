"""
01_pull_raw_api_data.py

Purpose:
    Pull real raw market metadata from Polymarket and Kalshi APIs.

Important:
    This script does NOT use local/sample data.
    This script does NOT clean data.
    This script does NOT compute Brier scores.
    This script only calls APIs and saves raw JSON/JSONL files.

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


# =============================================================================
# Config
# =============================================================================

WINDOW_DAYS = 180

# Polymarket Gamma API appears to reject too-large offset pagination.
# Earlier, offset=2500 caused a 422. So we use limit=100 and max_pages=25,
# meaning max offset is 2400.
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


# =============================================================================
# Helpers
# =============================================================================

def save_json(path: Path, data: Any) -> None:
    """Save a Python object as pretty JSON."""
    path.write_text(json.dumps(data, indent=2), encoding="utf-8")
    print(f"Saved JSON: {path}")


def save_jsonl(path: Path, rows: list[dict]) -> None:
    """Save a list of dicts as JSONL, one JSON object per line."""
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(json.dumps(row) + "\n")

    print(f"Saved JSONL: {path} ({len(rows)} rows)")


def parse_time(value: str | None) -> datetime | None:
    """
    Parse timestamp strings from Polymarket/Kalshi.

    Handles formats like:
        2026-03-19T23:20:15Z
        2026-03-19 23:20:15+00
        2028-01-01
    """
    if not value:
        return None

    value = str(value).strip()

    # Date only, like "2028-01-01"
    if len(value) == 10:
        value = value + "T00:00:00+00:00"

    # Convert Zulu time to Python ISO offset
    value = value.replace("Z", "+00:00")

    # Polymarket sometimes has "+00" instead of "+00:00"
    if value.endswith("+00"):
        value = value + ":00"

    try:
        dt = datetime.fromisoformat(value)
    except Exception:
        return None

    # Make timezone-naive timestamps explicitly UTC
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def get_polymarket_resolution_time(market: dict) -> datetime | None:
    """
    Best estimate of Polymarket actual resolution/close time.

    Important:
        For Polymarket, endDate can be the original deadline in the rules.
        Some markets resolve early, so closedTime / umaEndDate is better.
    """
    return (
        parse_time(market.get("closedTime"))
        or parse_time(market.get("umaEndDate"))
        or parse_time(market.get("endDateIso"))
        or parse_time(market.get("endDate"))
    )


def get_kalshi_resolution_time(market: dict) -> datetime | None:
    """
    Best estimate of Kalshi actual settlement time.
    """
    return (
        parse_time(market.get("settlement_ts"))
        or parse_time(market.get("close_time"))
        or parse_time(market.get("expiration_time"))
    )


# =============================================================================
# Polymarket
# =============================================================================

def pull_polymarket_closed_markets(
    window_days: int = WINDOW_DAYS,
    limit: int = POLYMARKET_LIMIT,
    max_pages: int = POLYMARKET_MAX_PAGES,
) -> list[dict]:
    """
    Pull raw closed Polymarket markets from Gamma API.

    This function:
        - makes real API calls
        - saves raw API pages
        - saves all pulled markets
        - saves recent closed markets

    It does NOT:
        - clean markets
        - calculate p_hat
        - calculate Brier score
    """
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
            # From your curl test, this returns newer-looking closed markets.
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


# =============================================================================
# Kalshi
# =============================================================================

def pull_kalshi_settled_markets(
    window_days: int = WINDOW_DAYS,
    limit: int = KALSHI_LIMIT,
    max_pages: int = KALSHI_MAX_PAGES,
) -> list[dict]:
    """
    Pull raw settled/finalized Kalshi markets.

    This function:
        - makes real API calls
        - saves raw API pages
        - saves all pulled markets
        - saves recent settled markets

    It does NOT:
        - clean markets
        - calculate p_hat
        - calculate Brier score
    """
    print("\n" + "=" * 80)
    print("Pulling Kalshi settled markets")

    base_url = "https://external-api.kalshi.com/trade-api/v2/markets"
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)

    all_raw_pages: list[dict] = []
    all_pulled_markets: list[dict] = []
    recent_settled_markets: list[dict] = []

    cursor = None

    for page in range(max_pages):
        params = {
            "status": "settled",
            "limit": limit,
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
            print("Kalshi returned 422. Stopping Kalshi pagination gracefully.")
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


# =============================================================================
# Main
# =============================================================================

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