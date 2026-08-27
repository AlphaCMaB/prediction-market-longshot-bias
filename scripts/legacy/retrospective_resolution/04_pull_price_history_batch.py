"""
04_pull_price_history_smoke_test.py

Purpose:
    Pull raw price-history data for a small sample of cleaned Polymarket and
    Kalshi markets.

Important:
    This script does NOT use local/sample data.
    This script does NOT calculate p_hat yet.
    This script does NOT calculate Brier scores.
    This script only calls price-history APIs and saves raw responses.

Inputs:
    data/processed/markets_metadata_clean.csv

Outputs:
    data/raw/price_history/polymarket/*.json
    data/raw/price_history/kalshi/*.json
    outputs/price_history_smoke_test_manifest.csv
"""

from __future__ import annotations

import csv
import json
import re
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


CLEAN_METADATA_PATH = Path("data/processed/markets_metadata_clean.csv")

POLYMARKET_PRICE_DIR = Path("data/raw/price_history/polymarket")
KALSHI_PRICE_DIR = Path("data/raw/price_history/kalshi")
OUTPUTS_DIR = Path("outputs")

POLYMARKET_PRICE_DIR.mkdir(parents=True, exist_ok=True)
KALSHI_PRICE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

#MANIFEST_PATH = OUTPUTS_DIR / "price_history_smoke_test_manifest.csv"
MANIFEST_PATH = OUTPUTS_DIR / "price_history_batch_manifest.csv"
# Start small.
MAX_POLYMARKET_MARKETS = 500
MAX_KALSHI_MARKETS = 500

SNAPSHOT_HOURS_BEFORE_RESOLUTION = 48

# We query a window around the target time.
# Later we will pick the nearest pre-resolution price from this history.
PRICE_HISTORY_WINDOW_HOURS = 12

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


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


def safe_filename(value: str) -> str:
    value = str(value)
    value = re.sub(r"[^A-Za-z0-9_.-]+", "_", value)
    return value[:180]


def read_clean_metadata(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing clean metadata file: {path}")

    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def count_history_points(data: Any) -> int:
    """
    Best-effort counter for common price-history response shapes.
    """
    if isinstance(data, dict):
        for key in ["history", "prices", "candlesticks", "candles", "data"]:
            value = data.get(key)
            if isinstance(value, list):
                return len(value)

    if isinstance(data, list):
        return len(data)

    return 0


def request_json(url: str, params: dict) -> tuple[int, str, Any]:
    response = requests.get(
        url,
        params=params,
        headers=HEADERS,
        timeout=30,
    )

    status_code = response.status_code
    final_url = response.url

    try:
        data = response.json()
    except Exception:
        data = {
            "raw_text_preview": response.text[:1000]
        }

    return status_code, final_url, data


def pull_polymarket_price_history(row: dict) -> dict:
    """
    Polymarket price-history endpoint uses the CLOB token ID.

    We use yes_token_id because our p_hat will eventually mean:
        probability that YES resolves true
    """
    market_id = row["market_id"]
    yes_token_id = row["yes_token_id"]
    resolution_time = parse_time(row["resolution_time"])

    if not resolution_time:
        return {
            "venue": "polymarket",
            "market_id": market_id,
            "status": "skipped",
            "reason": "missing_resolution_time",
        }

    target_time = resolution_time - timedelta(hours=SNAPSHOT_HOURS_BEFORE_RESOLUTION)
    start_time = target_time - timedelta(hours=PRICE_HISTORY_WINDOW_HOURS)
    end_time = target_time + timedelta(hours=PRICE_HISTORY_WINDOW_HOURS)

    url = "https://clob.polymarket.com/prices-history"

    params = {
        "market": yes_token_id,
        "startTs": int(start_time.timestamp()),
        "endTs": int(end_time.timestamp()),
        # 60-minute resolution. Good enough for first smoke test.
        "fidelity": 60,
    }

    print("\nPolymarket price-history call")
    print("market_id:", market_id)
    print("target_time:", target_time.isoformat())
    print("yes_token_id:", yes_token_id[:30] + "...")

    status_code, final_url, data = request_json(url, params)

    output_path = POLYMARKET_PRICE_DIR / f"{safe_filename(market_id)}.json"
    save_json(
        output_path,
        {
            "metadata": row,
            "target_time": target_time.isoformat(),
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "request_url": final_url,
            "status_code": status_code,
            "response": data,
        },
    )

    history_points = count_history_points(data)

    print("status:", status_code)
    print("history points:", history_points)
    print("saved:", output_path)

    return {
        "venue": "polymarket",
        "market_id": market_id,
        "ticker": row.get("ticker", ""),
        "status_code": status_code,
        "history_points": history_points,
        "target_time": target_time.isoformat(),
        "output_path": str(output_path),
        "request_url": final_url,
    }


def pull_kalshi_price_history(row: dict) -> dict:
    """
    Kalshi candlestick endpoint uses:
        series_ticker
        market ticker
    """
    market_id = row["market_id"]
    ticker = row["ticker"]
    series_ticker = row["kalshi_series_ticker"]
    resolution_time = parse_time(row["resolution_time"])

    if not resolution_time:
        return {
            "venue": "kalshi",
            "market_id": market_id,
            "status": "skipped",
            "reason": "missing_resolution_time",
        }

    if not series_ticker or not ticker:
        return {
            "venue": "kalshi",
            "market_id": market_id,
            "status": "skipped",
            "reason": "missing_series_or_ticker",
        }

    target_time = resolution_time - timedelta(hours=SNAPSHOT_HOURS_BEFORE_RESOLUTION)
    start_time = target_time - timedelta(hours=PRICE_HISTORY_WINDOW_HOURS)
    end_time = target_time + timedelta(hours=PRICE_HISTORY_WINDOW_HOURS)

    url = (
        "https://external-api.kalshi.com/trade-api/v2/"
        f"series/{series_ticker}/markets/{ticker}/candlesticks"
    )

    params = {
        "start_ts": int(start_time.timestamp()),
        "end_ts": int(end_time.timestamp()),
        # 60 means hourly candles.
        "period_interval": 60,
    }

    print("\nKalshi candlestick call")
    print("market_id:", market_id)
    print("series_ticker:", series_ticker)
    print("ticker:", ticker)
    print("target_time:", target_time.isoformat())

    status_code, final_url, data = request_json(url, params)

    output_path = KALSHI_PRICE_DIR / f"{safe_filename(market_id)}.json"
    save_json(
        output_path,
        {
            "metadata": row,
            "target_time": target_time.isoformat(),
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "request_url": final_url,
            "status_code": status_code,
            "response": data,
        },
    )

    history_points = count_history_points(data)

    print("status:", status_code)
    print("history points:", history_points)
    print("saved:", output_path)

    return {
        "venue": "kalshi",
        "market_id": market_id,
        "ticker": ticker,
        "status_code": status_code,
        "history_points": history_points,
        "target_time": target_time.isoformat(),
        "output_path": str(output_path),
        "request_url": final_url,
    }


def write_manifest(rows: list[dict]) -> None:
    fieldnames = [
        "venue",
        "market_id",
        "ticker",
        "status_code",
        "history_points",
        "target_time",
        "output_path",
        "request_url",
        "status",
        "reason",
    ]

    with MANIFEST_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()

        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})

    print(f"\nSaved manifest: {MANIFEST_PATH}")


def main() -> None:
    print("=" * 80)
    print("Price-history smoke test")
    print("No local data. No p_hat calculation. No Brier score.")

    metadata_rows = read_clean_metadata(CLEAN_METADATA_PATH)

    polymarket_rows = [
        row for row in metadata_rows
        if row["venue"] == "polymarket" and row.get("yes_token_id")
    ]

    kalshi_rows = [
        row for row in metadata_rows
        if row["venue"] == "kalshi"
        and row.get("kalshi_series_ticker")
        and row.get("ticker")
    ]

    print("Available Polymarket rows:", len(polymarket_rows))
    print("Available Kalshi rows:", len(kalshi_rows))

    polymarket_sample = polymarket_rows[:MAX_POLYMARKET_MARKETS]
    kalshi_sample = kalshi_rows[:MAX_KALSHI_MARKETS]

    manifest_rows = []

    print("\n" + "=" * 80)
    print(f"Pulling Polymarket price history for {len(polymarket_sample)} markets")

    for row in polymarket_sample:
        try:
            manifest_rows.append(pull_polymarket_price_history(row))
        except Exception as exc:
            print("ERROR:", exc)
            manifest_rows.append(
                {
                    "venue": "polymarket",
                    "market_id": row.get("market_id", ""),
                    "ticker": row.get("ticker", ""),
                    "status": "error",
                    "reason": repr(exc),
                }
            )

        time.sleep(0.25)

    print("\n" + "=" * 80)
    print(f"Pulling Kalshi price history for {len(kalshi_sample)} markets")

    for row in kalshi_sample:
        try:
            manifest_rows.append(pull_kalshi_price_history(row))
        except Exception as exc:
            print("ERROR:", exc)
            manifest_rows.append(
                {
                    "venue": "kalshi",
                    "market_id": row.get("market_id", ""),
                    "ticker": row.get("ticker", ""),
                    "status": "error",
                    "reason": repr(exc),
                }
            )

        time.sleep(0.25)

    write_manifest(manifest_rows)

    print("\n" + "=" * 80)
    print("Smoke test complete.")
    print("Check:")
    print(f"  {MANIFEST_PATH}")
    print("  data/raw/price_history/polymarket/")
    print("  data/raw/price_history/kalshi/")
    print("")
    print("Next step after this works:")
    print("  inspect the raw price-history response shapes.")
    print("  Then extract the 48-hour pre-resolution p_hat.")
    print("  Still do not compute Brier score yet.")


if __name__ == "__main__":
    main()
