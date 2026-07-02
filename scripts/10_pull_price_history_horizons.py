"""
10_pull_price_history_horizons.py

Purpose:
    Pull raw price-history data for multiple forecast horizons:
        - 24 hours before resolution
        - 48 hours before resolution
        - 168 hours / 7 days before resolution

Why:
    The current pilot result uses only the 48-hour horizon.
    This script lets us test whether calibration / favorite-longshot patterns
    are robust across different forecast horizons.

Inputs:
    data/processed/markets_metadata_clean.csv
    data/processed/kalshi_48h_candidates.csv

Outputs:
    data/raw/price_history_horizons/polymarket/<horizon>h/*.json
    data/raw/price_history_horizons/kalshi/<horizon>h/*.json
    outputs/horizon_price_history_manifest.csv

Important:
    This script only calls APIs and saves raw price-history responses.
    It does NOT extract p_hat.
    It does NOT calculate Brier scores.
"""

from __future__ import annotations

import csv
import json
import re
import time
from collections import Counter
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


# =============================================================================
# Config
# =============================================================================

MARKETS_METADATA_PATH = Path("data/processed/markets_metadata_clean.csv")
KALSHI_CANDIDATES_PATH = Path("data/processed/kalshi_48h_candidates.csv")

RAW_HORIZON_DIR = Path("data/raw/price_history_horizons")
OUTPUTS_DIR = Path("outputs")

OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
RAW_HORIZON_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_PATH = OUTPUTS_DIR / "horizon_price_history_manifest.csv"

HORIZONS_HOURS = [24, 48, 168]

# Query a 24-hour window around each horizon target time.
# Example for 48h horizon:
#   target = resolution_time - 48h
#   query  = target +/- 12h
PRICE_HISTORY_WINDOW_HOURS = 12

# Keep this moderate for now. Set to 0 to use all available rows.
MAX_POLYMARKET_MARKETS = 500
MAX_KALSHI_MARKETS = 500

# Reuse saved JSON files if the script is rerun.
REUSE_EXISTING = True

HEADERS = {
    "User-Agent": "Mozilla/5.0"
}


# =============================================================================
# Helpers
# =============================================================================

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


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")

    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def save_json(path: Path, data: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def read_json(path: Path) -> Any:
    return json.loads(path.read_text(encoding="utf-8"))


def count_history_points(data: Any) -> int:
    if isinstance(data, dict):
        for key in ["history", "candlesticks", "prices", "data"]:
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


def write_manifest(rows: list[dict]) -> None:
    fieldnames = [
        "venue",
        "horizon_hours",
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


# =============================================================================
# Input selection
# =============================================================================

def load_polymarket_rows() -> list[dict]:
    rows = read_csv(MARKETS_METADATA_PATH)

    rows = [
        row for row in rows
        if row.get("venue") == "polymarket"
        and row.get("yes_token_id")
        and row.get("resolution_time")
    ]

    if MAX_POLYMARKET_MARKETS and MAX_POLYMARKET_MARKETS > 0:
        rows = rows[:MAX_POLYMARKET_MARKETS]

    return rows


def load_kalshi_rows() -> list[dict]:
    # Prefer the filtered candidate file because these markets were already open
    # at least 48 hours before settlement.
    if KALSHI_CANDIDATES_PATH.exists():
        rows = read_csv(KALSHI_CANDIDATES_PATH)
    else:
        rows = read_csv(MARKETS_METADATA_PATH)
        rows = [row for row in rows if row.get("venue") == "kalshi"]

    rows = [
        row for row in rows
        if row.get("ticker")
        and row.get("kalshi_series_ticker")
        and row.get("resolution_time")
    ]

    if MAX_KALSHI_MARKETS and MAX_KALSHI_MARKETS > 0:
        rows = rows[:MAX_KALSHI_MARKETS]

    return rows


# =============================================================================
# API calls
# =============================================================================

def output_path_for(row: dict, venue: str, horizon_hours: int) -> Path:
    market_id = row.get("market_id") or row.get("ticker")
    return (
        RAW_HORIZON_DIR
        / venue
        / f"{horizon_hours}h"
        / f"{safe_filename(market_id)}.json"
    )


def make_common_times(row: dict, horizon_hours: int) -> tuple[datetime | None, datetime | None, datetime | None, datetime | None]:
    resolution_time = parse_time(row.get("resolution_time"))

    if resolution_time is None:
        return None, None, None, None

    target_time = resolution_time - timedelta(hours=horizon_hours)
    start_time = target_time - timedelta(hours=PRICE_HISTORY_WINDOW_HOURS)
    end_time = target_time + timedelta(hours=PRICE_HISTORY_WINDOW_HOURS)

    return resolution_time, target_time, start_time, end_time


def pull_polymarket_one(row: dict, horizon_hours: int) -> dict:
    market_id = row["market_id"]
    ticker = row.get("ticker", "")
    yes_token_id = row.get("yes_token_id", "")

    resolution_time, target_time, start_time, end_time = make_common_times(row, horizon_hours)

    if resolution_time is None or target_time is None or start_time is None or end_time is None:
        return {
            "venue": "polymarket",
            "horizon_hours": horizon_hours,
            "market_id": market_id,
            "ticker": ticker,
            "status": "skipped",
            "reason": "missing_resolution_time",
        }

    if not yes_token_id:
        return {
            "venue": "polymarket",
            "horizon_hours": horizon_hours,
            "market_id": market_id,
            "ticker": ticker,
            "status": "skipped",
            "reason": "missing_yes_token_id",
        }

    output_path = output_path_for(row, "polymarket", horizon_hours)

    if REUSE_EXISTING and output_path.exists():
        wrapper = read_json(output_path)
        status_code = wrapper.get("status_code", "")
        data = wrapper.get("response", {})
        final_url = wrapper.get("request_url", "")
        history_points = count_history_points(data)

        return {
            "venue": "polymarket",
            "horizon_hours": horizon_hours,
            "market_id": market_id,
            "ticker": ticker,
            "status_code": status_code,
            "history_points": history_points,
            "target_time": target_time.isoformat(),
            "output_path": str(output_path),
            "request_url": final_url,
            "status": "reused",
            "reason": "",
        }

    url = "https://clob.polymarket.com/prices-history"

    params = {
        "market": yes_token_id,
        "startTs": int(start_time.timestamp()),
        "endTs": int(end_time.timestamp()),
        "fidelity": 60,
    }

    status_code, final_url, data = request_json(url, params)

    save_json(
        output_path,
        {
            "metadata": row,
            "horizon_hours": horizon_hours,
            "target_time": target_time.isoformat(),
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "request_url": final_url,
            "status_code": status_code,
            "response": data,
        },
    )

    history_points = count_history_points(data)

    return {
        "venue": "polymarket",
        "horizon_hours": horizon_hours,
        "market_id": market_id,
        "ticker": ticker,
        "status_code": status_code,
        "history_points": history_points,
        "target_time": target_time.isoformat(),
        "output_path": str(output_path),
        "request_url": final_url,
        "status": "",
        "reason": "",
    }


def pull_kalshi_one(row: dict, horizon_hours: int) -> dict:
    market_id = row["market_id"]
    ticker = row["ticker"]
    series_ticker = row["kalshi_series_ticker"]

    resolution_time, target_time, start_time, end_time = make_common_times(row, horizon_hours)

    if resolution_time is None or target_time is None or start_time is None or end_time is None:
        return {
            "venue": "kalshi",
            "horizon_hours": horizon_hours,
            "market_id": market_id,
            "ticker": ticker,
            "status": "skipped",
            "reason": "missing_resolution_time",
        }

    if not series_ticker or not ticker:
        return {
            "venue": "kalshi",
            "horizon_hours": horizon_hours,
            "market_id": market_id,
            "ticker": ticker,
            "status": "skipped",
            "reason": "missing_series_or_ticker",
        }

    output_path = output_path_for(row, "kalshi", horizon_hours)

    if REUSE_EXISTING and output_path.exists():
        wrapper = read_json(output_path)
        status_code = wrapper.get("status_code", "")
        data = wrapper.get("response", {})
        final_url = wrapper.get("request_url", "")
        history_points = count_history_points(data)

        return {
            "venue": "kalshi",
            "horizon_hours": horizon_hours,
            "market_id": market_id,
            "ticker": ticker,
            "status_code": status_code,
            "history_points": history_points,
            "target_time": target_time.isoformat(),
            "output_path": str(output_path),
            "request_url": final_url,
            "status": "reused",
            "reason": "",
        }

    url = (
        "https://external-api.kalshi.com/trade-api/v2/"
        f"series/{series_ticker}/markets/{ticker}/candlesticks"
    )

    params = {
        "start_ts": int(start_time.timestamp()),
        "end_ts": int(end_time.timestamp()),
        "period_interval": 60,
    }

    status_code, final_url, data = request_json(url, params)

    save_json(
        output_path,
        {
            "metadata": row,
            "horizon_hours": horizon_hours,
            "target_time": target_time.isoformat(),
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "request_url": final_url,
            "status_code": status_code,
            "response": data,
        },
    )

    history_points = count_history_points(data)

    return {
        "venue": "kalshi",
        "horizon_hours": horizon_hours,
        "market_id": market_id,
        "ticker": ticker,
        "status_code": status_code,
        "history_points": history_points,
        "target_time": target_time.isoformat(),
        "output_path": str(output_path),
        "request_url": final_url,
        "status": "",
        "reason": "",
    }


# =============================================================================
# Main
# =============================================================================

def print_progress_summary(manifest_rows: list[dict]) -> None:
    counter = Counter(
        (
            row.get("venue"),
            row.get("horizon_hours"),
            "nonzero" if int(row.get("history_points") or 0) > 0 else "zero",
        )
        for row in manifest_rows
        if row.get("status_code")
    )

    print("\nCurrent summary:")
    for key, count in sorted(counter.items()):
        venue, horizon, bucket = key
        print(f"  {venue} {horizon}h {bucket}: {count}")


def main() -> None:
    print("=" * 80)
    print("Pull price history for multiple horizons")
    print("No p_hat extraction. No Brier score.")

    polymarket_rows = load_polymarket_rows()
    kalshi_rows = load_kalshi_rows()

    print("Polymarket rows:", len(polymarket_rows))
    print("Kalshi rows:", len(kalshi_rows))
    print("Horizons:", HORIZONS_HOURS)

    manifest_rows = []

    total_calls = (len(polymarket_rows) + len(kalshi_rows)) * len(HORIZONS_HOURS)
    call_index = 0

    for horizon_hours in HORIZONS_HOURS:
        print("\n" + "=" * 80)
        print(f"Horizon: {horizon_hours}h before resolution")

        for row in polymarket_rows:
            call_index += 1
            print(f"\n[{call_index}/{total_calls}] Polymarket {row.get('market_id')} horizon={horizon_hours}h")

            try:
                manifest_rows.append(pull_polymarket_one(row, horizon_hours))
            except Exception as exc:
                print("ERROR:", exc)
                manifest_rows.append(
                    {
                        "venue": "polymarket",
                        "horizon_hours": horizon_hours,
                        "market_id": row.get("market_id", ""),
                        "ticker": row.get("ticker", ""),
                        "status": "error",
                        "reason": repr(exc),
                    }
                )

            time.sleep(0.15)

        for row in kalshi_rows:
            call_index += 1
            print(f"\n[{call_index}/{total_calls}] Kalshi {row.get('market_id')} horizon={horizon_hours}h")

            try:
                manifest_rows.append(pull_kalshi_one(row, horizon_hours))
            except Exception as exc:
                print("ERROR:", exc)
                manifest_rows.append(
                    {
                        "venue": "kalshi",
                        "horizon_hours": horizon_hours,
                        "market_id": row.get("market_id", ""),
                        "ticker": row.get("ticker", ""),
                        "status": "error",
                        "reason": repr(exc),
                    }
                )

            time.sleep(0.15)

        write_manifest(manifest_rows)
        print_progress_summary(manifest_rows)

    print("\n" + "=" * 80)
    print("Horizon price-history pull complete.")
    print(f"Manifest: {MANIFEST_PATH}")


if __name__ == "__main__":
    main()
