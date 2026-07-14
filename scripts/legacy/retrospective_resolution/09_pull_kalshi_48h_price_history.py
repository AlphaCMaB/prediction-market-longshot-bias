"""
09_pull_kalshi_48h_price_history.py

Purpose:
    Pull Kalshi candlestick price history only for markets that were already
    filtered as open at least 48 hours before settlement.

Input:
    data/processed/kalshi_48h_candidates.csv

Outputs:
    data/raw/price_history/kalshi/*.json
    outputs/kalshi_48h_price_history_manifest.csv

Important:
    This script does NOT calculate p_hat.
    This script does NOT calculate Brier score.
    It only calls the Kalshi candlestick API and saves raw responses.
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


CANDIDATES_PATH = Path("data/processed/kalshi_48h_candidates.csv")

KALSHI_PRICE_DIR = Path("data/raw/price_history/kalshi")
OUTPUTS_DIR = Path("outputs")

KALSHI_PRICE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

MANIFEST_PATH = OUTPUTS_DIR / "kalshi_48h_price_history_manifest.csv"

SNAPSHOT_HOURS_BEFORE_RESOLUTION = 48
PRICE_HISTORY_WINDOW_HOURS = 12

# 0 means no artificial limit.
MAX_MARKETS = 0

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


def read_candidates(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing candidate file: {path}")

    with path.open("r", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    if MAX_MARKETS and MAX_MARKETS > 0:
        rows = rows[:MAX_MARKETS]

    return rows


def save_json(path: Path, data: Any) -> None:
    path.write_text(json.dumps(data, indent=2, ensure_ascii=False), encoding="utf-8")


def count_history_points(data: Any) -> int:
    if isinstance(data, dict):
        value = data.get("candlesticks")
        if isinstance(value, list):
            return len(value)

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


def pull_one(row: dict) -> dict:
    market_id = row["market_id"]
    ticker = row["ticker"]
    series_ticker = row["kalshi_series_ticker"]
    resolution_time = parse_time(row["resolution_time"])

    if not resolution_time:
        return {
            "venue": "kalshi",
            "market_id": market_id,
            "ticker": ticker,
            "status": "skipped",
            "reason": "missing_resolution_time",
        }

    if not series_ticker or not ticker:
        return {
            "venue": "kalshi",
            "market_id": market_id,
            "ticker": ticker,
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
        "period_interval": 60,
    }

    print("\nKalshi 48h candidate price-history call")
    print("market_id:", market_id)
    print("series_ticker:", series_ticker)
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
        "status": "",
        "reason": "",
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
    print("Pull Kalshi 48h candidate price history")
    print("No p_hat calculation. No Brier score.")

    candidates = read_candidates(CANDIDATES_PATH)

    print("Candidate rows:", len(candidates))

    manifest_rows = []

    for i, row in enumerate(candidates, start=1):
        print("\n" + "=" * 80)
        print(f"Candidate {i} / {len(candidates)}")

        try:
            manifest_rows.append(pull_one(row))
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

    nonzero = sum(int(row.get("history_points") or 0) > 0 for row in manifest_rows)
    zero = sum(int(row.get("history_points") or 0) == 0 for row in manifest_rows if row.get("status_code"))

    print("\n" + "=" * 80)
    print("Kalshi 48h price-history pull complete")
    print("Rows:", len(manifest_rows))
    print("Nonzero history:", nonzero)
    print("Zero history:", zero)
    print("")
    print("Next:")
    print("  python scripts/legacy/retrospective_resolution/05_extract_p_hat_batch.py")
    print("  cat outputs/p_hat_batch_report.md")


if __name__ == "__main__":
    main()
