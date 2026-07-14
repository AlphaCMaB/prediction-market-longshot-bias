"""
05_extract_p_hat_smoke_test.py

Purpose:
    Extract p_hat from raw price-history smoke-test files.

Important:
    This script does NOT call APIs.
    This script does NOT use sample/local fake data.
    This script does NOT calculate Brier scores.

It only reads raw price-history API responses and extracts:

    p_hat = market-implied YES probability near 48 hours before resolution

Inputs:
    data/raw/price_history/polymarket/*.json
    data/raw/price_history/kalshi/*.json

Outputs:
    data/processed/p_hat_smoke_test.csv
    outputs/p_hat_smoke_test_report.md
"""

from __future__ import annotations

import csv
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


POLYMARKET_PRICE_DIR = Path("data/raw/price_history/polymarket")
KALSHI_PRICE_DIR = Path("data/raw/price_history/kalshi")

PROCESSED_DIR = Path("data/processed")
OUTPUTS_DIR = Path("outputs")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = PROCESSED_DIR / "p_hat_smoke_test.csv"
REPORT_PATH = OUTPUTS_DIR / "p_hat_smoke_test_report.md"


# =============================================================================
# Helpers
# =============================================================================

def read_json(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


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


def ts_to_dt(value: Any) -> datetime | None:
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc)
    except Exception:
        return None


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None

    try:
        number = float(value)
    except Exception:
        return None

    if math.isnan(number) or math.isinf(number):
        return None

    return number


def valid_probability(value: float | None) -> bool:
    return value is not None and 0.0 <= value <= 1.0


def hours_between(later: datetime, earlier: datetime) -> float:
    return (later - earlier).total_seconds() / 3600.0


def choose_nearest_candidate(
    candidates: list[dict],
    target_time: datetime,
    resolution_time: datetime,
) -> tuple[dict | None, str]:
    """
    Choose the price observation closest to the target time, but never after
    resolution.

    The target_time is usually:
        resolution_time - 48 hours

    We allow observations slightly before or after target_time, as long as they
    are still before resolution. We record target_error_hours so later we can
    filter strictly if needed.
    """
    valid = []

    for candidate in candidates:
        price_time = candidate.get("price_time")
        p_hat = candidate.get("p_hat")

        if price_time is None:
            continue

        if not valid_probability(p_hat):
            continue

        if price_time > resolution_time:
            continue

        target_error_hours = abs(hours_between(price_time, target_time))

        candidate = dict(candidate)
        candidate["target_error_hours"] = target_error_hours
        candidate["hours_before_resolution"] = hours_between(resolution_time, price_time)

        valid.append(candidate)

    if not valid:
        return None, "no_valid_pre_resolution_price"

    best = min(valid, key=lambda row: row["target_error_hours"])
    return best, ""


# =============================================================================
# Polymarket extraction
# =============================================================================

def extract_polymarket_file(path: Path) -> dict:
    wrapper = read_json(path)

    metadata = wrapper.get("metadata", {})
    response = wrapper.get("response", {})

    market_id = metadata.get("market_id", path.stem)
    resolution_time = parse_time(metadata.get("resolution_time"))
    target_time = parse_time(wrapper.get("target_time"))

    if resolution_time is None:
        return drop_row("polymarket", market_id, path, "missing_resolution_time", metadata)

    if target_time is None:
        return drop_row("polymarket", market_id, path, "missing_target_time", metadata)

    history = response.get("history", [])

    if not isinstance(history, list) or not history:
        return drop_row("polymarket", market_id, path, "empty_history", metadata)

    candidates = []

    for point in history:
        if not isinstance(point, dict):
            continue

        price_time = ts_to_dt(point.get("t"))
        p_hat = to_float(point.get("p"))

        candidates.append(
            {
                "price_time": price_time,
                "p_hat": p_hat,
                "price_source": "polymarket_history_p",
                "bid": "",
                "ask": "",
                "spread": "",
            }
        )

    best, reason = choose_nearest_candidate(
        candidates=candidates,
        target_time=target_time,
        resolution_time=resolution_time,
    )

    if best is None:
        return drop_row("polymarket", market_id, path, reason, metadata)

    return ok_row(
        venue="polymarket",
        metadata=metadata,
        raw_path=path,
        target_time=target_time,
        resolution_time=resolution_time,
        best=best,
    )


# =============================================================================
# Kalshi extraction
# =============================================================================

def get_nested_float(row: dict, outer_key: str, inner_key: str) -> float | None:
    value = row.get(outer_key)

    if not isinstance(value, dict):
        return None

    return to_float(value.get(inner_key))


def extract_kalshi_yes_probability(candle: dict) -> dict | None:
    """
    Kalshi candlesticks may contain bid/ask OHLC fields.

    For p_hat, use midpoint of yes_bid.close_dollars and yes_ask.close_dollars
    when both exist.

    p_hat = (YES bid close + YES ask close) / 2

    If bid/ask does not exist, try price.close_dollars as fallback.
    """
    price_time = ts_to_dt(candle.get("end_period_ts"))

    bid = get_nested_float(candle, "yes_bid", "close_dollars")
    ask = get_nested_float(candle, "yes_ask", "close_dollars")

    if valid_probability(bid) and valid_probability(ask):
        p_hat = (bid + ask) / 2.0
        spread = ask - bid

        return {
            "price_time": price_time,
            "p_hat": p_hat,
            "price_source": "kalshi_yes_bid_ask_mid_close",
            "bid": bid,
            "ask": ask,
            "spread": spread,
        }

    price = candle.get("price")

    if isinstance(price, dict):
        fallback_price = to_float(price.get("close_dollars"))

        if valid_probability(fallback_price):
            return {
                "price_time": price_time,
                "p_hat": fallback_price,
                "price_source": "kalshi_price_close_dollars",
                "bid": "",
                "ask": "",
                "spread": "",
            }

    return None


def extract_kalshi_file(path: Path) -> dict:
    wrapper = read_json(path)

    metadata = wrapper.get("metadata", {})
    response = wrapper.get("response", {})

    market_id = metadata.get("market_id", path.stem)
    resolution_time = parse_time(metadata.get("resolution_time"))
    target_time = parse_time(wrapper.get("target_time"))

    if resolution_time is None:
        return drop_row("kalshi", market_id, path, "missing_resolution_time", metadata)

    if target_time is None:
        return drop_row("kalshi", market_id, path, "missing_target_time", metadata)

    candlesticks = response.get("candlesticks", [])

    if not isinstance(candlesticks, list) or not candlesticks:
        return drop_row("kalshi", market_id, path, "empty_candlesticks", metadata)

    candidates = []

    for candle in candlesticks:
        if not isinstance(candle, dict):
            continue

        candidate = extract_kalshi_yes_probability(candle)

        if candidate is not None:
            candidates.append(candidate)

    best, reason = choose_nearest_candidate(
        candidates=candidates,
        target_time=target_time,
        resolution_time=resolution_time,
    )

    if best is None:
        return drop_row("kalshi", market_id, path, reason, metadata)

    return ok_row(
        venue="kalshi",
        metadata=metadata,
        raw_path=path,
        target_time=target_time,
        resolution_time=resolution_time,
        best=best,
    )


# =============================================================================
# Output rows
# =============================================================================

def drop_row(
    venue: str,
    market_id: str,
    raw_path: Path,
    reason: str,
    metadata: dict,
) -> dict:
    return {
        "venue": venue,
        "market_id": market_id,
        "title": metadata.get("title", ""),
        "outcome": metadata.get("outcome", ""),
        "resolution_time": metadata.get("resolution_time", ""),
        "target_time": "",
        "price_time": "",
        "p_hat": "",
        "hours_before_resolution": "",
        "target_error_hours": "",
        "price_source": "",
        "bid": "",
        "ask": "",
        "spread": "",
        "status": "drop",
        "reason": reason,
        "raw_file": str(raw_path),
    }


def ok_row(
    venue: str,
    metadata: dict,
    raw_path: Path,
    target_time: datetime,
    resolution_time: datetime,
    best: dict,
) -> dict:
    price_time = best["price_time"]

    return {
        "venue": venue,
        "market_id": metadata.get("market_id", raw_path.stem),
        "title": metadata.get("title", ""),
        "outcome": metadata.get("outcome", ""),
        "resolution_time": resolution_time.isoformat(),
        "target_time": target_time.isoformat(),
        "price_time": price_time.isoformat(),
        "p_hat": best["p_hat"],
        "hours_before_resolution": best["hours_before_resolution"],
        "target_error_hours": best["target_error_hours"],
        "price_source": best.get("price_source", ""),
        "bid": best.get("bid", ""),
        "ask": best.get("ask", ""),
        "spread": best.get("spread", ""),
        "status": "ok",
        "reason": "",
        "raw_file": str(raw_path),
    }


def write_csv(rows: list[dict]) -> None:
    fieldnames = [
        "venue",
        "market_id",
        "title",
        "outcome",
        "resolution_time",
        "target_time",
        "price_time",
        "p_hat",
        "hours_before_resolution",
        "target_error_hours",
        "price_source",
        "bid",
        "ask",
        "spread",
        "status",
        "reason",
        "raw_file",
    ]

    with OUT_CSV.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved: {OUT_CSV}")


def write_report(rows: list[dict]) -> None:
    status_counter = Counter(row["status"] for row in rows)
    venue_status_counter = Counter((row["venue"], row["status"]) for row in rows)
    drop_reason_counter = Counter(
        (row["venue"], row["reason"])
        for row in rows
        if row["status"] == "drop"
    )

    ok_rows = [row for row in rows if row["status"] == "ok"]

    lines = []
    lines.append("# p_hat Smoke Test Report")
    lines.append("")
    lines.append("This report summarizes p_hat extraction from raw price-history files.")
    lines.append("")
    lines.append("No Brier score was calculated in this step.")
    lines.append("")
    lines.append("## Overall")
    lines.append("")
    lines.append(f"- Total files processed: {len(rows)}")
    lines.append(f"- OK rows: {status_counter.get('ok', 0)}")
    lines.append(f"- Dropped rows: {status_counter.get('drop', 0)}")
    lines.append("")
    lines.append("## By venue")
    lines.append("")

    venues = sorted(set(row["venue"] for row in rows))

    for venue in venues:
        lines.append(f"### {venue}")
        lines.append(f"- OK: {venue_status_counter.get((venue, 'ok'), 0)}")
        lines.append(f"- Drop: {venue_status_counter.get((venue, 'drop'), 0)}")
        lines.append("")

    lines.append("## Drop reasons")
    lines.append("")

    if not drop_reason_counter:
        lines.append("- None")
    else:
        for (venue, reason), count in sorted(drop_reason_counter.items()):
            lines.append(f"- {venue} / {reason}: {count}")

    lines.append("")
    lines.append("## Extracted p_hat preview")
    lines.append("")

    for row in ok_rows[:10]:
        lines.append(
            f"- {row['venue']} {row['market_id']}: "
            f"p_hat={row['p_hat']}, "
            f"outcome={row['outcome']}, "
            f"hours_before_resolution={float(row['hours_before_resolution']):.2f}, "
            f"target_error_hours={float(row['target_error_hours']):.2f}"
        )

    lines.append("")
    lines.append("## Next step")
    lines.append("")
    lines.append("If p_hat values look reasonable, scale price-history pulling beyond the smoke test.")
    lines.append("After full p_hat extraction, then compute Brier scores.")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")

    print(f"Saved: {REPORT_PATH}")


def main() -> None:
    print("=" * 80)
    print("Extract p_hat smoke test")
    print("No API calls. No Brier score.")

    rows = []

    polymarket_files = sorted(POLYMARKET_PRICE_DIR.glob("*.json"))
    kalshi_files = sorted(KALSHI_PRICE_DIR.glob("*.json"))

    print("Polymarket price files:", len(polymarket_files))
    print("Kalshi price files:", len(kalshi_files))

    for path in polymarket_files:
        rows.append(extract_polymarket_file(path))

    for path in kalshi_files:
        rows.append(extract_kalshi_file(path))

    write_csv(rows)
    write_report(rows)

    print("\n" + "=" * 80)
    print("p_hat smoke test complete")
    print("Rows:", len(rows))
    print("OK:", sum(row["status"] == "ok" for row in rows))
    print("Dropped:", sum(row["status"] == "drop" for row in rows))
    print("")
    print("Check:")
    print(f"  {OUT_CSV}")
    print(f"  {REPORT_PATH}")


if __name__ == "__main__":
    main()
