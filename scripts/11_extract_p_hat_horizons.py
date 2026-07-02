"""
11_extract_p_hat_horizons.py

Purpose:
    Extract p_hat from raw multi-horizon price-history files.

Inputs:
    data/raw/price_history_horizons/polymarket/<horizon>h/*.json
    data/raw/price_history_horizons/kalshi/<horizon>h/*.json

Outputs:
    data/processed/p_hat_horizons.csv
    outputs/p_hat_horizons_report.md

Important:
    This script does NOT call APIs.
    This script does NOT calculate Brier scores.
"""

from __future__ import annotations

import csv
import json
import math
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


RAW_HORIZON_DIR = Path("data/raw/price_history_horizons")

PROCESSED_DIR = Path("data/processed")
OUTPUTS_DIR = Path("outputs")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

OUT_CSV = PROCESSED_DIR / "p_hat_horizons.csv"
REPORT_PATH = OUTPUTS_DIR / "p_hat_horizons_report.md"

MAX_TARGET_ERROR_HOURS = 2.0


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


def get_horizon_from_wrapper(wrapper: dict, path: Path) -> int | None:
    horizon = wrapper.get("horizon_hours")

    if horizon is not None and horizon != "":
        try:
            return int(horizon)
        except Exception:
            pass

    folder = path.parent.name
    if folder.endswith("h"):
        try:
            return int(folder[:-1])
        except Exception:
            return None

    return None


def choose_nearest_candidate(
    candidates: list[dict],
    target_time: datetime,
    resolution_time: datetime,
) -> tuple[dict | None, str]:
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


def drop_row(
    venue: str,
    horizon_hours: int | str,
    market_id: str,
    raw_path: Path,
    reason: str,
    metadata: dict,
) -> dict:
    return {
        "venue": venue,
        "horizon_hours": horizon_hours,
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
    horizon_hours: int,
    metadata: dict,
    raw_path: Path,
    target_time: datetime,
    resolution_time: datetime,
    best: dict,
) -> dict:
    price_time = best["price_time"]

    return {
        "venue": venue,
        "horizon_hours": horizon_hours,
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


def extract_polymarket_file(path: Path) -> dict:
    wrapper = read_json(path)

    metadata = wrapper.get("metadata", {})
    response = wrapper.get("response", {})

    market_id = metadata.get("market_id", path.stem)
    horizon_hours = get_horizon_from_wrapper(wrapper, path)

    resolution_time = parse_time(metadata.get("resolution_time"))
    target_time = parse_time(wrapper.get("target_time"))

    if horizon_hours is None:
        return drop_row("polymarket", "", market_id, path, "missing_horizon_hours", metadata)

    if resolution_time is None:
        return drop_row("polymarket", horizon_hours, market_id, path, "missing_resolution_time", metadata)

    if target_time is None:
        return drop_row("polymarket", horizon_hours, market_id, path, "missing_target_time", metadata)

    history = response.get("history", [])

    if not isinstance(history, list) or not history:
        return drop_row("polymarket", horizon_hours, market_id, path, "empty_history", metadata)

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

    best, reason = choose_nearest_candidate(candidates, target_time, resolution_time)

    if best is None:
        return drop_row("polymarket", horizon_hours, market_id, path, reason, metadata)

    return ok_row("polymarket", horizon_hours, metadata, path, target_time, resolution_time, best)


def get_nested_float(row: dict, outer_key: str, inner_key: str) -> float | None:
    value = row.get(outer_key)

    if not isinstance(value, dict):
        return None

    return to_float(value.get(inner_key))


def extract_kalshi_yes_probability(candle: dict) -> dict | None:
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
    horizon_hours = get_horizon_from_wrapper(wrapper, path)

    resolution_time = parse_time(metadata.get("resolution_time"))
    target_time = parse_time(wrapper.get("target_time"))

    if horizon_hours is None:
        return drop_row("kalshi", "", market_id, path, "missing_horizon_hours", metadata)

    if resolution_time is None:
        return drop_row("kalshi", horizon_hours, market_id, path, "missing_resolution_time", metadata)

    if target_time is None:
        return drop_row("kalshi", horizon_hours, market_id, path, "missing_target_time", metadata)

    candlesticks = response.get("candlesticks", [])

    if not isinstance(candlesticks, list) or not candlesticks:
        return drop_row("kalshi", horizon_hours, market_id, path, "empty_candlesticks", metadata)

    candidates = []

    for candle in candlesticks:
        if not isinstance(candle, dict):
            continue

        candidate = extract_kalshi_yes_probability(candle)

        if candidate is not None:
            candidates.append(candidate)

    best, reason = choose_nearest_candidate(candidates, target_time, resolution_time)

    if best is None:
        return drop_row("kalshi", horizon_hours, market_id, path, reason, metadata)

    return ok_row("kalshi", horizon_hours, metadata, path, target_time, resolution_time, best)


def write_csv(rows: list[dict]) -> None:
    fieldnames = [
        "venue",
        "horizon_hours",
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

    venue_horizon_status = Counter(
        (row["venue"], str(row["horizon_hours"]), row["status"])
        for row in rows
    )

    drop_reason_counter = Counter(
        (row["venue"], str(row["horizon_hours"]), row["reason"])
        for row in rows
        if row["status"] == "drop"
    )

    lines = []
    lines.append("# p_hat Horizons Extraction Report")
    lines.append("")
    lines.append("This report summarizes p_hat extraction across multiple forecast horizons.")
    lines.append("")
    lines.append("No Brier score was calculated in this step.")
    lines.append("")
    lines.append("## Overall")
    lines.append("")
    lines.append(f"- Total files processed: {len(rows)}")
    lines.append(f"- OK rows: {status_counter.get('ok', 0)}")
    lines.append(f"- Dropped rows: {status_counter.get('drop', 0)}")
    lines.append(f"- Max target error hours intended for later filtering: {MAX_TARGET_ERROR_HOURS}")
    lines.append("")
    lines.append("## By venue and horizon")
    lines.append("")

    venues = sorted(set(row["venue"] for row in rows))
    horizons = sorted(
        set(str(row["horizon_hours"]) for row in rows),
        key=lambda x: int(x) if x.isdigit() else 999999,
    )

    for venue in venues:
        lines.append(f"### {venue}")
        for horizon in horizons:
            ok = venue_horizon_status.get((venue, horizon, "ok"), 0)
            drop = venue_horizon_status.get((venue, horizon, "drop"), 0)
            if ok or drop:
                lines.append(f"- {horizon}h: OK={ok}, Drop={drop}")
        lines.append("")

    lines.append("## Drop reasons")
    lines.append("")

    if not drop_reason_counter:
        lines.append("- None")
    else:
        for (venue, horizon, reason), count in sorted(drop_reason_counter.items()):
            lines.append(f"- {venue} {horizon}h / {reason}: {count}")

    lines.append("")
    lines.append("## Preview of extracted rows")
    lines.append("")

    ok_rows = [row for row in rows if row["status"] == "ok"]

    for row in ok_rows[:20]:
        lines.append(
            f"- {row['venue']} {row['horizon_hours']}h {row['market_id']}: "
            f"p_hat={row['p_hat']}, "
            f"outcome={row['outcome']}, "
            f"hours_before_resolution={float(row['hours_before_resolution']):.2f}, "
            f"target_error_hours={float(row['target_error_hours']):.2f}"
        )

    lines.append("")
    lines.append("## Next step")
    lines.append("")
    lines.append("Run horizon-level Brier/calibration analysis using `data/processed/p_hat_horizons.csv`.")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")

    print(f"Saved: {REPORT_PATH}")


def main() -> None:
    print("=" * 80)
    print("Extract p_hat for multiple horizons")
    print("No API calls. No Brier score.")

    rows = []

    polymarket_files = sorted((RAW_HORIZON_DIR / "polymarket").glob("*h/*.json"))
    kalshi_files = sorted((RAW_HORIZON_DIR / "kalshi").glob("*h/*.json"))

    print("Polymarket horizon price files:", len(polymarket_files))
    print("Kalshi horizon price files:", len(kalshi_files))

    for path in polymarket_files:
        rows.append(extract_polymarket_file(path))

    for path in kalshi_files:
        rows.append(extract_kalshi_file(path))

    write_csv(rows)
    write_report(rows)

    print("")
    print("=" * 80)
    print("p_hat horizon extraction complete")
    print("Rows:", len(rows))
    print("OK:", sum(row["status"] == "ok" for row in rows))
    print("Dropped:", sum(row["status"] == "drop" for row in rows))
    print("")
    print("Check:")
    print(f"  {OUT_CSV}")
    print(f"  {REPORT_PATH}")


if __name__ == "__main__":
    main()
