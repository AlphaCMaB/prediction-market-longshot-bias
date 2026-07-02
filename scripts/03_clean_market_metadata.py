"""
03_clean_market_metadata.py

Purpose:
    Convert raw Polymarket and Kalshi API metadata into a clean standardized
    market-level dataset.

Important:
    This script does NOT use local/sample data.
    This script does NOT pull price history.
    This script does NOT calculate p_hat.
    This script does NOT calculate Brier scores.

It only creates a clean metadata table with:
    - venue
    - market identifiers
    - title
    - resolution time
    - final binary outcome
    - price-history lookup fields

Inputs:
    data/raw/polymarket/polymarket_recent_closed_markets.jsonl
    data/raw/kalshi/kalshi_recent_settled_markets.jsonl

Outputs:
    data/processed/markets_metadata_clean.csv
    data/processed/markets_metadata_clean.jsonl
    outputs/drop_log.csv
    outputs/cleaning_report.md
"""

from __future__ import annotations

import ast
import csv
import json
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


POLYMARKET_RAW_PATH = Path("data/raw/polymarket/polymarket_recent_closed_markets.jsonl")
KALSHI_RAW_PATH = Path("data/raw/kalshi/kalshi_recent_settled_markets.jsonl")

PROCESSED_DIR = Path("data/processed")
OUTPUTS_DIR = Path("outputs")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

CLEAN_CSV_PATH = PROCESSED_DIR / "markets_metadata_clean.csv"
CLEAN_JSONL_PATH = PROCESSED_DIR / "markets_metadata_clean.jsonl"
DROP_LOG_PATH = OUTPUTS_DIR / "drop_log.csv"
REPORT_PATH = OUTPUTS_DIR / "cleaning_report.md"


# =============================================================================
# Helpers
# =============================================================================

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


def parse_list_field(value: Any) -> list[Any] | None:
    """
    Polymarket often stores fields like outcomes, outcomePrices, clobTokenIds
    as JSON strings, for example:
        "[\"Yes\", \"No\"]"

    This helper converts them into Python lists.
    """
    if value is None:
        return None

    if isinstance(value, list):
        return value

    if isinstance(value, str):
        value = value.strip()

        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass

        try:
            parsed = ast.literal_eval(value)
            if isinstance(parsed, list):
                return parsed
        except Exception:
            pass

    return None


def get_polymarket_resolution_time(market: dict) -> datetime | None:
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


def get_polymarket_category(market: dict) -> str:
    if market.get("category"):
        return str(market.get("category"))

    events = market.get("events")

    if isinstance(events, list) and events:
        first_event = events[0]
        if isinstance(first_event, dict) and first_event.get("category"):
            return str(first_event.get("category"))

    return "unknown"


def dedupe_clean_rows(rows: list[dict]) -> list[dict]:
    """
    Deduplicate by venue + market_id.
    """
    seen = set()
    deduped = []

    for row in rows:
        key = (row.get("venue"), row.get("market_id"))

        if key in seen:
            continue

        seen.add(key)
        deduped.append(row)

    return deduped


def add_drop(drop_log: list[dict], venue: str, market_id: str | None, reason: str) -> None:
    drop_log.append(
        {
            "venue": venue,
            "market_id": market_id or "",
            "reason": reason,
        }
    )


# =============================================================================
# Polymarket cleaning
# =============================================================================

def clean_polymarket_rows(raw_rows: list[dict], drop_log: list[dict]) -> list[dict]:
    clean_rows = []

    for market in raw_rows:
        market_id = str(market.get("id", ""))

        if market.get("closed") is not True:
            add_drop(drop_log, "polymarket", market_id, "not_closed")
            continue

        resolution_time = get_polymarket_resolution_time(market)

        if resolution_time is None:
            add_drop(drop_log, "polymarket", market_id, "missing_resolution_time")
            continue

        outcomes = parse_list_field(market.get("outcomes"))
        outcome_prices = parse_list_field(market.get("outcomePrices"))
        clob_token_ids = parse_list_field(market.get("clobTokenIds"))

        if not outcomes or len(outcomes) != 2:
            add_drop(drop_log, "polymarket", market_id, "not_binary_outcomes")
            continue

        if not outcome_prices or len(outcome_prices) != 2:
            add_drop(drop_log, "polymarket", market_id, "missing_binary_outcome_prices")
            continue

        if not clob_token_ids or len(clob_token_ids) != 2:
            add_drop(drop_log, "polymarket", market_id, "missing_binary_clob_token_ids")
            continue

        normalized_outcomes = [str(x).strip().lower() for x in outcomes]

        if "yes" not in normalized_outcomes or "no" not in normalized_outcomes:
            add_drop(drop_log, "polymarket", market_id, "outcomes_not_yes_no")
            continue

        yes_index = normalized_outcomes.index("yes")
        no_index = normalized_outcomes.index("no")

        yes_final_price = to_float(outcome_prices[yes_index])
        no_final_price = to_float(outcome_prices[no_index])

        if yes_final_price is None or no_final_price is None:
            add_drop(drop_log, "polymarket", market_id, "invalid_final_outcome_prices")
            continue

        # For resolved binary Polymarket markets, final outcomePrices should usually be 1/0.
        # If it resolves at 0.5 or some unusual value, skip for now.
        if yes_final_price not in {0.0, 1.0}:
            add_drop(drop_log, "polymarket", market_id, "non_binary_final_yes_price")
            continue

        outcome = int(yes_final_price)

        clean_rows.append(
            {
                "venue": "polymarket",
                "market_id": market_id,
                "event_id": "",
                "ticker": str(market.get("slug") or ""),
                "title": str(market.get("question") or ""),
                "category": get_polymarket_category(market),
                "resolution_time": resolution_time.isoformat(),
                "outcome": outcome,

                # For next step: Polymarket price-history API needs token ID.
                "yes_token_id": str(clob_token_ids[yes_index]),
                "no_token_id": str(clob_token_ids[no_index]),

                # Kalshi-specific fields left empty
                "kalshi_event_ticker": "",
                "kalshi_series_ticker": "",

                "volume": to_float(market.get("volumeNum") or market.get("volume")),
                "liquidity": to_float(market.get("liquidityNum") or market.get("liquidity")),

                # Keep raw final fields for auditability.
                "final_yes_price": yes_final_price,
                "final_no_price": no_final_price,
                "raw_status": str(market.get("umaResolutionStatus") or ""),
            }
        )

    return clean_rows


# =============================================================================
# Kalshi cleaning
# =============================================================================

def clean_kalshi_rows(raw_rows: list[dict], drop_log: list[dict]) -> list[dict]:
    clean_rows = []

    for market in raw_rows:
        ticker = str(market.get("ticker", ""))

        if not ticker:
            add_drop(drop_log, "kalshi", ticker, "missing_ticker")
            continue

        # Extra safety: exclude multivariate event markets if any slipped through.
        if ticker.startswith("KXMVE") or str(market.get("event_ticker", "")).startswith("KXMVE"):
            add_drop(drop_log, "kalshi", ticker, "multivariate_market")
            continue

        if market.get("market_type") != "binary":
            add_drop(drop_log, "kalshi", ticker, "not_binary_market_type")
            continue

        if market.get("status") not in {"settled", "finalized"}:
            add_drop(drop_log, "kalshi", ticker, "not_settled_or_finalized")
            continue

        resolution_time = get_kalshi_resolution_time(market)

        if resolution_time is None:
            add_drop(drop_log, "kalshi", ticker, "missing_resolution_time")
            continue

        settlement_value = to_float(market.get("settlement_value_dollars"))

        if settlement_value is None:
            add_drop(drop_log, "kalshi", ticker, "missing_settlement_value")
            continue

        # Skip ambiguous / 50-cent / non-binary settlements for now.
        if settlement_value not in {0.0, 1.0}:
            add_drop(drop_log, "kalshi", ticker, "non_binary_settlement_value")
            continue

        outcome = int(settlement_value)

        event_ticker = str(market.get("event_ticker") or "")

        # For Kalshi candlestick endpoint, we usually need series_ticker.
        # Most event tickers look like KXITFMATCH-26JUN18AGEDIB.
        # The series ticker is usually the part before the first dash: KXITFMATCH.
        series_ticker = event_ticker.split("-")[0] if event_ticker else ""

        clean_rows.append(
            {
                "venue": "kalshi",
                "market_id": ticker,
                "event_id": event_ticker,
                "ticker": ticker,
                "title": str(market.get("title") or ""),
                "category": series_ticker or "unknown",
                "resolution_time": resolution_time.isoformat(),
                "outcome": outcome,

                # Polymarket-specific fields left empty
                "yes_token_id": "",
                "no_token_id": "",

                # For next step: Kalshi candlestick API needs series + market ticker.
                "kalshi_event_ticker": event_ticker,
                "kalshi_series_ticker": series_ticker,

                "volume": to_float(market.get("volume_fp")),
                "liquidity": to_float(market.get("liquidity_dollars")),

                # Keep raw final fields for auditability.
                "final_yes_price": settlement_value,
                "final_no_price": 1.0 - settlement_value,
                "raw_status": str(market.get("status") or ""),
            }
        )

    return clean_rows


# =============================================================================
# Output
# =============================================================================

def write_clean_outputs(clean_rows: list[dict], drop_log: list[dict]) -> None:
    fieldnames = [
        "venue",
        "market_id",
        "event_id",
        "ticker",
        "title",
        "category",
        "resolution_time",
        "outcome",
        "yes_token_id",
        "no_token_id",
        "kalshi_event_ticker",
        "kalshi_series_ticker",
        "volume",
        "liquidity",
        "final_yes_price",
        "final_no_price",
        "raw_status",
    ]

    with CLEAN_CSV_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(clean_rows)

    with CLEAN_JSONL_PATH.open("w", encoding="utf-8") as f:
        for row in clean_rows:
            f.write(json.dumps(row, ensure_ascii=False) + "\n")

    with DROP_LOG_PATH.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["venue", "market_id", "reason"])
        writer.writeheader()
        writer.writerows(drop_log)

    print(f"Saved clean CSV: {CLEAN_CSV_PATH}")
    print(f"Saved clean JSONL: {CLEAN_JSONL_PATH}")
    print(f"Saved drop log: {DROP_LOG_PATH}")


def write_report(
    polymarket_raw_count: int,
    kalshi_raw_count: int,
    clean_rows: list[dict],
    drop_log: list[dict],
) -> None:
    clean_by_venue = Counter(row["venue"] for row in clean_rows)
    outcome_by_venue = defaultdict(Counter)

    for row in clean_rows:
        outcome_by_venue[row["venue"]][row["outcome"]] += 1

    drop_by_venue_reason = Counter(
        (row["venue"], row["reason"]) for row in drop_log
    )

    lines = []
    lines.append("# Cleaning Report")
    lines.append("")
    lines.append("This report summarizes metadata cleaning only.")
    lines.append("")
    lines.append("No price history was pulled in this step.")
    lines.append("No Brier score was calculated in this step.")
    lines.append("")
    lines.append("## Raw input counts")
    lines.append("")
    lines.append(f"- Polymarket raw rows: {polymarket_raw_count}")
    lines.append(f"- Kalshi raw rows: {kalshi_raw_count}")
    lines.append("")
    lines.append("## Clean output counts")
    lines.append("")
    lines.append(f"- Total clean rows: {len(clean_rows)}")

    for venue, count in sorted(clean_by_venue.items()):
        lines.append(f"- {venue}: {count}")

    lines.append("")
    lines.append("## Outcomes by venue")
    lines.append("")

    for venue, counter in sorted(outcome_by_venue.items()):
        lines.append(f"### {venue}")
        lines.append(f"- outcome = 1: {counter.get(1, 0)}")
        lines.append(f"- outcome = 0: {counter.get(0, 0)}")
        lines.append("")

    lines.append("## Drop reasons")
    lines.append("")

    if not drop_by_venue_reason:
        lines.append("- No dropped rows")
    else:
        for (venue, reason), count in sorted(drop_by_venue_reason.items()):
            lines.append(f"- {venue} / {reason}: {count}")

    lines.append("")
    lines.append("## Next step")
    lines.append("")
    lines.append("Pull price history for each clean market:")
    lines.append("")
    lines.append("- Polymarket: use yes_token_id with CLOB price-history API")
    lines.append("- Kalshi: use kalshi_series_ticker + ticker with candlestick API")
    lines.append("")
    lines.append("Only after getting the 48-hour pre-resolution probability should we calculate Brier scores.")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")

    print(f"Saved cleaning report: {REPORT_PATH}")


# =============================================================================
# Main
# =============================================================================

def main() -> None:
    print("=" * 80)
    print("Clean market metadata")
    print("No local data. No price history. No Brier score.")

    polymarket_raw = read_jsonl(POLYMARKET_RAW_PATH)
    kalshi_raw = read_jsonl(KALSHI_RAW_PATH)

    print(f"Polymarket raw rows: {len(polymarket_raw)}")
    print(f"Kalshi raw rows: {len(kalshi_raw)}")

    drop_log: list[dict] = []

    polymarket_clean = clean_polymarket_rows(polymarket_raw, drop_log)
    kalshi_clean = clean_kalshi_rows(kalshi_raw, drop_log)

    clean_rows = polymarket_clean + kalshi_clean
    clean_rows = dedupe_clean_rows(clean_rows)

    write_clean_outputs(clean_rows, drop_log)
    write_report(
        polymarket_raw_count=len(polymarket_raw),
        kalshi_raw_count=len(kalshi_raw),
        clean_rows=clean_rows,
        drop_log=drop_log,
    )

    print("\n" + "=" * 80)
    print("Cleaning complete")
    print("Clean rows:", len(clean_rows))
    print("Polymarket clean rows:", len(polymarket_clean))
    print("Kalshi clean rows:", len(kalshi_clean))
    print("Dropped rows:", len(drop_log))
    print("")
    print("Next: pull price history. Do not calculate Brier score yet.")


if __name__ == "__main__":
    main()