"""
26_pull_clean_kalshi_candlesticks.py

Pull one-minute Kalshi candlesticks around the final clean target times and
extract the latest usable market probability at or before each target.

Input:
    data/processed/price_snapshot_targets_clean.csv

Outputs:
    data/raw/kalshi/candlesticks_clean/*.json
    data/processed/price_snapshots_clean.csv
    data/processed/price_snapshot_missing_clean.csv
    outputs/price_snapshot_extraction_report.md

Method:
    - Group targets that share the same target timestamp.
    - Request up to 100 market tickers per batch from Kalshi's public
      /markets/candlesticks endpoint.
    - Use one-minute candlesticks.
    - Search only the six hours preceding each target.
    - Select the latest candlestick ending at or before the target.
    - Record trade price, bid, ask, midpoint, selected probability,
      source, and staleness.

Primary price rule:
    1. yes bid/ask midpoint when both are present;
    2. last trade close;
    3. previous trade price.

No historical-endpoint fallback is performed in this step. Missing markets are
written to a separate CSV so the size of any required fallback can be assessed
before making thousands of individual requests.
"""

from __future__ import annotations

import csv
import hashlib
import json
import math
import time
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import requests


INPUT_PATH = Path(
    "data/processed/price_snapshot_targets_clean.csv"
)

RAW_DIR = Path(
    "data/raw/kalshi/candlesticks_clean"
)

OUTPUT_PATH = Path(
    "data/processed/price_snapshots_clean.csv"
)

MISSING_PATH = Path(
    "data/processed/price_snapshot_missing_clean.csv"
)

REPORT_PATH = Path(
    "outputs/price_snapshot_extraction_report.md"
)

BASE_URL = (
    "https://external-api.kalshi.com"
    "/trade-api/v2/markets/candlesticks"
)

PERIOD_INTERVAL_MINUTES = 1
LOOKBACK_HOURS = 6
INITIAL_BATCH_SIZE = 100
REQUEST_TIMEOUT_SECONDS = 45
MAX_RETRIES = 5
SLEEP_SECONDS = 0.12


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")

    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)

    if not rows:
        path.write_text("", encoding="utf-8")
        print(f"Saved empty file: {path}")
        return

    fieldnames = list(rows[0].keys())

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(
            f,
            fieldnames=fieldnames,
            extrasaction="ignore",
        )
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved: {path} ({len(rows)} rows)")


def parse_time(value: str | None) -> datetime | None:
    if not value:
        return None

    text = str(value).strip().replace("Z", "+00:00")

    try:
        dt = datetime.fromisoformat(text)
    except Exception:
        return None

    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)

    return dt.astimezone(timezone.utc)


def unix_ts(dt: datetime) -> int:
    return int(dt.timestamp())


def iso_from_unix(value: int | float | str | None) -> str:
    if value in (None, ""):
        return ""

    try:
        ts = int(float(value))
    except Exception:
        return ""

    return datetime.fromtimestamp(
        ts,
        tz=timezone.utc,
    ).isoformat()


def safe_float(value: Any) -> float | None:
    if value in (None, ""):
        return None

    try:
        number = float(value)
    except Exception:
        return None

    if not math.isfinite(number):
        return None

    return number


def nested_value(
    item: dict,
    parent: str,
    names: list[str],
) -> float | None:
    obj = item.get(parent)

    if not isinstance(obj, dict):
        return None

    for name in names:
        value = safe_float(obj.get(name))

        if value is not None:
            return value

    return None


def market_id(row: dict) -> str:
    return str(
        row.get("market_id_analysis")
        or row.get("market_id")
        or row.get("ticker")
        or ""
    ).strip()


def family_id(row: dict) -> str:
    return str(
        row.get("family_id_analysis")
        or row.get("family_id_v2")
        or row.get("family_id")
        or market_id(row)
    ).strip()


def chunk(values: list[str], size: int) -> list[list[str]]:
    return [
        values[index:index + size]
        for index in range(0, len(values), size)
    ]


def cache_path(
    tickers: list[str],
    start_ts: int,
    end_ts: int,
) -> Path:
    raw_key = "|".join(
        [
            str(start_ts),
            str(end_ts),
            str(PERIOD_INTERVAL_MINUTES),
            *sorted(tickers),
        ]
    )

    digest = hashlib.sha256(
        raw_key.encode("utf-8")
    ).hexdigest()[:20]

    return RAW_DIR / (
        f"batch_{end_ts}_{len(tickers)}_{digest}.json"
    )


def request_batch(
    session: requests.Session,
    tickers: list[str],
    start_ts: int,
    end_ts: int,
) -> dict:
    path = cache_path(
        tickers,
        start_ts,
        end_ts,
    )

    if path.exists():
        with path.open(encoding="utf-8") as f:
            return json.load(f)

    params = {
        "market_tickers": ",".join(tickers),
        "start_ts": start_ts,
        "end_ts": end_ts,
        "period_interval": PERIOD_INTERVAL_MINUTES,
        "include_latest_before_start": "false",
    }

    last_error = None

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = session.get(
                BASE_URL,
                params=params,
                timeout=REQUEST_TIMEOUT_SECONDS,
            )

            if response.status_code == 429:
                wait = min(2 ** attempt, 30)

                print(
                    f"Rate limited; sleeping {wait}s"
                )

                time.sleep(wait)
                continue

            response.raise_for_status()
            payload = response.json()

            with path.open("w", encoding="utf-8") as f:
                json.dump(
                    payload,
                    f,
                    ensure_ascii=False,
                    indent=2,
                )

            time.sleep(SLEEP_SECONDS)
            return payload

        except Exception as exc:
            last_error = exc

            if attempt < MAX_RETRIES:
                wait = min(2 ** attempt, 30)

                print(
                    f"Retry {attempt}/{MAX_RETRIES}: {exc}"
                )

                time.sleep(wait)

    raise RuntimeError(
        f"Batch request failed: {last_error}"
    )


def fetch_with_recursive_split(
    session: requests.Session,
    tickers: list[str],
    start_ts: int,
    end_ts: int,
) -> list[dict]:
    """
    Split a request when the endpoint rejects an oversized response.
    """
    try:
        payload = request_batch(
            session,
            tickers,
            start_ts,
            end_ts,
        )

        markets = payload.get("markets")

        if isinstance(markets, list):
            return markets

        return []

    except Exception as exc:
        if len(tickers) == 1:
            print(
                f"Single-market batch failed for "
                f"{tickers[0]}: {exc}"
            )
            return []

        midpoint = len(tickers) // 2

        print(
            f"Splitting failed batch of {len(tickers)} "
            f"markets into {midpoint} and "
            f"{len(tickers) - midpoint}"
        )

        left = fetch_with_recursive_split(
            session,
            tickers[:midpoint],
            start_ts,
            end_ts,
        )
        right = fetch_with_recursive_split(
            session,
            tickers[midpoint:],
            start_ts,
            end_ts,
        )

        return left + right


def normalize_market_payload(
    market_payloads: list[dict],
) -> dict[str, list[dict]]:
    result = {}

    for item in market_payloads:
        ticker = str(
            item.get("market_ticker")
            or item.get("ticker")
            or ""
        ).strip()

        if not ticker:
            continue

        candles = item.get("candlesticks")

        if not isinstance(candles, list):
            candles = []

        result[ticker] = candles

    return result


def select_candlestick(
    candles: list[dict],
    target_ts: int,
) -> dict | None:
    valid = []

    for candle in candles:
        end_ts = candle.get("end_period_ts")

        try:
            end_ts_int = int(end_ts)
        except Exception:
            continue

        if end_ts_int <= target_ts:
            valid.append((end_ts_int, candle))

    if not valid:
        return None

    valid.sort(key=lambda item: item[0])
    return valid[-1][1]


def extract_prices(
    candle: dict,
) -> dict:
    trade_close = nested_value(
        candle,
        "price",
        ["close_dollars", "close"],
    )
    previous_price = nested_value(
        candle,
        "price",
        ["previous_dollars", "previous"],
    )
    yes_bid_close = nested_value(
        candle,
        "yes_bid",
        ["close_dollars", "close"],
    )
    yes_ask_close = nested_value(
        candle,
        "yes_ask",
        ["close_dollars", "close"],
    )

    midpoint = None

    if (
        yes_bid_close is not None
        and yes_ask_close is not None
    ):
        midpoint = (
            yes_bid_close + yes_ask_close
        ) / 2.0

    if midpoint is not None:
        primary = midpoint
        source = "yes_bid_ask_midpoint"
    elif trade_close is not None:
        primary = trade_close
        source = "trade_close"
    elif previous_price is not None:
        primary = previous_price
        source = "previous_trade"
    else:
        primary = None
        source = ""

    return {
        "p_hat_primary": primary,
        "price_source": source,
        "trade_close": trade_close,
        "previous_price": previous_price,
        "yes_bid_close": yes_bid_close,
        "yes_ask_close": yes_ask_close,
        "yes_midpoint": midpoint,
        "volume_fp": safe_float(
            candle.get("volume_fp")
            or candle.get("volume")
        ),
        "open_interest_fp": safe_float(
            candle.get("open_interest_fp")
            or candle.get("open_interest")
        ),
    }


def probability_bin(
    probability: float | None,
) -> str:
    if probability is None:
        return "missing"

    bounded = min(max(probability, 0.0), 1.0)

    if bounded == 1.0:
        lower = 0.9
        upper = 1.0
    else:
        lower_index = int(bounded * 10)
        lower = lower_index / 10.0
        upper = lower + 0.1

    return f"{lower:.1f}-{upper:.1f}"


def staleness_bucket(
    minutes: float | None,
) -> str:
    if minutes is None:
        return "missing"
    if minutes <= 5:
        return "0-5m"
    if minutes <= 15:
        return "5-15m"
    if minutes <= 60:
        return "15-60m"
    if minutes <= 180:
        return "1-3h"
    if minutes <= 360:
        return "3-6h"
    return ">6h"


def build_target_groups(
    rows: list[dict],
) -> dict[int, list[dict]]:
    groups = defaultdict(list)

    for row in rows:
        target = parse_time(row.get("target_time"))

        if target is None:
            continue

        groups[unix_ts(target)].append(row)

    return dict(groups)


def process_targets(
    rows: list[dict],
) -> list[dict]:
    RAW_DIR.mkdir(parents=True, exist_ok=True)

    session = requests.Session()
    session.headers.update(
        {
            "User-Agent": (
                "prediction-market-longshot-bias/"
                "clean-price-extraction"
            )
        }
    )

    groups = build_target_groups(rows)
    output_by_key = {}

    total_groups = len(groups)

    for group_index, (
        target_ts,
        target_rows,
    ) in enumerate(
        sorted(groups.items()),
        start=1,
    ):
        tickers = sorted(
            {
                market_id(row)
                for row in target_rows
                if market_id(row)
            }
        )

        start_ts = target_ts - (
            LOOKBACK_HOURS * 60 * 60
        )

        print(
            f"[{group_index}/{total_groups}] "
            f"target={iso_from_unix(target_ts)} "
            f"markets={len(tickers)}"
        )

        candle_lookup = {}

        for ticker_batch in chunk(
            tickers,
            INITIAL_BATCH_SIZE,
        ):
            payload_markets = fetch_with_recursive_split(
                session,
                ticker_batch,
                start_ts,
                target_ts,
            )

            candle_lookup.update(
                normalize_market_payload(payload_markets)
            )

        for row in target_rows:
            ticker = market_id(row)
            candles = candle_lookup.get(ticker, [])
            candle = select_candlestick(
                candles,
                target_ts,
            )

            output = dict(row)
            output["market_id_analysis"] = ticker
            output["family_id_analysis"] = family_id(row)
            output["query_start_time"] = iso_from_unix(
                start_ts
            )
            output["query_end_time"] = iso_from_unix(
                target_ts
            )
            output["candlestick_count_returned"] = len(
                candles
            )

            if candle is None:
                output.update(
                    {
                        "snapshot_status": (
                            "no_candlestick_within_6h"
                        ),
                        "snapshot_time": "",
                        "snapshot_staleness_minutes": "",
                        "staleness_bucket": "missing",
                        "p_hat_primary": "",
                        "price_source": "",
                        "trade_close": "",
                        "previous_price": "",
                        "yes_bid_close": "",
                        "yes_ask_close": "",
                        "yes_midpoint": "",
                        "volume_fp": "",
                        "open_interest_fp": "",
                        "probability_bin": "missing",
                    }
                )
            else:
                candle_end_ts = int(
                    candle["end_period_ts"]
                )
                staleness_minutes = (
                    target_ts - candle_end_ts
                ) / 60.0
                prices = extract_prices(candle)
                primary = prices["p_hat_primary"]

                output.update(
                    {
                        "snapshot_status": (
                            "ok"
                            if primary is not None
                            else "no_usable_price"
                        ),
                        "snapshot_time": iso_from_unix(
                            candle_end_ts
                        ),
                        "snapshot_staleness_minutes": round(
                            staleness_minutes,
                            3,
                        ),
                        "staleness_bucket": (
                            staleness_bucket(
                                staleness_minutes
                            )
                        ),
                        **prices,
                        "probability_bin": probability_bin(
                            primary
                        ),
                    }
                )

            key = str(
                row.get("target_key")
                or "|".join(
                    [
                        ticker,
                        str(row.get("analysis_sample", "")),
                        str(row.get("horizon_hours", "")),
                        str(row.get("target_time", "")),
                    ]
                )
            )

            output_by_key[key] = output

    return list(output_by_key.values())


def write_report(rows: list[dict]) -> None:
    status_counts = Counter(
        row["snapshot_status"] for row in rows
    )
    source_counts = Counter(
        row.get("price_source", "") or "missing"
        for row in rows
    )
    stale_counts = Counter(
        row.get("staleness_bucket", "missing")
        for row in rows
    )

    sample_counts = Counter()
    sample_family_sets = defaultdict(set)
    bin_row_counts = Counter()
    bin_family_sets = defaultdict(set)

    for row in rows:
        key = (
            row.get("analysis_sample", ""),
            int(row.get("horizon_hours", 0)),
        )

        if row["snapshot_status"] == "ok":
            sample_counts[key] += 1
            sample_family_sets[key].add(
                family_id(row)
            )

            bin_key = (
                key[0],
                key[1],
                row.get("probability_bin", "missing"),
            )

            bin_row_counts[bin_key] += 1
            bin_family_sets[bin_key].add(
                family_id(row)
            )

    lines = [
        "# Clean Price Snapshot Extraction",
        "",
        f"- Target rows: {len(rows)}",
        f"- Lookback window: {LOOKBACK_HOURS} hours",
        f"- Candlestick interval: "
        f"{PERIOD_INTERVAL_MINUTES} minute",
        "",
        "## Snapshot status",
        "",
    ]

    for key, value in sorted(status_counts.items()):
        lines.append(f"- {key}: {value}")

    lines.extend(["", "## Price source", ""])

    for key, value in sorted(source_counts.items()):
        lines.append(f"- {key}: {value}")

    lines.extend(["", "## Snapshot staleness", ""])

    for key, value in sorted(stale_counts.items()):
        lines.append(f"- {key}: {value}")

    lines.extend(
        [
            "",
            "## Valid snapshots by sample and horizon",
            "",
        ]
    )

    for key in sorted(sample_counts):
        lines.extend(
            [
                f"### {key[0]} / {key[1]}h",
                "",
                f"- Valid rows: {sample_counts[key]}",
                f"- Unique families: "
                f"{len(sample_family_sets[key])}",
                "",
            ]
        )

    lines.extend(
        [
            "## Probability-bin coverage",
            "",
            "Counts below include only rows with a usable price.",
            "",
        ]
    )

    for bin_key in sorted(
        bin_row_counts,
        key=lambda value: (
            value[0],
            value[1],
            value[2],
        ),
    ):
        sample, horizon, bin_label = bin_key

        lines.append(
            f"- {sample} / {horizon}h / "
            f"{bin_label}: "
            f"{bin_row_counts[bin_key]} rows, "
            f"{len(bin_family_sets[bin_key])} families"
        )

    lines.extend(
        [
            "",
            "## Next step",
            "",
            "Apply explicit staleness thresholds and create one "
            "observation per family-bin before bootstrap inference.",
            "",
            f"- Snapshots: `{OUTPUT_PATH}`",
            f"- Missing snapshots: `{MISSING_PATH}`",
        ]
    )

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text(
        "\n".join(lines),
        encoding="utf-8",
    )

    print(f"Saved: {REPORT_PATH}")


def main() -> None:
    rows = read_csv(INPUT_PATH)

    print(f"Target rows: {len(rows)}")

    snapshots = process_targets(rows)

    snapshots.sort(
        key=lambda row: (
            row.get("analysis_sample", ""),
            int(row.get("horizon_hours", 0)),
            family_id(row),
            market_id(row),
        )
    )

    missing = [
        row for row in snapshots
        if row["snapshot_status"] != "ok"
    ]

    write_csv(OUTPUT_PATH, snapshots)
    write_csv(MISSING_PATH, missing)
    write_report(snapshots)


if __name__ == "__main__":
    main()
