"""Extract snapshots; invoke as ``python -m scripts.pipeline_v2.extract_kalshi_candlesticks``."""

from __future__ import annotations

import argparse
import json
import tomllib
from collections import defaultdict
from datetime import timedelta
from pathlib import Path
from typing import Any

import requests

from scripts.common.io_utils import read_csv, write_csv
from scripts.common.time_utils import parse_iso_utc
from scripts.pipeline_v2.candlesticks import build_snapshot
from scripts.pipeline_v2.kalshi_candlestick_client import KalshiCandlestickClient


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--input", required=True, type=Path)
    parser.add_argument("--output", required=True, type=Path)
    parser.add_argument("--missing-output", required=True, type=Path)
    parser.add_argument("--report-output", required=True, type=Path)
    parser.add_argument("--cache-dir", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--market-limit", type=int)
    return parser.parse_args(argv)


def load_config(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        return tomllib.load(handle)


def _market_id(row: dict[str, Any]) -> str:
    return str(row.get("market_id") or row.get("ticker") or "").strip()


def _target_key(row: dict[str, Any]) -> str:
    return str(row.get("target_key") or "|".join(
        [
            str(row.get("venue") or ""),
            _market_id(row),
            str(row.get("timing_structure") or ""),
            str(row.get("horizon_hours") or ""),
            str(row.get("target_time") or ""),
        ]
    ))


def normalize_market_payload(markets: list[dict[str, Any]]) -> dict[str, list[dict[str, Any]]]:
    result: dict[str, list[dict[str, Any]]] = {}
    for market in markets:
        ticker = str(market.get("market_ticker") or market.get("ticker") or "").strip()
        rows = market.get("candlesticks", [])
        if ticker:
            result[ticker] = rows if isinstance(rows, list) else []
    return result


def extract_rows(
    target_rows: list[dict[str, Any]],
    *,
    client: KalshiCandlestickClient,
    lookback_hours: int,
    main_staleness_minutes: float,
    robustness_staleness_minutes: float,
    existing_keys: set[str] | None = None,
) -> list[dict[str, Any]]:
    existing_keys = existing_keys or set()
    groups: dict[int, list[dict[str, Any]]] = defaultdict(list)
    for row in target_rows:
        if _target_key(row) in existing_keys:
            continue
        target = parse_iso_utc(row.get("target_time"))
        if target is not None and _market_id(row):
            groups[int(target.timestamp())].append(row)

    output: list[dict[str, Any]] = []
    for target_ts, rows in sorted(groups.items()):
        tickers = sorted({_market_id(row) for row in rows})
        start_ts = target_ts - int(timedelta(hours=lookback_hours).total_seconds())
        markets = client.fetch(tickers, start_ts, target_ts)
        candle_lookup = normalize_market_payload(markets)
        for row in rows:
            result = dict(row)
            result["target_key"] = _target_key(row)
            result.update(
                build_snapshot(
                    candle_lookup.get(_market_id(row), []),
                    target_ts,
                    main_staleness_minutes=main_staleness_minutes,
                    robustness_staleness_minutes=robustness_staleness_minutes,
                )
            )
            output.append(result)
    return output


def run(args: argparse.Namespace, *, session: Any | None = None) -> dict[str, Any]:
    config = load_config(args.config)
    rows = read_csv(args.input)
    if args.market_limit is not None:
        allowed = sorted({_market_id(row) for row in rows})[: args.market_limit]
        rows = [row for row in rows if _market_id(row) in set(allowed)]

    existing: list[dict[str, Any]] = []
    if args.resume and args.output.exists():
        existing = read_csv(args.output)
    existing_keys = {_target_key(row) for row in existing}

    http_session = session or requests.Session()
    client = KalshiCandlestickClient(
        session=http_session,
        cache_dir=args.cache_dir,
        interval_minutes=int(config["candlestick_interval_minutes"]),
        batch_size=int(config["batch_size"]),
        dry_run=args.dry_run,
    )
    extracted = extract_rows(
        rows,
        client=client,
        lookback_hours=int(config["candlestick_lookback_hours"]),
        main_staleness_minutes=float(config["main_staleness_minutes"]),
        robustness_staleness_minutes=float(config["robustness_staleness_minutes"]),
        existing_keys=existing_keys,
    )
    summary = {
        "anticipated_request_count": client.anticipated_request_count,
        "completed_request_count": client.completed_request_count,
        "cache_hit_count": client.cache_hit_count,
        "new_snapshot_count": len(extracted),
    }
    print(json.dumps(summary, sort_keys=True))
    if args.dry_run:
        return summary

    combined = existing + extracted
    missing = [row for row in combined if row.get("snapshot_status") != "ok"]
    write_csv(args.output, combined, fieldnames=list(combined[0]) if combined else ["target_key"])
    write_csv(args.missing_output, missing, fieldnames=list(combined[0]) if combined else ["target_key"])
    args.report_output.parent.mkdir(parents=True, exist_ok=True)
    args.report_output.write_text(json.dumps(summary, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return summary


def main(argv: list[str] | None = None) -> None:
    run(parse_args(argv))


if __name__ == "__main__":
    main()
