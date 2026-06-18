from __future__ import annotations

import argparse
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

from .fetchers import fetch_kalshi, fetch_polymarket
from .metrics import brier_tables
from .quality import clean_analysis_df, quality_report


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Week 1 prediction-market pull/clean/Brier pipeline")
    p.add_argument("--venues", nargs="+", default=["polymarket", "kalshi"], choices=["polymarket", "kalshi"])
    p.add_argument("--window-days", type=int, default=180)
    p.add_argument("--snapshot-hours", type=int, default=48)
    p.add_argument("--outdir", default=".")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    root = Path(args.outdir)
    raw_dir = root / "data" / "raw"
    processed_dir = root / "data" / "processed"
    outputs_dir = root / "outputs"
    for d in [raw_dir, processed_dir, outputs_dir]:
        d.mkdir(parents=True, exist_ok=True)

    end = datetime.now(timezone.utc)
    start = end - timedelta(days=args.window_days)

    rows = []
    fetch_errors: list[str] = []
    if "polymarket" in args.venues:
        pm_rows, pm_errors = fetch_polymarket(start, end, args.snapshot_hours, raw_dir)
        rows.extend(pm_rows)
        fetch_errors.extend(pm_errors)
    if "kalshi" in args.venues:
        k_rows, k_errors = fetch_kalshi(start, end, args.snapshot_hours, raw_dir)
        rows.extend(k_rows)
        fetch_errors.extend(k_errors)

    raw_df = pd.DataFrame([r.to_dict() for r in rows])
    if raw_df.empty:
        raw_df = pd.DataFrame(columns=[
            "venue", "market_id", "token_id", "title", "category_raw", "category",
            "resolution_time", "target_price_time", "price_time", "snapshot_hours_before_resolution",
            "p_hat", "outcome", "volume", "liquidity", "price_source", "raw_url"
        ])
    raw_df.to_csv(processed_dir / "analysis_dataset_uncleaned.csv", index=False)
    clean_df, drop_log = clean_analysis_df(raw_df)
    clean_df.to_csv(processed_dir / "analysis_dataset.csv", index=False)
    try:
        clean_df.to_parquet(processed_dir / "analysis_dataset.parquet", index=False)
    except Exception:
        pass
    drop_log.to_csv(outputs_dir / "drop_log.csv", index=False)

    tables = brier_tables(clean_df) if len(clean_df) else {}
    for name, table in tables.items():
        table.to_csv(outputs_dir / f"brier_{name}.csv", index=False)
    if tables:
        tables["overall"].to_csv(outputs_dir / "brier_scores.csv", index=False)
    else:
        pd.DataFrame(columns=["group", "n_contracts", "mean_p_hat", "yes_rate", "brier_score", "baseline_brier"]).to_csv(outputs_dir / "brier_scores.csv", index=False)

    (outputs_dir / "data_quality_report.md").write_text(quality_report(clean_df, drop_log, fetch_errors), encoding="utf-8")
    print(f"Wrote clean dataset to {processed_dir / 'analysis_dataset.csv'}")
    print(f"Wrote report to {outputs_dir / 'data_quality_report.md'}")


if __name__ == "__main__":
    main()
