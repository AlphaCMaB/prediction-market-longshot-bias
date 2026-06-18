# Prediction Market Week 1 Pipeline

This is the Week 1 implementation for the project described in the PDF: pull resolved prediction-market contracts, clean/align the data, avoid leakage, and compute Brier scores. The PDF defines Stage 1 as the data pipeline and Brier score as the mean squared error between market probability and outcome. Calibration curves and longshot-bias tests are intentionally left for later.

## What this implements

- Pulls resolved binary markets from Polymarket and Kalshi.
- Uses a default six-month window (`--window-days 180`).
- Uses a default pre-resolution snapshot of 48 hours before resolution (`--snapshot-hours 48`).
- Stores raw API responses under `data/raw/`.
- Creates a clean aligned dataset under `data/processed/analysis_dataset.csv`.
- Drops rows with missing price, invalid outcome, invalid probability, or `price_time >= resolution_time`.
- Computes Brier score overall, by venue, by category, and by venue/category.
- Writes a data-quality report explaining counts, drops, and API failures.

## Why 6 months and 48 hours?

Six months is recent enough that API schemas and market structure are less likely to have changed, while long enough to collect a useful first sample. The 48-hour snapshot avoids the major leakage problem: if we use the settlement price or last trade right before resolution, we may be measuring knowledge of the outcome rather than market forecasting skill.

## Schema

Each row is one binary YES contract:

```text
venue
market_id
token_id
title
category_raw
category
resolution_time
target_price_time
price_time
snapshot_hours_before_resolution
p_hat
outcome
volume
liquidity
price_source
raw_url
```

`p_hat` is a market-implied YES probability in `[0, 1]`.

`outcome` is `1` if YES wins and `0` if YES loses.

## Run

```bash
cd prediction_market_week1
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python -m src.pm_week1 --venues polymarket kalshi --window-days 180 --snapshot-hours 48 --outdir .
```

## Outputs

```text
data/raw/polymarket_markets_raw.jsonl
data/raw/kalshi_markets_raw.jsonl
data/processed/analysis_dataset_uncleaned.csv
data/processed/analysis_dataset.csv
data/processed/analysis_dataset.parquet
outputs/drop_log.csv
outputs/brier_scores.csv
outputs/brier_by_venue.csv
outputs/brier_by_category.csv
outputs/brier_by_venue_category.csv
outputs/data_quality_report.md
```

## Important note

Some hosted execution environments block direct Kalshi API calls. This implementation does not fabricate Kalshi rows when a fetch fails. It records the error in `outputs/data_quality_report.md` so you can rerun locally or in Agent Mode with network access.
