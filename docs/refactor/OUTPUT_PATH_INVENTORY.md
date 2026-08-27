# Output Path Inventory

This inventory documents the current file chains and the intended future V2
namespaces. It is descriptive only. Existing raw data, processed data, caches,
and outputs must remain untouched until a separately reviewed migration.

## Methodology V1 file chain

### Initial 48-hour batch

```text
data/raw/polymarket/polymarket_recent_closed_markets.jsonl
data/raw/kalshi/kalshi_recent_settled_markets.jsonl
    -> data/processed/markets_metadata_clean.csv
    -> data/processed/markets_metadata_clean.jsonl
    -> data/raw/price_history/polymarket/*.json
    -> data/raw/price_history/kalshi/*.json
    -> data/processed/p_hat_batch.csv
    -> data/processed/p_hat_batch_clustered.csv
    -> data/processed/p_hat_batch_declustered_family_bin.csv
    -> data/processed/p_hat_batch_declustered_one_per_family.csv
```

Associated outputs include:

- `outputs/drop_log.csv`
- `outputs/cleaning_report.md`
- `outputs/price_history_batch_manifest.csv`
- `outputs/price_history_smoke_test_manifest.csv`
- `outputs/p_hat_batch_report.md`
- `outputs/p_hat_smoke_test_report.md`
- `outputs/declustering_report.md`
- `outputs/brier_summary_batch.csv`
- `outputs/calibration_bins_batch.csv`
- `outputs/brier_calibration_batch_report.md`
- `outputs/calibration_curve_batch.png`

The Kalshi-specific 48-hour branch uses:

```text
data/raw/kalshi/kalshi_recent_settled_markets.jsonl
    -> data/processed/kalshi_48h_candidates.csv
    -> data/raw/price_history/kalshi/*.json
```

with `outputs/kalshi_48h_candidate_report.md` and
`outputs/kalshi_48h_price_history_manifest.csv`.

### Retrospective multi-horizon analysis

```text
data/processed/markets_metadata_clean.csv
data/processed/kalshi_48h_candidates.csv
    -> data/raw/price_history_horizons/{polymarket,kalshi}/{24h,48h,168h}/*.json
    -> data/processed/p_hat_horizons.csv
    -> data/processed/p_hat_horizons_clustered.csv
    -> data/processed/p_hat_horizons_declustered_family_bin.csv
    -> bootstrap, liquidity, and category/venue outputs
```

The balanced-panel branch uses:

```text
data/processed/p_hat_horizons.csv
    -> data/processed/p_hat_horizons_balanced_panel.csv
    -> data/processed/p_hat_horizons_balanced_panel_declustered.csv
```

Associated V1 multi-horizon outputs currently live directly under `outputs/`
and include files prefixed with:

- `horizon_`
- `bootstrap_`
- `balanced_horizon_`
- `liquidity_robustness_`
- `category_venue_`

These artifacts use realized resolution or settlement-relative targets and
must remain labeled as legacy retrospective results.

## Methodology V2 transition file chain

The transition from retrospective anchors to occurrence anchors currently
follows this chain:

```text
data/processed/markets_metadata_clean.csv
data/raw/polymarket/polymarket_recent_closed_markets.jsonl
data/raw/kalshi/kalshi_recent_settled_markets.jsonl
    -> data/processed/market_resolution_anchor_audit.csv
    -> data/processed/markets_scheduled_event.csv
    -> data/processed/markets_deadline_window.csv
    -> data/processed/markets_anchor_unclear.csv
```

The more conservative family-review branch is:

```text
data/processed/markets_metadata_clean.csv + raw market metadata
    -> data/processed/resolution_anchor_contract_audit_v2.csv
    -> data/processed/resolution_anchor_contract_final.csv
    -> data/processed/markets_scheduled_absolute_final.csv
    -> data/processed/markets_scheduled_absolute_pending_verification.csv
    -> data/processed/markets_deadline_window_final.csv
    -> data/processed/markets_trigger_relative_final.csv
    -> data/processed/markets_excluded_anchor_final.csv
```

`data/manual/resolution_family_overrides.csv` is preserved as provenance for
this transition branch, not as the future authoritative V2 configuration.

The occurrence-anchor branch is:

```text
data/processed/markets_scheduled_absolute_final.csv
    -> data/raw/kalshi/events/*.json
    -> data/processed/kalshi_event_anchor_metadata.csv
    -> data/processed/markets_occurrence_anchor_all.csv
    -> data/processed/markets_fixed_clock_final.csv
    -> data/processed/markets_scheduled_event_start_final.csv
    -> data/processed/markets_scheduled_window_final.csv
    -> data/processed/markets_endogenous_subevent_final.csv
    -> data/processed/markets_deadline_window_from_occurrence_audit.csv
    -> data/processed/markets_occurrence_anchor_excluded.csv
```

The audit and corrected-manifest branch is:

```text
markets_fixed_clock_final.csv / markets_scheduled_event_start_final.csv
    -> data/processed/fixed_clock_occurrence_audit.csv
    -> data/processed/scheduled_event_occurrence_audit.csv
    -> data/processed/markets_fixed_clock_clean_candidates.csv
    -> data/processed/markets_scheduled_event_start_clean_candidates.csv
    -> data/processed/fixed_clock_horizon_manifest_clean.csv
    -> data/processed/scheduled_event_start_horizon_manifest_clean.csv
    -> data/processed/price_snapshot_targets_clean.csv
    -> data/processed/price_history_market_universe_clean.csv
```

The current production-shaped Kalshi extraction consumes
`data/processed/price_snapshot_targets_clean.csv` and is intended to produce:

- `data/raw/kalshi/candlesticks_clean/*.json`
- `data/processed/price_snapshots_clean.csv`
- `data/processed/price_snapshot_missing_clean.csv`
- `outputs/price_snapshot_extraction_report.md`

The pilot variant uses:

- `data/processed/price_snapshot_targets_pilot.csv`
- `data/raw/kalshi/candlesticks_pilot/*.json`
- `data/processed/price_snapshots_pilot.csv`
- `data/processed/price_snapshot_missing_pilot.csv`
- `outputs/price_snapshot_pilot_report.md`

## Existing raw cache locations

Current raw and cache locations include:

- `data/raw/polymarket/`
- `data/raw/kalshi/`
- `data/raw/price_history/polymarket/`
- `data/raw/price_history/kalshi/`
- `data/raw/price_history_horizons/polymarket/`
- `data/raw/price_history_horizons/kalshi/`
- `data/raw/kalshi/events/`
- `data/raw/kalshi/candlesticks_pilot/`
- `data/raw/kalshi/candlesticks_clean/` when generated

Raw responses are immutable research inputs. New network stages must be
cache-first and non-destructive; they must not overwrite an existing response
with a different payload.

## Existing processed outputs

Existing processed artifacts fall into these broad groups:

- cleaned market metadata;
- V1 48-hour candidates, prices, clusters, and de-clustered samples;
- V1 multi-horizon and balanced-panel samples;
- anchor audits, family reviews, and manual-override results;
- occurrence timing classifications and anomaly audits;
- corrected horizon manifests and price targets; and
- pilot candlestick snapshots and missing-snapshot records.

The exact current filenames remain under `data/processed/`. They must not be
renamed, rewritten, or moved during documentation or script archival.

## Artifacts that must remain untouched

Until an explicit migration is approved, do not modify or relocate:

- anything under `data/raw/`;
- anything under `data/processed/`;
- `data/manual/resolution_family_overrides.csv`;
- existing files under `outputs/`;
- raw request wrappers or their metadata and request URLs; or
- existing V1 and transition reports used to reconstruct methodology history.

## Proposed future V2 namespaces

Only newly generated V2 artifacts should eventually use namespaced paths.
Proposed namespaces are:

```text
data/processed/v2/
    anchors/
    timing/
    manifests/
    snapshots/
    analysis/

outputs/v2/
    diagnostics/
    manifests/
    snapshots/
    analysis/
```

If a future raw-cache namespace is needed, its design must preserve existing
raw files and support resumable, idempotent, date-partitioned operation. No
existing artifact should be moved into these namespaces without a separate
reviewed mapping and history-preserving migration plan.
