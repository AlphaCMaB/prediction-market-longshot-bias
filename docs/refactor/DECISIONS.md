# Methodology V2 Refactor Decisions

This document records the approved boundaries for refactoring the repository.
It describes future work; it does not authorize moving scripts or artifacts.

## Repository stages

The repository preserves three stages of work:

1. Original ingestion, cleaning, and exploratory analysis.
2. Exploratory methodology V1 using realized resolution or settlement times.
3. The transition to methodology V2 using anchors known before an event.

Methodology V1 remains reproducible and computationally unchanged, but its
results are exploratory. Methodology V2 is the primary framework for a
tradable, leakage-resistant analysis.

## Scripts 01–03

Scripts 01–03 are runnable stages, not common utility modules. Keep them
unchanged in their current locations for now.

Later work will:

- preserve their current implementations as historical entry points;
- extract reusable helpers into `scripts/common/`; and
- build clean runnable V2 stages under `scripts/pipeline_v2/`.

The scripts themselves must not simply be renamed or moved into
`scripts/common/`.

## Initial venue scope

Methodology V2 is initially Kalshi-only. Kalshi has the validated
occurrence-anchor workflow, and the current V2 candlestick extraction is
Kalshi-specific.

Polymarket may be added later as a separate extension only after an equivalent
ex-ante anchor and historical-price pipeline has been validated. Current V2
results must not be presented as a Kalshi-versus-Polymarket comparison.

## Anchor priority

Use the following priority:

1. Verified `occurrence_datetime`.
2. A manually verified scheduled timestamp from official event metadata or
   contract rules.
3. A verified `strike_date`, only when its semantic meaning is confirmed.
4. A manual override.

Never automatically treat `close_time` as the event anchor. A `strike_date`
without semantic verification is not sufficient.

Production records should distinguish the anchor timestamp, its source, and
its validation status.

## Timing classification and horizon eligibility

Timing classification and horizon eligibility are separate operations.

The eligibility pipeline may calculate candidate horizons of 1, 6, 12, 24,
and 48 hours.

Currently selected analysis horizons are:

- Primary: `fixed_clock` at 1 hour.
- Exploratory: `scheduled_event_start` at 1, 6, and 12 hours.

The 24-hour and 48-hour candidate horizons, and any unselected horizons for a
timing class, are retained only for sample-size diagnostics until enough
independent families exist.

The following timing structures must be classified but excluded from the
current price-target analysis:

- `scheduled_window`
- `deadline_window`
- `endogenous_subevent`
- `unclear`

They may later be studied using separate methodologies appropriate to their
timing structures.

## Snapshot selection and staleness

For each target, select the latest usable price at or before the target. Never
select a post-target price.

- Main specification: staleness no greater than 15 minutes.
- Robustness specification: staleness no greater than 60 minutes.
- Descriptive diagnostics: retain every at-or-before snapshot and report its
  staleness.

The staleness threshold must not be selected based on which threshold produces
the strongest favorite-longshot result.

## Family identifiers

Use the following family identifier priority:

1. Explicit manual family override.
2. Official venue event identifier: Kalshi event ticker for current V2, and a
   Polymarket event ID for a future extension.
3. Stable venue-specific derived identifier.
4. Normalized-title fallback.

Every record must contain both `family_id` and `family_id_source`. Statistical
uncertainty and sample-size interpretation must continue to operate at the
family level rather than treating related contracts as independent.

## Overrides

Preserve `data/manual/resolution_family_overrides.csv` as legacy provenance.
It is not the authoritative production V2 configuration.

A later phase may create a normalized V2 override file under `configs/` with:

- `family_id`
- `family_id_source`
- `timing_structure`
- `anchor_time`
- `anchor_source`
- `validation_status`
- `review_note`

That migration is outside the documentation phase.

## Existing artifacts and future namespaces

Do not move existing raw data, processed data, or outputs. Only newly generated
V2 artifacts should eventually use namespaced V2 paths. Historical artifacts
remain in their current locations until a separate inventory and migration
step is reviewed and approved.

## Methodology V1 reproducibility

V1 calculations must remain unchanged. When V1 scripts are eventually moved
with `git mv`, they must remain runnable from the repository root using their
new documented paths. Do not refactor their calculations during archival.

Both script-17 versions must be preserved as source and methodology history.
They do not need to be maintained as production pipeline stages.

## Future network stages

Future V2 network stages must be:

- resumable;
- idempotent;
- cache-first;
- non-destructive to existing raw responses;
- partitioned by date where practical;
- protected by retries and exponential backoff;
- restartable without redownloading completed data;
- explicit about API request counts; and
- capable of a dry-run or metadata-only mode where practical.

No large or automatic network pull is part of the refactor itself.
