# Methodology V2 Pipeline

This directory is reserved for the future concise methodology V2 production
pipeline. Methodology V2 is initially Kalshi-only. No production Python stages
have been created or moved here during the documentation phase.

V2 measures prices relative to event times that were known ex ante rather than
relative to realized platform resolution or settlement timestamps. Current V2
results must not be presented as a Kalshi-versus-Polymarket comparison.

## Planned stages

### 1. Ex-ante occurrence anchors

Build candidate anchors from verified event metadata and rules. Anchor priority
is:

1. verified `occurrence_datetime`;
2. manually verified official scheduled timestamp;
3. semantically verified `strike_date`; and
4. manual override.

`close_time` is never an automatic event anchor. An unverified `strike_date`
is insufficient.

### 2. Timing classification

Assign each family exactly one timing structure:

- `fixed_clock`
- `scheduled_event_start`
- `scheduled_window`
- `deadline_window`
- `endogenous_subevent`
- `unclear`

Classification describes the contract's timing semantics. It does not itself
make a contract eligible for a particular price horizon.

### 3. Anchor validation

Record the anchor value, source, validation status, and review provenance.
Reject or quarantine inconsistent family anchors, impossible temporal orderings,
and ambiguous metadata before price targets are created.

### 4. Horizon eligibility

Calculate candidate horizons of 1, 6, 12, 24, and 48 hours, separately from
timing classification. Confirm that the market was open and priceable at each
candidate target.

The selected analyses are:

- Primary: `fixed_clock` at 1 hour.
- Exploratory: `scheduled_event_start` at 1, 6, and 12 hours.

Other candidate horizons are retained for sample-size diagnostics. The
`scheduled_window`, `deadline_window`, `endogenous_subevent`, and `unclear`
classes are classified but excluded from the current price-target analysis.

### 5. Price-target creation

Create an explicit target manifest containing the market, family, timing
structure, verified anchor, horizon, and target timestamp. Record both
`family_id` and `family_id_source` using this priority:

1. manual family override;
2. official venue event identifier, currently the Kalshi event ticker;
3. stable venue-specific derived identifier; and
4. normalized-title fallback.

### 6. Kalshi candlestick extraction

Fetch or reuse cached Kalshi candlesticks and select the latest usable price at
or before each target. Never use a post-target price.

- Main specification: staleness no greater than 15 minutes.
- Robustness specification: staleness no greater than 60 minutes.
- Diagnostics: retain all at-or-before snapshots and report staleness.

The threshold must be specified before analysis and must not be selected to
maximize a favorite-longshot result.

Future network stages must be resumable, idempotent, cache-first,
non-destructive, protected by retry and exponential backoff, explicit about
request counts, and restartable without redownloading completed data. Date
partitioning and dry-run or metadata-only modes should be supported where
practical.

### 7. Family-level analysis

Contracts belonging to one event family are not independent. Calibration,
uncertainty, bootstrap procedures, and sample-size reporting must operate at
the family level. Probability-bin reports must show the number of independent
families, not only the number of contracts.

## Venue boundary

Polymarket is outside the initial V2 production scope. It may be added later
only after an equivalent ex-ante anchor workflow and historical-price pipeline
have been independently validated.

## Artifact boundary

Existing raw data, processed data, and outputs remain in their current paths.
Only newly generated V2 artifacts should eventually use namespaced V2 paths,
as documented in `docs/refactor/OUTPUT_PATH_INVENTORY.md`.
