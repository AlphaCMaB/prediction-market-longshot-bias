# Methodology V2 Pipeline

This directory contains the concise methodology V2 production pipeline.
Methodology V2 is initially Kalshi-only.

## Invocation

Run every pipeline stage from the repository root using Python's module form:

```bash
python -m scripts.pipeline_v2.build_occurrence_anchors --help
python -m scripts.pipeline_v2.classify_timing --help
python -m scripts.pipeline_v2.validate_anchors --help
python -m scripts.pipeline_v2.build_horizon_manifest --help
python -m scripts.pipeline_v2.build_price_target_manifest --help
python -m scripts.pipeline_v2.extract_kalshi_candlesticks --help
python -m scripts.pipeline_v2.pull_kalshi_settled_metadata --help
```

Direct path execution such as `python scripts/pipeline_v2/<stage>.py` is not a
supported invocation style. The stages use repository-root package imports and
do not mutate `sys.path`.

For example, inspect the annual settled-metadata plan without network or file
activity using:

```bash
python -m scripts.pipeline_v2.pull_kalshi_settled_metadata \
  --start-date 2025-07-01 \
  --end-date 2026-06-30 \
  --raw-root /tmp/kalshi-metadata-dry-run/raw \
  --manifest /tmp/kalshi-metadata-dry-run/manifest.jsonl \
  --page-size 1000 \
  --dry-run
```

Settled-metadata raw pages are immutable. Historical page caches are scoped to
the pinned cutoff snapshot and reused across requested analysis ranges; range
filtering occurs locally. Missing or invalid `ticker`/`settlement_ts` values
produce an immutable fatal audit and prevent monthly publication. Resolved
duplicate-payload conflicts likewise require an immutable audit containing all
variants and the selected winner before the monthly artifact is published.
Unresolved conflicts fail without publishing a completed monthly artifact.

For multi-month ingestion, immutable artifacts may exist before a run finishes,
but they are not complete or visible pipeline inputs until the single run commit
record references every monthly file and audit by path and hash. Planning,
consolidation, validation, and destination-conflict checks happen before this
commit boundary. Resume may reuse byte-identical orphaned artifacts, but never
treats them as completed without a valid commit record.

Every fetched market keeps page-level provenance separate from its untouched
raw payload, including the request identity, cursor hashes, page path and hash,
endpoint tier, cutoff, and applicable range. Responses containing sensitive
credential-like fields are rejected before hashing or cache publication. All
immutable raw pages, snapshots, derived artifacts, audits, and run commits are
installed from flushed same-directory temporary files using an atomic
no-replacement operation.

Cutoff responses receive the same sensitive-field rejection before schema
validation, hashing, storage, or routing. Logical commit bytes contain no wall
clock value and are deterministic for a canonical effective configuration and
artifact set, so identical concurrent publishers safely converge on the same
commit. The effective configuration records page size, retry and backoff
settings, request rate and timeout, MVE filter, resume and endpoint modes, page
limit, normalized date bounds, selected-month restriction, cutoff identity,
routing plan, and interpretation-schema version.

Run commits reference page provenance independently of record provenance.
Consequently terminal empty pages and cached pages remain required evidence of
cursor exhaustion. Commit validation verifies each page file hash, embedded
response hash, and the presence of a terminal page for every endpoint chain.

Pinned cutoff snapshots are untrusted input and undergo the identical recursive
sensitive-field check before schema validation, hashing, routing, or logging.
Endpoint modes must cover every historical and live segment required by each
selected month; skipped required modes and page-limited runs cannot commit.
After atomic commit installation, the final path is reloaded and fully validated
before success is reported.

Each monthly raw-market JSONL has a separate immutable record-provenance JSONL.
It maps every selected ticker and payload hash to all originating pages,
including identical duplicates and historical/live overlap, while leaving raw
payloads unchanged. The provenance file identifies the selected variant, lists
all variant sources, references its monthly output path and hash, and is itself
required and hash-verified by the run commit.

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
