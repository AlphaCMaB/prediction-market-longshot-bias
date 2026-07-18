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
python -m scripts.pipeline_v2.pull_kalshi_partitioned_metadata --help
python -m scripts.pipeline_v2.merge_kalshi_metadata_partitions --help
python -m scripts.pipeline_v2.prepare_kalshi_market_universe --help
python -m scripts.pipeline_v2.apply_anchor_verification --help
```

> **Safety notice:** `pull_kalshi_settled_metadata` is the legacy transaction
> implementation. Do not use it for an unbounded historical production run.
> Kalshi's historical-markets endpoint has no settlement-date parameters, so a
> nominal date envelope scans the complete historical archive. New historical
> work must use `pull_kalshi_partitioned_metadata`. The legacy command now
> rejects an unbounded historical run unless its compatibility-only opt-in is
> supplied explicitly; smoke-limited historical calls remain available.

Inspect the next bounded partition without network or file writes by pinning a
cutoff snapshot:

```bash
python -m scripts.pipeline_v2.pull_kalshi_partitioned_metadata \
  --start-date 2025-05-01 \
  --end-date 2026-07-16 \
  --raw-root data/pipeline_v2/market_acquisition/partitioned \
  --cutoff-snapshot /path/to/pinned-cutoff.json \
  --preflight
```

Each non-preflight invocation acquires at most one configured cursor partition.
Raw pages use deterministic gzip, every publication is guarded by the 5 GiB
namespace ceiling and 80 GiB free-space floor, and metadata/outcomes are
normalized into separate artifacts before the partition commit is published.
The commit is the unit of resume. Historical filtering is local; live monthly
requests also carry server-side settlement bounds.

After every planned segment has a committed terminal cursor, merge with:

```bash
python -m scripts.pipeline_v2.merge_kalshi_metadata_partitions \
  --start-date 2025-05-01 \
  --end-date 2026-07-16 \
  --raw-root data/pipeline_v2/market_acquisition/partitioned \
  --cutoff-snapshot /path/to/pinned-cutoff.json
```

The merge fails closed with an immutable incomplete report until every chain is
terminal and reject-free. Selection among duplicate metadata variants uses only
outcome-free metadata and `updated_time`; outcomes remain quarantined and are
never used to select a variant.

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

`--limit-pages N` is an acquisition-only smoke mode. The limit applies to the
total number of newly requested market pages across the invocation; cutoff
requests and cache hits do not consume it. Successful cutoff snapshots, raw
pages, and operational manifest entries are preserved, but every affected
chain is marked incomplete and no audit, provenance, monthly output, staging
completion, or run commit is produced. Smoke mode exits with status `3` and
never prints `run_complete=true`. Removing the option on a later run allows
resume to reuse cached smoke pages and continue from their stored cursors.

Active metadata requests use the canonical production base URL
`https://external-api.kalshi.com`. After the smoke request budget is exhausted,
pagination continues through any valid cached cursor pages and later cached
segments. It stops at the first required cache miss without sending another
market request; cache-hit manifest and provenance records are still retained.

V2 measures prices relative to event times that were known ex ante rather than
relative to realized platform resolution or settlement timestamps. Current V2
results must not be presented as a Kalshi-versus-Polymarket comparison.

## Study boundary and outcome quarantine

The acquisition cohort consists of markets located because settled-market
metadata is available. It is not the analysis cohort. The analysis cohort is
selected later using verified ex-ante anchor times within the frozen half-open
study window. `settlement_ts` is retrieval and diagnostic metadata; it is never
the forecasting anchor.

The universe-preparation stage validates a completed acquisition commit and
then publishes separate immutable metadata and outcome files:

```bash
python -m scripts.pipeline_v2.prepare_kalshi_market_universe \
  --raw-root data/raw/kalshi/settled_markets \
  --output-dir outputs/v2/market_universe
```

`market_metadata.csv` is the only market-level input allowed for anchor
evidence, verification, timing, horizon, target, and candlestick stages. It
contains `settlement_ts` only under the diagnostic name
`diagnostic_settlement_ts`. `market_outcomes.csv` quarantines `result`,
settlement value, and the raw settlement timestamp. Active research-feature
interfaces reject outcome columns mechanically. Invalid or unusual result
values remain unchanged and receive an explicit status; they are not coerced
to yes or no.

Presence of an API `occurrence_datetime` or `strike_date` is candidate evidence,
not verification. A reviewed family decision must pass through
`apply_anchor_verification` before `build_occurrence_anchors` can create an
eligible anchor. The strict decision schema is:

```text
family_id,family_id_source,verification_status,verified_anchor_time,
verified_anchor_source,timing_structure,evidence_reference,review_note
```

Only `verified_automatic` and `verified_manual` can advance. `needs_review`,
`rejected`, and unmatched families remain explicitly ineligible. Allowed
sources are `verified_occurrence_datetime`,
`verified_official_scheduled_timestamp`, `validated_strike_date`, and
`manual_override`.
Decisions match markets by the composite `(family_id, family_id_source)` key;
the same identifier text in another namespace is independent and cannot
overwrite the market's original family source.
The same composite identity is used by family-level anchor validation,
clean/excluded membership, downstream family counts, and deterministic
ordering, so one namespace cannot invalidate another.

The required serialized order is:

1. settled metadata acquisition;
2. validate the acquisition commit;
3. prepare the metadata/outcome split;
4. freeze the metadata universe;
5. build candidate anchor evidence;
6. apply family anchor-verification decisions;
7. build verified occurrence anchors;
8. classify timing;
9. validate anchors;
10. construct eligible horizons within the anchor window;
11. construct targets;
12. extract pre-target prices;
13. freeze the research sample; and
14. merge outcomes last.

Thus `result` is unavailable to anchor, timing, target, and price stages.
Outcomes are merged only after `p_hat` and sample inclusion are frozen. The
canonical study-rule record carries a schema version and deterministic SHA-256
fingerprint so later reports can identify the exact frozen design.

The frozen Phase 9A window is exactly
`[2025-07-01T00:00:00Z, 2026-07-01T00:00:00Z)`. Its timing vocabulary is
exactly `fixed_clock` and `scheduled_event_start`, and its binary-result
vocabulary is exactly `yes` and `no`. These vocabularies are canonicalized in
that order before fingerprinting. Configuration changes, omissions, or extra
values are rejected.

`diagnostic_settlement_ts` can produce descriptive early-settlement audit
fields, but those fields never determine clean/excluded status, horizon
eligibility, or targets. Horizon eligibility depends only on an allowed timing
structure, a verified and family-valid ex-ante anchor, and a market opening at
or before the candidate target. It does not consult settlement, close, or
expiration timestamps.
The field is stripped before occurrence-anchor and timing research CSVs are
serialized and therefore cannot appear in horizon or target manifests.
Header-only research CSVs retain canonical schemas. An all-unverified cohort
therefore completes with deterministic empty clean, horizon, target, and
unique-market manifests instead of being treated as malformed input.

Both horizon construction and price-target selection load and validate the
configured `StudyRules`. The target stage receives that validated object
explicitly and cannot fall back to an unvalidated configuration window.

`--limit` is a smoke/inspection aid. The universe report records the requested
limit, pre-limit and output counts, omissions, and whether the resulting
universe is complete. Supplying a limit at or above the full count records
`limited_run=true` but leaves `universe_complete=true` and zero omissions.

Research provenance is distinct from acquisition provenance. The acquisition
commit retains and validates full page and response hashes. After validation,
`market_source_provenance.jsonl` records stable page/request identifiers and a
`research_metadata_sha256` computed from the outcome-free, non-diagnostic
metadata projection; it does not copy outcome-dependent raw-page hashes into
the frozen research identity. `event_tickers.csv` contains only
`event_ticker`, `contract_count`, and `first_open_time`.

## Phase 9B-A: Kalshi event candidate evidence

For the Phase 10C production universe, use the bounded compressed implementation:

```console
python -m scripts.pipeline_v2.pull_kalshi_partitioned_event_metadata \
  --event-tickers data/pipeline_v2/market_acquisition/partitioned/merged_universes/6f8aa42abec876d3aa1f6336/event_tickers.csv.gz \
  --raw-root data/pipeline_v2/market_acquisition/partitioned \
  --expected-merge-id 6f8aa42abec876d3aa1f6336 \
  --expected-event-ticker-sha256 544b5464f7afa01d8d9fa4148db1a6fee07a5fbf6265554db314c771b818cc45 \
  --preflight
```

The production path streams gzip input, uses independently committed event
partitions, stores immutable gzip raw pages, applies recursive outcome
quarantine before normalized publication, and produces only compressed final
universes. Preflight is network-free and write-free. Limited runs have distinct
scope identities, and incomplete or missing-event scopes cannot publish a final
universe. See `DATA_RUNBOOK.md` for smoke and continuation commands.

The original single-run interface below remains for bounded tests and legacy
small acquisitions; it is not the Phase 10C production path.

Run the event-metadata acquisition in module form:

```console
python -m scripts.pipeline_v2.pull_kalshi_event_metadata \
  --event-tickers outputs/v2/event_tickers.csv \
  --output-root data/raw/kalshi/event_metadata \
  --config configs/pipeline_v2.toml
```

The stage calls `https://external-api.kalshi.com/trade-api/v2/events` in
deterministic batches of at most 200 tickers, with nested markets disabled and
milestones enabled. It follows cursors, preserves immutable raw responses,
supports cache-first resume, and uses a final acquisition commit as the
transaction boundary. Missing requested events remain explicit and prevent
the report from claiming a complete universe. Supplying `--limit-events`
records that the run was limited; actual truncation also makes the universe
incomplete. Dry-run performs no requests and writes nothing.

Event tickers must use uppercase alphanumeric groups separated by single
hyphens or periods. Leading, trailing, and internal whitespace is rejected
without repair. Tickers are validated before sorting, limiting, batching, or
comma serialization. HTTP redirects are disabled and every 3xx response is rejected;
production requests remain fixed to the documented `external-api.kalshi.com`
endpoint.

The normalized outputs are `event_metadata.csv`, `event_milestones.csv`,
`event_source_provenance.jsonl`, and `event_metadata_report.json`. API
`strike_date`, milestone dates, titles, categories, and settlement-source
references are candidate evidence only. They are not verification and do not
select an anchor. Results, settlement values and times, and market close or
expiration times remain quarantined. The Phase 9A
`apply_anchor_verification` handoff is still mandatory before eligibility.

## Phase 10D: family anchor-evidence review

Build deterministic review artifacts in module form:

```console
python -m scripts.pipeline_v2.build_kalshi_anchor_evidence \
  --market-metadata data/pipeline_v2/market_acquisition/partitioned/merged_universes/6f8aa42abec876d3aa1f6336/market_metadata.csv.gz \
  --event-metadata data/pipeline_v2/market_acquisition/partitioned/event_metadata_acquisition/merged_event_universes/69f1b1277bdfdbd530834fe6/event_metadata.csv.gz \
  --event-milestones data/pipeline_v2/market_acquisition/partitioned/event_metadata_acquisition/merged_event_universes/69f1b1277bdfdbd530834fe6/event_milestones.csv.gz \
  --output-root data/pipeline_v2/anchor_evidence/phase_10d \
  --guard-root data/pipeline_v2 \
  --config configs/pipeline_v2.toml
```

Production-sized inputs must also be pinned with the three approved input-
artifact SHA-256 values recorded in `DATA_RUNBOOK.md`. `--dry-run` performs
the full streaming derivation and reports exact output byte and hash estimates
without publishing files. Large market metadata is compacted to sufficient
family-level rows while streaming; equivalent market occurrence evidence is
represented once, with `supporting_source_count` preserving the number of
supporting contracts.

Publication is atomic and guarded by both the 5 GiB generated-namespace ceiling
and the 80 GiB minimum-free-space floor. Re-running against an existing complete
output fully re-derives and compares the report, sizes, and hashes without
rewriting any file.

The review flow is:

```text
market_metadata.csv + event_metadata.csv + event_milestones.csv
→ anchor_evidence.csv
→ anchor_family_review.csv
→ human or separately approved review
→ anchor_verification_decisions.csv
→ apply_anchor_verification
```

Candidate evidence is not verification, and API timestamp presence is not
verification. Only market `occurrence_datetime`, event `strike_date`, and
milestone `milestone_start_date` may become candidates. Date-only strike
evidence remains date-only and is never converted to midnight or assigned a
timezone. Update timestamps, milestone end dates, market open/close/expiration
times, settlement metadata, and outcomes are not anchor candidates. The
generated decisions template leaves every composite family at `needs_review`
with all verified fields blank, so the explicit Phase 9A verification handoff
remains mandatory.

The completed production checkpoint contains 427,090 composite families and
625,923 candidates. Of those families, 418,591 have at least one candidate and
8,499 have none; 198,686 have multiple distinct exact candidate times requiring
review. All 625,923 candidates remain `needs_review`, zero anchors are verified,
and outcomes remain unmerged. Exact sizes, hashes, and the rerun command are
recorded in `DATA_RUNBOOK.md`. The next phase is the separate review and
verification handoff, not horizon-price or bias estimation.

## Phase 10E: staged outcome-blind verification audit

`build_phase_10e_verification_design` profiles Phase 10D evidence patterns,
assigns proposed audit tiers, and draws a deterministic stratified review
sample. Its outputs are a pattern table, outcome-blind CSV and Markdown review
packets, an exact-schema blank decisions template, and a design report. The
command and pinned production hashes are recorded in `DATA_RUNBOOK.md`.

Tier and rule labels are proposals only. The builder always reports zero
verified anchors, zero outcome merges, zero horizon prices, and zero network
requests. Packet context is source-specific and safelisted; retrospective,
outcome, price, close/expiration, settlement-value, and update fields are not
published. An existing complete packet is re-derived and compared without
rewriting it.

The production design proposes 102,413 Tier 1 families, 93,997 Tier 2 families,
and 230,680 Tier 3 families. Its audit contains 150 families per tier. No
deterministic rule may be applied until the packet is reviewed, weighted
approval and disagreement diagnostics are computed, and the project owner
explicitly approves the rule.

`build_phase_10e_first_review` performs the recommendation-only AI pass over
the approved 450-case packet. It emits a complete first-review CSV, a compact
report, a 165-case human-review subset, and a matching uncertainty/disagreement
queue. Every output remains `needs_review`; the command cannot promote a rule
or verify an anchor. The exact production invocation and hashes are recorded in
`DATA_RUNBOOK.md`.

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

The settled-metadata client treats timeouts, connection failures, truncated
chunked bodies, and content-decoding failures as transport retries under the
configured retry and backoff limits. A page is published atomically only after
the complete response body has decoded and passed schema validation; failed
partial bodies never advance a cursor or become cache hits.

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
