# Project Status

Last updated: 2026-07-17
Branch: `methodology-v2-clean`

## Research objective

Measure favorite-longshot bias in prediction markets without look-ahead bias or
outcome leakage. Methodology V2 is currently Kalshi-only. The frozen analysis
anchor window remains `[2025-07-01T00:00:00Z, 2026-07-01T00:00:00Z)` and the
`StudyRules` fingerprint remains
`12d6955f57b50b5587fdadf02b2bc96e7de48d022c9ac3cc2fe0425d907b9901`.

## Completed work

- Phase 9A: frozen study rules, strict outcome quarantine, verified-anchor
  handoff, horizon and price-target construction, and empty-universe behavior.
- Secure settled-market ingestion foundation: immutable raw pages, cutoff
  snapshots, retries, cursor validation, resumability, provenance, atomic
  publication, and deterministic commits.
- Phase 9B-A: secure event metadata and milestone acquisition.
- Phase 9B-B: candidate anchor evidence and review artifacts. Every generated
  decision remains `needs_review`; no API timestamp is automatically verified.
- The truncated-response retry fix is committed as `ca2ca3e`.
- The incomplete 2025-05-01 through 2026-07-16 acquisition directory has been
  removed. No validated data was deleted during the 2026-07-17 audit.

## Acquisition incident finding

The historical acquisition did not enforce the requested settlement window on
the server. `segment_params()` adds `min_settled_ts` and `max_settled_ts` only
for the live endpoint. Kalshi's historical-markets endpoint does not offer date
filters; it accepts cursor/limit and ticker-, event-, or series-based filters.
The planner also collapses all historical months into one cursor chain.
This is confirmed by Kalshi's official
[historical-markets reference](https://docs.kalshi.com/api-reference/historical/get-historical-markets)
and [historical-data guide](https://docs.kalshi.com/getting_started/historical_data).

Consequently, the roughly 906 pages / 906,000 rows were an unbounded scan of
the global historical archive, followed by local date filtering. There is no
remaining raw manifest with which to measure the exact out-of-range share, but
the code and API contract are sufficient to establish the routing error. The
cursor-integrity protections make accidental repeated-page loops unlikely; no
evidence currently indicates that duplicate pagination caused the scale.

## Phase 10A-R completed

Phase 10A-R: disk-bounded, partitioned Kalshi settled-market acquisition.

Completed capabilities:

- deterministic gzip raw pages and immutable per-partition request manifests;
- bounded cursor partitions for historical data and bounded monthly segments
  for live data;
- independent validated partition commits and cursor-chain resume;
- configurable 5 GiB raw-root ceiling and 80 GiB minimum-free-space guard;
- read-only request/size preflight with an explicit unknown historical total;
- immediate separate metadata, outcome, provenance, and normalization outputs;
- deterministic outcome-independent cross-partition merge/deduplication;
- immutable incomplete-run/merge reports and final merge commits;
- atomic publication and final post-publication validation; and
- a default block on legacy unbounded historical acquisition.

Acceptance on 2026-07-17: 614 tests passed; Python compilation, unused-import
inspection, formatting checks for new modules, and `git diff --check` passed.
No network acquisition was run in this phase and the acquisition directory
remained empty.

## Phase 10A-S completed

The one-page bounded network smoke test passed on 2026-07-17 using the
provisional acquisition envelope
`[2025-05-01T00:00:00Z, 2026-07-17T00:00:00Z)`.

- Cutoff: `2026-05-18T00:00:00Z`.
- Requests: 2 successful (1 cutoff + 1 historical page), 0 retries.
- Rows: 1,000 input, 1,000 in range, 0 outside, 0 rejected.
- Raw page: 2,373,973 bytes uncompressed; 80,059 bytes gzip.
- Entire smoke root after reports: 339,295 bytes (356 KiB on disk).
- Partition commit: valid and nonterminal; resume preflight advanced to
  partition index 1 with the committed cursor hash.
- Quarantine: 1,000 metadata rows and 1,000 outcome rows; metadata contained no
  `result` or settlement-value field.
- Incomplete merge: exited with status 3, wrote an immutable incomplete report,
  and published no final universe.
- Guards: 256 MiB smoke ceiling and 80 GiB free-space floor remained intact.
- Smoke-review finding fixed: actual runs using an externally pinned cutoff now
  publish a validated immutable copy inside the guarded acquisition root;
  read-only preflight remains write-free. The full 614-test suite passed again.

Smoke artifacts are retained for audit under
`/private/tmp/kalshi-partitioned-smoke.IVGaQL`; no validated data was deleted.

## Current phase

Phase 10B: guarded production acquisition, one independently committed
partition at a time.

## Critical path

1. Acquire the required market universe partition by partition under the 5 GiB
   raw-data ceiling and 80 GiB free-space floor.
2. Validate the complete partition chain and deterministic merged universe.
3. Acquire event metadata for the resulting event-ticker universe.
4. Build anchor evidence and conduct the separate review/verification handoff.
5. Construct horizons and targets, acquire pre-target prices, freeze sample
   membership, and merge outcomes last.

## Current resource state

- Repository size: approximately 608 MiB.
- Available filesystem space: approximately 104 GiB.
- Safety floor: stop before available space falls below 80 GiB.
- Default raw acquisition budget: at most 5 GiB; any larger production run
  requires explicit approval.
- Phase 10A-R network requests: zero.
- Phase 10A-R acquisition data generated: zero bytes in the project acquisition
  directory; tests used temporary directories only.
- Phase 10A-S network requests: two; smoke data: 339,295 bytes in `/private/tmp`.
