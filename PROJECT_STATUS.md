# Project Status

Last updated: 2026-07-18
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

Phase 10B completed on 2026-07-17 and remains immutable.

- Acquisition completeness: all four segments terminal and fully validated.
- Historical: 352 partitions, 8,778 requests/pages, 8,777,951 input records;
  7,370,758 in range and 1,407,193 outside the provisional envelope.
- Live: 102 partitions, 2,491 requests/pages, 2,490,451 input/in-range records
  across May, June, and July 2026.
- Total: 454 independently validated partition commits, 11,269 logical and
  successful HTTP requests, 11,268,402 input records, 9,861,209 in range,
  1,407,193 outside, 0 rejected, 0 retries, and 0 rate limits.
- Exact cross-partition audit: 9,861,209 distinct tickers, 0 duplicates, 0
  excess rows, and 0 metadata conflicts.
- Final merge: complete, deterministic, compressed, and outcome-independent;
  merge ID `6f8aa42abec876d3aa1f6336`.
- Final market universe: 9,861,209 contracts. Final event-ticker universe:
  427,090 events.
- Metadata remains outcome-free. Quarantined outcomes are a separate compressed
  artifact and have not been merged into research features.
- Guarded namespace: 3,648,491,736 bytes of the 5 GiB ceiling. Filesystem free
  space: 103,643,897,856 bytes, 17,744,551,936 bytes above the 80 GiB floor.
- Final acceptance: 617 tests passed; compilation, formatting, unused-import,
  and diff checks passed.

No anchor timestamp was verified or changed, and the frozen analysis window and
`StudyRules` fingerprint remain unchanged.

Phase 10C implementation completed offline on 2026-07-18.

- The compressed Phase 10B event-ticker artifact was revalidated at SHA-256
  `544b5464f7afa01d8d9fa4148db1a6fee07a5fbf6265554db314c771b818cc45`.
- Shared CSV input now streams plain or gzip files without materializing the
  427,090-row source universe.
- Event acquisition now uses deterministic 5,000-event partitions composed of
  batches of at most 200 tickers, immutable gzip raw pages, bounded cursor
  traversal within each batch, and independent validated partition commits.
- Resume reuses raw pages only after request/response hashes and cursor
  transitions validate. It never treats an uncommitted page as completion.
- Read-only preflight reports source totals, unique and malformed tickers,
  duplicates, batches, minimum requests, partitions, raw and normalized
  storage estimates, total namespace size, and free-space margin.
- Preflight accounts for the existing Phase 10B namespace plus both partition
  artifacts and the final compressed merge; actual publications retain the
  5 GiB namespace ceiling and 80 GiB free-space floor.
- Normalized event and milestone artifacts apply the recursive research
  projection before publication. Outcomes remain confined to immutable raw
  evidence, timestamps remain unverified candidate evidence, and no anchor
  verification is invoked.
- Complete scopes receive deterministic compressed event metadata, milestone,
  and provenance outputs. Any missing event, duplicate behavior change,
  cross-partition conflict, cursor problem, schema change, or incomplete
  partition blocks final publication.
- Offline acceptance: 627 tests passed before the documentation handoff;
  targeted event acceptance contains 87 passing tests. Compilation, pyflakes,
  Black formatting, and `git diff --check` passed.

## Critical path

1. Commit and push the reviewed Phase 10C implementation.
2. Run the pinned, read-only production preflight and evaluate the request and
   storage envelope against the hard guards.
3. Run one separately scoped smoke of at most 200 events and validate raw-page
   compression, resume, output hashes, missing-event behavior, and accounting.
4. If the smoke and refreshed production preflight pass, acquire and merge the
   full event universe under independently committed partitions.
5. Build anchor evidence without automatically verifying any timestamp.
6. Conduct the separate review/verification handoff.
7. Construct horizons and targets, acquire pre-target prices, freeze sample
   membership, and merge outcomes last.

## Current resource state

- Repository size: approximately 608 MiB.
- Available filesystem space: approximately 96.5 GiB at Phase 10B completion.
- Safety floor: stop before available space falls below 80 GiB.
- Default raw acquisition budget: at most 5 GiB; any larger production run
  requires explicit approval.
- Phase 10A-R network requests: zero.
- Phase 10A-R acquisition data generated: zero bytes in the project acquisition
  directory; tests used temporary directories only.
- Phase 10A-S network requests: two; smoke data: 339,295 bytes in `/private/tmp`.
- Phase 10B final: 11,269 successful market-page requests, 3,648,491,736
  guarded bytes, 454 valid partition commits, and one valid final merge commit.
