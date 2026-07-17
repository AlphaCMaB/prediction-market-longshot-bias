# Next Actions

Last updated: 2026-07-17

## Phase 10A-R — completed

All architecture, safety, normalization, merge, reporting, and validation work
is implemented. Acceptance result: 614 tests passed with no production data or
network requests.

## Phase 10A-S — completed

The bounded smoke test passed with 2 requests, 0 retries, 80,059 compressed raw
bytes, 1,000 in-range rows, 0 rejects, a valid nonterminal partition commit,
correct resume state, and a fail-closed incomplete merge.

## Phase 10B — completed

All historical and live segments are terminal. The validated merge contains
9,861,209 outcome-free market rows and 427,090 event tickers. Outcomes remain
separate and quarantined.

## Phase 10C — exact next autonomous phase

1. Extend the shared CSV reader to stream `.csv.gz` inputs without materializing
   the 427,090-row event universe unnecessarily.
2. Add a bounded event-metadata preflight covering requests, estimated bytes,
   maximum namespace size, minimum free disk, resumability, and incomplete-run
   reporting.
3. Run offline tests and a small bounded event-metadata smoke before production.
4. Acquire event metadata only after that acceptance passes.
5. Stop after event metadata validation; do not begin anchor verification and
   do not expose the quarantined outcome artifact to research-feature stages.

## Approval gates

Ask the project owner before changing the frozen methodology, deleting any
validated data, exceeding 5 GiB of generated data, allowing free space below
80 GiB, using credentials/private access, or choosing between research designs
with materially different interpretations.
