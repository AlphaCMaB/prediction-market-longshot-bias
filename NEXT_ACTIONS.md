# Next Actions

Last updated: 2026-07-18

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

Implementation and offline acceptance are complete. The exact continuation is:

1. Commit and push only the Phase 10C source, tests, configuration, and project
   documentation.
2. Run the pinned production preflight with no network or writes. Require zero
   malformed/duplicate source tickers and estimates within both hard guards.
3. Run a separate `--limit-events 200 --partition-events 200` smoke scope.
4. Validate its immutable raw gzip page, terminal cursor, normalized research
   projection, partition commit, resume with zero redownloads, final compressed
   merge, hashes, missing-event count, and actual disk accounting.
5. Re-run the full production preflight using actual smoke bytes as an empirical
   reasonableness check. If it remains safe, run `--continue-all` for the full
   427,090-event scope.
6. Validate every event partition and the final merge. Stop after event metadata
   validation; do not begin anchor verification and do not expose quarantined
   outcomes to research-feature stages.

## Approval gates

Ask the project owner before changing the frozen methodology, deleting any
validated data, exceeding 5 GiB of generated data, allowing free space below
80 GiB, using credentials/private access, or choosing between research designs
with materially different interpretations.
