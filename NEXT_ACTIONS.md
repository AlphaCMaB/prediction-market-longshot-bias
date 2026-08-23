# Next Actions

Last updated: 2026-08-23

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

## Phase 10C — completed

Implementation, production preflight, corrected 200-event smoke, all 86
production partitions, and the deterministic final merge are complete.
Production retrieved all 427,090 events and published 208,598 milestone
associations with zero missing events or merge conflicts. Final no-network
resume, recursive quarantine inspection, hashes, disk accounting, and the
636-test acceptance suite pass.

## Phase 10D — completed

The complete outcome-free production universes produced 625,923 candidate rows
for 427,090 composite families. All candidate, family-review, and decision
rows remain `needs_review`; verified fields are blank, zero anchors were
verified, and zero outcomes were read. All four required outputs passed
deterministic rerun, quarantine, schema, hash, and storage validation.

## Phase 10E — completed

Modified PR1-M and PR2-M are explicitly approved and have been applied to the
complete outcome-blind universe. The result contains 98,625 PR1-M anchors and
69,329 PR2-M anchors: 167,954 verified families total. The other 259,136
families remain `needs_review`; none was rejected. Deterministic compressed
outputs and exclusion diagnostics are local and ignored. No price, horizon,
outcome, or network input was accessed.

## Phase 10F-A — completed; price definition approval required

The offline planner covers all 161,343 in-window verified families and
4,640,355 associated contracts. It identifies 112,166 families with a market
open by t−1h and 49,177 whose markets definitely opened too late. Zero are
unknown offline. The eligible 4,586,979 tickers project 58,468 minimum batched
candlestick requests.

The full acquisition does not fit current storage: empirical and conservative
additional-namespace estimates are 2.22 GB and 10.69 GB. The deterministic
200-family smoke projects 29,434,112 conservative bytes and fits the remaining
62,316,968-byte namespace headroom, but it is not authorized to run yet.

## Phase 10F-B — historical-route approval gate

1. Do not retry the 206 completed live-batch groups; their immutable commits
   prove that the live batch route returned zero markets for the archived
   smoke scope.
2. Obtain explicit approval for a newly pinned historical-endpoint validation
   sample. The documented historical endpoint is per ticker, so applying it to
   all 12,137 smoke tickers would materially change the request burden and is
   not authorized by the original 206-batch design.
3. Before any historical request, propose a compact deterministic ticker sample
   stratified across PR1/PR2, category, family size, and target month. Include
   paired exact-end/end-minus-one probes to establish candle boundaries.
4. If that validation succeeds, redesign production routing and recompute
   request/storage/time estimates. Do not infer the original 58,468 live-batch
   request estimate applies to archived histories.
5. Keep the primary midpoint, trade-close robustness, 15-/60-minute thresholds,
   outcome quarantine, and StudyRules unchanged. Do not archive or delete data
   without separate approval.

## Phase 10F-C — contract-sampling methodology gate

1. Obtain explicit approval or modification of the proposed primary
   family-weighted and secondary contract-weighted finite-population
   estimands. Do not draw the production sample before this gate.
2. Obtain explicit approval or modification of the 5,000-family, cap-three
   two-stage design and its rule × category × anchor-month × family-size
   strata, minimum allocation, seed, and exact inclusion weights.
3. Decide whether Phase 10F-D should proceed as a PR2-specific midpoint <=15m
   acquisition. Current evidence does not support midpoint <=15m as a
   cross-category primary measure because PR1 coverage was 0/135 in B2.
4. Decide whether PR1 should be deferred, restricted to a separately labeled
   60-minute midpoint feasibility study, or investigated using another
   explicitly approved ex-ante source. Do not change the frozen 15-minute
   primary rule by implication.
5. Approve the prespecified reporting gates (100 observable families and ESS
   100 for confirmatory subgroup reporting; 30 families and ESS 30 per
   probability bin) or replace them before outcomes are accessed.
6. Only after approval, implement a manifest-only deterministic production
   sample draw and conduct an offline acceptance review. Price acquisition,
   outcomes, pooling, spread exclusions, and strike-position enrichment remain
   separately gated.

## Approval gates

Ask the project owner before changing the frozen methodology, deleting any
validated data, exceeding 5 GiB of generated data, allowing free space below
80 GiB, using credentials/private access, or choosing between research designs
with materially different interpretations.
