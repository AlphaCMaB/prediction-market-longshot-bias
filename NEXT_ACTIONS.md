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

## Phase 10F-B — approval gate and bounded smoke

1. Explicitly approve the analytical price definition. Recommendation:
   contemporaneous closing yes-bid/yes-ask midpoint as primary, actual trade
   close as a separate robustness measure, and no silent fallback mixing.
2. Confirm that 15 minutes remains the primary staleness threshold and 60
   minutes remains a separately reported robustness threshold for both price
   measures.
3. After approval, upgrade the candlestick cache to deterministic gzip, atomic
   write-once partitions, immutable manifests, bounded timestamps, and
   no-network resume. Validate the documented historical endpoint/cutoff before
   substantive acquisition.
4. Run only the pinned 200-family smoke after a fresh storage preflight. Measure
   requests, bytes, market existence, any pre-target observation, 15-/60-minute
   eligibility, field availability, retries, rate limits, and deterministic
   resume.
5. Stop again with measured production estimates and a specific archival
   request. Do not archive, move, delete, acquire the full scope, or access
   outcomes.

## Approval gates

Ask the project owner before changing the frozen methodology, deleting any
validated data, exceeding 5 GiB of generated data, allowing free space below
80 GiB, using credentials/private access, or choosing between research designs
with materially different interpretations.
