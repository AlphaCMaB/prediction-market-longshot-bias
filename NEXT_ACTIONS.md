# Next Actions

Last updated: 2026-08-24

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

## Phase 10F-C — approved as modified

The owner froze separate family- and contract-weighted estimands conditional on
price observability, restricted confirmatory acquisition to PR2, approved 5,000
PR2 families with a three-contract cap, predeclared midpoint/trade and spread
sensitivities, and replaced the provisional reporting thresholds. PR1 anchors
remain valid but the current primary price source is not viable.

## Phase 10F-D — completed offline; Phase 10F-E approval gate

1. Review the deterministic 5,000-family/11,573-contract PR2 manifests, 51
   stratum allocations, exact inclusion probabilities, raw weights, hashes,
   and commit identity. Do not redraw identities after price or outcome access.
2. Obtain explicit Phase 10F-E network authorization before any of the 11,575
   projected requests. Re-run a fresh namespace/free-disk preflight immediately
   before acquisition because disk margin is declining independently of these
   compact artifacts.
3. If authorized, implement/resume immutable per-ticker acquisition only for
   the pinned contract manifest. Keep midpoint and actual trade close separate,
   reject post-target candles, and retain no-cutoff primary midpoint plus the
   predeclared $0.20/$0.10 spread sensitivities.
4. Stop if the namespace or 80 GiB disk guard would be crossed. Do not acquire
   PR1, pool rules, weaken 15-minute staleness, access outcomes, or change the
   sample.
5. After acquisition, report observability and ESS against the frozen gates:
   overall 500 families/ESS 500; subgroup 200 families/ESS 150; probability bin
   100 families/ESS 100. Missing prices remain valid anchors and are not given
   unapproved observation-propensity weights.

## Approval gates

Ask the project owner before changing the frozen methodology, deleting any
validated data, exceeding 5 GiB of generated data, allowing free space below
80 GiB, using credentials/private access, or choosing between research designs
with materially different interpretations.

## Phase 10F-E — completed; final pre-outcome sample audit

The frozen PR2 acquisition and all four outcome-blind analysis samples are
complete. The primary midpoint <=15m sample passes both frozen overall gates
with 4,377 observable families and family-weighted ESS 4,208.115. The sample,
weights, attrition classifications, spread diagnostics, request provenance,
and immutable hashes must now receive one final pre-outcome audit.

The next phase is the explicit outcome-access gate. Before any outcome join:

1. accept the final 10F-E acquisition and sample audit;
2. preserve commit identity
   `79e022b7d9d359b484632e82671ef0095eba040687a21bc9a34a9bb947cf08de`;
3. freeze the primary and robustness artifact hashes and the predeclared
   weighting/ESS rules;
4. obtain explicit authorization to read and join quarantined outcomes; and
5. keep unsupported subgroup or probability-bin results descriptive under the
   already frozen support thresholds.

Do not redraw failed or unobservable contracts, change staleness or spread
rules, reweight for price observability, pool PR1 with PR2, or access outcomes
without a new explicit authorization. Generated Phase 10F-E data remain local
and ignored; namespace headroom is only 20,644,964 bytes.
