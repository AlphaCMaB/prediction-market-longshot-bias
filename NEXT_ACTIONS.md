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

## Phase 10F final audit — completed; explicit outcome-access gate

The final pre-outcome audit passes under identity
`bd14ba156585c4b2ed43c798ea55c977e8496326642edca9748eb703491eab24`.
All frozen identities, partition/raw hashes, price projections, temporal rules,
inclusion probabilities, weights, support gates, and quarantine checks were
re-derived without network or outcome access.

The next action requires one explicit methodological and access approval. The
owner must review `PHASE_10F_FINAL_ANALYSIS_PLAN.md` and either approve or
modify, before outcome access:

1. the family-target `Y-P` estimand as primary and contract-target version as
   secondary;
2. fixed 0.10 calibration bins;
3. proposed scalar contrast `mean(Y-P | P<0.20) - mean(Y-P | P>=0.80)`;
4. the deterministic 10,000-replicate stratified family-cluster bootstrap and
   95% percentile intervals; and
5. the minimal one-to-one binary-outcome projection and join contract.

If modified, commit the revised plan and rerun the audit before any outcome is
opened. If approved, separately release the outcome quarantine. Until both
actions occur, do not open outcome artifacts, merge results, compute bias,
calibration or Brier scores, redraw the sample, change price rules, add
observation-propensity weights, or pool PR1 and PR2.
