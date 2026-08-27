# Phase 10F final analysis plan

Status: recorded before outcome access; pending explicit outcome-quarantine
release and approval of the inferential specification below.

This plan fixes the intended analysis before any outcome is read. It does not
authorize an outcome join or calculation. Phase 10F-E prices, sample identities,
and design weights are immutable inputs.

## Scope and populations

The confirmatory scope is PR2-M scheduled-event-start markets only. PR1-M
anchors remain valid but are excluded because the approved historical price
source did not support a useful PR1 primary sample. PR1 and PR2 will not be
pooled.

Five populations remain distinct:

1. anchor-valid families;
2. families structurally observable at t−1h;
3. the frozen probability sample of 5,000 families and 11,573 contracts;
4. the price-observable sample under each named price rule; and
5. the later outcome-analysis sample containing only price-observable contracts
   with an allowed binary outcome.

The primary price-observable sample is the immutable midpoint <=15-minute
artifact: 9,388 contracts from 4,377 families. Missing prices do not invalidate
an anchor or sampling identity, and no contract or family may be replaced.
Inference is conditional on qualifying price observability; no observation-
propensity correction is approved.

## Frozen estimands and weights

For family `i`, eligible-contract count `M_i`, contract `j`, ex-ante probability
`P_ij`, and binary YES outcome `Y_ij`, define the calibration gap
`Z_ij = Y_ij - P_ij`.

The approved primary family-target estimand gives equal target mass to each
eligible family and then equal mass to contracts within family. It uses the
preserved `family_weight_raw` in a Hájek ratio. The approved secondary
contract-target estimand represents a uniformly selected eligible Kalshi
contract and uses `contract_weight_raw` in a separate Hájek ratio. The two
weight systems must never be mixed.

Both estimands describe the corresponding price-observable subset. Results may
not be generalized automatically to contracts without a qualifying price.

## Frozen price analyses

The confirmatory analysis uses the latest fully pre-target YES bid/ask midpoint
with staleness no greater than 15 minutes and no spread exclusion. Both sides
must exist, and no trade, previous-price, post-target, or alternative-price
fallback is allowed.

Predeclared robustness analyses remain separate:

- midpoint <=60 minutes;
- documented trade close <=15 minutes;
- documented trade close <=60 minutes;
- primary midpoint restricted to spread <=0.20; and
- primary midpoint restricted to spread <=0.10.

No robustness definition may be promoted to primary after outcomes are read.

## Proposed favorite–longshot reporting specification

The initial report will show design-weighted mean price, realized YES rate, and
`Y-P` calibration gap in fixed 0.10 probability bins. Bins are left-closed and
right-open from `0.0-0.1` through `0.8-0.9`; `0.9-1.0` includes 1.0. These bin
boundaries are fixed before outcome access.

The proposed scalar favorite–longshot contrast is:

`Delta_FL = mean(Y-P | P < 0.20) - mean(Y-P | P >= 0.80)`.

A negative value is directionally consistent with stronger overpricing among
longshots than favorites. It must not be described as causal, and the sign
alone is insufficient without its uncertainty interval and the full calibration
profile. The family-target contrast is confirmatory; the contract-target
contrast is secondary. Overall mean `Y-P`, calibration intercept/slope, and
Brier score are secondary diagnostics rather than substitutes for the
predeclared contrast and bin profile. Returns and Kelly calculations are out of
scope for the initial estimate.

This reporting specification is a recommendation recorded before outcomes. It
must receive explicit approval together with outcome-quarantine release; if it
is modified, the modification must be committed and re-audited before any
outcome access.

## Proposed uncertainty procedure

The recommended uncertainty procedure is a deterministic stratified
family-cluster bootstrap with 10,000 replicates. Within each original
anchor-month × family-size sampling stratum, sampled families are resampled as
clusters; all sampled contracts belonging to a selected family occurrence move
together. Each replicate recomputes the relevant Hájek estimator using only its
named weight system. Report two-sided 95% percentile intervals and the
bootstrap two-sided tail probability for the zero-gap or zero-contrast null.

The seed will be fixed in source before outcomes are accessed. No number of
replicates, interval type, model term, subgroup, or bin may be selected based on
results. This procedure is also pending explicit approval at the outcome-access
gate.

## Support and reporting gates

Overall confirmatory inference requires at least 500 contributing families and
family-weighted ESS at least 500. Subgroup inference requires at least 200
families and ESS at least 150. Each probability bin requires at least 100
families and ESS at least 100. Cells below their gate may be reported only as
low-support descriptive results.

The final pre-outcome audit determines support using prices only. These gates
may not change after outcome access. Outcome attrition must be reported
separately and cannot trigger replacement, threshold relaxation, or sample
redrawing.

## Future outcome join contract

Only after explicit authorization, create a minimal quarantined projection
containing exact market ticker and the allowed binary result. Map `yes` to 1
and `no` to 0 under the frozen StudyRules vocabulary. Join one-to-one by exact
ticker to the already frozen price artifacts. Do not import settlement times,
settlement values, close or expiration times, post-anchor prices, or unrestricted
raw outcome records into research features.

Reject duplicate, missing-identity, nonbinary, or conflicting outcome rows
without replacement. Report outcome availability as a new attrition layer.
Before any estimate, hash the minimal outcome projection and the joined sample,
rerun recursive quarantine checks, and verify that price inclusion, prices,
weights, bins, and all Phase 10F-E hashes are unchanged.

## Interpretation and stopping rules

The confirmatory conclusion is PR2-specific and conditional on observable
t−1h quotes. Category comparisons are unavailable because the confirmatory
sample is Sports only. Month and family-size results must follow their support
gates and remain secondary. Differences between observable and missing-price
contracts are a limitation, not evidence of no bias.

No claim for or against favorite–longshot bias, calibration, predictive
accuracy, or profitability is permitted until the outcome join is explicitly
approved and the complete frozen procedure is run. Any proposed change after
outcome access requires a clearly labeled exploratory analysis and cannot
replace the confirmatory specification.
