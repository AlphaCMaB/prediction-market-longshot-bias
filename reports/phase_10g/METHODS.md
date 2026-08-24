# Methods

## Study design and ex-ante timing

The analysis uses a Kalshi-only prediction-market design with a frozen anchor window from July 1, 2025 through June 30, 2026. Resolution and settlement timestamps were not used as research anchors because they are retrospective: conditioning the pricing horizon on information recorded after the event would create look-ahead bias. Candidate anchors were instead constructed from outcome-blind event and market metadata and evaluated using only information available before the event. The verification process distinguished fixed-clock candidates (PR1-M) from scheduled-event-start candidates tied to one official milestone (PR2-M). Candidate rules were subjected to outcome-blind audit and independent validation before their modified forms were approved and applied.

The confirmatory analysis is restricted to PR2-M scheduled-event-start Sports markets. PR1-M anchors remain valid, but the approved historical price source yielded too little qualifying PR1 midpoint coverage for a comparable confirmatory analysis. The analysis therefore does not pool PR1-M and PR2-M markets.

## Horizon and price construction

For every verified family, the target was fixed at one hour before the verified event start. The primary probability measure is the latest fully pre-target YES bid/ask midpoint whose observation time is no more than 15 minutes before the target. Both sides of the quote were required. Post-target candles, previous-price fallbacks, and mixing between quote and trade measures were prohibited. Separate pre-specified robustness definitions used midpoint staleness up to 60 minutes, documented trade closes within 15 or 60 minutes, and primary midpoints restricted to bid-ask spreads no larger than 0.20 or 0.10.

## Sampling and estimands

The eligible PR2-M population contained 64,775 families. A deterministic stratified probability sample selected 5,000 families by verified-anchor month and family-size stratum. Within each selected family, at most three eligible contracts were sampled, yielding 11,573 contracts. Exact first- and second-stage inclusion probabilities were retained. The primary family-target estimator gives equal target mass to each eligible family and equal mass to sampled contracts within family; its preserved family weights are used in a Hajek ratio. A separately weighted contract-target estimator represents a uniformly selected eligible contract and is reported as secondary. The weight systems were not mixed, and no observation-propensity correction was added after price availability was observed.

For contract j in family i, the calibration gap is Y_ij - P_ij, where P is the frozen pre-event price and Y is the binary YES resolution. The pre-specified favorite-longshot contrast is the weighted mean gap among contracts with P < 0.20 minus the weighted mean gap among contracts with P >= 0.80. Under this definition, the classical favorite-longshot pattern predicts a negative contrast. The calibration profile uses ten fixed 0.10 probability bins.

## Outcome quarantine and inference

Outcomes remained quarantined until anchors, sample membership, prices, inclusion probabilities, weights, exclusions, and the analysis plan were frozen and audited. The released projection contains only contract identifier, frozen sample identifier, and binary outcome. It excludes settlement timestamps, settlement values, post-resolution metadata, and fields that could alter eligibility. The outcome join was performed in memory and fingerprinted before estimation; unresolved or nonbinary rows were retained as missing outcomes without replacement or adaptive reweighting.

Uncertainty uses the pre-specified deterministic stratified family-cluster bootstrap with 10,000 replicates. Families are resampled as clusters within anchor-month by family-size strata, and each replicate recomputes the named Hajek estimator. Reported intervals are two-sided 95% percentile intervals. Overall inference required at least 500 contributing families and family-weighted effective sample size of 500; subgroup and probability-bin thresholds were likewise frozen before outcome access.
