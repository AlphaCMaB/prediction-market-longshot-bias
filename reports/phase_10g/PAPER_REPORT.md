# Mentor-facing executive summary

The project has completed its first fully pre-specified favorite-longshot analysis. The design is deliberately outcome-blind through sample and price construction: event anchors were verified from ex-ante metadata, the t-1h sample and weights were frozen, and only then was a minimal binary-outcome projection released.

The confirmatory sample contains 9,353 resolved contracts from 4,360 scheduled-event-start Sports families. The primary family-weighted longshot-minus-favorite calibration contrast is 0.01004, with a 95% family-cluster bootstrap interval from -0.02358 to 0.04565. Because the classical pattern predicts a negative contrast and this interval includes zero, the appropriate conclusion is: **in the pre-specified PR2 Sports sample, we detect no statistically distinguishable evidence of favorite-longshot bias.**

All pre-specified robustness intervals also include zero, including alternative staleness windows, trade-close prices, and two spread restrictions. The result is nevertheless conditional on price observability and should not be generalized to all Kalshi markets. The main paper limitation is that fixed-clock PR1 markets lacked sufficient historical-price coverage under the frozen source, and observable PR2 contracts differed compositionally from unavailable-price contracts.

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

# Results

## Primary sample and outcome coverage

The frozen probability sample contained 11,573 contracts from 5,000 PR2-M Sports families. Binary outcomes were available for 11,495 contracts; 78 were unresolved or nonbinary and were retained as missing without replacement. Family-target and contract-target weighted resolution coverage were 99.21% and 99.46%, respectively. The primary price rule yielded 9,388 observable contracts from 4,377 families. Of these, 9,353 contracts and 4,360 families contributed to the primary outcome analysis.

## Overall calibration and favorite-longshot contrast

Under the primary family target, the weighted mean predicted probability was 0.4595 and the weighted realized YES frequency was 0.4623. The weighted calibration gap, Y-P, was 0.00285 (95% CI [-0.00333, 0.00906]). The primary family-weighted Brier score was 0.19876; this is a secondary descriptive measure.

The pre-specified longshot-minus-favorite contrast was 0.01004 (95% CI [-0.02358, 0.04565]). The classical favorite-longshot pattern predicts a negative contrast under the frozen definition. The point estimate was small and positive, and the confidence interval included zero. Thus, in the pre-specified PR2 Sports sample, we detect no statistically distinguishable evidence of favorite-longshot bias.

## Calibration profile and robustness

All ten fixed probability bins passed the pre-specified family-count and effective-sample-size gates. Their weighted calibration gaps do not display a monotonic pattern from longshots to favorites. 1 of the ten descriptive bin intervals excludes zero. Because all ten bins are shown, the bin was not a standalone pre-specified effect, and the profile is not monotonic, this isolated interval is not interpreted as evidence of a distinct pricing effect.

All five pre-specified family-weighted robustness contrasts had confidence intervals that included zero. The midpoint <=60-minute contrast was 0.00259 (95% CI [-0.02855, 0.03502]); the trade-close contrasts were 0.00607 [-0.03703, 0.05149] at 15 minutes and 0.00777 [-0.02906, 0.04551] at 60 minutes. Restricting the primary midpoint to spreads <=0.20 produced 0.00908 [-0.02500, 0.04491], while the <=0.10 restriction produced 0.00807 [-0.02630, 0.04453]. The secondary contract-target estimate was 0.02912 (95% CI [-0.01530, 0.07528]). These estimates agree qualitatively with the primary finding and do not replace it.

## Observability and missingness

Primary price exclusions comprised 928 contracts with no pre-target candle, 1,256 with a valid midpoint only under the 15-to-60-minute robustness window, and one preserved API/data failure. Pre-outcome diagnostics showed that price observability was not compositionally neutral: the family-weighted standardized difference in hours since market open was 0.257, and the largest absolute observed-versus-unavailable weighted-share differences were 0.197 by family-size bin, 0.123 by anchor month, and 0.027 by target UTC hour. These differences limit generalization beyond contracts with qualifying pre-target quotes.

## Table 1 Sample Construction

| Stage | Component | Count | Interpretation |
| --- | --- | --- | --- |
| 1 | Source event-family universe | 427090 | Complete Kalshi event universe before anchor screening |
| 2 | Verified ex-ante anchors | 167954 | PR1-M and PR2-M rules; not an attrition estimate |
| 3 | Verified anchors in frozen window | 161343 | Anchor time within the pre-specified analysis window |
| 4 | Structurally eligible at t-1h | 112166 | At least one associated market existed by the target |
| 5 | PR2 eligible family population | 64775 | Confirmatory scheduled-event-start Sports population |
| 6 | Frozen sampled families | 5000 | Probability sample drawn before price and outcome access |
| 7 | Frozen sampled contracts | 11573 | At most three contracts per sampled family |
| 8 | Primary price-observable contracts | 9388 | Valid midpoint at t-1h with no more than 15-minute staleness |
| 9 | Primary binary-resolved contracts | 9353 | Price-observable contracts with yes/no resolution |
| 10 | Primary binary-resolved families | 4360 | Families contributing at least one resolved primary contract |

## Table 2 Primary And Robustness

| Specification | Target | Contracts | Families | Family ESS | Weighted Y-P | Y-P 95% CI | Longshot-favorite contrast | Contrast 95% CI |
| --- | --- | --- | --- | --- | --- | --- | --- | --- |
| Midpoint <=15m (primary) | Family | 9353 | 4360 | 4191.3 | 0.00285 | [-0.00333, 0.00906] | 0.01004 | [-0.02358, 0.04565] |
| Midpoint <=60m | Family | 10592 | 4711 | 4620.6 | 0.00326 | [-0.00260, 0.00925] | 0.00259 | [-0.02855, 0.03502] |
| Trade close <=15m | Family | 5431 | 3163 | 2812.0 | -0.00520 | [-0.01238, 0.00206] | 0.00607 | [-0.03703, 0.05149] |
| Trade close <=60m | Family | 7177 | 3836 | 3504.8 | -0.00486 | [-0.01135, 0.00173] | 0.00777 | [-0.02906, 0.04551] |
| Midpoint <=15m; spread <=0.20 | Family | 8948 | 4184 | 4009.6 | 0.00248 | [-0.00373, 0.00882] | 0.00908 | [-0.02500, 0.04491] |
| Midpoint <=15m; spread <=0.10 | Family | 8677 | 4093 | 3904.7 | 0.00233 | [-0.00401, 0.00874] | 0.00807 | [-0.02630, 0.04453] |
| Midpoint <=15m (secondary contract target) | Contract | 9353 | 4360 | 1290.1 | 0.00737 | [-0.00858, 0.02342] | 0.02912 | [-0.01530, 0.07528] |

## Table 3 Calibration Deciles

| Probability range | Weighted mean probability | Weighted observed frequency | Weighted Y-P | Y-P 95% CI | Unique families | Family ESS | Frozen gate |
| --- | --- | --- | --- | --- | --- | --- | --- |
| 0.0-0.1 | 0.0516 | 0.0403 | -0.0113 | [-0.0261, 0.0049] | 523 | 482.7 | Pass |
| 0.1-0.2 | 0.1520 | 0.1723 | 0.0203 | [-0.0048, 0.0462] | 776 | 721.9 | Pass |
| 0.2-0.3 | 0.2531 | 0.2667 | 0.0137 | [-0.0089, 0.0368] | 1219 | 1123.5 | Pass |
| 0.3-0.4 | 0.3502 | 0.3838 | 0.0337 | [0.0070, 0.0613] | 1110 | 1019.5 | Pass |
| 0.4-0.5 | 0.4507 | 0.4610 | 0.0103 | [-0.0167, 0.0377] | 1358 | 1214.0 | Pass |
| 0.5-0.6 | 0.5479 | 0.5393 | -0.0086 | [-0.0360, 0.0181] | 1324 | 1190.0 | Pass |
| 0.6-0.7 | 0.6486 | 0.6290 | -0.0196 | [-0.0525, 0.0129] | 909 | 858.6 | Pass |
| 0.7-0.8 | 0.7472 | 0.7168 | -0.0304 | [-0.0647, 0.0024] | 722 | 691.9 | Pass |
| 0.8-0.9 | 0.8444 | 0.8293 | -0.0151 | [-0.0502, 0.0181] | 482 | 465.1 | Pass |
| 0.9-1.0 | 0.9476 | 0.9625 | 0.0149 | [-0.0076, 0.0347] | 302 | 298.8 | Pass |

## Table 4 Missingness Observability

| Domain | Metric | Value | Interpretation |
| --- | --- | --- | --- |
| Outcome availability | Binary-resolved sampled contracts | 11495 | Allowed yes/no outcome available |
| Outcome availability | Unresolved/nonbinary sampled contracts | 78 | Retained as missing; no replacement |
| Outcome availability | Family-target weighted resolution coverage | 99.21% | Design-weighted coverage |
| Outcome availability | Contract-target weighted resolution coverage | 99.46% | Design-weighted coverage |
| Primary outcome sample | Resolved price-observable contracts | 9353 | Contribute to primary analysis |
| Primary outcome sample | Families with a resolved primary contract | 4360 | Contribute to primary analysis |
| Primary price exclusion | No pre-target candle | 928 | No eligible candle at or before target |
| Primary price exclusion | Midpoint only within 15-60m | 1256 | Eligible for 60-minute robustness only |
| Primary price exclusion | API/data failure | 1 | Preserved failure; no replacement |
| Observability balance | Hours-since-open standardized difference | 0.257 | Observed versus unavailable-price contracts |
| Observability balance | Maximum family-size share difference | 0.197 | Absolute weighted-share difference |
| Observability balance | Maximum month share difference | 0.123 | Absolute weighted-share difference |
| Observability balance | Maximum target-hour share difference | 0.027 | Absolute weighted-share difference |

# Discussion

The confirmatory analysis does not detect the classical favorite-longshot pattern in the frozen PR2-M Sports sample. The primary longshot-minus-favorite estimate is small relative to its uncertainty, and all pre-specified robustness intervals include zero. The agreement across midpoint, trade-close, staleness, and spread definitions reduces concern that the conclusion is driven by one particular price construction. It does not, however, prove that the true contrast is exactly zero.

This result is narrower than a venue-wide statement about Kalshi. The estimand pertains to scheduled-event-start Sports families that passed the modified PR2 verification rule, entered the frozen probability sample, had a valid observable price one hour before the event, and had a binary resolution available. Fixed-clock PR1-M markets could not support a comparable confirmatory analysis under the frozen historical-price source because qualifying midpoint coverage was too sparse. Other categories, horizons, platforms, and unobservable contracts may have different pricing patterns.

The result can be viewed alongside evidence from conventional betting markets, where favorite-longshot bias is often studied using realized returns or bookmaker odds. Prediction-market contracts differ in trading mechanism, participant composition, fee structure, and how information is incorporated into prices. These institutional differences could matter, but the present analysis was not designed to identify causal mechanisms and therefore does not attribute the finding to any specific market feature.

Future work should first preserve the same ex-ante verification and outcome-quarantine discipline. Useful extensions include obtaining an independently validated historical-price source for PR1-M markets, expanding to categories with adequate within-category support, and constructing comparable samples on other prediction-market platforms. Such analyses should pre-specify how platform differences, contract-family dependence, price observability, fees, and resolution conventions enter the estimand before outcomes are examined.

# Limitations

1. **PR2 Sports scope.** The confirmatory estimate applies to scheduled-event-start Sports markets verified under PR2-M. It does not cover all Kalshi markets. PR1-M fixed-clock anchors remain valid, but qualifying midpoint coverage under the frozen historical-price specification was too sparse for a comparable confirmatory analysis.

2. **Conditional price observability.** Inference is conditional on contracts and families with valid pre-target quotes. Observability was not random: the family-weighted standardized difference in hours since market open was 0.257, while maximum absolute observed-versus-unavailable share differences were 0.197 by family size, 0.123 by month, and 0.027 by target hour. No observation-propensity correction was used because its identifying assumptions were not considered defensible.

3. **Historical midpoint interpretation.** A bid/ask midpoint is an indicative probability, not necessarily an executable transaction price. Upper-tail spreads can be wide. The predeclared spread restrictions and separate trade-close analyses did not materially change the qualitative conclusion, but they do not eliminate all liquidity or execution concerns.

4. **Within-family dependence.** Contracts from the same event family are not independent. The primary weighting scheme gives equal target mass to families, and uncertainty was estimated by resampling complete families within the original sampling strata. Residual dependence across related families is not explicitly modeled.

5. **Resolution availability.** Seventy-eight sampled contracts were unresolved or nonbinary. They were retained as missing outcomes without replacement, sample redrawing, or adaptive reweighting. Although weighted resolution coverage exceeded 99%, unresolved observations were not distributed uniformly across months and family sizes.

6. **Statistical uncertainty.** Failure to reject a zero contrast is not proof that the true effect equals zero. The primary confidence interval indicates the range of effect sizes compatible with the frozen design and data. The ten calibration bins are descriptive jointly; one interval excluding zero is not treated as a standalone discovery because no post-hoc multiple-comparison search was specified.
