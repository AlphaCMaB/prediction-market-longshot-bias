# Limitations

1. **PR2 Sports scope.** The confirmatory estimate applies to scheduled-event-start Sports markets verified under PR2-M. It does not cover all Kalshi markets. PR1-M fixed-clock anchors remain valid, but qualifying midpoint coverage under the frozen historical-price specification was too sparse for a comparable confirmatory analysis.

2. **Conditional price observability.** Inference is conditional on contracts and families with valid pre-target quotes. Observability was not random: the family-weighted standardized difference in hours since market open was 0.257, while maximum absolute observed-versus-unavailable share differences were 0.197 by family size, 0.123 by month, and 0.027 by target hour. No observation-propensity correction was used because its identifying assumptions were not considered defensible.

3. **Historical midpoint interpretation.** A bid/ask midpoint is an indicative probability, not necessarily an executable transaction price. Upper-tail spreads can be wide. The predeclared spread restrictions and separate trade-close analyses did not materially change the qualitative conclusion, but they do not eliminate all liquidity or execution concerns.

4. **Within-family dependence.** Contracts from the same event family are not independent. The primary weighting scheme gives equal target mass to families, and uncertainty was estimated by resampling complete families within the original sampling strata. Residual dependence across related families is not explicitly modeled.

5. **Resolution availability.** Seventy-eight sampled contracts were unresolved or nonbinary. They were retained as missing outcomes without replacement, sample redrawing, or adaptive reweighting. Although weighted resolution coverage exceeded 99%, unresolved observations were not distributed uniformly across months and family sizes.

6. **Statistical uncertainty.** Failure to reject a zero contrast is not proof that the true effect equals zero. The primary confidence interval indicates the range of effect sizes compatible with the frozen design and data. The ten calibration bins are descriptive jointly; one interval excluding zero is not treated as a standalone discovery because no post-hoc multiple-comparison search was specified.
