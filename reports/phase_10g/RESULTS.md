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
