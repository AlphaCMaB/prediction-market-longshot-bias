# Mentor-facing executive summary

The project has completed its first fully pre-specified favorite-longshot analysis. The design is deliberately outcome-blind through sample and price construction: event anchors were verified from ex-ante metadata, the t-1h sample and weights were frozen, and only then was a minimal binary-outcome projection released.

The confirmatory sample contains 9,353 resolved contracts from 4,360 scheduled-event-start Sports families. The primary family-weighted longshot-minus-favorite calibration contrast is 0.01004, with a 95% family-cluster bootstrap interval from -0.02358 to 0.04565. Because the classical pattern predicts a negative contrast and this interval includes zero, the appropriate conclusion is: **in the pre-specified PR2 Sports sample, we detect no statistically distinguishable evidence of favorite-longshot bias.**

All pre-specified robustness intervals also include zero, including alternative staleness windows, trade-close prices, and two spread restrictions. The result is nevertheless conditional on price observability and should not be generalized to all Kalshi markets. The main paper limitation is that fixed-clock PR1 markets lacked sufficient historical-price coverage under the frozen source, and observable PR2 contracts differed compositionally from unavailable-price contracts.
