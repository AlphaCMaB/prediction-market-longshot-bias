# Legacy Methodology Scripts

This directory is reserved for historical scripts that will be moved only in
a later approved phase using `git mv`. No Python scripts have been moved as part
of creating this documentation.

## Retrospective methodology V1

Methodology V1 selected historical prices relative to realized platform
resolution or settlement timestamps, including targets such as 24, 48, and
168 hours before resolution.

That approach was useful for exploratory calibration, robustness, liquidity,
and category analyses. However, when the realized resolution time was not
known before the event, aligning a historical price to that timestamp can use
future information. It therefore does not define the primary tradable
analysis.

V1 is retained for:

- exact reproducibility of the exploratory work;
- methodological comparison with occurrence-anchored V2 results;
- provenance for the development of family clustering and robustness checks;
  and
- documentation of why the anchor methodology changed.

Its results must be described as exploratory retrospective results, not as the
primary leakage-resistant or tradable analysis.

When these scripts are eventually archived:

- their calculations will remain computationally unchanged;
- moves will use `git mv` to preserve history;
- they will remain runnable from the repository root using documented paths;
- their current processed inputs and outputs will not be silently relocated;
  and
- both script-17 variants will be preserved as methodology history rather than
  maintained as production stages.

Transition and audit scripts will be kept separately from the retrospective
resolution pipeline so that historical results, audit provenance, and the
production V2 workflow remain clearly distinguishable.
