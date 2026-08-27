# Retrospective-Resolution Methodology V1

These scripts implement exploratory Methodology V1. They select historical
prices relative to realized platform resolution or settlement timestamps,
including targets such as 24, 48, and 168 hours before resolution.

The scripts are retained for exact reproducibility and methodological
comparison with the ex-ante occurrence-anchor approach used by Methodology V2.
Their results are exploratory Methodology V1 results; they are not the primary
tradable or leakage-resistant analysis.

Run these scripts from the repository root, using their paths under
`scripts/legacy/retrospective_resolution/`. For example:

```bash
python scripts/legacy/retrospective_resolution/05_extract_p_hat_batch.py
```

Smoke-test variants are under
`scripts/legacy/retrospective_resolution/smoke_tests/`.

All repository-relative data and output paths remain unchanged. Existing raw
data, processed data, and outputs have not been moved. No target-time,
probability, clustering, statistical, or other V1 calculation was changed
during archival; the files were moved with `git mv` to preserve their history.
