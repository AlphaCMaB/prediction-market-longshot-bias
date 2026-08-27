# Initial Findings Memo: Prediction Market Longshot Bias Project

**Status:** Preliminary data and methodology findings through the Phase 10E design checkpoint

**Scope:** Kalshi only

**Important limitation:** Favorite–longshot bias is **not yet identified**. Candidate anchor evidence has been constructed, but zero anchors have been verified; outcomes remain quarantined; horizon prices have not been constructed; and no bias estimate has been run.

## 1. Executive summary

The project has constructed and validated a large, outcome-quarantined Kalshi research universe. Phase 10B produced a deterministic universe of 9,861,209 distinct market contracts associated with 427,090 event tickers. Phase 10C requested and retrieved metadata for all 427,090 unique events in 86 independently committed partitions. It made 2,768 successful requests, with no retries or rate limits. Collection-level retrieval omitted 316 valid events, but exact-event and milestone fallbacks recovered every omission. The normalized event universe contains no malformed, rejected, missing, duplicate, or conflicting events.

Phase 10D then used only approved, outcome-independent projections to construct candidate timing evidence for all 427,090 composite families. It produced 625,923 candidates: 208,308 from market occurrence timestamps, 209,017 from event strike timestamps, and 208,598 from milestone start timestamps. A total of 418,591 families have at least one candidate; 8,499 have none. All candidates have exact timestamp precision, while 198,686 families contain multiple distinct exact candidate times and therefore require review. These are descriptive findings about data coverage and research readiness, not evidence about trader behavior.

No candidate has been promoted automatically to a verified forecasting anchor. Every family remains `needs_review`, every verification field is blank, outcomes remain unmerged, and no horizon price or analysis sample has been constructed. The next phase is a separate human or explicitly approved verification process.

## 2. Research question

The project asks whether prediction-market contracts exhibit favorite–longshot bias: a systematic relationship between ex-ante market-implied probability and subsequent performance or calibration. A valid test requires a defensible pre-event price, not one selected using settlement information or the outcome.

Methodology V2 therefore separates data acquisition, anchor construction, price selection, and outcome analysis. The frozen analysis window is `[2025-07-01T00:00:00Z, 2026-07-01T00:00:00Z)`, and the StudyRules fingerprint is `12d6955f57b50b5587fdadf02b2bc96e7de48d022c9ac3cc2fe0425d907b9901`. Settlement time is retrieval and diagnostic information, not the forecasting anchor. Eligibility ultimately requires a reviewed ex-ante event time and an at-or-before horizon price.

## 3. What has been completed

Phase 10B completed terminal traversal of the required historical and live market segments. The acquisition comprised 454 independently validated partition commits and 11,269 successful requests, with no retries or rate limits. It processed 11,268,402 records: 9,861,209 were inside the provisional acquisition envelope, 1,407,193 were outside it, and none were rejected. The deterministic merge found 9,861,209 distinct market tickers, with no duplicate tickers, excess rows, or metadata conflicts. Outcome data were published separately from research metadata.

Phase 10C retrieved all 427,090 authenticated event tickers, preserved compressed raw responses, normalized outcome-free projections, and retained provenance. It recorded 208,598 event–milestone associations and reconciled eight timestamp-only freshness variants without treating them as substantive conflicts.

Phase 10D constructed deterministic, review-only candidate evidence from three allowed sources: market `occurrence_datetime`, event `strike_date`, and milestone `milestone_start_date`. Equivalent market occurrence evidence within a family was represented once while retaining its supporting-source count. Milestone end dates, source update timestamps, market close and expiration times, settlement fields, and outcomes were excluded. The full offline suite passed 640 tests.

### Current empirical status of the project

| Component | Current status | Verified quantity | Interpretation | Remaining limitation |
|---|---|---:|---|---|
| Market universe | Complete acquisition universe | 9,861,209 distinct contracts; 454 partitions | Large, deterministic market metadata universe | Not the final analysis sample |
| Event universe | Complete metadata universe | 427,090 unique events; 86 partitions | All authenticated event tickers were retrieved and normalized | Metadata is evidence, not verification |
| Milestone evidence | Complete acquisition and merge | 208,598 associations | Substantial structured timing evidence is available | Association does not establish anchor validity |
| Anchor candidate construction | Complete | 625,923 candidates; 418,591 families with candidates | The universe is ready for separate candidate review | 8,499 families have no candidate; conflicts remain unresolved |
| Anchor verification | Not begun | 0 anchors verified | No API timestamp has been accepted automatically | All 427,090 families remain `needs_review` |
| Outcomes | Acquired but quarantined | 0 outcome fields in normalized event projections | Outcomes could not influence metadata or candidate construction | Outcomes cannot be merged until sample construction is frozen |
| Horizon prices | Not constructed | 0 production horizon-price manifests | No price has been selected for bias estimation | Requires verified anchors and eligibility rules |
| Bias estimates | Not run | 0 estimates | Favorite–longshot bias is not identified | Calibration, returns, scoring, and regressions remain future work |
| Reproducibility | Validated through Phase 10D | 640 tests passed; deterministic outputs | Inputs and candidate evidence are auditable | Analytical reproducibility must be reassessed in later phases |

## 4. Preliminary data-universe findings

The principal empirical observation is scale and structure. The Phase 10B universe contains 9,861,209 market contracts linked to 427,090 event tickers—about 23.1 contracts per event ticker as a universe-wide ratio. This indicates a many-contract-to-event organization, but does not describe its distribution; medians, quantiles, concentration, and final eligible-sample composition remain **[TO BE COMPUTED]**. The acquisition universe is wider than the frozen window and is not the estimation sample.

Candidate timing evidence is widespread but heterogeneous. Phase 10D found at least one allowed candidate for 418,591 families, or approximately 98.0% of the 427,090-family universe; 8,499 families have none. The 625,923 candidates are nearly evenly divided among market occurrence, event strike, and milestone start sources. All are exact timestamps rather than date-only observations. In relation to the frozen window, 539,109 candidates fall inside it, 8,196 precede it, and 78,618 fall at or after its endpoint. These classifications describe timestamps, not eligible observations.

The 198,686 families with multiple distinct exact candidate times demonstrate why timestamp presence cannot substitute for verification: permitted sources may refer to different moments or timing semantics. These cases remain unresolved. There were no families mapped to multiple event tickers, invalid candidate values, or sentinel timestamps.

The dataset is suitable for candidate-anchor review because coverage, provenance, and competing evidence are explicit. It is not suitable for bias estimation because the anchor, horizon, price observation, and final sample remain unresolved.

## 5. Data-quality and API findings

The acquisition revealed two consequential API behaviors. First, the historical market endpoint did not enforce settlement-date filters server-side. Phase 10B therefore required traversal to the terminal cursor followed by local filtering. Of 8,777,951 historical records, 7,370,758 were inside the provisional envelope and 1,407,193 were outside it. A client-supplied date range could not be assumed to define the returned archive.

Second, collection-level event retrieval omitted 316 authenticated tickers. Exact-event and milestone fallbacks recovered all omitted events and their associated evidence. A collection-only workflow would have produced an incomplete subset.

### Phase 10C data-quality diagnostics

| Diagnostic | Verified result | Research significance |
|---|---:|---|
| Requested and retrieved events | 427,090 / 427,090 | Complete coverage of the authenticated Phase 10B event universe |
| Independently committed partitions | 86 | Bounded audit and resume units |
| Successful requests | 2,768 | Complete production request record |
| Retries / rate limits | 0 / 0 | No retry-dependent or throttling anomaly |
| Collection omissions recovered | 316 / 316 | Collection incompleteness did not become sample attrition |
| Malformed / rejected / missing events | 0 / 0 / 0 | No normalized event loss from these classes |
| Duplicate / conflicting events | 0 / 0 | One deterministic normalized representation per event |
| Milestone associations | 208,598 | Large body of timing-related candidate evidence |
| Duplicate / conflicting milestones | 0 / 0 | Deterministic milestone merge |
| Timestamp-only freshness variants | 8 | Freshness differences were separated from substantive conflicts |
| Outcome fields in event projections | 0 | Recursive outcome-quarantine check passed |

The eight freshness variants differed only in source update timestamps; substantive fields agreed. They were reconciled deterministically, while substantive disagreements remained fail-closed.

## 6. Outcome-quarantine and look-ahead-bias protections

The central validity protection is ordering. Market outcomes remain in a separate compressed Phase 10B artifact and have not entered market metadata, event research features, or candidate-anchor construction. Recursive research projections exclude outcome, result, settlement, close, and expiration fields. Phase 10C found zero outcome fields in normalized event projections, and Phase 10D used hashes of the approved research projections rather than unrestricted source rows.

API timestamps are not automatically accepted as anchors. Settlement time remains diagnostic; update timestamps and milestone end dates are excluded; date-only evidence would remain date-only rather than being converted to midnight; and every candidate remains unverified. This design limits both direct leakage and discretionary sample construction informed by realized results.

## 7. Reproducibility and computational validation

The production datasets are partitioned, compressed, resumable, and auditable. Atomic publication, disk guards, hash-pinned production inputs, deterministic merges, and fail-closed validation protect both completeness and identity. Phase 10D was first executed as a full no-write dry run, then published atomically, and finally re-derived in a deterministic no-write rerun that left all output sizes, hashes, and modification times unchanged.

The Phase 10C event metadata and milestone SHA-256 identities are `ef97e0093234e7b963f739d7ddd435691b5f8580e6551f398754d1b95807f3bf` and `96f6c754f8ffbb0c9aa1d12e1a1cdf953079b773de419dc5e90917673334ab82`; its provenance and merge-report identities are `6d6c9bb646abf9963e8f9ca978c681ef1db07b455114aa3e83a27f417c4671f0` and `7e80509ca3f03d184343672f5d008913677edfdd11d7266216ac9eff2d959774`. The Phase 10C merge commit identity is `b2294aeac4c28217cce52358d31f257169e88e2d0d217cf3cb12757fbdd8eb43`.

Phase 10D’s evidence, family-review, decisions-template, and report SHA-256 identities are, respectively, `8bd614d88c139aa02bc5fbfeb12dd9c2287cd98766a7f029d084347b6afd1e10`, `794178aced9711498f57b77fd74ff61412185eb4ee5f76aeafc275aa501e0795`, `4963a7c9af0c923c5c9a24d0985c4e0e463229dd5b6677a3dc3152e08ebe5203`, and `ec9d3e907caf0eb5ae989cdf9805ecce1d5538ab3fe5ea278370a80c7c06508e`. These identities establish computational provenance, not substantive validity of any candidate anchor.

## 8. What cannot yet be concluded

No evidence for or against favorite–longshot bias can yet be reported. The project cannot conclude whether low-probability contracts are overpriced, whether high-probability contracts outperform, whether forecasts are calibrated, or whether patterns differ by category, horizon, liquidity, or family. Returns, Brier scores, calibration curves, regressions, statistical significance, and economic magnitudes are all **[TO BE COMPUTED]** only after the remaining design stages.

The project also cannot interpret settlement outcomes, state the final analysis-sample size, report the number of verified event families, or describe horizon-price availability. Those quantities are deliberately unidentified until ex-ante timing and sample-selection rules are applied without outcome access.

## 9. Phase 10D findings and planned next analysis

Phase 10D completed candidate construction, not verification. Allowed evidence exists for most families, three source types contribute at similar scale, and many families present competing exact times. It does not determine which candidate—if any—is the relevant ex-ante occurrence.

Phase 10E has proposed a staged, outcome-blind audit rather than reviewing all 427,090 families. The proposal assigns 102,413 families to a fixed-clock single-candidate tier, 93,997 to a conservative Sports milestone-start tier, and 230,680 to manual review. A reproducible packet samples 150 families per tier. These labels are not verified anchors: both proposed rules remain unapproved, and approval and reviewer-disagreement rates are **[TO BE COMPUTED AFTER REVIEW]**.

The next phase is a separate human or explicitly approved review of the immutable evidence and blank verification template. Reviewers must decide whether evidence supports an allowed anchor source and timing structure, while recording provenance and leaving unresolved families quarantined. Only after reviewed decisions are applied and validated may later stages construct eligible horizons, target timestamps, and at-or-before prices. Sample membership and research features must be frozen before outcomes are merged.

## 10. Candidate tables and figures for the eventual paper

The eventual paper could include:

1. **Data-construction flow diagram:** market acquisition, outcome quarantine, event metadata, candidate evidence, verification, horizons, prices, frozen sample, and outcome merge.
2. **Market-universe table:** contracts and event tickers by acquisition segment and in-range status; category composition **[TO BE COMPUTED]**.
3. **Contracts-per-event figure:** median, quantiles, and upper-tail concentration **[TO BE COMPUTED]**.
4. **Event-coverage table:** collection returns, exact-event recoveries, missing events, and milestone associations.
5. **Anchor-evidence funnel:** 427,090 families, 418,591 with candidates, 8,499 without, 198,686 with multiple distinct exact times, and verified or excluded counts **[TO BE COMPUTED AFTER REVIEW]**.
6. **Candidate-source and timing table:** candidate counts by source, window relation, category, and eventual timing classification.
7. **Horizon-price availability figure:** eligible and priceable contracts by preregistered horizon and staleness threshold **[TO BE COMPUTED]**.
8. **Favorite–longshot results:** calibration plots, probability-bin tables, family-clustered uncertainty, returns, Brier scores, and regression estimates **[NOT YET IDENTIFIED]**.

At this checkpoint, only data-construction, coverage, candidate-evidence, and validation exhibits are supportable. Behavioral figures must wait until the full outcome-blind sample-construction sequence is complete.
