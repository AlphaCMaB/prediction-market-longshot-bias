# Project Status

Last updated: 2026-08-25
Branch: `methodology-v2-clean`

## Research objective

Measure favorite-longshot bias in prediction markets without look-ahead bias or
outcome leakage. Methodology V2 is currently Kalshi-only. The frozen analysis
anchor window remains `[2025-07-01T00:00:00Z, 2026-07-01T00:00:00Z)` and the
`StudyRules` fingerprint remains
`12d6955f57b50b5587fdadf02b2bc96e7de48d022c9ac3cc2fe0425d907b9901`.

## Completed work

- Phase 9A: frozen study rules, strict outcome quarantine, verified-anchor
  handoff, horizon and price-target construction, and empty-universe behavior.
- Secure settled-market ingestion foundation: immutable raw pages, cutoff
  snapshots, retries, cursor validation, resumability, provenance, atomic
  publication, and deterministic commits.
- Phase 9B-A: secure event metadata and milestone acquisition.
- Phase 9B-B: candidate anchor evidence and review artifacts. Every generated
  decision remains `needs_review`; no API timestamp is automatically verified.
- The truncated-response retry fix is committed as `ca2ca3e`.
- The incomplete 2025-05-01 through 2026-07-16 acquisition directory has been
  removed. No validated data was deleted during the 2026-07-17 audit.

## Acquisition incident finding

The historical acquisition did not enforce the requested settlement window on
the server. `segment_params()` adds `min_settled_ts` and `max_settled_ts` only
for the live endpoint. Kalshi's historical-markets endpoint does not offer date
filters; it accepts cursor/limit and ticker-, event-, or series-based filters.
The planner also collapses all historical months into one cursor chain.
This is confirmed by Kalshi's official
[historical-markets reference](https://docs.kalshi.com/api-reference/historical/get-historical-markets)
and [historical-data guide](https://docs.kalshi.com/getting_started/historical_data).

Consequently, the roughly 906 pages / 906,000 rows were an unbounded scan of
the global historical archive, followed by local date filtering. There is no
remaining raw manifest with which to measure the exact out-of-range share, but
the code and API contract are sufficient to establish the routing error. The
cursor-integrity protections make accidental repeated-page loops unlikely; no
evidence currently indicates that duplicate pagination caused the scale.

## Phase 10A-R completed

Phase 10A-R: disk-bounded, partitioned Kalshi settled-market acquisition.

Completed capabilities:

- deterministic gzip raw pages and immutable per-partition request manifests;
- bounded cursor partitions for historical data and bounded monthly segments
  for live data;
- independent validated partition commits and cursor-chain resume;
- configurable 5 GiB raw-root ceiling and 80 GiB minimum-free-space guard;
- read-only request/size preflight with an explicit unknown historical total;
- immediate separate metadata, outcome, provenance, and normalization outputs;
- deterministic outcome-independent cross-partition merge/deduplication;
- immutable incomplete-run/merge reports and final merge commits;
- atomic publication and final post-publication validation; and
- a default block on legacy unbounded historical acquisition.

Acceptance on 2026-07-17: 614 tests passed; Python compilation, unused-import
inspection, formatting checks for new modules, and `git diff --check` passed.
No network acquisition was run in this phase and the acquisition directory
remained empty.

## Phase 10A-S completed

The one-page bounded network smoke test passed on 2026-07-17 using the
provisional acquisition envelope
`[2025-05-01T00:00:00Z, 2026-07-17T00:00:00Z)`.

- Cutoff: `2026-05-18T00:00:00Z`.
- Requests: 2 successful (1 cutoff + 1 historical page), 0 retries.
- Rows: 1,000 input, 1,000 in range, 0 outside, 0 rejected.
- Raw page: 2,373,973 bytes uncompressed; 80,059 bytes gzip.
- Entire smoke root after reports: 339,295 bytes (356 KiB on disk).
- Partition commit: valid and nonterminal; resume preflight advanced to
  partition index 1 with the committed cursor hash.
- Quarantine: 1,000 metadata rows and 1,000 outcome rows; metadata contained no
  `result` or settlement-value field.
- Incomplete merge: exited with status 3, wrote an immutable incomplete report,
  and published no final universe.
- Guards: 256 MiB smoke ceiling and 80 GiB free-space floor remained intact.
- Smoke-review finding fixed: actual runs using an externally pinned cutoff now
  publish a validated immutable copy inside the guarded acquisition root;
  read-only preflight remains write-free. The full 614-test suite passed again.

Smoke artifacts are retained for audit under
`/private/tmp/kalshi-partitioned-smoke.IVGaQL`; no validated data was deleted.

## Validated production universes

Phase 10B completed on 2026-07-17 and remains immutable.

- Acquisition completeness: all four segments terminal and fully validated.
- Historical: 352 partitions, 8,778 requests/pages, 8,777,951 input records;
  7,370,758 in range and 1,407,193 outside the provisional envelope.
- Live: 102 partitions, 2,491 requests/pages, 2,490,451 input/in-range records
  across May, June, and July 2026.
- Total: 454 independently validated partition commits, 11,269 logical and
  successful HTTP requests, 11,268,402 input records, 9,861,209 in range,
  1,407,193 outside, 0 rejected, 0 retries, and 0 rate limits.
- Exact cross-partition audit: 9,861,209 distinct tickers, 0 duplicates, 0
  excess rows, and 0 metadata conflicts.
- Final merge: complete, deterministic, compressed, and outcome-independent;
  merge ID `6f8aa42abec876d3aa1f6336`.
- Final market universe: 9,861,209 contracts. Final event-ticker universe:
  427,090 events.
- Metadata remains outcome-free. Quarantined outcomes are a separate compressed
  artifact and have not been merged into research features.
- Guarded namespace: 3,648,491,736 bytes of the 5 GiB ceiling. Filesystem free
  space: 103,643,897,856 bytes, 17,744,551,936 bytes above the 80 GiB floor.
- Final acceptance: 617 tests passed; compilation, formatting, unused-import,
  and diff checks passed.

No anchor timestamp was verified or changed, and the frozen analysis window and
`StudyRules` fingerprint remain unchanged.

Phase 10C implementation completed offline on 2026-07-18.

- The compressed Phase 10B event-ticker artifact was revalidated at SHA-256
  `544b5464f7afa01d8d9fa4148db1a6fee07a5fbf6265554db314c771b818cc45`.
- Shared CSV input now streams plain or gzip files without materializing the
  427,090-row source universe.
- Event acquisition now uses deterministic 5,000-event partitions composed of
  batches of at most 200 tickers, immutable gzip raw pages, bounded cursor
  traversal within each batch, and independent validated partition commits.
- Resume reuses raw pages only after request/response hashes and cursor
  transitions validate. It never treats an uncommitted page as completion.
- Read-only preflight reports source totals, unique and malformed tickers,
  duplicates, batches, minimum requests, partitions, raw and normalized
  storage estimates, total namespace size, and free-space margin.
- Preflight accounts for the existing Phase 10B namespace plus both partition
  artifacts and the final compressed merge; actual publications retain the
  5 GiB namespace ceiling and 80 GiB free-space floor.
- Normalized event and milestone artifacts apply the recursive research
  projection before publication. Outcomes remain confined to immutable raw
  evidence, timestamps remain unverified candidate evidence, and no anchor
  verification is invoked.
- Complete scopes receive deterministic compressed event metadata, milestone,
  and provenance outputs. Any missing event, duplicate behavior change,
  cross-partition conflict, cursor problem, schema change, or incomplete
  partition blocks final publication.
- Offline acceptance: 636 tests passed after the collection-fallback fix.
  Compilation, pyflakes,
  Black formatting, and `git diff --check` passed.

Phase 10C production preflight and bounded smoke passed on 2026-07-18.

- Production source audit: 427,090 total and unique events, zero malformed
  tickers, zero duplicates, sorted input, and exact pinned SHA-256 match.
- Production plan: 86 deterministic partitions; 2,136 minimum collection
  requests; 27,762 estimated total requests using the observed 3% exact
  fallback rate.
- Projected additional storage: 1,145,832,448 bytes, producing a projected
  4,794,373,826-byte namespace and retaining 16,207,182,848 bytes above the
  80 GiB free-space floor at preflight time.
- The first immutable smoke scope correctly stopped incomplete at 194/200 and
  identified six valid collection omissions. No final universe was published.
- The corrected scope retrieved 200/200 events through one collection request,
  six single-event fallbacks, and six related-milestone requests: 13 successful
  attempts, zero retries, and zero rate limits.
- Corrected smoke output: 200 event rows, 114 milestone associations, zero
  missing events, duplicates, or conflicts; 25,502 compressed raw-page bytes
  and 27,836 compressed partition-artifact bytes.
- Smoke merge ID: `2b75ffc9269f451fed90b82d`; merge commit SHA-256:
  `840766f4e8e0792714e8ea6a6ff306d07fe1ab51aec86c728ba036163b01d0ea`.
- A no-network resume reused the committed scope and reproduced all output
  hashes without publishing another partition. Recursive normalized-field
  inspection found no quarantined key.

Phase 10C production acquisition completed on 2026-07-18.

- Production scope `6de9f91508597d5343bfe745` contains 86 independently
  validated partitions covering all 427,090 Phase 10B event tickers.
- Requests: 2,768 logical and successful HTTP attempts, zero retries and zero
  rate limits. The collection endpoint omitted 316 requested events; all 316
  were recovered through exact-event and related-milestone requests.
- Completeness: 427,090 requested, retrieved, normalized, and provenanced;
  zero malformed, rejected, missing, duplicate, or conflicting events.
- Milestones: 208,598 deterministic event-milestone associations, zero
  duplicate or conflicting merged rows. Eight source-freshness timestamp
  variants were reconciled under the accepted timestamp-only rule.
- Production merge: complete and published as
  `69f1b1277bdfdbd530834fe6`. It contains no normalized outcome field, does not
  merge Phase 10B outcomes, and verifies no anchor.
- Compressed production bytes: 96,133,690 raw pages; 113,609,192 partition
  artifacts; 110,500,035 final merge artifacts.
- Final hashes: event metadata
  `ef97e0093234e7b963f739d7ddd435691b5f8580e6551f398754d1b95807f3bf`,
  event milestones
  `96f6c754f8ffbb0c9aa1d12e1a1cdf953079b773de419dc5e90917673334ab82`,
  provenance
  `6d6c9bb646abf9963e8f9ca978c681ef1db07b455114aa3e83a27f417c4671f0`,
  and merge commit
  `b2294aeac4c28217cce52358d31f257169e88e2d0d217cf3cb12757fbdd8eb43`.
- Final no-network resume made zero requests and left every acquisition file
  unchanged. Recursive inspection of all final normalized JSON projections
  found zero forbidden outcome paths.
- Final acceptance: 636 tests passed. Compilation, scoped pyflakes, Black,
  and `git diff --check` passed; the acceptance review found no remaining
  Phase 10C issue.

Phase 10D candidate anchor-evidence construction completed on 2026-07-19.

- The complete outcome-free Phase 10B/10C universes produced 427,090
  composite families and 625,923 candidate rows. Every candidate, family
  review, and verification-template row remains `needs_review`; zero anchors
  were verified and zero outcomes were read or merged.
- Candidate sources are frozen and exhaustive: 208,308 grouped market
  `occurrence_datetime` candidates, 209,017 event `strike_date` candidates,
  and 208,598 milestone-start candidates. Equivalent market occurrence values
  within the same composite family/event are represented once with their
  supporting-market count; no candidate value or source type is discarded.
- Candidate availability: 418,591 families have at least one candidate and
  8,499 have none. All 625,923 candidates are exact timestamps; the production
  data contain zero date-only candidates, invalid candidate values, year-0001
  sentinels, missing event metadata, or multi-event-ticker families.
- Frozen-window diagnostics: 539,109 candidates are inside the half-open
  analysis window, 8,196 are before it, and 78,618 are at or after its end.
  These are availability diagnostics only and do not make a candidate
  eligible. 198,686 families contain multiple distinct exact candidate times
  and therefore require review.
- Outputs are complete and untruncated under
  `data/pipeline_v2/anchor_evidence/phase_10d/`. A deterministic no-write rerun
  reproduced every hash and left all file sizes and modification times
  unchanged.
- Final acceptance: 118 focused tests and the complete 640-test offline suite
  passed. Compilation, imports, scoped pyflakes, Black, TOML validation, and
  `git diff --check` passed.

Phase 10E outcome-blind audit design reached the approval checkpoint on
2026-07-19.

- Candidate patterns: 217,835 families have one exact candidate; 2,070 have
  multiple candidates agreeing on one exact time; 198,686 have multiple
  distinct exact times; and 8,499 have no candidate.
- Proposed audit tiers partition all 427,090 families: Tier 1 contains 102,413
  proposed fixed-clock/single-exact cases; Tier 2 contains 93,997 conservative
  Sports milestone-start cases; Tier 3 contains 230,680 manual-review cases.
- The proposed in-window pool is 189,466 families: 97,369 in Tier 1 and 92,097
  in Tier 2. These are availability counts, not verified or eligible families.
- A deterministic stratified audit packet contains 150 families per tier, 450
  total, with exact-schema decision rows all left `needs_review` and every
  verified field blank. Approval and disagreement rates are not yet observed.
- Packet inspection identified and excluded an endogenous-subevent false
  positive class before publication. PR1 and PR2 remain explicitly unapproved.
- Zero outcomes, prices, close/expiration times, or settlement values were read;
  zero anchors were verified, zero horizons were built, and zero network
  requests were made.

The recommendation-only AI first review completed on 2026-07-19 without
promoting either proposed rule.

- All 450 sampled families were reviewed from the outcome-blind packet. Tier 1
  produced 86 rule-case recommendations, 60 uncertain human-review referrals,
  and four rejection recommendations. Tier 2 produced 135 rule-case
  recommendations, 13 uncertain referrals, and two rejection recommendations.
  All 150 Tier 3 cases remain quarantined.
- Confidence was high for 237 cases, medium for 210, and low for three. All 450
  actual verification statuses remain `needs_review`; these are AI
  recommendations, not verified anchors or approved-rule decisions.
- The inverse-probability-weighted AI recommendation rates are 50.10% for PR1
  and 92.34% for PR2. The corresponding AI uncertainty rates are 49.86% and
  7.26%; AI rejection rates are 0.04% and 0.40%. Human approval and AI-human
  disagreement rates remain unobserved.
- The compact human-review handoff contains 165 cases: deterministic sets of 50
  from each proposed-rule tier, every low-confidence, rejected, or ambiguity-
  flagged Tier 1–2 case, and 10 diagnostic Tier 3 cases. Planning burden is 11
  reviewer-hours at four minutes per case.
- A deterministic no-write rerun reproduced all four output hashes and
  modification times. The complete 649-test offline suite, compilation, scoped
  Black and pyflakes, TOML validation, quarantine checks, and `git diff
  --check` passed. Repository-wide Black remains a pre-existing baseline
  failure across 64 files outside this phase's scope.

The finalized 165-case annotation table was imported on 2026-08-22 and is
methodologically classified as an **AI-assisted outcome-blind review**, not an
independent human review.

- The source CSV contains exactly 165 rows and unique audit IDs and has SHA-256
  `85153b54eb7bb7d1e136c907770aa86fae57cc404821ad081c8d67204b55fff9`.
  Its IDs exactly match the immutable compact subset; both immutable source
  packet hashes were revalidated before import.
- Final decisions are 149 approvals, five rejections, and 11 uncertain. All
  finalized correction invariants and controlled vocabularies passed.
- PR1's unweighted approval/rejection/uncertainty rates are 93.75%/2.08%/4.17%;
  inverse-probability-weighted rates are 97.63%/0.003%/2.37%. PR2's are
  94.92%/3.39%/1.69% unweighted and 98.73%/0.86%/0.41% weighted. These are
  AI-assisted diagnostics, not independent-human estimates and not rule
  approvals.
- Every imported status remains `needs_review`; verified anchor time and
  source remain blank. No outcome, post-event information, price, horizon,
  verification application, or network client was accessed.
- A fresh deterministic validation packet contains 100 cases: 50 PR1 and 50
  PR2. It is stratified by category and observed failure mode, excludes Tier 3
  from rule inference, and contains no AI recommendation or AI-assisted
  decision. The estimated burden is 6.67 hours at four minutes per case.
- The new interface hash-pins the packet and sample manifest, atomically
  autosaves independent-human decisions, resumes safely, and loads the
  AI-assisted comparator only after all 100 fresh decisions are complete.
  PR1 and PR2 remain explicitly unapproved.
- Acceptance passed 30 focused Phase 10E tests and the complete 670-test
  offline suite, plus compilation, scoped Black and pyflakes, TOML validation,
  recursive outcome-quarantine inspection, deterministic rerun, production
  packet preflight, and `git diff --check`.

The fresh independent outcome-blind human validation completed on 2026-08-22.

- Both reviewer source texts were SHA-256 pinned before import. The import
  covers exactly 100 unique validation and audit IDs, split 50 PR1 and 50 PR2,
  with no missing, extra, or duplicate IDs.
- The reviewer recorded 95 approvals, five rejections, zero uncertainty, and
  high confidence for all 100 cases. Reviewer decisions, rationales, timing
  structures, and literal ambiguity labels were preserved without correction
  or harmonization.
- PR1: 47/50 approved and 3/50 rejected; weighted approval is 97.17% and
  weighted confirmed false-positive rate is 2.83%. AI-assisted/human
  disagreement is 3/50 (6.00%), or 2.82% weighted.
- PR2: 48/50 approved and 2/50 rejected; weighted approval is 99.96% and
  weighted confirmed false-positive rate is 0.04%. AI-assisted/human
  disagreement is 5/50 (10.00%), or 1.27% weighted.
- The eight disagreements are explicitly quarantined in a separate queue.
  No disagreement was changed. Category- and failure-mode-specific diagnostics
  and approximate design-weighted confidence intervals are published in the
  independent-human report.
- Zero uncertainty responses and uniform high confidence are consistent with a
  reviewer response-style effect. This is recorded as a limitation because it
  may compress expressed uncertainty, although it does not invalidate the
  completed review.
- Outcome-blind recommendation: MODIFY both PR1 and PR2 with the enumerated
  exclusions. This is not rule approval. All 100 statuses remain
  `needs_review`; zero production anchors or rules were applied.
- Independent-review acceptance passed the complete 671-test offline suite,
  compilation, scoped Black and pyflakes, TOML validation, deterministic
  no-write rerun, decision quarantine, disagreement integrity, source-hash
  preservation, and `git diff --check`.

Phase 10E rule approval and deterministic application completed on 2026-08-23.

- The project owner explicitly approved modified rules PR1-M and PR2-M. The
  approval source is pinned at SHA-256
  `10affde71153a9428175435c945df1ae0a8ced412a2c3ea60f086dc59623f81e`.
- PR1-M verified 98,625 fixed-clock families. It retains exact-time,
  date/semantic, publication, deadline/window, and multiple-clock exclusions;
  officially defined benchmark settlement observations remain allowed when
  they are the contract's exact reference. Short market duration is not an
  anchor exclusion because price availability belongs to Phase 10F.
- PR2-M verified 69,329 scheduled-event-start families. Set/map/series scope,
  endogenous and partial subevents, conditional or unclear starts, date and
  semantic mismatches, post-event timing, and sub-minute non-schedule values
  remain excluded from deterministic verification.
- In total, 167,954 of 427,090 families are `verified_automatic`; the remaining
  259,136 remain `needs_review`. No family was retrospectively rejected.
- The complete application is compressed, atomic, deterministic, and pinned to
  the Phase 10D family/evidence inputs, Phase 10C event metadata, the independent
  validation report, frozen `StudyRules`, approved rule specification, and
  implementation hashes.
- Zero outcomes or prices were accessed, horizon availability was not tested,
  and no network request was made. The analysis window and `StudyRules`
  fingerprint remain unchanged.
- Acceptance review caught and fixed first-goalscorer, first-five-innings, and
  sub-minute scheduled-start false positives before final publication. Two
  four superseded snapshots remain locally retained; no generated evidence was
  deleted.

Phase 10F-A offline horizon-price planning completed on 2026-08-23.

- All 161,343 verified families inside the frozen analysis window were joined
  outcome-blind to 4,640,355 associated market contracts. PR1-M contributes
  93,896 families and PR2-M contributes 67,447.
- At t−1h, 112,166 families (69.52%) definitely had at least one open market;
  49,177 (30.48%) definitely opened too late. Zero families have unknown
  market-existence status. Anchor verification remains unchanged.
- Short-duration crypto drives expected structural attrition: 46,502 of 90,391
  in-window Crypto families (51.45%) are labelled
  `valid_anchor_but_no_t_minus_1h_market`. The corresponding counts are 2,672
  Sports, two Financials, one Climate and Weather, and zero Commodities.
- The offline-eligible scope contains 4,586,979 market tickers across 16,375
  distinct target timestamps. Sharing bounded candlestick requests across up
  to 100 tickers with the same target projects 58,468 minimum logical requests,
  before retries or recursive splits.
- The local multi-market one-minute candlestick endpoint is the recommended
  acquisition source for a bounded smoke, but its cache must first become
  compressed, atomic, immutable, partitioned, and safely resumable. Historical
  cutoff behavior remains unvalidated and is a smoke-test gate.
- The existing extractor silently mixes midpoint, trade-close, and previous-
  trade measures. Phase 10F-A recommends a two-measure design—contemporaneous
  bid/ask midpoint as primary and actual trade close as a separately labelled
  robustness measure—but this materially affects interpretation and requires
  explicit project-owner approval.
- Empirical full-scope storage is projected at 2,223,806,009 additional bytes;
  the conservative projection is 10,690,239,232 bytes. Neither fits the 5 GiB
  namespace, and the conservative projection also crosses the 80 GiB disk
  floor. Full production acquisition is not authorized.
- A deterministic 200-family smoke plan covers the required rule/category and
  market-existence strata. It projects 12,137 eligible tickers, 206 minimum
  requests, and 29,434,112 conservative additional bytes. It fits the current
  guards but has not been run.
- Five deterministic Phase 10F-A artifacts use 94,079,385 bytes. No outcome,
  price history, network request, archive, move, deletion, or StudyRules change
  occurred.

## Critical path

1. Obtain the project owner's analytical price-definition decision: midpoint
   primary with trade-close robustness, trade-close primary, or another
   explicitly frozen non-mixing definition.
2. After approval, implement the compressed/atomic partitioned smoke client and
   run only the approved 200-family smoke if a fresh preflight still fits the
   5 GiB namespace and 80 GiB floor.
3. Use measured smoke requests, bytes, field availability, and staleness
   attrition to prepare an archival approval request and revised production
   plan. Do not acquire the full scope or merge outcomes.

## Current resource state

- Repository size: approximately 608 MiB.
- Available filesystem space: 99,062,157,312 bytes (92.26 GiB) at final Phase
  10E design validation, 13,162,811,392 bytes above the safety floor.
- Safety floor: stop before available space falls below 80 GiB.
- Default raw acquisition budget: at most 5 GiB; any larger production run
  requires explicit approval.
- Phase 10A-R network requests: zero.
- Phase 10A-R acquisition data generated: zero bytes in the project acquisition
  directory; tests used temporary directories only.
- Phase 10A-S network requests: two; smoke data: 339,295 bytes in `/private/tmp`.
- Phase 10B final: 11,269 successful market-page requests, 3,648,491,736
  guarded bytes, 454 valid partition commits, and one valid final merge commit.
- Shared guarded namespace after Phase 10C: 3,972,267,951 bytes (3.70 GiB) of
  the 5 GiB ceiling. Phase 10C contributes 323,776,215 file bytes locally.
- Shared generated namespace after Phase 10D: 5,131,698,784 bytes of the 5 GiB
  ceiling, leaving 237,010,336 bytes. Phase 10D contributes 1,159,430,833
  bytes across four ignored local artifacts.
- The canonical Phase 10E design packet adds 1,758,331 bytes. A 1,761,991-byte
  superseded pre-acceptance packet is retained locally as
  `phase_10e_design_rejected_v1`; no validated data was deleted.
- The four Phase 10E first-review artifacts add 645,036 bytes. Shared generated
  namespace after the first-review checkpoint: 5,135,864,142 bytes, leaving
  232,844,978 bytes below the 5 GiB ceiling.
- Available filesystem space at first-review validation: 99,051,147,264 bytes
  (92.25 GiB), 13,151,801,344 bytes above the 80 GiB safety floor.
- The accepted AI-assisted import, diagnostics, validation packet, manifest,
  and design report add 254,224 bytes. Two small pre-acceptance directories
  totaling 508,717 bytes are retained rather than deleted. Shared generated
  bytes are 5,136,627,083, leaving 232,082,037 bytes below the 5 GiB ceiling.
  Free disk is 93,633,667,072 bytes (87.20 GiB), 7,734,321,152 bytes above the
  floor. At that packet-construction checkpoint, no fresh independent-human
  decision or report existed.
- The five accepted independent-human review artifacts add 98,895 bytes.
  Shared generated bytes are 5,136,732,126, leaving 231,976,994 bytes below
  the 5 GiB ceiling. Free disk is 92,795,133,952 bytes (86.42 GiB),
  6,895,788,032 bytes above the 80 GiB floor.
- Final Phase 10E rule outputs plus four retained acceptance-review snapshots
  bring the shared namespace to 5,212,312,767 bytes, leaving 156,396,353 bytes
  below the 5 GiB ceiling. Free disk is 91,032,793,088 bytes (84.78 GiB),
  5,133,447,168 bytes above the 80 GiB floor.
- Phase 10F-A adds 94,079,385 bytes. Shared generated bytes are now
  5,306,392,152, leaving 62,316,968 bytes below the ceiling. Free disk after
  publication is approximately 89,821,372,416 bytes (83.65 GiB), about
  3,922,026,496 bytes above the 80 GiB floor.

## Phase 10F-B — implementation complete; bounded smoke hard-stopped

The approved non-mixing midpoint/trade-close extractor, inclusive-end candle
boundary checks, deterministic request grouping, compressed immutable raw
cache, independent request commits, storage guards, no-network resume, spread
diagnostics, and incomplete-run reporting are implemented. The full offline
suite passes 703 tests.

The production preflight passed for exactly 200 families: 65 structural
late-opening families were skipped, 135 families mapped to 12,137 eligible
tickers, and 206 batch groups were planned. All 206 network responses were
successfully committed with zero retries and zero rate limits, but the live
batch endpoint returned an empty `markets` array for every group: zero returned
market objects and zero candles. The runner therefore failed closed before the
boundary probe, normalization, price acceptance, spread calculation, or any
production projection based on prices.

The incomplete smoke uses 635,141 bytes before its compact report: 109,154
compressed raw bytes and 525,987 request-commit bytes. Report SHA-256:
`a7bdeb4221c45ba42b8ae6c32f271f60d1e4f10d6565eaec0677031333d0d6bb`.
The outcome count remains zero and StudyRules are unchanged. Current guarded
usage is approximately 5,307,027,293 bytes, leaving approximately 61.68 MB;
free disk is approximately 84.61 GiB. Phase 10F-B remains incomplete pending
an explicitly approved historical-endpoint smoke design.

## Phase 10F-B2 — completed; per-market production route rejected

The strict 200-ticker historical validation completed at the 202-request cap:
one cutoff request, 200 ticker requests, and one exact-boundary probe. All 200
sample tickers routed to the historical endpoint under the fetched
`market_settled_ts` cutoff of `2026-06-24T00:00:00Z`. Every ticker request
succeeded; retries, rate limits, 404s, other failures, post-target candles, and
duplicate candles were all zero. The typed historical schema observed was
`yes_bid.close`, `yes_ask.close`, and `price.close`; `price.previous` was never
used.

The endpoint returned 1,164 candles across 54 nonempty tickers; 146 responses
were empty. Midpoint coverage was 44/200 at 15 minutes and 54/200 at 60
minutes. Trade-close coverage was 24/200 and 38/200. PR1-M coverage was
especially sparse: zero 15-minute midpoint observations and only four at 60
minutes among 135 sampled tickers. PR2-M Sports supplied nearly all usable
observations. This is a source-availability finding, not a bias estimate.

The exact-target/end-minus-one probe passed and confirms that
`end_period_ts` is safe as an inclusive candle-end timestamp when it is no
later than target. The no-network replay validated all 202 immutable request
commits with zero requests.

Measured throughput was 1.16315 total requests/second. An auditable census of
4,586,979 tickers projects 4,586,981 requests, about 45.64 days, and
9,508,027,682 namespace bytes after including raw, normalized, request-commit,
and manifest storage. It violates both storage guards and is operationally
infeasible. Phase 10F production remains unauthorized. The next methodology
gate is a family-aware, outcome-blind contract-subsampling design.

## Phase 10F-C — offline estimand and sampling design complete; approval required

The outcome-blind eligible frame is frozen at 112,166 families and 4,586,979
contracts, nested within 161,343 anchor-valid in-window families. The proposed
primary target gives equal conceptual weight to each eligible family and then
to contracts within family; a uniformly selected eligible contract is the
separate robustness target. Missing pre-target prices remain a measurement
limitation and never invalidate an anchor.

The proposed two-stage design uses stratified SRSWOR of families by approved
rule, category, verified-anchor month, and deterministic family-size bin,
followed by uniform SRSWOR of contracts within selected families. The leading
design is 5,000 families with a three-contract cap: approximately 13,033
tickers, 13,035 requests including fixed controls, 3.11 hours, and 27,014,949
auditable bytes at B2 empirical rates. No production family or contract has
been selected.

The PR1 feasibility warning remains decisive for the current cross-category
primary candidate: B2 observed 0/135 PR1 midpoint quotes within 15 minutes
(Wilson planning upper bound 2.77%). The leading design projects zero PR1
observations at the point estimate versus approximately 4,515 PR2 observations.
Midpoint <=15m is therefore viable for a PR2-specific analysis on current
evidence, not as a defensible cross-category primary measure. PR1 and PR2 must
not be automatically pooled.

Seven compact ignored design artifacts add 34,405 bytes. Shared generated
usage is 5,307,526,419 bytes, leaving 61,182,701 bytes below the 5 GiB ceiling.
At publication, free disk was 89,737,601,024 bytes, 3,838,255,104 bytes above
the 80 GiB floor. No network request, new price, outcome access, archive,
deletion, production sample draw, or StudyRules change occurred. Compilation,
deterministic rerun, artifact-hash acceptance checks, and all 724 offline tests
pass.

## Phase 10F-D — approved PR2 sampling manifest complete; network approval required

The project owner approved Phase 10F-C with modifications: the family-weighted
and contract-weighted estimands are frozen conditional on qualifying price
observability; confirmatory acquisition is PR2-only; PR1 anchors remain valid
but 47,391 structurally eligible PR1 families carry downstream status
`valid_anchor_but_primary_price_source_not_viable`. PR1 and PR2 may not be
pooled.

The offline PR2 draw contains exactly 5,000 unique families and 11,573 unique
contracts from the 64,775-family, 319,364-contract structurally eligible PR2
population. Stage-one strata use verified-anchor month × family-size bin; all
51 nonempty strata are represented. Stage two is uniform SRSWOR with at most
three contracts per family. Every row preserves both inclusion-probability
factors, their product, and separate raw family-/contract-target weights.

Validation found zero duplicate families, zero duplicate selected tickers, a
maximum of three contracts per family, and exact probability/weight identities.
Stage-one weights reconstruct all 64,775 families; the contract Horvitz–
Thompson size estimate is 322,564.344 versus the known 319,364-contract frame,
which is ordinary sample variation. Deterministic gzip manifests and the
no-network resume reproduce commit identity
`8a95158441c245988d2562b732762d9a6f3c5c9cd6d0bb33b9fcc6f3b8de2bc9`.

B2 PR2 availability projects 7,834 midpoint-15m contracts (Wilson planning
range 6,436–9,003). The conservative one-ticker-per-family projection is 3,385
observable families (2,781–3,890) and family ESS 3,366 (2,766–3,869), above
the frozen overall thresholds in the planning range. These are source-planning
estimates, not acquired prices.

Future acquisition would require 11,575 requests including cutoff and boundary
controls, about 9,951 seconds (2.76 hours), and 23,988,862 auditable bytes at B2
rates. It is not authorized. Phase 10F-D generated 366,858 compact local bytes,
made zero network requests, acquired zero prices, accessed zero outcomes,
changed zero anchors, and left StudyRules unchanged. All 730 offline tests,
compilation, deterministic resume, hash checks, and the acceptance review pass.
Fresh acceptance usage is 5,307,893,277 generated bytes with 60,815,843 bytes
of namespace headroom. Free disk is 89,212,518,400 bytes (83.09 GiB),
3,313,172,480 bytes above the floor. At current projections a later authorized
acquisition would leave 36,826,981 namespace bytes and 3,289,183,618 free-space
margin bytes; both must be recomputed immediately before any network run.

## Phase 10F-E — frozen PR2 price acquisition complete; pre-outcome gate reached

The immutable Phase 10F-D sample was preserved exactly: 11,573 contracts from
5,000 PR2 families under sampling commit
`8a95158441c245988d2562b732762d9a6f3c5c9cd6d0bb33b9fcc6f3b8de2bc9`.
All 116 acquisition partitions are complete. Of the 11,573 frozen contract
requests, 11,572 succeeded and the previously preserved transport failure
remains an explicit `API/data_failure`; no replacement was drawn. Acquisition
recorded three retries and zero rate limits.

A bounded investigation of request 11,060 established an unambiguous live
quote schema. All bid/ask candles had documented dollar closes. One earlier
candle lacked a documented trade close while a later valid trade close was
available. The normalizer therefore treats quote and trade validity
independently, never substitutes `previous_dollars`, and records the isolated
condition as `trade_schema_unavailable`. Immutable raw bytes and a raw-capture
commit are now published before normalization so future schema failures remain
auditable and resumable.

The primary midpoint-at-t−1h sample with at most 15 minutes of staleness has
9,388 contracts and 4,377 observable families. Contract-target weighted
coverage is 0.696388; family-target weighted coverage is 0.816086. Family-
weighted ESS is 4,208.115 and contract-weighted ESS is 3,155.763, so the frozen
500-family and family-ESS-500 final gate passes. Robustness samples contain
10,644 contracts/4,735 families for midpoint <=60m, 5,455/3,178 for trade
close <=15m, and 7,213/3,855 for trade close <=60m.

Primary exclusion consists of 928 contracts with no pre-target candle, the one
preserved API/data failure, and 1,256 otherwise valid midpoints that are between
15 and 60 minutes stale. There are zero missing-bid, missing-ask, or greater-
than-60-minute stale-midpoint cases. Trade diagnostics additionally contain
3,430 no-trade cases and one trade-schema-unavailable case. No post-target
candle, duplicate request,
inclusion-probability change, outcome field, or outcome access was found.

Final commit identity is
`79e022b7d9d359b484632e82671ef0095eba040687a21bc9a34a9bb947cf08de`.
The deterministic no-network replay and all 752 offline tests pass. Guarded
namespace use is 5,348,064,156 bytes, leaving 20,644,964 bytes below the 5-GiB
ceiling. Free disk at finalization was about 91.18 GiB, above the 80-GiB hard
floor. Phase 10F-E stops at the required final pre-outcome sample audit; no
outcome was accessed and no favorite–longshot estimate was calculated.

## Phase 10F final pre-outcome audit — complete; outcome release not authorized

The deterministic final audit independently rebuilt all 116 Phase 10F-E
partitions, rehashed 11,575 request commits and 8,860,920 compressed raw bytes,
reconstructed every inclusion probability and raw weight, and reproduced all
four analysis projections exactly. It found zero duplicate tickers or request
identities, zero post-target rows, zero previous-price fallbacks, zero anchor or
StudyRules changes, and zero outcome or settlement columns in research
artifacts. A second run reused the immutable audit with no write or network
request.

The primary sample retains 9,388 contracts, 4,377 families, and family-
weighted ESS 4,208.115. All ten fixed 0.10 probability bins independently pass
the frozen 100-family/ESS-100 bin gate. Sports is the only category and passes
the subgroup gate. Anchor months November 2025 through April 2026 pass the
200-family/ESS-150 subgroup gate; other months are descriptive. Family-size
bins `2-5` and `6-25` pass; `1`, `26-100`, and `101-400` are descriptive.

Outcome-blind observability diagnostics show nontrivial composition differences
that must remain a limitation: the family-weighted standardized difference in
hours since market open is 0.257; maximum absolute observed-versus-missing
share differences are 0.197 by family-size bin, 0.123 by month, and 0.027 by
target UTC hour. No observation-propensity correction is approved or applied.

The audit identity is
`bd14ba156585c4b2ed43c798ea55c977e8496326642edca9748eb703491eab24`.
The recorded pre-outcome analysis plan SHA-256 is
`1da09fbe7d8fc14a25109c7ebd1f66969ca61e3f64f3cf32b5703dd5109da73b`.
Its proposed scalar contrast and stratified family-cluster bootstrap require
explicit approval together with outcome-quarantine release. No outcome was
accessed and no favorite–longshot calculation was performed. The focused
Phase 10F suite passes 57 tests, the full offline suite passes 760 tests, and
compilation, Black, pyflakes, TOML validation, and `git diff --check` pass.

## Phase 10G — approved outcome release and frozen initial analysis complete

The owner approved the final pre-outcome audit, the recorded Phase 10F
analysis plan, and release of only the minimal binary-outcome projection.
Phase 10G therefore joined outcomes in memory to the unchanged frozen PR2
sample and persisted only contract identifier, frozen sample identifier, and
binary resolution outcome. The release has 11,573 unique contract rows:
11,495 binary resolutions and 78 unresolved/nonbinary source results. It
contains no settlement timestamp, settlement value, post-resolution metadata,
price, anchor, weight, or eligibility field.

Resolution coverage is high but not perfect. Of 5,000 frozen families, 4,964
have at least one resolved contract, 45 have at least one unresolved contract,
and 36 have no resolved contract. In the primary price-observable sample,
9,353 of 9,388 contracts resolve, covering 4,360 of 4,377 families; 35
contracts are unresolved and 17 families have no resolved primary contract.
No unresolved contract was replaced or filtered before the availability
comparison.

Under the pre-specified primary family target, the weighted calibration gap
`Y-P` is 0.00285 (95% family-cluster bootstrap interval -0.00333 to 0.00906).
The predeclared favorite-longshot contrast,
`gap(P<0.20) - gap(P>=0.80)`, is 0.01004 (95% interval -0.02358 to 0.04565).
The classical favorite-longshot pattern would imply a negative contrast, so
the primary estimate does not provide statistically distinguishable evidence
of that pattern in this frozen, price-observable PR2 Sports sample. This is not
evidence that bias is absent in Kalshi generally: the sample is conditional on
PR2 eligibility and price observability, and the audit documented
observability-related composition differences.

All pre-specified robustness contrasts likewise have intervals spanning zero:
midpoint <=60m 0.00259, trade close <=15m 0.00607, trade close <=60m 0.00777,
midpoint <=15m with spread <=0.20 0.00908, and midpoint <=15m with spread
<=0.10 0.00807. The secondary contract-target contrast is 0.02912 (95%
interval -0.01530 to 0.07528). One of ten descriptive calibration-bin
intervals excludes zero; it is not a pre-specified standalone effect and does
not form a monotone favorite-longshot pattern.

Phase 10G uses 10,000 deterministic stratified family-cluster bootstrap
replicates and reproduces the published artifacts without network access. The
authoritative v3 commit identity is
`931a1d35de134e91eee3ed71041a712414c1435fbcd37f1ffc28b263e746252e`.
V2 added the already-approved spread sensitivities and clearer family-
resolution diagnostics; v3 adds the analysis-plan-required pre-estimation
fingerprints for the minimal outcome projection and the nonpersisted in-memory
joined sample. All earlier output roots are preserved and all shared numerical
estimates are identical. The full offline suite passes 766 tests. Generated
namespace use is 5,348,508,429 bytes, leaving 20,200,691 bytes below the
ceiling; free disk is approximately 100.42 GiB. No network request, sample
redraw, anchor change, price change, weight change, StudyRules change, or
post-outcome eligibility change occurred.

## Phase 10H — deterministic paper-ready reporting complete

The frozen authoritative Phase 10G v3 analysis has been converted into a
paper-ready reporting package under `reports/phase_10g/`. The package contains
separate Methods, Results, Discussion, Limitations, and mentor-summary drafts;
four CSV tables; four figures in both publication PNG and editable SVG formats;
a combined paper report; and a reproducibility manifest.

Every numerical table and figure is generated directly from hash-pinned Phase
10B/10E/10F/10G artifacts. The generator rejects changed source hashes or a
changed Phase 10G identity. A second execution reproduced every byte. The
reporting code commit is
`250b9d3f3f1117b7f421020c80b368f2eb02bf5e`, and the reporting-manifest
SHA-256 is
`db298df905de11e145638d1f633f6829b5a9006f8012f0c02755e5f38443ccc8`.

The reporting preserves the approved conclusion: in the pre-specified PR2
Sports sample, no statistically distinguishable evidence of favorite-longshot
bias is detected. It does not generalize the finding to Kalshi as a whole,
claim perfect calibration, reinterpret the isolated descriptive decile, or
promote a robustness definition. The frozen sample, outcomes, estimands,
weights, prices, inference, thresholds, and exclusions are unchanged. No
network request or new inferential analysis was performed. The acceptance
review verified all table cells against their pinned sources, visually
inspected all four figures, reproduced all 19 files byte-for-byte, and passed
the full 769-test offline suite plus scoped Black, pyflakes, compilation, TOML,
and `git diff --check` validation.
