# Project Status

Last updated: 2026-08-22
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

## Critical path

1. Complete the fresh independent human review of the 100-case blinded packet
   without outcomes, post-event information, post-anchor prices, or access to
   the AI-assisted decisions.
2. Compute human approval, AI-human disagreement, and confirmed false-positive
   diagnostics; obtain explicit approval, modification, or rejection of PR1
   and PR2.
3. Only after approval, apply and validate decisions. Then construct horizons
   and prices, freeze the pilot sample, and merge outcomes last.

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
  floor. No fresh independent-human decision or report exists yet.
