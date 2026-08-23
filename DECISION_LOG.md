# Decision Log

## 2026-07-17 — Historical settlement dates are not server-filterable

Status: accepted implementation fact

Kalshi's `GET /trade-api/v2/historical/markets` endpoint does not accept
settlement-time range parameters. The repository's `segment_params()` correctly
sent settlement bounds only to `GET /trade-api/v2/markets`, while the planner
incorrectly treated the historical archive scan as if local monthly filtering
made the acquisition itself bounded.

Primary references: [Get Historical Markets](https://docs.kalshi.com/api-reference/historical/get-historical-markets)
and [Historical Data](https://docs.kalshi.com/getting_started/historical_data).

Decision: never describe a historical settlement envelope as a server-side
filter. Traverse the historical archive in bounded cursor partitions, record
the lack of server-side selectivity, and apply the acquisition envelope during
immediate normalization. Live monthly requests may continue to use
`min_settled_ts`/`max_settled_ts` server-side.

## 2026-07-17 — Use bounded cursor partitions for historical acquisition

Status: accepted architecture decision

Monthly historical requests would each rescan the entire archive because the
endpoint has no date filter. Therefore historical raw acquisition partitions
are fixed-size cursor segments, not nominal calendar requests. Each partition
has its own immutable compressed pages, normalized quarantine outputs,
provenance, and validated commit. Calendar month remains a normalization field
and the live endpoint remains calendar-partitioned.

This is an operational/storage decision and does not change sample membership,
the frozen analysis window, anchor semantics, or `StudyRules`.

## 2026-07-17 — A partition commit is the unit of resumability

Status: accepted architecture decision

A raw page is reusable cache evidence but is not completion evidence. A
partition is complete only after its page chain, hashes, normalized metadata,
quarantined outcomes, provenance, and summary are atomically referenced by a
validated commit. The next partition starts at the prior committed end cursor.
Uncommitted pages can be reused on resume but cannot advance the chain.

## 2026-07-17 — Fail closed on incomplete archive traversal

Status: accepted architecture decision

Bounded historical partitions are expected to leave the overall archive scan
incomplete until a terminal cursor is committed. Every run must report that
state explicitly. Deterministic merge may inspect committed partial data but
must not publish or label a final complete universe while the chain is
incomplete.

## 2026-07-17 — The million-row scale is empirically in-range

Status: accepted acquisition finding

The guarded production cursor chain passed the old failure point and committed
1,150 historical pages containing 1,150,000 rows. Immediate local validation
classified every row as inside the provisional acquisition envelope and found
no rejects. Pagination used 46 contiguous committed cursor partitions with
zero retries or cursor-integrity errors. A direct cross-partition audit found
1,150,000 distinct tickers and zero repeated tickers.

Finding: the earlier ~906,000-row scale was not created by duplicate pagination
or repeated market tickers, and it cannot be dismissed as entirely out-of-range
data. At least 1.15 million distinct tickers in the current historical traversal
genuinely satisfy the provisional settlement envelope. The endpoint still
lacks server-side date filters, the archive remains nonterminal, and this
acquisition finding does not change the frozen analysis window or define the
eventual analysis sample.

## 2026-07-17 — Historical traversal must reach the terminal cursor

Status: accepted acquisition finding

The complete historical chain contained 8,777,951 records. Although the first
millions were almost entirely inside the provisional settlement envelope,
later partitions contained mostly out-of-range rows while still intermittently
containing valid in-range records. The terminal result was 7,370,758 in-range
and 1,407,193 out-of-range records.

Decision: do not infer a safe early-stop boundary from settlement timestamps or
page position. Historical completion requires the server's terminal cursor.

## 2026-07-17 — Use the exact-audited compressed streaming merge at scale

Status: accepted operational decision

The complete acquisition produced 9,861,209 in-range rows. Materializing the
legacy merge in memory and publishing uncompressed copies could not be safely
guaranteed under the 5 GiB namespace ceiling. The production fast path therefore
performs an exact external ticker audit, requires zero duplicates, streams each
validated partition in deterministic order, and publishes gzip artifacts under
the same byte and free-space guards.

The exact audit found 9,861,209 distinct tickers, zero duplicates, and zero
conflicts. Because selection was unnecessary, row provenance remains
losslessly anchored in the 454 partition commits and is referenced by a compact
final provenance manifest. This changes storage mechanics only; it does not use
outcomes to select metadata or alter the study methodology.

## 2026-07-18 — Event metadata uses source-indexed bounded partitions

Status: accepted operational decision

The production event universe is the authenticated, sorted
`event_tickers.csv.gz` artifact from Phase 10B. Phase 10C partitions that
ordered universe into deterministic 5,000-event slices, each containing
25 request batches of at most 200 tickers. The partition identity binds the
Phase 10B source hash, scope, offset, ticker digest, and effective request
configuration. A limited smoke has a separate scope identity and cannot be
mistaken for completion of the production universe.

Each partition independently publishes immutable compressed raw responses,
outcome-quarantined normalized event and milestone rows, research provenance,
a request manifest, a normalization report, and a validated commit. Cached
pages are reusable evidence, but only a commit advances the source offset.

## 2026-07-18 — Reserve partition and final-merge storage in preflight

Status: accepted safety decision

The 5 GiB ceiling applies to the shared generated namespace, including the
immutable Phase 10B data. Event preflight therefore measures current bytes at
the Phase 10B raw root and reserves estimated space for remaining compressed
raw pages, remaining normalized partition artifacts, and the final compressed
event-universe merge. The same root retains the 80 GiB minimum-free-space
guard for every actual publication.

This intentionally reserves both required normalized representations:
independently auditable partition outputs and the deterministic final research
universe. A preflight that omits the final copy is not sufficient authority for
network acquisition.

## 2026-07-18 — Event timestamps remain unverified candidate evidence

Status: reaffirmed frozen methodology

Recursive research projection removes outcome, result, settlement, close, and
expiration fields at every nesting depth before event or milestone metadata is
published for research use. Immutable raw responses may retain those fields as
audit evidence, but their hashes do not enter normalized research features.
Strike and milestone dates remain candidate evidence only. Phase 10C neither
runs anchor verification nor changes any verification status.

## 2026-07-18 — Preserve authenticated parenthesized event tickers

Status: accepted compatibility finding

The first production preflight stopped before network or writes because the
legacy event-ticker validator rejected 43 authenticated Phase 10B rows. All 43
belong to `KXCITIESWEATHER-24DEC13` and use balanced parenthesized uppercase
alphanumeric city tokens. The source contained no other unrecognized
characters, no duplicate tickers, and no ordering failures.

Decision: accept zero or more balanced `(TOKEN)` suffixes in addition to the
existing uppercase alphanumeric, hyphen, and period grammar. Commas,
whitespace, unbalanced parentheses, empty tokens, punctuation inside tokens,
and lowercase remain invalid. This preserves source identities exactly and is
an API compatibility correction, not a methodology or sample-membership
choice.

## 2026-07-18 — Recover collection omissions through documented exact routes

Status: accepted operational correction

The first 200-event network smoke committed an incomplete partition after the
collection request returned 194 requested events and omitted six. All six were
then retrieved successfully through the documented single-event endpoint. A
six-ticker collection retry still returned none, and the documented
multivariate endpoint returned none for their series. The omission is
therefore an endpoint collection behavior, not a nonexistent identifier,
historical cutoff, request-size, or multivariate-routing issue.

Decision: retain the efficient collection request, then deterministically fetch
each omitted requested ticker through `GET /events/{event_ticker}` and query
`GET /milestones?related_event_ticker=...` for its milestone evidence. Both
fallback responses receive the same immutable gzip, cursor, hash, budget,
quarantine, provenance, and commit protections as collection pages. The
fallback strategy is part of the acquisition scope identity, so the original
incomplete smoke remains immutable and cannot be confused with the corrected
scope.

Preflight now reports minimum, empirical estimated, and worst-case request
counts. Its empirical estimate uses the observed 3% collection-omission rate
until a successful corrected smoke provides a sharper storage and request
check. Exact fallback preserves the Phase 10B event universe; silently dropping
collection omissions is prohibited.

## 2026-07-18 — Reconcile milestone freshness markers without changing evidence

Status: accepted operational correction

Production stopped in the uncommitted fourth partition when one milestone ID
had two projections. Raw-page comparison showed every research and anchor
candidate field was identical; only `last_updated_ts` differed. Collection
responses supplied the sentinel `0001-01-01T00:00:00Z`, while the documented
related-milestone fallback supplied the real 2026 update timestamp.

Decision: exclude only source freshness markers (`last_updated_ts` and its
legacy alias) from milestone conflict identity, count every timestamp variant,
and retain the lexically latest RFC3339 timestamp deterministically. Title,
category, type, start/end dates, details, source identifiers, and event
associations remain conflict-critical and still fail closed. This resolution
uses neither outcomes nor anchor verification and permits all validated event
partitions and uncommitted raw pages to be reused.

## 2026-07-18 — Accept the complete partitioned event universe

Status: accepted production checkpoint

The production scope completed all 86 deterministic partitions and retrieved
all 427,090 authenticated Phase 10B event tickers. Kalshi collection responses
omitted 316 requested identifiers; the already accepted exact-event and
related-milestone fallback recovered every omission. There were no missing,
duplicate, or conflicting event rows, no merge conflicts, and no request
retries or rate limits.

Decision: freeze event merge `69f1b1277bdfdbd530834fe6` as the Phase 10C
production input to candidate anchor-evidence construction. Its compressed
event metadata, milestone associations, and provenance are eligible research
inputs; raw responses and Phase 10B outcomes are not. The merge neither
verifies an anchor nor changes `StudyRules` or the analysis window. Any future
replacement requires a new auditable scope and merge identity; Phase 10B and
this validated Phase 10C checkpoint remain immutable.

## 2026-07-19 — Group equivalent market occurrence evidence at family level

Status: accepted operational representation

The raw Phase 10B universe contains 3,778,206 market rows with a nonempty
`occurrence_datetime`, but only 208,308 distinct normalized occurrence values
within the same composite family and event ticker. Serializing one identical
candidate per contract would repeat evidence, obscure the family-level review,
and exceed the shared generated-data ceiling.

Decision: represent each identical normalized market occurrence value once per
`(family_id, family_id_source, event_ticker)`, while recording the number and
first/last identifiers of supporting market sources. Different normalized
times remain separate candidates, and repeated source identities with
different approved context fail closed. Event strike and milestone candidates
retain their source identities. This is lossless for candidate values and
family conflict detection, does not select an anchor, and does not change the
frozen methodology.

## 2026-07-19 — Accept the complete Phase 10D candidate-evidence universe

Status: accepted production checkpoint

Phase 10D produced 625,923 unverified candidates for all 427,090 composite
families. The outputs contain 208,308 market-occurrence, 209,017 event-strike,
and 208,598 milestone-start candidates. All candidates are exact timestamps;
418,591 families have evidence and 8,499 have none. There are zero missing
event-metadata families, invalid values, sentinels, or multi-event-ticker
families. Every review and decision row remains `needs_review`, all verified
fields are blank, and outcomes were unavailable.

Decision: freeze the four artifacts under
`data/pipeline_v2/anchor_evidence/phase_10d/` as the production input to the
separate review handoff. The 198,686 families with multiple distinct exact
candidate times are review findings, not errors and not automatic exclusions.
No candidate becomes eligible until an approved verification decision is
applied. Phase 10D does not authorize horizon construction or outcome access.

## 2026-07-19 — Propose staged Phase 10E verification tiers for audit

Status: pending explicit approval; no rule accepted

Outcome-blind Phase 10D diagnostics show 217,835 families with one exact
candidate, 2,070 with multiple candidates agreeing on one exact time, 198,686
with multiple distinct exact times, and 8,499 with no candidate.

Proposal: audit PR1 on 102,413 single-exact families whose existing semantic
heuristic proposes `fixed_clock`; audit PR2 on 93,997 Sports families with one
unique official milestone-start time, conservative title/context agreement,
and no subevent or window flag. Assign the remaining 230,680 families to Tier
3 manual review. These labels are sampling strata only and do not verify any
anchor.

The deterministic audit contains 150 families per tier. Proposed approval
rates remain unobserved until review, and 50 independently double-reviewed
families per tier are recommended for disagreement measurement. An early
packet version admitted endogenous set-level subevents into Tier 2; acceptance
review identified the failure, the rule was tightened, and that version was
retained under `phase_10e_design_rejected_v1` rather than deleted. PR1 and PR2
must not be applied without explicit project-owner approval.

## 2026-07-19 — Complete recommendation-only AI first review

Status: audit evidence recorded; no rule accepted

All 450 deterministic audit cases were reviewed using only the supplied
outcome-blind candidate evidence, titles, categories, timing context, and
allowed ex-ante metadata. Every actual verification status remains
`needs_review`. Tier 1 received 86 rule-case, 60 uncertainty, and four rejection
recommendations; Tier 2 received 135 rule-case, 13 uncertainty, and two
rejection recommendations; Tier 3 remains fully quarantined.

Decision: treat these results only as AI first-review diagnostics. The
inverse-probability-weighted rule-case recommendation rates of 50.10% for PR1
and 92.34% for PR2 are not approval rates and cannot promote either rule. The
independent human review must cover deterministic 50-case sets from Tier 1 and
Tier 2, every low-confidence, rejected, or ambiguity-flagged Tier 1–2 case, and
10 Tier 3 quarantine diagnostics, for 165 unique cases. Human approval,
confirmed false-positive, and AI-human disagreement rates must be presented
before the project owner may approve, modify, or reject either proposed rule.

## 2026-07-19 — Approve the review workflow but not the proposed rules

Status: implementation approved; PR1 and PR2 not approved

The project owner approved the Phase 10E design and outcome-blind first-review
implementation, including commits `ee75bb1` and `6473c35`. Human review remains
incomplete; outcomes and prohibited post-event information remain unopened.
This approval does not authorize verified anchors, horizons, outcomes, or rule
application.

Direction: PR1 must exclude recurring short-duration contracts whose one-hour
horizon can precede market existence, non-exact deadlines/windows, publication
timing, settlement-language ambiguity, and multiple scheduled-time ambiguity.
PR2 remains potentially viable but must exclude set, map, or series-level
markets; endogenous, partial, or conditional subevents; ticker/candidate date
mismatches; unrelated milestones; and multiple plausible start times. These
are audit directions, not promoted rule definitions.

Decision: implement a compact local interface over the immutable 165-case
subset. Human entries remain recommendation-only `needs_review` records. Only
after the complete human audit produces weighted approval, confirmed false-
positive, AI-human disagreement, category, and failure-mode diagnostics may the
project owner separately approve, modify, or reject PR1 and PR2.

## 2026-08-22 — Reclassify the finalized 165-case annotations and require a fresh validation

Status: AI-assisted evidence accepted; PR1 and PR2 not approved

The project owner clarified that AI assistance was used during annotation and
correction of the finalized 165-case table. Decision: label this artifact only
as an `AI-assisted outcome-blind review`. It must not be used or reported as an
independent human review, and the finalized decisions must not be altered.

The hash-pinned import confirms 149 approvals, five rejections, and 11
uncertain recommendations, including every specified correction. The import
is written separately from immutable evidence and projects every case as
`needs_review`, with blank verified-anchor fields. Its weighted and unweighted
diagnostics are descriptive audit evidence only.

For independent validation, draw a fresh deterministic and stratified sample
of 50 PR1 and 50 PR2 cases. Exclude Tier 3 from rule-approval inference and
withhold AI recommendations and AI-assisted decisions from the reviewer. Load
the AI-assisted comparator only after all 100 decisions are complete. Neither
rule may be promoted until those human approval, false-positive, uncertainty,
and disagreement diagnostics are presented for explicit approval.

## 2026-08-22 — Complete independent human validation and recommend rule modifications

Status: independent validation complete; PR1 and PR2 not approved

The independent reviewer completed all 100 blinded cases without access to AI
recommendations, AI-assisted decisions, outcomes, settlement results,
post-event information, or post-anchor prices. The review contains 95
approvals, five rejections, zero uncertain decisions, and 100 high-confidence
ratings. The literal reviewer entries are preserved exactly, including all
eight disagreements with the AI-assisted review.

PR1 has 47 approvals and three rejections; inverse-probability-weighted
approval and false-positive rates are 97.17% and 2.83%. PR2 has 48 approvals
and two rejections; weighted rates are 99.96% and 0.04%. Weighted disagreement
rates are 2.82% for PR1 and 1.27% for PR2. These estimates are accompanied by
Kish-effective-sample-size Wilson intervals conditional on the upstream audit
sample.

Recommendation: MODIFY both rules before approval. PR1 must retain exclusions
for short-duration/horizon-existence risk, non-exact deadlines or windows,
publication/reporting/settlement timing, multiple scheduled times, and semantic
or timestamp mismatch. PR2 must retain exclusions for set/map/series markets,
endogenous or conditional subevents, date mismatch, unrelated milestones,
multiple plausible starts, and post-event settlement/reporting timestamps.
The zero-uncertainty and all-high-confidence response pattern must be reported
as a possible reviewer-style limitation.

This recommendation does not approve or apply either rule. Every status remains
`needs_review`; zero anchors, horizons, prices, or outcomes were opened.

## 2026-08-23 — Approve and apply modified PR1-M and PR2-M outcome-blind rules

Status: accepted and applied; Phase 10F not started

The project owner explicitly approved modified PR1-M and PR2-M after the
AI-assisted review and fresh independent-human validation. PR1-M requires a
single exact predetermined fixed-clock candidate, with exact contract-reference
agreement and no competing clock, deadline/window, publication, platform
settlement, or semantic/date ambiguity. An official benchmark settlement value
is allowed only when its observation time and value are exactly the contract
reference. Short duration is not an anchor exclusion; t-1h availability is a
separate price-stage question.

PR2-M requires a unique exact official milestone start for the exact event
scope with title/event/milestone agreement. Endogenous or conditional
subevents, partial-event scopes, set/map/series ambiguity, date/scope conflicts,
unrelated or post-event milestones, multiple starts, and sub-minute values not
consistent with a predetermined schedule remain unverified.

Decision: apply both rules deterministically to the full outcome-blind family
universe. The accepted result verifies 98,625 PR1-M and 69,329 PR2-M families.
All 259,136 uncovered or excluded families remain `needs_review`; none becomes
`rejected`. Two acceptance-review snapshots are retained because initial
classification admitted first-goalscorer, first-five-innings, and sub-minute
scheduled-start cases. No data was deleted.

This decision authorizes only anchor verification. It does not authorize
outcome access, horizon construction, price acquisition, changing the frozen
analysis window or `StudyRules`, or exceeding storage guards. Phase 10F must
first produce an outcome-blind request/storage preflight and measured bounded
smoke. Any destructive archival or pruning requires separate approval.

## 2026-08-23 — Complete Phase 10F-A offline planning and defer price definition

Status: offline plan accepted; analytical price definition pending approval

The Phase 10E anchors were joined without outcomes to the Phase 10B market
metadata. For each in-window family, the planner subtracts exactly one hour,
retains every associated market ticker through a lossless compact encoding, and
compares only market `open_time` with target time. This separates anchor
validity from market existence and later price/staleness eligibility.

Decision: label the 49,177 families that opened after target as
`valid_anchor_but_no_t_minus_1h_market`; never revoke their verified anchors.
The other 112,166 families proceed only to price-availability testing. No
market-existence status is unknown offline.

The leading source for a bounded smoke is the existing multi-market one-minute
candlestick endpoint because it batches tickers sharing a target window and
exposes trade and bid/ask fields. This is a source recommendation, not network
authorization. The current uncompressed/non-atomic cache and silent
midpoint/trade fallback are not accepted for production.

Recommendation pending explicit approval: define the primary probability as
the contemporaneous closing yes-bid/yes-ask midpoint when both sides exist, and
report actual trade close as a separate robustness measure. Do not substitute
one for the other within a sample. Keep the primary 15-minute and robustness
60-minute staleness thresholds separate and never accept a post-target candle.

Full acquisition is storage-infeasible under current guards. The empirical
projection requires about 2.22 GB and the conservative projection 10.69 GB.
Only the deterministic 200-family smoke is storage-feasible at 29.43 MB
conservative. No smoke, archive, move, deletion, price request, or outcome
access is authorized by this decision.

## Standing frozen decisions

- Analysis anchor window:
  `[2025-07-01T00:00:00Z, 2026-07-01T00:00:00Z)`.
- Allowed timing structures: `fixed_clock`, `scheduled_event_start`.
- Allowed results: `yes`, `no`.
- API occurrence, strike, milestone, close, expiration, settlement, and outcome
  fields never become verified anchors automatically.
- Outcome data remains unavailable until features, anchors, horizons, targets,
  prices, and sample inclusion are frozen.

## 2026-08-23 — Phase 10F-B live-batch route rejected by bounded smoke

Status: hard stop; no price accepted

The project owner approved the deterministic 200-family smoke with a closing
YES bid/ask midpoint primary measure, actual trade close as a separate
robustness measure, 15-/60-minute staleness thresholds, and empirical inclusive-
end boundary validation. The implementation and 703-test offline acceptance
suite passed, and the storage preflight remained within both guards.

Decision: reject the live multi-market batch endpoint as a validated route for
this archived smoke scope. Every one of 206 successfully committed responses
contained `{"markets":[]}`. Thus 12,137 requested tickers yielded zero market
objects and zero candles. This is an endpoint-routing failure, not liquidity or
staleness attrition. No boundary semantic was empirically established, no
normalized price was published, and the primary price definition is not
permanently frozen.

Do not automatically switch to the per-market historical endpoint. That change
raises the smoke request count from 206 batched requests toward one request per
ticker and materially changes production feasibility. A smaller deterministic
historical-route validation and revised request model require explicit approval.
