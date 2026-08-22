# Phase 10E Outcome-Blind Verification Audit and Pilot Plan

## Status and approval boundary

Phase 10E has completed the design, sampling, recommendation-only AI first
review, finalized 165-case AI-assisted outcome-blind review import, and fresh
human-validation packet. The independent human validation remains at 0/100.
The project has not
verified an anchor, applied a verification rule, constructed a horizon price,
read an outcome, or estimated favorite–longshot bias. The labels below are
proposed audit tiers, not eligibility decisions. Both proposed deterministic
rules remain `proposed_not_approved` and require explicit project-owner
approval after the audit decisions are recorded and evaluated.

The analysis used only the immutable Phase 10D family and candidate evidence
and the outcome-free Phase 10C event projection. The StudyRules fingerprint
remains `12d6955f57b50b5587fdadf02b2bc96e7de48d022c9ac3cc2fe0425d907b9901`.

## Candidate-evidence patterns

The 427,090-family universe partitions exactly as follows:

| Evidence pattern | Families | Share | Interpretation |
|---|---:|---:|---|
| One exact candidate | 217,835 | 51.00% | Mechanically simple, but timing semantics and source meaning still require audit |
| Multiple candidates, one exact time | 2,070 | 0.48% | Sources agree on time, but duplicate/source semantics still require review |
| Multiple distinct exact times | 198,686 | 46.52% | Competing times; manual review is required |
| No candidate | 8,499 | 1.99% | No permitted Phase 10D evidence is available |

No production family has a date-only candidate, invalid candidate value,
year-0001 sentinel, missing event metadata, or multiple event ticker. Those
failure modes remain assigned to Tier 3 if encountered in a future universe.

By source combination, the universe contains 107,484 event-strike-only
families, 108,293 milestone-only families, 3,439 market-occurrence-only
families, 100,771 occurrence-plus-strike families, 97,842
occurrence-plus-milestone families, 128 strike-plus-milestone families, 634
families with all three source types, and 8,499 with none.

## Proposed outcome-blind tiers

| Tier | Families | Proposed treatment | Approval status |
|---|---:|---|---|
| Tier 1 | 102,413 | Audit a single allowed exact candidate where the existing outcome-blind timing heuristic proposes `fixed_clock` | Not approved |
| Tier 2 | 93,997 | Audit one unique exact official milestone-start candidate for a Sports family with conservative event/milestone/family context agreement and no subevent/window flag | Not approved |
| Tier 3 | 230,680 | Manual review, rejection, or continued quarantine | No scalable rule proposed |

### Proposed Rule PR1: fixed-clock, single exact candidate

The proposed rule requires exactly one candidate row, exactly one unique exact
time, no conflicting exact times, no multiple-event ambiguity, an allowed
frozen candidate source, and an outcome-blind `fixed_clock` timing proposal.
The pool contains 102,096 event-strike candidates, 314 market-occurrence
candidates, and three milestone-start candidates. An audit must establish that
the timing heuristic and source interpretation are sufficiently reliable; API
presence alone cannot verify the anchor.

### Proposed Rule PR2: scheduled start, single official milestone

The proposed rule requires one unique exact milestone-start candidate, no
conflicting times or multiple-event ambiguity, Sports category, conservative
informative-token agreement between event and milestone titles, at least one
informative family-context match, and no `endogenous_subevent`,
`scheduled_window`, or `deadline_window` timing flag. The proposed time is the
milestone start, and the proposed timing structure is `scheduled_event_start`.

An initial packet inspection identified set-level markets as a false-positive
mode: they can share the parent match milestone while their own timing is an
endogenous subevent. The rule was tightened before this approval packet was
published, moving 5,207 initially proposed families out of Tier 2. This is why
the audit precedes rule promotion.

## Audit design and review burden

The packet is a deterministic, stratified SHA-256 sample using seed
`phase-10e-outcome-blind-audit-v1`. It contains 150 families from each tier,
450 total. Tier 1 is stratified by category, candidate month/window status,
and source. Tier 2 is stratified by month/window status and title-agreement
class. Tier 3 is stratified by manual-review reason and category. Population,
allocation, and inverse sampling weight are recorded for every row.

At the worst-case 50% approval rate, 150 observations give an approximate 95%
margin of ±8.0 percentage points per tier before finite-population and design-
effect adjustments. Independently double-reviewing 50 cases per tier gives a
worst-case disagreement-rate margin of approximately ±13.86 points. Approval
and disagreement rates remain **[TO BE MEASURED AFTER HUMAN REVIEW]**. The AI
first review is available as a separate diagnostic: PR1 received 86 rule-case,
60 uncertainty, and four rejection recommendations; PR2 received 135, 13, and
two, respectively. Weighted AI rule-case recommendation rates are 50.10% and
92.34%, but these are not rule-approval estimates.

The finalized compact table contains 165 unique cases, but AI assistance during
annotation and correction means it is an AI-assisted outcome-blind review, not
an independent human review. It records 149 approvals, five rejections, and 11
uncertain decisions. For Tier 1/PR1, unweighted approval, rejection, and
uncertainty are 93.75%, 2.08%, and 4.17%; weighted values are 97.63%, 0.003%,
and 2.37%. For Tier 2/PR2, the corresponding rates are 94.92%, 3.39%, and
1.69% unweighted and 98.73%, 0.86%, and 0.41% weighted. These diagnostics do
not approve either rule.

The fresh independent-human packet contains 50 PR1 and 50 PR2 cases and
excludes Tier 3 from rule-approval inference. It is deterministically
stratified by category and observed failure mode and displays neither the
AI recommendation nor the AI-assisted decision. At four minutes per case, the
remaining burden is approximately 6.67 reviewer-hours. The local interface
atomically autosaves, resumes safely, and leaves the frozen verification
projection at `needs_review`.

Reviewing the full universe under the same assumptions would require about
3,413.77 Tier 1 hours, 4,699.85 Tier 2 hours, and 23,068 Tier 3 hours—31,181.62
hours in total. This impractical burden is the reason to audit scalable rules
before applying them and to leave Tier 3 quarantined from the initial estimate.

Audit reviewers record recommendation-only decisions and must leave every
sampled family at `needs_review`. No reviewer may mark a family
`verified_automatic` or `verified_manual` during this audit. Reviewers must not
consult outcomes, results, settlement values, settlement timestamps, prices,
close/expiration times, or other post-event information. Only after human
diagnostics and explicit project-owner rule approval may a separate application
step change verification status.

## Coverage of the proposed primary pool

Tier 1 contributes 97,369 families whose proposed candidate lies inside the
frozen window; Tier 2 contributes 92,097. The combined proposed in-window pool
is 189,466 families.

Tier 1 is concentrated in Crypto (94,454 total families), with additional
Climate and Weather (4,311) and Financials (3,638) coverage; its remaining
categories are very small. Tier 2 is entirely Sports by construction. The
initial primary design should therefore report category-specific estimates
for Crypto, Climate and Weather, Financials, and Sports rather than treating
the pooled sample as representative of Kalshi generally.

The combined proposed in-window pool covers every frozen-window month:

| Month | Proposed families |
|---|---:|
| 2025-07 | 3,603 |
| 2025-08 | 3,994 |
| 2025-09 | 4,460 |
| 2025-10 | 6,884 |
| 2025-11 | 11,395 |
| 2025-12 | 15,861 |
| 2026-01 | 30,113 |
| 2026-02 | 36,722 |
| 2026-03 | 46,621 |
| 2026-04 | 29,500 |
| 2026-05 | 151 |
| 2026-06 | 162 |

The strong concentration in January–April 2026 and sparse May–June coverage
must be preserved in diagnostics and sensitivity analyses.

## Selection and attrition risks

The largest risks are:

1. **Category selection:** Tier 1 is dominated by Crypto, while Tier 2 is
   restricted to Sports. Results cannot be generalized automatically to the
   full market universe.
2. **Time selection:** proposed eligibility is concentrated late in the
   window, so calendar conditions and platform maturity may be confounded with
   category composition.
3. **Semantic false positives:** an event-to-milestone association can be
   technically valid while the milestone is not the contract's relevant
   forecasting anchor. Title overlap is a conservative screen, not proof.
4. **Lexical false negatives:** abbreviations, team aliases, and player props
   may be valid despite weak title overlap, shifting defensible cases into
   Tier 3.
5. **Strike-date interpretation:** a precise event strike timestamp can encode
   a deadline or reporting convention rather than the fixed occurrence the
   research design requires.
6. **One-hour availability:** short-duration fixed-clock contracts, including
   15-minute crypto markets, may have no price one hour before the proposed
   anchor. Verification does not imply horizon eligibility.
7. **Family dependence:** many contracts share an event family; uncertainty
   must be clustered or aggregated at the family level.

Price matching has not been run, so empirical attrition is **[TO BE
COMPUTED]**. For planning only, applying 90%, 75%, and 50% family match rates
to the 189,466-family pool yields 170,519, 142,100, and 94,733 matched families.
These scenarios are not forecasts. Open-time and price-history availability
may make attrition systematic, especially for short-duration markets.

## Recommended initial pilot sample

After audit completion and explicit rule approval, the recommended minimum is
2,000 price-matched families: approximately 500 each from Crypto, Climate and
Weather, Financials, and Sports, distributed across available months. The
primary target is one hour before the verified anchor, with maximum 15-minute
price staleness. The pilot should be described as diagnostic preliminary
evidence, not a confirmatory estimate; power and effective sample size remain
unknown until market counts, family clustering, and price availability are
observed.

## Exact path to the first preliminary bias estimate

1. Complete the fresh independent outcome-blind human review of the blinded
   100-case subset.
2. Compute weighted approval rates, disagreement rates, reasons for rejection,
   and category/time-specific false-positive diagnostics.
3. Present those diagnostics for explicit approval, modification, or rejection
   of PR1 and PR2. No rule may be promoted implicitly.
4. If approved, version the exact rule specification, apply it without outcome
   access, and validate that all other families remain `needs_review` or are
   rejected.
5. Build the one-hour target manifest from verified families only. Then acquire
   or extract at-or-before prices with the 15-minute staleness ceiling and
   report price-matching attrition by tier, category, month, and family size.
6. Select and freeze the 2,000-family diagnostic pilot and all research
   features before opening quarantined outcome data.
7. Merge outcomes last, validate binary labels, and produce the first
   descriptive calibration/return estimates with family-clustered uncertainty
   and explicit category-specific results.

Phase 10E stops at step 1 pending fresh human validation and project-owner
approval.
