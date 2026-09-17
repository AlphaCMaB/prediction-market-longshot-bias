# Phase 10J prospective cross-category collection plan

Status: prospective route selected by the owner on 2026-09-16. This document
specifies an offline design and approval gates. It does not authorize a network
smoke or production collection.

Phase 10J is a new prospective study. It does not extend the frozen Phase 10G
analysis window, alter Phase 10G or Phase 10I, or retroactively add markets to
either sample. Its sample, prices, outcomes, weights, and conclusions must be
reported under a new identity.

## Objective

Build a cross-category dataset that measures prices a trader could actually
execute, together with contemporaneous depth and pre-target trading activity.
The design is intended to answer whether calibration and favorite-side trading
returns differ across categories and liquidity states when price observability
is measured prospectively rather than reconstructed after settlement.

Market making remains a conditional scenario. Phase 10J will not place orders,
infer queue priority, or claim passive fills.

## Proposed enrollment window

The proposed first production cohort is:

`[2026-10-01T00:00:00Z, 2027-01-01T00:00:00Z)`

The interval contains 92 days and is not yet approved for production. A family
belongs to the cohort when its frozen ex-ante anchor falls inside this interval.
The interval is selected before any Phase 10J outcome is observed. Extending or
shortening it after collection begins would create a separately identified
cohort rather than modify this one.

## Eligible categories and timing rules

Only the already approved outcome-blind timing rules may be carried forward:

- Sports: `PR2_M_SCHEDULED_START_SINGLE_MILESTONE`;
- Crypto, Financials, Climate and Weather, and Commodities:
  `PR1_M_FIXED_CLOCK_SINGLE_EXACT`.

Politics and Entertainment are not eligible under the approved rules. They
remain outside the inferential cohort until a new outcome-blind anchor audit is
designed, independently validated, and explicitly approved. Their absence may
not be repaired by using close, expiration, settlement, publication, or result
timestamps.

The Phase 10E rule definitions are reused without modifying the frozen
`StudyRules`. A changed API schema, candidate source, or rule classification
fails closed.

## Prospective enrollment and anchor freeze

Discovery polls only open or unopened events and the associated official
milestones and series metadata. Normalized discovery records recursively omit
outcome, result, settlement, and post-resolution fields.

An otherwise eligible family must have one approved exact candidate no later
than two hours before that candidate time. At `candidate_time - 2 hours`, the
current allowed ex-ante evidence is hash-pinned and the anchor is frozen. The
price target is one hour before that frozen anchor.

Candidate changes observed before the two-hour freeze replace the proposed
candidate while preserving every earlier evidence version. A change after the
freeze never rewrites the anchor or sampling identity. Pre-target schedule
changes are recorded as ex-ante diagnostics; they do not cause a replacement
family to enter the sample. Post-target information cannot change eligibility.

Families first discovered after the two-hour freeze deadline are explicit late-
discovery exclusions. Markets that do not exist by the one-hour target remain
sampled but price-unobservable; they are not replaced.

## Probability sampling

Sampling is decided before prices and outcomes with a fixed SHA-256 Bernoulli
draw within category. Historical eligible-family counts are used only for
planning the probabilities; they are not treated as the future population.

The 365-day Phase 10F frame contained 64,775 Sports, 43,889 Crypto, 2,930
Financials, 571 Climate and Weather, and one Commodities family. Scaling those
counts to 92 days and targeting 625 enrollments in each category where possible
gives these fixed family inclusion probabilities:

| Category | Planning population | Family inclusion probability |
|---|---:|---:|
| Sports | 16,326.849 | 0.038280502744 |
| Crypto | 11,062.433 | 0.056497517948 |
| Financials | 738.521 | 0.846286541030 |
| Climate and Weather | 143.923 | 1.000000000000 |
| Commodities | 0.252 | 1.000000000000 |

Climate and Weather and Commodities are expected to be descriptive because the
fixed window is unlikely to provide 500 independent families. No category is
oversampled after its outcomes, prices, or realized availability are known.

For an included family, select at most three contracts by deterministic hash
rank from the complete contract roster frozen two hours before the anchor.
Contracts must have an open time no later than the price target. The family
probability is the fixed category probability; the conditional contract
probability is `min(3, M) / M`, where `M` is the eligible frozen roster size.
Persist both exact probabilities and their product. Analyze categories
separately; any pooled cross-category result is secondary and explicitly
reweighted to a stated target.

The sampling seed is
`phase-10j-prospective-bernoulli-contract-cap-v1`. Hash ranks are independent of
titles, prices, liquidity, outcomes, and API success.

## Executable-price capture

The primary price is the latest complete pre-target order-book snapshot. The
collector schedules two read-only attempts for each target group:

- target minus 60 seconds;
- target minus 10 seconds.

Use the authenticated multiple-orderbooks endpoint for at most 100 tickers per
request. Accept a response only when both request start and response receipt
are no later than the frozen target. The later valid response is primary; the
earlier response is an immutable fallback. If receipt occurs after the target,
discard the body without parsing or publication and commit only the request
identity, timing, and `late_response_discarded` status. It cannot supply a
price or post-target information.

Kalshi order books contain YES bids and NO bids. Therefore:

- executable YES ask = `1 - best NO bid`;
- executable NO ask = `1 - best YES bid`.

Preserve all returned levels and fixed-point quantities. Calculate one-, ten-,
and 100-contract depth-aware VWAPs without assuming unavailable size. A size is
unobservable if the book cannot fill it. Reject crossed books, invalid prices or
quantities, duplicate tickers, an unexpected ticker, or a changed schema.

The primary applied analysis uses one-contract taker prices before fees. Depth-
aware ten- and 100-contract estimates are capacity scenarios. Fee fields from
series metadata and the contemporaneous official schedule are preserved as
evidence, but account-specific rebates and unobserved overrides cannot be
reconstructed. Gross spread-crossing results remain primary; fee-adjusted
results remain clearly labeled scenarios.

## Trades and liquidity

After the target, query the public trades endpoint with ticker, `min_ts`, and
`max_ts` fixed to the 60-minute pre-target interval. Reject any returned trade
after the target, outside the requested ticker, or with a duplicate trade ID.
Cursor traversal must terminate and every page must be immutable and
compressed.

Pre-target liquidity measures include:

- trade count and total contracts during the preceding hour;
- last-trade price and age, without previous-price fallback;
- top-of-book spread and displayed size;
- depth available at one, ten, and 100 contracts;
- open interest and volume fields observed before the target; and
- market age at the target.

Midpoint calibration, taker execution, trade-close robustness, and conditional
maker scenarios remain separate measures. No fallback may mix them.

## Outcome quarantine

No Phase 10J discovery, sampling, anchor, price, depth, trade, or fee artifact
may contain an outcome, settlement value, settlement timestamp, or post-
resolution field. Responses from an endpoint expected to be outcome-free fail
closed if a prohibited value appears. Research projections recursively remove
prohibited keys.

Outcomes cannot be requested until the cohort closes, sample identities and
inclusion probabilities are frozen, capture attrition is reported, and a final
pre-outcome analysis plan receives explicit approval. Any later release must be
the same minimal binary projection used in Phase 10G under a new cohort
identity.

## Storage, credentials, and operational gates

The current generated namespace uses 5,348,533,021 bytes and has only
20,176,099 bytes below its 5 GiB ceiling. Free disk is comfortably above the
80 GiB floor, but the namespace headroom is not sufficient for production.

The planning frame implies about 2,019 enrolled families and at most 6,058
sampled contracts. Before production, a bounded authenticated smoke must
measure compressed discovery, order-book, trade, commit, and normalized bytes.
No production projection may substitute an assumed byte rate for that smoke.

Phase 10J-B may be proposed as a maximum 20-family, 60-contract schema smoke,
with at most 5 MiB of generated data and no order placement. It requires:

- explicit network-smoke approval;
- Kalshi read credentials supplied outside the repository;
- a fresh account rate-limit query;
- a fresh namespace and disk preflight; and
- immutable cleanup-free publication under a separate smoke scope.

Production requires a separately approved namespace ceiling above 5 GiB. The
smoke must determine the requested increase. Existing Phase 10B–10G artifacts
may not be deleted, moved, or rewritten to create space.

## Phase gates

1. **10J-A — offline design and planner:** no network, credentials, sample
   realization, or outcome access.
2. **10J-B — authenticated schema smoke:** separately approved, no more than 20
   families, 60 contracts, or 5 MiB.
3. **10J-C — production enrollment and capture:** separately approved window,
   storage ceiling, request budget, and rate limits; resumable partitions and
   explicit incomplete-run reporting.
4. **10J-D — pre-outcome freeze:** report enrollment, sampling, attrition,
   category support, effective sample size, hashes, and observability before
   requesting any outcome release.

At every stage: no trading, no sample replacement, no post-target quote, no
automatic rule expansion, no anchor rewrite after freeze, no outcome access,
and no change to Phase 10G or Phase 10I.
