# Phase 10I exploratory trading analysis plan

Status: approved in principle on 2026-09-15; exact exploratory specification
recorded before Phase 10I estimates are calculated.

Phase 10I is explicitly post-confirmatory. Phase 10G outcomes and results have
already been observed. Nothing in this phase may replace, revise, or be
presented as part of the frozen Phase 10G confirmatory analysis.

## Research questions

Phase 10I asks three applied questions:

1. Did the Phase 10G longshot/favorite comparison survive the observed
   bid/ask spread when positions are entered from the executable side?
2. Within the price-observable Sports sample, was any executable edge stronger
   in markets with lower pre-target trading activity or weaker liquidity?
3. What new sampling and acquisition design would be required for a defensible
   comparison beyond Sports?

The first two questions use only the already frozen Sports sample. The third
uses no Sports result to redraw or relabel the original sample.

## Immutable Phase 10G boundary

The following remain unchanged:

- analysis identity:
  `931a1d35de134e91eee3ed71041a712414c1435fbcd37f1ffc28b263e746252e`;
- reporting-manifest SHA-256:
  `db298df905de11e145638d1f633f6829b5a9006f8012f0c02755e5f38443ccc8`;
- sample identities, anchors, one-hour horizon, prices, outcomes, inclusion
  probabilities, weights, exclusions, and StudyRules;
- the primary Phase 10G conclusion and its confirmatory status.

Phase 10I must fail closed if a pinned Phase 10F or Phase 10G input hash differs.
It may write only new, separately named Phase 10I artifacts.

## Sports executable-price analysis

### Analysis population

Use the frozen Phase 10G primary sample: contracts with a fully pre-target YES
bid and ask observed no more than 15 minutes before the one-hour target and an
allowed binary outcome. Tail membership is fixed from the frozen midpoint:

- longshot: midpoint `< 0.20`;
- favorite: midpoint `>= 0.80`.

No contract is added, replaced, or reclassified using its outcome.

### Taker entries

For a longshot contract, the strategy takes the other side of possible
longshot overpricing by buying NO at the top-of-book proxy
`no_ask = 1 - yes_bid`. Gross profit per contract held to resolution is:

`(1 - Y) - no_ask`.

For a favorite contract, the strategy buys YES at `yes_ask`. Gross profit per
contract held to resolution is:

`Y - yes_ask`.

These are top-of-book, one-contract execution proxies. Candle-close quotes do
not provide depth, queue position, latency, or slippage beyond the displayed
price, so the results must not be described as a realized backtest.

The primary applied statistic is family-target weighted gross profit in dollars
per contract. Report longshots, favorites, and their combined strategy
separately. Secondary statistics are the weighted win rate, average entry cost,
and capital return calculated as the ratio of weighted aggregate profit to
weighted aggregate entry cost. Do not average contract-level percentage
returns.

### Fee scenarios

Exact historical per-market fees and participant rebates are not present in the
frozen data. The primary result therefore incorporates the observed spread but
is reported before fees. Two separately labeled standard-schedule scenarios
apply the published general taker formula:

`fee(C, p) = ceil_to_cent(0.07 * C * p * (1 - p))`.

Report per-contract results for `C=1` and `C=100`. The 100-contract result is a
fee-rounding scenario, not a depth-supported fill claim. Markets with special
fees, promotions, or rebates cannot be reconstructed and remain a limitation.
The formula and exceptions are documented in Kalshi's
[official fee schedule](https://kalshi.com/docs/kalshi-fee-schedule.pdf).

### Uncertainty and weights

Use the frozen `family_weight_raw` as the primary target and
`contract_weight_raw` as a secondary target. Preserve the original
anchor-month × family-size strata and resample whole families using 10,000
deterministic cluster-bootstrap replicates. Report two-sided 95% percentile
intervals. These are exploratory intervals and are not a second confirmatory
test.

## Pre-target activity and liquidity analysis

Derive activity only from the immutable one-hour raw candle response ending at
the frozen target. Reject any candle after the target. For every contract,
derive:

- total reported volume in the returned pre-target window;
- count of candles with an actual trade close;
- latest pre-target open interest;
- quote spread;
- quote staleness;
- number of returned pre-target candles; and
- hours since market open.

The primary attention comparison is `zero reported volume` versus `positive
reported volume` during the one-hour response window. Secondary descriptive
groups are positive-volume quartiles, spread quartiles, open-interest
quartiles, and actual-trade-present versus no-trade. Quantile cut points are
computed without outcomes and recorded in the report.

Within each supported group, report the family-weighted executable profit and
the number of contracts, families, and effective sample size. A group requires
at least 200 contributing families and family-weighted ESS of at least 150 for
an interval; smaller groups are descriptive only. The zero-versus-positive
volume difference is the named exploratory interaction. All other group
comparisons are descriptive and appear together.

The 928 sampled contracts with no pre-target candle cannot be assigned an
executable target price. They remain an explicit unpriced population and are
not replaced with a stale previous trade. Phase 10I may describe their ex-ante
composition but may not estimate a trading return for them.

## Market-making scenarios

Market-making is scenario analysis only:

- passive NO entry for longshots: `1 - yes_ask`;
- passive YES entry for favorites: `yes_bid`.

Report profit conditional on a complete fill under two maker-fee scenarios:
zero and the published general maker formula
`ceil_to_cent(0.0175 * C * p * (1-p))`, again for `C=1` and `C=100`.
Do not estimate a fill rate, realized P&L, Sharpe ratio, capacity, or queue
position from candle data. The spread-capture difference between taker and
conditional-maker entry is mechanical and must not be called alpha.

## Cross-category extension

The existing structurally eligible frame contains:

| Category | Eligible families | Timing rule |
|---|---:|---|
| Sports | 64,775 | PR2 scheduled event start |
| Crypto | 43,889 | PR1 fixed clock |
| Financials | 2,930 | PR1 fixed clock |
| Climate and Weather | 571 | PR1 fixed clock |
| Commodities | 1 | PR1 fixed clock |

Politics and Entertainment have no eligible families under the frozen rules.
They cannot be silently added by changing Phase 10G StudyRules.

The prior outcome-blind price-source pilot found 0/135 usable PR1 midpoints and
0/135 usable PR1 trades within 15 minutes. Accordingly, Phase 10I will first
produce a no-network design and storage preflight. It will not draw a final
sample or acquire prices until one of these separately approved routes is
viable:

1. an independently validated historical source with executable quotes or
   trades; or
2. prospective collection of timestamped quotes, depth, trades, and fees for a
   newly defined future window.

Any cross-category sample will be stratified separately by category, anchor
month, and family size. It will retain exact inclusion probabilities and set a
minimum of 500 sampled families per inferential category where the population
permits. Commodities is descriptive only under the existing frame. New timing
rules for Politics or Entertainment require a new outcome-blind anchor audit.

The cross-category extension must have a new sample identity, new outcome
quarantine, and new report label. It must not reuse the Phase 10G Sports weights
or reinterpret Phase 10G as a category comparison.

## Storage and publication guards

At plan time, the guarded namespace uses 5,348,533,021 bytes and has 20,176,099
bytes remaining under the 5 GiB ceiling. Free disk is 103,197,274,112 bytes,
17,297,928,192 bytes above the 80 GiB hard floor.

Phase 10I-A may create only compact aggregate outputs outside the guarded raw
namespace. No cross-category acquisition may start unless a fresh preflight
shows that the complete auditable acquisition fits both guards. Existing
Phase 10B–10G data may not be deleted or rewritten to make room.

## Reporting language

Every Phase 10I output must say that:

- the analysis is exploratory and was designed after Phase 10G outcomes were
  known;
- executable-price proxies incorporate the observed top-of-book spread but not
  depth or slippage;
- fee-adjusted results are schedule scenarios, not reconstructed account P&L;
- maker results are conditional-on-fill scenarios;
- low-attention results apply only where a target-time price exists; and
- the frozen Phase 10G conclusion remains the sole confirmatory result.
