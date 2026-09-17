# Phase 10J-A: prospective collection preflight

## Decision

The prospective route is technically feasible, but production is not yet
authorized and does not fit the current generated-data namespace. Phase 10J-A
made zero network requests, realized no future sample, and accessed no outcome.

The proposed cohort is `2026-10-01T00:00:00Z` through `2027-01-01T00:00:00Z`. It is
a new study identity and leaves Phase 10G and Phase 10I unchanged.

## Sampling plan

| Category | Planning families | Inclusion probability | Expected sample | Role |
|---|---:|---:|---:|---|
| Sports | 16326.8 | 0.038281 | 625.0 | inferential |
| Crypto | 11062.4 | 0.056498 | 625.0 | inferential |
| Financials | 738.5 | 0.846287 | 625.0 | inferential |
| Climate and Weather | 143.9 | 1.000000 | 143.9 | descriptive |
| Commodities | 0.3 | 1.000000 | 0.3 | descriptive |

Expected enrollment is 2019.2 families and
at most 6,058 contracts under the three-
contract cap. Base request planning spans
6,180 to
10,098 before
discovery polling, trade pagination, retries, or rate limiting.

## Capture design

For each frozen target, request two read-only order-book snapshots before the
target and retain the later response that completes no later than the target.
Preserve price levels and quantities and calculate executable one-, ten-, and
100-contract VWAPs. Fetch only trades timestamped inside the preceding hour.
Midpoint, taker, actual-trade, and conditional-maker measures remain separate.

Official endpoint validation establishes that the multiple-orderbooks endpoint
accepts up to 100 tickers and requires authentication, while the public trades
endpoint supports ticker and Unix-time filters. Exact account rate limits are
unknown until the authenticated smoke.

## Storage scenarios

| Scenario | Projected incremental bytes | Fits 5 GiB namespace |
|---|---:|---:|
| compact | 58,015,744 | no |
| planning | 165,658,624 | no |
| stress | 430,571,520 | no |

Even the compact scenario exceeds the current
20,176,099-byte headroom.
Production therefore requires a smoke-calibrated namespace increase; existing
validated data cannot be deleted or moved to create room.

## Next gate

Approve the exact 92-day cohort and a Phase 10J-B smoke capped at 20 families,
60 contracts, 5 MiB, and read-only authenticated requests. Read credentials
must be supplied outside Git. The smoke must validate schemas, response timing,
trade-filter semantics, rate limits, gzip ratios, request commits, resume, and
outcome quarantine before a production ceiling or request budget is proposed.
