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

## Standing frozen decisions

- Analysis anchor window:
  `[2025-07-01T00:00:00Z, 2026-07-01T00:00:00Z)`.
- Allowed timing structures: `fixed_clock`, `scheduled_event_start`.
- Allowed results: `yes`, `no`.
- API occurrence, strike, milestone, close, expiration, settlement, and outcome
  fields never become verified anchors automatically.
- Outcome data remains unavailable until features, anchors, horizons, targets,
  prices, and sample inclusion are frozen.
