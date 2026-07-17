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

## Standing frozen decisions

- Analysis anchor window:
  `[2025-07-01T00:00:00Z, 2026-07-01T00:00:00Z)`.
- Allowed timing structures: `fixed_clock`, `scheduled_event_start`.
- Allowed results: `yes`, `no`.
- API occurrence, strike, milestone, close, expiration, settlement, and outcome
  fields never become verified anchors automatically.
- Outcome data remains unavailable until features, anchors, horizons, targets,
  prices, and sample inclusion are frozen.
