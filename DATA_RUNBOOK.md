# Data Runbook

Last updated: 2026-07-17

## Non-negotiable safety limits

- Do not run the legacy monolithic historical acquisition.
- Do not generate more than 5 GiB of raw acquisition data without explicit
  approval.
- Stop before filesystem free space falls below 80 GiB.
- Do not delete validated data without explicit approval.
- Do not use destructive Git commands.
- Do not place credentials, authorization headers, cookies, or signatures in
  commands, logs, manifests, or cached responses.
- Do not automatically verify candidate anchors.

## Preflight checklist

From the repository root:

```bash
git status --short --branch
df -h .
du -sh data/pipeline_v2/market_acquisition 2>/dev/null || true
```

Then run the partitioned acquisition preflight command documented by Phase
10A-R. Confirm all of the following before any network request:

- normalized half-open settlement envelope;
- pinned/fetched historical cutoff identity;
- next partition identity and start cursor hash;
- maximum requests in the next partition;
- estimated compressed bytes for that partition;
- current raw bytes and remaining configured budget;
- current free bytes and margin above the 80 GiB floor;
- explicit notice that historical total pages and selectivity are unknown until
  traversed.

Preflight is read-only. A cutoff fetch, if requested rather than pinned, is a
network operation and must be counted separately.

## Partition lifecycle

1. Resolve and pin a cutoff snapshot.
   Actual acquisition publishes a validated immutable copy inside the guarded
   raw root; preflight validates the external snapshot without writing.
2. Determine the first uncommitted partition from validated commits only.
3. Fetch at most the configured page count, reusing valid compressed cache pages.
4. Before each raw-page publication, check both the raw-byte budget and minimum
   free-space floor using the actual compressed byte count.
5. Validate page schema, cursor progress, response hash, file hash, and gzip
   integrity.
6. Filter records to the acquisition settlement envelope locally. Historical
   date filtering is not server-side.
7. Immediately publish separate normalized metadata, quarantined outcomes, and
   provenance artifacts.
8. Publish the partition summary and partition commit last. Only the commit
   makes the partition complete.
9. Emit/update the run-state report. A nonterminal end cursor means the overall
   historical traversal is explicitly incomplete.
10. Resume by validating the last committed partition and beginning at its end
    cursor. Uncommitted pages may be reused, but never advance the committed
    chain by themselves.

## Expected artifact separation

```text
raw compressed pages (.json.gz)
        |
        +--> outcome-free normalized metadata
        +--> quarantined normalized outcomes
        +--> page/record provenance
        +--> partition summary
                    |
                    +--> partition commit (published last)
```

The outcome-free metadata artifact is the only market-level input allowed into
event metadata, anchor evidence, verification, timing, horizon, target, and
price stages. Outcome files are merged only after sample inclusion is frozen.

## Failure and resume behavior

- Budget guard or disk-floor guard: stop before publication; preserve prior
  commits and valid cached pages; report the exact guard and byte margins.
- Transport interruption: retry only configured transient failures; never cache
  a partial response body; resume from validated pages.
- Invalid page/cursor/hash: fail closed; do not publish a partition commit.
- Process interruption after artifacts but before commit: artifacts remain
  immutable orphans and may be byte-identically reused; partition remains
  incomplete.
- Duplicate ticker with conflicting metadata and indistinguishable update time:
  quarantine/fail the merge; do not choose using result or settlement value.
- Nonterminal final partition: write an explicit incomplete-run report and do
  not publish a final complete universe.

## Production authorization sequence

1. Offline tests and simulations pass.
2. One bounded network smoke test passes and is reviewed at the phase level.
3. Production partitions are run individually under the configured guards.
4. A complete terminal partition chain is validated.
5. Cross-partition merge/deduplication succeeds deterministically.
6. Only then proceed to event metadata and anchor-evidence work.

Raw pruning is not part of the normal lifecycle. Any future pruning workflow
must first validate acquisition commits and normalized-output hashes, identify
exact targets, and obtain explicit confirmation.

## Accepted smoke baseline — 2026-07-17

The first bounded live smoke used one historical page plus one cutoff request.
It observed 2,373,973 uncompressed page bytes and 80,059 gzip bytes for 1,000
rows, with zero retries and zero normalization rejects. The full smoke namespace
was 339,295 bytes after partition artifacts, commit, run-state report, and
incomplete-merge report. These values are empirical planning inputs only; the
historical endpoint still provides no total-page estimate and every production
write remains subject to the hard guards.

## Superseded production resume checkpoint — 2026-07-17

- Raw root: `data/pipeline_v2/market_acquisition/partitioned`.
- Cutoff snapshot: `cutoff_443dd48c9d69f40b3cc5.json`.
- Cutoff: `2026-05-18T00:00:00Z`.
- Historical segment: `e7df4bc51bed4e45b26204a5`.
- Valid commits: 46; next partition index: 46.
- Next cursor hash: `99041db10ae82785`.
- Committed rows: 1,150,000 in range; 0 outside; 0 rejected.
- Distinct ticker audit: 1,150,000; repeated tickers: 0.
- Namespace bytes: 385,259,604 of 5,368,709,120.
- Historical traversal: incomplete/nonterminal.
- Final merge: prohibited until all historical and live segments are terminal.

Resume with the standard partitioned acquisition command and the cutoff copy
inside the raw root. The runner validates the full committed chain before any
new request and starts from the recorded end cursor; never supply a cursor
manually.

Read-only resume preflight:

```bash
python -m scripts.pipeline_v2.pull_kalshi_partitioned_metadata \
  --start-date 2025-05-01 \
  --end-date 2026-07-16 \
  --raw-root data/pipeline_v2/market_acquisition/partitioned \
  --cutoff-snapshot data/pipeline_v2/market_acquisition/partitioned/cutoff_snapshots/cutoff_443dd48c9d69f40b3cc5.json \
  --preflight
```

Acquire exactly the next configured partition after the preflight passes:

```bash
python -m scripts.pipeline_v2.pull_kalshi_partitioned_metadata \
  --start-date 2025-05-01 \
  --end-date 2026-07-16 \
  --raw-root data/pipeline_v2/market_acquisition/partitioned \
  --cutoff-snapshot data/pipeline_v2/market_acquisition/partitioned/cutoff_snapshots/cutoff_443dd48c9d69f40b3cc5.json \
  --continue-segment
```

`--continue-segment` validates the existing chain once, validates and commits
each subsequent 25-page partition independently, checks cursor continuity after
every commit, and performs a fresh full-chain validation at the terminal cursor.
It stops before beginning the next segment.

After a segment milestone has been reviewed, `--continue-all-segments` may be
used instead. It retains the validated immutable-commit set in one process,
while still performing a fresh full-chain validation whenever each segment
reaches its terminal cursor.

These dates describe the provisional acquisition envelope, not a change to the
frozen analysis window.

## Phase 10B final production record — 2026-07-17

- Raw root: `data/pipeline_v2/market_acquisition/partitioned`.
- All segments terminal: historical, 2026-05 live, 2026-06 live, 2026-07 live.
- Partitions: 454 total (352 historical, 102 live).
- Requests/pages: 11,269 total (8,778 historical, 2,491 live).
- HTTP attempts: 11,269; retries: 0; rate limits: 0.
- Input records: 11,268,402.
- In range: 9,861,209; outside envelope: 1,407,193; rejected: 0.
- Exact ticker audit: 9,861,209 unique, 0 duplicate, 0 conflict.
- Merge ID: `6f8aa42abec876d3aa1f6336`.
- Final contracts: 9,861,209; final event tickers: 427,090.
- Namespace bytes: 3,648,491,736 of 5,368,709,120.
- Free bytes: 103,643,897,856; margin above floor: 17,744,551,936.

Terminal partition identities:

- Historical: `4d899e360a6bff3d0bdb8845`, commit SHA-256
  `33d1e06188abd9d17fea53a0b43269d6fc09855c0c17d646897fa45bc1fec117`.
- Live 2026-05: `46e1601cee2d86fb2d6f1ce0`, commit SHA-256
  `b1ec6b4e0dc9d20c0884ecca297204d2d49eb93659c783373342e44fb1f8a869`.
- Live 2026-06: `46cc8749dd9e7ee262cd29f8`, commit SHA-256
  `9b53d24f6b4934969662ba3f76d7b84cfbbd33ab6db71f9b1f427a8ddb4c296f`.
- Live 2026-07: `c28c77f8dc02617a3cdd9fdf`, commit SHA-256
  `e2b1592636a994dfe1a85740a5980457d655453a3cfb0072487ca5ddbf3c253c`.

Final merge artifacts under
`merged_universes/6f8aa42abec876d3aa1f6336/`:

- `market_metadata.csv.gz`: 291,422,056 bytes; SHA-256
  `7acd4b59afc1ee0d952396cecb062e4216259c0ff4cb4893d5a8e00c50e26c44`.
- `market_outcomes.csv.gz`: 43,574,176 bytes; SHA-256
  `2114fd25b79627c9c36d716485382548b3812108007c5990bf2f384ca82cc451`.
- `event_tickers.csv.gz`: 2,636,443 bytes; SHA-256
  `544b5464f7afa01d8d9fa4148db1a6fee07a5fbf6265554db314c771b818cc45`.
- `source_provenance_manifest.json`: 256,720 bytes; SHA-256
  `1194e1e4a9dfb528d5b5177ba9f41b35add45ae620af6ca97ea9f63031d18530`.
- Merge commit SHA-256:
  `aeaaafd5fcff3fbc649bd6ca250d2613bf947453045d9ca371a2f4c24c35d3b4`.

Only `market_metadata.csv.gz` and `event_tickers.csv.gz` may feed the next
research stages. `market_outcomes.csv.gz` remains quarantined until sample
membership and research features are frozen. No anchor verification was run.
