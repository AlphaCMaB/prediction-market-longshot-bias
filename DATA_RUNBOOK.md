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

