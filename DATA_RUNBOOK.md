# Data Runbook

Last updated: 2026-08-24

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

## Phase 10C: partitioned event metadata

Pinned inputs:

```text
merge ID: 6f8aa42abec876d3aa1f6336
event-ticker SHA-256: 544b5464f7afa01d8d9fa4148db1a6fee07a5fbf6265554db314c771b818cc45
event count: 427090
```

Run the read-only production preflight first:

```console
python -m scripts.pipeline_v2.pull_kalshi_partitioned_event_metadata \
  --event-tickers data/pipeline_v2/market_acquisition/partitioned/merged_universes/6f8aa42abec876d3aa1f6336/event_tickers.csv.gz \
  --raw-root data/pipeline_v2/market_acquisition/partitioned \
  --expected-merge-id 6f8aa42abec876d3aa1f6336 \
  --expected-event-ticker-sha256 544b5464f7afa01d8d9fa4148db1a6fee07a5fbf6265554db314c771b818cc45 \
  --config configs/pipeline_v2.toml \
  --preflight
```

Preflight performs no request and writes nothing. Require
`ready_for_network=true`, zero malformed tickers, zero duplicates, a sorted
source, a projected namespace below 5 GiB, and projected free space above
80 GiB.

The bounded smoke is a separate deterministic scope:

```console
python -m scripts.pipeline_v2.pull_kalshi_partitioned_event_metadata \
  --event-tickers data/pipeline_v2/market_acquisition/partitioned/merged_universes/6f8aa42abec876d3aa1f6336/event_tickers.csv.gz \
  --raw-root data/pipeline_v2/market_acquisition/partitioned \
  --expected-merge-id 6f8aa42abec876d3aa1f6336 \
  --expected-event-ticker-sha256 544b5464f7afa01d8d9fa4148db1a6fee07a5fbf6265554db314c771b818cc45 \
  --config configs/pipeline_v2.toml \
  --limit-events 200 \
  --partition-events 200 \
  --continue-all
```

After validating smoke hashes, resume behavior, missing-event count, schema,
compression, quarantine, and disk accounting, re-run the full preflight. The
full production command differs only by omitting both limit arguments:

```console
python -m scripts.pipeline_v2.pull_kalshi_partitioned_event_metadata \
  --event-tickers data/pipeline_v2/market_acquisition/partitioned/merged_universes/6f8aa42abec876d3aa1f6336/event_tickers.csv.gz \
  --raw-root data/pipeline_v2/market_acquisition/partitioned \
  --expected-merge-id 6f8aa42abec876d3aa1f6336 \
  --expected-event-ticker-sha256 544b5464f7afa01d8d9fa4148db1a6fee07a5fbf6265554db314c771b818cc45 \
  --config configs/pipeline_v2.toml \
  --continue-all
```

Raw pages, normalized partition artifacts, commits, run reports, and final
event universes live under `event_metadata_acquisition/` inside the shared
guarded root. They are ignored generated data and must never be committed.
Every successful partition is independently resumable. Any committed missing
event makes the scope explicitly incomplete and blocks final publication.

The collection endpoint may omit valid requested identifiers. Phase 10C
recovers each omission through the documented single-event endpoint and then
queries related milestones for the same ticker. These fallbacks are bounded,
compressed, hashed, and committed like collection pages. Never interpret a
collection omission as evidence that the Phase 10B event should be dropped.

Final research outputs are compressed `event_metadata.csv.gz`,
`event_milestones.csv.gz`, and `event_source_provenance.jsonl.gz`. They contain
no outcome fields and do not verify anchors. Do not expose Phase 10B
`market_outcomes.csv.gz` or raw event responses to downstream research-feature
construction.

### Phase 10C smoke checkpoint — 2026-07-18

- Corrected smoke scope: `a2034bb331b27ef433702458`.
- Events: 200 requested, 200 retrieved, 6 collection omissions recovered,
  zero missing.
- Requests: 13 logical/successful HTTP attempts (1 collection, 6 exact event,
  6 related milestone), zero retries and rate limits.
- Milestone associations: 114.
- Compressed raw pages: 25,502 bytes; compressed partition artifacts: 27,836
  bytes.
- Merge ID: `2b75ffc9269f451fed90b82d`.
- Event metadata SHA-256:
  `ab53de4207c7152f73a9e03e44f1c683587c779694bb8e04301824c8757a07d1`.
- Event milestones SHA-256:
  `29c152810ab8409eddeb6860a0d8d2351360e0b0068365abf6ce6f80edbc2a91`.
- Event provenance SHA-256:
  `34233bcd5b6dc673a3f651b37d923fbef4de3976288779d15cd7869fe0c9175d`.
- Merge commit SHA-256:
  `840766f4e8e0792714e8ea6a6ff306d07fe1ab51aec86c728ba036163b01d0ea`.
- No-network resume made no request and reproduced the same hashes.

### Phase 10C production checkpoint — 2026-07-18

- Scope: `6de9f91508597d5343bfe745`; merge:
  `69f1b1277bdfdbd530834fe6`.
- Partitions/events: 86; 427,090 requested and retrieved; zero missing,
  malformed, duplicate, rejected, or conflicting events.
- Requests: 2,768 logical/successful attempts; zero retries and rate limits.
  Exact-event plus related-milestone fallback recovered all 316 collection
  omissions.
- Milestone associations: 208,598; zero merged duplicates or conflicts.
- Compressed sizes: 96,133,690 raw-page bytes; 113,609,192 partition-artifact
  bytes; 110,500,035 final-artifact bytes.
- Final files under
  `event_metadata_acquisition/merged_event_universes/69f1b1277bdfdbd530834fe6/`:
  - `event_metadata.csv.gz` — 9,651,125 bytes; SHA-256
    `ef97e0093234e7b963f739d7ddd435691b5f8580e6551f398754d1b95807f3bf`.
  - `event_milestones.csv.gz` — 79,753,923 bytes; SHA-256
    `96f6c754f8ffbb0c9aa1d12e1a1cdf953079b773de419dc5e90917673334ab82`.
  - `event_source_provenance.jsonl.gz` — 21,094,129 bytes; SHA-256
    `6d6c9bb646abf9963e8f9ca978c681ef1db07b455114aa3e83a27f417c4671f0`.
  - `merge_report.json` — 858 bytes; SHA-256
    `7e80509ca3f03d184343672f5d008913677edfdd11d7266216ac9eff2d959774`.
  - `merge_commit.json` — 24,946 bytes; SHA-256
    `b2294aeac4c28217cce52358d31f257169e88e2d0d217cf3cb12757fbdd8eb43`.
- Shared guarded namespace: 3,972,267,951 bytes of 5 GiB. Free filesystem:
  101,689,225,216 bytes, 15,789,879,296 bytes above the 80 GiB floor.

All generated files in this checkpoint remain ignored and local. A complete
no-network resume must return scope complete, reproduce merge
`69f1b1277bdfdbd530834fe6`, issue zero requests, and leave all file sizes and
modification times unchanged.

Only the three compressed final research artifacts may feed Phase 10D
candidate anchor-evidence construction. Preserve timestamps as unverified
candidate evidence, do not invoke anchor verification, and do not read raw
responses or quarantined market outcomes into research metadata.

## Phase 10D: candidate anchor evidence

The production command is local and performs no network request:

```console
python -m scripts.pipeline_v2.build_kalshi_anchor_evidence \
  --market-metadata data/pipeline_v2/market_acquisition/partitioned/merged_universes/6f8aa42abec876d3aa1f6336/market_metadata.csv.gz \
  --event-metadata data/pipeline_v2/market_acquisition/partitioned/event_metadata_acquisition/merged_event_universes/69f1b1277bdfdbd530834fe6/event_metadata.csv.gz \
  --event-milestones data/pipeline_v2/market_acquisition/partitioned/event_metadata_acquisition/merged_event_universes/69f1b1277bdfdbd530834fe6/event_milestones.csv.gz \
  --output-root data/pipeline_v2/anchor_evidence/phase_10d \
  --guard-root data/pipeline_v2 \
  --config configs/pipeline_v2.toml \
  --expected-market-sha256 7acd4b59afc1ee0d952396cecb062e4216259c0ff4cb4893d5a8e00c50e26c44 \
  --expected-event-sha256 ef97e0093234e7b963f739d7ddd435691b5f8580e6551f398754d1b95807f3bf \
  --expected-milestone-sha256 96f6c754f8ffbb0c9aa1d12e1a1cdf953079b773de419dc5e90917673334ab82
```

Add `--dry-run` to compute complete diagnostics, hashes, and exact CSV byte
estimates without creating the output directory. Production-sized inputs
require all three pinned hashes. Large market input is streamed into
family-level sufficient rows so repeated identical occurrence evidence is
represented once with an explicit support count. Output is written to a
guarded sibling work directory and the complete four-file directory is
published atomically. A rerun recomputes every artifact hash and fails on any
conflict without modifying the published files.

### Phase 10D production checkpoint — 2026-07-19

- Composite families: 427,090; with candidates: 418,591; without candidates:
  8,499.
- Candidate rows: 625,923, all `needs_review` and all exact timestamps.
- Sources: 208,308 market occurrence; 209,017 event strike; 208,598 milestone
  start.
- Window diagnostics: 539,109 inside; 8,196 before; 78,618 at or after the
  frozen half-open window. Date-only/overlapping candidates: zero.
- Review diagnostics: 198,686 families with multiple distinct exact candidate
  times; zero multiple-event-ticker families, missing-event families, invalid
  values, or ignored sentinels.
- Verification template: 427,090 rows, all `needs_review`; every verified time,
  source, timing structure, and evidence reference is blank.
- `anchor_evidence.csv`: 948,679,350 bytes; SHA-256
  `8bd614d88c139aa02bc5fbfeb12dd9c2287cd98766a7f029d084347b6afd1e10`.
- `anchor_family_review.csv`: 161,000,334 bytes; SHA-256
  `794178aced9711498f57b77fd74ff61412185eb4ee5f76aeafc275aa501e0795`.
- `anchor_verification_decisions_template.csv`: 49,747,044 bytes; SHA-256
  `4963a7c9af0c923c5c9a24d0985c4e0e463229dd5b6677a3dc3152e08ebe5203`.
- `anchor_evidence_report.json`: 4,105 bytes; SHA-256
  `ec9d3e907caf0eb5ae989cdf9805ecce1d5538ab3fe5ea278370a80c7c06508e`.
- Total Phase 10D bytes: 1,159,430,833. Shared namespace: 5,131,698,784
  bytes, 237,010,336 bytes below the 5 GiB ceiling. Free disk at validation:
  95,696,416,768 bytes, 9,797,070,848 bytes above the 80 GiB floor.

Do not edit the generated decisions template in place. Copy it into the
separately controlled review workflow, preserve the exact composite-family
schema, and do not consult outcomes. Phase 10D artifacts are evidence and
review inputs only; they do not authorize anchor verification, horizon-price
construction, or bias estimation.

## Phase 10E: outcome-blind verification audit design

The command is local, reads only approved Phase 10D/10C projections, and makes
no network request:

```console
python -m scripts.pipeline_v2.build_phase_10e_verification_design \
  --family-review data/pipeline_v2/anchor_evidence/phase_10d/anchor_family_review.csv \
  --anchor-evidence data/pipeline_v2/anchor_evidence/phase_10d/anchor_evidence.csv \
  --event-metadata data/pipeline_v2/market_acquisition/partitioned/event_metadata_acquisition/merged_event_universes/69f1b1277bdfdbd530834fe6/event_metadata.csv.gz \
  --output-root data/pipeline_v2/anchor_evidence/phase_10e_design \
  --guard-root data/pipeline_v2 \
  --config configs/pipeline_v2.toml \
  --audit-per-tier 150 \
  --expected-family-sha256 794178aced9711498f57b77fd74ff61412185eb4ee5f76aeafc275aa501e0795 \
  --expected-evidence-sha256 8bd614d88c139aa02bc5fbfeb12dd9c2287cd98766a7f029d084347b6afd1e10 \
  --expected-event-sha256 ef97e0093234e7b963f739d7ddd435691b5f8580e6551f398754d1b95807f3bf
```

The audit seed is `phase-10e-outcome-blind-audit-v1`. Sampling is deterministic
and stratified, and every packet row records its stratum population, allocation,
and inverse weight. The packet projection excludes outcome, result, settlement,
close/expiration, price, and update fields. The decisions template preserves
the exact frozen verification schema and leaves all 450 rows `needs_review`
with verified fields blank. Re-running the command must reproduce every byte
without modifying the published directory.

### Phase 10E design checkpoint — 2026-07-19

- Tier counts: 102,413 Tier 1; 93,997 Tier 2; 230,680 Tier 3.
- Proposed in-window pool: 189,466 families (97,369 Tier 1; 92,097 Tier 2).
- Audit: 150 families per tier; 450 total. Approval and disagreement rates are
  not observed until the packet is reviewed.
- `phase_10e_pattern_counts.csv`: 6,305 bytes; SHA-256
  `88ac7886660b5f8fa69d13d823e6687501f1f3788bdc4d5c2918432252288df6`.
- `phase_10e_audit_review_packet.csv`: 814,765 bytes; SHA-256
  `89fc0b28be4365c78558d1aed1d77578d5d379a891c4664bb75e62bb411ed05b`.
- `phase_10e_audit_review_packet.md`: 901,521 bytes; SHA-256
  `fd63b71738659c233a9905ab1b4ff84cecd4681f311d75f848f15a3e8f46adcb`.
- `phase_10e_audit_decisions_template.csv`: 27,528 bytes; SHA-256
  `d83a0ca8515c932a48f2c2033cde2b9c5293f1b8a466194ff8b694a47da2cc06`.
- `phase_10e_design_report.json`: 8,212 bytes; SHA-256
  `f06c381154064be0ce1b0fe615f2390e46b05fec0e9ce1641d5ad7347d195257`.
- Canonical packet bytes: 1,758,331. A superseded 1,761,991-byte packet is
  retained under `phase_10e_design_rejected_v1` because no generated evidence
  was deleted.
- Shared generated namespace: 5,135,219,106 bytes, leaving 233,490,014 bytes
  below the 5 GiB ceiling. Free disk at final validation: 99,062,157,312 bytes.

PR1 and PR2 remain proposed and unapproved. Do not run
`apply_anchor_verification`, construct horizons, read prices, or access outcomes
from this checkpoint.

### Phase 10E recommendation-only AI first review — 2026-07-19

The local command reads only the canonical outcome-blind audit packet and makes
no network request:

```console
python -m scripts.pipeline_v2.build_phase_10e_first_review \
  --packet data/pipeline_v2/anchor_evidence/phase_10e_design/phase_10e_audit_review_packet.csv \
  --output-root data/pipeline_v2/anchor_evidence/phase_10e_first_review \
  --guard-root data/pipeline_v2 \
  --expected-packet-sha256 89fc0b28be4365c78558d1aed1d77578d5d379a891c4664bb75e62bb411ed05b
```

The builder records recommendations only. All 450 verification statuses remain
`needs_review`, PR1 and PR2 remain `not_approved`, Tier 3 remains quarantined,
and human and AI-human statistics remain explicitly unavailable until the
independent review is returned.

- `phase_10e_first_review.csv` — 175,948 bytes; SHA-256
  `ad993c0470534765cd6264f45600838a0fd160d5a6ddd3fa9d10cdede94578ff`.
- `phase_10e_first_review_report.json` — 72,301 bytes; SHA-256
  `3a2b66202df4639135f6b4736afc6cf8acee7cb4f65a1f143ea5d01ab21190a4`.
- `phase_10e_human_review_subset.csv` — 320,828 bytes; SHA-256
  `904c5c7b787a6cc573878f7ddcb0d5aa46bc0c29228b1e92cbe8a235563ec1cc`.
- `phase_10e_disagreement_and_uncertainty_queue.csv` — 75,959 bytes; SHA-256
  `3653a6ae8b8b4effff15a273492ff8f6c8c49acce4de3fac191933ae1e499bca`.

Total first-review output is 645,036 bytes. The shared generated namespace is
5,135,864,142 bytes, leaving 232,844,978 bytes below the 5 GiB ceiling. Free
disk at validation is 99,051,147,264 bytes. A complete rerun must reproduce all
four hashes without changing file sizes or modification times.

### Phase 10E local human-review interface — ready, audit not started

This earlier 165-case interface checkpoint is superseded for methodological
inference: the finalized annotations used AI assistance and therefore are not
an independent human review. The source subset remains immutable and retained.

Preflight and validate the immutable inputs without creating a decisions file:

```console
python -m scripts.pipeline_v2.review_phase_10e_human --validate-only
```

Start or resume the interactive review:

```console
python -m scripts.pipeline_v2.review_phase_10e_human
```

Decision keys are `A` for approve candidate, `R` for reject, `U` for uncertain,
`B` for the previous case, and `Q` for a safe stop. Rejection and uncertainty
require a short rationale. Timing structure, candidate relevance, confidence,
and standardized ambiguity flags are recorded for every completed case.

The interface reads and hash-validates the canonical packet and compact subset,
but never writes either source. It autosaves atomically to
`data/pipeline_v2/anchor_evidence/phase_10e_human_review/phase_10e_human_decisions.csv`.
When all 165 cases exist, it validates an in-memory projection against the exact
frozen verification schema with every status still `needs_review`, then writes
`phase_10e_human_review_report.json`. The report separates PR1 and PR2 and
contains two-phase inverse-probability-weighted human approval, confirmed
false-positive, uncertainty, AI-human disagreement, category, and failure-mode
diagnostics. It cannot approve a rule or apply an anchor.

The conservative generated-storage allowance is 1,728,512 bytes, versus
232,844,978 bytes of current headroom. Current generated bytes remain
5,135,864,142 because the human audit has not started. No network client,
outcome input, horizon builder, or production decision-application stage is
imported or invoked by this workflow.

### Phase 10E AI-assisted import and fresh human-validation design — 2026-08-22

The finalized annotation source is a compact CSV labeled `AI-assisted
outcome-blind review`. Import it with no network access:

```console
python -m scripts.pipeline_v2.import_phase_10e_ai_assisted_review \
  --annotation /Users/yimeiliu/Downloads/phase_10e_final_ai_assisted_annotations.csv \
  --expected-annotation-sha256 85153b54eb7bb7d1e136c907770aa86fae57cc404821ad081c8d67204b55fff9 \
  --subset data/pipeline_v2/anchor_evidence/phase_10e_first_review/phase_10e_human_review_subset.csv \
  --packet data/pipeline_v2/anchor_evidence/phase_10e_design/phase_10e_audit_review_packet.csv \
  --first-review data/pipeline_v2/anchor_evidence/phase_10e_first_review/phase_10e_first_review.csv \
  --output-root data/pipeline_v2/anchor_evidence/phase_10e_ai_assisted_review \
  --guard-root data/pipeline_v2
```

Validated input and import facts:

- annotation SHA-256:
  `85153b54eb7bb7d1e136c907770aa86fae57cc404821ad081c8d67204b55fff9`;
- 165 rows, 165 unique audit IDs, and exact immutable-subset coverage;
- A=149, R=5, U=11;
- all controlled vocabularies and finalized correction invariants passed;
- all 165 verification statuses remain `needs_review`; verified anchor time
  and source are blank;
- zero rules approved, anchors verified, horizons built, outcomes accessed, or
  network requests.

Published ignored artifacts under
`data/pipeline_v2/anchor_evidence/phase_10e_ai_assisted_review/`:

- `phase_10e_ai_assisted_import_validation_report.json`: 1,232 bytes;
  SHA-256 `f438d3cebc885baf744038e2305b893812dfc5d3c7834b5b49a17b46436ccd98`;
- `phase_10e_ai_assisted_decisions.csv`: 85,548 bytes; SHA-256
  `33b6481e622843e88ddc100418358c2d7ff23770a929804b9ae262ff40e47354`;
- `phase_10e_ai_assisted_diagnostics.json`: 6,550 bytes; SHA-256
  `d596748f8945de32a7ae30b3a14c8a2ba483fba97dfc1baa2eb042d4ce306640`;
- `phase_10e_independent_human_packet.csv`: 139,930 bytes; SHA-256
  `99269b00ac01dec9e55ea1eb80884a629fb8131ea13d6485b1f5b055f134015c`;
- `phase_10e_independent_human_sample_manifest.csv`: 18,045 bytes; SHA-256
  `60b8fab36351b1e59114879a65fb830cc1f6e09669dd7a50f87a32b5c3de6575`;
- `phase_10e_independent_human_design_report.json`: 2,919 bytes; SHA-256
  `6b040b453a840796601012e53615781474063530dafe814cca336008ee6b10ed`.

The six accepted artifacts add 254,224 bytes. Acceptance review corrected the
second-stage inclusion probability to the design-fixed 50/150 for
non-mandatory cases. The pre-correction directory and an intermediate
correct-weight/wrong-publication-path directory are retained locally as
`phase_10e_ai_assisted_review_rejected_v1` and `_rejected_v2`; nothing was
deleted. Start or resume the fresh independent review with:

```console
python -m scripts.pipeline_v2.review_phase_10e_independent_validation \
  --packet data/pipeline_v2/anchor_evidence/phase_10e_ai_assisted_review/phase_10e_independent_human_packet.csv \
  --manifest data/pipeline_v2/anchor_evidence/phase_10e_ai_assisted_review/phase_10e_independent_human_sample_manifest.csv \
  --ai-assisted-decisions data/pipeline_v2/anchor_evidence/phase_10e_ai_assisted_review/phase_10e_ai_assisted_decisions.csv \
  --expected-packet-sha256 99269b00ac01dec9e55ea1eb80884a629fb8131ea13d6485b1f5b055f134015c \
  --expected-manifest-sha256 60b8fab36351b1e59114879a65fb830cc1f6e09669dd7a50f87a32b5c3de6575
```

Use `Q` to stop safely. The next invocation resumes at the first unfinished
case. The AI-assisted decisions are not loaded until all 100 fresh human
decisions are complete. Do not approve or apply either rule from these files.
The full offline suite passed 670 tests. Shared generated bytes, including both
retained pre-acceptance directories, are 5,136,627,083; headroom is
232,082,037 bytes. Free disk is 93,633,667,072 bytes (87.20 GiB).

### Phase 10E independent human review import — 2026-08-22

The two reviewer source texts are immutable inputs pinned by SHA-256:

- cases HV-001–050:
  `1004f94a11e6a6ed57d3856333e4319ffc525b1cb1bb958ae63755a03dfca922`;
- cases HV-051–100:
  `752d5a8a5cf7634db86e99711ccfb832bbe006be19f1a1a9dd3661be0855a94c`.

Import locally with no outcome or network input:

```console
python -m scripts.pipeline_v2.import_phase_10e_independent_review \
  --review-source /Users/yimeiliu/.codex/attachments/644618fc-da1d-42a3-924b-5879461b6340/pasted-text.txt \
  --expected-review-source-sha256 1004f94a11e6a6ed57d3856333e4319ffc525b1cb1bb958ae63755a03dfca922 \
  --review-source /Users/yimeiliu/.codex/attachments/fd4a9ba9-39fb-49c7-a061-c27ffffbaefa/pasted-text.txt \
  --expected-review-source-sha256 752d5a8a5cf7634db86e99711ccfb832bbe006be19f1a1a9dd3661be0855a94c \
  --packet data/pipeline_v2/anchor_evidence/phase_10e_ai_assisted_review/phase_10e_independent_human_packet.csv \
  --manifest data/pipeline_v2/anchor_evidence/phase_10e_ai_assisted_review/phase_10e_independent_human_sample_manifest.csv \
  --ai-assisted-decisions data/pipeline_v2/anchor_evidence/phase_10e_ai_assisted_review/phase_10e_ai_assisted_decisions.csv \
  --expected-packet-sha256 99269b00ac01dec9e55ea1eb80884a629fb8131ea13d6485b1f5b055f134015c \
  --expected-manifest-sha256 60b8fab36351b1e59114879a65fb830cc1f6e09669dd7a50f87a32b5c3de6575 \
  --output-root data/pipeline_v2/anchor_evidence/phase_10e_independent_human_validation \
  --guard-root data/pipeline_v2
```

Validated facts: 100 unique validation IDs, 100 unique audit IDs, exact packet
coverage, 50 PR1 and 50 PR2 cases, A=95/R=5/U=0, and 100 high-confidence
ratings. All reviewer terms and disagreements are preserved. Every status
remains `needs_review`, verified fields remain blank, and zero rules, anchors,
horizons, outcomes, post-event fields, or network calls are applied.

Published ignored artifacts:

- `phase_10e_independent_human_import_validation_report.json`: 32,747 bytes;
  SHA-256 `15fc9c4352bb7e16209caa5b4bcd1c7f24ec147694a505b058f1bc482db53900`;
- `phase_10e_independent_human_decisions.csv`: 42,642 bytes; SHA-256
  `2dff8eaa2c966e2b63de0c5b78be24135d97c9b1526c2387b58677522cf3316a`;
- `phase_10e_independent_human_report.json`: 18,082 bytes; SHA-256
  `f8b9986f3410c76e0e82eceb9ec5dacbc92b75f1a0d1e3c76da82cbb60d5bec3`;
- `phase_10e_independent_human_disagreement_queue.csv`: 2,934 bytes;
  SHA-256 `e3dd30c44725b491217369463e4120511410dd3ec6286b456dbe1c37772fe9bc`;
- `phase_10e_rule_recommendations.json`: 2,490 bytes; SHA-256
  `a3add089dafd2e95ef0c15cda2a47b7f193f94751ba66bdcdc03a467c85f7ac3`.

Do not apply the MODIFY recommendations until the project owner explicitly
approves each final rule specification.

Acceptance: 671 offline tests passed, together with compilation, scoped Black
and pyflakes, TOML validation, deterministic rerun, exact source-hash
preservation, decision quarantine, disagreement integrity, and `git diff
--check`. The five artifacts add 98,895 bytes. Shared generated bytes are
5,136,732,126, leaving 231,976,994 bytes of headroom. Free disk is
92,795,133,952 bytes (86.42 GiB), 6,895,788,032 bytes above the safety floor.

### Phase 10E approved-rule application — 2026-08-23

The explicit approval is immutable input SHA-256
`10affde71153a9428175435c945df1ae0a8ced412a2c3ea60f086dc59623f81e`.
Run the offline preflight by adding `--preflight-only`; omit it only after the
preflight fits both guards:

```console
python -m scripts.pipeline_v2.apply_phase_10e_approved_rules \
  --family-review data/pipeline_v2/anchor_evidence/phase_10d/anchor_family_review.csv \
  --anchor-evidence data/pipeline_v2/anchor_evidence/phase_10d/anchor_evidence.csv \
  --event-metadata data/pipeline_v2/market_acquisition/partitioned/event_metadata_acquisition/merged_event_universes/69f1b1277bdfdbd530834fe6/event_metadata.csv.gz \
  --approval /Users/yimeiliu/.codex/attachments/6e99ba4c-d6f7-414d-9e45-6b2adc2c1325/pasted-text.txt \
  --independent-report data/pipeline_v2/anchor_evidence/phase_10e_independent_human_validation/phase_10e_independent_human_report.json \
  --output-root data/pipeline_v2/anchor_evidence/phase_10e_approved_rules \
  --guard-root data/pipeline_v2 \
  --config configs/pipeline_v2.toml \
  --expected-family-sha256 794178aced9711498f57b77fd74ff61412185eb4ee5f76aeafc275aa501e0795 \
  --expected-evidence-sha256 8bd614d88c139aa02bc5fbfeb12dd9c2287cd98766a7f029d084347b6afd1e10 \
  --expected-event-sha256 ef97e0093234e7b963f739d7ddd435691b5f8580e6551f398754d1b95807f3bf \
  --expected-approval-sha256 10affde71153a9428175435c945df1ae0a8ced412a2c3ea60f086dc59623f81e \
  --expected-independent-report-sha256 f8b9986f3410c76e0e82eceb9ec5dacbc92b75f1a0d1e3c76da82cbb60d5bec3
```

Accepted counts: 427,090 total families; 98,625 PR1-M and 69,329 PR2-M
verified; 167,954 verified total; 259,136 remain `needs_review`; zero rejected.
Outputs are deterministic gzip except for the two canonical JSON reports:

- decisions: 4,719,774 bytes; SHA-256
  `f97d41716fe521f8d20637d2785a691bcf6618bf98f08b18a814bbad1119c148`;
- verified anchors: 10,177,763 bytes; SHA-256
  `c52e1f26e8e059c305d9f1a3fac311d325ea110552bea044e60dce778c8aacb8`;
- exclusions: 168,700 bytes; SHA-256
  `37e0f9e8e3988b6467a54c04f5e753c4a176703165bd30f167af37ca44aa8e54`;
- provenance: 2,156 bytes; SHA-256
  `c8efe96ce632448e8d6fc6536949a9ea8f478051e20e6b643825072dcb09182d`;
- application report: 8,509 bytes; SHA-256
  `afa8306d8a85bd11c11c0dfe3ca005ea6ef70db88ba8f3dee8928fc33a89a5ab`.

The final output adds 15,076,902 bytes. Four superseded acceptance-review
snapshots are retained locally as `phase_10e_approved_rules_rejected_v1`,
`_rejected_v2`, `_rejected_v3`, and `_rejected_v4`. Shared generated bytes are
5,212,312,767, leaving 156,396,353 bytes. Free disk is 91,032,793,088 bytes
(84.78 GiB). Do not delete any retained artifact without explicit approval.

Phase 10F storage gate: 167,954 verified families multiplied by only 1 KiB of
compressed raw response per family is already 171,984,896 bytes, exceeding the
current 156,396,353-byte headroom before manifests or normalized price rows.
This is a conservative feasibility illustration, not a request forecast. The
next offline planner must determine exact contract scope and requests, then a
bounded smoke must measure compressed bytes per request and per matched price.

Proposed non-destructive archival plan, pending explicit approval:

1. Preserve final Phase 10B/10C merged universes, merge/partition commits,
   manifests, reports, output hashes, and all Phase 10D/final Phase 10E inputs.
2. Copy the immutable Phase 10B/10C raw pages and normalized partition
   artifacts to external cold storage with paths and SHA-256 manifests intact;
   verify a complete byte-for-byte restore into a temporary location.
3. Only after explicit project-owner approval, remove the verified archived
   local copies. The current raw-page plus partition-artifact inventory is
   approximately 3,530,706,944 bytes (3.29 GiB), so it is the material storage
   target; no deletion is authorized here.
4. Separately, four superseded Phase 10E acceptance snapshots total 60,503,739
   bytes. They may be candidates for approved pruning after their diagnostic
   reason and final successor hashes are preserved. The canonical Phase 10E
   output is not a pruning target.

### Phase 10F-A offline horizon-price planner — 2026-08-23

Run without any network client or outcome input:

```console
python -m scripts.pipeline_v2.build_phase_10f_offline_planner \
  --verified-anchors data/pipeline_v2/anchor_evidence/phase_10e_approved_rules/phase_10e_verified_anchors.csv.gz \
  --market-metadata data/pipeline_v2/market_acquisition/partitioned/merged_universes/6f8aa42abec876d3aa1f6336/market_metadata.csv.gz \
  --output-root data/pipeline_v2/horizon_prices/phase_10f_a \
  --guard-root data/pipeline_v2 \
  --config configs/pipeline_v2.toml \
  --empirical-cache-dir data/raw/kalshi/candlesticks_pilot \
  --expected-anchors-sha256 c52e1f26e8e059c305d9f1a3fac311d325ea110552bea044e60dce778c8aacb8 \
  --expected-market-metadata-sha256 7acd4b59afc1ee0d952396cecb062e4216259c0ff4cb4893d5a8e00c50e26c44
```

Add `--preflight-only` for a full scan that writes nothing. An accepted rerun
derives all artifacts in `/tmp`, compares every hash, and preserves the original
publication-time disk snapshot without creating a second guarded copy.

Exact findings: 161,343 in-window verified families; 93,896 PR1-M and 67,447
PR2-M. The families map to 4,640,355 unique contracts. At t−1h, 112,166
families definitely existed, 49,177 definitely opened later, and zero are
unknown. There are 4,586,979 offline-eligible contracts across 16,375 target
timestamps and 58,468 minimum 100-ticker candlestick batches.

Published ignored artifacts:

- `phase_10f_horizon_planner.csv`: 93,981,868 bytes; SHA-256
  `90be78a79d5671006b65e54b2819cc8ad13e115f3875e3f8925be99c9966f41e`;
- `phase_10f_horizon_planner_report.json`: 3,638 bytes; SHA-256
  `5e6f6522dc161baad3492cef64e9d38701d0b7e58aa8d642b22f4b83c3566a89`;
- `phase_10f_price_source_design.md`: 4,244 bytes; SHA-256
  `95f45304e0d2138b18231299fa3d9213cd493b4aa48f985d2187f4991152904b`;
- `phase_10f_storage_preflight.json`: 3,908 bytes; SHA-256
  `92127986894330dbcb8071dcce3298b8744aad57f0a4224ef27bd93dab585eec`;
- `phase_10f_smoke_plan.json`: 85,727 bytes; SHA-256
  `f0d6bf31ee5ef486aa13b4de123b67385f8f278f6ff18921c53986e2d245ac10`.

The compact planner encoding is lossless: each full ticker is reconstructed
from its family prefix and a pipe-delimited suffix token, with explicit escape
forms for non-prefix tickers. It avoids duplicating the family prefix across
4.64 million contracts while preserving exact retrieval identities.

The empirical production projection uses 16 retained local pilot responses
containing 40 returned markets and 864 candles; their deterministic gzip ratio
is 349.825 bytes per returned market. It projects 1,604,639,929 raw bytes,
587,133,312 normalized bytes, and 32,032,768 manifest/provenance bytes:
2,223,806,009 additional namespace bytes total. The conservative projection is
10,690,239,232 bytes. Neither fits the namespace; the conservative model also
crosses the disk floor. Production acquisition remains blocked.

The deterministic 200-family smoke contains 50 short-duration Crypto, 30
existing Crypto, 25 Financials, 15 Climate and Weather, 15 late-opening Sports,
and 65 existing Sports families. It projects 12,137 eligible tickers, 206
minimum requests, and 29,434,112 conservative bytes. It fits the guards but is
not authorized until the project owner freezes the non-mixing analytical price
definition.

The five Phase 10F-A outputs total 94,079,385 bytes. Shared namespace usage is
5,306,392,152 bytes, leaving 62,316,968 bytes. Free disk after publication is
approximately 89,821,372,416 bytes (83.65 GiB). No data was archived, moved, or
deleted.

### Phase 10F-B bounded-smoke hard stop — 2026-08-23

The accepted runner is `scripts.pipeline_v2.run_phase_10f_b_smoke`. Always run
`--preflight-only` first. The production preflight verified 200 families, 65
network skips, 12,137 eligible tickers, 206 groups, 62,316,968 bytes of initial
headroom, and 29,434,112 conservative additional bytes.

The authorized network attempt committed 206 immutable gzip responses and 206
request commits under:

`data/pipeline_v2/horizon_prices/phase_10f_b_smoke/`

All response payloads had the valid top-level schema but contained zero market
objects. No raw response may be redownloaded. Validate the checkpoint without
network by adding `--no-network-resume` to the original command. That rerun
reused all 206 commits, made zero requests, and published:

- `phase_10f_b_incomplete_report.json`: SHA-256
  `a7bdeb4221c45ba42b8ae6c32f271f60d1e4f10d6565eaec0677031333d0d6bb`.

Observed network accounting: 206 requests, zero retries, zero rate limits,
12,137 requested tickers, zero returned markets, zero candles, 109,154
compressed raw bytes, and 2,884 canonical uncompressed response bytes. No
normalized output or final smoke commit exists. Do not run the historical
per-market route until a new bounded sample and its materially larger request
model are explicitly approved.

### Phase 10F-B2 historical validation — 2026-08-23

`scripts.pipeline_v2.run_phase_10f_b2` pins the B hard-stop report, planner,
smoke plan, market metadata, and StudyRules. Its deterministic seed is
`phase-10f-b2-200-ticker-stratified-v1`; sample SHA-256 is
`862afdda09cf5a995bc04e05b8c44628b6521e23b1314fb5fb432f9fd466c83e`.
Run `--preflight-only` before network and `--no-network-resume` afterward.

The completed checkpoint under
`data/pipeline_v2/horizon_prices/phase_10f_b2/` contains 202 immutable raw
responses and request commits. Network totals: 202 requests, zero retries,
zero rate limits, 200 successful historical ticker requests, zero live
requests, zero HTTP failures, 146 empty ticker responses, and 1,164 candles.
The cutoff was retrieved at `2026-08-23T15:24:25.867323+00:00`; its market
settlement boundary was `2026-06-24T00:00:00Z`.

Key artifact hashes:

- ticker sample: `1e24fb430ca28661ef7d46f30dfa3368ca85625c285d2aaff2e3597437325921`;
- normalized diagnostics: `4a5eeda03e765a96c6db52efb3bb24af0790ad737cc064261c245edfab63d3f4`;
- request manifest: `41a72665c0b3472836e014488a73b99e90851d4dcc207302af411e8b0c429d82`;
- provenance: `cc3f57333dfafa45ddbe15a57908b134a306d02fa24032174c937d0e2efefb05`;
- primary report: `b29deb4e46ae09ac9e40b393e15e2fec5212f91a3a662e212a9925f3cb641225`;
- final commit: `880c46d1254bb571ca36f957964010f38e0f4342542b437eed7445d4da5335d4`;
- acceptance projection: `c7821ee78ea3f9b3e150b9c51e439fdc658927da7413ccc022ddb2bd6e5814b0`.

The acceptance projection supersedes the primary report's initial
raw-plus-normalized storage estimate because it also includes independent
request commits and the deterministic manifest. The corrected production
projection is 9,508,027,682 bytes, not 2,466,923,177 bytes. Both estimates
reject production, but the corrected figure is the auditable namespace
requirement.

### Phase 10F-C offline sampling design — 2026-08-23

Generate or deterministically validate the compact planning artifacts with no
network access:

```console
python -m scripts.pipeline_v2.build_phase_10f_c_design --preflight-only
python -m scripts.pipeline_v2.build_phase_10f_c_design
```

The builder pins the Phase 10F-A planner, B2 report, B2 acceptance projection,
and frozen StudyRules. It scans only the outcome-blind planner projection and
does not decode contract identities or draw a production sample. Existing
outputs are immutable: a byte difference fails closed.

Output root:
`data/pipeline_v2/horizon_prices/phase_10f_c/`

- `phase_10f_c_estimand_design.md` —
  `6c53d1dfb925f2f5cd2c34237e583ef9dbaebd4c9f0bf20e19b7d4c6f6dea19a`;
- `phase_10f_c_sampling_frame_report.json` —
  `d4208332f76732b83a1858e083124828efc40b764249427e91987d23d934e697`;
- `phase_10f_c_candidate_sample_sizes.csv` —
  `da03f8733a93676d90edcfee7cdbfad97b0853a0edf77a5087ecb27671869fa0`;
- `phase_10f_c_weighting_specification.md` —
  `1e9e2edf4f6c7b87133da5278196a0f2a959646b9549d16bba4cc34aa8831624`;
- `phase_10f_c_pr1_viability_report.json` —
  `02bf41cb8b0e61244aceac555f981ddf1992b924b55b09189512bbc2f25a0718`;
- `phase_10f_c_missing_price_plan.md` —
  `80607e2402d4a13030c7a0fd78e37459f42bd7b717181d70bcc378f637c874d7`;
- `phase_10f_c_proposed_sampling_manifest_schema.json` —
  `ae17b8ff4372a86df61099d26a5d88fe8d6ed979b80ff047779b415a9d5f54ef`.

The 5,000-family/cap-three recommendation projects 13,032.882 tickers,
13,035 requests including cutoff and boundary controls, 11,206.637 seconds,
and 27,014,949 bytes. These are planning expectations, not an acquired sample.
Never use the schema or deterministic seed to draw production identities until
the methodology is explicitly approved. Phase acceptance: 724 offline tests,
compilation, deterministic rerun, exact artifact hashes, population totals,
quarantine fields, and both storage guards passed.

### Phase 10F-D approved PR2 manifest — 2026-08-24

The offline-only builder uses the pinned Phase 10F-A planner, outcome-free
market metadata, B2 planning evidence, and StudyRules. It uses bundled PyArrow
to project only ticker/event/family/open-time columns while retaining bounded
memory. Always preflight first:

```console
python -m scripts.pipeline_v2.build_phase_10f_d_sampling_manifest --preflight-only
python -m scripts.pipeline_v2.build_phase_10f_d_sampling_manifest
```

The second command now performs a deterministic no-network resume and validates
the immutable outputs under
`data/pipeline_v2/horizon_prices/phase_10f_d/`. It must never redraw a sample
after price availability or outcomes become visible.

Detailed hashes:

- family manifest: `fbf41690c4f7bdd9233131f8e84dc6fe0f47297930a90d688586289af4a8c0f6`;
- contract manifest: `aaaebdc86f01df5d1c6aecc9b985beb0638ebe6db24e8422930d1496e0752c4d`;
- stratum allocation: `6bbfa422b4427e644cd3b68e277b35fcfe066beed22d2737f487390695e40920`;
- inclusion validation: `f93b422db647c149de23aac3195973399f140b0e8d727e9a9d5b71bb4b9ac077`;
- family-weight diagnostics: `71ce9fd1c877294ad4437cef310660895599c6f3b28c9e93d2e334a3facfe38c`;
- contract-weight diagnostics: `cf48c12ebc8bab134ee06a7409d2454ae40da4422fde479304ad0c5f534e67fc`;
- final sampling manifest: `c4861c15bc15bc4775750d466761416e3f6e05ac467e94358e37a6c863952a41`;
- production preflight: `f1a04163010c726ff6089db8df4a5f0e02f6c013d08771807b277796c42b5284`;
- final commit file: `832e6f6e8d5ad19403b1200a1bc2142a3f8339c7926cfd55ab271def46690c96`.

Commit identity is
`8a95158441c245988d2562b732762d9a6f3c5c9cd6d0bb33b9fcc6f3b8de2bc9`.
The manifest contains 5,000 families, 11,573 contracts, and all 51 nonempty
month/size strata. It projects 11,575 total future requests, 9,951.425 seconds,
and 23,988,862 auditable bytes. These requests remain unauthorized. Phase
acceptance is 730 offline tests with zero price, outcome, network, anchor, or
StudyRules change. The fresh acceptance snapshot records 5,307,893,277 guarded
bytes, 60,815,843 namespace bytes remaining, 89,212,518,400 free disk bytes,
and 3,313,172,480 bytes above the disk floor. Recompute rather than reuse these
values at the Phase 10F-E gate.

### Phase 10F-E schema investigation and frozen acquisition — 2026-08-24

The completed output root is
`data/pipeline_v2/horizon_prices/phase_10f_e/`. Existing raw responses,
request commits, partitions, and final artifacts are immutable and ignored by
Git. Never delete, rewrite, or redownload them.

Validate the completed acquisition without network access:

```console
python -m scripts.pipeline_v2.run_phase_10f_e --no-network-resume
```

This command validates the pinned Phase 10F-D sampling identity, all partition
and request-commit hashes, request uniqueness, outcome quarantine, final
artifact hashes, and deterministic replay. It returns `existing_final_reused`
and performs zero network requests.

The separately authorized request-11,060 investigation used:

```console
python -m scripts.pipeline_v2.investigate_phase_10f_e_schema --sample-index 11060
```

Do not rerun it: its immutable report is under
`schema_investigation_11060/`, and its raw response is already the committed
request for partition 111. The report SHA-256 is
`a2344ba0a7dbcce859f87b21008b47af977012b9c1518ba41ff99a892b17f327`;
the compressed raw SHA-256 is
`39a0c5b50459168c162ac5209588562caaa8a8045187d70fffd7e53a6379feaa`.

Final acquisition facts: 11,573 frozen identities, 11,572 successes, one
preserved transport failure, three retries, zero rate limits, 8,860,920
compressed raw bytes, 96,528,568 uncompressed response bytes, and 9,472.671
network-elapsed seconds. The final manifest accounts for 11,578 physical
requests. Including the original pre-persistence schema-failed attempt retained
only in the investigation metadata, the historical run made 11,579 physical
requests in total. No identity was replaced.

Final artifact SHA-256 values:

- acquisition report: `b8ded3848d8a85e7d15a2cf9f839bbf7a491c6c6d2afe08d451616ef553dd553`;
- normalized prices: `11f9ce8d3ed32ad9c3974a7f162c08b414e3aa5b87af80974283fd09175ef0d8`;
- observability report: `efb2ffbd621d6c29a048992f562519c6f53879bfd86778167408f79540696dce`;
- primary midpoint <=15m: `a95a6f3c7f55d5f297b2dfec29dcea591b7f8fb49e52000650ef5b8e22fd4b86`;
- midpoint <=60m: `93f2d6cf67c8d9c8240672512b1d6a9d706f7a4997621fea01f0963eafc54197`;
- trade close <=15m: `39f185734d92b0e6550edbae3ba9c8ca986c0d9e5b33fc75fee6d2afeda23b77`;
- trade close <=60m: `59443fb614502e9515617d78b860d65b40177658144cbb2619b3aa4220eeef71`;
- raw request manifest: `3775c9acc93d87c83b7e2203b69af96da6d44a7ba3738f39df15df0660ed75b4`;
- provenance: `d03fb265c8f3c781280ecfdb333bfb73c226e627434cd149d72cde8654633cbd`.

The final commit identity is
`79e022b7d9d359b484632e82671ef0095eba040687a21bc9a34a9bb947cf08de`.
Guarded storage is 5,348,064,156 bytes with 20,644,964 bytes remaining. Do
not start any new generated-data phase near this ceiling. No outcome access is
authorized; the next action is a final pre-outcome audit and explicit release
of the outcome quarantine.

### Phase 10F final pre-outcome sample audit — 2026-08-24

Run the audit without network or outcome access:

```console
python -m scripts.pipeline_v2.audit_phase_10f_e_sample --preflight-only
python -m scripts.pipeline_v2.audit_phase_10f_e_sample
```

The first command checks the pinned Phase 10F-D/10F-E identities, StudyRules,
analysis-plan hash, storage ceiling, and free-space floor. The second rechecks
all partitions, request commits and raw hashes, exact sample projections,
one-hour targets, at-or-before observation times, midpoint/spread arithmetic,
staleness flags, inclusion probabilities, raw weights, support gates, and
outcome quarantine. Rerunning the second command validates and reuses the
immutable audit.

Generated ignored outputs are under
`data/pipeline_v2/horizon_prices/phase_10f_e/final_pre_outcome_audit/`:

- audit report SHA-256:
  `8ac3fb4b1de1ede9336306589a7b17aa47df94cd1a8245ef441d6360e7c75a73`;
- audit commit SHA-256:
  `6deaaebdd4dd296d90f9651f4e7a43f40472bc07d5d168da4dc5f865f02b3f92`;
- audit identity:
  `bd14ba156585c4b2ed43c798ea55c977e8496326642edca9748eb703491eab24`.

The tracked analysis plan is `PHASE_10F_FINAL_ANALYSIS_PLAN.md`, SHA-256
`1da09fbe7d8fc14a25109c7ebd1f66969ca61e3f64f3cf32b5703dd5109da73b`.
It is recorded before outcome access but its proposed scalar contrast and
bootstrap specification remain an approval gate. Do not read or project the
quarantined outcome artifact until the owner approves the plan and explicitly
releases the quarantine.

The audit adds 14,049 guarded bytes. Final guarded use is 5,348,078,205 bytes,
leaving 20,630,915 bytes under the 5-GiB ceiling. Free disk at publication was
107,925,622,784 bytes (100.51 GiB), safely above the 80-GiB floor.
