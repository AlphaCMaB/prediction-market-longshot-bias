# Next Actions

Last updated: 2026-08-22

## Phase 10A-R — completed

All architecture, safety, normalization, merge, reporting, and validation work
is implemented. Acceptance result: 614 tests passed with no production data or
network requests.

## Phase 10A-S — completed

The bounded smoke test passed with 2 requests, 0 retries, 80,059 compressed raw
bytes, 1,000 in-range rows, 0 rejects, a valid nonterminal partition commit,
correct resume state, and a fail-closed incomplete merge.

## Phase 10B — completed

All historical and live segments are terminal. The validated merge contains
9,861,209 outcome-free market rows and 427,090 event tickers. Outcomes remain
separate and quarantined.

## Phase 10C — completed

Implementation, production preflight, corrected 200-event smoke, all 86
production partitions, and the deterministic final merge are complete.
Production retrieved all 427,090 events and published 208,598 milestone
associations with zero missing events or merge conflicts. Final no-network
resume, recursive quarantine inspection, hashes, disk accounting, and the
636-test acceptance suite pass.

## Phase 10D — completed

The complete outcome-free production universes produced 625,923 candidate rows
for 427,090 composite families. All candidate, family-review, and decision
rows remain `needs_review`; verified fields are blank, zero anchors were
verified, and zero outcomes were read. All four required outputs passed
deterministic rerun, quarantine, schema, hash, and storage validation.

## Phase 10E — AI-assisted review imported; fresh human validation pending

The finalized 165-case table is an AI-assisted outcome-blind review. It has
been hash-pinned, validated, imported separately, and diagnosed without
changing any production verification status. PR1 and PR2 remain proposals,
not approved rules.

1. Independently review the fresh blinded 100-case packet (50 PR1, 50 PR2)
   without outcomes, post-event information, post-anchor prices, or prior AI
   decisions.
2. Calculate independent-human approval, confirmed false-positive,
   uncertainty, and AI-assisted-versus-human disagreement rates.
3. Request explicit approval, modification, or rejection of each proposed rule.
4. Do not apply decisions, construct horizons, acquire prices, or access outcomes
   until that approval is recorded.

Start or resume the local review from the repository root:

```console
python -m scripts.pipeline_v2.review_phase_10e_independent_validation \
  --packet data/pipeline_v2/anchor_evidence/phase_10e_ai_assisted_review/phase_10e_independent_human_packet.csv \
  --manifest data/pipeline_v2/anchor_evidence/phase_10e_ai_assisted_review/phase_10e_independent_human_sample_manifest.csv \
  --ai-assisted-decisions data/pipeline_v2/anchor_evidence/phase_10e_ai_assisted_review/phase_10e_ai_assisted_decisions.csv \
  --expected-packet-sha256 99269b00ac01dec9e55ea1eb80884a629fb8131ea13d6485b1f5b055f134015c \
  --expected-manifest-sha256 60b8fab36351b1e59114879a65fb830cc1f6e09669dd7a50f87a32b5c3de6575
```

The command hash-validates both source packets and autosaves after every
completed case. Use `Q` to stop safely; the next invocation resumes at the
first incomplete case. PR1 must be modified and PR2 must be audited/tightened
as recorded in `DECISION_LOG.md`; neither rule is approved.

## Approval gates

Ask the project owner before changing the frozen methodology, deleting any
validated data, exceeding 5 GiB of generated data, allowing free space below
80 GiB, using credentials/private access, or choosing between research designs
with materially different interpretations.
