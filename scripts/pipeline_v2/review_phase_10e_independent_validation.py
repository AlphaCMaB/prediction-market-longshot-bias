"""Run the fresh 100-case Phase 10E outcome-blind human validation locally."""

from __future__ import annotations

import argparse
from collections import Counter
import json
from pathlib import Path
import sys
import textwrap
from typing import Any, Callable, Iterable, Mapping

from scripts.pipeline_v2.phase_10e_human_review import (
    AMBIGUITY_FLAGS,
    atomic_write_json,
    sha256_file,
)
from scripts.pipeline_v2.phase_10e_independent_validation import (
    atomic_save_independent_decisions,
    build_independent_decision,
    build_independent_validation_report,
    load_ai_assisted_decisions,
    load_independent_decisions,
    load_validation_sources,
    validate_independent_decision,
)
from scripts.pipeline_v2.timing import TIMING_STRUCTURES


DEFAULT_DECISIONS = Path(
    "data/pipeline_v2/anchor_evidence/phase_10e_independent_human_validation/"
    "phase_10e_independent_human_decisions.csv"
)
DEFAULT_REPORT = Path(
    "data/pipeline_v2/anchor_evidence/phase_10e_independent_human_validation/"
    "phase_10e_independent_human_report.json"
)
DEFAULT_GUARD_ROOT = Path("data/pipeline_v2")


def _choice(
    prompt: str, choices: Mapping[str, str], *, input_fn: Callable[[str], str]
) -> str:
    while True:
        value = input_fn(prompt).strip().casefold()
        if value in choices:
            return choices[value]
        print(f"Choose one of: {', '.join(choices)}")


def _flags(*, input_fn: Callable[[str], str]) -> tuple[str, ...]:
    print("Standard ambiguity flags (comma-separated numbers; blank means none):")
    for number, flag in enumerate(AMBIGUITY_FLAGS, 1):
        print(f"  {number:>2}. {flag}")
    while True:
        raw = input_fn("Flags: ").strip()
        if not raw:
            return ()
        try:
            numbers = [int(item.strip()) for item in raw.split(",")]
        except ValueError:
            print("Enter comma-separated flag numbers.")
            continue
        if any(number < 1 or number > len(AMBIGUITY_FLAGS) for number in numbers):
            print("One or more flag numbers are outside the displayed range.")
            continue
        return tuple(sorted({AMBIGUITY_FLAGS[number - 1] for number in numbers}))


def _safe_candidate_lines(row: Mapping[str, Any]) -> list[str]:
    candidates = json.loads(str(row.get("candidates_json") or "[]"))
    lines = []
    for number, candidate in enumerate(candidates, 1):
        lines.extend(
            [
                f"Candidate {number}: {candidate.get('candidate_id') or '[no ID]'}",
                f"  source: {candidate.get('candidate_source_type') or '[none]'}",
                "  exact time: "
                + str(
                    candidate.get("candidate_time_utc")
                    or candidate.get("candidate_date")
                    or "[none]"
                ),
                f"  precision: {candidate.get('candidate_precision') or '[none]'}",
                f"  title: {candidate.get('candidate_title') or '[none]'}",
                f"  evidence reference: {candidate.get('evidence_reference') or '[none]'}",
            ]
        )
        for key, value in sorted(
            (candidate.get("safe_evidence_context") or {}).items()
        ):
            lines.append(f"  {key}: {value}")
    return lines or ["Candidates: [none]"]


def render_case(
    row: Mapping[str, Any], *, position: int, total: int, completed: int
) -> str:
    wrap = lambda value: textwrap.fill(
        str(value or "[none]"),
        width=100,
        initial_indent="  ",
        subsequent_indent="  ",
    )
    return "\n".join(
        [
            "=" * 100,
            f"Independent outcome-blind validation | case {position}/{total} | completed {completed}/{total}",
            "No AI recommendation is displayed. Recommendation stage only; status stays needs_review.",
            "=" * 100,
            f"Validation ID: {row['validation_id']}   Tier: {row['proposed_tier']}   Rule: {row['proposed_rule']}",
            f"Category: {row['category']}   Window: {row['analysis_window_status']}",
            f"Family: {row['family_id']} ({row['family_id_source']})",
            "Family title:",
            wrap(row.get("family_title")),
            "Event title:",
            wrap(row.get("event_title")),
            "Event subtitle:",
            wrap(row.get("event_sub_title")),
            f"Evidence pattern: {row['evidence_pattern']}",
            f"Semantic agreement: {row['semantic_agreement']}",
            f"Candidate count: {row['candidate_count']}   Unique exact times: {row['unique_exact_time_count']}",
            "-" * 100,
            *_safe_candidate_lines(row),
            "-" * 100,
            "Keys: [A] approve candidate  [R] reject  [U] uncertain  [B] back  [Q] save and quit",
        ]
    )


def prompt_for_decision(
    row: Mapping[str, Any],
    *,
    packet_sha256: str,
    manifest_sha256: str,
    input_fn: Callable[[str], str] = input,
) -> dict[str, str] | str:
    decision = _choice(
        "Decision [A/R/U/B/Q]: ",
        {
            "a": "approve_candidate",
            "r": "reject",
            "u": "uncertain",
            "b": "back",
            "q": "quit",
        },
        input_fn=input_fn,
    )
    if decision in {"back", "quit"}:
        return decision
    timing_choices = {str(i): value for i, value in enumerate(TIMING_STRUCTURES, 1)}
    for key, value in timing_choices.items():
        print(f"  {key}. {value}")
    timing = _choice("Timing: ", timing_choices, input_fn=input_fn)
    relevance = _choice(
        "Is this candidate the relevant ex-ante anchor? [Y/N/?]: ",
        {"y": "yes", "n": "no", "?": "uncertain"},
        input_fn=input_fn,
    )
    confidence = _choice(
        "Confidence [H/M/L]: ",
        {"h": "high", "m": "medium", "l": "low"},
        input_fn=input_fn,
    )
    flags = _flags(input_fn=input_fn)
    while True:
        rationale = input_fn(
            "Short rationale"
            + (" (required)" if decision in {"reject", "uncertain"} else " (optional)")
            + ": "
        ).strip()
        if len(rationale) > 500:
            print("Rationale must be 500 characters or fewer.")
            continue
        if decision in {"reject", "uncertain"} and len(rationale) < 8:
            print("Rejection or uncertainty requires at least eight characters.")
            continue
        break
    return build_independent_decision(
        row,
        packet_sha256=packet_sha256,
        manifest_sha256=manifest_sha256,
        decision=decision,
        timing_structure=timing,
        candidate_relevance=relevance,
        confidence=confidence,
        ambiguity_flags=flags,
        rationale=rationale,
    )


def progress_summary(decisions: Iterable[Mapping[str, Any]], total: int) -> str:
    rows = list(decisions)
    counts = Counter(row["independent_human_decision"] for row in rows)
    return (
        f"Progress: {len(rows)}/{total} | approve={counts['approve_candidate']} "
        f"reject={counts['reject']} uncertain={counts['uncertain']}"
    )


def run_interactive(
    packets: list[dict[str, str]],
    manifest_by_id: Mapping[str, Mapping[str, Any]],
    existing_rows: list[dict[str, str]],
    *,
    packet_sha256: str,
    manifest_sha256: str,
    ai_assisted_path: Path,
    decisions_path: Path,
    report_path: Path,
    guard_root: Path,
    existing_sha256: str | None,
    input_fn: Callable[[str], str] = input,
    output_fn: Callable[[str], None] = print,
) -> tuple[list[dict[str, str]], bool]:
    decisions = {row["validation_id"]: dict(row) for row in existing_rows}
    packet_by_id = {row["validation_id"]: row for row in packets}
    current_hash = existing_sha256
    index = next(
        (i for i, row in enumerate(packets) if row["validation_id"] not in decisions),
        len(packets) - 1,
    )
    while len(decisions) < len(packets):
        row = packets[index]
        output_fn(
            render_case(
                row, position=index + 1, total=len(packets), completed=len(decisions)
            )
        )
        response = prompt_for_decision(
            row,
            packet_sha256=packet_sha256,
            manifest_sha256=manifest_sha256,
            input_fn=input_fn,
        )
        if response == "quit":
            output_fn(progress_summary(decisions.values(), len(packets)))
            return list(decisions.values()), False
        if response == "back":
            index = max(0, index - 1)
            continue
        validated = validate_independent_decision(
            response,
            packet_by_validation_id=packet_by_id,
            packet_sha256=packet_sha256,
            manifest_sha256=manifest_sha256,
        )
        decisions[row["validation_id"]] = validated
        current_hash = atomic_save_independent_decisions(
            decisions_path,
            decisions.values(),
            packets=packets,
            packet_sha256=packet_sha256,
            manifest_sha256=manifest_sha256,
            guard_root=guard_root,
            expected_existing_sha256=current_hash,
        )
        output_fn(
            f"Autosaved {row['validation_id']} | {progress_summary(decisions.values(), len(packets))}"
        )
        index = (
            next(
                i
                for i, candidate in enumerate(packets)
                if candidate["validation_id"] not in decisions
            )
            if len(decisions) < len(packets)
            else index
        )
    # The prior AI-assisted decisions are loaded only after all fresh human
    # decisions are complete, so they cannot influence annotation.
    ai_assisted = load_ai_assisted_decisions(ai_assisted_path)
    report = build_independent_validation_report(
        packets,
        manifest_by_id,
        list(decisions.values()),
        ai_assisted,
        packet_sha256=packet_sha256,
        manifest_sha256=manifest_sha256,
    )
    report_hash = atomic_write_json(report_path, report, guard_root=guard_root)
    output_fn(
        "Independent validation complete; report SHA-256 "
        f"{report_hash}. Stop for explicit PR1/PR2 approval."
    )
    return list(decisions.values()), True


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--ai-assisted-decisions", type=Path, required=True)
    parser.add_argument("--expected-packet-sha256", required=True)
    parser.add_argument("--expected-manifest-sha256", required=True)
    parser.add_argument("--decisions", type=Path, default=DEFAULT_DECISIONS)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--guard-root", type=Path, default=DEFAULT_GUARD_ROOT)
    parser.add_argument("--validate-only", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        packet_sha256 = sha256_file(args.packet)
        manifest_sha256 = sha256_file(args.manifest)
        packets, manifests = load_validation_sources(
            args.packet,
            args.manifest,
            expected_packet_sha256=args.expected_packet_sha256,
            expected_manifest_sha256=args.expected_manifest_sha256,
        )
        protected = {
            args.packet.resolve(),
            args.manifest.resolve(),
            args.ai_assisted_decisions.resolve(),
        }
        if args.decisions.resolve() in protected or args.report.resolve() in {
            *protected,
            args.decisions.resolve(),
        }:
            raise ValueError("decisions and report paths must be separate from sources")
        decisions, existing_hash = load_independent_decisions(
            args.decisions,
            packets=packets,
            packet_sha256=packet_sha256,
            manifest_sha256=manifest_sha256,
        )
        if args.validate_only:
            print(progress_summary(decisions, len(packets)))
            print(
                "Source hashes and saved recommendation-only schema passed validation."
            )
            return
        run_interactive(
            packets,
            manifests,
            decisions,
            packet_sha256=packet_sha256,
            manifest_sha256=manifest_sha256,
            ai_assisted_path=args.ai_assisted_decisions,
            decisions_path=args.decisions,
            report_path=args.report,
            guard_root=args.guard_root,
            existing_sha256=existing_hash,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Phase 10E independent validation failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
