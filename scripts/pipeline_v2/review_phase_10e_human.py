"""Run the compact local Phase 10E outcome-blind human-review interface.

This interface records recommendations only. It never calls a network client,
applies a verification decision, constructs a horizon, or reads an outcome.
"""

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
    EXPECTED_PACKET_SHA256,
    EXPECTED_SUBSET_SHA256,
    atomic_save_human_decisions,
    atomic_write_json,
    build_final_report,
    build_human_decision,
    load_human_decisions,
    load_review_subset,
    validate_human_decision,
)
from scripts.pipeline_v2.timing import TIMING_STRUCTURES


DEFAULT_SUBSET = Path(
    "data/pipeline_v2/anchor_evidence/phase_10e_first_review/"
    "phase_10e_human_review_subset.csv"
)
DEFAULT_PACKET = Path(
    "data/pipeline_v2/anchor_evidence/phase_10e_design/"
    "phase_10e_audit_review_packet.csv"
)
DEFAULT_DECISIONS = Path(
    "data/pipeline_v2/anchor_evidence/phase_10e_human_review/"
    "phase_10e_human_decisions.csv"
)
DEFAULT_REPORT = Path(
    "data/pipeline_v2/anchor_evidence/phase_10e_human_review/"
    "phase_10e_human_review_report.json"
)
DEFAULT_GUARD_ROOT = Path("data/pipeline_v2")


def _wrap(value: Any, *, width: int = 100, indent: str = "  ") -> str:
    text = str(value or "[none]")
    return textwrap.fill(
        text, width=width, initial_indent=indent, subsequent_indent=indent
    )


def _safe_candidate_lines(row: Mapping[str, Any]) -> list[str]:
    candidates = json.loads(str(row.get("candidates_json") or "[]"))
    lines = []
    for number, candidate in enumerate(candidates, 1):
        lines.extend(
            [
                f"Candidate {number}: {candidate.get('candidate_id') or '[no ID]'}",
                f"  source: {candidate.get('candidate_source_type') or '[none]'}",
                f"  exact time: {candidate.get('candidate_time_utc') or candidate.get('candidate_date') or '[none]'}",
                f"  precision: {candidate.get('candidate_precision') or '[none]'}",
                f"  title: {candidate.get('candidate_title') or '[none]'}",
                f"  evidence reference: {candidate.get('evidence_reference') or '[none]'}",
            ]
        )
        context = candidate.get("safe_evidence_context") or {}
        for key in sorted(context):
            lines.append(f"  {key}: {context[key]}")
    return lines or ["Candidates: [none]"]


def render_case(
    row: Mapping[str, Any], *, position: int, total: int, completed: int
) -> str:
    lines = [
        "=" * 100,
        f"Phase 10E outcome-blind human review | case {position}/{total} | completed {completed}/{total}",
        "Recommendation stage only: all actual statuses remain needs_review.",
        "=" * 100,
        f"Audit ID: {row['audit_id']}   Tier: {row['proposed_tier']}   Rule: {row['proposed_rule']}",
        f"Category: {row['category']}   Analysis-window status: {row['analysis_window_status']}",
        f"Family: {row['family_id']} ({row['family_id_source']})",
        "Family title:",
        _wrap(row.get("family_title")),
        "Event title:",
        _wrap(row.get("event_title")),
        "Event subtitle:",
        _wrap(row.get("event_sub_title")),
        f"Evidence pattern: {row['evidence_pattern']}",
        f"Semantic agreement: {row['semantic_agreement']}",
        f"Candidate count: {row['candidate_count']}   Unique exact times: {row['unique_exact_time_count']}",
        "-" * 100,
        *_safe_candidate_lines(row),
        "-" * 100,
        "Decision keys: [A] approve candidate  [R] reject  [U] uncertain  [B] back  [Q] save and quit",
    ]
    return "\n".join(lines)


def _choice(
    prompt: str,
    choices: Mapping[str, str],
    *,
    input_fn: Callable[[str], str] = input,
) -> str:
    while True:
        value = input_fn(prompt).strip().casefold()
        if value in choices:
            return choices[value]
        print(f"Choose one of: {', '.join(choices)}")


def _flags(*, input_fn: Callable[[str], str] = input) -> tuple[str, ...]:
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


def prompt_for_decision(
    row: Mapping[str, Any],
    *,
    input_fn: Callable[[str], str] = input,
) -> dict[str, str] | str:
    action = _choice(
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
    if action in {"back", "quit"}:
        return action
    timing_choices = {str(i): value for i, value in enumerate(TIMING_STRUCTURES, 1)}
    print("Timing structures:")
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
            + (" (required)" if action in {"reject", "uncertain"} else " (optional)")
            + ": "
        ).strip()
        if len(rationale) > 500:
            print("Rationale must be 500 characters or fewer.")
            continue
        if action in {"reject", "uncertain"} and len(rationale) < 8:
            print("Rejection or uncertainty requires at least eight characters.")
            continue
        break
    return build_human_decision(
        row,
        human_decision=action,
        timing_structure=timing,
        candidate_relevance=relevance,
        confidence=confidence,
        ambiguity_flags=flags,
        rationale=rationale,
    )


def progress_summary(decisions: Iterable[Mapping[str, Any]], total: int) -> str:
    rows = list(decisions)
    counts = Counter(str(row["human_decision"]) for row in rows)
    return (
        f"Progress: {len(rows)}/{total} | approve={counts['approve_candidate']} "
        f"reject={counts['reject']} uncertain={counts['uncertain']}"
    )


def run_interactive(
    subset_rows: list[dict[str, str]],
    existing_rows: list[dict[str, str]],
    *,
    decisions_path: Path,
    report_path: Path,
    guard_root: Path,
    existing_sha256: str | None,
    input_fn: Callable[[str], str] = input,
    output_fn: Callable[[str], None] = print,
) -> tuple[list[dict[str, str]], bool]:
    decisions = {row["audit_id"]: dict(row) for row in existing_rows}
    subset_by_id = {row["audit_id"]: row for row in subset_rows}
    if len(decisions) == len(subset_rows):
        report = build_final_report(subset_rows, list(decisions.values()))
        report_hash = atomic_write_json(report_path, report, guard_root=guard_root)
        output_fn(
            "Human review was already complete. Exact recommendation-only schema "
            f"validation passed; report SHA-256 {report_hash}."
        )
        return list(decisions.values()), True
    index = next(
        (
            number
            for number, row in enumerate(subset_rows)
            if row["audit_id"] not in decisions
        ),
        len(subset_rows) - 1,
    )
    current_hash = existing_sha256
    while True:
        row = subset_rows[index]
        output_fn(
            render_case(
                row,
                position=index + 1,
                total=len(subset_rows),
                completed=len(decisions),
            )
        )
        if row["audit_id"] in decisions:
            output_fn(
                "This case already has a saved decision; a new entry will replace it."
            )
        response = prompt_for_decision(row, input_fn=input_fn)
        if response == "quit":
            output_fn(progress_summary(decisions.values(), len(subset_rows)))
            return list(decisions.values()), False
        if response == "back":
            index = max(0, index - 1)
            continue
        validated = validate_human_decision(response, subset_by_id=subset_by_id)
        decisions[row["audit_id"]] = validated
        current_hash = atomic_save_human_decisions(
            decisions_path,
            decisions.values(),
            subset_rows=subset_rows,
            guard_root=guard_root,
            expected_existing_sha256=current_hash,
        )
        output_fn(
            f"Autosaved {row['audit_id']} | {progress_summary(decisions.values(), len(subset_rows))}"
        )
        if len(decisions) == len(subset_rows):
            report = build_final_report(subset_rows, list(decisions.values()))
            report_hash = atomic_write_json(report_path, report, guard_root=guard_root)
            output_fn(
                "Human review complete. Exact recommendation-only schema validation passed; "
                f"report SHA-256 {report_hash}. Stop for explicit PR1/PR2 approval."
            )
            return list(decisions.values()), True
        index = next(
            number
            for number, candidate in enumerate(subset_rows)
            if candidate["audit_id"] not in decisions
        )


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--subset", type=Path, default=DEFAULT_SUBSET)
    parser.add_argument("--packet", type=Path, default=DEFAULT_PACKET)
    parser.add_argument("--decisions", type=Path, default=DEFAULT_DECISIONS)
    parser.add_argument("--report", type=Path, default=DEFAULT_REPORT)
    parser.add_argument("--guard-root", type=Path, default=DEFAULT_GUARD_ROOT)
    parser.add_argument("--expected-subset-sha256", default=EXPECTED_SUBSET_SHA256)
    parser.add_argument("--expected-packet-sha256", default=EXPECTED_PACKET_SHA256)
    parser.add_argument(
        "--validate-only",
        action="store_true",
        help="Validate source and saved decisions without starting an interactive review.",
    )
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        if args.expected_subset_sha256 != EXPECTED_SUBSET_SHA256:
            raise ValueError("the interface only accepts the hash-pinned human subset")
        if args.expected_packet_sha256 != EXPECTED_PACKET_SHA256:
            raise ValueError("the interface only accepts the hash-pinned audit packet")
        subset = load_review_subset(
            args.subset,
            expected_subset_sha256=args.expected_subset_sha256,
            packet_path=args.packet,
            expected_packet_sha256=args.expected_packet_sha256,
        )
        decisions, decisions_hash = load_human_decisions(
            args.decisions, subset_rows=subset
        )
        source_paths = {args.subset.resolve(), args.packet.resolve()}
        if args.decisions.resolve() in source_paths:
            raise ValueError(
                "decisions path must be separate from hash-pinned source packets"
            )
        if args.report.resolve() in {*source_paths, args.decisions.resolve()}:
            raise ValueError("report path must be separate from sources and decisions")
        if args.validate_only:
            print(progress_summary(decisions, len(subset)))
            if len(decisions) == len(subset):
                build_final_report(subset, decisions)
                print(
                    "Complete decision set passes exact recommendation-only schema validation."
                )
            else:
                print(
                    "Decision set is valid but incomplete; no final report calculated."
                )
            return
        run_interactive(
            subset,
            decisions,
            decisions_path=args.decisions,
            report_path=args.report,
            guard_root=args.guard_root,
            existing_sha256=decisions_hash,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Phase 10E human review failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
