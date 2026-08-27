"""Import the completed 100-case Phase 10E independent human review.

The command is local, outcome-blind, recommendation-only, and fail-closed. It
does not apply either proposed rule or modify any production anchor state.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
import shutil
import sys
from typing import Any, Iterable, Mapping
import uuid

from scripts.pipeline_v2.phase_10e_human_review import logical_bytes, sha256_file
from scripts.pipeline_v2.phase_10e_independent_validation import (
    DISAGREEMENT_QUEUE_FIELDS,
    INDEPENDENT_DECISION_FIELDS,
    build_disagreement_queue,
    build_independent_validation_report,
    build_rule_recommendations,
    load_ai_assisted_decisions,
    load_validation_sources,
    parse_independent_review_texts,
)


REQUIRED_OUTPUTS = (
    "phase_10e_independent_human_import_validation_report.json",
    "phase_10e_independent_human_decisions.csv",
    "phase_10e_independent_human_report.json",
    "phase_10e_independent_human_disagreement_queue.csv",
    "phase_10e_rule_recommendations.json",
)


def _write_csv(
    path: Path, rows: Iterable[Mapping[str, Any]], fields: tuple[str, ...]
) -> None:
    with path.open("x", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.write_text(
        json.dumps(value, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _directory_matches(left: Path, right: Path) -> bool:
    if not left.is_dir():
        return False
    left_names = sorted(path.name for path in left.iterdir() if path.is_file())
    right_names = sorted(path.name for path in right.iterdir() if path.is_file())
    return left_names == right_names == sorted(REQUIRED_OUTPUTS) and all(
        sha256_file(left / name) == sha256_file(right / name) for name in left_names
    )


def run(
    source_paths: list[Path],
    source_sha256: list[str],
    packet_path: Path,
    manifest_path: Path,
    ai_assisted_path: Path,
    output_root: Path,
    *,
    expected_packet_sha256: str,
    expected_manifest_sha256: str,
    guard_root: Path,
    max_generated_bytes: int = 5 * 1024**3,
    min_free_bytes: int = 80 * 1024**3,
) -> dict[str, Any]:
    source_packet_before = sha256_file(packet_path)
    source_manifest_before = sha256_file(manifest_path)
    ai_assisted_before = sha256_file(ai_assisted_path)
    packets, manifest_by_id = load_validation_sources(
        packet_path,
        manifest_path,
        expected_packet_sha256=expected_packet_sha256,
        expected_manifest_sha256=expected_manifest_sha256,
    )
    decisions, parse_report = parse_independent_review_texts(
        source_paths,
        expected_source_sha256=source_sha256,
        packets=packets,
        packet_sha256=expected_packet_sha256,
        manifest_sha256=expected_manifest_sha256,
    )
    ai_assisted = load_ai_assisted_decisions(ai_assisted_path)
    report = build_independent_validation_report(
        packets,
        manifest_by_id,
        decisions,
        ai_assisted,
        packet_sha256=expected_packet_sha256,
        manifest_sha256=expected_manifest_sha256,
    )
    queue = build_disagreement_queue(packets, decisions, ai_assisted)
    recommendations = build_rule_recommendations(report)
    import_validation = {
        "schema_version": "1.0",
        "review_type": "fresh_independent_human_outcome_blind_validation",
        **{
            key: value
            for key, value in parse_report.items()
            if key != "source_by_validation_id"
        },
        "source_by_validation_id": parse_report["source_by_validation_id"],
        "unique_audit_id_count": len({row["audit_id"] for row in decisions}),
        "exact_packet_id_match": True,
        "missing_validation_ids": [],
        "extra_validation_ids": [],
        "duplicate_validation_ids": [],
        "cases_by_rule": {
            rule: sum(row["proposed_rule"] == rule for row in decisions)
            for rule in (
                "PR1_FIXED_CLOCK_SINGLE_EXACT",
                "PR2_SCHEDULED_START_SINGLE_MILESTONE",
            )
        },
        "controlled_vocabulary_validation": "passed_preserving_reviewer_terms",
        "source_packet_sha256": expected_packet_sha256,
        "source_manifest_sha256": expected_manifest_sha256,
        "ai_assisted_decisions_sha256": ai_assisted_before,
        "source_packets_preserved": True,
        "prohibited_outcome_or_post_event_fields": 0,
        "verification_status_counts": {"needs_review": len(decisions)},
        "verified_anchor_time_nonblank": 0,
        "verified_anchor_source_nonblank": 0,
        "rules_approved": 0,
        "rules_applied": 0,
        "anchors_verified": 0,
        "horizon_prices_built": False,
        "outcomes_accessed": False,
        "post_event_information_accessed": False,
        "network_requests": 0,
    }

    output_root = output_root.resolve()
    guard_root = guard_root.resolve()
    try:
        output_root.relative_to(guard_root)
    except ValueError as exc:
        raise ValueError(
            "output root must remain inside the generated guard root"
        ) from exc
    protected = {
        packet_path.resolve(),
        manifest_path.resolve(),
        ai_assisted_path.resolve(),
        *(path.resolve() for path in source_paths),
    }
    if output_root in protected:
        raise ValueError("output root must be separate from every source artifact")
    output_root.parent.mkdir(parents=True, exist_ok=True)
    temp_root = output_root.parent / f".{output_root.name}.work-{uuid.uuid4().hex}"
    temp_root.mkdir()
    try:
        _write_csv(
            temp_root / REQUIRED_OUTPUTS[1], decisions, INDEPENDENT_DECISION_FIELDS
        )
        decisions_hash = sha256_file(temp_root / REQUIRED_OUTPUTS[1])
        report["independent_human_decisions_sha256"] = decisions_hash
        report["disagreement_queue_case_count"] = len(queue)
        _write_json(temp_root / REQUIRED_OUTPUTS[2], report)
        _write_csv(temp_root / REQUIRED_OUTPUTS[3], queue, DISAGREEMENT_QUEUE_FIELDS)
        recommendations["independent_human_report_sha256"] = sha256_file(
            temp_root / REQUIRED_OUTPUTS[2]
        )
        _write_json(temp_root / REQUIRED_OUTPUTS[4], recommendations)
        import_validation["independent_human_decisions_sha256"] = decisions_hash
        import_validation["independent_human_report_sha256"] = sha256_file(
            temp_root / REQUIRED_OUTPUTS[2]
        )
        import_validation["disagreement_queue_sha256"] = sha256_file(
            temp_root / REQUIRED_OUTPUTS[3]
        )
        import_validation["rule_recommendations_sha256"] = sha256_file(
            temp_root / REQUIRED_OUTPUTS[4]
        )
        _write_json(temp_root / REQUIRED_OUTPUTS[0], import_validation)
        publication_hashes = {
            name: sha256_file(temp_root / name) for name in REQUIRED_OUTPUTS
        }
        new_bytes = logical_bytes(temp_root)
        if logical_bytes(guard_root) + new_bytes > max_generated_bytes:
            raise ValueError("independent review import would exceed namespace ceiling")
        if shutil.disk_usage(guard_root).free - new_bytes < min_free_bytes:
            raise ValueError("independent review import would cross free-disk floor")
        if output_root.exists():
            if not _directory_matches(output_root, temp_root):
                raise ValueError(
                    "existing independent-review output conflicts with rerun"
                )
            shutil.rmtree(temp_root)
        else:
            os.replace(temp_root, output_root)
        for name, expected_hash in publication_hashes.items():
            if sha256_file(output_root / name) != expected_hash:
                raise ValueError("post-publication artifact hash validation failed")
    except Exception:
        if temp_root.exists():
            shutil.rmtree(temp_root)
        raise

    if (
        sha256_file(packet_path) != source_packet_before
        or sha256_file(manifest_path) != source_manifest_before
        or sha256_file(ai_assisted_path) != ai_assisted_before
    ):
        raise ValueError(
            "a hash-pinned source changed during independent-review import"
        )
    summary = {
        "reviewed_cases": len(decisions),
        "decision_counts": parse_report["decision_counts"],
        "confidence_counts": parse_report["confidence_counts"],
        "disagreement_cases": len(queue),
        "rules_approved": 0,
        "anchors_verified": 0,
        "outcomes_accessed": False,
        "network_requests": 0,
        "output_root": str(output_root),
        "output_hashes": {
            name: sha256_file(output_root / name) for name in REQUIRED_OUTPUTS
        },
        "output_bytes": {
            name: (output_root / name).stat().st_size for name in REQUIRED_OUTPUTS
        },
    }
    print(json.dumps(summary, sort_keys=True))
    return summary


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--review-source", type=Path, action="append", required=True)
    parser.add_argument(
        "--expected-review-source-sha256", action="append", required=True
    )
    parser.add_argument("--packet", type=Path, required=True)
    parser.add_argument("--manifest", type=Path, required=True)
    parser.add_argument("--ai-assisted-decisions", type=Path, required=True)
    parser.add_argument("--expected-packet-sha256", required=True)
    parser.add_argument("--expected-manifest-sha256", required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--guard-root", type=Path, required=True)
    parser.add_argument("--max-generated-bytes", type=int, default=5 * 1024**3)
    parser.add_argument("--min-free-bytes", type=int, default=80 * 1024**3)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        run(
            args.review_source,
            args.expected_review_source_sha256,
            args.packet,
            args.manifest,
            args.ai_assisted_decisions,
            args.output_root,
            expected_packet_sha256=args.expected_packet_sha256,
            expected_manifest_sha256=args.expected_manifest_sha256,
            guard_root=args.guard_root,
            max_generated_bytes=args.max_generated_bytes,
            min_free_bytes=args.min_free_bytes,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Phase 10E independent-review import failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
