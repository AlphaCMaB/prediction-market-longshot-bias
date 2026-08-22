"""Import finalized AI-assisted annotations and build a fresh human audit.

This local command is outcome-blind and recommendation-only. It never issues a
network request, applies PR1/PR2, verifies anchors, or constructs prices.
"""

from __future__ import annotations

import argparse
import csv
import json
import os
from pathlib import Path
import shutil
import sys
import uuid
from typing import Any, Iterable, Mapping

from scripts.pipeline_v2.phase_10e_ai_assisted_review import (
    AI_ASSISTED_DECISION_FIELDS,
    EXPECTED_FIRST_REVIEW_SHA256,
    EXPECTED_PACKET_SHA256,
    EXPECTED_SUBSET_SHA256,
    INDEPENDENT_MANIFEST_FIELDS,
    INDEPENDENT_PACKET_FIELDS,
    build_ai_assisted_diagnostics,
    build_independent_validation_design,
    import_ai_assisted_annotations,
    load_first_review,
)
from scripts.pipeline_v2.phase_10e_human_review import (
    load_review_subset,
    logical_bytes,
    sha256_file,
)


REQUIRED_OUTPUTS = (
    "phase_10e_ai_assisted_import_validation_report.json",
    "phase_10e_ai_assisted_decisions.csv",
    "phase_10e_ai_assisted_diagnostics.json",
    "phase_10e_independent_human_packet.csv",
    "phase_10e_independent_human_sample_manifest.csv",
    "phase_10e_independent_human_design_report.json",
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
    annotation_path: Path,
    subset_path: Path,
    packet_path: Path,
    first_review_path: Path,
    output_root: Path,
    *,
    guard_root: Path,
    expected_annotation_sha256: str,
    expected_subset_sha256: str = EXPECTED_SUBSET_SHA256,
    expected_packet_sha256: str = EXPECTED_PACKET_SHA256,
    expected_first_review_sha256: str = EXPECTED_FIRST_REVIEW_SHA256,
    max_generated_bytes: int = 5 * 1024**3,
    min_free_bytes: int = 80 * 1024**3,
) -> dict[str, Any]:
    if expected_subset_sha256 != EXPECTED_SUBSET_SHA256:
        raise ValueError("only the hash-pinned 165-case subset may be imported")
    if expected_packet_sha256 != EXPECTED_PACKET_SHA256:
        raise ValueError("only the hash-pinned canonical audit packet may be imported")
    if expected_first_review_sha256 != EXPECTED_FIRST_REVIEW_SHA256:
        raise ValueError("only the hash-pinned first-review artifact may be used")
    subset = load_review_subset(
        subset_path,
        expected_subset_sha256=expected_subset_sha256,
        packet_path=packet_path,
        expected_packet_sha256=expected_packet_sha256,
    )
    first_review = load_first_review(first_review_path)
    imported = import_ai_assisted_annotations(
        annotation_path,
        subset,
        expected_annotation_sha256=expected_annotation_sha256,
    )
    diagnostics, analysis_rows = build_ai_assisted_diagnostics(
        subset, imported, first_review
    )
    packets, manifests, validation_report = build_independent_validation_design(
        analysis_rows, per_rule=50
    )

    output_root = output_root.resolve()
    guard_root = guard_root.resolve()
    try:
        output_root.relative_to(guard_root)
    except ValueError as exc:
        raise ValueError(
            "output root must remain inside the generated guard root"
        ) from exc
    output_root.parent.mkdir(parents=True, exist_ok=True)
    temp_root = output_root.parent / f".{output_root.name}.work-{uuid.uuid4().hex}"
    temp_root.mkdir()
    try:
        annotation_hash = sha256_file(annotation_path)
        validation_summary = {
            "schema_version": "1.0",
            "annotation_type": "ai_assisted_outcome_blind_review",
            "source_annotation_path": str(annotation_path.resolve()),
            "source_annotation_sha256": annotation_hash,
            "source_annotation_bytes": annotation_path.stat().st_size,
            "row_count": len(imported),
            "unique_audit_id_count": len({row["audit_id"] for row in imported}),
            "audit_ids_exactly_match_immutable_subset": True,
            "decision_counts": {
                "A_approve": sum(
                    row["ai_assisted_decision"] == "approve_candidate"
                    for row in imported
                ),
                "R_reject": sum(
                    row["ai_assisted_decision"] == "reject" for row in imported
                ),
                "U_uncertain": sum(
                    row["ai_assisted_decision"] == "uncertain" for row in imported
                ),
            },
            "controlled_vocabulary_validation": "passed",
            "finalized_correction_invariants": "passed",
            "source_subset_sha256": expected_subset_sha256,
            "source_packet_sha256": expected_packet_sha256,
            "source_first_review_sha256": expected_first_review_sha256,
            "source_packets_preserved": True,
            "verification_status_counts": {"needs_review": len(imported)},
            "verified_anchor_time_nonblank": 0,
            "verified_anchor_source_nonblank": 0,
            "rules_approved": 0,
            "outcomes_accessed": False,
            "post_event_information_accessed": False,
            "horizon_prices_built": False,
            "network_requests": 0,
        }
        _write_json(temp_root / REQUIRED_OUTPUTS[0], validation_summary)
        _write_csv(
            temp_root / REQUIRED_OUTPUTS[1], imported, AI_ASSISTED_DECISION_FIELDS
        )
        _write_json(temp_root / REQUIRED_OUTPUTS[2], diagnostics)
        _write_csv(temp_root / REQUIRED_OUTPUTS[3], packets, INDEPENDENT_PACKET_FIELDS)
        _write_csv(
            temp_root / REQUIRED_OUTPUTS[4], manifests, INDEPENDENT_MANIFEST_FIELDS
        )
        packet_hash = sha256_file(temp_root / REQUIRED_OUTPUTS[3])
        manifest_hash = sha256_file(temp_root / REQUIRED_OUTPUTS[4])
        validation_report.update(
            {
                "source_annotation_sha256": expected_annotation_sha256,
                "source_subset_sha256": expected_subset_sha256,
                "source_packet_sha256": expected_packet_sha256,
                "source_first_review_sha256": expected_first_review_sha256,
                "independent_human_packet_sha256": packet_hash,
                "independent_human_sample_manifest_sha256": manifest_hash,
                "exact_start_command": (
                    "python -m scripts.pipeline_v2.review_phase_10e_independent_validation "
                    f"--packet {output_root / REQUIRED_OUTPUTS[3]} "
                    f"--manifest {output_root / REQUIRED_OUTPUTS[4]} "
                    f"--ai-assisted-decisions {output_root / REQUIRED_OUTPUTS[1]} "
                    f"--expected-packet-sha256 {packet_hash} "
                    f"--expected-manifest-sha256 {manifest_hash}"
                ),
            }
        )
        _write_json(temp_root / REQUIRED_OUTPUTS[5], validation_report)
        publication_hashes = {
            name: sha256_file(temp_root / name) for name in REQUIRED_OUTPUTS
        }
        new_bytes = logical_bytes(temp_root)
        if logical_bytes(guard_root) + new_bytes > max_generated_bytes:
            raise ValueError(
                "review import would exceed the generated-namespace ceiling"
            )
        if shutil.disk_usage(guard_root).free - new_bytes < min_free_bytes:
            raise ValueError("review import would cross the minimum free-disk floor")
        if output_root.exists():
            if not _directory_matches(output_root, temp_root):
                raise ValueError(
                    "existing AI-assisted review output conflicts with rerun"
                )
            shutil.rmtree(temp_root)
        else:
            os.replace(temp_root, output_root)
        for name, expected_hash in publication_hashes.items():
            if sha256_file(output_root / name) != expected_hash:
                raise ValueError(
                    "post-publication review artifact hash validation failed"
                )
    except Exception:
        if temp_root.exists():
            shutil.rmtree(temp_root)
        raise

    summary = {
        "annotation_type": "ai_assisted_outcome_blind_review",
        "imported_cases": len(imported),
        "verification_status_counts": {"needs_review": len(imported)},
        "verified_anchor_time_nonblank": 0,
        "verified_anchor_source_nonblank": 0,
        "independent_validation_cases": len(packets),
        "independent_validation_by_rule": validation_report["sample_counts_by_rule"],
        "output_root": str(output_root),
        "output_hashes": {
            name: sha256_file(output_root / name) for name in REQUIRED_OUTPUTS
        },
        "output_bytes": {
            name: (output_root / name).stat().st_size for name in REQUIRED_OUTPUTS
        },
        "anchors_verified": 0,
        "rules_approved": 0,
        "outcomes_accessed": False,
        "post_event_information_accessed": False,
        "horizon_prices_built": False,
        "network_requests": 0,
    }
    print(json.dumps(summary, sort_keys=True))
    return summary


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--annotation", type=Path, required=True)
    parser.add_argument("--expected-annotation-sha256", required=True)
    parser.add_argument("--subset", type=Path, required=True)
    parser.add_argument("--packet", type=Path, required=True)
    parser.add_argument("--first-review", type=Path, required=True)
    parser.add_argument("--output-root", type=Path, required=True)
    parser.add_argument("--guard-root", type=Path, required=True)
    parser.add_argument("--expected-subset-sha256", default=EXPECTED_SUBSET_SHA256)
    parser.add_argument("--expected-packet-sha256", default=EXPECTED_PACKET_SHA256)
    parser.add_argument(
        "--expected-first-review-sha256", default=EXPECTED_FIRST_REVIEW_SHA256
    )
    parser.add_argument("--max-generated-bytes", type=int, default=5 * 1024**3)
    parser.add_argument("--min-free-bytes", type=int, default=80 * 1024**3)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        run(
            args.annotation,
            args.subset,
            args.packet,
            args.first_review,
            args.output_root,
            guard_root=args.guard_root,
            expected_annotation_sha256=args.expected_annotation_sha256,
            expected_subset_sha256=args.expected_subset_sha256,
            expected_packet_sha256=args.expected_packet_sha256,
            expected_first_review_sha256=args.expected_first_review_sha256,
            max_generated_bytes=args.max_generated_bytes,
            min_free_bytes=args.min_free_bytes,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Phase 10E AI-assisted import failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
