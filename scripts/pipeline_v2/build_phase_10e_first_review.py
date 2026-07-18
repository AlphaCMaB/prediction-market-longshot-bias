"""Build compact recommendation-only Phase 10E AI first-review outputs."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import uuid
from typing import Any, Iterable, Mapping

from scripts.pipeline_v2.phase_10e_first_review import (
    REVIEW_PROTOCOL_VERSION,
    FirstReview,
    review_case,
)
from scripts.pipeline_v2.study_rules import validate_research_feature_columns


FIRST_REVIEW_FIELDS = (
    "audit_id",
    "family_id",
    "family_id_source",
    "proposed_tier",
    "proposed_rule",
    "category",
    "analysis_month_or_status",
    "candidate_source_type",
    "proposed_candidate_time",
    "sampling_weight",
    "review_protocol_version",
    "reviewer_decision",
    "recommended_verification_status",
    "recommended_timing_structure",
    "candidate_is_relevant_ex_ante_anchor",
    "confidence",
    "concise_rationale",
    "ambiguity_flags_json",
    "human_review_required",
)
QUEUE_FIELDS = (*FIRST_REVIEW_FIELDS, "queue_reason")
REQUIRED_OUTPUTS = (
    "phase_10e_first_review.csv",
    "phase_10e_first_review_report.json",
    "phase_10e_human_review_subset.csv",
    "phase_10e_disagreement_and_uncertainty_queue.csv",
)
HUMAN_SUBSET_SEED = "phase-10e-human-double-review-v1"
TIER3_DIAGNOSTIC_SEED = "phase-10e-tier3-diagnostic-v1"


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _logical_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _write_csv(path: Path, rows: Iterable[Mapping[str, Any]], fields: tuple[str, ...]):
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields, lineterminator="\n")
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def _review_mapping(review: FirstReview) -> dict[str, Any]:
    return {
        "review_protocol_version": REVIEW_PROTOCOL_VERSION,
        "reviewer_decision": review.reviewer_decision,
        "recommended_verification_status": review.recommended_verification_status,
        "recommended_timing_structure": review.recommended_timing_structure,
        "candidate_is_relevant_ex_ante_anchor": review.candidate_is_relevant_ex_ante_anchor,
        "confidence": review.confidence,
        "concise_rationale": review.concise_rationale,
        "ambiguity_flags_json": json.dumps(
            list(review.ambiguity_flags), separators=(",", ":")
        ),
        "human_review_required": str(review.human_review_required).lower(),
    }


def _month_or_status(row: Mapping[str, Any]) -> str:
    timestamp = str(row.get("proposed_candidate_time") or "")
    status = str(row.get("analysis_window_status") or "")
    return timestamp[:7] if status == "inside_analysis_window" else status


def _rank(row: Mapping[str, Any], seed: str) -> str:
    payload = "\x00".join(
        (
            seed,
            str(row.get("proposed_tier") or ""),
            str(row.get("family_id") or ""),
            str(row.get("family_id_source") or ""),
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _group_rates(rows: Iterable[Mapping[str, Any]]) -> dict[str, Any]:
    values = tuple(rows)
    count = len(values)
    total_weight = sum(float(row["sampling_weight"]) for row in values)
    decisions = Counter(str(row["reviewer_decision"]) for row in values)
    confidence = Counter(str(row["confidence"]) for row in values)
    flags = Counter(
        flag for row in values for flag in json.loads(str(row["ambiguity_flags_json"]))
    )
    weighted_decisions = Counter()
    weighted_flags = Counter()
    for row in values:
        weight = float(row["sampling_weight"])
        weighted_decisions[str(row["reviewer_decision"])] += weight
        for flag in json.loads(str(row["ambiguity_flags_json"])):
            weighted_flags[flag] += weight
    return {
        "sample_count": count,
        "weighted_population": round(total_weight, 6),
        "unweighted_decision_counts": dict(sorted(decisions.items())),
        "unweighted_decision_rates": {
            key: round(value / count, 6) if count else 0
            for key, value in sorted(decisions.items())
        },
        "weighted_decision_population_estimates": {
            key: round(value, 6) for key, value in sorted(weighted_decisions.items())
        },
        "weighted_decision_rates": {
            key: round(value / total_weight, 6) if total_weight else 0
            for key, value in sorted(weighted_decisions.items())
        },
        "confidence_counts": dict(sorted(confidence.items())),
        "ambiguity_flag_counts": dict(sorted(flags.items())),
        "weighted_ambiguity_population_estimates": {
            key: round(value, 6) for key, value in sorted(weighted_flags.items())
        },
    }


def _breakdowns(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    groups = defaultdict(list)
    for row in rows:
        groups[str(row.get(field) or "[missing]")].append(row)
    return {key: _group_rates(groups[key]) for key in sorted(groups)}


def _compare_existing(existing: Path, derived: Path) -> bool:
    if not existing.is_dir():
        return False
    names = tuple(sorted(path.name for path in existing.iterdir() if path.is_file()))
    if names != tuple(sorted(REQUIRED_OUTPUTS)):
        return False
    return all(_sha256(existing / name) == _sha256(derived / name) for name in names)


def run(
    packet_path: Path,
    output_root: Path,
    *,
    guard_root: Path,
    expected_packet_sha256: str | None = None,
    max_generated_bytes: int = 5 * 1024**3,
    min_free_bytes: int = 80 * 1024**3,
) -> dict[str, Any]:
    packet_hash = _sha256(packet_path)
    if expected_packet_sha256 and packet_hash != expected_packet_sha256:
        raise ValueError("audit packet SHA-256 does not match the approved design")
    with packet_path.open(newline="", encoding="utf-8") as handle:
        packet_rows = list(csv.DictReader(handle))
    if len(packet_rows) != 450:
        raise ValueError("approved audit packet must contain exactly 450 families")
    identities = {(row["family_id"], row["family_id_source"]) for row in packet_rows}
    if len(identities) != len(packet_rows):
        raise ValueError("audit packet contains duplicate family identities")

    reviews = []
    packet_by_id = {}
    for packet in packet_rows:
        review = review_case(packet)
        row = {
            "audit_id": packet["audit_id"],
            "family_id": packet["family_id"],
            "family_id_source": packet["family_id_source"],
            "proposed_tier": packet["proposed_tier"],
            "proposed_rule": packet["proposed_rule"],
            "category": packet["category"],
            "analysis_month_or_status": _month_or_status(packet),
            "candidate_source_type": packet["proposed_candidate_source_type"],
            "proposed_candidate_time": packet["proposed_candidate_time"],
            "sampling_weight": packet["sampling_weight"],
            **_review_mapping(review),
        }
        reviews.append(row)
        packet_by_id[row["audit_id"]] = packet
    reviews.sort(key=lambda row: row["audit_id"])
    validate_research_feature_columns(FIRST_REVIEW_FIELDS)
    if any(row["recommended_verification_status"] != "needs_review" for row in reviews):
        raise ValueError("first-review output attempted to verify an anchor")

    deterministic_double_review = set()
    for tier in ("tier_1", "tier_2"):
        members = [row for row in reviews if row["proposed_tier"] == tier]
        deterministic_double_review.update(
            row["audit_id"]
            for row in sorted(members, key=lambda row: _rank(row, HUMAN_SUBSET_SEED))[
                :50
            ]
        )
    tier3_diagnostic = {
        row["audit_id"]
        for row in sorted(
            [row for row in reviews if row["proposed_tier"] == "tier_3"],
            key=lambda row: _rank(row, TIER3_DIAGNOSTIC_SEED),
        )[:10]
    }

    human_reasons = defaultdict(set)
    for row in reviews:
        audit_id = row["audit_id"]
        if audit_id in deterministic_double_review:
            human_reasons[audit_id].add("deterministic_50_per_rule_tier")
        if audit_id in tier3_diagnostic:
            human_reasons[audit_id].add("tier_3_quarantine_diagnostic")
        if row["proposed_tier"] in {"tier_1", "tier_2"}:
            if row["confidence"] == "low":
                human_reasons[audit_id].add("low_confidence")
            if row["reviewer_decision"] == "recommend_reject":
                human_reasons[audit_id].add("ai_recommended_rejection")
            if row["ambiguity_flags_json"] != "[]":
                human_reasons[audit_id].add("ambiguity_flag")
            if row["human_review_required"] == "true":
                human_reasons[audit_id].add("case_requires_human_review")

    human_subset = []
    for row in reviews:
        if row["audit_id"] not in human_reasons:
            continue
        packet = packet_by_id[row["audit_id"]]
        human_subset.append(
            {
                **packet,
                **row,
                "human_subset_reason": "|".join(sorted(human_reasons[row["audit_id"]])),
            }
        )
    human_subset.sort(key=lambda row: row["audit_id"])
    human_fields = tuple(
        dict.fromkeys(
            (
                *packet_rows[0].keys(),
                *FIRST_REVIEW_FIELDS,
                "human_subset_reason",
            )
        )
    )
    validate_research_feature_columns(human_fields)

    queue = []
    for row in reviews:
        reasons = human_reasons.get(row["audit_id"], set())
        if not reasons:
            continue
        queue.append({**row, "queue_reason": "|".join(sorted(reasons))})
    queue.sort(key=lambda row: row["audit_id"])
    validate_research_feature_columns(QUEUE_FIELDS)

    tier_rows = {
        tier: [row for row in reviews if row["proposed_tier"] == tier]
        for tier in ("tier_1", "tier_2", "tier_3")
    }
    report = {
        "schema_version": "1.0",
        "review_protocol_version": REVIEW_PROTOCOL_VERSION,
        "input_packet_sha256": packet_hash,
        "reviewed_family_count": len(reviews),
        "anchors_verified": 0,
        "verification_status_counts": {"needs_review": len(reviews)},
        "outcomes_accessed": False,
        "prices_accessed": False,
        "horizon_prices_built": False,
        "network_requests": 0,
        "ai_first_review_statistics": {
            tier: _group_rates(rows) for tier, rows in tier_rows.items()
        },
        "ai_first_review_breakdowns": {
            tier: {
                "category": _breakdowns(rows, "category"),
                "month_or_status": _breakdowns(rows, "analysis_month_or_status"),
                "candidate_source": _breakdowns(rows, "candidate_source_type"),
            }
            for tier, rows in tier_rows.items()
        },
        "human_review_statistics": {
            "status": "not_yet_available_pending_human_review",
            "subset_family_count": len(human_subset),
            "deterministic_tier_1_count": sum(
                row["audit_id"] in deterministic_double_review
                and row["proposed_tier"] == "tier_1"
                for row in reviews
            ),
            "deterministic_tier_2_count": sum(
                row["audit_id"] in deterministic_double_review
                and row["proposed_tier"] == "tier_2"
                for row in reviews
            ),
            "tier_3_diagnostic_count": len(tier3_diagnostic),
        },
        "ai_human_disagreement_statistics": {
            "status": "not_yet_available_pending_human_review"
        },
        "human_review_time_burden": {
            "subset_family_count": len(human_subset),
            "planning_minutes_per_family": 4,
            "estimated_hours": round(len(human_subset) * 4 / 60, 2),
        },
        "rule_status": {
            "PR1_FIXED_CLOCK_SINGLE_EXACT": "not_approved",
            "PR2_SCHEDULED_START_SINGLE_MILESTONE": "not_approved",
        },
    }

    output_root = output_root.resolve()
    guard_root = guard_root.resolve()
    output_root.parent.mkdir(parents=True, exist_ok=True)
    temp_root = output_root.parent / f".{output_root.name}.work-{uuid.uuid4().hex}"
    temp_root.mkdir()
    try:
        _write_csv(temp_root / REQUIRED_OUTPUTS[0], reviews, FIRST_REVIEW_FIELDS)
        _write_csv(temp_root / REQUIRED_OUTPUTS[2], human_subset, human_fields)
        _write_csv(temp_root / REQUIRED_OUTPUTS[3], queue, QUEUE_FIELDS)
        report["output_hashes"] = {
            name: _sha256(temp_root / name)
            for name in (REQUIRED_OUTPUTS[0], REQUIRED_OUTPUTS[2], REQUIRED_OUTPUTS[3])
        }
        report["output_bytes"] = {
            name: (temp_root / name).stat().st_size
            for name in (REQUIRED_OUTPUTS[0], REQUIRED_OUTPUTS[2], REQUIRED_OUTPUTS[3])
        }
        (temp_root / REQUIRED_OUTPUTS[1]).write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        publication_hashes = {
            name: _sha256(temp_root / name) for name in REQUIRED_OUTPUTS
        }
        if _logical_bytes(guard_root) > max_generated_bytes:
            raise ValueError("generated namespace would exceed the configured ceiling")
        if shutil.disk_usage(guard_root).free < min_free_bytes:
            raise ValueError("free disk would fall below the configured floor")
        if output_root.exists():
            if not _compare_existing(output_root, temp_root):
                raise ValueError(
                    "existing first-review output conflicts with deterministic rerun"
                )
            shutil.rmtree(temp_root)
        else:
            os.replace(temp_root, output_root)
        for name in REQUIRED_OUTPUTS:
            if _sha256(output_root / name) != publication_hashes[name]:
                raise ValueError("post-publication first-review hash validation failed")
    except Exception:
        if temp_root.exists():
            shutil.rmtree(temp_root)
        raise
    print(json.dumps(report, sort_keys=True))
    return report


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--packet", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--guard-root", required=True, type=Path)
    parser.add_argument("--expected-packet-sha256")
    parser.add_argument("--max-generated-bytes", type=int, default=5 * 1024**3)
    parser.add_argument("--min-free-bytes", type=int, default=80 * 1024**3)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        run(
            args.packet,
            args.output_root,
            guard_root=args.guard_root,
            expected_packet_sha256=args.expected_packet_sha256,
            max_generated_bytes=args.max_generated_bytes,
            min_free_bytes=args.min_free_bytes,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Phase 10E first review failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
