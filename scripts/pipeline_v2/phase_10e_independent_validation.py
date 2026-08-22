"""Outcome-blind state and diagnostics for the fresh Phase 10E human validation.

The 100-case packet intentionally contains no AI recommendation or prior
AI-assisted decision. Decisions remain recommendations only and cannot verify
anchors or apply either proposed rule.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import csv
import io
import json
import os
from pathlib import Path
import shutil
import uuid
from typing import Any, Iterable, Mapping

from scripts.pipeline_v2.phase_10e_ai_assisted_review import (
    AI_ASSISTED_DECISION_FIELDS,
    INDEPENDENT_MANIFEST_FIELDS,
    INDEPENDENT_PACKET_FIELDS,
)
from scripts.pipeline_v2.phase_10e_human_review import (
    CONFIDENCE_LEVELS,
    _validate_candidate,
    logical_bytes,
    parse_ambiguity_flags,
    sha256_file,
)
from scripts.pipeline_v2.study_rules import validate_research_feature_columns
from scripts.pipeline_v2.timing import TIMING_STRUCTURES


INDEPENDENT_REVIEW_PROTOCOL_VERSION = "phase-10e-independent-human-validation-v1"
INDEPENDENT_HUMAN_DECISIONS = ("approve_candidate", "reject", "uncertain")
INDEPENDENT_DECISION_FIELDS = (
    "validation_id",
    "audit_id",
    "family_id",
    "family_id_source",
    "proposed_tier",
    "proposed_rule",
    "source_packet_sha256",
    "source_manifest_sha256",
    "review_protocol_version",
    "independent_human_decision",
    "verification_status",
    "verified_anchor_time",
    "verified_anchor_source",
    "recommended_timing_structure",
    "candidate_is_relevant_ex_ante_anchor",
    "confidence",
    "ambiguity_flags_json",
    "concise_rationale",
)


def _read_csv(path: Path) -> tuple[list[dict[str, str]], tuple[str, ...]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader), tuple(reader.fieldnames or ())


def load_validation_sources(
    packet_path: Path,
    manifest_path: Path,
    *,
    expected_packet_sha256: str,
    expected_manifest_sha256: str,
) -> tuple[list[dict[str, str]], dict[str, dict[str, str]]]:
    if sha256_file(packet_path) != expected_packet_sha256:
        raise ValueError("independent-human packet SHA-256 does not match its pin")
    if sha256_file(manifest_path) != expected_manifest_sha256:
        raise ValueError("independent-human manifest SHA-256 does not match its pin")
    packets, packet_fields = _read_csv(packet_path)
    manifests, manifest_fields = _read_csv(manifest_path)
    if packet_fields != INDEPENDENT_PACKET_FIELDS:
        raise ValueError("independent-human packet has the wrong exact schema")
    if manifest_fields != INDEPENDENT_MANIFEST_FIELDS:
        raise ValueError("independent-human manifest has the wrong exact schema")
    validate_research_feature_columns(packet_fields)
    if len(packets) != 100 or len(manifests) != 100:
        raise ValueError("independent-human sources must contain exactly 100 cases")
    for label, rows in (("packet", packets), ("manifest", manifests)):
        if len({row["validation_id"] for row in rows}) != 100:
            raise ValueError(f"independent-human {label} has duplicate validation IDs")
        if len({row["audit_id"] for row in rows}) != 100:
            raise ValueError(f"independent-human {label} has duplicate audit IDs")
    manifest_by_id = {row["validation_id"]: row for row in manifests}
    if {row["validation_id"] for row in packets} != set(manifest_by_id):
        raise ValueError("packet and manifest validation IDs differ")
    if Counter(row["proposed_rule"] for row in packets) != {
        "PR1_FIXED_CLOCK_SINGLE_EXACT": 50,
        "PR2_SCHEDULED_START_SINGLE_MILESTONE": 50,
    }:
        raise ValueError(
            "independent-human packet must contain 50 PR1 and 50 PR2 cases"
        )
    for packet in packets:
        manifest = manifest_by_id[packet["validation_id"]]
        for field in (
            "audit_id",
            "proposed_tier",
            "proposed_rule",
            "category",
        ):
            if packet[field] != manifest[field]:
                raise ValueError(
                    f"packet/manifest conflict for {packet['validation_id']} on {field}"
                )
        for field in (
            "independent_human_decision",
            "recommended_timing_structure",
            "candidate_is_relevant_ex_ante_anchor",
            "confidence",
            "concise_rationale",
        ):
            if packet[field]:
                raise ValueError("fresh human packet contains a prior decision")
        if packet["ambiguity_flags_json"] != "[]":
            raise ValueError("fresh human packet contains prior ambiguity flags")
        candidates = json.loads(packet["candidates_json"] or "[]")
        if not isinstance(candidates, list):
            raise ValueError("fresh packet candidates_json must be a list")
        for candidate in candidates:
            _validate_candidate(candidate)
    return sorted(packets, key=lambda row: row["validation_id"]), manifest_by_id


def build_independent_decision(
    packet: Mapping[str, Any],
    *,
    packet_sha256: str,
    manifest_sha256: str,
    decision: str,
    timing_structure: str,
    candidate_relevance: str,
    confidence: str,
    ambiguity_flags: Iterable[str],
    rationale: str,
) -> dict[str, str]:
    return {
        "validation_id": str(packet["validation_id"]),
        "audit_id": str(packet["audit_id"]),
        "family_id": str(packet["family_id"]),
        "family_id_source": str(packet["family_id_source"]),
        "proposed_tier": str(packet["proposed_tier"]),
        "proposed_rule": str(packet["proposed_rule"]),
        "source_packet_sha256": packet_sha256,
        "source_manifest_sha256": manifest_sha256,
        "review_protocol_version": INDEPENDENT_REVIEW_PROTOCOL_VERSION,
        "independent_human_decision": decision,
        "verification_status": "needs_review",
        "verified_anchor_time": "",
        "verified_anchor_source": "",
        "recommended_timing_structure": timing_structure,
        "candidate_is_relevant_ex_ante_anchor": candidate_relevance,
        "confidence": confidence,
        "ambiguity_flags_json": json.dumps(
            sorted(set(ambiguity_flags)), separators=(",", ":")
        ),
        "concise_rationale": rationale.strip(),
    }


def validate_independent_decision(
    source: Mapping[str, Any],
    *,
    packet_by_validation_id: Mapping[str, Mapping[str, Any]],
    packet_sha256: str,
    manifest_sha256: str,
) -> dict[str, str]:
    if set(source) != set(INDEPENDENT_DECISION_FIELDS):
        missing = sorted(set(INDEPENDENT_DECISION_FIELDS) - set(source))
        extra = sorted(set(source) - set(INDEPENDENT_DECISION_FIELDS))
        raise ValueError(
            f"independent decision schema mismatch; missing={missing}; extra={extra}"
        )
    row = {
        field: str(source.get(field) or "").strip()
        for field in INDEPENDENT_DECISION_FIELDS
    }
    packet = packet_by_validation_id.get(row["validation_id"])
    if packet is None:
        raise ValueError("independent decision references an unknown validation ID")
    for field in (
        "audit_id",
        "family_id",
        "family_id_source",
        "proposed_tier",
        "proposed_rule",
    ):
        if row[field] != str(packet[field]):
            raise ValueError(f"independent decision conflicts with packet on {field}")
    if row["source_packet_sha256"] != packet_sha256:
        raise ValueError("independent decision has the wrong packet hash")
    if row["source_manifest_sha256"] != manifest_sha256:
        raise ValueError("independent decision has the wrong manifest hash")
    if row["review_protocol_version"] != INDEPENDENT_REVIEW_PROTOCOL_VERSION:
        raise ValueError("independent decision has the wrong protocol version")
    if row["independent_human_decision"] not in INDEPENDENT_HUMAN_DECISIONS:
        raise ValueError("unsupported independent-human decision")
    if row["verification_status"] != "needs_review":
        raise ValueError("independent audit cannot change verification status")
    if row["verified_anchor_time"] or row["verified_anchor_source"]:
        raise ValueError("independent audit cannot populate verified-anchor fields")
    if row["recommended_timing_structure"] not in TIMING_STRUCTURES:
        raise ValueError("unsupported timing structure")
    if row["candidate_is_relevant_ex_ante_anchor"] not in {
        "yes",
        "no",
        "uncertain",
    }:
        raise ValueError("unsupported candidate relevance")
    if row["confidence"] not in CONFIDENCE_LEVELS:
        raise ValueError("unsupported confidence")
    flags = parse_ambiguity_flags(row["ambiguity_flags_json"])
    row["ambiguity_flags_json"] = json.dumps(list(flags), separators=(",", ":"))
    if len(row["concise_rationale"]) > 500:
        raise ValueError("rationale exceeds 500 characters")
    if (
        row["independent_human_decision"] in {"reject", "uncertain"}
        and len(row["concise_rationale"]) < 8
    ):
        raise ValueError("rejection or uncertainty requires a short rationale")
    return row


def load_independent_decisions(
    path: Path,
    *,
    packets: list[dict[str, str]],
    packet_sha256: str,
    manifest_sha256: str,
) -> tuple[list[dict[str, str]], str | None]:
    if not path.exists():
        return [], None
    rows, fields = _read_csv(path)
    if fields != INDEPENDENT_DECISION_FIELDS:
        raise ValueError("saved independent decisions have the wrong exact schema")
    packet_by_id = {row["validation_id"]: row for row in packets}
    validated = [
        validate_independent_decision(
            row,
            packet_by_validation_id=packet_by_id,
            packet_sha256=packet_sha256,
            manifest_sha256=manifest_sha256,
        )
        for row in rows
    ]
    if len({row["validation_id"] for row in validated}) != len(validated):
        raise ValueError("saved independent decisions contain duplicate IDs")
    return sorted(validated, key=lambda row: row["validation_id"]), sha256_file(path)


def _csv_bytes(rows: Iterable[Mapping[str, Any]]) -> bytes:
    handle = io.StringIO(newline="")
    writer = csv.DictWriter(
        handle, fieldnames=INDEPENDENT_DECISION_FIELDS, lineterminator="\n"
    )
    writer.writeheader()
    for row in sorted(rows, key=lambda item: str(item["validation_id"])):
        writer.writerow(
            {field: row.get(field, "") for field in INDEPENDENT_DECISION_FIELDS}
        )
    return handle.getvalue().encode("utf-8")


def atomic_save_independent_decisions(
    path: Path,
    rows: Iterable[Mapping[str, Any]],
    *,
    packets: list[dict[str, str]],
    packet_sha256: str,
    manifest_sha256: str,
    guard_root: Path,
    expected_existing_sha256: str | None,
    max_generated_bytes: int = 5 * 1024**3,
    min_free_bytes: int = 80 * 1024**3,
) -> str:
    packet_by_id = {row["validation_id"]: row for row in packets}
    validated = [
        validate_independent_decision(
            row,
            packet_by_validation_id=packet_by_id,
            packet_sha256=packet_sha256,
            manifest_sha256=manifest_sha256,
        )
        for row in rows
    ]
    if len({row["validation_id"] for row in validated}) != len(validated):
        raise ValueError("cannot save duplicate independent decisions")
    if path.exists():
        current_hash = sha256_file(path)
        if expected_existing_sha256 is None or current_hash != expected_existing_sha256:
            raise ValueError("independent decisions changed since they were loaded")
    elif expected_existing_sha256 is not None:
        raise ValueError("expected independent decisions file is missing")
    guard_root = guard_root.resolve()
    guard_root.mkdir(parents=True, exist_ok=True)
    try:
        path.resolve().relative_to(guard_root)
    except ValueError as exc:
        raise ValueError("decisions path must remain inside the guard root") from exc
    payload = _csv_bytes(validated)
    if logical_bytes(guard_root) + len(payload) > max_generated_bytes:
        raise ValueError("independent decision save would exceed the namespace ceiling")
    if shutil.disk_usage(guard_root).free - len(payload) < min_free_bytes:
        raise ValueError("independent decision save would cross the free-disk floor")
    path.parent.mkdir(parents=True, exist_ok=True)
    temp = path.parent / f".{path.name}.work-{uuid.uuid4().hex}"
    try:
        with temp.open("xb") as handle:
            handle.write(payload)
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp, path)
    finally:
        if temp.exists():
            temp.unlink()
    return sha256_file(path)


def load_ai_assisted_decisions(path: Path) -> dict[str, dict[str, str]]:
    rows, fields = _read_csv(path)
    if fields != AI_ASSISTED_DECISION_FIELDS:
        raise ValueError("AI-assisted decisions have the wrong exact schema")
    if len(rows) != 165 or len({row["audit_id"] for row in rows}) != 165:
        raise ValueError("AI-assisted decisions must contain 165 unique audit IDs")
    if any(row["verification_status"] != "needs_review" for row in rows):
        raise ValueError("AI-assisted source attempted to verify an anchor")
    return {row["audit_id"]: row for row in rows}


def _rate_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    counts = Counter(row["independent_human_decision"] for row in rows)
    weighted = Counter()
    total_weight = 0.0
    for row in rows:
        weight = float(row["independent_validation_analysis_weight"])
        total_weight += weight
        weighted[row["independent_human_decision"]] += weight
    keys = INDEPENDENT_HUMAN_DECISIONS
    return {
        "case_count": len(rows),
        "unweighted_counts": {key: counts[key] for key in keys},
        "unweighted_rates": {
            key: round(counts[key] / len(rows), 6) if rows else 0 for key in keys
        },
        "weighted_population": round(total_weight, 6),
        "weighted_rates": {
            key: round(weighted[key] / total_weight, 6) if total_weight else 0
            for key in keys
        },
    }


def build_independent_validation_report(
    packets: list[dict[str, str]],
    manifest_by_id: Mapping[str, Mapping[str, Any]],
    decisions: list[dict[str, str]],
    ai_assisted_by_audit_id: Mapping[str, Mapping[str, Any]],
    *,
    packet_sha256: str,
    manifest_sha256: str,
) -> dict[str, Any]:
    if len(decisions) != 100:
        raise ValueError("independent human validation is incomplete")
    decision_by_id = {row["validation_id"]: row for row in decisions}
    if set(decision_by_id) != {row["validation_id"] for row in packets}:
        raise ValueError("independent decisions do not cover the exact packet")
    combined = []
    for packet in packets:
        decision = decision_by_id[packet["validation_id"]]
        manifest = manifest_by_id[packet["validation_id"]]
        ai_row = ai_assisted_by_audit_id.get(packet["audit_id"])
        if ai_row is None:
            raise ValueError("independent case lacks its AI-assisted comparator")
        combined.append(
            {
                **packet,
                **decision,
                **manifest,
                "ai_assisted_decision": ai_row["ai_assisted_decision"],
                "ai_assisted_ambiguity_flags_json": ai_row["ambiguity_flags_json"],
                "ai_assisted_human_agree": ai_row["ai_assisted_decision"]
                == decision["independent_human_decision"],
            }
        )
    rules = defaultdict(list)
    for row in combined:
        rules[row["proposed_rule"]].append(row)
    rule_reports = {}
    for rule, rows in sorted(rules.items()):
        total_weight = sum(
            float(row["independent_validation_analysis_weight"]) for row in rows
        )
        disagreement_weight = sum(
            float(row["independent_validation_analysis_weight"])
            for row in rows
            if not row["ai_assisted_human_agree"]
        )
        categories = defaultdict(list)
        human_failure_modes = Counter()
        for row in rows:
            categories[row["category"]].append(row)
            flags = json.loads(row["ambiguity_flags_json"])
            human_failure_modes.update(flags or ["no_human_ambiguity_flag"])
        rates = _rate_summary(rows)
        rule_reports[rule] = {
            **rates,
            "weighted_human_approval_rate": rates["weighted_rates"][
                "approve_candidate"
            ],
            "weighted_confirmed_false_positive_rate": rates["weighted_rates"]["reject"],
            "weighted_human_uncertainty_rate": rates["weighted_rates"]["uncertain"],
            "unweighted_ai_assisted_human_disagreement_rate": round(
                sum(not row["ai_assisted_human_agree"] for row in rows) / len(rows),
                6,
            ),
            "weighted_ai_assisted_human_disagreement_rate": round(
                disagreement_weight / total_weight, 6
            ),
            "category_specific": {
                category: _rate_summary(values)
                for category, values in sorted(categories.items())
            },
            "human_failure_mode_counts": dict(sorted(human_failure_modes.items())),
        }
    return {
        "schema_version": "1.0",
        "review_type": "fresh_independent_human_outcome_blind_validation",
        "review_protocol_version": INDEPENDENT_REVIEW_PROTOCOL_VERSION,
        "source_packet_sha256": packet_sha256,
        "source_manifest_sha256": manifest_sha256,
        "reviewed_case_count": len(decisions),
        "rule_specific": rule_reports,
        "confirmed_false_positive_definition": "independent_human_decision=reject",
        "verification_status_counts": {"needs_review": len(decisions)},
        "anchors_verified": 0,
        "rules_approved": 0,
        "outcomes_accessed": False,
        "post_event_information_accessed": False,
        "horizon_prices_built": False,
        "network_requests": 0,
    }
