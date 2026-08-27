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
import math
import os
from pathlib import Path
import re
import shutil
import uuid
from typing import Any, Iterable, Mapping

from scripts.pipeline_v2.phase_10e_ai_assisted_review import (
    AI_ASSISTED_DECISION_FIELDS,
    INDEPENDENT_MANIFEST_FIELDS,
    INDEPENDENT_PACKET_FIELDS,
)
from scripts.pipeline_v2.phase_10e_human_review import (
    AMBIGUITY_FLAGS,
    CONFIDENCE_LEVELS,
    _validate_candidate,
    logical_bytes,
    sha256_file,
)
from scripts.pipeline_v2.study_rules import validate_research_feature_columns
from scripts.pipeline_v2.timing import TIMING_STRUCTURES


INDEPENDENT_REVIEW_PROTOCOL_VERSION = "phase-10e-independent-human-validation-v1"
INDEPENDENT_HUMAN_DECISIONS = ("approve_candidate", "reject", "uncertain")
INDEPENDENT_TIMING_STRUCTURES = (*TIMING_STRUCTURES, "neither/uncertain")
INDEPENDENT_AMBIGUITY_FLAGS = (
    *AMBIGUITY_FLAGS,
    "timestamp_mismatch",
    "unrelated_milestone",
    "settlement_timing",
)
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
DISAGREEMENT_QUEUE_FIELDS = (
    "validation_id",
    "audit_id",
    "proposed_rule",
    "proposed_tier",
    "category",
    "family_id",
    "family_id_source",
    "ai_assisted_decision",
    "independent_human_decision",
    "disagreement_direction",
    "confidence",
    "ai_assisted_ambiguity_flags_json",
    "independent_human_ambiguity_flags_json",
    "concise_rationale",
)
_RAW_DECISION_MAP = {
    "A": "approve_candidate",
    "R": "reject",
    "U": "uncertain",
}


def _read_csv(path: Path) -> tuple[list[dict[str, str]], tuple[str, ...]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        return list(reader), tuple(reader.fieldnames or ())


def parse_independent_ambiguity_flags(value: Any) -> tuple[str, ...]:
    parsed = json.loads(str(value or "[]"))
    if not isinstance(parsed, list) or any(
        not isinstance(flag, str) for flag in parsed
    ):
        raise ValueError("independent ambiguity flags must be a string list")
    if len(parsed) != len(set(parsed)):
        raise ValueError("independent ambiguity flags must be unique")
    unknown = set(parsed) - set(INDEPENDENT_AMBIGUITY_FLAGS)
    if unknown:
        raise ValueError(
            f"unsupported independent-review ambiguity flags: {sorted(unknown)}"
        )
    return tuple(sorted(parsed))


def _extract_review_field(block: str, label: str) -> str:
    match = re.search(
        rf"(?:^|\|)\s*{re.escape(label)}:\s*([^|\n]+)",
        block,
        flags=re.MULTILINE,
    )
    if match is None:
        raise ValueError(f"review block lacks {label!r}")
    return match.group(1).strip()


def parse_independent_review_texts(
    source_paths: list[Path],
    *,
    expected_source_sha256: list[str],
    packets: list[dict[str, str]],
    packet_sha256: str,
    manifest_sha256: str,
) -> tuple[list[dict[str, str]], dict[str, Any]]:
    if len(source_paths) != len(expected_source_sha256) or not source_paths:
        raise ValueError("review source paths and SHA-256 pins must align")
    blocks: list[tuple[str, str, str]] = []
    source_reports = []
    heading = re.compile(r"^P10E-HV-(\d{3})\s*$", flags=re.MULTILINE)
    for path, expected_hash in zip(source_paths, expected_source_sha256):
        actual_hash = sha256_file(path)
        if actual_hash != expected_hash:
            raise ValueError(f"review source SHA-256 mismatch for {path}")
        text = path.read_text(encoding="utf-8")
        matches = list(heading.finditer(text))
        if not matches:
            raise ValueError(
                f"review source contains no exact validation headings: {path}"
            )
        for index, match in enumerate(matches):
            end = matches[index + 1].start() if index + 1 < len(matches) else len(text)
            blocks.append(
                (
                    f"P10E-HV-{match.group(1)}",
                    text[match.end() : end].strip(),
                    actual_hash,
                )
            )
        source_reports.append(
            {
                "path": str(path.resolve()),
                "sha256": actual_hash,
                "bytes": path.stat().st_size,
                "exact_validation_heading_count": len(matches),
            }
        )
    observed_ids = [validation_id for validation_id, _, _ in blocks]
    duplicates = sorted(
        validation_id
        for validation_id, count in Counter(observed_ids).items()
        if count > 1
    )
    packet_by_id = {row["validation_id"]: row for row in packets}
    missing = sorted(set(packet_by_id) - set(observed_ids))
    extra = sorted(set(observed_ids) - set(packet_by_id))
    if duplicates or missing or extra:
        raise ValueError(
            "independent-review ID mismatch; "
            f"missing={missing}; extra={extra}; duplicates={duplicates}"
        )
    if len(blocks) != 100:
        raise ValueError("independent review must contain exactly 100 cases")
    decisions = []
    source_by_validation_id = {}
    for validation_id, block, source_hash in blocks:
        packet = packet_by_id[validation_id]
        decision_match = re.search(
            r"(?:^|\|)\s*A / R / U:\s*([ARU])(?:\s|\||$)",
            block,
            flags=re.MULTILINE,
        )
        if decision_match is None:
            raise ValueError(f"{validation_id} lacks a controlled A/R/U decision")
        raw_decision = decision_match.group(1)
        timing_structure = _extract_review_field(block, "Timing structure")
        relevance = _extract_review_field(block, "Candidate is relevant").casefold()
        confidence = _extract_review_field(block, "Confidence").casefold()
        ambiguity_flag = _extract_review_field(block, "Ambiguity flag")
        rationale_match = re.search(
            r"(?:^|\n)\s*Rationale:\s*(.+)\s*$",
            block,
            flags=re.DOTALL,
        )
        if rationale_match is None:
            raise ValueError(f"{validation_id} lacks a rationale")
        rationale = rationale_match.group(1).strip()
        flags = () if ambiguity_flag == "none" else (ambiguity_flag,)
        row = build_independent_decision(
            packet,
            packet_sha256=packet_sha256,
            manifest_sha256=manifest_sha256,
            decision=_RAW_DECISION_MAP[raw_decision],
            timing_structure=timing_structure,
            candidate_relevance=relevance,
            confidence=confidence,
            ambiguity_flags=flags,
            rationale=rationale,
        )
        decisions.append(
            validate_independent_decision(
                row,
                packet_by_validation_id=packet_by_id,
                packet_sha256=packet_sha256,
                manifest_sha256=manifest_sha256,
            )
        )
        source_by_validation_id[validation_id] = {
            "source_sha256": source_hash,
            "raw_decision": raw_decision,
            "raw_timing_structure": timing_structure,
            "raw_candidate_relevance": relevance,
            "raw_confidence": confidence,
            "raw_ambiguity_flag": ambiguity_flag,
        }
    decisions.sort(key=lambda row: row["validation_id"])
    decision_counts = Counter(row["independent_human_decision"] for row in decisions)
    if decision_counts != {
        "approve_candidate": 95,
        "reject": 5,
    }:
        raise ValueError(
            "independent-review counts do not match A=95, R=5, U=0; "
            f"found={dict(sorted(decision_counts.items()))}"
        )
    if any(row["confidence"] != "high" for row in decisions):
        raise ValueError("all 100 independent-review confidences must be high")
    if Counter(row["proposed_rule"] for row in decisions) != {
        "PR1_FIXED_CLOCK_SINGLE_EXACT": 50,
        "PR2_SCHEDULED_START_SINGLE_MILESTONE": 50,
    }:
        raise ValueError(
            "independent review must contain exactly 50 PR1 and 50 PR2 cases"
        )
    return decisions, {
        "source_artifacts": source_reports,
        "source_by_validation_id": source_by_validation_id,
        "row_count": len(decisions),
        "unique_validation_id_count": len({row["validation_id"] for row in decisions}),
        "decision_counts": {
            "A_approve": decision_counts["approve_candidate"],
            "R_reject": decision_counts["reject"],
            "U_uncertain": decision_counts["uncertain"],
        },
        "confidence_counts": dict(
            sorted(Counter(row["confidence"] for row in decisions).items())
        ),
    }


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
    if row["recommended_timing_structure"] not in INDEPENDENT_TIMING_STRUCTURES:
        raise ValueError("unsupported timing structure")
    if row["candidate_is_relevant_ex_ante_anchor"] not in {
        "yes",
        "no",
        "uncertain",
    }:
        raise ValueError("unsupported candidate relevance")
    if row["confidence"] not in CONFIDENCE_LEVELS:
        raise ValueError("unsupported confidence")
    flags = parse_independent_ambiguity_flags(row["ambiguity_flags_json"])
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


def _wilson_interval(
    successes: float, trials: float, *, z: float = 1.959964
) -> list[float]:
    if trials <= 0:
        return [0.0, 1.0]
    proportion = min(1.0, max(0.0, successes / trials))
    denominator = 1.0 + z**2 / trials
    center = (proportion + z**2 / (2.0 * trials)) / denominator
    half_width = (
        z
        * math.sqrt(proportion * (1.0 - proportion) / trials + z**2 / (4.0 * trials**2))
        / denominator
    )
    return [
        round(max(0.0, center - half_width), 6),
        round(min(1.0, center + half_width), 6),
    ]


def _confidence_intervals(rows: list[dict[str, Any]]) -> dict[str, Any]:
    weights = [float(row["independent_validation_analysis_weight"]) for row in rows]
    weight_total = sum(weights)
    effective_sample_size = (
        weight_total**2 / sum(weight**2 for weight in weights) if weights else 0.0
    )
    output = {}
    for decision in INDEPENDENT_HUMAN_DECISIONS:
        raw_successes = sum(
            row["independent_human_decision"] == decision for row in rows
        )
        weighted_rate = (
            sum(
                weight
                for row, weight in zip(rows, weights)
                if row["independent_human_decision"] == decision
            )
            / weight_total
            if weight_total
            else 0.0
        )
        output[decision] = {
            "unweighted_wilson_95": _wilson_interval(raw_successes, len(rows)),
            "weighted_kish_wilson_95": _wilson_interval(
                weighted_rate * effective_sample_size, effective_sample_size
            ),
        }
    return {
        "method": (
            "Two-sided 95% Wilson intervals. Weighted intervals use the Kish "
            "effective sample size from final inverse-probability weights and "
            "are conditional on the upstream Phase 10E audit sample."
        ),
        "effective_sample_size": round(effective_sample_size, 6),
        "intervals": output,
    }


def _decision_label(value: str) -> str:
    return "approve" if value == "approve_candidate" else value


def _disagreement_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    disagreement_rows = [row for row in rows if not row["ai_assisted_human_agree"]]
    total_weight = sum(
        float(row["independent_validation_analysis_weight"]) for row in rows
    )
    disagreement_weight = sum(
        float(row["independent_validation_analysis_weight"])
        for row in disagreement_rows
    )
    directions = defaultdict(list)
    for row in disagreement_rows:
        direction = (
            f"AI_{_decision_label(row['ai_assisted_decision'])}"
            f"__human_{_decision_label(row['independent_human_decision'])}"
        )
        directions[direction].append(row)
    return {
        "disagreement_count": len(disagreement_rows),
        "unweighted_disagreement_rate": (
            round(len(disagreement_rows) / len(rows), 6) if rows else 0
        ),
        "weighted_disagreement_rate": (
            round(disagreement_weight / total_weight, 6) if total_weight else 0
        ),
        "direction": {
            direction: {
                "count": len(values),
                "unweighted_case_rate": (
                    round(len(values) / len(rows), 6) if rows else 0
                ),
                "weighted_case_rate": (
                    round(
                        sum(
                            float(row["independent_validation_analysis_weight"])
                            for row in values
                        )
                        / total_weight,
                        6,
                    )
                    if total_weight
                    else 0
                ),
            }
            for direction, values in sorted(directions.items())
        },
    }


def _disagreement_breakdown(
    groups: Mapping[str, list[dict[str, Any]]]
) -> dict[str, Any]:
    return {
        key: {"case_count": len(values), **_disagreement_summary(values)}
        for key, values in sorted(groups.items())
    }


def build_disagreement_queue(
    packets: list[dict[str, str]],
    decisions: list[dict[str, str]],
    ai_assisted_by_audit_id: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, str]]:
    packet_by_id = {row["validation_id"]: row for row in packets}
    queue = []
    for decision in decisions:
        packet = packet_by_id[decision["validation_id"]]
        ai_row = ai_assisted_by_audit_id[decision["audit_id"]]
        if ai_row["ai_assisted_decision"] == decision["independent_human_decision"]:
            continue
        direction = (
            f"AI_{_decision_label(ai_row['ai_assisted_decision'])}"
            f"__human_{_decision_label(decision['independent_human_decision'])}"
        )
        queue.append(
            {
                "validation_id": decision["validation_id"],
                "audit_id": decision["audit_id"],
                "proposed_rule": decision["proposed_rule"],
                "proposed_tier": decision["proposed_tier"],
                "category": packet["category"],
                "family_id": decision["family_id"],
                "family_id_source": decision["family_id_source"],
                "ai_assisted_decision": ai_row["ai_assisted_decision"],
                "independent_human_decision": decision["independent_human_decision"],
                "disagreement_direction": direction,
                "confidence": decision["confidence"],
                "ai_assisted_ambiguity_flags_json": ai_row["ambiguity_flags_json"],
                "independent_human_ambiguity_flags_json": decision[
                    "ambiguity_flags_json"
                ],
                "concise_rationale": decision["concise_rationale"],
            }
        )
    return sorted(queue, key=lambda row: row["validation_id"])


def build_rule_recommendations(report: Mapping[str, Any]) -> dict[str, Any]:
    specifications = {
        "PR1_FIXED_CLOCK_SINGLE_EXACT": {
            "label": "PR1 — Fixed-clock, single exact candidate",
            "recommended_decision": "MODIFY",
            "required_exclusions": [
                "recurring short-duration contracts where the one-hour horizon precedes market existence",
                "deadline or window markets without one exact ex-ante decision time",
                "publication-time candidates",
                "settlement or reporting-time ambiguity",
                "multiple plausible scheduled times",
                "title-to-candidate timestamp mismatch",
                "semantically unrelated milestone or reporting time",
            ],
            "rationale": (
                "Independent review is strongly favorable but identifies nonzero "
                "outcome-blind false positives, while previously documented horizon-"
                "existence and timing-semantic exclusions remain necessary."
            ),
        },
        "PR2_SCHEDULED_START_SINGLE_MILESTONE": {
            "label": "PR2 — Scheduled-event-start, single official milestone",
            "recommended_decision": "MODIFY",
            "required_exclusions": [
                "set-level markets",
                "map or series-level markets",
                "endogenous or partial subevents",
                "conditional subevents",
                "ticker-to-candidate date mismatch",
                "semantically unrelated milestones",
                "multiple distinct plausible start times",
                "post-event settlement or reporting timestamps",
            ],
            "rationale": (
                "Independent review is strongly favorable but still finds "
                "post-event or settlement-timing false positives; known subevent "
                "and semantic failure modes require explicit exclusions."
            ),
        },
    }
    output = {}
    for rule, specification in specifications.items():
        values = report["rule_specific"][rule]
        disagreement = values["ai_assisted_vs_independent_human"]
        output[rule] = {
            **specification,
            "independent_human_weighted_approval_rate": values[
                "weighted_human_approval_rate"
            ],
            "independent_human_false_positive_rate": values[
                "weighted_confirmed_false_positive_rate"
            ],
            "ai_assisted_human_disagreement_count": disagreement["disagreement_count"],
            "ai_assisted_human_weighted_disagreement_rate": disagreement[
                "weighted_disagreement_rate"
            ],
            "approval_status": "not_approved_pending_explicit_project_owner_decision",
        }
    return {
        "schema_version": "1.0",
        "recommendation_stage_only": True,
        "rules": output,
        "anchors_verified": 0,
        "rules_applied": 0,
        "horizon_prices_built": False,
        "outcomes_accessed": False,
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
        categories = defaultdict(list)
        ai_failure_modes = defaultdict(list)
        human_failure_mode_groups = defaultdict(list)
        human_failure_modes = Counter()
        for row in rows:
            categories[row["category"]].append(row)
            ai_flags = json.loads(row["ai_assisted_ambiguity_flags_json"])
            for flag in ai_flags or ["no_ai_assisted_ambiguity_flag"]:
                ai_failure_modes[flag].append(row)
            human_flags = json.loads(row["ambiguity_flags_json"])
            human_failure_modes.update(human_flags or ["no_human_ambiguity_flag"])
            for flag in human_flags or ["no_human_ambiguity_flag"]:
                human_failure_mode_groups[flag].append(row)
        rates = _rate_summary(rows)
        disagreement = _disagreement_summary(rows)
        rule_reports[rule] = {
            **rates,
            "weighted_human_approval_rate": rates["weighted_rates"][
                "approve_candidate"
            ],
            "weighted_confirmed_false_positive_rate": rates["weighted_rates"]["reject"],
            "weighted_human_uncertainty_rate": rates["weighted_rates"]["uncertain"],
            "confidence_intervals": _confidence_intervals(rows),
            "ai_assisted_vs_independent_human": disagreement,
            "unweighted_ai_assisted_human_disagreement_rate": disagreement[
                "unweighted_disagreement_rate"
            ],
            "weighted_ai_assisted_human_disagreement_rate": disagreement[
                "weighted_disagreement_rate"
            ],
            "category_specific": {
                category: {
                    **_rate_summary(values),
                    "disagreement": _disagreement_summary(values),
                }
                for category, values in sorted(categories.items())
            },
            "human_failure_mode_counts": dict(sorted(human_failure_modes.items())),
            "ai_assisted_failure_mode_specific_disagreement": _disagreement_breakdown(
                ai_failure_modes
            ),
            "independent_human_failure_mode_specific_disagreement": _disagreement_breakdown(
                human_failure_mode_groups
            ),
        }
    return {
        "schema_version": "1.0",
        "review_type": "fresh_independent_human_outcome_blind_validation",
        "review_protocol_version": INDEPENDENT_REVIEW_PROTOCOL_VERSION,
        "source_packet_sha256": packet_sha256,
        "source_manifest_sha256": manifest_sha256,
        "reviewed_case_count": len(decisions),
        "raw_decision_counts": dict(
            sorted(
                Counter(row["independent_human_decision"] for row in decisions).items()
            )
        ),
        "confidence_counts": dict(
            sorted(Counter(row["confidence"] for row in decisions).items())
        ),
        "reviewer_style_limitation": {
            "zero_uncertain_decisions": all(
                row["independent_human_decision"] != "uncertain" for row in decisions
            ),
            "all_high_confidence": all(
                row["confidence"] == "high" for row in decisions
            ),
            "interpretation": (
                "The absence of uncertainty and uniform high confidence are "
                "consistent with a reviewer response-style effect. This does not "
                "invalidate the review, but it may compress expressed uncertainty "
                "and should be reported as a limitation."
            ),
        },
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
