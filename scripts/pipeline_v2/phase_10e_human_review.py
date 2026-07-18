"""Outcome-blind human-review state, validation, and diagnostics for Phase 10E.

Human decisions in this module are recommendations only.  The exact frozen
verification-schema projection always remains ``needs_review`` with blank
verified-anchor fields, so these records cannot verify or apply an anchor.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import uuid
from typing import Any, Iterable, Mapping

from scripts.pipeline_v2.apply_anchor_verification import (
    DECISION_FIELDS,
    validate_decisions as validate_verification_decisions,
)
from scripts.pipeline_v2.phase_10e_verification_design import (
    ALLOWED_CANDIDATE_SOURCES,
    SAFE_CONTEXT_KEYS,
)
from scripts.pipeline_v2.study_rules import validate_research_feature_columns
from scripts.pipeline_v2.timing import TIMING_STRUCTURES


HUMAN_REVIEW_PROTOCOL_VERSION = "phase-10e-human-outcome-blind-review-v1"
EXPECTED_SUBSET_SHA256 = (
    "904c5c7b787a6cc573878f7ddcb0d5aa46bc0c29228b1e92cbe8a235563ec1cc"
)
EXPECTED_PACKET_SHA256 = (
    "89fc0b28be4365c78558d1aed1d77578d5d379a891c4664bb75e62bb411ed05b"
)
HUMAN_DECISIONS = ("approve_candidate", "reject", "uncertain")
CONFIDENCE_LEVELS = ("high", "medium", "low")
AMBIGUITY_FLAGS = (
    "recurring_intraday_one_hour_preexistence_risk",
    "deadline_or_window_not_fixed_clock",
    "publication_or_result_timing",
    "settlement_or_result_timing_language",
    "multiple_plausible_scheduled_times",
    "set_level_market",
    "map_or_series_level_market",
    "partial_or_endogenous_subevent",
    "conditional_endogenous_subevent",
    "ticker_date_candidate_date_mismatch",
    "semantic_mismatch_or_unrelated_milestone",
    "multiple_distinct_exact_candidate_times",
    "multiple_candidates_same_time",
    "date_only_evidence",
    "insufficient_evidence",
    "other_timing_or_semantic_ambiguity",
)
HUMAN_DECISION_FIELDS = (
    "audit_id",
    "family_id",
    "family_id_source",
    "proposed_tier",
    "proposed_rule",
    "source_subset_sha256",
    "review_protocol_version",
    "human_decision",
    "recommended_verification_status",
    "recommended_timing_structure",
    "candidate_is_relevant_ex_ante_anchor",
    "confidence",
    "ambiguity_flags_json",
    "concise_rationale",
)
_CANDIDATE_KEYS = frozenset(
    {
        "candidate_id",
        "candidate_source_type",
        "candidate_original_value",
        "candidate_time_utc",
        "candidate_date",
        "candidate_precision",
        "potential_verified_anchor_source",
        "candidate_title",
        "evidence_reference",
        "supporting_source_count",
        "analysis_window_status",
        "safe_evidence_context",
    }
)


def sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def logical_bytes(root: Path) -> int:
    return sum(path.stat().st_size for path in root.rglob("*") if path.is_file())


def _read_csv(path: Path) -> tuple[list[dict[str, str]], tuple[str, ...]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        if reader.fieldnames is None:
            raise ValueError(f"CSV has no header: {path}")
        return list(reader), tuple(reader.fieldnames)


def _validate_candidate(candidate: Any) -> None:
    if not isinstance(candidate, dict):
        raise ValueError("review candidate must be an object")
    validate_research_feature_columns(candidate)
    extra = set(candidate) - _CANDIDATE_KEYS
    if extra:
        raise ValueError(
            f"review candidate contains non-safelisted keys: {sorted(extra)}"
        )
    source = str(candidate.get("candidate_source_type") or "")
    if source not in ALLOWED_CANDIDATE_SOURCES:
        raise ValueError(f"review candidate has disallowed source: {source!r}")
    context = candidate.get("safe_evidence_context")
    if not isinstance(context, dict):
        raise ValueError("safe_evidence_context must be an object")
    validate_research_feature_columns(context)
    extra_context = set(context) - SAFE_CONTEXT_KEYS[source]
    if extra_context:
        raise ValueError(
            f"review context contains non-safelisted keys: {sorted(extra_context)}"
        )


def load_review_subset(
    subset_path: Path,
    *,
    expected_subset_sha256: str = EXPECTED_SUBSET_SHA256,
    packet_path: Path | None = None,
    expected_packet_sha256: str = EXPECTED_PACKET_SHA256,
) -> list[dict[str, str]]:
    if sha256_file(subset_path) != expected_subset_sha256:
        raise ValueError(
            "human-review subset SHA-256 does not match the approved packet"
        )
    if packet_path is not None and sha256_file(packet_path) != expected_packet_sha256:
        raise ValueError(
            "canonical audit packet SHA-256 does not match the approved design"
        )
    rows, fields = _read_csv(subset_path)
    validate_research_feature_columns(fields)
    if len(rows) != 165:
        raise ValueError("approved human-review subset must contain exactly 165 cases")
    audit_ids = [row.get("audit_id", "") for row in rows]
    identities = [
        (row.get("family_id", ""), row.get("family_id_source", "")) for row in rows
    ]
    if not all(audit_ids) or len(set(audit_ids)) != len(rows):
        raise ValueError("human-review subset contains blank or duplicate audit IDs")
    if not all(all(identity) for identity in identities) or len(set(identities)) != len(
        rows
    ):
        raise ValueError(
            "human-review subset contains blank or duplicate family identities"
        )
    for row in rows:
        if row.get("recommended_verification_status") != "needs_review":
            raise ValueError("source review packet attempted to verify an anchor")
        candidates = json.loads(row.get("candidates_json") or "[]")
        if not isinstance(candidates, list):
            raise ValueError("candidates_json must be a list")
        for candidate in candidates:
            _validate_candidate(candidate)
    return sorted(rows, key=lambda row: row["audit_id"])


def parse_ambiguity_flags(value: Any) -> tuple[str, ...]:
    parsed = json.loads(str(value or "[]"))
    if not isinstance(parsed, list) or any(
        not isinstance(flag, str) for flag in parsed
    ):
        raise ValueError("ambiguity_flags_json must be a string list")
    if len(parsed) != len(set(parsed)):
        raise ValueError("ambiguity flags must be unique")
    unknown = set(parsed) - set(AMBIGUITY_FLAGS)
    if unknown:
        raise ValueError(f"unsupported ambiguity flags: {sorted(unknown)}")
    return tuple(sorted(parsed))


def validate_human_decision(
    source: Mapping[str, Any],
    *,
    subset_by_id: Mapping[str, Mapping[str, Any]],
) -> dict[str, str]:
    row = {
        field: str(source.get(field) or "").strip() for field in HUMAN_DECISION_FIELDS
    }
    if set(source) != set(HUMAN_DECISION_FIELDS):
        missing = sorted(set(HUMAN_DECISION_FIELDS) - set(source))
        extra = sorted(set(source) - set(HUMAN_DECISION_FIELDS))
        raise ValueError(
            f"human decision schema mismatch; missing={missing}; extra={extra}"
        )
    packet = subset_by_id.get(row["audit_id"])
    if packet is None:
        raise ValueError(f"decision references unknown audit ID {row['audit_id']!r}")
    for field in ("family_id", "family_id_source", "proposed_tier", "proposed_rule"):
        if row[field] != str(packet.get(field) or ""):
            raise ValueError(f"decision {row['audit_id']!r} conflicts on {field}")
    if row["source_subset_sha256"] != EXPECTED_SUBSET_SHA256:
        raise ValueError("decision source-subset hash is not the approved hash")
    if row["review_protocol_version"] != HUMAN_REVIEW_PROTOCOL_VERSION:
        raise ValueError("unsupported human-review protocol version")
    if row["human_decision"] not in HUMAN_DECISIONS:
        raise ValueError("unsupported human review decision")
    if row["recommended_verification_status"] != "needs_review":
        raise ValueError("human audit decisions must remain needs_review")
    if row["recommended_timing_structure"] not in TIMING_STRUCTURES:
        raise ValueError("unsupported recommended timing structure")
    if row["candidate_is_relevant_ex_ante_anchor"] not in {"yes", "no", "uncertain"}:
        raise ValueError("unsupported candidate-relevance recommendation")
    if row["confidence"] not in CONFIDENCE_LEVELS:
        raise ValueError("unsupported human-review confidence")
    flags = parse_ambiguity_flags(row["ambiguity_flags_json"])
    row["ambiguity_flags_json"] = json.dumps(list(flags), separators=(",", ":"))
    rationale = row["concise_rationale"]
    if len(rationale) > 500:
        raise ValueError("human-review rationale exceeds 500 characters")
    if row["human_decision"] in {"reject", "uncertain"} and len(rationale) < 8:
        raise ValueError("rejection or uncertainty requires a short rationale")
    return row


def verification_projection(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, str]]:
    projected = [
        {
            "family_id": str(row["family_id"]),
            "family_id_source": str(row["family_id_source"]),
            "verification_status": "needs_review",
            "verified_anchor_time": "",
            "verified_anchor_source": "",
            "timing_structure": "",
            "evidence_reference": "",
            "review_note": "Human audit recommendation only; rule not approved.",
        }
        for row in rows
    ]
    validate_verification_decisions(projected, DECISION_FIELDS)
    return projected


def load_human_decisions(
    path: Path, *, subset_rows: Iterable[Mapping[str, Any]]
) -> tuple[list[dict[str, str]], str | None]:
    if not path.exists():
        return [], None
    rows, fields = _read_csv(path)
    if fields != HUMAN_DECISION_FIELDS:
        raise ValueError("existing human-decisions file has the wrong exact schema")
    subset_by_id = {str(row["audit_id"]): row for row in subset_rows}
    validated = [
        validate_human_decision(row, subset_by_id=subset_by_id) for row in rows
    ]
    if len({row["audit_id"] for row in validated}) != len(validated):
        raise ValueError("human-decisions file contains duplicate audit IDs")
    verification_projection(validated)
    return sorted(validated, key=lambda row: row["audit_id"]), sha256_file(path)


def _csv_bytes(rows: Iterable[Mapping[str, Any]]) -> bytes:
    handle = io.StringIO(newline="")
    writer = csv.DictWriter(
        handle, fieldnames=HUMAN_DECISION_FIELDS, lineterminator="\n"
    )
    writer.writeheader()
    for row in sorted(rows, key=lambda item: str(item["audit_id"])):
        writer.writerow({field: row.get(field, "") for field in HUMAN_DECISION_FIELDS})
    return handle.getvalue().encode("utf-8")


def atomic_save_human_decisions(
    path: Path,
    rows: Iterable[Mapping[str, Any]],
    *,
    subset_rows: Iterable[Mapping[str, Any]],
    guard_root: Path,
    expected_existing_sha256: str | None,
    max_generated_bytes: int = 5 * 1024**3,
    min_free_bytes: int = 80 * 1024**3,
) -> str:
    subset_rows = list(subset_rows)
    subset_by_id = {str(row["audit_id"]): row for row in subset_rows}
    validated = [
        validate_human_decision(dict(row), subset_by_id=subset_by_id) for row in rows
    ]
    if len({row["audit_id"] for row in validated}) != len(validated):
        raise ValueError("cannot save duplicate human decisions")
    verification_projection(validated)
    if path.exists():
        current_hash = sha256_file(path)
        if expected_existing_sha256 is None or current_hash != expected_existing_sha256:
            raise ValueError("human-decisions file changed since it was loaded")
    elif expected_existing_sha256 is not None:
        raise ValueError("expected human-decisions file is missing")
    guard_root = guard_root.resolve()
    guard_root.mkdir(parents=True, exist_ok=True)
    try:
        path.resolve().relative_to(guard_root)
    except ValueError as exc:
        raise ValueError(
            "human-decisions path must remain inside the guard root"
        ) from exc
    payload = _csv_bytes(validated)
    if logical_bytes(guard_root) + len(payload) > max_generated_bytes:
        raise ValueError("atomic save would exceed the generated-namespace ceiling")
    if shutil.disk_usage(guard_root).free - len(payload) < min_free_bytes:
        raise ValueError("atomic save would cross the minimum free-disk floor")
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


def build_human_decision(
    packet: Mapping[str, Any],
    *,
    human_decision: str,
    timing_structure: str,
    candidate_relevance: str,
    confidence: str,
    ambiguity_flags: Iterable[str],
    rationale: str,
) -> dict[str, str]:
    return {
        "audit_id": str(packet["audit_id"]),
        "family_id": str(packet["family_id"]),
        "family_id_source": str(packet["family_id_source"]),
        "proposed_tier": str(packet["proposed_tier"]),
        "proposed_rule": str(packet["proposed_rule"]),
        "source_subset_sha256": EXPECTED_SUBSET_SHA256,
        "review_protocol_version": HUMAN_REVIEW_PROTOCOL_VERSION,
        "human_decision": human_decision,
        "recommended_verification_status": "needs_review",
        "recommended_timing_structure": timing_structure,
        "candidate_is_relevant_ex_ante_anchor": candidate_relevance,
        "confidence": confidence,
        "ambiguity_flags_json": json.dumps(
            sorted(set(ambiguity_flags)), separators=(",", ":")
        ),
        "concise_rationale": rationale.strip(),
    }


def _weighted_rates(rows: list[dict[str, Any]]) -> dict[str, Any]:
    decision_weights = Counter()
    total_weight = 0.0
    for row in rows:
        weight = float(row["human_analysis_weight"])
        total_weight += weight
        decision_weights[row["human_decision"]] += weight
    return {
        "reviewed_count": len(rows),
        "analysis_weight_total": round(total_weight, 6),
        "unweighted_counts": dict(
            sorted(Counter(row["human_decision"] for row in rows).items())
        ),
        "unweighted_rates": {
            key: round(value / len(rows), 6) if rows else 0
            for key, value in sorted(
                Counter(row["human_decision"] for row in rows).items()
            )
        },
        "weighted_rates": {
            key: round(value / total_weight, 6) if total_weight else 0
            for key, value in sorted(decision_weights.items())
        },
    }


def build_final_report(
    subset_rows: list[dict[str, str]], decisions: list[dict[str, str]]
) -> dict[str, Any]:
    if len(decisions) != len(subset_rows):
        raise ValueError("human review is incomplete; all 165 cases are required")
    subset_by_id = {row["audit_id"]: row for row in subset_rows}
    decision_by_id = {row["audit_id"]: row for row in decisions}
    if set(subset_by_id) != set(decision_by_id):
        raise ValueError("human decisions do not cover the exact approved subset")
    verification_projection(decisions)

    combined = []
    for audit_id in sorted(subset_by_id):
        packet = subset_by_id[audit_id]
        decision = decision_by_id[audit_id]
        required_flagged = any(
            marker in packet["human_subset_reason"]
            for marker in (
                "ambiguity_flag",
                "ai_recommended_rejection",
                "low_confidence",
                "case_requires_human_review",
            )
        )
        second_phase_probability = 1.0 if required_flagged else 1.0 / 3.0
        combined.append(
            {
                **packet,
                **decision,
                "human_analysis_weight": float(packet["sampling_weight"])
                / second_phase_probability,
                "ai_human_agree": {
                    "recommend_rule_case": "approve_candidate",
                    "recommend_reject": "reject",
                    "uncertain_human_review": "uncertain",
                    "quarantine_tier_3": "uncertain",
                }.get(packet["reviewer_decision"])
                == decision["human_decision"],
            }
        )

    rule_rows = {
        "PR1_FIXED_CLOCK_SINGLE_EXACT": [
            row for row in combined if row["proposed_tier"] == "tier_1"
        ],
        "PR2_SCHEDULED_START_SINGLE_MILESTONE": [
            row for row in combined if row["proposed_tier"] == "tier_2"
        ],
    }
    human_statistics = {}
    disagreement_statistics = {}
    category_statistics = {}
    failure_mode_statistics = {}
    for rule, rows in rule_rows.items():
        rate_summary = _weighted_rates(rows)
        human_statistics[rule] = {
            **rate_summary,
            "weighted_human_approval_rate": rate_summary["weighted_rates"].get(
                "approve_candidate", 0
            ),
            "weighted_confirmed_false_positive_rate": rate_summary[
                "weighted_rates"
            ].get("reject", 0),
            "weighted_human_uncertainty_rate": rate_summary["weighted_rates"].get(
                "uncertain", 0
            ),
        }
        total_weight = sum(float(row["human_analysis_weight"]) for row in rows)
        disagreement_weight = sum(
            float(row["human_analysis_weight"])
            for row in rows
            if not row["ai_human_agree"]
        )
        disagreement_statistics[rule] = {
            "reviewed_count": len(rows),
            "unweighted_disagreement_count": sum(
                not row["ai_human_agree"] for row in rows
            ),
            "unweighted_disagreement_rate": round(
                sum(not row["ai_human_agree"] for row in rows) / len(rows), 6
            ),
            "weighted_disagreement_rate": (
                round(disagreement_weight / total_weight, 6) if total_weight else 0
            ),
        }
        categories = defaultdict(list)
        for row in rows:
            categories[row["category"]].append(row)
        category_statistics[rule] = {
            category: _weighted_rates(values)
            for category, values in sorted(categories.items())
        }
        flags = defaultdict(list)
        for row in rows:
            source_flags = json.loads(row["ambiguity_flags_json"])
            if not source_flags:
                flags["no_human_ambiguity_flag"].append(row)
            for flag in source_flags:
                flags[flag].append(row)
        failure_mode_statistics[rule] = {
            flag: _weighted_rates(values) for flag, values in sorted(flags.items())
        }

    return {
        "schema_version": "1.0",
        "review_protocol_version": HUMAN_REVIEW_PROTOCOL_VERSION,
        "source_subset_sha256": EXPECTED_SUBSET_SHA256,
        "reviewed_case_count": len(decisions),
        "actual_verification_status_counts": {"needs_review": len(decisions)},
        "anchors_verified": 0,
        "rules_approved": 0,
        "rule_status": {
            "PR1_FIXED_CLOCK_SINGLE_EXACT": "not_approved",
            "PR2_SCHEDULED_START_SINGLE_MILESTONE": "not_approved",
        },
        "outcomes_accessed": False,
        "post_event_information_accessed": False,
        "horizon_prices_built": False,
        "network_requests": 0,
        "exact_verification_schema_validation": "passed_recommendation_only_projection",
        "weighting_note": (
            "Original audit weights are multiplied by three for non-flagged cases "
            "selected through the deterministic 50-of-150 second-phase sample; "
            "all required flagged cases have second-phase inclusion probability one."
        ),
        "human_review_statistics": human_statistics,
        "confirmed_false_positive_definition": "human_decision=reject",
        "ai_human_disagreement_statistics": disagreement_statistics,
        "category_statistics": category_statistics,
        "failure_mode_statistics": failure_mode_statistics,
        "tier_3_diagnostic": {
            "interpretation": "small diagnostic only; no population weighting",
            "reviewed_count": sum(row["proposed_tier"] == "tier_3" for row in combined),
            "unweighted_counts": dict(
                sorted(
                    Counter(
                        row["human_decision"]
                        for row in combined
                        if row["proposed_tier"] == "tier_3"
                    ).items()
                )
            ),
        },
    }


def atomic_write_json(
    path: Path,
    value: Mapping[str, Any],
    *,
    guard_root: Path,
    max_generated_bytes: int = 5 * 1024**3,
    min_free_bytes: int = 80 * 1024**3,
) -> str:
    payload = (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    guard_root = guard_root.resolve()
    guard_root.mkdir(parents=True, exist_ok=True)
    try:
        path.resolve().relative_to(guard_root)
    except ValueError as exc:
        raise ValueError("final-report path must remain inside the guard root") from exc
    if logical_bytes(guard_root) + len(payload) > max_generated_bytes:
        raise ValueError("final report would exceed the generated-namespace ceiling")
    if shutil.disk_usage(guard_root).free - len(payload) < min_free_bytes:
        raise ValueError("final report would cross the minimum free-disk floor")
    payload_hash = hashlib.sha256(payload).hexdigest()
    if path.exists():
        if sha256_file(path) != payload_hash:
            raise ValueError(
                "existing human-review report conflicts with final decisions"
            )
        return payload_hash
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
