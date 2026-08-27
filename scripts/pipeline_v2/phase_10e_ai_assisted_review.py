"""Deterministic Phase 10E AI-assisted annotation import and validation design.

The finalized 165-row table is explicitly AI-assisted and outcome-blind.  Its
entries remain recommendations: every verification status is ``needs_review``
and every verified-anchor field is blank.  This module never applies a rule.
"""

from __future__ import annotations

from collections import Counter, defaultdict
import csv
import hashlib
import json
import math
from pathlib import Path
import re
from typing import Any, Iterable, Mapping

from scripts.pipeline_v2.apply_anchor_verification import (
    DECISION_FIELDS,
    validate_decisions as validate_verification_decisions,
)
from scripts.pipeline_v2.phase_10e_human_review import (
    CONFIDENCE_LEVELS,
    EXPECTED_PACKET_SHA256,
    EXPECTED_SUBSET_SHA256,
    parse_ambiguity_flags,
    sha256_file,
)
from scripts.pipeline_v2.study_rules import validate_research_feature_columns


AI_ASSISTED_PROTOCOL_VERSION = "phase-10e-ai-assisted-outcome-blind-review-v1"
INDEPENDENT_VALIDATION_SEED = "phase-10e-independent-human-validation-v1"
EXPECTED_FIRST_REVIEW_SHA256 = (
    "ad993c0470534765cd6264f45600838a0fd160d5a6ddd3fa9d10cdede94578ff"
)
ANNOTATION_HEADERS = (
    "review_number",
    "audit_id",
    "human_decision",
    "timing_structure",
    "candidate_relevant",
    "confidence",
    "ambiguity_flags",
    "rationale",
    "review_label",
)
AI_ASSISTED_DECISION_FIELDS = (
    "audit_id",
    "review_number",
    "family_id",
    "family_id_source",
    "proposed_tier",
    "proposed_rule",
    "annotation_type",
    "source_annotation_sha256",
    "source_subset_sha256",
    "source_packet_sha256",
    "review_protocol_version",
    "ai_assisted_decision",
    "verification_status",
    "verified_anchor_time",
    "verified_anchor_source",
    "recommended_timing_structure",
    "candidate_is_relevant_ex_ante_anchor",
    "confidence",
    "ambiguity_flags_json",
    "concise_rationale",
)
INDEPENDENT_PACKET_FIELDS = (
    "validation_id",
    "audit_id",
    "proposed_tier",
    "proposed_rule",
    "category",
    "family_id",
    "family_id_source",
    "family_title",
    "event_title",
    "event_sub_title",
    "candidate_count",
    "unique_exact_time_count",
    "evidence_pattern",
    "semantic_agreement",
    "analysis_window_status",
    "candidates_json",
    "reviewer_instruction",
    "independent_human_decision",
    "recommended_timing_structure",
    "candidate_is_relevant_ex_ante_anchor",
    "confidence",
    "ambiguity_flags_json",
    "concise_rationale",
)
INDEPENDENT_MANIFEST_FIELDS = (
    "validation_id",
    "audit_id",
    "proposed_tier",
    "proposed_rule",
    "category",
    "validation_stratum",
    "validation_stratum_population",
    "validation_stratum_sample_count",
    "validation_inclusion_probability",
    "source_audit_sampling_weight",
    "ai_assisted_subset_inclusion_probability",
    "ai_assisted_analysis_weight",
    "independent_validation_analysis_weight",
)
_DECISION_MAP = {
    "A": "approve_candidate",
    "A - Approve": "approve_candidate",
    "R": "reject",
    "R - Reject": "reject",
    "U": "uncertain",
    "U - Uncertain": "uncertain",
}
_TIMING_MAP = {
    "fixed_clock": "fixed_clock",
    "scheduled_event_start": "scheduled_event_start",
    "neither/uncertain": "unclear",
}
_FINAL_DECISION_INVARIANTS = {
    **{
        number: "approve_candidate"
        for number in (
            104,
            109,
            114,
            115,
            116,
            120,
            122,
            128,
            129,
            133,
            137,
            141,
            146,
            148,
            151,
        )
    },
    **{number: "uncertain" for number in (54, 58, 59, 67, 138)},
    **{number: "reject" for number in (27, 87, 101, 112, 160)},
    **{number: "approve_candidate" for number in (81, 84, 85, 86, 165)},
}


def read_annotation_table(path: Path) -> list[dict[str, str]]:
    if path.suffix.casefold() != ".csv":
        raise ValueError("finalized annotation source must be the compact CSV")
    with path.open(newline="", encoding="utf-8-sig") as handle:
        reader = csv.DictReader(handle)
        if tuple(reader.fieldnames or ()) != ANNOTATION_HEADERS:
            raise ValueError("annotation CSV has an unexpected exact header")
        return list(reader)


def _flags_from_cell(value: Any) -> tuple[str, ...]:
    text = str(value or "").strip()
    if not text:
        return ()
    if text.startswith("["):
        return parse_ambiguity_flags(text)
    normalized = re.split(r"\s*[,;|]\s*", text)
    return parse_ambiguity_flags(json.dumps([item for item in normalized if item]))


def _verification_projection(rows: Iterable[Mapping[str, Any]]) -> list[dict[str, str]]:
    projected = [
        {
            "family_id": str(row["family_id"]),
            "family_id_source": str(row["family_id_source"]),
            "verification_status": "needs_review",
            "verified_anchor_time": "",
            "verified_anchor_source": "",
            "timing_structure": "",
            "evidence_reference": "",
            "review_note": "AI-assisted audit recommendation only; rules not approved.",
        }
        for row in rows
    ]
    validate_verification_decisions(projected, DECISION_FIELDS)
    return projected


def import_ai_assisted_annotations(
    annotation_path: Path,
    subset_rows: list[dict[str, str]],
    *,
    expected_annotation_sha256: str,
) -> list[dict[str, str]]:
    annotation_hash = sha256_file(annotation_path)
    if annotation_hash != expected_annotation_sha256:
        raise ValueError("annotation table SHA-256 does not match the supplied pin")
    raw_rows = read_annotation_table(annotation_path)
    if len(raw_rows) != 165:
        raise ValueError("finalized annotation table must contain exactly 165 rows")
    subset_by_id = {row["audit_id"]: row for row in subset_rows}
    expected_numbers = {
        row["audit_id"]: number for number, row in enumerate(subset_rows, 1)
    }
    observed_ids = [str(row.get("audit_id") or "").strip() for row in raw_rows]
    duplicates = sorted(
        key for key, count in Counter(observed_ids).items() if count > 1
    )
    missing = sorted(set(subset_by_id) - set(observed_ids))
    extra = sorted(set(observed_ids) - set(subset_by_id))
    if duplicates or missing or extra:
        raise ValueError(
            f"annotation audit-ID mismatch; missing={missing}; extra={extra}; duplicates={duplicates}"
        )
    imported = []
    for raw in raw_rows:
        audit_id = str(raw["audit_id"]).strip()
        packet = subset_by_id[audit_id]
        try:
            review_number = int(str(raw["review_number"]).strip())
        except ValueError as exc:
            raise ValueError(f"invalid review number for {audit_id}") from exc
        if review_number != expected_numbers[audit_id]:
            raise ValueError(f"review number does not match audit ID {audit_id}")
        if str(raw["review_label"]).strip() != "AI-assisted outcome-blind review":
            raise ValueError(f"review row {review_number} has the wrong review label")
        decision_token = str(raw["human_decision"] or "").strip()
        if decision_token not in _DECISION_MAP:
            raise ValueError(
                f"missing or unsupported decision in review row {review_number}"
            )
        timing_token = str(raw["timing_structure"] or "").strip()
        if timing_token not in _TIMING_MAP:
            raise ValueError(
                f"missing or unsupported timing in review row {review_number}"
            )
        relevance = str(raw["candidate_relevant"] or "").strip().casefold()
        if relevance not in {"yes", "no", "uncertain"}:
            raise ValueError(
                f"missing or unsupported relevance in review row {review_number}"
            )
        confidence = str(raw["confidence"] or "").strip().casefold()
        if confidence not in CONFIDENCE_LEVELS:
            raise ValueError(
                f"missing or unsupported confidence in review row {review_number}"
            )
        rationale = str(raw["rationale"] or "").strip()
        decision = _DECISION_MAP[decision_token]
        if decision in {"reject", "uncertain"} and len(rationale) < 8:
            raise ValueError(f"review row {review_number} requires a short rationale")
        if len(rationale) > 500:
            raise ValueError(
                f"review row {review_number} rationale exceeds 500 characters"
            )
        flags = _flags_from_cell(raw["ambiguity_flags"])
        imported.append(
            {
                "audit_id": audit_id,
                "review_number": str(review_number),
                "family_id": packet["family_id"],
                "family_id_source": packet["family_id_source"],
                "proposed_tier": packet["proposed_tier"],
                "proposed_rule": packet["proposed_rule"],
                "annotation_type": "ai_assisted_outcome_blind_review",
                "source_annotation_sha256": annotation_hash,
                "source_subset_sha256": EXPECTED_SUBSET_SHA256,
                "source_packet_sha256": EXPECTED_PACKET_SHA256,
                "review_protocol_version": AI_ASSISTED_PROTOCOL_VERSION,
                "ai_assisted_decision": decision,
                "verification_status": "needs_review",
                "verified_anchor_time": "",
                "verified_anchor_source": "",
                "recommended_timing_structure": _TIMING_MAP[timing_token],
                "candidate_is_relevant_ex_ante_anchor": relevance,
                "confidence": confidence,
                "ambiguity_flags_json": json.dumps(list(flags), separators=(",", ":")),
                "concise_rationale": rationale,
            }
        )
    imported.sort(key=lambda row: int(row["review_number"]))
    expected_decision_counts = {
        "approve_candidate": 149,
        "reject": 5,
        "uncertain": 11,
    }
    decision_counts = Counter(row["ai_assisted_decision"] for row in imported)
    if dict(decision_counts) != expected_decision_counts:
        raise ValueError(
            "finalized decision counts do not match A=149, R=5, U=11; "
            f"found={dict(sorted(decision_counts.items()))}"
        )
    for review_number, expected in sorted(_FINAL_DECISION_INVARIANTS.items()):
        actual = imported[review_number - 1]["ai_assisted_decision"]
        if actual != expected:
            raise ValueError(
                f"finalized correction invariant failed at review {review_number}: expected {expected}, found {actual}"
            )
    if "deadline_or_window_not_fixed_clock" not in json.loads(
        imported[86]["ambiguity_flags_json"]
    ):
        raise ValueError("review 87 must retain deadline_or_window_not_fixed_clock")
    validate_research_feature_columns(AI_ASSISTED_DECISION_FIELDS)
    _verification_projection(imported)
    return imported


def load_first_review(path: Path) -> dict[str, dict[str, str]]:
    if sha256_file(path) != EXPECTED_FIRST_REVIEW_SHA256:
        raise ValueError(
            "first-review packet SHA-256 does not match the approved artifact"
        )
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))
    if len(rows) != 450 or len({row["audit_id"] for row in rows}) != 450:
        raise ValueError("first-review packet must contain 450 unique audit IDs")
    return {row["audit_id"]: row for row in rows}


def _mandatory_for_ai_assisted_subset(row: Mapping[str, Any]) -> bool:
    return (
        str(row.get("confidence") or "") == "low"
        or str(row.get("reviewer_decision") or "") == "recommend_reject"
        or str(row.get("ambiguity_flags_json") or "") != "[]"
        or str(row.get("human_review_required") or "") == "true"
    )


def ai_assisted_analysis_rows(
    subset_rows: list[dict[str, str]],
    imported: list[dict[str, str]],
    first_review_by_id: Mapping[str, Mapping[str, Any]],
) -> list[dict[str, Any]]:
    imported_by_id = {row["audit_id"]: row for row in imported}
    for packet in subset_rows:
        first_review = first_review_by_id.get(packet["audit_id"])
        if first_review is None:
            raise ValueError("compact subset lacks its hash-pinned first-review row")
        for field in ("proposed_tier", "proposed_rule"):
            if str(first_review[field]) != str(packet[field]):
                raise ValueError(f"first-review/subset conflict on {field}")
    output = []
    for tier in ("tier_1", "tier_2"):
        selected = [row for row in subset_rows if row["proposed_tier"] == tier]
        for packet in selected:
            mandatory = _mandatory_for_ai_assisted_subset(packet)
            # The approved first-review design selected 50 of the 150 audit
            # cases in each proposed-rule tier, then added every mandatory
            # flagged case. Non-mandatory inclusion probability is therefore
            # exactly 50/150, not the realized non-mandatory sample share.
            subset_probability = 1.0 if mandatory else 1.0 / 3.0
            output.append(
                {
                    **packet,
                    **imported_by_id[packet["audit_id"]],
                    "ai_assisted_subset_inclusion_probability": subset_probability,
                    "ai_assisted_analysis_weight": float(packet["sampling_weight"])
                    / subset_probability,
                }
            )
    return sorted(output, key=lambda row: row["audit_id"])


def _rate_summary(rows: list[Mapping[str, Any]], weight_field: str) -> dict[str, Any]:
    counts = Counter(str(row["ai_assisted_decision"]) for row in rows)
    weighted = Counter()
    total_weight = 0.0
    for row in rows:
        weight = float(row[weight_field])
        total_weight += weight
        weighted[str(row["ai_assisted_decision"])] += weight
    return {
        "case_count": len(rows),
        "unweighted_counts": {
            key: counts[key] for key in ("approve_candidate", "reject", "uncertain")
        },
        "unweighted_rates": {
            key: round(counts[key] / len(rows), 6) if rows else 0
            for key in ("approve_candidate", "reject", "uncertain")
        },
        "weighted_population": round(total_weight, 6),
        "weighted_rates": {
            key: round(weighted[key] / total_weight, 6) if total_weight else 0
            for key in ("approve_candidate", "reject", "uncertain")
        },
    }


def build_ai_assisted_diagnostics(
    subset_rows: list[dict[str, str]],
    imported: list[dict[str, str]],
    first_review_by_id: Mapping[str, Mapping[str, Any]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    imported_by_id = {row["audit_id"]: row for row in imported}
    combined_all = [
        {**packet, **imported_by_id[packet["audit_id"]]} for packet in subset_rows
    ]
    analysis = ai_assisted_analysis_rows(subset_rows, imported, first_review_by_id)
    rule_groups = {
        "PR1_FIXED_CLOCK_SINGLE_EXACT": [
            row for row in analysis if row["proposed_tier"] == "tier_1"
        ],
        "PR2_SCHEDULED_START_SINGLE_MILESTONE": [
            row for row in analysis if row["proposed_tier"] == "tier_2"
        ],
    }
    category_groups = defaultdict(list)
    for row in analysis:
        category_groups[row["category"]].append(row)
    failure_modes = Counter(
        flag for row in combined_all for flag in json.loads(row["ambiguity_flags_json"])
    )
    diagnostics = {
        "schema_version": "1.0",
        "annotation_type": "ai_assisted_outcome_blind_review",
        "review_protocol_version": AI_ASSISTED_PROTOCOL_VERSION,
        "reviewed_case_count": len(imported),
        "all_165_unweighted": _rate_summary(
            [{**row, "unit_weight": 1} for row in combined_all], "unit_weight"
        ),
        "rule_inference_population": "Tier 1 and Tier 2 only; Tier 3 excluded",
        "combined_tier_1_2": _rate_summary(analysis, "ai_assisted_analysis_weight"),
        "rule_specific": {
            rule: _rate_summary(rows, "ai_assisted_analysis_weight")
            for rule, rows in rule_groups.items()
        },
        "category_specific": {
            category: _rate_summary(rows, "ai_assisted_analysis_weight")
            for category, rows in sorted(category_groups.items())
        },
        "failure_mode_counts_all_165": dict(sorted(failure_modes.items())),
        "tier_3_diagnostic_unweighted": _rate_summary(
            [
                {**row, "unit_weight": 1}
                for row in combined_all
                if row["proposed_tier"] == "tier_3"
            ],
            "unit_weight",
        ),
        "independent_human_review_completed": False,
        "ai_assisted_vs_human_disagreement_available": False,
        "anchors_verified": 0,
        "verification_status_counts": {"needs_review": len(imported)},
        "rules_approved": 0,
        "rule_status": {
            "PR1_FIXED_CLOCK_SINGLE_EXACT": "not_approved",
            "PR2_SCHEDULED_START_SINGLE_MILESTONE": "not_approved",
        },
        "outcomes_accessed": False,
        "post_event_information_accessed": False,
        "horizon_prices_built": False,
        "network_requests": 0,
    }
    return diagnostics, analysis


def _sample_rank(row: Mapping[str, Any], stratum: str) -> str:
    payload = "\x00".join(
        (
            INDEPENDENT_VALIDATION_SEED,
            str(row["proposed_tier"]),
            stratum,
            str(row["audit_id"]),
        )
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def _primary_failure_mode(row: Mapping[str, Any]) -> str:
    flags = json.loads(str(row.get("ambiguity_flags_json") or "[]"))
    return sorted(flags)[0] if flags else "no_ai_assisted_ambiguity_flag"


def _allocate_stratified(
    rows: list[dict[str, Any]], sample_size: int
) -> list[dict[str, Any]]:
    groups = defaultdict(list)
    for row in rows:
        stratum = f"{row['category']}|{_primary_failure_mode(row)}"
        groups[stratum].append(row)
    if len(groups) > sample_size:
        raise ValueError("too many independent-validation strata for requested sample")
    allocations = {stratum: 1 for stratum in groups}
    remaining = sample_size - len(groups)
    capacities = {stratum: len(values) - 1 for stratum, values in groups.items()}
    capacity_total = sum(capacities.values())
    used = 0
    fractions = []
    if remaining and capacity_total:
        for stratum in sorted(groups):
            exact = remaining * capacities[stratum] / capacity_total
            extra = min(capacities[stratum], math.floor(exact))
            allocations[stratum] += extra
            used += extra
            fractions.append((exact - extra, stratum))
        for _, stratum in sorted(fractions, key=lambda item: (-item[0], item[1])):
            if used >= remaining:
                break
            if allocations[stratum] < len(groups[stratum]):
                allocations[stratum] += 1
                used += 1
    if sum(allocations.values()) != sample_size:
        raise ValueError("failed to allocate independent-validation sample")
    selected = []
    for stratum in sorted(groups):
        population = sorted(groups[stratum], key=lambda row: _sample_rank(row, stratum))
        allocation = allocations[stratum]
        probability = allocation / len(population)
        for row in population[:allocation]:
            selected.append(
                {
                    **row,
                    "validation_stratum": stratum,
                    "validation_stratum_population": len(population),
                    "validation_stratum_sample_count": allocation,
                    "validation_inclusion_probability": probability,
                    "independent_validation_analysis_weight": float(
                        row["ai_assisted_analysis_weight"]
                    )
                    / probability,
                }
            )
    return sorted(selected, key=lambda row: (row["proposed_tier"], row["audit_id"]))


def build_independent_validation_design(
    analysis_rows: list[dict[str, Any]], *, per_rule: int = 50
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    selected = []
    for tier in ("tier_1", "tier_2"):
        pool = [row for row in analysis_rows if row["proposed_tier"] == tier]
        if len(pool) < per_rule:
            raise ValueError(f"{tier} has fewer than {per_rule} AI-assisted cases")
        selected.extend(_allocate_stratified(pool, per_rule))
    packets = []
    manifests = []
    for number, row in enumerate(selected, 1):
        validation_id = f"P10E-HV-{number:03d}"
        packets.append(
            {
                "validation_id": validation_id,
                "audit_id": row["audit_id"],
                "proposed_tier": row["proposed_tier"],
                "proposed_rule": row["proposed_rule"],
                "category": row["category"],
                "family_id": row["family_id"],
                "family_id_source": row["family_id_source"],
                "family_title": row["family_title"],
                "event_title": row["event_title"],
                "event_sub_title": row["event_sub_title"],
                "candidate_count": row["candidate_count"],
                "unique_exact_time_count": row["unique_exact_time_count"],
                "evidence_pattern": row["evidence_pattern"],
                "semantic_agreement": row["semantic_agreement"],
                "analysis_window_status": row["analysis_window_status"],
                "candidates_json": row["candidates_json"],
                "reviewer_instruction": (
                    "Independently review only the supplied ex-ante evidence. Do not consult AI annotations, outcomes, settlement information, or post-anchor prices."
                ),
                "independent_human_decision": "",
                "recommended_timing_structure": "",
                "candidate_is_relevant_ex_ante_anchor": "",
                "confidence": "",
                "ambiguity_flags_json": "[]",
                "concise_rationale": "",
            }
        )
        manifests.append(
            {
                "validation_id": validation_id,
                "audit_id": row["audit_id"],
                "proposed_tier": row["proposed_tier"],
                "proposed_rule": row["proposed_rule"],
                "category": row["category"],
                "validation_stratum": row["validation_stratum"],
                "validation_stratum_population": row["validation_stratum_population"],
                "validation_stratum_sample_count": row[
                    "validation_stratum_sample_count"
                ],
                "validation_inclusion_probability": round(
                    row["validation_inclusion_probability"], 12
                ),
                "source_audit_sampling_weight": row["sampling_weight"],
                "ai_assisted_subset_inclusion_probability": round(
                    row["ai_assisted_subset_inclusion_probability"], 12
                ),
                "ai_assisted_analysis_weight": round(
                    row["ai_assisted_analysis_weight"], 12
                ),
                "independent_validation_analysis_weight": round(
                    row["independent_validation_analysis_weight"], 12
                ),
            }
        )
    validate_research_feature_columns(INDEPENDENT_PACKET_FIELDS)
    if any(
        key.startswith("ai_assisted")
        or key in {"reviewer_decision", "human_subset_reason"}
        for key in INDEPENDENT_PACKET_FIELDS
    ):
        raise ValueError("independent packet schema exposes a prior recommendation")
    report = {
        "schema_version": "1.0",
        "sampling_seed": INDEPENDENT_VALIDATION_SEED,
        "sample_count": len(packets),
        "sample_counts_by_rule": dict(
            sorted(Counter(row["proposed_rule"] for row in packets).items())
        ),
        "sample_counts_by_category": dict(
            sorted(Counter(row["category"] for row in packets).items())
        ),
        "strata": dict(
            sorted(Counter(row["validation_stratum"] for row in manifests).items())
        ),
        "tier_3_excluded_from_rule_inference": True,
        "estimated_minutes_per_case": 4,
        "estimated_review_hours": round(len(packets) * 4 / 60, 2),
        "prior_ai_recommendations_in_packet": 0,
        "prior_ai_assisted_decisions_in_packet": 0,
        "outcomes_in_packet": 0,
        "settlement_fields_in_packet": 0,
        "post_anchor_prices_in_packet": 0,
        "anchors_verified": 0,
        "rules_approved": 0,
        "horizon_prices_built": False,
        "network_requests": 0,
    }
    return packets, manifests, report
