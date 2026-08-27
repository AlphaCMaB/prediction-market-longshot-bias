"""Build the approval-gated, outcome-blind Phase 10E audit packet.

This command classifies proposed audit tiers and samples them reproducibly. It
does not create verified decisions, apply anchors, build horizons, read prices,
or issue network requests.
"""

from __future__ import annotations

import argparse
from collections import Counter
import csv
import gzip
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import uuid
from typing import Any, Iterable, Mapping

from scripts.pipeline_v2.anchor_evidence import DECISION_TEMPLATE_FIELDS
from scripts.pipeline_v2.phase_10e_verification_design import (
    AUDIT_SEED,
    DESIGN_SCHEMA_VERSION,
    TierAssignment,
    assign_tier,
    distinct_exact_times,
    evidence_pattern,
    event_tickers,
    integer,
    safe_candidate_projection,
    source_combination,
    stratified_sample,
    tier_one_mechanical,
    tier_two_mechanical,
)
from scripts.pipeline_v2.study_rules import (
    load_study_rules,
    validate_research_feature_columns,
)


PATTERN_FIELDS = (
    "source_combination",
    "candidate_count",
    "unique_exact_time_count",
    "evidence_pattern",
    "family_count",
)
PACKET_FIELDS = (
    "audit_id",
    "proposed_tier",
    "proposed_rule",
    "tier_reason",
    "proposed_timing_structure",
    "semantic_agreement",
    "audit_stratum",
    "stratum_family_count",
    "stratum_sample_count",
    "sampling_weight",
    "family_id",
    "family_id_source",
    "category",
    "event_ticker",
    "series_ticker",
    "family_title",
    "event_title",
    "event_sub_title",
    "market_count",
    "candidate_count",
    "unique_exact_time_count",
    "source_combination",
    "evidence_pattern",
    "proposed_candidate_id",
    "proposed_candidate_time",
    "proposed_candidate_source_type",
    "proposed_verified_anchor_source",
    "analysis_window_status",
    "candidates_json",
    "reviewer_instruction",
)
REQUIRED_OUTPUTS = (
    "phase_10e_pattern_counts.csv",
    "phase_10e_audit_review_packet.csv",
    "phase_10e_audit_review_packet.md",
    "phase_10e_audit_decisions_template.csv",
    "phase_10e_design_report.json",
)


def _open_csv(path: Path):
    if path.suffix == ".gz":
        return gzip.open(path, "rt", newline="", encoding="utf-8")
    return path.open("r", newline="", encoding="utf-8")


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


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _event_projection(row: Mapping[str, Any]) -> dict[str, str]:
    return {
        "event_ticker": str(row.get("event_ticker") or "").strip(),
        "series_ticker": str(row.get("series_ticker") or "").strip(),
        "title": str(row.get("title") or ""),
        "sub_title": str(row.get("sub_title") or ""),
        "category": str(row.get("category") or ""),
    }


def _load_events(path: Path) -> dict[str, dict[str, str]]:
    output = {}
    with _open_csv(path) as handle:
        reader = csv.DictReader(handle)
        required = {"event_ticker", "series_ticker", "title", "sub_title", "category"}
        if not required.issubset(reader.fieldnames or ()):
            raise ValueError("event metadata lacks the outcome-blind review projection")
        for source in reader:
            row = _event_projection(source)
            ticker = row["event_ticker"]
            if not ticker:
                raise ValueError("event metadata contains a blank ticker")
            if ticker in output and output[ticker] != row:
                raise ValueError(f"conflicting event review projection for {ticker!r}")
            output[ticker] = row
    return output


def _family_identity(row: Mapping[str, Any]) -> tuple[str, str]:
    identity = (
        str(row.get("family_id") or "").strip(),
        str(row.get("family_id_source") or "").strip(),
    )
    if not all(identity):
        raise ValueError("family review row lacks composite identity")
    return identity


def _load_families(path: Path) -> dict[tuple[str, str], dict[str, str]]:
    output = {}
    with _open_csv(path) as handle:
        reader = csv.DictReader(handle)
        for source in reader:
            row = dict(source)
            identity = _family_identity(row)
            if identity in output:
                raise ValueError(f"duplicate family review identity {identity!r}")
            output[identity] = row
    return output


def _candidate_identity(row: Mapping[str, Any]) -> tuple[str, str]:
    return (
        str(row.get("family_id") or "").strip(),
        str(row.get("family_id_source") or "").strip(),
    )


def _read_proposal_candidates(
    path: Path,
    tier_one_pool: set[tuple[str, str]],
    tier_two_pool: set[tuple[str, str]],
) -> tuple[
    dict[tuple[str, str], dict[str, str]], dict[tuple[str, str], dict[str, str]]
]:
    singles = {}
    milestones = {}
    with _open_csv(path) as handle:
        for row in csv.DictReader(handle):
            identity = _candidate_identity(row)
            if identity in tier_one_pool:
                if identity in singles:
                    raise ValueError(
                        f"Tier 1 mechanical family has multiple rows: {identity!r}"
                    )
                singles[identity] = dict(row)
            if (
                identity in tier_two_pool
                and row.get("candidate_source_type") == "event_milestone_start_date"
            ):
                if identity in milestones:
                    raise ValueError(
                        f"Tier 2 mechanical family has multiple milestone rows: {identity!r}"
                    )
                milestones[identity] = dict(row)
    if singles.keys() != tier_one_pool:
        raise ValueError("Tier 1 mechanical pool is not covered by evidence rows")
    if milestones.keys() != tier_two_pool:
        raise ValueError("Tier 2 mechanical pool is not covered by milestone evidence")
    return singles, milestones


def _candidate_month(candidate: Mapping[str, Any] | None) -> str:
    if candidate is None:
        return "no_proposed_candidate"
    status = str(candidate.get("analysis_window_status") or "")
    timestamp = str(candidate.get("candidate_time_utc") or "")
    return timestamp[:7] if status == "inside_analysis_window" else status


def _tier_stratum(
    assignment: TierAssignment,
    family: Mapping[str, Any],
    candidate: Mapping[str, Any] | None,
) -> str:
    category = str(family.get("category") or "[uncategorized]")
    if assignment.tier == "tier_1":
        source = str(candidate.get("candidate_source_type") or "") if candidate else ""
        return "|".join((category, _candidate_month(candidate), source))
    if assignment.tier == "tier_2":
        return "|".join(
            (category, _candidate_month(candidate), assignment.semantic_agreement)
        )
    return "|".join((assignment.reason, category))


def _pattern_rows(families: Mapping[tuple[str, str], Mapping[str, Any]]):
    counts = Counter(
        (
            source_combination(row),
            integer(row.get("candidate_count")),
            len(distinct_exact_times(row)),
            evidence_pattern(row),
        )
        for row in families.values()
    )
    return [
        {
            "source_combination": key[0],
            "candidate_count": key[1],
            "unique_exact_time_count": key[2],
            "evidence_pattern": key[3],
            "family_count": count,
        }
        for key, count in sorted(counts.items())
    ]


def _assignment_statistics(
    identities: Iterable[tuple[str, str]],
    families: Mapping[tuple[str, str], Mapping[str, Any]],
    assignments: Mapping[tuple[str, str], TierAssignment],
    proposal_candidates: Mapping[tuple[str, str], Mapping[str, Any]],
) -> dict[str, Any]:
    ids = tuple(identities)
    categories = Counter(
        str(families[key].get("category") or "[uncategorized]") for key in ids
    )
    reasons = Counter(assignments[key].reason for key in ids)
    patterns = Counter(evidence_pattern(families[key]) for key in ids)
    windows = Counter(
        str(proposal_candidates[key].get("analysis_window_status") or "")
        for key in ids
        if key in proposal_candidates
    )
    months = Counter(
        str(proposal_candidates[key].get("candidate_time_utc") or "")[:7]
        for key in ids
        if key in proposal_candidates
        and proposal_candidates[key].get("analysis_window_status")
        == "inside_analysis_window"
    )
    sources = Counter(
        str(proposal_candidates[key].get("candidate_source_type") or "")
        for key in ids
        if key in proposal_candidates
    )
    return {
        "family_count": len(ids),
        "category_counts": dict(sorted(categories.items())),
        "reason_counts": dict(sorted(reasons.items())),
        "evidence_pattern_counts": dict(sorted(patterns.items())),
        "proposed_candidate_window_counts": dict(sorted(windows.items())),
        "inside_window_month_counts": dict(sorted(months.items())),
        "proposed_candidate_source_counts": dict(sorted(sources.items())),
    }


def _packet_markdown(
    packet_rows: list[dict[str, Any]], report: Mapping[str, Any]
) -> str:
    lines = [
        "# Phase 10E Outcome-Blind Audit Review Packet",
        "",
        "> **Approval gate:** Proposed tiers and rules are sampling labels only. This packet",
        "> contains zero verified anchors and must not be used as an applied decision file.",
        "",
        "## Reviewer instructions",
        "",
        "Review only the family, event, and allowed candidate evidence shown here. Do not",
        "consult outcomes, results, settlement values, close/expiration times, prices, or",
        "post-event information. Record decisions in a copy of",
        "`phase_10e_audit_decisions_template.csv`, preserving its exact schema.",
        "",
        "A reviewed family may be marked `verified_manual`, `rejected`, or left",
        "`needs_review`. A verified decision must state the exact candidate time, one",
        "frozen allowed source, an allowed timing structure, evidence reference, and note.",
        "No deterministic promotion is approved by this packet.",
        "",
        "## Audit design",
        "",
        f"- Seed: `{report['audit_design']['seed']}`",
        f"- Sample: {report['audit_design']['sample_family_count']} families",
        f"  ({report['audit_design']['sample_per_tier']} per tier)",
        "- Sampling: deterministic stratified hash sample with recorded weights",
        "- Recommended independent double review: 50 families per tier",
        "- Current approval and disagreement rates: **[TO BE MEASURED AFTER REVIEW]**",
        "",
        "## Sampled families",
        "",
    ]
    for row in packet_rows:
        lines.extend(
            [
                f"### {row['audit_id']} — {row['family_id']}",
                "",
                f"- Proposed tier/rule: `{row['proposed_tier']}` / `{row['proposed_rule']}`",
                f"- Sampling stratum/weight: `{row['audit_stratum']}` / {row['sampling_weight']}",
                f"- Category: {row['category']}",
                f"- Event ticker/series: `{row['event_ticker']}` / `{row['series_ticker']}`",
                f"- Family title: {row['family_title']}",
                f"- Event title: {row['event_title']}",
                f"- Event subtitle: {row['event_sub_title']}",
                f"- Evidence pattern: `{row['evidence_pattern']}`; source combination: `{row['source_combination']}`",
                f"- Tier rationale: {row['tier_reason']}",
                f"- Proposed timing (unapproved): `{row['proposed_timing_structure']}`",
                "",
                "Candidate evidence:",
                "",
                "```json",
                json.dumps(
                    json.loads(row["candidates_json"]), indent=2, ensure_ascii=False
                ),
                "```",
                "",
                "Reviewer action: enter the outcome-blind decision in the separate exact-schema template.",
                "",
            ]
        )
    return "\n".join(lines) + "\n"


def _compare_existing(existing: Path, derived: Path) -> bool:
    if not existing.is_dir():
        return False
    if tuple(
        sorted(path.name for path in existing.iterdir() if path.is_file())
    ) != tuple(sorted(REQUIRED_OUTPUTS)):
        return False
    return all(
        _sha256(existing / name) == _sha256(derived / name) for name in REQUIRED_OUTPUTS
    )


def run(
    family_review_path: Path,
    evidence_path: Path,
    event_metadata_path: Path,
    output_root: Path,
    *,
    config_path: Path,
    guard_root: Path,
    audit_per_tier: int = 150,
    seed: str = AUDIT_SEED,
    expected_family_sha256: str | None = None,
    expected_evidence_sha256: str | None = None,
    expected_event_sha256: str | None = None,
    max_generated_bytes: int = 5 * 1024**3,
    min_free_bytes: int = 80 * 1024**3,
) -> dict[str, Any]:
    if audit_per_tier <= 0:
        raise ValueError("audit sample per tier must be positive")
    rules = load_study_rules(config_path)
    inputs = {
        "family_review": family_review_path,
        "anchor_evidence": evidence_path,
        "event_metadata": event_metadata_path,
    }
    expected = {
        "family_review": expected_family_sha256,
        "anchor_evidence": expected_evidence_sha256,
        "event_metadata": expected_event_sha256,
    }
    input_hashes = {name: _sha256(path) for name, path in inputs.items()}
    for name, expected_hash in expected.items():
        if expected_hash and input_hashes[name] != expected_hash:
            raise ValueError(f"{name} SHA-256 does not match the approved input")

    events = _load_events(event_metadata_path)
    families = _load_families(family_review_path)
    tier_one_pool = {key for key, row in families.items() if tier_one_mechanical(row)}
    tier_two_pool = {key for key, row in families.items() if tier_two_mechanical(row)}
    singles, milestones = _read_proposal_candidates(
        evidence_path, tier_one_pool, tier_two_pool
    )

    assignments = {}
    proposal_candidates = {}
    for identity, family in families.items():
        tickers = event_tickers(family)
        event = events.get(tickers[0], {}) if len(tickers) == 1 else {}
        assignment = assign_tier(
            family,
            event,
            single_candidate=singles.get(identity),
            milestone_candidate=milestones.get(identity),
        )
        assignments[identity] = assignment
        if assignment.tier == "tier_1":
            proposal_candidates[identity] = singles[identity]
        elif assignment.tier == "tier_2":
            proposal_candidates[identity] = milestones[identity]

    tier_ids = {
        tier: {
            key for key, assignment in assignments.items() if assignment.tier == tier
        }
        for tier in ("tier_1", "tier_2", "tier_3")
    }
    if set().union(*tier_ids.values()) != set(families):
        raise ValueError("proposed tiers do not partition the family universe")

    sample_metadata = {}
    for tier, identities in tier_ids.items():
        strata = {
            identity: _tier_stratum(
                assignments[identity],
                families[identity],
                proposal_candidates.get(identity),
            )
            for identity in identities
        }
        selected = stratified_sample(
            identities,
            tier=tier,
            strata=strata,
            sample_size=audit_per_tier,
            seed=seed,
        )
        for identity, stratum, population, sample_count, weight in selected:
            sample_metadata[identity] = {
                "audit_stratum": stratum,
                "stratum_family_count": population,
                "stratum_sample_count": sample_count,
                "sampling_weight": f"{weight:.8f}",
            }

    sampled_candidates: dict[tuple[str, str], list[dict[str, Any]]] = {
        key: [] for key in sample_metadata
    }
    with _open_csv(evidence_path) as handle:
        for row in csv.DictReader(handle):
            identity = _candidate_identity(row)
            if identity in sampled_candidates:
                sampled_candidates[identity].append(safe_candidate_projection(row))
    for identity, candidates in sampled_candidates.items():
        candidates.sort(key=lambda row: row["candidate_id"])
        if len(candidates) != integer(families[identity].get("candidate_count")):
            raise ValueError(
                f"audit packet candidate coverage mismatch for {identity!r}"
            )

    packet_rows = []
    for sequence, identity in enumerate(
        sorted(sample_metadata, key=lambda key: (assignments[key].tier, key)), start=1
    ):
        family = families[identity]
        assignment = assignments[identity]
        tickers = event_tickers(family)
        event = events.get(tickers[0], {}) if len(tickers) == 1 else {}
        proposed = proposal_candidates.get(identity, {})
        metadata = sample_metadata[identity]
        packet_rows.append(
            {
                "audit_id": f"P10E-{sequence:04d}",
                "proposed_tier": assignment.tier,
                "proposed_rule": assignment.proposed_rule,
                "tier_reason": assignment.reason,
                "proposed_timing_structure": assignment.proposed_timing_structure,
                "semantic_agreement": assignment.semantic_agreement,
                **metadata,
                "family_id": identity[0],
                "family_id_source": identity[1],
                "category": str(family.get("category") or "[uncategorized]"),
                "event_ticker": "|".join(tickers),
                "series_ticker": str(event.get("series_ticker") or ""),
                "family_title": str(family.get("representative_title") or ""),
                "event_title": str(event.get("title") or ""),
                "event_sub_title": str(event.get("sub_title") or ""),
                "market_count": integer(family.get("market_count")),
                "candidate_count": integer(family.get("candidate_count")),
                "unique_exact_time_count": len(distinct_exact_times(family)),
                "source_combination": source_combination(family),
                "evidence_pattern": evidence_pattern(family),
                "proposed_candidate_id": str(proposed.get("candidate_id") or ""),
                "proposed_candidate_time": str(
                    proposed.get("candidate_time_utc")
                    or proposed.get("candidate_date")
                    or ""
                ),
                "proposed_candidate_source_type": str(
                    proposed.get("candidate_source_type") or ""
                ),
                "proposed_verified_anchor_source": str(
                    proposed.get("potential_verified_anchor_source") or ""
                ),
                "analysis_window_status": str(
                    proposed.get("analysis_window_status") or "not_applicable"
                ),
                "candidates_json": _canonical_json(sampled_candidates[identity]),
                "reviewer_instruction": (
                    "Review outcome-blind evidence; record a decision only in the exact-schema template."
                ),
            }
        )

    validate_research_feature_columns(PATTERN_FIELDS)
    validate_research_feature_columns(PACKET_FIELDS)
    validate_research_feature_columns(DECISION_TEMPLATE_FIELDS)
    pattern_rows = _pattern_rows(families)
    high_level_patterns = Counter(evidence_pattern(row) for row in families.values())
    source_combinations = Counter(source_combination(row) for row in families.values())
    report = {
        "schema_version": DESIGN_SCHEMA_VERSION,
        "study_rules_fingerprint": rules.fingerprint,
        "approval_gate": "required_before_any_deterministic_verification_rule",
        "anchors_verified": 0,
        "outcomes_merged": False,
        "horizon_prices_built": False,
        "network_requests": 0,
        "input_hashes": input_hashes,
        "family_count": len(families),
        "candidate_evidence_patterns": dict(sorted(high_level_patterns.items())),
        "source_combination_counts": dict(sorted(source_combinations.items())),
        "tier_counts": {tier: len(ids) for tier, ids in tier_ids.items()},
        "tier_diagnostics": {
            tier: _assignment_statistics(
                ids, families, assignments, proposal_candidates
            )
            for tier, ids in tier_ids.items()
        },
        "proposed_rules": {
            "PR1_FIXED_CLOCK_SINGLE_EXACT": {
                "status": "proposed_not_approved",
                "tier": "tier_1",
                "requirements": [
                    "one exact candidate row and one unique exact time",
                    "no conflicting exact times or multiple event tickers",
                    "allowed frozen candidate source",
                    "existing outcome-blind timing heuristic proposes fixed_clock",
                    "audit approval required before any promotion",
                ],
            },
            "PR2_SCHEDULED_START_SINGLE_MILESTONE": {
                "status": "proposed_not_approved",
                "tier": "tier_2",
                "requirements": [
                    "one unique exact official milestone-start candidate",
                    "no conflicting exact times or multiple event tickers",
                    "Sports category",
                    "conservative event-to-milestone and family-context title agreement",
                    "no endogenous-subevent, scheduled-window, or deadline-window timing flag",
                    "audit approval required before any promotion",
                ],
            },
        },
        "audit_design": {
            "seed": seed,
            "sample_per_tier": audit_per_tier,
            "sample_family_count": len(packet_rows),
            "sampling_method": "deterministic_stratified_sha256_with_inverse_weights",
            "approval_rate_status": "not_observed_pending_review",
            "disagreement_rate_status": "not_observed_pending_independent_double_review",
            "worst_case_95pct_approval_margin_percentage_points": round(
                1.96 * (0.25 / audit_per_tier) ** 0.5 * 100, 2
            ),
            "recommended_double_review_per_tier": 50,
            "worst_case_95pct_disagreement_margin_percentage_points": round(
                1.96 * (0.25 / 50) ** 0.5 * 100, 2
            ),
        },
        "manual_review_burden": {
            "tier_1_minutes_per_family": 2,
            "tier_2_minutes_per_family": 3,
            "tier_3_minutes_per_family": 6,
            "single_review_hours": round(audit_per_tier * (2 + 3 + 6) / 60, 2),
            "additional_double_review_hours": round(50 * (2 + 3 + 6) / 60, 2),
            "total_recommended_reviewer_hours": round(
                (audit_per_tier + 50) * (2 + 3 + 6) / 60, 2
            ),
        },
        "primary_sample_plan": {
            "timing_structures": ["fixed_clock", "scheduled_event_start"],
            "horizon_hours": 1,
            "maximum_price_staleness_minutes": 15,
            "inside_window_proposed_family_count": sum(
                1
                for key, candidate in proposal_candidates.items()
                if assignments[key].tier in {"tier_1", "tier_2"}
                and candidate.get("analysis_window_status") == "inside_analysis_window"
            ),
            "price_matching_attrition_status": "not_identified_until_later_price-only stage",
            "price_match_scenarios": {
                rate: round(
                    sum(
                        1
                        for key, candidate in proposal_candidates.items()
                        if assignments[key].tier in {"tier_1", "tier_2"}
                        and candidate.get("analysis_window_status")
                        == "inside_analysis_window"
                    )
                    * fraction
                )
                for rate, fraction in (
                    ("90_percent", 0.9),
                    ("75_percent", 0.75),
                    ("50_percent", 0.5),
                )
            },
            "recommended_minimum_price_matched_pilot_families": 2000,
            "recommended_major_category_targets": {
                "Crypto": 500,
                "Climate and Weather": 500,
                "Financials": 500,
                "Sports": 500,
            },
            "interpretation": "diagnostic preliminary estimate, not confirmatory inference",
        },
    }

    output_root = output_root.resolve()
    guard_root = guard_root.resolve()
    output_root.parent.mkdir(parents=True, exist_ok=True)
    temp_root = output_root.parent / f".{output_root.name}.work-{uuid.uuid4().hex}"
    temp_root.mkdir()
    try:
        _write_csv(temp_root / REQUIRED_OUTPUTS[0], pattern_rows, PATTERN_FIELDS)
        _write_csv(temp_root / REQUIRED_OUTPUTS[1], packet_rows, PACKET_FIELDS)
        decision_rows = [
            {
                "family_id": row["family_id"],
                "family_id_source": row["family_id_source"],
                "verification_status": "needs_review",
                "verified_anchor_time": "",
                "verified_anchor_source": "",
                "timing_structure": "",
                "evidence_reference": "",
                "review_note": "",
            }
            for row in packet_rows
        ]
        _write_csv(
            temp_root / REQUIRED_OUTPUTS[3], decision_rows, DECISION_TEMPLATE_FIELDS
        )
        (temp_root / REQUIRED_OUTPUTS[2]).write_text(
            _packet_markdown(packet_rows, report), encoding="utf-8"
        )
        report["output_hashes"] = {
            name: _sha256(temp_root / name) for name in REQUIRED_OUTPUTS[:-1]
        }
        report["output_bytes"] = {
            name: (temp_root / name).stat().st_size for name in REQUIRED_OUTPUTS[:-1]
        }
        (temp_root / REQUIRED_OUTPUTS[4]).write_text(
            json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
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
                    "existing Phase 10E design conflicts with deterministic rerun"
                )
            shutil.rmtree(temp_root)
        else:
            os.replace(temp_root, output_root)
        if tuple(sorted(path.name for path in output_root.iterdir())) != tuple(
            sorted(REQUIRED_OUTPUTS)
        ):
            raise ValueError("published Phase 10E design has an invalid artifact set")
        for name in REQUIRED_OUTPUTS:
            if _sha256(output_root / name) != publication_hashes[name]:
                raise ValueError("post-publication hash validation failed")
    except Exception:
        if temp_root.exists():
            shutil.rmtree(temp_root)
        raise
    print(json.dumps(report, sort_keys=True))
    return report


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--family-review", required=True, type=Path)
    parser.add_argument("--anchor-evidence", required=True, type=Path)
    parser.add_argument("--event-metadata", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--guard-root", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--audit-per-tier", type=int, default=150)
    parser.add_argument("--seed", default=AUDIT_SEED)
    parser.add_argument("--expected-family-sha256")
    parser.add_argument("--expected-evidence-sha256")
    parser.add_argument("--expected-event-sha256")
    parser.add_argument("--max-generated-bytes", type=int, default=5 * 1024**3)
    parser.add_argument("--min-free-bytes", type=int, default=80 * 1024**3)
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        run(
            args.family_review,
            args.anchor_evidence,
            args.event_metadata,
            args.output_root,
            config_path=args.config,
            guard_root=args.guard_root,
            audit_per_tier=args.audit_per_tier,
            seed=args.seed,
            expected_family_sha256=args.expected_family_sha256,
            expected_evidence_sha256=args.expected_evidence_sha256,
            expected_event_sha256=args.expected_event_sha256,
            max_generated_bytes=args.max_generated_bytes,
            min_free_bytes=args.min_free_bytes,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Phase 10E design failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
