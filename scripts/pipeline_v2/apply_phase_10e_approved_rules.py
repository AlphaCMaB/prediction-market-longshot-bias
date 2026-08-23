"""Apply explicitly approved Phase 10E rules to the outcome-blind universe.

The command is offline, hash-pinned, atomic, compressed, and deterministic.
It produces anchor decisions only; it never reads outcomes or prices and never
tests horizon availability.
"""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any, Iterable, Mapping

import scripts.pipeline_v2.phase_10e_approved_rules as approved_rules_module

from scripts.pipeline_v2.apply_anchor_verification import (
    DECISION_FIELDS,
    validate_decisions,
)
from scripts.pipeline_v2.build_phase_10e_verification_design import (
    _load_events,
    _load_families,
    _read_proposal_candidates,
)
from scripts.pipeline_v2.kalshi_metadata_cache import StorageBudget
from scripts.pipeline_v2.phase_10e_approved_rules import (
    APPROVED_RULES,
    PR1,
    PR2,
    RULE_SCHEMA_VERSION,
    RULE_SPECIFICATION,
    RULE_SPECIFICATION_SHA256,
    classify_pr1,
    classify_pr2,
)
from scripts.pipeline_v2.phase_10e_verification_design import (
    assign_tier,
    event_tickers,
    tier_one_mechanical,
    tier_two_mechanical,
)
from scripts.pipeline_v2.study_rules import (
    load_study_rules,
    validate_research_feature_columns,
)


OUTPUT_FILES = (
    "phase_10e_applied_verification_decisions.csv.gz",
    "phase_10e_verified_anchors.csv.gz",
    "phase_10e_rule_exclusions.csv.gz",
    "phase_10e_rule_application_report.json",
    "phase_10e_rule_provenance.json",
)
VERIFIED_FIELDS = (
    "family_id",
    "family_id_source",
    "rule",
    "category",
    "anchor_month",
    "verified_anchor_time",
    "verified_anchor_source",
    "timing_structure",
    "evidence_reference",
    "candidate_id",
)
EXCLUSION_FIELDS = (
    "family_id",
    "family_id_source",
    "proposed_rule",
    "category",
    "candidate_month",
    "primary_exclusion_reason",
    "all_exclusion_reasons",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _logical_bytes(path: Path) -> int:
    return (
        sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
        if path.exists()
        else 0
    )


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


class _DeterministicGzipCsv:
    def __init__(self, path: Path, fields: Iterable[str]):
        self.raw = path.open("wb")
        self.gzip = gzip.GzipFile(
            fileobj=self.raw, mode="wb", filename="", mtime=0, compresslevel=9
        )
        self.text = io.TextIOWrapper(self.gzip, encoding="utf-8", newline="")
        self.fields = tuple(fields)
        self.writer = csv.DictWriter(
            self.text, fieldnames=self.fields, lineterminator="\n"
        )
        self.writer.writeheader()

    def writerow(self, row: Mapping[str, Any]) -> None:
        self.writer.writerow({field: row.get(field, "") for field in self.fields})

    def close(self) -> None:
        self.text.flush()
        self.gzip.close()
        self.raw.close()

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, tb):
        self.close()


def _verify_hash(path: Path, expected: str | None, label: str) -> str:
    actual = _sha256(path)
    if expected and actual != expected:
        raise ValueError(
            f"{label} SHA-256 mismatch: expected={expected} actual={actual}"
        )
    return actual


def _compare_existing(existing: Path, derived: Path) -> bool:
    if not existing.is_dir():
        return False
    if sorted(p.name for p in existing.iterdir() if p.is_file()) != sorted(
        OUTPUT_FILES
    ):
        return False
    return all(
        _sha256(existing / name) == _sha256(derived / name) for name in OUTPUT_FILES
    )


def _month(candidate: Mapping[str, Any] | None) -> str:
    timestamp = str((candidate or {}).get("candidate_time_utc") or "")
    return timestamp[:7] if len(timestamp) >= 7 else "[none]"


def estimate_storage(family_count: int, potential_rule_count: int) -> dict[str, int]:
    # Deliberately conservative compressed estimates calibrated above the
    # existing Phase 10E packet row widths.  Publication also needs one atomic
    # temporary copy, so the peak incremental amount is doubled.
    decisions = max(2_000_000, family_count * 90)
    verified = max(1_000_000, potential_rule_count * 100)
    exclusions = max(1_000_000, potential_rule_count * 55)
    reports = 2 * 1024**2
    publication = decisions + verified + exclusions + reports
    return {
        "projected_decisions_bytes": decisions,
        "projected_verified_anchors_bytes": verified,
        "projected_exclusions_bytes": exclusions,
        "projected_reports_bytes": reports,
        "projected_publication_bytes": publication,
        "projected_atomic_peak_incremental_bytes": publication,
    }


def _preserve_publication_snapshot(
    preflight: dict[str, Any], output_root: Path, *, rules_fingerprint: str
) -> dict[str, Any]:
    """Keep volatile disk accounting fixed after first atomic publication."""
    report_path = output_root / OUTPUT_FILES[3]
    if not report_path.exists():
        return preflight
    existing = json.loads(report_path.read_text(encoding="utf-8"))
    if existing.get("input_hashes") != preflight.get("input_hashes"):
        raise ValueError("existing rule report has different input identities")
    if existing.get("rule_specification_sha256") != RULE_SPECIFICATION_SHA256:
        raise ValueError("existing rule report has a different rule specification")
    if existing.get("study_rules_fingerprint") != rules_fingerprint:
        raise ValueError("existing rule report has different frozen StudyRules")
    output = dict(preflight)
    for key in ("storage_before", "projected_namespace_bytes", "projected_free_bytes"):
        output[key] = existing[key]
    return output


def run(
    family_review_path: Path,
    evidence_path: Path,
    event_metadata_path: Path,
    approval_path: Path,
    independent_report_path: Path,
    output_root: Path,
    *,
    config_path: Path,
    guard_root: Path,
    expected_family_sha256: str | None = None,
    expected_evidence_sha256: str | None = None,
    expected_event_sha256: str | None = None,
    expected_approval_sha256: str | None = None,
    expected_independent_report_sha256: str | None = None,
    max_generated_bytes: int = 5 * 1024**3,
    min_free_bytes: int = 80 * 1024**3,
    preflight_only: bool = False,
) -> dict[str, Any]:
    rules = load_study_rules(config_path)
    input_hashes = {
        "family_review": _verify_hash(
            family_review_path, expected_family_sha256, "family review"
        ),
        "anchor_evidence": _verify_hash(
            evidence_path, expected_evidence_sha256, "anchor evidence"
        ),
        "event_metadata": _verify_hash(
            event_metadata_path, expected_event_sha256, "event metadata"
        ),
        "explicit_approval": _verify_hash(
            approval_path, expected_approval_sha256, "explicit approval"
        ),
        "independent_human_report": _verify_hash(
            independent_report_path,
            expected_independent_report_sha256,
            "independent human report",
        ),
    }
    report_source = json.loads(independent_report_path.read_text(encoding="utf-8"))
    if (
        report_source.get("anchors_verified") != 0
        or report_source.get("outcomes_accessed") is not False
    ):
        raise ValueError(
            "independent report does not preserve the outcome-blind pre-application state"
        )

    events = _load_events(event_metadata_path)
    families = _load_families(family_review_path)
    tier_one_pool = {
        identity for identity, row in families.items() if tier_one_mechanical(row)
    }
    tier_two_pool = {
        identity for identity, row in families.items() if tier_two_mechanical(row)
    }
    estimate = estimate_storage(len(families), len(tier_one_pool | tier_two_pool))
    budget = StorageBudget(
        guard_root, max_bytes=max_generated_bytes, min_free_bytes=min_free_bytes
    )
    before = budget.snapshot()
    budget.check_additional(estimate["projected_atomic_peak_incremental_bytes"])
    preflight = {
        "schema_version": RULE_SCHEMA_VERSION,
        "mode": "preflight" if preflight_only else "production",
        "family_count": len(families),
        "event_count": len(events),
        "tier_one_mechanical_count": len(tier_one_pool),
        "tier_two_mechanical_count": len(tier_two_pool),
        "storage_before": before,
        "storage_estimate": estimate,
        "projected_namespace_bytes": before["used_bytes"]
        + estimate["projected_publication_bytes"],
        "projected_free_bytes": before["free_bytes"]
        - estimate["projected_publication_bytes"],
        "fits_guards": True,
        "study_rules_fingerprint": rules.fingerprint,
        "rule_specification_sha256": RULE_SPECIFICATION_SHA256,
        "input_hashes": input_hashes,
    }
    if preflight_only:
        print(json.dumps(preflight, sort_keys=True))
        return preflight
    preflight = _preserve_publication_snapshot(
        preflight, output_root, rules_fingerprint=rules.fingerprint
    )

    singles, milestones = _read_proposal_candidates(
        evidence_path, tier_one_pool, tier_two_pool
    )
    output_root.parent.mkdir(parents=True, exist_ok=True)
    temp_root = Path(
        tempfile.mkdtemp(prefix=f".{output_root.name}.", dir=output_root.parent)
    )
    status_counts = Counter()
    rule_counts = Counter()
    tier_counts = Counter()
    exclusion_counts: dict[str, Counter] = {PR1: Counter(), PR2: Counter()}
    category_counts: dict[str, Counter] = defaultdict(Counter)
    month_counts: dict[str, Counter] = defaultdict(Counter)
    identities = set()
    verified_identities = set()
    try:
        with (
            _DeterministicGzipCsv(
                temp_root / OUTPUT_FILES[0], DECISION_FIELDS
            ) as decisions,
            _DeterministicGzipCsv(
                temp_root / OUTPUT_FILES[1], VERIFIED_FIELDS
            ) as verified,
            _DeterministicGzipCsv(
                temp_root / OUTPUT_FILES[2], EXCLUSION_FIELDS
            ) as exclusions,
        ):
            for identity in sorted(families):
                family = families[identity]
                tickers = event_tickers(family)
                event = events.get(tickers[0], {}) if len(tickers) == 1 else {}
                assignment = assign_tier(
                    family,
                    event,
                    single_candidate=singles.get(identity),
                    milestone_candidate=milestones.get(identity),
                )
                tier_counts[assignment.tier] += 1
                candidate = (
                    singles.get(identity)
                    if assignment.tier == "tier_1"
                    else milestones.get(identity)
                )
                rule_decision = None
                if assignment.tier == "tier_1" and candidate is not None:
                    rule_decision = classify_pr1(family, event, candidate)
                elif assignment.tier == "tier_2" and candidate is not None:
                    rule_decision = classify_pr2(family, event, candidate)
                approved = bool(rule_decision and rule_decision.approved)
                status = "verified_automatic" if approved else "needs_review"
                row = {
                    "family_id": identity[0],
                    "family_id_source": identity[1],
                    "verification_status": status,
                    "verified_anchor_time": (
                        candidate.get("candidate_time_utc", "") if approved else ""
                    ),
                    "verified_anchor_source": (
                        candidate.get("potential_verified_anchor_source", "")
                        if approved
                        else ""
                    ),
                    "timing_structure": (
                        rule_decision.timing_structure if approved else ""
                    ),
                    "evidence_reference": (
                        candidate.get("evidence_reference", "") if approved else ""
                    ),
                    "review_note": (
                        f"Applied {rule_decision.rule} under explicit Phase 10E approval."
                        if approved
                        else "Not covered by an approved deterministic rule; remains outcome-blind needs_review."
                    ),
                }
                decisions.writerow(row)
                identities.add(identity)
                status_counts[status] += 1
                category = str(family.get("category") or "[uncategorized]")
                month = _month(candidate)
                category_counts[category][status] += 1
                month_counts[month][status] += 1
                if approved:
                    verified_identities.add(identity)
                    rule_counts[rule_decision.rule] += 1
                    category_counts[category][rule_decision.rule] += 1
                    month_counts[month][rule_decision.rule] += 1
                    verified.writerow(
                        {
                            "family_id": identity[0],
                            "family_id_source": identity[1],
                            "rule": rule_decision.rule,
                            "category": category,
                            "anchor_month": month,
                            "verified_anchor_time": candidate.get(
                                "candidate_time_utc", ""
                            ),
                            "verified_anchor_source": candidate.get(
                                "potential_verified_anchor_source", ""
                            ),
                            "timing_structure": rule_decision.timing_structure,
                            "evidence_reference": candidate.get(
                                "evidence_reference", ""
                            ),
                            "candidate_id": candidate.get("candidate_id", ""),
                        }
                    )
                elif rule_decision:
                    for reason in rule_decision.reasons:
                        exclusion_counts[rule_decision.rule][reason] += 1
                    exclusions.writerow(
                        {
                            "family_id": identity[0],
                            "family_id_source": identity[1],
                            "proposed_rule": rule_decision.rule,
                            "category": category,
                            "candidate_month": month,
                            "primary_exclusion_reason": rule_decision.primary_reason,
                            "all_exclusion_reasons": "|".join(rule_decision.reasons),
                        }
                    )
                if len(identities) % 25000 == 0:
                    budget.check_additional(_logical_bytes(temp_root))

        if identities != set(families) or len(identities) != 427090:
            raise ValueError(
                "applied decisions do not exactly cover the 427,090-family universe"
            )
        # Re-read through the standard schema validator; this also proves that
        # no verified row has a blank/invalid anchor, source, or timing value.
        with gzip.open(
            temp_root / OUTPUT_FILES[0], "rt", encoding="utf-8", newline=""
        ) as handle:
            reader = csv.DictReader(handle)
            decision_rows = list(reader)
            validate_decisions(decision_rows, reader.fieldnames or ())
        validate_research_feature_columns(DECISION_FIELDS)
        validate_research_feature_columns(VERIFIED_FIELDS)
        validate_research_feature_columns(EXCLUSION_FIELDS)
        if any(row["verification_status"] == "rejected" for row in decision_rows):
            raise ValueError("rule application retrospectively rejected a family")
        if any(
            not row["verified_anchor_time"] or not row["verified_anchor_source"]
            for row in decision_rows
            if row["verification_status"] == "verified_automatic"
        ):
            raise ValueError("verified decision lacks a complete anchor")
        report = {
            **preflight,
            "mode": "completed",
            "outcome_fields_accessed": 0,
            "prices_accessed": False,
            "horizon_availability_tested": False,
            "rules_applied": list(APPROVED_RULES),
            "family_count": len(identities),
            "verified_family_count": len(verified_identities),
            "status_counts": dict(sorted(status_counts.items())),
            "tier_counts": dict(sorted(tier_counts.items())),
            "rule_verified_counts": dict(sorted(rule_counts.items())),
            "rule_exclusion_counts": {
                rule: dict(sorted(counts.items()))
                for rule, counts in exclusion_counts.items()
            },
            "category_counts": {
                key: dict(sorted(value.items()))
                for key, value in sorted(category_counts.items())
            },
            "month_counts": {
                key: dict(sorted(value.items()))
                for key, value in sorted(month_counts.items())
            },
        }
        provenance = {
            "schema_version": RULE_SCHEMA_VERSION,
            "input_hashes": input_hashes,
            "study_rules_fingerprint": rules.fingerprint,
            "rule_specification": RULE_SPECIFICATION,
            "rule_specification_sha256": RULE_SPECIFICATION_SHA256,
            "implementation_hashes": {
                "application_module_sha256": _sha256(Path(__file__)),
                "classifier_module_sha256": _sha256(
                    Path(approved_rules_module.__file__)
                ),
            },
            "production_verification_status_before_application": "needs_review",
            "outcome_blind": True,
            "network_requests": 0,
        }
        (temp_root / OUTPUT_FILES[4]).write_text(
            json.dumps(provenance, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        report["output_hashes"] = {
            name: _sha256(temp_root / name)
            for name in OUTPUT_FILES[:3] + (OUTPUT_FILES[4],)
        }
        report["output_bytes"] = {
            name: (temp_root / name).stat().st_size
            for name in OUTPUT_FILES[:3] + (OUTPUT_FILES[4],)
        }
        (temp_root / OUTPUT_FILES[3]).write_text(
            json.dumps(report, indent=2, sort_keys=True, ensure_ascii=False) + "\n",
            encoding="utf-8",
        )
        budget.check_additional(_logical_bytes(temp_root))
        publication_hashes = {name: _sha256(temp_root / name) for name in OUTPUT_FILES}
        if output_root.exists():
            if not _compare_existing(output_root, temp_root):
                raise ValueError(
                    "existing Phase 10E rule application conflicts with deterministic rerun"
                )
            shutil.rmtree(temp_root)
        else:
            os.replace(temp_root, output_root)
        if any(
            _sha256(output_root / name) != publication_hashes[name]
            for name in OUTPUT_FILES
        ):
            raise ValueError("post-publication hash validation failed")
        print(json.dumps(report, sort_keys=True))
        return report
    except Exception:
        if temp_root.exists():
            shutil.rmtree(temp_root)
        raise


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--family-review", required=True, type=Path)
    parser.add_argument("--anchor-evidence", required=True, type=Path)
    parser.add_argument("--event-metadata", required=True, type=Path)
    parser.add_argument("--approval", required=True, type=Path)
    parser.add_argument("--independent-report", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--guard-root", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--expected-family-sha256")
    parser.add_argument("--expected-evidence-sha256")
    parser.add_argument("--expected-event-sha256")
    parser.add_argument("--expected-approval-sha256")
    parser.add_argument("--expected-independent-report-sha256")
    parser.add_argument("--max-generated-bytes", type=int, default=5 * 1024**3)
    parser.add_argument("--min-free-bytes", type=int, default=80 * 1024**3)
    parser.add_argument("--preflight-only", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        run(
            args.family_review,
            args.anchor_evidence,
            args.event_metadata,
            args.approval,
            args.independent_report,
            args.output_root,
            config_path=args.config,
            guard_root=args.guard_root,
            expected_family_sha256=args.expected_family_sha256,
            expected_evidence_sha256=args.expected_evidence_sha256,
            expected_event_sha256=args.expected_event_sha256,
            expected_approval_sha256=args.expected_approval_sha256,
            expected_independent_report_sha256=args.expected_independent_report_sha256,
            max_generated_bytes=args.max_generated_bytes,
            min_free_bytes=args.min_free_bytes,
            preflight_only=args.preflight_only,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Phase 10E rule application failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
