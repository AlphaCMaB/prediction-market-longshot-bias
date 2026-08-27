"""Run the deterministic final pre-outcome audit of the frozen Phase 10F-E sample."""

from __future__ import annotations

import argparse
from collections import Counter
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

from scripts.pipeline_v2.kalshi_metadata_cache import StorageBudget, canonical_json
from scripts.pipeline_v2.phase_10f_e import sample_metrics
from scripts.pipeline_v2.phase_10f_final_audit import (
    FinalAuditError,
    mutually_exclusive_attrition,
    support_diagnostics,
    validate_analysis_projection,
    validate_sampling_design,
    validate_temporal_and_price_rules,
    weighted_observability_balance,
)
from scripts.pipeline_v2.run_phase_10f_b2 import _publish, _sha256
from scripts.pipeline_v2.run_phase_10f_e import (
    ANALYSIS_FIELDS,
    CONTRACT_MANIFEST_SHA256,
    NORMALIZED_FIELDS,
    SAMPLE_COMMIT_IDENTITY,
    SAMPLE_COMMIT_SHA256,
    STUDY_RULES_FINGERPRINT,
    _load_complete_partition,
    _load_frozen_sample,
    _partition_identity,
    _read_gzip_csv,
)
from scripts.pipeline_v2.study_rules import (
    load_study_rules,
    validate_research_feature_columns,
)


SCHEMA_VERSION = "phase-10f-final-pre-outcome-audit-v1"
PHASE_E_COMMIT_IDENTITY = (
    "79e022b7d9d359b484632e82671ef0095eba040687a21bc9a34a9bb947cf08de"
)
PHASE_E_COMMIT_SHA256 = (
    "f9628c1e6dc55708b2c72b869c353b57af3c60a2de70abc5dcb0124ac65ea0b2"
)
EXPECTED_NORMALIZED_SHA256 = (
    "11f9ce8d3ed32ad9c3974a7f162c08b414e3aa5b87af80974283fd09175ef0d8"
)
EXPECTED_PRIMARY_SHA256 = (
    "a95a6f3c7f55d5f297b2dfec29dcea591b7f8fb49e52000650ef5b8e22fd4b86"
)
EXPECTED_ANALYSIS_HASHES = {
    "primary_midpoint_15m": EXPECTED_PRIMARY_SHA256,
    "robustness_midpoint_60m": (
        "93f2d6cf67c8d9c8240672512b1d6a9d706f7a4997621fea01f0963eafc54197"
    ),
    "robustness_trade_close_15m": (
        "39f185734d92b0e6550edbae3ba9c8ca986c0d9e5b33fc75fee6d2afeda23b77"
    ),
    "robustness_trade_close_60m": (
        "59443fb614502e9515617d78b860d65b40177658144cbb2619b3aa4220eeef71"
    ),
}
SAMPLE_DEFINITIONS = {
    "primary_midpoint_15m": "midpoint_within_15m",
    "robustness_midpoint_60m": "midpoint_within_60m",
    "robustness_trade_close_15m": "trade_within_15m",
    "robustness_trade_close_60m": "trade_within_60m",
}
MAX_AUDIT_BYTES = 1024**2


class AuditRunnerError(RuntimeError):
    pass


def _json_bytes(value: Any) -> bytes:
    return canonical_json(value) + b"\n"


def _verify(path: Path, expected: str, label: str) -> str:
    actual = _sha256(path)
    if actual != expected:
        raise AuditRunnerError(
            f"{label} SHA-256 mismatch: expected={expected} actual={actual}"
        )
    return actual


def _identity(document: Mapping[str, Any], label: str) -> str:
    payload = dict(document)
    identity = str(payload.pop("commit_identity", ""))
    if hashlib.sha256(_json_bytes(payload)).hexdigest() != identity:
        raise AuditRunnerError(f"{label} commit identity is invalid")
    return identity


def _load_and_validate_phase_e(args: argparse.Namespace) -> dict[str, Any]:
    _verify(args.phase_e_commit, PHASE_E_COMMIT_SHA256, "Phase 10F-E commit")
    commit = json.loads(args.phase_e_commit.read_text())
    if _identity(commit, "Phase 10F-E") != PHASE_E_COMMIT_IDENTITY:
        raise AuditRunnerError("Phase 10F-E commit identity changed")
    if (
        not commit.get("complete")
        or not commit.get("primary_threshold_passed")
        or commit.get("sample_commit_identity") != SAMPLE_COMMIT_IDENTITY
        or int(commit.get("outcome_fields_accessed", -1)) != 0
    ):
        raise AuditRunnerError("Phase 10F-E final gate state changed")
    expected_paths = {item["path"] for item in commit["artifacts"]}
    if len(expected_paths) != len(commit["artifacts"]):
        raise AuditRunnerError("Phase 10F-E commit contains duplicate artifact paths")
    for artifact in commit["artifacts"]:
        path = args.phase_e_root / artifact["path"]
        if _sha256(path) != artifact["sha256"] or path.stat().st_size != int(
            artifact["bytes"]
        ):
            raise AuditRunnerError("Phase 10F-E artifact identity changed")
    return commit


def _validate_partitions(
    root: Path,
    frozen_rows: Sequence[Mapping[str, Any]],
    normalized: list[dict[str, Any]],
) -> dict[str, Any]:
    rebuilt: list[dict[str, Any]] = []
    request_rows: list[dict[str, Any]] = []
    partitions = 0
    for index, start in enumerate(range(0, len(frozen_rows), 100), 1):
        rows = frozen_rows[start : start + 100]
        loaded = _load_complete_partition(root, index, _partition_identity(index, rows))
        if loaded is None:
            raise AuditRunnerError(f"partition {index} is incomplete")
        partition_rows, commits, partition_commit = loaded
        if int(partition_commit["partition_index"]) != index:
            raise AuditRunnerError("partition order changed")
        rebuilt.extend(partition_rows)
        request_rows.extend(commits)
        partitions += 1
    if rebuilt != normalized:
        raise AuditRunnerError("final normalized prices differ from partition rebuild")
    if len(request_rows) != len(frozen_rows):
        raise AuditRunnerError("partition request count changed")
    return {
        "passed": True,
        "partitions_validated": partitions,
        "normalized_rows_rebuilt": len(rebuilt),
        "sample_request_commits_rebuilt": len(request_rows),
    }


def _request_file_paths(root: Path, request: Mapping[str, Any]) -> tuple[Path, Path]:
    partition = request.get("partition_index")
    base = (
        root / "partitions" / f"partition_{int(partition):04d}"
        if partition is not None
        else root / "controls"
    )
    request_id = str(request["request_id"])
    return (
        base / str(request["raw_path"]),
        base / "request_commits" / f"request_{request_id}.json",
    )


def _validate_request_manifest(
    root: Path,
    manifest_path: Path,
    normalized: Sequence[Mapping[str, Any]],
) -> dict[str, Any]:
    commits = [json.loads(line) for line in manifest_path.read_text().splitlines()]
    request_ids = [str(commit["request_id"]) for commit in commits]
    if request_ids != sorted(request_ids) or len(request_ids) != len(set(request_ids)):
        raise AuditRunnerError("request manifest is unsorted or contains duplicates")
    purposes = Counter(str(commit["request"]["purpose"]) for commit in commits)
    if purposes != Counter(
        {
            "sample_price_window": 11_573,
            "routing_cutoff": 1,
            "boundary_end_minus_one": 1,
        }
    ):
        raise AuditRunnerError("request scope changed")
    normalized_ids = {str(row["request_id"]) for row in normalized}
    sample_ids = {
        str(commit["request_id"])
        for commit in commits
        if commit["request"]["purpose"] == "sample_price_window"
    }
    if normalized_ids != sample_ids:
        raise AuditRunnerError("normalized/request-manifest identities differ")

    raw_bytes = 0
    for commit in commits:
        request_id = hashlib.sha256(canonical_json(commit["request"])).hexdigest()[:24]
        if request_id != commit["request_id"]:
            raise AuditRunnerError("request identity hash changed")
        raw_path, commit_path = _request_file_paths(root, commit)
        if _sha256(raw_path) != commit["raw_sha256"]:
            raise AuditRunnerError("immutable raw response hash changed")
        raw_bytes += raw_path.stat().st_size
        stored = json.loads(commit_path.read_text())
        projected = dict(commit)
        projected.pop("partition_index", None)
        if stored != projected:
            raise AuditRunnerError("request commit differs from final manifest")
    return {
        "passed": True,
        "request_commits_validated": len(commits),
        "sample_request_identities": purposes["sample_price_window"],
        "control_request_identities": len(commits) - purposes["sample_price_window"],
        "duplicate_request_ids": 0,
        "compressed_raw_bytes_rehashed": raw_bytes,
    }


def _validate_quarantine(
    normalized: Sequence[Mapping[str, Any]],
    analyses: Mapping[str, Sequence[Mapping[str, Any]]],
) -> dict[str, Any]:
    validate_research_feature_columns(NORMALIZED_FIELDS)
    validate_research_feature_columns(ANALYSIS_FIELDS)
    forbidden_fragments = ("outcome", "result", "settlement")
    inspected = [set(normalized[0]) if normalized else set()]
    inspected.extend(set(rows[0]) for rows in analyses.values() if rows)
    offending = sorted(
        {
            field
            for fields in inspected
            for field in fields
            if any(fragment in field.casefold() for fragment in forbidden_fragments)
        }
    )
    if offending:
        raise AuditRunnerError(
            "research sample contains outcome/post-event columns: "
            + ", ".join(offending)
        )
    return {
        "passed": True,
        "outcome_fields_accessed": 0,
        "outcome_columns_found": 0,
        "settlement_columns_found": 0,
        "network_requests_made": 0,
    }


def _validate_existing_audit(
    args: argparse.Namespace, input_hashes: Mapping[str, str], budget: StorageBudget
) -> dict[str, Any]:
    commit_path = args.output_root / "phase_10f_final_pre_outcome_commit.json"
    commit = json.loads(commit_path.read_text())
    if _identity(commit, "final pre-outcome audit") != commit["commit_identity"]:
        raise AuditRunnerError("final pre-outcome audit identity changed")
    if commit.get("input_hashes") != dict(input_hashes):
        raise AuditRunnerError("final pre-outcome audit inputs changed")
    for artifact in commit["artifacts"]:
        if _sha256(args.output_root / artifact["path"]) != artifact["sha256"]:
            raise AuditRunnerError("final pre-outcome audit artifact changed")
    report = json.loads(
        (args.output_root / "phase_10f_final_pre_outcome_audit.json").read_text()
    )
    return {
        **report,
        "existing_audit_reused": True,
        "audit_commit_identity": commit["commit_identity"],
        "audit_commit_sha256": _sha256(commit_path),
        "storage_now": budget.snapshot(),
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    phase_e = _load_and_validate_phase_e(args)
    _verify(args.sample_commit, SAMPLE_COMMIT_SHA256, "Phase 10F-D commit")
    _verify(args.contract_manifest, CONTRACT_MANIFEST_SHA256, "contract manifest")
    rules = load_study_rules(args.config)
    if rules.fingerprint != STUDY_RULES_FINGERPRINT:
        raise AuditRunnerError("StudyRules fingerprint changed")
    input_hashes = {
        "phase_10f_d_commit": SAMPLE_COMMIT_SHA256,
        "phase_10f_d_contract_manifest": CONTRACT_MANIFEST_SHA256,
        "phase_10f_d_sample_identity": SAMPLE_COMMIT_IDENTITY,
        "phase_10f_e_commit": PHASE_E_COMMIT_SHA256,
        "phase_10f_e_commit_identity": PHASE_E_COMMIT_IDENTITY,
        "final_analysis_plan": _sha256(args.analysis_plan),
        "study_rules_fingerprint": rules.fingerprint,
    }
    budget = StorageBudget(
        args.guard_root,
        max_bytes=args.max_generated_bytes,
        min_free_bytes=args.min_free_bytes,
    )
    budget.check_additional(MAX_AUDIT_BYTES)
    commit_path = args.output_root / "phase_10f_final_pre_outcome_commit.json"
    if commit_path.exists():
        return _validate_existing_audit(args, input_hashes, budget)
    if args.preflight_only:
        return {
            "schema_version": SCHEMA_VERSION,
            "preflight_only": True,
            "passed": True,
            "network_requests_made": 0,
            "outcomes_accessed": 0,
            "maximum_additional_bytes": MAX_AUDIT_BYTES,
            "input_hashes": input_hashes,
            "storage": budget.snapshot(),
        }

    frozen_rows, sample_commit = _load_frozen_sample(
        args.contract_manifest, args.sample_commit
    )
    if _identity(sample_commit, "Phase 10F-D") != SAMPLE_COMMIT_IDENTITY:
        raise AuditRunnerError("Phase 10F-D sample identity changed")
    normalized_path = args.phase_e_root / "phase_10f_e_normalized_prices.csv.gz"
    _verify(normalized_path, EXPECTED_NORMALIZED_SHA256, "normalized prices")
    normalized = _read_gzip_csv(normalized_path, typed=True)
    design = validate_sampling_design(normalized)
    temporal = validate_temporal_and_price_rules(normalized)

    for frozen, observed in zip(frozen_rows, normalized):
        if any(str(observed.get(field)) != str(frozen.get(field)) for field in frozen):
            raise AuditRunnerError(
                "Phase 10F-D sample field changed in normalized prices"
            )

    analyses: dict[str, list[dict[str, Any]]] = {}
    projections: dict[str, Any] = {}
    measures: dict[str, Any] = {}
    for name, flag in SAMPLE_DEFINITIONS.items():
        path = args.phase_e_root / f"phase_10f_e_{name}.csv.gz"
        _verify(path, EXPECTED_ANALYSIS_HASHES[name], name)
        analyses[name] = _read_gzip_csv(path, typed=True)
        projections[name] = validate_analysis_projection(
            normalized,
            analyses[name],
            flag=flag,
            sample_name=name,
            analysis_fields=ANALYSIS_FIELDS,
        )
        measures[name] = sample_metrics(normalized, flag=flag)

    partition_validation = _validate_partitions(
        args.phase_e_root, frozen_rows, normalized
    )
    request_validation = _validate_request_manifest(
        args.phase_e_root,
        args.phase_e_root / "phase_10f_e_raw_request_manifest.jsonl",
        normalized,
    )
    quarantine = _validate_quarantine(normalized, analyses)
    primary = analyses["primary_midpoint_15m"]
    support = support_diagnostics(primary)
    primary_metrics = measures["primary_midpoint_15m"]
    overall_passed = bool(
        primary_metrics["usable_unique_families"] >= 500
        and primary_metrics["family_weighted_ess"] >= 500
    )
    if not overall_passed:
        raise AuditRunnerError("frozen primary sample no longer passes its gate")

    report = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "audit_passed": True,
        "phase_10f_d_sample_identity": SAMPLE_COMMIT_IDENTITY,
        "phase_10f_e_commit_identity": PHASE_E_COMMIT_IDENTITY,
        "input_hashes": input_hashes,
        "phase_10f_e_artifacts_validated": len(phase_e["artifacts"]),
        "sampling_design": design,
        "partition_validation": partition_validation,
        "request_validation": request_validation,
        "temporal_and_price_validation": temporal,
        "analysis_projection_validation": projections,
        "measure_metrics": measures,
        "mutually_exclusive_attrition": mutually_exclusive_attrition(normalized),
        "primary_support_diagnostics": support,
        "primary_observability_balance": weighted_observability_balance(normalized),
        "primary_gate": {
            "required_families": 500,
            "required_family_weighted_ess": 500,
            "observed_families": primary_metrics["usable_unique_families"],
            "observed_family_weighted_ess": primary_metrics["family_weighted_ess"],
            "passed": overall_passed,
        },
        "outcome_quarantine": quarantine,
        "methodology_state": {
            "sample_redrawn": False,
            "inclusion_probabilities_changed": False,
            "weights_changed": False,
            "anchors_changed": False,
            "price_definitions_changed": False,
            "study_rules_changed": False,
            "outcomes_accessed": 0,
            "favorite_longshot_estimates_computed": 0,
        },
        "next_gate": {
            "outcome_access_authorized": False,
            "required_action": (
                "explicitly approve the recorded inferential specification and "
                "release the outcome quarantine"
            ),
        },
    }
    report_path = args.output_root / "phase_10f_final_pre_outcome_audit.json"
    _publish(budget, report_path, _json_bytes(report))
    artifacts = [
        {
            "path": report_path.name,
            "sha256": _sha256(report_path),
            "bytes": report_path.stat().st_size,
        }
    ]
    commit_without_identity = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "audit_passed": True,
        "input_hashes": input_hashes,
        "artifacts": artifacts,
        "outcomes_accessed": 0,
        "outcome_access_authorized": False,
    }
    audit_identity = hashlib.sha256(_json_bytes(commit_without_identity)).hexdigest()
    _publish(
        budget,
        commit_path,
        _json_bytes({**commit_without_identity, "commit_identity": audit_identity}),
    )
    return {
        **report,
        "existing_audit_reused": False,
        "audit_commit_identity": audit_identity,
        "audit_commit_sha256": _sha256(commit_path),
        "output_hashes": {item["path"]: item["sha256"] for item in artifacts},
        "storage_now": budget.snapshot(),
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--phase-e-root",
        type=Path,
        default=Path("data/pipeline_v2/horizon_prices/phase_10f_e"),
    )
    parser.add_argument(
        "--phase-e-commit",
        type=Path,
        default=Path(
            "data/pipeline_v2/horizon_prices/phase_10f_e/phase_10f_e_commit.json"
        ),
    )
    parser.add_argument(
        "--sample-commit",
        type=Path,
        default=Path(
            "data/pipeline_v2/horizon_prices/phase_10f_d/phase_10f_d_commit.json"
        ),
    )
    parser.add_argument(
        "--contract-manifest",
        type=Path,
        default=Path(
            "data/pipeline_v2/horizon_prices/phase_10f_d/phase_10f_d_contract_sampling_manifest.csv.gz"
        ),
    )
    parser.add_argument(
        "--analysis-plan", type=Path, default=Path("PHASE_10F_FINAL_ANALYSIS_PLAN.md")
    )
    parser.add_argument("--config", type=Path, default=Path("configs/pipeline_v2.toml"))
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path(
            "data/pipeline_v2/horizon_prices/phase_10f_e/final_pre_outcome_audit"
        ),
    )
    parser.add_argument("--guard-root", type=Path, default=Path("data/pipeline_v2"))
    parser.add_argument("--max-generated-bytes", type=int, default=5 * 1024**3)
    parser.add_argument("--min-free-bytes", type=int, default=80 * 1024**3)
    parser.add_argument("--preflight-only", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        print(json.dumps(run(parse_args(argv)), sort_keys=True))
        return 0
    except (AuditRunnerError, FinalAuditError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
