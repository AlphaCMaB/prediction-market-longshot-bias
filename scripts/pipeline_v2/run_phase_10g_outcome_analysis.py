"""Release minimal outcomes and run the frozen Phase 10G analysis offline."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import gzip
import hashlib
import io
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

from scripts.pipeline_v2.kalshi_metadata_cache import StorageBudget, canonical_json
from scripts.pipeline_v2.phase_10f_e import kish_ess
from scripts.pipeline_v2.phase_10g_analysis import (
    BOOTSTRAP_REPLICATES,
    BOOTSTRAP_SEED,
    SAMPLE_FLAGS,
    WEIGHT_SYSTEMS,
    OutcomeAnalysisError,
    bootstrap_intervals,
    calibration_bins,
    family_identity,
    weighted_estimate,
)
from scripts.pipeline_v2.run_phase_10f_b2 import _publish, _sha256
from scripts.pipeline_v2.run_phase_10f_e import (
    EXPECTED_CONTRACTS,
    NORMALIZED_FIELDS,
    SAMPLE_COMMIT_IDENTITY,
    STUDY_RULES_FINGERPRINT,
    _read_gzip_csv,
)
from scripts.pipeline_v2.study_rules import load_study_rules


SCHEMA_VERSION = "phase-10g-frozen-outcome-analysis-v3"
FINAL_AUDIT_IDENTITY = (
    "bd14ba156585c4b2ed43c798ea55c977e8496326642edca9748eb703491eab24"
)
FINAL_AUDIT_COMMIT_SHA256 = (
    "6deaaebdd4dd296d90f9651f4e7a43f40472bc07d5d168da4dc5f865f02b3f92"
)
FINAL_AUDIT_REPORT_SHA256 = (
    "8ac3fb4b1de1ede9336306589a7b17aa47df94cd1a8245ef441d6360e7c75a73"
)
FINAL_ANALYSIS_PLAN_SHA256 = (
    "1da09fbe7d8fc14a25109c7ebd1f66969ca61e3f64f3cf32b5703dd5109da73b"
)
PHASE_E_COMMIT_SHA256 = (
    "f9628c1e6dc55708b2c72b869c353b57af3c60a2de70abc5dcb0124ac65ea0b2"
)
NORMALIZED_PRICES_SHA256 = (
    "11f9ce8d3ed32ad9c3974a7f162c08b414e3aa5b87af80974283fd09175ef0d8"
)
QUARANTINED_OUTCOMES_SHA256 = (
    "2114fd25b79627c9c36d716485382548b3812108007c5990bf2f384ca82cc451"
)
MAX_ADDITIONAL_BYTES = 8 * 1024**2
MINIMAL_OUTCOME_FIELDS = (
    "contract_identifier",
    "frozen_sample_identifier",
    "binary_resolution_outcome",
)
JOINED_DERIVED_FIELDS = (
    "binary_resolution_outcome",
    "midpoint_15m_spread_lte_0_20",
    "midpoint_15m_spread_lte_0_10",
)
ESTIMATE_FIELDS = (
    "sample_name",
    "weight_system",
    "resolved_contracts",
    "resolved_families",
    "family_aggregated_weight_ess",
    "contract_weighted_ess",
    "weighted_mean_price",
    "weighted_yes_rate",
    "weighted_calibration_gap",
    "gap_ci_lower",
    "gap_ci_upper",
    "gap_tail_probability",
    "weighted_brier_score",
    "longshot_favorite_contrast",
    "contrast_ci_lower",
    "contrast_ci_upper",
    "contrast_tail_probability",
)
BIN_FIELDS = (
    "weight_system",
    "probability_bin",
    "resolved_contracts",
    "resolved_families",
    "family_aggregated_weight_ess",
    "weighted_mean_price",
    "weighted_yes_rate",
    "weighted_calibration_gap",
    "gap_ci_lower",
    "gap_ci_upper",
    "support_gate_passed",
)


class PhaseGError(RuntimeError):
    pass


def _json_bytes(value: Any) -> bytes:
    return canonical_json(value) + b"\n"


def _verify(path: Path, expected: str, label: str) -> str:
    actual = _sha256(path)
    if actual != expected:
        raise PhaseGError(
            f"{label} SHA-256 mismatch: expected={expected} actual={actual}"
        )
    return actual


def _identity(document: Mapping[str, Any], label: str) -> str:
    payload = dict(document)
    identity = str(payload.pop("commit_identity", ""))
    if hashlib.sha256(_json_bytes(payload)).hexdigest() != identity:
        raise PhaseGError(f"{label} commit identity is invalid")
    return identity


def _gzip_csv(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in fields})
    return gzip.compress(buffer.getvalue().encode(), compresslevel=9, mtime=0)


def _csv_bytes(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in fields})
    return buffer.getvalue().encode()


def _rows_sha256(rows: Sequence[Mapping[str, Any]]) -> str:
    digest = hashlib.sha256()
    for row in rows:
        digest.update(canonical_json(row))
        digest.update(b"\n")
    return digest.hexdigest()


def _validate_joined_scope(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    expected = set(NORMALIZED_FIELDS) | set(JOINED_DERIVED_FIELDS)
    prohibited_fragments = ("outcome", "result", "settlement", "postresolution")
    for index, row in enumerate(rows):
        if set(row) != expected:
            raise PhaseGError(f"joined sample schema changed at row {index}")
        for key, value in row.items():
            canonical = "".join(
                character for character in key.casefold() if character.isalnum()
            )
            if key != "binary_resolution_outcome" and any(
                fragment in canonical for fragment in prohibited_fragments
            ):
                raise PhaseGError(f"prohibited joined-sample field: {key}")
            if isinstance(value, (Mapping, list, tuple)):
                raise PhaseGError(f"unexpected nested joined-sample value: {key}")
    return {
        "passed": True,
        "rows": len(rows),
        "fields": sorted(expected),
        "allowed_outcome_fields": ["binary_resolution_outcome"],
        "prohibited_post_resolution_fields": 0,
    }


def _validate_pre_outcome_inputs(args: argparse.Namespace) -> dict[str, str]:
    hashes = {
        "final_audit_commit": _verify(
            args.final_audit_commit,
            FINAL_AUDIT_COMMIT_SHA256,
            "final pre-outcome audit commit",
        ),
        "final_audit_report": _verify(
            args.final_audit_report,
            FINAL_AUDIT_REPORT_SHA256,
            "final pre-outcome audit report",
        ),
        "final_analysis_plan": _verify(
            args.analysis_plan,
            FINAL_ANALYSIS_PLAN_SHA256,
            "final analysis plan",
        ),
        "phase_10f_e_commit": _verify(
            args.phase_e_commit, PHASE_E_COMMIT_SHA256, "Phase 10F-E commit"
        ),
        "normalized_prices": _verify(
            args.normalized_prices,
            NORMALIZED_PRICES_SHA256,
            "normalized prices",
        ),
        "quarantined_outcomes": _verify(
            args.quarantined_outcomes,
            QUARANTINED_OUTCOMES_SHA256,
            "quarantined outcomes",
        ),
    }
    audit_commit = json.loads(args.final_audit_commit.read_text())
    if (
        _identity(audit_commit, "final pre-outcome audit") != FINAL_AUDIT_IDENTITY
        or not audit_commit.get("audit_passed")
        or audit_commit.get("outcomes_accessed") != 0
    ):
        raise PhaseGError("final pre-outcome audit state changed")
    report = json.loads(args.final_audit_report.read_text())
    if (
        not report.get("audit_passed")
        or report["primary_gate"].get("passed") is not True
    ):
        raise PhaseGError("final pre-outcome audit gate changed")
    rules = load_study_rules(args.config)
    if rules.fingerprint != STUDY_RULES_FINGERPRINT:
        raise PhaseGError("StudyRules fingerprint changed")
    hashes["study_rules_fingerprint"] = rules.fingerprint
    hashes["frozen_sample_identity"] = SAMPLE_COMMIT_IDENTITY
    return hashes


def _release_minimal_outcomes(
    path: Path,
    frozen_tickers: Sequence[str],
    *,
    expected_contracts: int = EXPECTED_CONTRACTS,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    target = set(frozen_tickers)
    observed: dict[str, int | None] = {}
    source_rows_matched = 0
    source_result_counts: Counter[str] = Counter()
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise PhaseGError("quarantined outcome artifact is empty") from exc
        if "ticker" not in header or "result" not in header:
            raise PhaseGError("quarantined outcome schema changed")
        ticker_index = header.index("ticker")
        result_index = header.index("result")
        for source in reader:
            ticker = source[ticker_index]
            if ticker not in target:
                continue
            if ticker in observed:
                raise PhaseGError("duplicate frozen ticker in quarantined outcomes")
            source_rows_matched += 1
            result = source[result_index].strip().casefold()
            source_result_counts[result or "[missing]"] += 1
            observed[ticker] = 1 if result == "yes" else 0 if result == "no" else None
    rows = [
        {
            "contract_identifier": ticker,
            "frozen_sample_identifier": SAMPLE_COMMIT_IDENTITY,
            "binary_resolution_outcome": (
                observed[ticker] if observed.get(ticker) in {0, 1} else ""
            ),
        }
        for ticker in frozen_tickers
    ]
    if len(rows) != expected_contracts or len(
        {row["contract_identifier"] for row in rows}
    ) != len(rows):
        raise PhaseGError("minimal outcome projection identity changed")
    resolution_counts = Counter(
        "resolved" if row["binary_resolution_outcome"] in {0, 1} else "unresolved"
        for row in rows
    )
    return rows, {
        "source_rows_matched": source_rows_matched,
        "source_rows_missing": len(rows) - source_rows_matched,
        "source_result_counts": dict(sorted(source_result_counts.items())),
        "resolution_counts": dict(sorted(resolution_counts.items())),
        "released_fields": list(MINIMAL_OUTCOME_FIELDS),
        "source_fields_used": ["ticker", "result"],
        "settlement_fields_released": 0,
        "post_resolution_metadata_fields_released": 0,
    }


def _resolution_diagnostics(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    resolved = [row for row in rows if row.get("binary_resolution_outcome") in {0, 1}]
    unresolved = [
        row for row in rows if row.get("binary_resolution_outcome") not in {0, 1}
    ]
    primary = [row for row in rows if bool(row.get("midpoint_within_15m"))]
    primary_resolved = [
        row for row in primary if row.get("binary_resolution_outcome") in {0, 1}
    ]
    all_families = {family_identity(row) for row in rows}
    resolved_families = {family_identity(row) for row in resolved}
    unresolved_families = {family_identity(row) for row in unresolved}
    primary_families = {family_identity(row) for row in primary}
    primary_resolved_families = {family_identity(row) for row in primary_resolved}
    primary_unresolved_families = {
        family_identity(row)
        for row in primary
        if row.get("binary_resolution_outcome") not in {0, 1}
    }

    def coverage(weight_field: str, group: Sequence[Mapping[str, Any]]) -> float:
        denominator = sum(float(row[weight_field]) for row in rows)
        return sum(float(row[weight_field]) for row in group) / denominator

    comparison: dict[str, Any]
    if not unresolved:
        comparison = {
            "estimable": False,
            "reason": "no unresolved frozen observations",
            "post_outcome_filtering_applied": False,
        }
    else:

        def breakdown(group: Sequence[Mapping[str, Any]], field: str) -> dict[str, int]:
            return dict(sorted(Counter(str(row[field]) for row in group).items()))

        comparison = {
            "estimable": True,
            "resolved_by_anchor_month": breakdown(resolved, "anchor_month"),
            "unresolved_by_anchor_month": breakdown(unresolved, "anchor_month"),
            "resolved_by_family_size_bin": breakdown(resolved, "family_size_bin"),
            "unresolved_by_family_size_bin": breakdown(unresolved, "family_size_bin"),
            "resolved_mean_hours_since_open": sum(
                float(row["hours_since_market_open"]) for row in resolved
            )
            / len(resolved),
            "unresolved_mean_hours_since_open": sum(
                float(row["hours_since_market_open"]) for row in unresolved
            )
            / len(unresolved),
            "post_outcome_filtering_applied": False,
        }
    return {
        "frozen_sample": {
            "contracts": len(rows),
            "families": len(all_families),
            "resolved_contracts": len(resolved),
            "families_with_any_resolved_contract": len(resolved_families),
            "unresolved_contracts": len(unresolved),
            "families_with_any_unresolved_contract": len(unresolved_families),
            "families_with_no_resolved_contract": len(all_families - resolved_families),
            "family_target_weighted_resolution_coverage": coverage(
                "family_weight_raw", resolved
            ),
            "contract_target_weighted_resolution_coverage": coverage(
                "contract_weight_raw", resolved
            ),
        },
        "primary_price_observable": {
            "contracts": len(primary),
            "families": len(primary_families),
            "resolved_contracts": len(primary_resolved),
            "families_with_any_resolved_contract": len(primary_resolved_families),
            "unresolved_contracts": len(primary) - len(primary_resolved),
            "families_with_any_unresolved_contract": len(primary_unresolved_families),
            "families_with_no_resolved_contract": len(
                primary_families - primary_resolved_families
            ),
        },
        "resolved_vs_unresolved_ex_ante_comparison": comparison,
    }


def _subgroup_estimates(
    rows: Sequence[Mapping[str, Any]], field: str
) -> dict[str, Any]:
    groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
    for row in rows:
        if bool(row.get("midpoint_within_15m")):
            groups[str(row[field])].append(row)
    result = {}
    for label, group in sorted(groups.items()):
        estimate = weighted_estimate(
            group,
            sample_name="primary_midpoint_15m",
            weight_field=WEIGHT_SYSTEMS["family_target"],
            require_contrast=False,
        )
        result[label] = {
            **estimate,
            "inferential_support_gate_passed": (
                estimate["resolved_families"] >= 200
                and estimate["family_aggregated_weight_ess"] >= 150
            ),
            "reporting_status": (
                "secondary_inferentially_supported"
                if estimate["resolved_families"] >= 200
                and estimate["family_aggregated_weight_ess"] >= 150
                else "low_support_descriptive_only"
            ),
        }
    return result


def _validate_existing(
    args: argparse.Namespace, input_hashes: Mapping[str, str], budget: StorageBudget
) -> dict[str, Any]:
    commit_path = args.output_root / "phase_10g_commit.json"
    commit = json.loads(commit_path.read_text())
    if _identity(commit, "Phase 10G") != commit["commit_identity"]:
        raise PhaseGError("Phase 10G identity changed")
    if commit.get("input_hashes") != dict(input_hashes):
        raise PhaseGError("Phase 10G inputs changed")
    for artifact in commit["artifacts"]:
        if _sha256(args.output_root / artifact["path"]) != artifact["sha256"]:
            raise PhaseGError("Phase 10G artifact changed")
    report = json.loads(
        (args.output_root / "phase_10g_analysis_report.json").read_text()
    )
    return {
        **report,
        "existing_final_reused": True,
        "final_commit_identity": commit["commit_identity"],
        "final_commit_sha256": _sha256(commit_path),
        "storage_now": budget.snapshot(),
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    input_hashes = _validate_pre_outcome_inputs(args)
    budget = StorageBudget(
        args.guard_root,
        max_bytes=args.max_generated_bytes,
        min_free_bytes=args.min_free_bytes,
    )
    budget.check_additional(MAX_ADDITIONAL_BYTES)
    commit_path = args.output_root / "phase_10g_commit.json"
    if commit_path.exists():
        return _validate_existing(args, input_hashes, budget)
    if args.preflight_only:
        return {
            "schema_version": SCHEMA_VERSION,
            "preflight_only": True,
            "passed": True,
            "input_hashes": input_hashes,
            "maximum_additional_bytes": MAX_ADDITIONAL_BYTES,
            "projected_remaining_namespace_bytes": (
                budget.snapshot()["remaining_budget_bytes"] - MAX_ADDITIONAL_BYTES
            ),
            "outcome_rows_read": 0,
            "network_requests_made": 0,
            "storage": budget.snapshot(),
        }

    normalized = _read_gzip_csv(args.normalized_prices, typed=True)
    if (
        len(normalized) != EXPECTED_CONTRACTS
        or tuple(normalized[0]) != NORMALIZED_FIELDS
    ):
        raise PhaseGError("frozen normalized sample schema/count changed")
    tickers = [str(row["ticker"]) for row in normalized]
    minimal, release = _release_minimal_outcomes(args.quarantined_outcomes, tickers)
    minimal_outcome_payload = _gzip_csv(minimal, MINIMAL_OUTCOME_FIELDS)
    minimal_outcome_sha256 = hashlib.sha256(minimal_outcome_payload).hexdigest()
    outcome_by_ticker = {
        str(row["contract_identifier"]): (
            int(row["binary_resolution_outcome"])
            if row["binary_resolution_outcome"] in {0, 1}
            else None
        )
        for row in minimal
    }
    joined = [
        {
            **row,
            "binary_resolution_outcome": outcome_by_ticker[str(row["ticker"])],
            "midpoint_15m_spread_lte_0_20": bool(
                row.get("midpoint_within_15m")
                and row.get("spread") is not None
                and float(row["spread"]) <= 0.20
            ),
            "midpoint_15m_spread_lte_0_10": bool(
                row.get("midpoint_within_15m")
                and row.get("spread") is not None
                and float(row["spread"]) <= 0.10
            ),
        }
        for row in normalized
    ]
    joined_scope = _validate_joined_scope(joined)
    joined_sample_sha256 = _rows_sha256(joined)
    resolution = _resolution_diagnostics(joined)
    primary_state = resolution["primary_price_observable"]
    if primary_state["families_with_any_resolved_contract"] < 500:
        raise PhaseGError("resolved primary sample fails the frozen family gate")
    primary_family_weights: dict[tuple[str, str], float] = defaultdict(float)
    for row in joined:
        if bool(row.get("midpoint_within_15m")) and row.get(
            "binary_resolution_outcome"
        ) in {0, 1}:
            primary_family_weights[family_identity(row)] += float(
                row["family_weight_raw"]
            )
    if kish_ess(list(primary_family_weights.values())) < 500:
        raise PhaseGError("resolved primary sample fails the frozen ESS gate")

    estimates: dict[str, Any] = {}
    for sample_name in SAMPLE_FLAGS:
        estimates[sample_name] = {
            weight_name: weighted_estimate(
                joined, sample_name=sample_name, weight_field=weight_field
            )
            for weight_name, weight_field in WEIGHT_SYSTEMS.items()
        }
    bootstrap = bootstrap_intervals(joined)
    estimate_rows = []
    for sample_name, weight_results in estimates.items():
        for weight_name, result in weight_results.items():
            gap_interval = bootstrap["intervals"][f"{sample_name}|{weight_name}|gap"]
            contrast_interval = bootstrap["intervals"][
                f"{sample_name}|{weight_name}|contrast"
            ]
            result["weighted_calibration_gap_inference"] = gap_interval
            result["longshot_favorite_contrast"]["inference"] = contrast_interval
            estimate_rows.append(
                {
                    "sample_name": sample_name,
                    "weight_system": weight_name,
                    **{
                        key: result[key]
                        for key in (
                            "resolved_contracts",
                            "resolved_families",
                            "family_aggregated_weight_ess",
                            "contract_weighted_ess",
                            "weighted_mean_price",
                            "weighted_yes_rate",
                            "weighted_calibration_gap",
                            "weighted_brier_score",
                        )
                    },
                    "gap_ci_lower": gap_interval["ci_lower"],
                    "gap_ci_upper": gap_interval["ci_upper"],
                    "gap_tail_probability": gap_interval[
                        "two_sided_bootstrap_tail_probability_plus_one"
                    ],
                    "longshot_favorite_contrast": result["longshot_favorite_contrast"][
                        "estimate"
                    ],
                    "contrast_ci_lower": contrast_interval["ci_lower"],
                    "contrast_ci_upper": contrast_interval["ci_upper"],
                    "contrast_tail_probability": contrast_interval[
                        "two_sided_bootstrap_tail_probability_plus_one"
                    ],
                }
            )

    bins: dict[str, Any] = {}
    bin_rows = []
    for weight_name, weight_field in WEIGHT_SYSTEMS.items():
        bins[weight_name] = calibration_bins(joined, weight_field=weight_field)
        for row in bins[weight_name]:
            interval = (
                bootstrap["intervals"].get(
                    f"primary_midpoint_15m|family_target|bin|{row['probability_bin']}|gap"
                )
                if weight_name == "family_target"
                else None
            )
            if interval is not None:
                row["weighted_calibration_gap_inference"] = interval
            bin_rows.append(
                {
                    "weight_system": weight_name,
                    **row,
                    "gap_ci_lower": interval["ci_lower"] if interval else "",
                    "gap_ci_upper": interval["ci_upper"] if interval else "",
                }
            )

    analysis = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "confirmatory_scope": "PR2_M_only",
        "primary_sample": "primary_midpoint_15m",
        "input_hashes": input_hashes,
        "minimal_outcome_release": release,
        "pre_estimation_integrity": {
            "minimal_outcome_projection_sha256": minimal_outcome_sha256,
            "joined_sample_sha256": joined_sample_sha256,
            "joined_sample_persisted": False,
            "joined_scope_validation": joined_scope,
            "frozen_price_input_sha256": input_hashes["normalized_prices"],
            "checks_completed_before_estimation": True,
        },
        "resolution_availability": resolution,
        "estimates": estimates,
        "primary_calibration_bins": bins,
        "primary_subgroups": {
            "anchor_month": _subgroup_estimates(joined, "anchor_month"),
            "family_size_bin": _subgroup_estimates(joined, "family_size_bin"),
            "category": {
                "Sports": {
                    "reporting_status": "confirmatory_scope_only_category",
                    "category_comparison_available": False,
                }
            },
        },
        "bootstrap": bootstrap,
        "limitations": {
            "conditional_on_price_observability": True,
            "observation_propensity_correction_applied": False,
            "pr1_included": False,
            "category_scope": "Sports_only",
            "post_outcome_filtering_applied": False,
            "sample_redrawn": False,
        },
        "outcome_release_controls": {
            "persisted_contract_level_fields": list(MINIMAL_OUTCOME_FIELDS),
            "joined_contract_level_analysis_persisted": False,
            "settlement_timestamps_released": 0,
            "post_resolution_metadata_released": 0,
            "network_requests_made": 0,
        },
    }
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "input_hashes": input_hashes,
        "sample_identity": SAMPLE_COMMIT_IDENTITY,
        "bootstrap_seed": BOOTSTRAP_SEED,
        "bootstrap_replicates": BOOTSTRAP_REPLICATES,
        "minimal_outcome_fields": list(MINIMAL_OUTCOME_FIELDS),
        "outcome_source_fields_used": ["ticker", "result"],
        "minimal_outcome_projection_sha256": minimal_outcome_sha256,
        "joined_sample_sha256": joined_sample_sha256,
        "joined_sample_persisted": False,
        "joined_scope_validation": joined_scope,
        "settlement_or_post_resolution_fields_used": 0,
        "network_requests_made": 0,
        "sample_redrawn": False,
        "weights_changed": False,
        "price_definitions_changed": False,
        "anchors_changed": False,
        "study_rules_changed": False,
    }
    artifacts = {
        "phase_10g_minimal_binary_outcomes.csv.gz": minimal_outcome_payload,
        "phase_10g_resolution_availability_report.json": _json_bytes(resolution),
        "phase_10g_weighted_estimates.csv": _csv_bytes(estimate_rows, ESTIMATE_FIELDS),
        "phase_10g_primary_calibration_bins.csv": _csv_bytes(bin_rows, BIN_FIELDS),
        "phase_10g_analysis_report.json": _json_bytes(analysis),
        "phase_10g_provenance.json": _json_bytes(provenance),
    }
    for name, content in artifacts.items():
        _publish(budget, args.output_root / name, content)
    refs = [
        {
            "path": name,
            "sha256": _sha256(args.output_root / name),
            "bytes": (args.output_root / name).stat().st_size,
        }
        for name in sorted(artifacts)
    ]
    final_without_identity = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "input_hashes": input_hashes,
        "sample_identity": SAMPLE_COMMIT_IDENTITY,
        "artifacts": refs,
        "minimal_outcome_fields": list(MINIMAL_OUTCOME_FIELDS),
        "minimal_outcome_projection_sha256": minimal_outcome_sha256,
        "joined_sample_sha256": joined_sample_sha256,
        "joined_sample_persisted": False,
        "settlement_fields_released": 0,
        "network_requests_made": 0,
    }
    identity = hashlib.sha256(_json_bytes(final_without_identity)).hexdigest()
    _publish(
        budget,
        commit_path,
        _json_bytes({**final_without_identity, "commit_identity": identity}),
    )
    return {
        **analysis,
        "existing_final_reused": False,
        "final_commit_identity": identity,
        "final_commit_sha256": _sha256(commit_path),
        "output_hashes": {item["path"]: item["sha256"] for item in refs},
        "storage_now": budget.snapshot(),
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    root = Path("data/pipeline_v2/horizon_prices/phase_10f_e")
    parser.add_argument(
        "--final-audit-commit",
        type=Path,
        default=root
        / "final_pre_outcome_audit/phase_10f_final_pre_outcome_commit.json",
    )
    parser.add_argument(
        "--final-audit-report",
        type=Path,
        default=root / "final_pre_outcome_audit/phase_10f_final_pre_outcome_audit.json",
    )
    parser.add_argument(
        "--analysis-plan", type=Path, default=Path("PHASE_10F_FINAL_ANALYSIS_PLAN.md")
    )
    parser.add_argument(
        "--phase-e-commit", type=Path, default=root / "phase_10f_e_commit.json"
    )
    parser.add_argument(
        "--normalized-prices",
        type=Path,
        default=root / "phase_10f_e_normalized_prices.csv.gz",
    )
    parser.add_argument(
        "--quarantined-outcomes",
        type=Path,
        default=Path(
            "data/pipeline_v2/market_acquisition/partitioned/merged_universes/"
            "6f8aa42abec876d3aa1f6336/market_outcomes.csv.gz"
        ),
    )
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/pipeline_v2/horizon_prices/phase_10g_outcome_analysis_v3"),
    )
    parser.add_argument("--config", type=Path, default=Path("configs/pipeline_v2.toml"))
    parser.add_argument("--guard-root", type=Path, default=Path("data/pipeline_v2"))
    parser.add_argument("--max-generated-bytes", type=int, default=5 * 1024**3)
    parser.add_argument("--min-free-bytes", type=int, default=80 * 1024**3)
    parser.add_argument("--preflight-only", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        print(json.dumps(run(parse_args(argv)), sort_keys=True))
        return 0
    except (PhaseGError, OutcomeAnalysisError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
