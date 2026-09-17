"""Build the deterministic no-network Phase 10J-A prospective preflight."""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping, Sequence

from scripts.pipeline_v2.kalshi_metadata_cache import StorageBudget, canonical_json
from scripts.pipeline_v2.phase_10j_prospective import (
    CONTRACT_CAP,
    DISCOVERY_START,
    SCHEMA_VERSION,
    TARGET_FAMILIES,
    WINDOW_DAYS,
    WINDOW_END_EXCLUSIVE,
    WINDOW_START,
    planning_rows,
    prospective_counts,
    storage_scenarios,
)
from scripts.pipeline_v2.study_rules import (
    load_study_rules,
    validate_research_feature_columns,
)


EXPECTED_HASHES = {
    "phase_10j_plan": "b29e777123c8942510470195c790ac0bf7d199e3fc5d505cb626ff6e10d061d3",
    "phase_10i_cross_category_preflight": "e3ddce6cffe180fb19eaebf106508244a96fffd240679b840db8f7c9462f236f",
    "study_rules_config": "b39fe2b84540c76b388250883311a0d7e33303eb0f45277a330504d619c0d706",
}
EXPECTED_STUDY_RULES_FINGERPRINT = (
    "12d6955f57b50b5587fdadf02b2bc96e7de48d022c9ac3cc2fe0425d907b9901"
)
MAX_REPORT_BYTES = 1024**2
OUTPUT_NAMES = (
    "PHASE_10J_OFFLINE_PREFLIGHT.md",
    "phase_10j_preflight.json",
    "phase_10j_sampling_plan.csv",
    "phase_10j_capture_schema.json",
    "reproducibility_manifest.json",
)


class PhaseJPlannerError(RuntimeError):
    """Raised when Phase 10J-A input or publication validation fails."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _verify(path: Path, expected: str, label: str) -> str:
    actual = _sha256(path)
    if actual != expected:
        raise PhaseJPlannerError(
            f"{label} SHA-256 mismatch: expected={expected} actual={actual}"
        )
    return actual


def _json_bytes(value: Any) -> bytes:
    return canonical_json(value) + b"\n"


def _csv_bytes(rows: Sequence[Mapping[str, Any]]) -> bytes:
    fields = (
        "category",
        "rule",
        "annual_historical_eligible_families",
        "planning_population_92_days",
        "family_inclusion_probability",
        "family_inclusion_probability_numerator",
        "family_inclusion_probability_denominator",
        "family_weight_raw",
        "expected_sampled_families",
        "expected_inferential_support",
    )
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row[field] for field in fields})
    return buffer.getvalue().encode()


def _capture_schema() -> dict[str, Any]:
    family_fields = (
        "cohort_id",
        "family_id",
        "family_id_source",
        "event_ticker",
        "category",
        "rule",
        "candidate_id",
        "candidate_time_utc",
        "candidate_source_type",
        "anchor_frozen_at",
        "verified_anchor_time",
        "verified_anchor_source",
        "price_target_time",
        "family_hash_draw",
        "family_inclusion_probability",
        "family_inclusion_probability_numerator",
        "family_inclusion_probability_denominator",
        "family_weight_raw",
        "evidence_sha256",
    )
    contract_fields = (
        "cohort_id",
        "family_id",
        "family_id_source",
        "market_ticker",
        "market_open_time",
        "family_contract_count",
        "sampled_contract_count_in_family",
        "contract_inclusion_probability_given_family",
        "contract_inclusion_probability",
        "contract_weight_raw",
    )
    orderbook_fields = (
        "cohort_id",
        "market_ticker",
        "price_target_time",
        "request_started_at",
        "response_received_at",
        "request_id",
        "raw_sha256",
        "yes_bid",
        "yes_bid_size",
        "no_bid",
        "no_bid_size",
        "yes_ask",
        "no_ask",
        "yes_spread",
        "yes_level_count",
        "no_level_count",
        "buy_yes_vwap_1",
        "buy_yes_vwap_10",
        "buy_yes_vwap_100",
        "buy_no_vwap_1",
        "buy_no_vwap_10",
        "buy_no_vwap_100",
        "capture_status",
    )
    trade_fields = (
        "cohort_id",
        "market_ticker",
        "price_target_time",
        "trade_window_start",
        "trade_window_end",
        "trade_count_60m",
        "traded_contracts_60m",
        "last_trade_yes_price",
        "last_trade_time",
        "last_trade_id",
        "request_page_count",
        "raw_page_hashes_sha256",
    )
    for fields in (family_fields, contract_fields, orderbook_fields, trade_fields):
        validate_research_feature_columns(fields)
    return {
        "schema_version": SCHEMA_VERSION,
        "family_enrollment_fields": list(family_fields),
        "sampled_contract_fields": list(contract_fields),
        "orderbook_observation_fields": list(orderbook_fields),
        "pre_target_trade_summary_fields": list(trade_fields),
        "raw_quote_and_trade_payloads": "immutable_gzip",
        "discovery_payload_policy": (
            "write only an allowlisted ex-ante projection plus the response digest; "
            "discard the full endpoint body"
        ),
        "late_orderbook_response_policy": (
            "discard body without parsing; persist timing/status only"
        ),
        "outcome_fields": 0,
        "settlement_fields": 0,
    }


def _recorded_storage(output_root: Path, current: Mapping[str, int]) -> dict[str, int]:
    report_path = output_root / "phase_10j_preflight.json"
    if not report_path.exists():
        return dict(current)
    report = json.loads(report_path.read_text())
    recorded = report.get("storage_snapshot")
    if not isinstance(recorded, Mapping):
        raise PhaseJPlannerError("published storage snapshot is invalid")
    if int(recorded.get("max_bytes", -1)) != int(current["max_bytes"]) or int(
        recorded.get("min_free_bytes", -1)
    ) != int(current["min_free_bytes"]):
        raise PhaseJPlannerError("published storage guard changed")
    return {str(key): int(value) for key, value in recorded.items()}


def _markdown(report: Mapping[str, Any]) -> str:
    counts = report["planning_counts"]
    storage = report["storage_scenarios"]
    rows = "\n".join(
        f"| {row['category']} | {row['planning_population_92_days']:.1f} | "
        f"{row['family_inclusion_probability']:.6f} | "
        f"{row['expected_sampled_families']:.1f} | "
        f"{'inferential' if row['expected_inferential_support'] else 'descriptive'} |"
        for row in report["sampling_plan"]
    )
    storage_rows = "\n".join(
        f"| {row['scenario']} | {row['projected_incremental_bytes']:,} | "
        f"{'yes' if row['fits_current_namespace_headroom'] else 'no'} |"
        for row in storage
    )
    return f"""# Phase 10J-A: prospective collection preflight

## Decision

The prospective route is technically feasible, but production is not yet
authorized and does not fit the current generated-data namespace. Phase 10J-A
made zero network requests, realized no future sample, and accessed no outcome.

The proposed cohort is `{WINDOW_START}` through `{WINDOW_END_EXCLUSIVE}`. It is
a new study identity and leaves Phase 10G and Phase 10I unchanged.

## Sampling plan

| Category | Planning families | Inclusion probability | Expected sample | Role |
|---|---:|---:|---:|---|
{rows}

Expected enrollment is {counts['expected_sampled_families']:.1f} families and
at most {counts['maximum_sampled_contracts']:,} contracts under the three-
contract cap. Base request planning spans
{counts['minimum_base_requests_excluding_discovery_and_pagination']:,} to
{counts['maximum_base_requests_excluding_discovery_and_pagination']:,} before
discovery polling, trade pagination, retries, or rate limiting.

## Capture design

For each frozen target, request two read-only order-book snapshots before the
target and retain the later response that completes no later than the target.
Preserve price levels and quantities and calculate executable one-, ten-, and
100-contract VWAPs. Fetch only trades timestamped inside the preceding hour.
Midpoint, taker, actual-trade, and conditional-maker measures remain separate.

Official endpoint validation establishes that the multiple-orderbooks endpoint
accepts up to 100 tickers and requires authentication, while the public trades
endpoint supports ticker and Unix-time filters. Exact account rate limits are
unknown until the authenticated smoke.

## Storage scenarios

| Scenario | Projected incremental bytes | Fits 5 GiB namespace |
|---|---:|---:|
{storage_rows}

Even the compact scenario exceeds the current
{report['storage_snapshot']['remaining_budget_bytes']:,}-byte headroom.
Production therefore requires a smoke-calibrated namespace increase; existing
validated data cannot be deleted or moved to create room.

## Next gate

Approve the exact 92-day cohort and a Phase 10J-B smoke capped at 20 families,
60 contracts, 5 MiB, and read-only authenticated requests. Read credentials
must be supplied outside Git. The smoke must validate schemas, response timing,
trade-filter semantics, rate limits, gzip ratios, request commits, resume, and
outcome quarantine before a production ceiling or request budget is proposed.
"""


def _publish(output_root: Path, artifacts: Mapping[str, bytes]) -> None:
    if sum(map(len, artifacts.values())) > MAX_REPORT_BYTES:
        raise PhaseJPlannerError("Phase 10J-A report budget exceeded")
    output_root.mkdir(parents=True, exist_ok=True)
    existing = {path.name for path in output_root.iterdir() if path.is_file()}
    if existing - set(artifacts):
        raise PhaseJPlannerError(f"unexpected output files: {sorted(existing)}")
    for name, content in artifacts.items():
        path = output_root / name
        if path.exists() and path.read_bytes() != content:
            raise PhaseJPlannerError(f"immutable output differs: {name}")
    for name, content in artifacts.items():
        path = output_root / name
        if path.exists():
            continue
        temporary = output_root / f".{name}.tmp"
        temporary.write_bytes(content)
        os.replace(temporary, path)


def _code_commit(root: Path, supplied: str | None) -> str:
    value = (
        supplied
        or subprocess.check_output(
            ["git", "rev-parse", "HEAD"], cwd=root, text=True
        ).strip()
    )
    if len(value) != 40 or any(
        character not in "0123456789abcdef" for character in value
    ):
        raise PhaseJPlannerError("code commit must be a full lowercase Git SHA")
    return value


def run(args: argparse.Namespace) -> dict[str, Any]:
    paths = {
        "phase_10j_plan": args.plan,
        "phase_10i_cross_category_preflight": args.phase_i_preflight,
        "study_rules_config": args.config,
    }
    hashes = {
        label: _verify(path, EXPECTED_HASHES[label], label)
        for label, path in paths.items()
    }
    rules = load_study_rules(args.config)
    if rules.fingerprint != EXPECTED_STUDY_RULES_FINGERPRINT:
        raise PhaseJPlannerError("frozen StudyRules changed")
    prior = json.loads(args.phase_i_preflight.read_text())
    if not prior.get("hard_stop", {}).get("triggered") or prior.get("sample_drawn"):
        raise PhaseJPlannerError("Phase 10I cross-category boundary changed")
    observed_counts = {
        row["category"]: int(row["eligible_families"])
        for row in prior["categories"]
        if row["category"] in {item["category"] for item in planning_rows()}
    }
    expected_counts = {
        row["category"]: int(row["annual_historical_eligible_families"])
        for row in planning_rows()
    }
    if observed_counts != expected_counts:
        raise PhaseJPlannerError("historical planning counts changed")

    budget = StorageBudget(
        args.guard_root,
        max_bytes=args.max_generated_bytes,
        min_free_bytes=args.min_free_bytes,
    )
    current_storage = budget.snapshot()
    storage = _recorded_storage(args.output_root, current_storage)
    rows = planning_rows()
    counts = prospective_counts()
    scenarios = storage_scenarios(int(counts["maximum_sampled_contracts"]))
    for scenario in scenarios:
        projected = int(scenario["projected_incremental_bytes"])
        scenario["fits_current_namespace_headroom"] = bool(
            projected <= int(storage["remaining_budget_bytes"])
        )
        scenario["projected_namespace_bytes"] = int(storage["used_bytes"]) + projected
        scenario["projected_free_bytes"] = int(storage["free_bytes"]) - projected
        scenario["free_space_floor_passes"] = bool(
            scenario["projected_free_bytes"] >= int(storage["min_free_bytes"])
        )
    schema = _capture_schema()
    report = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "prospective_route_selected": True,
        "production_authorized": False,
        "network_smoke_authorized": False,
        "proposed_window": {
            "discovery_start": DISCOVERY_START,
            "start": WINDOW_START,
            "end_exclusive": WINDOW_END_EXCLUSIVE,
            "days": WINDOW_DAYS,
            "owner_approval_required": True,
        },
        "target_families_per_supported_category": TARGET_FAMILIES,
        "within_family_contract_cap": CONTRACT_CAP,
        "sampling_plan": rows,
        "planning_counts": counts,
        "storage_scenarios": scenarios,
        "storage_snapshot": storage,
        "smoke_gate": {
            "maximum_families": 20,
            "maximum_contracts": 60,
            "maximum_generated_bytes": 5 * 1024**2,
            "read_only": True,
            "credentials_required": True,
            "account_rate_limit_query_required": True,
            "explicit_approval_required": True,
        },
        "endpoint_plan": {
            "event_discovery": "GET /events for open/unopened events",
            "milestones": "GET /milestones with minimum_start_date/min_updated_ts",
            "series_fee_metadata": "GET /series",
            "orderbooks": "GET /markets/orderbooks; authenticated; <=100 tickers",
            "trades": "GET /markets/trades; ticker/min_ts/max_ts; paginated",
            "account_limits": "GET /account/limits; authenticated",
        },
        "capture_schema": schema,
        "input_hashes": hashes,
        "study_rules_fingerprint": rules.fingerprint,
        "controls": {
            "network_requests_made": 0,
            "future_sample_realized": False,
            "outcomes_accessed": False,
            "study_rules_changed": False,
            "phase_10g_changed": False,
            "phase_10i_changed": False,
            "generated_namespace_written": False,
            "order_placement_allowed": False,
            "politics_entertainment_included": False,
        },
        "hard_stop": {
            "triggered": True,
            "reason": "every production storage scenario exceeds current namespace headroom",
            "release_conditions": [
                "approve the exact prospective window",
                "approve and run the bounded authenticated schema smoke",
                "measure storage and request rates",
                "approve the smoke-calibrated namespace ceiling and production budget",
            ],
        },
    }
    code_commit = _code_commit(args.repository_root.resolve(), args.code_commit)
    artifacts = {
        "phase_10j_preflight.json": _json_bytes(report),
        "phase_10j_sampling_plan.csv": _csv_bytes(rows),
        "phase_10j_capture_schema.json": _json_bytes(schema),
        "PHASE_10J_OFFLINE_PREFLIGHT.md": _markdown(report).encode(),
    }
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "code_commit": code_commit,
        "input_hashes": hashes,
        "study_rules_fingerprint": rules.fingerprint,
        "generated_artifacts": [
            {
                "path": name,
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
            for name, content in sorted(artifacts.items())
        ],
        "network_requests_made": 0,
        "outcomes_accessed": False,
    }
    artifacts["reproducibility_manifest.json"] = _json_bytes(manifest)
    _publish(args.output_root, artifacts)
    return {
        "complete": True,
        "artifact_count": len(artifacts),
        "output_bytes": sum(map(len, artifacts.values())),
        "manifest_sha256": hashlib.sha256(
            artifacts["reproducibility_manifest.json"]
        ).hexdigest(),
        "code_commit": code_commit,
        "network_requests_made": 0,
        "production_authorized": False,
        "hard_stop": report["hard_stop"],
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--repository-root", type=Path, default=Path("."))
    parser.add_argument(
        "--plan", type=Path, default=Path("PHASE_10J_PROSPECTIVE_COLLECTION_PLAN.md")
    )
    parser.add_argument(
        "--phase-i-preflight",
        type=Path,
        default=Path("reports/phase_10i/phase_10i_cross_category_preflight.json"),
    )
    parser.add_argument("--config", type=Path, default=Path("configs/pipeline_v2.toml"))
    parser.add_argument("--output-root", type=Path, default=Path("reports/phase_10j"))
    parser.add_argument("--guard-root", type=Path, default=Path("data/pipeline_v2"))
    parser.add_argument("--max-generated-bytes", type=int, default=5 * 1024**3)
    parser.add_argument("--min-free-bytes", type=int, default=80 * 1024**3)
    parser.add_argument("--code-commit")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        print(json.dumps(run(parse_args(argv)), sort_keys=True))
        return 0
    except (PhaseJPlannerError, OSError, ValueError, KeyError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
