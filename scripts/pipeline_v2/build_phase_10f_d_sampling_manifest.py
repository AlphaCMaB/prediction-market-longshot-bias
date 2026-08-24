"""Build the approved Phase 10F-D PR2 probability sample entirely offline."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
import gzip
import hashlib
import io
import json
import math
import os
from pathlib import Path
import tempfile
from typing import Any, Mapping, Sequence

from scripts.pipeline_v2.kalshi_metadata_cache import StorageBudget
from scripts.pipeline_v2.phase_10f_c_design import (
    FrameFamily,
    SAMPLING_SEED,
    allocate_stratified_sample,
    draw_stage_two_sample,
    family_size_bin,
    select_stage_one_families,
    wilson_interval,
)
from scripts.pipeline_v2.phase_10f_planner import (
    EXISTED,
    classify_market_open,
    decode_market_tickers,
)
from scripts.pipeline_v2.study_rules import (
    load_study_rules,
    validate_research_feature_columns,
)


SCHEMA_VERSION = "phase-10f-d-pr2-sampling-manifest-v1"
PR1 = "PR1_M_FIXED_CLOCK_SINGLE_EXACT"
PR2 = "PR2_M_SCHEDULED_START_SINGLE_MILESTONE"
PR1_STATUS = "valid_anchor_but_primary_price_source_not_viable"
TARGET_FAMILIES = 5000
CONTRACT_CAP = 3
EXPECTED_PLANNER_HASH = "90be78a79d5671006b65e54b2819cc8ad13e115f3875e3f8925be99c9966f41e"
EXPECTED_MARKET_HASH = "7acd4b59afc1ee0d952396cecb062e4216259c0ff4cb4893d5a8e00c50e26c44"
EXPECTED_B2_HASH = "b29deb4e46ae09ac9e40b393e15e2fec5212f91a3a662e212a9925f3cb641225"
EXPECTED_B2_ACCEPTANCE_HASH = "c7821ee78ea3f9b3e150b9c51e439fdc658927da7413ccc022ddb2bd6e5814b0"

FAMILY_FIELDS = (
    "family_sample_index", "family_id", "family_id_source", "event_ticker",
    "rule", "category", "timing_structure", "verified_anchor_time",
    "target_time", "verified_source", "anchor_month", "family_size_bin",
    "eligible_contract_count", "sampled_contract_count", "stratum_family_count",
    "stratum_sampled_family_count", "pi_family", "family_stage1_weight_raw",
    "sampling_seed",
)
CONTRACT_FIELDS = (
    "contract_sample_index", "family_sample_index", "family_id",
    "family_id_source", "event_ticker", "ticker", "rule", "category",
    "timing_structure", "verified_anchor_time", "target_time",
    "verified_source", "anchor_month", "family_size_bin",
    "eligible_contract_count", "sampled_contract_count",
    "stratum_family_count", "stratum_sampled_family_count", "pi_family",
    "pi_contract_given_family", "pi_contract", "family_weight_raw",
    "contract_weight_raw", "sampling_seed",
)
STRATUM_FIELDS = (
    "anchor_month", "family_size_bin", "population_family_count",
    "sampled_family_count", "pi_family", "population_eligible_contract_count",
    "selected_contract_count", "family_stage1_weight_sum",
    "contract_ht_population_estimate",
)
DETERMINISTIC_NAMES = (
    "phase_10f_d_family_sampling_manifest.csv.gz",
    "phase_10f_d_contract_sampling_manifest.csv.gz",
    "phase_10f_d_stratum_allocation.csv",
    "phase_10f_d_inclusion_probability_validation.json",
    "phase_10f_d_family_weight_diagnostics.json",
    "phase_10f_d_contract_weight_diagnostics.json",
    "phase_10f_d_sampling_manifest.json",
)
RUNTIME_NAMES = (
    "phase_10f_d_production_preflight.json",
    "phase_10f_d_commit.json",
)


class ManifestError(RuntimeError):
    pass


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _verify(path: Path, expected: str, label: str) -> str:
    actual = _sha256(path)
    if actual != expected:
        raise ManifestError(
            f"{label} SHA-256 mismatch: expected={expected} actual={actual}"
        )
    return actual


def _json_bytes(value: Any) -> bytes:
    return (
        json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)
        + "\n"
    ).encode()


def _csv_bytes(
    rows: Sequence[Mapping[str, Any]], fields: Sequence[str], *, compressed: bool
) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in fields})
    raw = buffer.getvalue().encode()
    return gzip.compress(raw, compresslevel=9, mtime=0) if compressed else raw


def _kish(weights: Sequence[float]) -> float:
    if not weights or sum(weight * weight for weight in weights) <= 0:
        raise ManifestError("effective sample size requires positive weights")
    return sum(weights) ** 2 / sum(weight * weight for weight in weights)


def _load_frame(
    planner: Path,
) -> tuple[list[FrameFamily], dict[tuple[str, str], dict[str, str]], dict[str, int]]:
    frame: list[FrameFamily] = []
    sources: dict[tuple[str, str], dict[str, str]] = {}
    status_counts: Counter[str] = Counter()
    with planner.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        fields = reader.fieldnames or ()
        validate_research_feature_columns(fields)
        required = {
            "family_id", "family_id_source", "event_ticker",
            "associated_market_tickers_compact", "market_ticker_encoding",
            "verified_anchor_time", "target_time", "verified_source", "rule",
            "category", "timing_structure", "market_existence_at_target",
            "market_count", "eligible_market_retrieval_count",
        }
        if not required.issubset(fields):
            raise ManifestError("planner projection changed")
        identities: set[tuple[str, str]] = set()
        for row in reader:
            identity = (row["family_id"], row["family_id_source"])
            if identity in identities:
                raise ManifestError("planner contains duplicate family identity")
            identities.add(identity)
            if row["rule"] == PR1 and row["market_existence_at_target"] == EXISTED:
                status_counts[PR1_STATUS] += 1
            if row["rule"] != PR2 or row["market_existence_at_target"] != EXISTED:
                continue
            if row["category"] != "Sports" or row["timing_structure"] != "scheduled_event_start":
                raise ManifestError("PR2 population semantics changed")
            if row["market_ticker_encoding"] != "family-prefix-relative-v1":
                raise ManifestError("market ticker encoding changed")
            count = int(row["eligible_market_retrieval_count"])
            candidate = FrameFamily(
                family_id=identity[0], family_id_source=identity[1], rule=PR2,
                category="Sports", anchor_month=row["verified_anchor_time"][:7],
                contract_count=count,
            )
            family_size_bin(count)
            frame.append(candidate)
            sources[identity] = dict(row)
    if len(frame) != 64775 or sum(item.contract_count for item in frame) != 319364:
        raise ManifestError("PR2 eligible population count changed")
    if status_counts[PR1_STATUS] != 47391:
        raise ManifestError("PR1 downstream-status population count changed")
    return frame, sources, dict(status_counts)


def _attach_selected_contracts(
    selected: Sequence[FrameFamily],
    sources: Mapping[tuple[str, str], Mapping[str, str]],
    market_metadata: Path,
) -> list[FrameFamily]:
    selected_sources = {item.identity: sources[item.identity] for item in selected}
    planned = {
        identity: set(
            decode_market_tickers(row["family_id"], row["associated_market_tickers_compact"])
        )
        for identity, row in selected_sources.items()
    }
    observed: dict[tuple[str, str], set[str]] = defaultdict(set)
    eligible: dict[tuple[str, str], list[str]] = defaultdict(list)
    ticker_owner: dict[str, tuple[str, str]] = {}
    required = ("ticker", "event_ticker", "family_id", "family_id_source", "open_time")
    with gzip.open(market_metadata, "rt", encoding="utf-8", newline="") as handle:
        try:
            header = next(csv.reader(handle))
        except StopIteration as exc:
            raise ManifestError("market metadata is empty") from exc
    validate_research_feature_columns(header)
    if not set(required).issubset(header):
        raise ManifestError("market metadata projection changed")
    try:
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.csv as arrow_csv
    except ImportError as exc:
        raise ManifestError("Phase 10F-D requires the bundled pyarrow runtime") from exc
    selected_family_ids = pa.array(sorted(identity[0] for identity in selected_sources))
    with pa.memory_map(str(market_metadata), "r") as mapped:
        compressed = pa.CompressedInputStream(mapped, "gzip")
        reader = arrow_csv.open_csv(
            compressed,
            read_options=arrow_csv.ReadOptions(block_size=16 * 1024 * 1024),
            parse_options=arrow_csv.ParseOptions(newlines_in_values=True),
            convert_options=arrow_csv.ConvertOptions(
                include_columns=list(required),
                column_types={field: pa.string() for field in required},
            ),
        )
        for batch in reader:
            mask = pc.is_in(batch.column("family_id"), value_set=selected_family_ids)
            if not pc.any(mask).as_py():
                continue
            for source in batch.filter(mask).to_pylist():
                identity = (source["family_id"], source["family_id_source"])
                plan = selected_sources.get(identity)
                if plan is None:
                    continue
                ticker = source["ticker"]
                if ticker in observed[identity] or ticker not in planned[identity]:
                    raise ManifestError("selected family market association conflict")
                if source["event_ticker"] != plan["event_ticker"]:
                    raise ManifestError("selected event identity changed")
                observed[identity].add(ticker)
                if classify_market_open(source["open_time"], plan["target_time"]) == EXISTED:
                    previous = ticker_owner.setdefault(ticker, identity)
                    if previous != identity:
                        raise ManifestError("eligible ticker belongs to multiple families")
                    eligible[identity].append(ticker)
    result: list[FrameFamily] = []
    for item in selected:
        identity = item.identity
        if observed[identity] != planned[identity]:
            raise ManifestError("selected market associations are incomplete")
        tickers = tuple(sorted(eligible[identity]))
        if len(tickers) != item.contract_count:
            raise ManifestError("selected eligible ticker count changed")
        result.append(
            FrameFamily(
                family_id=item.family_id, family_id_source=item.family_id_source,
                rule=item.rule, category=item.category, anchor_month=item.anchor_month,
                contract_count=item.contract_count, contract_ids=tickers,
            )
        )
    return sorted(result, key=lambda item: item.identity)


def _family_rows(
    selected: Sequence[FrameFamily], sources: Mapping[tuple[str, str], Mapping[str, str]],
    stratum_counts: Mapping[tuple[str, str, str, str], int],
    allocation: Mapping[tuple[str, str, str, str], int],
) -> tuple[list[dict[str, Any]], dict[tuple[str, str], int]]:
    rows = []
    indices: dict[tuple[str, str], int] = {}
    for index, item in enumerate(sorted(selected, key=lambda row: row.identity), 1):
        source = sources[item.identity]
        N_h = int(stratum_counts[item.stratum])
        n_h = int(allocation[item.stratum])
        pi = n_h / N_h
        indices[item.identity] = index
        rows.append({
            "family_sample_index": index,
            "family_id": item.family_id, "family_id_source": item.family_id_source,
            "event_ticker": source["event_ticker"], "rule": item.rule,
            "category": item.category, "timing_structure": source["timing_structure"],
            "verified_anchor_time": source["verified_anchor_time"],
            "target_time": source["target_time"], "verified_source": source["verified_source"],
            "anchor_month": item.anchor_month, "family_size_bin": item.size_bin,
            "eligible_contract_count": item.contract_count,
            "sampled_contract_count": min(CONTRACT_CAP, item.contract_count),
            "stratum_family_count": N_h, "stratum_sampled_family_count": n_h,
            "pi_family": pi, "family_stage1_weight_raw": 1 / pi,
            "sampling_seed": SAMPLING_SEED,
        })
    return rows, indices


def _contract_rows(
    sampled: Sequence[Mapping[str, Any]],
    sources: Mapping[tuple[str, str], Mapping[str, str]],
    family_indices: Mapping[tuple[str, str], int],
) -> list[dict[str, Any]]:
    rows = []
    for index, item in enumerate(sampled, 1):
        identity = (str(item["family_id"]), str(item["family_id_source"]))
        source = sources[identity]
        rows.append({
            "contract_sample_index": index,
            "family_sample_index": family_indices[identity],
            "family_id": identity[0], "family_id_source": identity[1],
            "event_ticker": source["event_ticker"], "ticker": item["contract_id"],
            "rule": item["rule"], "category": item["category"],
            "timing_structure": source["timing_structure"],
            "verified_anchor_time": source["verified_anchor_time"],
            "target_time": source["target_time"], "verified_source": source["verified_source"],
            "anchor_month": item["anchor_month"], "family_size_bin": item["family_size_bin"],
            "eligible_contract_count": item["family_contract_count"],
            "sampled_contract_count": item["sampled_contract_count_in_family"],
            "stratum_family_count": item["stratum_family_count"],
            "stratum_sampled_family_count": item["stratum_sampled_family_count"],
            "pi_family": item["pi_family"],
            "pi_contract_given_family": item["pi_contract_given_family"],
            "pi_contract": item["pi_contract"],
            "family_weight_raw": item["family_weight_raw"],
            "contract_weight_raw": item["contract_weight_raw"],
            "sampling_seed": item["sampling_seed"],
        })
    return rows


def _validate(
    family_rows: Sequence[Mapping[str, Any]], contract_rows: Sequence[Mapping[str, Any]],
    stratum_counts: Mapping[tuple[str, str, str, str], int],
    allocation: Mapping[tuple[str, str, str, str], int],
) -> dict[str, Any]:
    family_ids = [(row["family_id"], row["family_id_source"]) for row in family_rows]
    contracts = [(row["family_id"], row["family_id_source"], row["ticker"]) for row in contract_rows]
    if len(family_rows) != TARGET_FAMILIES or len(set(family_ids)) != TARGET_FAMILIES:
        raise ManifestError("family manifest identity validation failed")
    if len(contracts) != len(set(contracts)):
        raise ManifestError("contract manifest contains duplicates")
    per_family = Counter((row["family_id"], row["family_id_source"]) for row in contract_rows)
    if not per_family or max(per_family.values()) > CONTRACT_CAP or set(per_family) != set(family_ids):
        raise ManifestError("within-family contract cap validation failed")
    family_weight_sum = 0.0
    contract_weight_sum = 0.0
    for row in family_rows:
        N_h = int(row["stratum_family_count"]); n_h = int(row["stratum_sampled_family_count"])
        if float(row["pi_family"]) != n_h / N_h:
            raise ManifestError("first-stage inclusion probability mismatch")
        if float(row["family_stage1_weight_raw"]) != 1 / float(row["pi_family"]):
            raise ManifestError("first-stage weight mismatch")
    for row in contract_rows:
        M_i = int(row["eligible_contract_count"]); m_i = int(row["sampled_contract_count"])
        pi_f = float(row["pi_family"]); pi_c = float(row["pi_contract_given_family"])
        pi = float(row["pi_contract"])
        if pi_c != m_i / M_i or pi != pi_f * pi_c:
            raise ManifestError("contract inclusion probability mismatch")
        if float(row["contract_weight_raw"]) != 1 / pi:
            raise ManifestError("contract weight mismatch")
        if float(row["family_weight_raw"]) != 1 / (pi * M_i):
            raise ManifestError("family weight mismatch")
        family_weight_sum += float(row["family_weight_raw"])
        contract_weight_sum += float(row["contract_weight_raw"])
    expected_family_ht = sum(1 / float(row["pi_family"]) for row in family_rows)
    if not math.isclose(expected_family_ht, 64775, rel_tol=0, abs_tol=1e-8):
        raise ManifestError("stage-one weights do not reconstruct PR2 family population")
    if not math.isclose(family_weight_sum, expected_family_ht, rel_tol=0, abs_tol=1e-8):
        raise ManifestError("contract rows do not reconstruct family weights")
    realized = Counter((row["rule"], row["category"], row["anchor_month"], row["family_size_bin"]) for row in family_rows)
    if any(realized[key] != value for key, value in allocation.items()):
        raise ManifestError("stratum allocation changed")
    return {
        "schema_version": SCHEMA_VERSION,
        "passed": True,
        "sampled_family_count": len(family_rows),
        "sampled_contract_count": len(contract_rows),
        "duplicate_family_identity_count": len(family_rows) - len(set(family_ids)),
        "duplicate_contract_identity_count": len(contract_rows) - len(set(contracts)),
        "maximum_contracts_per_family": max(per_family.values()),
        "first_stage_probabilities_exact": True,
        "second_stage_probabilities_exact": True,
        "overall_probabilities_exact": True,
        "family_weights_reconstructable": True,
        "contract_weights_reconstructable": True,
        "family_population_reconstructed": expected_family_ht,
        "family_population_target": 64775,
        "family_weight_sum_from_contract_rows": family_weight_sum,
        "contract_population_ht_estimate": contract_weight_sum,
        "contract_population_target": 319364,
        "outcome_columns_in_family_manifest": [],
        "outcome_columns_in_contract_manifest": [],
        "production_anchor_state_changed": False,
    }


def _stratum_rows(
    frame: Sequence[FrameFamily], family_rows: Sequence[Mapping[str, Any]],
    contract_rows: Sequence[Mapping[str, Any]],
) -> list[dict[str, Any]]:
    population_families = Counter((item.anchor_month, item.size_bin) for item in frame)
    population_contracts = Counter()
    for item in frame:
        population_contracts[(item.anchor_month, item.size_bin)] += item.contract_count
    selected_families = Counter((row["anchor_month"], row["family_size_bin"]) for row in family_rows)
    selected_contracts = Counter((row["anchor_month"], row["family_size_bin"]) for row in contract_rows)
    family_weights = Counter(); contract_weights = Counter()
    for row in family_rows:
        family_weights[(row["anchor_month"], row["family_size_bin"])] += float(row["family_stage1_weight_raw"])
    for row in contract_rows:
        contract_weights[(row["anchor_month"], row["family_size_bin"])] += float(row["contract_weight_raw"])
    return [{
        "anchor_month": key[0], "family_size_bin": key[1],
        "population_family_count": population_families[key],
        "sampled_family_count": selected_families[key],
        "pi_family": selected_families[key] / population_families[key],
        "population_eligible_contract_count": population_contracts[key],
        "selected_contract_count": selected_contracts[key],
        "family_stage1_weight_sum": family_weights[key],
        "contract_ht_population_estimate": contract_weights[key],
    } for key in sorted(population_families)]


def _diagnostics(
    family_rows: Sequence[Mapping[str, Any]], contract_rows: Sequence[Mapping[str, Any]],
    b2: Mapping[str, Any], validation: Mapping[str, Any],
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    evidence = b2["by_rule"][PR2]
    x = int(evidence["usable_midpoint_15m"]); n = int(evidence["sample_tickers"])
    point = x / n; lower, upper = wilson_interval(x, n)
    family_weights = [float(row["family_stage1_weight_raw"]) for row in family_rows]
    family_design_ess = _kish(family_weights)
    family_diag = {
        "schema_version": SCHEMA_VERSION,
        "target_estimand": "family_weighted_conditional_on_valid_pre_target_price_observability",
        "sampled_families": len(family_rows),
        "raw_stage1_weight_sum": sum(family_weights),
        "stage1_family_design_ess": family_design_ess,
        "expected_unique_observable_families_conservative": len(family_rows) * point,
        "expected_unique_observable_families_wilson_95_lower": len(family_rows) * lower,
        "expected_unique_observable_families_wilson_95_upper": len(family_rows) * upper,
        "expected_observable_family_ess_conservative": family_design_ess * point,
        "expected_observable_family_ess_wilson_95_lower": family_design_ess * lower,
        "expected_observable_family_ess_wilson_95_upper": family_design_ess * upper,
        "family_observability_assumption": (
            "Conservative one-ticker-per-family projection using B2 PR2 evidence; additional sampled "
            "contracts may improve family yield, but within-family availability dependence is unknown."
        ),
        "approved_thresholds": {
            "overall_unique_observable_families": 500,
            "overall_family_weighted_ess": 500,
            "subgroup_unique_observable_families": 200,
            "subgroup_ess": 150,
            "probability_bin_unique_families": 100,
            "probability_bin_ess": 100,
        },
    }
    contract_family_weights = [float(row["family_weight_raw"]) for row in contract_rows]
    contract_weights = [float(row["contract_weight_raw"]) for row in contract_rows]
    contract_diag = {
        "schema_version": SCHEMA_VERSION,
        "target_estimand": "contract_weighted_conditional_on_valid_pre_target_price_observability",
        "sampled_contracts": len(contract_rows),
        "family_target_row_weight_sum": sum(contract_family_weights),
        "family_target_row_weight_ess": _kish(contract_family_weights),
        "contract_target_raw_weight_sum_ht": sum(contract_weights),
        "contract_population_target": validation["contract_population_target"],
        "contract_target_row_weight_ess": _kish(contract_weights),
        "midpoint_15m_b2_pr2": {
            "successes": x, "trials": n, "point": point,
            "wilson_95_lower": lower, "wilson_95_upper": upper,
        },
        "expected_usable_midpoint_15m_contracts": len(contract_rows) * point,
        "expected_usable_midpoint_15m_wilson_95_lower": len(contract_rows) * lower,
        "expected_usable_midpoint_15m_wilson_95_upper": len(contract_rows) * upper,
    }
    sensitivity = {
        "confirmatory_price": {
            "measure": "latest_fully_pre_target_yes_bid_ask_midpoint",
            "maximum_staleness_minutes": 15, "requires_bid_and_ask": True,
            "fallback_mixing": False, "spread_cutoff": None,
        },
        "predeclared_robustness": [
            {"measure": "midpoint", "maximum_staleness_minutes": 60},
            {"measure": "actual_trade_close", "maximum_staleness_minutes": 15},
            {"measure": "actual_trade_close", "maximum_staleness_minutes": 60},
        ],
        "predeclared_spread_sensitivities_dollars": [0.20, 0.10],
        "post_target_candles_allowed": False,
    }
    return family_diag, contract_diag, sensitivity


def _build_contents(args: argparse.Namespace) -> tuple[dict[str, bytes], dict[str, Any]]:
    input_hashes = {
        "planner": _verify(args.planner, args.expected_planner_sha256, "planner"),
        "market_metadata": _verify(args.market_metadata, args.expected_market_metadata_sha256, "market metadata"),
        "b2_report": _verify(args.b2_report, args.expected_b2_report_sha256, "B2 report"),
        "b2_acceptance": _verify(args.b2_acceptance, args.expected_b2_acceptance_sha256, "B2 acceptance"),
    }
    rules = load_study_rules(args.config)
    frame, sources, pr1_status = _load_frame(args.planner)
    stratum_counts = Counter(item.stratum for item in frame)
    allocation = allocate_stratified_sample(stratum_counts, TARGET_FAMILIES)
    selected = select_stage_one_families(frame, allocation)
    if len(selected) != TARGET_FAMILIES:
        raise ManifestError("stage-one sample is not exactly 5,000 families")
    selected_complete = _attach_selected_contracts(
        selected, sources, args.market_metadata
    )
    sampled = draw_stage_two_sample(
        selected_complete, stratum_counts, allocation, CONTRACT_CAP
    )
    family_rows, family_indices = _family_rows(
        selected_complete, sources, stratum_counts, allocation
    )
    contract_rows = _contract_rows(sampled, sources, family_indices)
    validate_research_feature_columns(FAMILY_FIELDS)
    validate_research_feature_columns(CONTRACT_FIELDS)
    validation = _validate(family_rows, contract_rows, stratum_counts, allocation)
    strata = _stratum_rows(frame, family_rows, contract_rows)
    b2 = json.loads(args.b2_report.read_text())
    acceptance = json.loads(args.b2_acceptance.read_text())
    family_diag, contract_diag, price_spec = _diagnostics(
        family_rows, contract_rows, b2, validation
    )
    requests = len(contract_rows) + 2
    throughput = float(acceptance["measured_total_requests_per_second"])
    per_ticker_bytes = sum(float(acceptance[key]) for key in (
        "measured_compressed_raw_bytes_per_ticker_request",
        "measured_normalized_bytes_per_ticker",
        "measured_request_commit_and_manifest_bytes_per_ticker",
    ))
    family_content = _csv_bytes(family_rows, FAMILY_FIELDS, compressed=True)
    contract_content = _csv_bytes(contract_rows, CONTRACT_FIELDS, compressed=True)
    stratum_content = _csv_bytes(strata, STRATUM_FIELDS, compressed=False)
    contents: dict[str, bytes] = {
        DETERMINISTIC_NAMES[0]: family_content,
        DETERMINISTIC_NAMES[1]: contract_content,
        DETERMINISTIC_NAMES[2]: stratum_content,
        DETERMINISTIC_NAMES[3]: _json_bytes(validation),
        DETERMINISTIC_NAMES[4]: _json_bytes(family_diag),
        DETERMINISTIC_NAMES[5]: _json_bytes(contract_diag),
    }
    detail_hashes = {name: hashlib.sha256(content).hexdigest() for name, content in contents.items()}
    summary = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "approved_scope": "PR2_confirmatory_offline_sampling_manifest_only",
        "input_hashes": input_hashes,
        "study_rules_fingerprint": rules.fingerprint,
        "population": {
            "pr2_structurally_eligible_families": len(frame),
            "pr2_structurally_eligible_contracts": sum(item.contract_count for item in frame),
            "pr1_downstream_status": PR1_STATUS,
            "pr1_families_with_that_status": pr1_status[PR1_STATUS],
        },
        "design": {
            "stage_one": "stratified_family_srswor",
            "effective_strata": ["verified_anchor_month", "family_size_bin"],
            "manifest_preserved_fields": ["rule", "category"],
            "target_families": TARGET_FAMILIES,
            "stage_two": "uniform_contract_srswor_within_selected_family",
            "contract_cap": CONTRACT_CAP,
            "sampling_seed": SAMPLING_SEED,
        },
        "realized": {
            "sampled_families": len(family_rows),
            "sampled_contracts": len(contract_rows),
            "nonempty_strata": len(strata),
            "anchor_month_counts": dict(sorted(Counter(row["anchor_month"] for row in family_rows).items())),
            "family_size_bin_counts": dict(sorted(Counter(row["family_size_bin"] for row in family_rows).items())),
        },
        "price_specification": price_spec,
        "missing_price_interpretation": (
            "Inference is limited to design-weighted PR2 contracts/families with an observable valid "
            "pre-target quote under the t-1h and <=15-minute rules. Missing prices are not anchor failures."
        ),
        "pool_pr1_pr2": False,
        "projected_acquisition": {
            "ticker_requests": len(contract_rows),
            "total_requests_including_cutoff_and_probe": requests,
            "runtime_seconds": requests / throughput,
            "auditable_storage_bytes": math.ceil(len(contract_rows) * per_ticker_bytes),
            "network_authorized": False,
        },
        "expected_midpoint_15m": {
            "usable_contracts": contract_diag["expected_usable_midpoint_15m_contracts"],
            "wilson_95_lower": contract_diag["expected_usable_midpoint_15m_wilson_95_lower"],
            "wilson_95_upper": contract_diag["expected_usable_midpoint_15m_wilson_95_upper"],
            "unique_observable_families_conservative": family_diag["expected_unique_observable_families_conservative"],
            "unique_observable_families_wilson_95_lower": family_diag["expected_unique_observable_families_wilson_95_lower"],
            "unique_observable_families_wilson_95_upper": family_diag["expected_unique_observable_families_wilson_95_upper"],
            "family_ess_conservative": family_diag["expected_observable_family_ess_conservative"],
            "family_ess_wilson_95_lower": family_diag["expected_observable_family_ess_wilson_95_lower"],
            "family_ess_wilson_95_upper": family_diag["expected_observable_family_ess_wilson_95_upper"],
        },
        "artifact_hashes": detail_hashes,
        "network_requests_made": 0, "prices_acquired": 0, "outcomes_accessed": 0,
        "study_rules_changed": False, "anchors_changed": 0,
    }
    contents[DETERMINISTIC_NAMES[6]] = _json_bytes(summary)
    return contents, summary


def _publish(path: Path, content: bytes) -> None:
    if path.exists():
        if path.read_bytes() != content:
            raise ManifestError(f"immutable output conflict: {path}")
        return
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content); handle.flush(); os.fsync(handle.fileno())
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def run(args: argparse.Namespace) -> dict[str, Any]:
    contents, summary = _build_contents(args)
    budget = StorageBudget(
        args.guard_root, max_bytes=args.max_generated_bytes,
        min_free_bytes=args.min_free_bytes,
    )
    commit_path = args.output_root / RUNTIME_NAMES[1]
    if commit_path.exists():
        commit = json.loads(commit_path.read_text())
        for name, content in contents.items():
            path = args.output_root / name
            if not path.exists() or hashlib.sha256(path.read_bytes()).hexdigest() != hashlib.sha256(content).hexdigest():
                raise ManifestError(f"deterministic resume mismatch: {name}")
        preflight_path = args.output_root / RUNTIME_NAMES[0]
        if _sha256(preflight_path) != commit["artifact_hashes"][RUNTIME_NAMES[0]]:
            raise ManifestError("preflight hash changed")
        commit_payload = dict(commit)
        commit_identity = str(commit_payload.pop("commit_identity", ""))
        if hashlib.sha256(_json_bytes(commit_payload)).hexdigest() != commit_identity:
            raise ManifestError("commit identity changed")
        return {"resumed": True, "network_requests": 0, "summary": summary, "storage": budget.snapshot(), "commit_identity": commit_identity}

    deterministic_bytes = sum(len(content) for content in contents.values())
    estimated_runtime_bytes = 4096
    budget.check_additional(deterministic_bytes + estimated_runtime_bytes)
    before = budget.snapshot()
    preflight = {
        "schema_version": SCHEMA_VERSION,
        "passed": True,
        "snapshot_before_publication": before,
        "planned_artifact_bytes_excluding_preflight_and_commit": deterministic_bytes,
        "reserved_preflight_and_commit_bytes": estimated_runtime_bytes,
        "projected_acquisition": summary["projected_acquisition"],
        "projected_namespace_after_acquisition_bytes": before["used_bytes"] + deterministic_bytes + estimated_runtime_bytes + summary["projected_acquisition"]["auditable_storage_bytes"],
        "projected_namespace_headroom_after_acquisition_bytes": before["max_bytes"] - before["used_bytes"] - deterministic_bytes - estimated_runtime_bytes - summary["projected_acquisition"]["auditable_storage_bytes"],
        "projected_free_margin_after_acquisition_bytes": before["free_space_margin_bytes"] - deterministic_bytes - estimated_runtime_bytes - summary["projected_acquisition"]["auditable_storage_bytes"],
        "network_requests_made": 0,
    }
    preflight_content = _json_bytes(preflight)
    all_precommit = {**contents, RUNTIME_NAMES[0]: preflight_content}
    hashes = {name: hashlib.sha256(content).hexdigest() for name, content in all_precommit.items()}
    commit_without_identity = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "artifact_hashes": hashes,
        "sampled_families": summary["realized"]["sampled_families"],
        "sampled_contracts": summary["realized"]["sampled_contracts"],
        "network_requests_made": 0,
    }
    identity = hashlib.sha256(_json_bytes(commit_without_identity)).hexdigest()
    commit_content = _json_bytes({**commit_without_identity, "commit_identity": identity})
    budget.check_additional(sum(len(content) for content in all_precommit.values()) + len(commit_content))
    if args.preflight_only:
        return {"preflight_only": True, "additional_artifact_bytes": sum(len(content) for content in all_precommit.values()) + len(commit_content), "summary": summary, "storage": before}
    args.output_root.mkdir(parents=True, exist_ok=True)
    for name, content in all_precommit.items():
        _publish(args.output_root / name, content)
    _publish(commit_path, commit_content)
    return {"preflight_only": False, "network_requests": 0, "summary": summary, "storage": budget.snapshot(), "commit_identity": identity, "output_hashes": {**hashes, RUNTIME_NAMES[1]: hashlib.sha256(commit_content).hexdigest()}}


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument("--planner", type=Path, default=Path("data/pipeline_v2/horizon_prices/phase_10f_a/phase_10f_horizon_planner.csv"))
    parser.add_argument("--market-metadata", type=Path, default=Path("data/pipeline_v2/market_acquisition/partitioned/merged_universes/6f8aa42abec876d3aa1f6336/market_metadata.csv.gz"))
    parser.add_argument("--b2-report", type=Path, default=Path("data/pipeline_v2/horizon_prices/phase_10f_b2/phase_10f_b2_report.json"))
    parser.add_argument("--b2-acceptance", type=Path, default=Path("data/pipeline_v2/horizon_prices/phase_10f_b2/phase_10f_b2_acceptance_report.json"))
    parser.add_argument("--config", type=Path, default=Path("configs/pipeline_v2.toml"))
    parser.add_argument("--output-root", type=Path, default=Path("data/pipeline_v2/horizon_prices/phase_10f_d"))
    parser.add_argument("--guard-root", type=Path, default=Path("data/pipeline_v2"))
    parser.add_argument("--expected-planner-sha256", default=EXPECTED_PLANNER_HASH)
    parser.add_argument("--expected-market-metadata-sha256", default=EXPECTED_MARKET_HASH)
    parser.add_argument("--expected-b2-report-sha256", default=EXPECTED_B2_HASH)
    parser.add_argument("--expected-b2-acceptance-sha256", default=EXPECTED_B2_ACCEPTANCE_HASH)
    parser.add_argument("--max-generated-bytes", type=int, default=5 * 1024**3)
    parser.add_argument("--min-free-bytes", type=int, default=80 * 1024**3)
    parser.add_argument("--preflight-only", action="store_true")
    return parser.parse_args(argv)


if __name__ == "__main__":
    print(json.dumps(run(parse_args()), indent=2, sort_keys=True))
