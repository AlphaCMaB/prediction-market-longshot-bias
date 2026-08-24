"""Acquire and freeze prices for the immutable Phase 10F-D PR2 sample."""

from __future__ import annotations

import argparse
from collections import Counter
import csv
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import sys
import time
from typing import Any, Mapping, Sequence

import requests

from scripts.common.time_utils import parse_iso_utc
from scripts.pipeline_v2.build_phase_10f_d_sampling_manifest import CONTRACT_FIELDS
from scripts.pipeline_v2.kalshi_metadata_cache import (
    CacheError,
    StorageBudget,
    canonical_json,
)
from scripts.pipeline_v2.phase_10f_b2 import (
    B2ValidationError,
    extract_observation,
    route_for_settlement,
)
from scripts.pipeline_v2.phase_10f_e import (
    PriceFreezeError,
    attrition_counts,
    classify_price_observability,
    distribution,
    grouped_metrics,
    sample_metrics,
)
from scripts.pipeline_v2.run_phase_10f_b2 import (
    BoundedNetworkClient,
    _publish,
    _sha256,
)
from scripts.pipeline_v2.study_rules import (
    load_study_rules,
    validate_research_feature_columns,
)


SCHEMA_VERSION = "phase-10f-e-pr2-price-freeze-v1"
SAMPLE_COMMIT_IDENTITY = (
    "8a95158441c245988d2562b732762d9a6f3c5c9cd6d0bb33b9fcc6f3b8de2bc9"
)
SAMPLE_COMMIT_SHA256 = (
    "832e6f6e8d5ad19403b1200a1bc2142a3f8339c7926cfd55ab271def46690c96"
)
CONTRACT_MANIFEST_SHA256 = (
    "aaaebdc86f01df5d1c6aecc9b985beb0638ebe6db24e8422930d1496e0752c4d"
)
MARKET_METADATA_SHA256 = (
    "7acd4b59afc1ee0d952396cecb062e4216259c0ff4cb4893d5a8e00c50e26c44"
)
STUDY_RULES_FINGERPRINT = (
    "12d6955f57b50b5587fdadf02b2bc96e7de48d022c9ac3cc2fe0425d907b9901"
)
B2_ACCEPTANCE_SHA256 = (
    "c7821ee78ea3f9b3e150b9c51e439fdc658927da7413ccc022ddb2bd6e5814b0"
)
EXPECTED_FAMILIES = 5000
EXPECTED_CONTRACTS = 11573
PARTITION_SIZE = 100
MAX_ATTEMPTS_PER_LOGICAL_REQUEST = 3
FINALIZATION_RESERVE_BYTES = 4 * 1024**2
PRIMARY_FAMILY_THRESHOLD = 500
PRIMARY_FAMILY_ESS_THRESHOLD = 500

TECHNICAL_FIELDS = (
    "routing_tier",
    "market_open_time",
    "hours_since_market_open",
    "request_id",
    "http_status",
    "request_success",
    "failure_kind",
    "empty_response",
    "candle_count",
    "schema_variant",
    "earliest_end_period_ts",
    "latest_end_period_ts",
    "post_target_candle_count",
    "duplicate_candle_count",
    "missing_bid",
    "missing_ask",
    "yes_bid",
    "yes_ask",
    "midpoint",
    "spread",
    "midpoint_observation_time",
    "midpoint_staleness_minutes",
    "midpoint_within_15m",
    "midpoint_within_60m",
    "trade_close",
    "trade_observation_time",
    "trade_staleness_minutes",
    "trade_within_15m",
    "trade_within_60m",
    "previous_trade_used",
    "midpoint_observability_status",
    "trade_observability_status",
    "api_or_data_failure",
    "no_pre_target_candle",
    "midpoint_too_stale",
    "no_trade",
    "trade_too_stale",
)
NORMALIZED_FIELDS = (*CONTRACT_FIELDS, *TECHNICAL_FIELDS)
ANALYSIS_FIELDS = (
    "price_sample_name",
    "contract_sample_index",
    "family_sample_index",
    "family_id",
    "family_id_source",
    "event_ticker",
    "ticker",
    "rule",
    "category",
    "timing_structure",
    "verified_anchor_time",
    "target_time",
    "verified_source",
    "anchor_month",
    "family_size_bin",
    "eligible_contract_count",
    "sampled_contract_count",
    "stratum_family_count",
    "stratum_sampled_family_count",
    "pi_family",
    "pi_contract_given_family",
    "pi_contract",
    "family_weight_raw",
    "contract_weight_raw",
    "yes_bid",
    "yes_ask",
    "midpoint",
    "spread",
    "midpoint_observation_time",
    "midpoint_staleness_minutes",
    "trade_close",
    "trade_observation_time",
    "trade_staleness_minutes",
    "midpoint_observability_status",
    "trade_observability_status",
)

BOOL_FIELDS = frozenset(
    {
        "request_success",
        "empty_response",
        "missing_bid",
        "missing_ask",
        "midpoint_within_15m",
        "midpoint_within_60m",
        "trade_within_15m",
        "trade_within_60m",
        "previous_trade_used",
        "api_or_data_failure",
        "no_pre_target_candle",
        "midpoint_too_stale",
        "no_trade",
        "trade_too_stale",
    }
)
INT_FIELDS = frozenset(
    {
        "contract_sample_index",
        "family_sample_index",
        "eligible_contract_count",
        "sampled_contract_count",
        "stratum_family_count",
        "stratum_sampled_family_count",
        "http_status",
        "candle_count",
        "earliest_end_period_ts",
        "latest_end_period_ts",
        "post_target_candle_count",
        "duplicate_candle_count",
    }
)
FLOAT_FIELDS = frozenset(
    {
        "pi_family",
        "pi_contract_given_family",
        "pi_contract",
        "family_weight_raw",
        "contract_weight_raw",
        "hours_since_market_open",
        "yes_bid",
        "yes_ask",
        "midpoint",
        "spread",
        "midpoint_staleness_minutes",
        "trade_close",
        "trade_staleness_minutes",
    }
)


class PhaseEError(RuntimeError):
    pass


def _verify(path: Path, expected: str, label: str) -> str:
    actual = _sha256(path)
    if actual != expected:
        raise PhaseEError(
            f"{label} SHA-256 mismatch: expected={expected} actual={actual}"
        )
    return actual


def _json_bytes(value: Any, *, pretty: bool = False) -> bytes:
    if pretty:
        return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode()
    return canonical_json(value) + b"\n"


def _gzip_csv(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in fields})
    return gzip.compress(buffer.getvalue().encode(), compresslevel=9, mtime=0)


def _read_gzip_csv(path: Path, *, typed: bool = False) -> list[dict[str, Any]]:
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
    return [_typed_row(row) for row in rows] if typed else rows


def _typed_row(row: Mapping[str, str]) -> dict[str, Any]:
    result: dict[str, Any] = dict(row)
    for field in BOOL_FIELDS:
        if field in result:
            result[field] = str(result[field]).casefold() == "true"
    for field in INT_FIELDS:
        if field in result:
            result[field] = int(result[field]) if result[field] != "" else None
    for field in FLOAT_FIELDS:
        if field in result:
            result[field] = float(result[field]) if result[field] != "" else None
    return result


def _commit_identity(commit: Mapping[str, Any]) -> str:
    payload = dict(commit)
    identity = str(payload.pop("commit_identity", ""))
    if hashlib.sha256(_json_bytes(payload)).hexdigest() != identity:
        raise PhaseEError("Phase 10F-D commit identity is invalid")
    return identity


def _load_frozen_sample(
    contract_manifest: Path, sample_commit_path: Path
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    _verify(sample_commit_path, SAMPLE_COMMIT_SHA256, "Phase 10F-D commit")
    _verify(contract_manifest, CONTRACT_MANIFEST_SHA256, "contract manifest")
    commit = json.loads(sample_commit_path.read_text())
    if _commit_identity(commit) != SAMPLE_COMMIT_IDENTITY:
        raise PhaseEError("frozen sample commit identity changed")
    if (
        commit["artifact_hashes"].get(contract_manifest.name)
        != CONTRACT_MANIFEST_SHA256
    ):
        raise PhaseEError("sample commit does not pin the contract manifest")
    rows = _read_gzip_csv(contract_manifest)
    validate_research_feature_columns(rows[0].keys() if rows else ())
    identities = {(row["family_id"], row["family_id_source"]) for row in rows}
    contracts = {
        (row["family_id"], row["family_id_source"], row["ticker"]) for row in rows
    }
    if len(rows) != EXPECTED_CONTRACTS or len(contracts) != EXPECTED_CONTRACTS:
        raise PhaseEError("frozen sample must contain exactly 11,573 unique contracts")
    if len(identities) != EXPECTED_FAMILIES:
        raise PhaseEError("frozen sample must contain exactly 5,000 unique families")
    per_family = Counter((row["family_id"], row["family_id_source"]) for row in rows)
    if max(per_family.values()) > 3:
        raise PhaseEError("frozen sample exceeds the three-contract family cap")
    for row in rows:
        M_i = int(row["eligible_contract_count"])
        m_i = int(row["sampled_contract_count"])
        N_h = int(row["stratum_family_count"])
        n_h = int(row["stratum_sampled_family_count"])
        pi_f = float(row["pi_family"])
        pi_c = float(row["pi_contract_given_family"])
        pi = float(row["pi_contract"])
        if not (
            pi_f == n_h / N_h
            and pi_c == m_i / M_i
            and pi == pi_f * pi_c
            and float(row["contract_weight_raw"]) == 1 / pi
            and float(row["family_weight_raw"]) == 1 / (pi * M_i)
        ):
            raise PhaseEError("frozen sample inclusion probability or weight changed")
    return sorted(rows, key=lambda row: int(row["contract_sample_index"])), commit


def _attach_routing_metadata(
    rows: Sequence[Mapping[str, str]], market_metadata: Path
) -> dict[str, dict[str, str]]:
    try:
        import pyarrow as pa
        import pyarrow.compute as pc
        import pyarrow.csv as arrow_csv
    except ImportError as exc:
        raise PhaseEError("Phase 10F-E requires bundled pyarrow") from exc
    tickers = {str(row["ticker"]): row for row in rows}
    required = (
        "ticker",
        "family_id",
        "family_id_source",
        "event_ticker",
        "open_time",
        "diagnostic_settlement_ts",
    )
    with gzip.open(market_metadata, "rt", encoding="utf-8", newline="") as handle:
        header = next(csv.reader(handle))
    if not set(required).issubset(header):
        raise PhaseEError("market routing metadata schema changed")
    ticker_values = pa.array(sorted(tickers))
    found: dict[str, dict[str, str]] = {}
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
            mask = pc.is_in(batch.column("ticker"), value_set=ticker_values)
            if not pc.any(mask).as_py():
                continue
            for source in batch.filter(mask).to_pylist():
                ticker = str(source["ticker"])
                plan = tickers.get(ticker)
                if plan is None or ticker in found:
                    raise PhaseEError("routing ticker is duplicate or outside sample")
                if (
                    source["family_id"] != plan["family_id"]
                    or source["family_id_source"] != plan["family_id_source"]
                    or source["event_ticker"] != plan["event_ticker"]
                ):
                    raise PhaseEError("routing metadata identity changed")
                if not source["diagnostic_settlement_ts"] or not source["open_time"]:
                    raise PhaseEError(
                        "sampled ticker lacks exact routing/open timestamp"
                    )
                found[ticker] = source
    if set(found) != set(tickers):
        raise PhaseEError("routing metadata does not cover the frozen sample")
    return found


def _preflight(
    budget: StorageBudget,
    rows: Sequence[Mapping[str, Any]],
    b2_acceptance: Mapping[str, Any],
) -> dict[str, Any]:
    state = budget.snapshot()
    per_ticker = sum(
        float(b2_acceptance[field])
        for field in (
            "measured_compressed_raw_bytes_per_ticker_request",
            "measured_normalized_bytes_per_ticker",
            "measured_request_commit_and_manifest_bytes_per_ticker",
        )
    )
    projected = math.ceil(len(rows) * per_ticker)
    reserve = projected + FINALIZATION_RESERVE_BYTES
    budget.check_additional(reserve)
    return {
        "schema_version": SCHEMA_VERSION,
        "passed": True,
        "sample_commit_identity": SAMPLE_COMMIT_IDENTITY,
        "sampled_families": len(
            {(row["family_id"], row["family_id_source"]) for row in rows}
        ),
        "sampled_contracts": len(rows),
        "logical_request_budget": {
            "cutoff": 1,
            "sample_tickers": len(rows),
            "boundary_probe": 1,
            "total": len(rows) + 2,
        },
        "maximum_attempts_per_logical_request": MAX_ATTEMPTS_PER_LOGICAL_REQUEST,
        "projected_acquisition_bytes": projected,
        "finalization_reserve_bytes": FINALIZATION_RESERVE_BYTES,
        "storage_before": state,
        "projected_namespace_headroom_after": state["remaining_budget_bytes"] - reserve,
        "projected_free_margin_after": state["free_space_margin_bytes"] - reserve,
        "network_requests_made": 0,
    }


def _partition_identity(index: int, rows: Sequence[Mapping[str, str]]) -> str:
    projection = {
        "schema_version": SCHEMA_VERSION,
        "sample_commit_identity": SAMPLE_COMMIT_IDENTITY,
        "partition_index": index,
        "contract_sample_indices": [int(row["contract_sample_index"]) for row in rows],
        "tickers": [row["ticker"] for row in rows],
    }
    return hashlib.sha256(canonical_json(projection)).hexdigest()[:24]


def _partition_paths(root: Path, index: int) -> tuple[Path, Path, Path, Path]:
    partition = root / "partitions" / f"partition_{index:04d}"
    return (
        partition,
        partition / "normalized.csv.gz",
        partition / "request_manifest.jsonl",
        partition / "partition_commit.json",
    )


def _load_complete_partition(
    output_root: Path, index: int, identity: str
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]] | None:
    _, normalized_path, request_path, commit_path = _partition_paths(output_root, index)
    if not commit_path.exists():
        return None
    commit = json.loads(commit_path.read_text())
    if (
        commit.get("partition_identity") != identity
        or commit.get("sample_commit_identity") != SAMPLE_COMMIT_IDENTITY
    ):
        raise PhaseEError("partition identity changed")
    for name, path in (
        ("normalized", normalized_path),
        ("request_manifest", request_path),
    ):
        if _sha256(path) != commit["artifact_hashes"][name]:
            raise PhaseEError("partition artifact hash changed")
    rows = _read_gzip_csv(normalized_path, typed=True)
    requests_rows = [json.loads(line) for line in request_path.read_text().splitlines()]
    if (
        len(rows) != commit["contract_count"]
        or len(requests_rows) != commit["contract_count"]
    ):
        raise PhaseEError("partition row count changed")
    return rows, requests_rows, commit


def _hours_open(open_time: str, target_time: str) -> float:
    opened = parse_iso_utc(open_time)
    target = parse_iso_utc(target_time)
    if opened is None or target is None or opened > target:
        raise PhaseEError("sampled market did not exist by target")
    return (target - opened).total_seconds() / 3600


def _run_partition(
    *,
    output_root: Path,
    index: int,
    rows: Sequence[Mapping[str, str]],
    routes: Mapping[str, str],
    routing_metadata: Mapping[str, Mapping[str, str]],
    cutoff_hash: str,
    budget: StorageBudget,
    session: Any,
    base_url: str,
    requests_per_second: float,
    max_retries: int,
    timeout_seconds: float,
    no_network: bool,
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], dict[str, Any]]:
    identity = _partition_identity(index, rows)
    complete = _load_complete_partition(output_root, index, identity)
    if complete is not None:
        normalized, commits, partition_commit = complete
        return (
            normalized,
            commits,
            {
                "partition_index": index,
                "partition_identity": identity,
                "resumed_complete": True,
                "physical_requests": 0,
                "retries": 0,
                "rate_limits": 0,
                "network_seconds": float(partition_commit.get("network_seconds", 0.0)),
            },
        )
    partition_root, normalized_path, request_path, commit_path = _partition_paths(
        output_root, index
    )
    client = BoundedNetworkClient(
        session=session,
        output_root=partition_root,
        budget=budget,
        base_url=base_url,
        max_requests=max(1, len(rows) * MAX_ATTEMPTS_PER_LOGICAL_REQUEST),
        max_retries=max_retries,
        timeout_seconds=timeout_seconds,
        requests_per_second=requests_per_second,
        network_forbidden=no_network,
    )
    normalized: list[dict[str, Any]] = []
    commits: list[dict[str, Any]] = []
    for plan in rows:
        ticker = plan["ticker"]
        target = parse_iso_utc(plan["target_time"])
        if target is None:
            raise PhaseEError("sample target is invalid")
        target_ts = int(target.timestamp())
        candles, commit = client.fetch_candles(
            ticker=ticker,
            route=routes[ticker],
            start_ts=target_ts - 3600,
            end_ts=target_ts,
            cutoff_hash=cutoff_hash,
        )
        observation = extract_observation(candles, target_ts=target_ts)
        technical = {
            "routing_tier": routes[ticker],
            "market_open_time": routing_metadata[ticker]["open_time"],
            "hours_since_market_open": _hours_open(
                routing_metadata[ticker]["open_time"], plan["target_time"]
            ),
            "request_id": commit["request_id"],
            "http_status": commit["http_status"],
            "request_success": commit["success"],
            "failure_kind": commit["failure_kind"],
            "empty_response": bool(commit["success"] and not candles),
            **observation,
        }
        classification = classify_price_observability(technical)
        normalized.append({**plan, **technical, **classification})
        commits.append({**commit, "partition_index": index})
    normalized_content = _gzip_csv(normalized, NORMALIZED_FIELDS)
    request_content = b"".join(
        canonical_json(commit) + b"\n"
        for commit in sorted(commits, key=lambda item: item["request_id"])
    )
    _publish(budget, normalized_path, normalized_content)
    _publish(budget, request_path, request_content)
    partition_commit = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "sample_commit_identity": SAMPLE_COMMIT_IDENTITY,
        "partition_index": index,
        "partition_identity": identity,
        "contract_count": len(rows),
        "first_contract_sample_index": int(rows[0]["contract_sample_index"]),
        "last_contract_sample_index": int(rows[-1]["contract_sample_index"]),
        "artifact_hashes": {
            "normalized": _sha256(normalized_path),
            "request_manifest": _sha256(request_path),
        },
        "physical_requests": client.physical_requests,
        "retries": sum(int(item.get("retries", 0)) for item in commits),
        "rate_limits": sum(int(item.get("rate_limits", 0)) for item in commits),
        "network_seconds": client.elapsed_network_seconds,
    }
    _publish(budget, commit_path, _json_bytes(partition_commit))
    return (
        normalized,
        commits,
        {
            "partition_index": index,
            "partition_identity": identity,
            "resumed_complete": False,
            "physical_requests": client.physical_requests,
            "retries": partition_commit["retries"],
            "rate_limits": partition_commit["rate_limits"],
            "network_seconds": client.elapsed_network_seconds,
        },
    )


def _no_network_replay(
    *,
    args: argparse.Namespace,
    budget: StorageBudget,
    frozen_rows: Sequence[Mapping[str, str]],
    routes: Mapping[str, str],
    cutoff_hash: str,
    boundary: Mapping[str, Any],
) -> dict[str, Any]:
    controls = BoundedNetworkClient(
        session=None,
        output_root=args.output_root / "controls",
        budget=budget,
        base_url=args.base_url,
        max_requests=10,
        network_forbidden=True,
    )
    controls.fetch_cutoff()
    hits = controls.resume_hits
    for index, start in enumerate(range(0, len(frozen_rows), PARTITION_SIZE), 1):
        partition_rows = frozen_rows[start : start + PARTITION_SIZE]
        client = BoundedNetworkClient(
            session=None,
            output_root=_partition_paths(args.output_root, index)[0],
            budget=budget,
            base_url=args.base_url,
            max_requests=max(1, len(partition_rows) * MAX_ATTEMPTS_PER_LOGICAL_REQUEST),
            network_forbidden=True,
        )
        for plan in partition_rows:
            target = parse_iso_utc(plan["target_time"])
            if target is None:
                raise PhaseEError("sample target is invalid during replay")
            target_ts = int(target.timestamp())
            client.fetch_candles(
                ticker=plan["ticker"],
                route=routes[plan["ticker"]],
                start_ts=target_ts - 3600,
                end_ts=target_ts,
                cutoff_hash=cutoff_hash,
            )
        hits += client.resume_hits
    probe_target = int(boundary["target_ts"])
    controls.fetch_candles(
        ticker=str(boundary["ticker"]),
        route=routes[str(boundary["ticker"])],
        start_ts=probe_target - 60,
        end_ts=probe_target - 1,
        cutoff_hash=cutoff_hash,
        purpose="boundary_end_minus_one",
    )
    hits += controls.resume_hits - 1
    expected = len(frozen_rows) + 2
    if hits != expected or controls.physical_requests != 0:
        raise PhaseEError("deterministic no-network replay failed")
    return {
        "passed": True,
        "validated_request_commits": hits,
        "expected_request_commits": expected,
        "physical_network_requests": 0,
    }


def _analysis_rows(
    rows: Sequence[Mapping[str, Any]], *, sample_name: str, flag: str
) -> list[dict[str, Any]]:
    return [
        {
            **{field: row.get(field, "") for field in ANALYSIS_FIELDS},
            "price_sample_name": sample_name,
        }
        for row in rows
        if bool(row.get(flag))
    ]


def _breakdown_diagnostics(
    rows: Sequence[Mapping[str, Any]], *, flag: str
) -> dict[str, Any]:
    observable = [row for row in rows if bool(row.get(flag))]
    missing = [row for row in rows if not bool(row.get(flag))]
    return {
        "overall": sample_metrics(rows, flag=flag),
        "by_anchor_month": grouped_metrics(rows, flag=flag, field="anchor_month"),
        "by_family_size_bin": grouped_metrics(rows, flag=flag, field="family_size_bin"),
        "by_sampling_stratum": grouped_metrics(
            [
                {
                    **row,
                    "sampling_stratum": f"{row['anchor_month']}|{row['family_size_bin']}",
                }
                for row in rows
            ],
            flag=flag,
            field="sampling_stratum",
        ),
        "by_target_utc_hour": grouped_metrics(
            [
                {**row, "target_utc_hour": str(row["target_time"])[11:13]}
                for row in rows
            ],
            flag=flag,
            field="target_utc_hour",
        ),
        "hours_since_market_open": {
            "observable": distribution(
                [float(row["hours_since_market_open"]) for row in observable]
            ),
            "missing": distribution(
                [float(row["hours_since_market_open"]) for row in missing]
            ),
        },
        "timing_structure": grouped_metrics(rows, flag=flag, field="timing_structure"),
        "contract_position": "not available in the frozen outcome-blind manifest",
    }


def _validate_no_post_target(rows: Sequence[Mapping[str, Any]]) -> None:
    for row in rows:
        target = parse_iso_utc(row["target_time"])
        if target is None:
            raise PhaseEError("invalid target in normalized prices")
        target_ts = int(target.timestamp())
        for field in ("latest_end_period_ts", "earliest_end_period_ts"):
            value = row.get(field)
            if value is not None and int(value) > target_ts:
                raise PhaseEError("post-target candle entered normalized prices")
        if bool(row.get("previous_trade_used")):
            raise PhaseEError("previous_trade entered normalized prices")


def _finalize(
    *,
    args: argparse.Namespace,
    budget: StorageBudget,
    rows: list[dict[str, Any]],
    sample_commits: list[dict[str, Any]],
    cutoff: Mapping[str, Any],
    cutoff_commit: Mapping[str, Any],
    boundary: Mapping[str, Any],
    boundary_commit: Mapping[str, Any],
    partition_stats: Sequence[Mapping[str, Any]],
    preflight: Mapping[str, Any],
    input_hashes: Mapping[str, str],
    routes: Mapping[str, str],
    wall_seconds: float,
    no_network_replay: Mapping[str, Any],
) -> dict[str, Any]:
    _validate_no_post_target(rows)
    validate_research_feature_columns(NORMALIZED_FIELDS)
    validate_research_feature_columns(ANALYSIS_FIELDS)
    primary = _analysis_rows(
        rows, sample_name="primary_midpoint_15m", flag="midpoint_within_15m"
    )
    midpoint60 = _analysis_rows(
        rows, sample_name="robustness_midpoint_60m", flag="midpoint_within_60m"
    )
    trade15 = _analysis_rows(
        rows, sample_name="robustness_trade_close_15m", flag="trade_within_15m"
    )
    trade60 = _analysis_rows(
        rows, sample_name="robustness_trade_close_60m", flag="trade_within_60m"
    )
    sample_definitions = {
        "primary_midpoint_15m": primary,
        "robustness_midpoint_60m": midpoint60,
        "robustness_trade_close_15m": trade15,
        "robustness_trade_close_60m": trade60,
    }
    normalized_content = _gzip_csv(rows, NORMALIZED_FIELDS)
    sample_contents = {
        f"phase_10f_e_{name}.csv.gz": _gzip_csv(sample_rows, ANALYSIS_FIELDS)
        for name, sample_rows in sample_definitions.items()
    }
    all_commits = [dict(cutoff_commit), *sample_commits, dict(boundary_commit)]
    request_manifest_content = b"".join(
        canonical_json(commit) + b"\n"
        for commit in sorted(all_commits, key=lambda item: item["request_id"])
    )
    measures = {
        "primary_midpoint_15m": sample_metrics(rows, flag="midpoint_within_15m"),
        "robustness_midpoint_60m": sample_metrics(rows, flag="midpoint_within_60m"),
        "robustness_trade_close_15m": sample_metrics(rows, flag="trade_within_15m"),
        "robustness_trade_close_60m": sample_metrics(rows, flag="trade_within_60m"),
    }
    spreads = [float(row["spread"]) for row in rows if row.get("midpoint_within_15m")]
    spread_report = {
        **distribution(spreads),
        "spread_lte_0_20_sensitivity_contracts": sum(
            value <= 0.20 for value in spreads
        ),
        "spread_lte_0_10_sensitivity_contracts": sum(
            value <= 0.10 for value in spreads
        ),
    }
    observability = {
        "schema_version": SCHEMA_VERSION,
        "primary": _breakdown_diagnostics(rows, flag="midpoint_within_15m"),
        "robustness": measures,
        "spread": spread_report,
        "attrition": attrition_counts(rows),
        "observation_propensity_correction_applied": False,
        "missing_price_is_anchor_failure": False,
        "outcome_fields_accessed": 0,
    }
    observability_content = _json_bytes(observability, pretty=True)
    successful = [commit for commit in sample_commits if commit["success"]]
    acquisition = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "sample_commit_identity": SAMPLE_COMMIT_IDENTITY,
        "requests_attempted": len(sample_commits),
        "requests_succeeded": len(successful),
        "routing_attempt_counts": dict(sorted(Counter(routes.values()).items())),
        "routing_success_counts": dict(
            sorted(Counter(commit["request"]["route"] for commit in successful).items())
        ),
        "empty_responses": sum(row["empty_response"] for row in rows),
        "api_failures": sum(not row["request_success"] for row in rows),
        "retries": sum(int(commit.get("retries", 0)) for commit in all_commits),
        "rate_limits": sum(int(commit.get("rate_limits", 0)) for commit in all_commits),
        "candles_returned": sum(int(row["candle_count"]) for row in rows),
        "compressed_raw_bytes": sum(
            int(commit["compressed_bytes"]) for commit in all_commits
        ),
        "uncompressed_response_bytes": sum(
            int(commit["uncompressed_response_bytes"]) for commit in all_commits
        ),
        "physical_network_requests": 1
        + sum(int(commit.get("attempts", 1)) for commit in sample_commits)
        + int(boundary_commit.get("attempts", 1)),
        "wall_clock_seconds": wall_seconds,
        "network_elapsed_seconds": sum(
            float(item.get("network_seconds", 0.0)) for item in partition_stats
        ),
        "requests_per_second": (
            (len(sample_commits) + 2)
            / sum(float(item.get("network_seconds", 0.0)) for item in partition_stats)
            if sum(float(item.get("network_seconds", 0.0)) for item in partition_stats)
            > 0
            else None
        ),
        "partitions": len(partition_stats),
        "partition_size": PARTITION_SIZE,
        "primary_threshold": {
            "required_unique_families": PRIMARY_FAMILY_THRESHOLD,
            "required_family_weighted_ess": PRIMARY_FAMILY_ESS_THRESHOLD,
            "observed_unique_families": measures["primary_midpoint_15m"][
                "usable_unique_families"
            ],
            "observed_family_weighted_ess": measures["primary_midpoint_15m"][
                "family_weighted_ess"
            ],
            "passed": measures["primary_midpoint_15m"]["usable_unique_families"]
            >= PRIMARY_FAMILY_THRESHOLD
            and measures["primary_midpoint_15m"]["family_weighted_ess"]
            >= PRIMARY_FAMILY_ESS_THRESHOLD,
        },
        "measures": measures,
        "spread": spread_report,
        "attrition": observability["attrition"],
        "boundary_validation": boundary,
        "no_adaptive_replacement": True,
        "sample_redrawn": False,
        "pr1_prices_acquired": 0,
        "outcome_fields_accessed": 0,
        "deterministic_no_network_replay": dict(no_network_replay),
        "storage_at_report": budget.snapshot(),
    }
    acquisition_content = _json_bytes(acquisition, pretty=True)
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "input_hashes": dict(input_hashes),
        "sample_commit_identity": SAMPLE_COMMIT_IDENTITY,
        "preflight_sha256": _sha256(args.output_root / "phase_10f_e_preflight.json"),
        "cutoff": {
            "market_settled_ts": cutoff["market_settled_ts"],
            "retrieved_at_utc": cutoff_commit["retrieved_at_utc"],
            "raw_sha256": cutoff_commit["raw_sha256"],
            "routing_use_only": True,
        },
        "boundary": boundary,
        "price_specification": {
            "primary": "latest fully pre-target YES bid/ask midpoint <=15m; no spread cutoff",
            "robustness": [
                "midpoint <=60m",
                "actual trade close <=15m",
                "actual trade close <=60m",
            ],
            "spread_sensitivities": [0.20, 0.10],
            "post_target_allowed": False,
            "previous_trade_allowed": False,
            "fallback_mixing": False,
        },
        "partition_count": len(partition_stats),
        "deterministic_no_network_replay": dict(no_network_replay),
        "settlement_timestamp_used_as_research_feature": False,
        "outcome_fields_accessed": 0,
        "study_rules_changed": False,
    }
    provenance_content = _json_bytes(provenance, pretty=True)
    artifacts: dict[str, bytes] = {
        "phase_10f_e_raw_request_manifest.jsonl": request_manifest_content,
        "phase_10f_e_normalized_prices.csv.gz": normalized_content,
        **sample_contents,
        "phase_10f_e_price_observability_report.json": observability_content,
        "phase_10f_e_provenance.json": provenance_content,
        "phase_10f_e_acquisition_report.json": acquisition_content,
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
        "sample_commit_identity": SAMPLE_COMMIT_IDENTITY,
        "request_commit_count": len(all_commits),
        "partition_count": len(partition_stats),
        "artifacts": refs,
        "primary_threshold_passed": acquisition["primary_threshold"]["passed"],
        "outcome_fields_accessed": 0,
    }
    identity = hashlib.sha256(_json_bytes(final_without_identity)).hexdigest()
    final_content = _json_bytes({**final_without_identity, "commit_identity": identity})
    final_path = args.output_root / "phase_10f_e_commit.json"
    _publish(budget, final_path, final_content)
    return {
        **acquisition,
        "output_hashes": {item["path"]: item["sha256"] for item in refs},
        "final_commit_sha256": _sha256(final_path),
        "final_commit_identity": identity,
        "storage_now": budget.snapshot(),
    }


def _validate_final(args: argparse.Namespace, budget: StorageBudget) -> dict[str, Any]:
    final_path = args.output_root / "phase_10f_e_commit.json"
    commit = json.loads(final_path.read_text())
    payload = dict(commit)
    identity = payload.pop("commit_identity")
    if hashlib.sha256(_json_bytes(payload)).hexdigest() != identity:
        raise PhaseEError("final Phase 10F-E commit identity changed")
    for artifact in commit["artifacts"]:
        if _sha256(args.output_root / artifact["path"]) != artifact["sha256"]:
            raise PhaseEError("final Phase 10F-E artifact changed")
    report = json.loads(
        (args.output_root / "phase_10f_e_acquisition_report.json").read_text()
    )
    return {
        **report,
        "existing_final_reused": True,
        "final_commit_identity": identity,
        "final_commit_sha256": _sha256(final_path),
        "storage_now": budget.snapshot(),
    }


def run(args: argparse.Namespace, *, session: Any | None = None) -> dict[str, Any]:
    input_hashes = {
        "sample_commit": _verify(
            args.sample_commit, SAMPLE_COMMIT_SHA256, "sample commit"
        ),
        "contract_manifest": _verify(
            args.contract_manifest, CONTRACT_MANIFEST_SHA256, "contract manifest"
        ),
        "market_metadata": _verify(
            args.market_metadata, MARKET_METADATA_SHA256, "market metadata"
        ),
        "b2_acceptance": _verify(
            args.b2_acceptance, B2_ACCEPTANCE_SHA256, "B2 acceptance"
        ),
    }
    rules = load_study_rules(args.config)
    if rules.fingerprint != STUDY_RULES_FINGERPRINT:
        raise PhaseEError("StudyRules fingerprint changed")
    input_hashes["study_rules_fingerprint"] = rules.fingerprint
    frozen_rows, _ = _load_frozen_sample(args.contract_manifest, args.sample_commit)
    routing_metadata = _attach_routing_metadata(frozen_rows, args.market_metadata)
    budget = StorageBudget(
        args.guard_root,
        max_bytes=args.max_generated_bytes,
        min_free_bytes=args.min_free_bytes,
    )
    b2_acceptance = json.loads(args.b2_acceptance.read_text())
    preflight = _preflight(budget, frozen_rows, b2_acceptance)
    if args.preflight_only:
        return preflight
    args.output_root.mkdir(parents=True, exist_ok=True)
    preflight_path = args.output_root / "phase_10f_e_preflight.json"
    if preflight_path.exists():
        stored_preflight = json.loads(preflight_path.read_text())
        if stored_preflight.get("sample_commit_identity") != SAMPLE_COMMIT_IDENTITY:
            raise PhaseEError("stored preflight sample identity changed")
    else:
        _publish(budget, preflight_path, _json_bytes(preflight, pretty=True))

    final_path = args.output_root / "phase_10f_e_commit.json"
    if final_path.exists():
        return _validate_final(args, budget)

    active_session = session or requests.Session()
    started = time.monotonic()
    controls_root = args.output_root / "controls"
    controls = BoundedNetworkClient(
        session=active_session,
        output_root=controls_root,
        budget=budget,
        base_url=args.base_url,
        max_requests=10,
        max_retries=args.max_retries,
        timeout_seconds=args.timeout_seconds,
        requests_per_second=args.requests_per_second,
        network_forbidden=args.no_network_resume,
    )
    cutoff, cutoff_commit = controls.fetch_cutoff()
    cutoff_hash = cutoff_commit["raw_sha256"]
    routes = {
        ticker: route_for_settlement(
            source["diagnostic_settlement_ts"], cutoff["market_settled_ts"]
        )
        for ticker, source in routing_metadata.items()
    }

    all_rows: list[dict[str, Any]] = []
    all_commits: list[dict[str, Any]] = []
    partition_stats: list[dict[str, Any]] = []
    partitions = [
        frozen_rows[start : start + PARTITION_SIZE]
        for start in range(0, len(frozen_rows), PARTITION_SIZE)
    ]
    for index, partition_rows in enumerate(partitions, 1):
        if _sha256(args.sample_commit) != SAMPLE_COMMIT_SHA256:
            raise PhaseEError("sample commit changed at partition boundary")
        remaining = len(frozen_rows) - len(all_rows)
        per_ticker = preflight["projected_acquisition_bytes"] / len(frozen_rows)
        budget.check_additional(
            math.ceil(remaining * per_ticker) + FINALIZATION_RESERVE_BYTES
        )
        normalized, commits, stats = _run_partition(
            output_root=args.output_root,
            index=index,
            rows=partition_rows,
            routes=routes,
            routing_metadata=routing_metadata,
            cutoff_hash=cutoff_hash,
            budget=budget,
            session=active_session,
            base_url=args.base_url,
            requests_per_second=args.requests_per_second,
            max_retries=args.max_retries,
            timeout_seconds=args.timeout_seconds,
            no_network=args.no_network_resume,
        )
        all_rows.extend(normalized)
        all_commits.extend(commits)
        partition_stats.append(stats)
        print(
            json.dumps(
                {
                    "phase": "10F-E",
                    "partition": index,
                    "partitions": len(partitions),
                    "completed_contracts": len(all_rows),
                    "physical_requests_this_partition": stats["physical_requests"],
                    "storage": budget.snapshot(),
                },
                sort_keys=True,
            ),
            flush=True,
        )
    if len(all_rows) != EXPECTED_CONTRACTS or len(all_commits) != EXPECTED_CONTRACTS:
        raise PhaseEError("production partitions do not cover the frozen sample")

    exact_candidates = sorted(
        row["ticker"]
        for row in all_rows
        if row.get("latest_end_period_ts") is not None
        and parse_iso_utc(row["target_time"]) is not None
        and int(row["latest_end_period_ts"])
        == int(parse_iso_utc(row["target_time"]).timestamp())
    )
    if not exact_candidates:
        raise PhaseEError("no exact-target candle is available for boundary control")
    probe_ticker = exact_candidates[0]
    probe_row = next(row for row in all_rows if row["ticker"] == probe_ticker)
    probe_target = int(parse_iso_utc(probe_row["target_time"]).timestamp())
    probe_candles, boundary_commit = controls.fetch_candles(
        ticker=probe_ticker,
        route=routes[probe_ticker],
        start_ts=probe_target - 60,
        end_ts=probe_target - 1,
        cutoff_hash=cutoff_hash,
        purpose="boundary_end_minus_one",
    )
    if any(int(row["end_period_ts"]) >= probe_target for row in probe_candles):
        raise PhaseEError("boundary probe returned exact/post-target candle")
    boundary = {
        "passed": True,
        "documented_semantics": "inclusive_end_period_ts",
        "base_exact_target_candle_observed": True,
        "end_minus_one_probe_excluded_exact_target": True,
        "ticker": probe_ticker,
        "target_ts": probe_target,
        "probe_request_id": boundary_commit["request_id"],
        "post_target_information_accepted": False,
    }
    wall_seconds = time.monotonic() - started
    replay = _no_network_replay(
        args=args,
        budget=budget,
        frozen_rows=frozen_rows,
        routes=routes,
        cutoff_hash=cutoff_hash,
        boundary=boundary,
    )
    result = _finalize(
        args=args,
        budget=budget,
        rows=all_rows,
        sample_commits=all_commits,
        cutoff=cutoff,
        cutoff_commit=cutoff_commit,
        boundary=boundary,
        boundary_commit=boundary_commit,
        partition_stats=partition_stats,
        preflight=preflight,
        input_hashes=input_hashes,
        routes=routes,
        wall_seconds=wall_seconds,
        no_network_replay=replay,
    )
    return result


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--contract-manifest",
        type=Path,
        default=Path(
            "data/pipeline_v2/horizon_prices/phase_10f_d/phase_10f_d_contract_sampling_manifest.csv.gz"
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
        "--market-metadata",
        type=Path,
        default=Path(
            "data/pipeline_v2/market_acquisition/partitioned/merged_universes/6f8aa42abec876d3aa1f6336/market_metadata.csv.gz"
        ),
    )
    parser.add_argument(
        "--b2-acceptance",
        type=Path,
        default=Path(
            "data/pipeline_v2/horizon_prices/phase_10f_b2/phase_10f_b2_acceptance_report.json"
        ),
    )
    parser.add_argument("--config", type=Path, default=Path("configs/pipeline_v2.toml"))
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/pipeline_v2/horizon_prices/phase_10f_e"),
    )
    parser.add_argument("--guard-root", type=Path, default=Path("data/pipeline_v2"))
    parser.add_argument(
        "--base-url", default="https://external-api.kalshi.com/trade-api/v2"
    )
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=float, default=45)
    parser.add_argument("--requests-per-second", type=float, default=3)
    parser.add_argument("--max-generated-bytes", type=int, default=5 * 1024**3)
    parser.add_argument("--min-free-bytes", type=int, default=80 * 1024**3)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--no-network-resume", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        result = run(parse_args(argv))
        print(json.dumps(result, sort_keys=True))
        return 0
    except (
        PhaseEError,
        PriceFreezeError,
        B2ValidationError,
        CacheError,
        OSError,
        ValueError,
    ) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
