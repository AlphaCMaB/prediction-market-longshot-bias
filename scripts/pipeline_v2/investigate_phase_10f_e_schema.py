"""Bounded one-request schema investigation for Phase 10F-E sample 11060."""

from __future__ import annotations

import argparse
from collections import Counter
import gzip
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

import requests

from scripts.common.time_utils import parse_iso_utc
from scripts.pipeline_v2.kalshi_metadata_cache import StorageBudget, canonical_json
from scripts.pipeline_v2.phase_10f_b2 import (
    B2ValidationError,
    LIVE_ROUTE,
    extract_observation,
    route_for_settlement,
)
from scripts.pipeline_v2.phase_10f_e import classify_price_observability
from scripts.pipeline_v2.run_phase_10f_b2 import BoundedNetworkClient, _publish, _sha256
from scripts.pipeline_v2.run_phase_10f_e import (
    B2_ACCEPTANCE_SHA256,
    CONTRACT_MANIFEST_SHA256,
    MARKET_METADATA_SHA256,
    SAMPLE_COMMIT_IDENTITY,
    SAMPLE_COMMIT_SHA256,
    _attach_routing_metadata,
    _load_frozen_sample,
    _verify,
)


SAMPLE_INDEX = 11060
TICKER = "KXWCGAME-26JUN25TUNNED-TIE"
MIN_RESUME_FREE_BYTES = 85 * 1024**3
MAX_INVESTIGATION_BYTES = 1024**2
ORIGINAL_ERROR = "unknown or ambiguous live_fixed_point_dollars schema in price"


class InvestigationError(RuntimeError):
    pass


def _json_bytes(value: Any, *, pretty: bool = False) -> bytes:
    if pretty:
        return (json.dumps(value, indent=2, sort_keys=True) + "\n").encode("utf-8")
    return canonical_json(value) + b"\n"


def _presence(value: Any, documented: str, legacy: str) -> dict[str, Any]:
    keys = sorted(str(key) for key in value) if isinstance(value, Mapping) else []
    conflicting = False
    if isinstance(value, Mapping) and documented in value and legacy in value:
        try:
            conflicting = float(value[documented]) != float(value[legacy])
        except (TypeError, ValueError):
            conflicting = True
    return {
        "object": isinstance(value, Mapping),
        "keys": keys,
        documented: documented in value if isinstance(value, Mapping) else False,
        legacy: legacy in value if isinstance(value, Mapping) else False,
        "conflicting_close_values": conflicting,
    }


def inspect_live_payload(
    payload: Any, *, ticker: str, target_ts: int
) -> dict[str, Any]:
    if not isinstance(payload, Mapping) or set(payload) != {"markets"}:
        raise InvestigationError("live response top-level schema changed")
    markets = payload.get("markets")
    if (
        not isinstance(markets, list)
        or len(markets) != 1
        or markets[0].get("market_ticker") != ticker
        or not isinstance(markets[0].get("candlesticks"), list)
    ):
        raise InvestigationError("live response market identity/schema changed")
    candles = markets[0]["candlesticks"]
    observations = []
    quote_ambiguities = 0
    trade_schema_unavailable = 0
    previous_fields_present = 0
    for candle in candles:
        bid = _presence(candle.get("yes_bid"), "close_dollars", "close")
        ask = _presence(candle.get("yes_ask"), "close_dollars", "close")
        price = _presence(candle.get("price"), "close_dollars", "close")
        price_value = candle.get("price")
        price_previous_dollars = bool(
            isinstance(price_value, Mapping) and "previous_dollars" in price_value
        )
        price_previous = bool(
            isinstance(price_value, Mapping) and "previous" in price_value
        )
        previous_fields_present += int(price_previous_dollars or price_previous)
        for side in (bid, ask):
            if (
                not side["object"]
                or not side["close_dollars"]
                or side["conflicting_close_values"]
            ):
                quote_ambiguities += 1
        unavailable = (
            not price["object"] or not price["close_dollars"] or price["close"]
        )
        trade_schema_unavailable += int(unavailable)
        observations.append(
            {
                "end_period_ts": candle.get("end_period_ts"),
                "yes_bid": bid,
                "yes_ask": ask,
                "price": {
                    **price,
                    "previous_dollars": price_previous_dollars,
                    "previous": price_previous,
                },
            }
        )
    timestamps = [int(row["end_period_ts"]) for row in observations]
    key_structures = Counter(
        canonical_json(
            {
                "yes_bid": row["yes_bid"]["keys"],
                "yes_ask": row["yes_ask"]["keys"],
                "price": row["price"]["keys"],
            }
        ).decode("utf-8")
        for row in observations
    )
    return {
        "candle_count": len(observations),
        "end_period_ts_min": min(timestamps) if timestamps else None,
        "end_period_ts_max": max(timestamps) if timestamps else None,
        "post_target_candles": sum(timestamp > target_ts for timestamp in timestamps),
        "duplicate_end_period_ts": len(timestamps) - len(set(timestamps)),
        "quote_ambiguity_count": quote_ambiguities,
        "trade_schema_unavailable_candles": trade_schema_unavailable,
        "candles_with_previous_price_fields": previous_fields_present,
        "literal_key_structures": [
            {"structure": json.loads(structure), "count": count}
            for structure, count in sorted(key_structures.items())
        ],
        "per_candle_key_presence": observations,
    }


def _request_projection(
    *, ticker: str, target_ts: int, cutoff_hash: str
) -> dict[str, Any]:
    return {
        "endpoint": "/markets/candlesticks",
        "params": {
            "market_tickers": ticker,
            "start_ts": target_ts - 3600,
            "end_ts": target_ts,
            "period_interval": 1,
            "include_latest_before_start": "false",
        },
        "purpose": "sample_price_window",
        "route": LIVE_ROUTE,
        "routing_cutoff_sha256": cutoff_hash,
    }


def _preflight(args: argparse.Namespace) -> dict[str, Any]:
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
    rows, _ = _load_frozen_sample(args.contract_manifest, args.sample_commit)
    matches = [row for row in rows if int(row["contract_sample_index"]) == SAMPLE_INDEX]
    if len(matches) != 1 or matches[0]["ticker"] != TICKER:
        raise InvestigationError("sample index 11060 identity changed")
    plan = matches[0]
    routing = _attach_routing_metadata([plan], args.market_metadata)[TICKER]
    budget = StorageBudget(
        args.guard_root,
        max_bytes=args.max_generated_bytes,
        min_free_bytes=args.hard_floor_bytes,
    )
    state = budget.snapshot()
    if state["free_bytes"] < args.resume_min_free_bytes:
        raise InvestigationError("bounded investigation requires at least 85 GiB free")
    budget.check_additional(MAX_INVESTIGATION_BYTES)
    controls = BoundedNetworkClient(
        session=None,
        output_root=args.output_root / "controls",
        budget=budget,
        base_url=args.base_url,
        max_requests=0,
        network_forbidden=True,
    )
    cutoff, cutoff_commit = controls.fetch_cutoff()
    route = route_for_settlement(
        routing["diagnostic_settlement_ts"], cutoff["market_settled_ts"]
    )
    if route != LIVE_ROUTE:
        raise InvestigationError("sample 11060 no longer routes to the live endpoint")
    target = parse_iso_utc(plan["target_time"])
    if target is None:
        raise InvestigationError("sample 11060 target is invalid")
    target_ts = int(target.timestamp())
    request = _request_projection(
        ticker=TICKER, target_ts=target_ts, cutoff_hash=cutoff_commit["raw_sha256"]
    )
    request_id = hashlib.sha256(canonical_json(request)).hexdigest()[:24]
    partition_root = args.output_root / "partitions" / "partition_0111"
    for path in (
        partition_root / "raw" / f"request_{request_id}.json.gz",
        partition_root / "request_commits" / f"request_{request_id}.json",
        partition_root / "raw_captures" / f"request_{request_id}.json",
    ):
        if path.exists():
            raise InvestigationError(f"investigation request already exists: {path}")
    return {
        "schema_version": "phase-10f-e-schema-investigation-v1",
        "passed": True,
        "network_requests_made": 0,
        "maximum_additional_network_requests": 1,
        "sample_commit_identity": SAMPLE_COMMIT_IDENTITY,
        "sample_index": SAMPLE_INDEX,
        "ticker": TICKER,
        "target_ts": target_ts,
        "request_id": request_id,
        "request": request,
        "route": route,
        "cutoff": cutoff,
        "cutoff_commit": cutoff_commit,
        "routing_metadata": routing,
        "input_hashes": input_hashes,
        "storage": state,
    }


def run(args: argparse.Namespace) -> dict[str, Any]:
    preflight = _preflight(args)
    if args.preflight_only:
        return preflight
    budget = StorageBudget(
        args.guard_root,
        max_bytes=args.max_generated_bytes,
        min_free_bytes=args.hard_floor_bytes,
    )
    investigation_root = args.output_root / "schema_investigation_11060"
    original_failure = {
        "schema_version": "phase-10f-e-schema-investigation-v1",
        "sample_index": SAMPLE_INDEX,
        "ticker": TICKER,
        "original_attempt_ordinal": 1,
        "original_raw_response_preserved": False,
        "failure_stage": "normalization_before_raw_publication",
        "failure_message": ORIGINAL_ERROR,
        "production_partition_published": False,
        "outcome_fields_accessed": 0,
    }
    _publish(
        budget,
        investigation_root / "original_failed_attempt.json",
        _json_bytes(original_failure, pretty=True),
    )
    partition_root = args.output_root / "partitions" / "partition_0111"
    client = BoundedNetworkClient(
        session=requests.Session(),
        output_root=partition_root,
        budget=budget,
        base_url=args.base_url,
        max_requests=1,
        max_retries=1,
        timeout_seconds=args.timeout_seconds,
        requests_per_second=args.requests_per_second,
    )
    candles, commit = client.fetch_candles(
        ticker=TICKER,
        route=LIVE_ROUTE,
        start_ts=preflight["target_ts"] - 3600,
        end_ts=preflight["target_ts"],
        cutoff_hash=preflight["cutoff_commit"]["raw_sha256"],
    )
    if client.physical_requests != 1:
        raise InvestigationError(
            "bounded investigation did not make exactly one request"
        )
    raw_path = partition_root / commit["raw_path"]
    wrapper = json.loads(gzip.decompress(raw_path.read_bytes()))
    schema = inspect_live_payload(
        wrapper["response"], ticker=TICKER, target_ts=preflight["target_ts"]
    )
    observation = extract_observation(candles, target_ts=preflight["target_ts"])
    classification = classify_price_observability(
        {
            **observation,
            "request_success": commit["success"],
            "candle_count": len(candles),
        }
    )
    reproduced = schema["trade_schema_unavailable_candles"] > 0
    criteria = {
        "live_quotes_unambiguous": schema["quote_ambiguity_count"] == 0,
        "midpoint_extraction_deterministic": bool(observation["midpoint_valid"]),
        "ambiguity_confined_to_trade": (
            schema["quote_ambiguity_count"] == 0
            and schema["trade_schema_unavailable_candles"] > 0
        ),
        "previous_price_fallback_used": bool(observation["previous_trade_used"]),
        "timestamp_semantics_valid": (
            schema["post_target_candles"] == 0
            and schema["duplicate_end_period_ts"] == 0
        ),
        "legacy_failure_signature_reproduced": reproduced,
    }
    criteria_passed = bool(
        criteria["live_quotes_unambiguous"]
        and criteria["midpoint_extraction_deterministic"]
        and criteria["ambiguity_confined_to_trade"]
        and not criteria["previous_price_fallback_used"]
        and criteria["timestamp_semantics_valid"]
        and criteria["legacy_failure_signature_reproduced"]
    )
    report = {
        "schema_version": "phase-10f-e-schema-investigation-v1",
        "complete": True,
        "sample_commit_identity": SAMPLE_COMMIT_IDENTITY,
        "sample_index": SAMPLE_INDEX,
        "ticker": TICKER,
        "request_id": commit["request_id"],
        "endpoint": commit["request"]["endpoint"],
        "routing_decision": commit["request"]["route"],
        "fetched_cutoff_used": preflight["cutoff"]["market_settled_ts"],
        "market_settlement_timestamp_routing_only": preflight["routing_metadata"][
            "diagnostic_settlement_ts"
        ],
        "http_status": commit["http_status"],
        "content_type": commit.get("content_type", ""),
        "raw_path": str(raw_path.relative_to(args.output_root)),
        "raw_sha256": _sha256(raw_path),
        "raw_published_before_normalization": True,
        "original_failure": original_failure,
        "bounded_retry": {
            "physical_requests": client.physical_requests,
            "attempt_ordinal_for_request": 2,
            "retries": commit["retries"],
            "rate_limits": commit["rate_limits"],
            "reproduced_original_failure_class": reproduced,
            "exact_original_response_comparison_available": False,
        },
        "raw_schema": schema,
        "midpoint": {
            "valid": observation["midpoint_valid"],
            "failure_reason": observation["midpoint_failure_reason"],
            "value": observation["midpoint"],
            "staleness_minutes": observation["midpoint_staleness_minutes"],
            "within_15m": observation["midpoint_within_15m"],
            "within_60m": observation["midpoint_within_60m"],
        },
        "trade_close": {
            "valid": observation["trade_close_valid"],
            "failure_reason": observation["trade_failure_reason"],
            "value": observation["trade_close"],
            "within_15m": observation["trade_within_15m"],
            "within_60m": observation["trade_within_60m"],
        },
        "classification": classification,
        "conditional_resume_criteria": criteria,
        "conditional_resume_criteria_satisfied": criteria_passed,
        "outcome_fields_accessed": 0,
        "storage": budget.snapshot(),
    }
    report_path = investigation_root / "phase_10f_e_schema_investigation_report.json"
    _publish(budget, report_path, _json_bytes(report, pretty=True))
    return {
        **report,
        "report_path": str(report_path),
        "report_sha256": _sha256(report_path),
    }


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
    parser.add_argument(
        "--output-root",
        type=Path,
        default=Path("data/pipeline_v2/horizon_prices/phase_10f_e"),
    )
    parser.add_argument("--guard-root", type=Path, default=Path("data/pipeline_v2"))
    parser.add_argument(
        "--base-url", default="https://external-api.kalshi.com/trade-api/v2"
    )
    parser.add_argument("--requests-per-second", type=float, default=3)
    parser.add_argument("--timeout-seconds", type=float, default=45)
    parser.add_argument("--max-generated-bytes", type=int, default=5 * 1024**3)
    parser.add_argument("--hard-floor-bytes", type=int, default=80 * 1024**3)
    parser.add_argument(
        "--resume-min-free-bytes", type=int, default=MIN_RESUME_FREE_BYTES
    )
    parser.add_argument("--preflight-only", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        print(json.dumps(run(parse_args(argv)), sort_keys=True))
        return 0
    except (InvestigationError, B2ValidationError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
