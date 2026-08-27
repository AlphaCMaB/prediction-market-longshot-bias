"""Build the offline, outcome-blind Phase 10F-A horizon-price plan."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import timedelta
import gzip
import hashlib
import json
import math
import os
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any, Mapping

from scripts.common.time_utils import format_iso_utc, parse_iso_utc
from scripts.pipeline_v2.kalshi_metadata_cache import StorageBudget
from scripts.pipeline_v2.phase_10f_planner import (
    EXISTED,
    OPENED_AFTER,
    PLANNER_SCHEMA_VERSION,
    FamilyPlan,
    classify_market_open,
    plan_to_row,
    projected_batched_requests,
    select_smoke_cases,
)
from scripts.pipeline_v2.study_rules import (
    analysis_window_bounds,
    load_study_rules,
    validate_research_feature_columns,
)


OUTPUT_FILES = (
    "phase_10f_horizon_planner.csv",
    "phase_10f_horizon_planner_report.json",
    "phase_10f_price_source_design.md",
    "phase_10f_storage_preflight.json",
    "phase_10f_smoke_plan.json",
)
PLANNER_FIELDS = (
    "family_id",
    "family_id_source",
    "event_ticker",
    "associated_market_tickers_compact",
    "market_ticker_encoding",
    "verified_anchor_time",
    "target_time",
    "verified_source",
    "rule",
    "category",
    "timing_structure",
    "earliest_market_open_time",
    "market_existence_at_target",
    "market_count",
    "eligible_market_retrieval_count",
    "opened_after_target_market_count",
    "unknown_open_time_market_count",
    "expected_price_history_retrieval_unit",
)
ANCHOR_REQUIRED_FIELDS = frozenset(
    {
        "family_id",
        "family_id_source",
        "rule",
        "category",
        "verified_anchor_time",
        "verified_anchor_source",
        "timing_structure",
    }
)
MARKET_PROJECTION_FIELDS = (
    "ticker",
    "event_ticker",
    "family_id",
    "family_id_source",
    "open_time",
)
SMOKE_QUOTAS = {
    "crypto_no_t_minus_1h": 50,
    "crypto_existed": 30,
    "financials": 25,
    "climate_weather": 15,
    "sports_no_t_minus_1h": 15,
    "sports_existed": 65,
}


def _open_csv(path: Path):
    return (
        gzip.open(path, "rt", encoding="utf-8", newline="")
        if path.suffix == ".gz"
        else path.open("r", encoding="utf-8", newline="")
    )


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _logical_bytes(path: Path) -> int:
    return (
        sum(item.stat().st_size for item in path.rglob("*") if item.is_file())
        if path.exists()
        else 0
    )


def _verify_hash(path: Path, expected: str | None, label: str) -> str:
    actual = _sha256(path)
    if expected and actual != expected:
        raise ValueError(
            f"{label} SHA-256 mismatch: expected={expected} actual={actual}"
        )
    return actual


def _load_anchor_plans(
    path: Path, *, window_start, window_end
) -> dict[tuple[str, str], FamilyPlan]:
    plans: dict[tuple[str, str], FamilyPlan] = {}
    with _open_csv(path) as handle:
        reader = csv.DictReader(handle)
        fields = set(reader.fieldnames or ())
        if not ANCHOR_REQUIRED_FIELDS.issubset(fields):
            raise ValueError(
                "verified-anchor input lacks required outcome-blind fields"
            )
        validate_research_feature_columns(fields)
        for row in reader:
            anchor = parse_iso_utc(row.get("verified_anchor_time"))
            if anchor is None or not (window_start <= anchor < window_end):
                continue
            identity = (
                str(row.get("family_id") or "").strip(),
                str(row.get("family_id_source") or "").strip(),
            )
            if not all(identity) or identity in plans:
                raise ValueError(
                    "verified-anchor identities must be unique and complete"
                )
            target = anchor - timedelta(hours=1)
            plans[identity] = FamilyPlan(
                family_id=identity[0],
                family_id_source=identity[1],
                rule=str(row.get("rule") or ""),
                category=str(row.get("category") or "[uncategorized]"),
                verified_anchor_time=format_iso_utc(anchor),
                target_time=format_iso_utc(target),
                verified_source=str(row.get("verified_anchor_source") or ""),
                timing_structure=str(row.get("timing_structure") or ""),
            )
    if len(plans) != 161343:
        raise ValueError(
            f"expected 161,343 in-window verified families, found {len(plans)}"
        )
    return plans


def _attach_markets(
    path: Path, plans: Mapping[tuple[str, str], FamilyPlan]
) -> dict[str, int]:
    projected_rows = 0
    with _open_csv(path) as handle:
        reader = csv.reader(handle)
        try:
            header = next(reader)
        except StopIteration as exc:
            raise ValueError("market metadata is empty") from exc
        if len(header) != len(set(header)):
            raise ValueError("market metadata contains duplicate column names")
        missing = sorted(set(MARKET_PROJECTION_FIELDS) - set(header))
        if missing:
            raise ValueError(
                f"market metadata lacks planner projection fields: {missing}"
            )
        indices = {field: header.index(field) for field in MARKET_PROJECTION_FIELDS}
        for source in reader:
            identity = (
                source[indices["family_id"]].strip(),
                source[indices["family_id_source"]].strip(),
            )
            plan = plans.get(identity)
            if plan is None:
                continue
            projected_rows += 1
            ticker = source[indices["ticker"]].strip()
            event_ticker = source[indices["event_ticker"]].strip()
            if not ticker or not event_ticker:
                raise ValueError("matched market row lacks ticker identity")
            if plan.event_ticker and plan.event_ticker != event_ticker:
                raise ValueError(f"family {identity!r} maps to multiple event tickers")
            plan.event_ticker = event_ticker
            plan.market_tickers.append(ticker)
            opened_value = source[indices["open_time"]].strip()
            opened = parse_iso_utc(opened_value)
            if opened is not None and (
                plan.earliest_market_open_time is None
                or opened < plan.earliest_market_open_time
            ):
                plan.earliest_market_open_time = opened
            status = classify_market_open(opened_value, plan.target_time)
            if status == EXISTED:
                plan.eligible_market_count += 1
            elif status == OPENED_AFTER:
                plan.opened_after_target_market_count += 1
            else:
                plan.unknown_open_time_market_count += 1
    for identity, plan in plans.items():
        if not plan.market_tickers or not plan.event_ticker:
            raise ValueError(f"verified family lacks market metadata: {identity!r}")
        if len(plan.market_tickers) != len(set(plan.market_tickers)):
            raise ValueError(
                f"verified family contains duplicate market tickers: {identity!r}"
            )
    return {"matched_market_rows": projected_rows}


def _empirical_cache_metrics(path: Path | None) -> dict[str, Any]:
    if path is None or not path.exists():
        return {
            "available": False,
            "compressed_bytes_per_returned_market": None,
            "source_file_count": 0,
        }
    file_hashes = {}
    raw_bytes = gzip_bytes = market_count = candle_count = 0
    for source in sorted(path.glob("*.json")):
        content = source.read_bytes()
        payload = json.loads(content)
        markets = payload.get("markets", [])
        if not isinstance(markets, list):
            raise ValueError("local empirical candlestick cache has invalid schema")
        raw_bytes += len(content)
        gzip_bytes += len(gzip.compress(content, compresslevel=9, mtime=0))
        market_count += len(markets)
        for market in markets:
            candles = market.get("candlesticks", []) if isinstance(market, dict) else []
            candle_count += len(candles) if isinstance(candles, list) else 0
        file_hashes[source.name] = hashlib.sha256(content).hexdigest()
    return {
        "available": bool(file_hashes and market_count),
        "source_directory": str(path),
        "source_file_count": len(file_hashes),
        "source_file_hashes": file_hashes,
        "uncompressed_bytes": raw_bytes,
        "deterministic_gzip_bytes": gzip_bytes,
        "returned_market_count": market_count,
        "candlestick_count": candle_count,
        "compressed_bytes_per_returned_market": (
            gzip_bytes / market_count if market_count else None
        ),
    }


def _storage_model(
    *,
    eligible_markets: int,
    request_count: int,
    empirical: Mapping[str, Any],
    storage_before: Mapping[str, int],
) -> dict[str, Any]:
    empirical_per_market = float(
        empirical.get("compressed_bytes_per_returned_market") or 512
    )
    empirical_raw = math.ceil(eligible_markets * empirical_per_market)
    conservative_raw = eligible_markets * 2048
    empirical_normalized = eligible_markets * 128
    conservative_normalized = eligible_markets * 256
    empirical_manifest = request_count * 512
    conservative_manifest = request_count * 2048
    fixed_reports = 2 * 1024**2
    empirical_total = (
        empirical_raw + empirical_normalized + empirical_manifest + fixed_reports
    )
    conservative_total = (
        conservative_raw
        + conservative_normalized
        + conservative_manifest
        + fixed_reports
    )
    empirical_peak = empirical_total + empirical_normalized
    conservative_peak = conservative_total + conservative_normalized
    return {
        "model_scope": "eligible market tickers, one 60-minute bounded one-minute-candlestick window",
        "empirical_basis": empirical,
        "empirical_projection": {
            "raw_gzip_bytes": empirical_raw,
            "normalized_gzip_bytes": empirical_normalized,
            "manifest_and_provenance_bytes": empirical_manifest + fixed_reports,
            "total_additional_namespace_bytes": empirical_total,
            "temporary_peak_additional_bytes": empirical_peak,
            "projected_namespace_bytes": storage_before["used_bytes"] + empirical_total,
            "projected_free_bytes": storage_before["free_bytes"] - empirical_peak,
            "fits_namespace_ceiling": storage_before["used_bytes"] + empirical_total
            <= storage_before["max_bytes"],
            "fits_free_space_floor": storage_before["free_bytes"] - empirical_peak
            >= storage_before["min_free_bytes"],
        },
        "conservative_projection": {
            "raw_gzip_bytes": conservative_raw,
            "normalized_gzip_bytes": conservative_normalized,
            "manifest_and_provenance_bytes": conservative_manifest + fixed_reports,
            "total_additional_namespace_bytes": conservative_total,
            "temporary_peak_additional_bytes": conservative_peak,
            "projected_namespace_bytes": storage_before["used_bytes"]
            + conservative_total,
            "projected_free_bytes": storage_before["free_bytes"] - conservative_peak,
            "fits_namespace_ceiling": storage_before["used_bytes"] + conservative_total
            <= storage_before["max_bytes"],
            "fits_free_space_floor": storage_before["free_bytes"] - conservative_peak
            >= storage_before["min_free_bytes"],
        },
        "production_acquisition_authorized": False,
    }


def _source_design(report: Mapping[str, Any]) -> str:
    requests = report["projected_price_history_requests"]
    markets = report["eligible_market_retrieval_count"]
    return f"""# Phase 10F-A price-source design

This design is outcome-blind and offline. It does not approve or perform a price-history request.

## Recommended acquisition source

Use the existing multi-market Kalshi candlestick endpoint as the leading source for a bounded smoke, after its cache is upgraded to deterministic gzip, atomic publication, immutable request manifests, and partition resume. The local client uses `GET /trade-api/v2/markets/candlesticks`, accepts multiple market tickers, one shared `start_ts`/`end_ts`, one-minute intervals, and returns per-market candlesticks with trade-price and closing yes-bid/yes-ask fields. Grouping the {markets:,} offline-eligible contracts by exact target timestamp and batches of 100 projects {requests:,} minimum logical requests. Recursive split, retries, or endpoint limits can only increase that number.

| Candidate source | Temporal precision | Prices available in local evidence | Batching/pagination | Historical behavior | Main advantages | Main limitations and look-ahead controls |
|---|---|---|---|---|---|---|
| Multi-market candlesticks | One minute | Trade close/previous and closing yes bid/ask | Up to 100 tickers per shared time window in the current client; no response pagination implemented | Cutoff/routing semantics are not validated offline and must be tested in the smoke | Lowest projected requests; bounded timestamps; quote and trade diagnostics in one immutable response | Current cache is uncompressed and non-atomic; current extractor silently mixes midpoint/trade fallbacks; never accept a candle after target |
| Per-market/series candlesticks | One minute | Same candlestick fields | One market per request in legacy code | Not revalidated | Simple identity and failure isolation | Roughly one request per eligible contract; materially inefficient and unsuitable for production scale |
| Historical trades | Event timestamp precision if the endpoint exposes complete history | Actual transactions only | No validated V2 client; likely cursor pagination per ticker/time range | Unknown offline | Direct interpretation as transacted probability | Sparse trading, pagination, much higher request burden, and no contemporaneous spread; must not be assumed available |
| Historical order book | Potentially quote-event precision | Bid/ask and executable depth if true historical snapshots exist | No local endpoint or client | No historical availability established | Best microstructure detail | Live order books cannot reconstruct past quotes; using a current book would be look-ahead leakage and is prohibited |

## Analytical price-definition decision

The existing extractor is not acceptable unchanged because it prioritizes midpoint, then trade close, then a previous trade, silently mixing economically different measures.

Recommended design for approval: use the contemporaneous closing yes-bid/yes-ask midpoint as the primary implied probability only when both sides are present in the latest candle at or before target and within 15 minutes. Compute actual trade close as a separately labelled robustness measure rather than a fallback. Repeat both definitions with the predeclared 60-minute robustness staleness threshold. Never use `previous_trade` without its true trade timestamp.

The midpoint is temporally aligned and less dependent on a recent transaction, but can be distorted by wide spreads or shallow quotes and is not itself a transacted price. A last trade is directly observed but can be stale and gives greater attrition in thin markets. This choice materially changes interpretation and sample composition, so explicit project-owner approval is required before the smoke extraction logic is frozen.

## Required Phase 10F-B changes before a smoke

1. Pin the endpoint and request schema against official documentation without changing StudyRules.
2. Use a bounded 60-minute window ending exactly at target; reject every post-target candle.
3. Publish raw responses as deterministic gzip with atomic write-once semantics and request/response hashes.
4. Partition the 200-family smoke independently and support no-network resume.
5. Keep anchor validity, market existence, observation availability, staleness, and sample inclusion as separate fields.
"""


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


def run(
    anchors_path: Path,
    market_metadata_path: Path,
    output_root: Path,
    *,
    config_path: Path,
    guard_root: Path,
    empirical_cache_dir: Path | None = None,
    expected_anchors_sha256: str | None = None,
    expected_market_metadata_sha256: str | None = None,
    max_generated_bytes: int = 5 * 1024**3,
    min_free_bytes: int = 80 * 1024**3,
    preflight_only: bool = False,
) -> dict[str, Any]:
    rules = load_study_rules(config_path)
    start, end = analysis_window_bounds(rules)
    input_hashes = {
        "verified_anchors": _verify_hash(
            anchors_path, expected_anchors_sha256, "verified anchors"
        ),
        "market_metadata": _verify_hash(
            market_metadata_path, expected_market_metadata_sha256, "market metadata"
        ),
    }
    plans = _load_anchor_plans(anchors_path, window_start=start, window_end=end)
    market_scan = _attach_markets(market_metadata_path, plans)
    batch_size = 100
    eligible_by_target = Counter()
    for plan in plans.values():
        eligible_by_target[plan.target_time] += plan.eligible_market_count
    request_count = projected_batched_requests(eligible_by_target, batch_size)
    empirical = _empirical_cache_metrics(empirical_cache_dir)
    budget = StorageBudget(
        guard_root, max_bytes=max_generated_bytes, min_free_bytes=min_free_bytes
    )
    storage_before = budget.snapshot()
    storage_model = _storage_model(
        eligible_markets=sum(eligible_by_target.values()),
        request_count=request_count,
        empirical=empirical,
        storage_before=storage_before,
    )

    existence_counts = Counter(
        plan.market_existence_at_target for plan in plans.values()
    )
    rule_counts = Counter(plan.rule for plan in plans.values())
    category_counts = Counter(plan.category for plan in plans.values())
    month_counts = Counter(plan.verified_anchor_time[:7] for plan in plans.values())
    timing_counts = Counter(plan.timing_structure for plan in plans.values())
    category_existence_counts: dict[str, Counter] = defaultdict(Counter)
    rule_existence_counts: dict[str, Counter] = defaultdict(Counter)
    for plan in plans.values():
        category_existence_counts[plan.category][plan.market_existence_at_target] += 1
        rule_existence_counts[plan.rule][plan.market_existence_at_target] += 1
    smoke_cases, realized_quotas = select_smoke_cases(plans.values(), SMOKE_QUOTAS)
    smoke_eligible_by_target = Counter()
    for plan in smoke_cases:
        smoke_eligible_by_target[plan.target_time] += plan.eligible_market_count
    smoke_requests = projected_batched_requests(smoke_eligible_by_target, batch_size)
    smoke_eligible_markets = sum(smoke_eligible_by_target.values())
    smoke_conservative_bytes = (
        smoke_eligible_markets * (2048 + 256) + smoke_requests * 2048 + 1024**2
    )
    smoke_plan = {
        "schema_version": PLANNER_SCHEMA_VERSION,
        "network_run_authorized": False,
        "family_count": len(smoke_cases),
        "selection_seed": "phase-10f-a-bounded-smoke-v1",
        "stratum_quotas": SMOKE_QUOTAS,
        "realized_stratum_counts": realized_quotas,
        "projected_eligible_market_tickers": smoke_eligible_markets,
        "projected_minimum_requests": smoke_requests,
        "conservative_additional_bytes": smoke_conservative_bytes,
        "fits_current_namespace_headroom": smoke_conservative_bytes
        <= storage_before["remaining_budget_bytes"],
        "fits_current_free_space_floor": storage_before["free_bytes"]
        - smoke_conservative_bytes
        >= min_free_bytes,
        "required_measurements": [
            "requests_per_family",
            "raw_compressed_bytes",
            "normalized_bytes",
            "market_existed_at_target_rate",
            "any_pre_target_price_rate",
            "within_15_minute_rate",
            "within_60_minute_rate",
            "price_source_field_availability",
            "retries_and_rate_limits",
            "deterministic_no_network_resume",
        ],
        "cases": [
            {
                "family_id": plan.family_id,
                "family_id_source": plan.family_id_source,
                "rule": plan.rule,
                "category": plan.category,
                "timing_structure": plan.timing_structure,
                "target_time": plan.target_time,
                "market_existence_at_target": plan.market_existence_at_target,
                "market_count": len(plan.market_tickers),
                "eligible_market_retrieval_count": plan.eligible_market_count,
            }
            for plan in smoke_cases
        ],
    }
    report = {
        "schema_version": PLANNER_SCHEMA_VERSION,
        "input_hashes": input_hashes,
        "study_rules_fingerprint": rules.fingerprint,
        "analysis_window": {
            "start_utc": format_iso_utc(start),
            "end_utc_exclusive": format_iso_utc(end),
        },
        "horizon_hours": 1,
        "primary_staleness_minutes": 15,
        "robustness_staleness_minutes": 60,
        "in_window_verified_family_count": len(plans),
        "rule_counts": dict(sorted(rule_counts.items())),
        "market_existence_counts": dict(sorted(existence_counts.items())),
        "category_counts": dict(sorted(category_counts.items())),
        "category_existence_counts": {
            key: dict(sorted(value.items()))
            for key, value in sorted(category_existence_counts.items())
        },
        "month_counts": dict(sorted(month_counts.items())),
        "timing_structure_counts": dict(sorted(timing_counts.items())),
        "rule_existence_counts": {
            key: dict(sorted(value.items()))
            for key, value in sorted(rule_existence_counts.items())
        },
        "associated_market_count": market_scan["matched_market_rows"],
        "eligible_market_retrieval_count": sum(eligible_by_target.values()),
        "opened_after_target_market_count": sum(
            plan.opened_after_target_market_count for plan in plans.values()
        ),
        "unknown_open_time_market_count": sum(
            plan.unknown_open_time_market_count for plan in plans.values()
        ),
        "distinct_target_timestamp_count": sum(
            count > 0 for count in eligible_by_target.values()
        ),
        "request_batch_size": batch_size,
        "projected_price_history_requests": request_count,
        "request_projection_assumptions": [
            "multi-market candlestick requests",
            "at most 100 tickers per request",
            "tickers share one exact target timestamp and bounded time window",
            "no retries, recursive splits, or endpoint-specific failures",
        ],
        "outcome_fields_accessed": 0,
        "network_requests": 0,
        "price_history_acquired": False,
        "anchors_changed": 0,
    }
    validate_research_feature_columns(PLANNER_FIELDS)
    if preflight_only:
        summary = {**report, "storage_model": storage_model, "smoke_plan": smoke_plan}
        print(json.dumps(summary, sort_keys=True))
        return summary

    output_root.parent.mkdir(parents=True, exist_ok=True)
    temp_parent = Path(tempfile.mkdtemp(prefix="phase10f-a-", dir="/tmp"))
    temp_root = temp_parent / output_root.name
    temp_root.mkdir()
    try:
        planner_path = temp_root / OUTPUT_FILES[0]
        with planner_path.open("w", encoding="utf-8", newline="") as handle:
            writer = csv.DictWriter(
                handle, fieldnames=PLANNER_FIELDS, lineterminator="\n"
            )
            writer.writeheader()
            for identity in sorted(plans):
                writer.writerow(plan_to_row(plans[identity]))
        (temp_root / OUTPUT_FILES[2]).write_text(
            _source_design(report), encoding="utf-8"
        )
        (temp_root / OUTPUT_FILES[4]).write_text(
            json.dumps(smoke_plan, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        planner_outputs_bytes = sum(
            (temp_root / name).stat().st_size
            for name in (OUTPUT_FILES[0], OUTPUT_FILES[2], OUTPUT_FILES[4])
        )
        storage_preflight = {
            "schema_version": PLANNER_SCHEMA_VERSION,
            "storage_before": storage_before,
            "planner_artifacts_bytes_before_reports": planner_outputs_bytes,
            "planner_artifacts_fit_namespace": storage_before["used_bytes"]
            + planner_outputs_bytes
            + 2 * 1024**2
            <= max_generated_bytes,
            "planner_artifacts_fit_free_space": storage_before["free_bytes"]
            - planner_outputs_bytes
            >= min_free_bytes,
            "price_acquisition_model": storage_model,
            "archive_or_delete_performed": False,
        }
        if output_root.exists():
            existing = json.loads(
                (output_root / OUTPUT_FILES[3]).read_text(encoding="utf-8")
            )
            if existing.get("schema_version") != PLANNER_SCHEMA_VERSION:
                raise ValueError(
                    "existing Phase 10F-A storage report has a different schema"
                )
            storage_preflight["storage_before"] = existing["storage_before"]
            storage_preflight["planner_artifacts_fit_namespace"] = existing[
                "planner_artifacts_fit_namespace"
            ]
            storage_preflight["planner_artifacts_fit_free_space"] = existing[
                "planner_artifacts_fit_free_space"
            ]
            storage_preflight["price_acquisition_model"] = existing[
                "price_acquisition_model"
            ]
        (temp_root / OUTPUT_FILES[3]).write_text(
            json.dumps(storage_preflight, indent=2, sort_keys=True) + "\n",
            encoding="utf-8",
        )
        report["output_bytes"] = {
            name: (temp_root / name).stat().st_size
            for name in OUTPUT_FILES
            if name != OUTPUT_FILES[1]
        }
        report["output_hashes"] = {
            name: _sha256(temp_root / name)
            for name in OUTPUT_FILES
            if name != OUTPUT_FILES[1]
        }
        (temp_root / OUTPUT_FILES[1]).write_text(
            json.dumps(report, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        additional = _logical_bytes(temp_root)
        # A deterministic comparison rerun publishes no second copy inside the
        # guarded namespace. Its temporary derivation is outside guard_root and
        # is already reflected in the live free-space snapshot.
        budget.check_additional(0 if output_root.exists() else additional)
        if not storage_preflight["planner_artifacts_fit_namespace"]:
            raise ValueError("Phase 10F-A planner artifacts exceed namespace headroom")
        if not storage_preflight["planner_artifacts_fit_free_space"]:
            raise ValueError("Phase 10F-A planner artifacts cross the free-space floor")
        publication_hashes = {name: _sha256(temp_root / name) for name in OUTPUT_FILES}
        if output_root.exists():
            if not _compare_existing(output_root, temp_root):
                raise ValueError(
                    "existing Phase 10F-A output conflicts with deterministic rerun"
                )
        else:
            os.replace(temp_root, output_root)
        if any(
            _sha256(output_root / name) != publication_hashes[name]
            for name in OUTPUT_FILES
        ):
            raise ValueError("Phase 10F-A post-publication hash validation failed")
        print(json.dumps(report, sort_keys=True))
        return report
    finally:
        if temp_parent.exists():
            shutil.rmtree(temp_parent)


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--verified-anchors", required=True, type=Path)
    parser.add_argument("--market-metadata", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--guard-root", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--empirical-cache-dir", type=Path)
    parser.add_argument("--expected-anchors-sha256")
    parser.add_argument("--expected-market-metadata-sha256")
    parser.add_argument("--max-generated-bytes", type=int, default=5 * 1024**3)
    parser.add_argument("--min-free-bytes", type=int, default=80 * 1024**3)
    parser.add_argument("--preflight-only", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    try:
        run(
            args.verified_anchors,
            args.market_metadata,
            args.output_root,
            config_path=args.config,
            guard_root=args.guard_root,
            empirical_cache_dir=args.empirical_cache_dir,
            expected_anchors_sha256=args.expected_anchors_sha256,
            expected_market_metadata_sha256=args.expected_market_metadata_sha256,
            max_generated_bytes=args.max_generated_bytes,
            min_free_bytes=args.min_free_bytes,
            preflight_only=args.preflight_only,
        )
    except (OSError, ValueError, json.JSONDecodeError) as exc:
        print(f"Phase 10F-A planner failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
