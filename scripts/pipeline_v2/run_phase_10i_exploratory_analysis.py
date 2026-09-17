"""Run the approved post-confirmatory Phase 10I analysis offline."""

from __future__ import annotations

import argparse
from collections import Counter
import csv
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import subprocess
import sys
from typing import Any, Mapping, Sequence

from scripts.pipeline_v2.kalshi_metadata_cache import (
    StorageBudget,
    canonical_json,
)
from scripts.pipeline_v2.phase_10f_e import kish_ess
from scripts.pipeline_v2.phase_10i_analysis import (
    BOOTSTRAP_REPLICATES,
    TAIL_GROUPS,
    WEIGHT_SYSTEMS,
    PhaseIAnalysisError,
    bootstrap_metric_intervals,
    execution_values,
    quantile_cutpoints,
    quantile_label,
    weighted_execution_summary,
)
from scripts.pipeline_v2.run_phase_10f_e import (
    EXPECTED_CONTRACTS,
    NORMALIZED_FIELDS,
    _read_gzip_csv,
)


SCHEMA_VERSION = "phase-10i-exploratory-execution-v1"
EXPECTED_FAMILIES = 5_000
ANALYSIS_IDENTITY = "931a1d35de134e91eee3ed71041a712414c1435fbcd37f1ffc28b263e746252e"
INPUT_HASHES = {
    "phase_10i_plan": "5c9c7021bc24ebafbe49bae2de37780c609bccc7055cdf2dc66d692243165972",
    "phase_10g_manifest": "db298df905de11e145638d1f633f6829b5a9006f8012f0c02755e5f38443ccc8",
    "phase_10g_commit": "3c8236bbb3731d0c43679a16efa22c1087744a755df929dbadcdd3cacf609f40",
    "minimal_outcomes": "0c4dc35fcaa59205c379c6de8db227973ed279ef1425d4d43c6520be8195a4b0",
    "normalized_prices": "11f9ce8d3ed32ad9c3974a7f162c08b414e3aa5b87af80974283fd09175ef0d8",
    "raw_request_manifest": "3775c9acc93d87c83b7e2203b69af96da6d44a7ba3738f39df15df0660ed75b4",
    "phase_10f_c_frame_report": "d4208332f76732b83a1858e083124828efc40b764249427e91987d23d934e697",
    "phase_10f_b2_acceptance": "c7821ee78ea3f9b3e150b9c51e439fdc658927da7413ccc022ddb2bd6e5814b0",
}
MAX_REPORT_BYTES = 8 * 1024**2
TAKER_FIELDS = (
    "taker_gross_profit",
    "taker_net_profit_c1",
    "taker_net_profit_c100",
)
MAKER_FIELDS = (
    "maker_gross_profit",
    "maker_net_profit_c1",
    "maker_net_profit_c100",
)
SUPPORT_FAMILIES = 200
SUPPORT_FAMILY_ESS = 150


class PhaseIError(RuntimeError):
    """Raised when the Phase 10I workflow fails closed."""


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _verify(path: Path, expected: str, label: str) -> str:
    actual = _sha256(path)
    if actual != expected:
        raise PhaseIError(
            f"{label} SHA-256 mismatch: expected={expected} actual={actual}"
        )
    return actual


def _json_bytes(value: Any) -> bytes:
    return canonical_json(value) + b"\n"


def _csv_bytes(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in fields})
    return buffer.getvalue().encode()


def _nested_number(value: Any, *keys: str) -> float | None:
    if not isinstance(value, Mapping):
        return None
    for key in keys:
        candidate = value.get(key)
        if candidate not in {None, ""}:
            return float(candidate)
    return None


def _ticker_from_request(request: Mapping[str, Any]) -> str:
    endpoint = str(request["endpoint"])
    if endpoint == "/markets/candlesticks":
        ticker = str(request.get("params", {}).get("market_tickers", ""))
        if not ticker or "," in ticker:
            raise PhaseIError("live sample request is not single-ticker")
        return ticker
    parts = endpoint.strip("/").split("/")
    if len(parts) < 3 or parts[-1] != "candlesticks":
        raise PhaseIError(f"unexpected price endpoint: {endpoint}")
    return parts[-2]


def _raw_activity(
    manifest_path: Path, phase_e_root: Path
) -> tuple[dict[str, dict[str, Any]], dict[str, Any]]:
    activities: dict[str, dict[str, Any]] = {}
    raw_bytes = 0
    post_target_candles = 0
    schema_variants: Counter[str] = Counter()
    with manifest_path.open(encoding="utf-8") as handle:
        records = [json.loads(line) for line in handle if line.strip()]
    sample = [
        record
        for record in records
        if record.get("request", {}).get("purpose") == "sample_price_window"
    ]
    if len(sample) != EXPECTED_CONTRACTS:
        raise PhaseIError("raw sample request count changed")
    for record in sample:
        request_id = str(record["request_id"])
        partition = int(record["partition_index"])
        path = (
            phase_e_root
            / "partitions"
            / f"partition_{partition:04d}"
            / str(record["raw_path"])
        )
        if _sha256(path) != record["raw_sha256"]:
            raise PhaseIError(f"raw request hash changed: {request_id}")
        raw_bytes += path.stat().st_size
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            document = json.load(handle)
        if document.get("request") != record.get("request"):
            raise PhaseIError(f"raw request identity changed: {request_id}")
        response = document.get("response")
        if hashlib.sha256(canonical_json(response)).hexdigest() != document.get(
            "response_sha256"
        ):
            raise PhaseIError(f"raw response digest changed: {request_id}")
        ticker = _ticker_from_request(record["request"])
        if ticker in activities:
            raise PhaseIError(f"duplicate raw ticker: {ticker}")
        target = int(record["request"]["params"]["end_ts"])
        candles = []
        response_ticker = None
        if isinstance(response, Mapping) and "markets" in response:
            markets = response.get("markets", [])
            if len(markets) != 1:
                raise PhaseIError(f"live raw response is not single-market: {ticker}")
            candles = markets[0].get("candlesticks", [])
            response_ticker = markets[0].get("market_ticker")
        elif isinstance(response, Mapping):
            candles = response.get("candlesticks", [])
            response_ticker = response.get("ticker")
        if response_ticker not in {None, ticker}:
            raise PhaseIError(f"raw response ticker mismatch: {ticker}")
        seen_timestamps: set[int] = set()
        total_volume = 0.0
        trade_candles = 0
        latest_open_interest: float | None = None
        latest_open_interest_ts = -1
        for candle in candles:
            timestamp = int(candle["end_period_ts"])
            if timestamp in seen_timestamps:
                raise PhaseIError(f"duplicate raw candle timestamp: {ticker}")
            seen_timestamps.add(timestamp)
            if timestamp > target:
                post_target_candles += 1
                raise PhaseIError(f"post-target raw candle: {ticker}")
            volume = _nested_number(candle, "volume_fp", "volume")
            if volume is not None:
                if volume < 0:
                    raise PhaseIError(f"negative volume: {ticker}")
                total_volume += volume
            trade_close = _nested_number(candle.get("price"), "close_dollars", "close")
            if trade_close is not None:
                trade_candles += 1
            open_interest = _nested_number(candle, "open_interest_fp", "open_interest")
            if open_interest is not None and timestamp >= latest_open_interest_ts:
                latest_open_interest = open_interest
                latest_open_interest_ts = timestamp
        for variant in record.get("schema_variants", []):
            schema_variants[str(variant)] += 1
        activities[ticker] = {
            "pre_target_volume_60m": total_volume if candles else None,
            "pre_target_trade_candle_count": trade_candles if candles else None,
            "latest_pre_target_open_interest": latest_open_interest,
            "raw_pre_target_candle_count": len(candles),
            "raw_request_success": bool(record.get("success")),
        }
    if len(activities) != EXPECTED_CONTRACTS:
        raise PhaseIError("raw activity ticker count changed")
    return activities, {
        "manifest_records": len(records),
        "sample_request_records": len(sample),
        "unique_sample_tickers": len(activities),
        "compressed_raw_bytes_rehashed": raw_bytes,
        "post_target_candles": post_target_candles,
        "schema_variants": dict(sorted(schema_variants.items())),
    }


def _minimal_outcomes(path: Path) -> dict[str, int | None]:
    outcomes: dict[str, int | None] = {}
    with gzip.open(path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        expected = {
            "contract_identifier",
            "frozen_sample_identifier",
            "binary_resolution_outcome",
        }
        if set(reader.fieldnames or ()) != expected:
            raise PhaseIError("minimal outcome schema changed")
        for row in reader:
            ticker = str(row["contract_identifier"])
            if ticker in outcomes:
                raise PhaseIError("duplicate minimal outcome ticker")
            value = row["binary_resolution_outcome"]
            outcomes[ticker] = int(value) if value in {"0", "1"} else None
    if len(outcomes) != EXPECTED_CONTRACTS:
        raise PhaseIError("minimal outcome row count changed")
    return outcomes


def _prepare_rows(
    args: argparse.Namespace,
) -> tuple[list[dict[str, Any]], dict[str, Any]]:
    normalized = _read_gzip_csv(args.normalized_prices, typed=True)
    if (
        len(normalized) != EXPECTED_CONTRACTS
        or tuple(normalized[0]) != NORMALIZED_FIELDS
    ):
        raise PhaseIError("normalized price schema/count changed")
    outcomes = _minimal_outcomes(args.minimal_outcomes)
    activity, validation = _raw_activity(args.raw_manifest, args.phase_e_root)
    tickers = [str(row["ticker"]) for row in normalized]
    if len(set(tickers)) != EXPECTED_CONTRACTS or set(tickers) != set(outcomes):
        raise PhaseIError("price/outcome ticker identities changed")
    if set(tickers) != set(activity):
        raise PhaseIError("price/raw ticker identities changed")
    rows = []
    for source in normalized:
        ticker = str(source["ticker"])
        row = {
            **source,
            **activity[ticker],
            "binary_resolution_outcome": outcomes[ticker],
        }
        if bool(row.get("midpoint_within_15m")) and row.get(
            "binary_resolution_outcome"
        ) in {0, 1}:
            midpoint = float(row["midpoint"])
            if midpoint < 0.20 or midpoint >= 0.80:
                row.update(execution_values(row))
        rows.append(row)
    families = {(str(row["family_id"]), str(row["family_id_source"])) for row in rows}
    if len(families) != EXPECTED_FAMILIES:
        raise PhaseIError("frozen family frame changed")
    return rows, validation


def _group_rows(
    rows: Sequence[Mapping[str, Any]], group: str
) -> list[Mapping[str, Any]]:
    if group == "combined":
        return [
            row for row in rows if row.get("strategy_group") in {"longshot", "favorite"}
        ]
    return [row for row in rows if row.get("strategy_group") == group]


def _family_ess(rows: Sequence[Mapping[str, Any]], weight_field: str) -> float:
    family_weights: dict[tuple[str, str], float] = {}
    for row in rows:
        key = (str(row["family_id"]), str(row["family_id_source"]))
        family_weights[key] = family_weights.get(key, 0.0) + float(row[weight_field])
    return kish_ess(list(family_weights.values()))


def _assign_attention(rows: list[dict[str, Any]]) -> dict[str, Any]:
    price_observable = [row for row in rows if bool(row.get("midpoint_within_15m"))]
    positive_volume = [
        float(row["pre_target_volume_60m"])
        for row in price_observable
        if row.get("pre_target_volume_60m") is not None
        and float(row["pre_target_volume_60m"]) > 0
    ]
    spreads = [float(row["spread"]) for row in price_observable]
    open_interest = [
        float(row["latest_pre_target_open_interest"])
        for row in price_observable
        if row.get("latest_pre_target_open_interest") is not None
    ]
    cuts = {
        "positive_volume": quantile_cutpoints(positive_volume),
        "spread": quantile_cutpoints(spreads),
        "open_interest": quantile_cutpoints(open_interest),
    }
    for row in price_observable:
        volume = row.get("pre_target_volume_60m")
        row["volume_status"] = (
            "unknown"
            if volume is None
            else "zero_volume" if float(volume) == 0 else "positive_volume"
        )
        row["positive_volume_quartile"] = (
            quantile_label(float(volume), cuts["positive_volume"])
            if volume is not None and float(volume) > 0
            else None
        )
        row["spread_quartile"] = quantile_label(float(row["spread"]), cuts["spread"])
        interest = row.get("latest_pre_target_open_interest")
        row["open_interest_quartile"] = (
            quantile_label(float(interest), cuts["open_interest"])
            if interest is not None
            else None
        )
        trades = row.get("pre_target_trade_candle_count")
        row["trade_activity"] = (
            "unknown"
            if trades is None
            else "actual_trade_present" if int(trades) > 0 else "no_actual_trade"
        )
    return {
        "cutpoints": {key: list(value) for key, value in cuts.items()},
        "cutpoints_use_outcomes": False,
        "price_observable_rows_used": len(price_observable),
    }


def _predicate(group: str, field: str | None = None, label: str | None = None):
    def selected(row: Mapping[str, Any]) -> bool:
        in_group = (
            row.get("strategy_group") in {"longshot", "favorite"}
            if group == "combined"
            else row.get("strategy_group") == group
        )
        return bool(in_group and (field is None or row.get(field) == label))

    return selected


def _execution_results(
    rows: list[dict[str, Any]], replicates: int
) -> tuple[dict[str, Any], list[dict[str, Any]], dict[str, Any]]:
    summaries: dict[str, Any] = {}
    metrics = {}
    for weight_name, weight_field in WEIGHT_SYSTEMS.items():
        summaries[weight_name] = {}
        for group in TAIL_GROUPS:
            group_rows = _group_rows(rows, group)
            result = weighted_execution_summary(group_rows, weight_field=weight_field)
            summaries[weight_name][group] = result
            for value_field in (*TAKER_FIELDS, *MAKER_FIELDS):
                metrics[f"execution|{weight_name}|{group}|{value_field}"] = (
                    _predicate(group),
                    value_field,
                    weight_field,
                )

    domains = {
        "volume_status": ("zero_volume", "positive_volume"),
        "positive_volume_quartile": ("Q1", "Q2", "Q3", "Q4"),
        "spread_quartile": ("Q1", "Q2", "Q3", "Q4"),
        "open_interest_quartile": ("Q1", "Q2", "Q3", "Q4"),
        "trade_activity": ("no_actual_trade", "actual_trade_present"),
    }
    attention_rows = []
    supported_attention_metrics = {}
    for domain, labels in domains.items():
        for label in labels:
            group = [
                row
                for row in rows
                if row.get("strategy_group") in {"longshot", "favorite"}
                and row.get(domain) == label
            ]
            if not group:
                continue
            summary = weighted_execution_summary(
                group, weight_field=WEIGHT_SYSTEMS["family_target"]
            )
            supported = bool(
                summary["families"] >= SUPPORT_FAMILIES
                and summary["family_aggregated_weight_ess"] >= SUPPORT_FAMILY_ESS
            )
            taker_metric_name = f"attention|{domain}|{label}|taker_gross_profit"
            maker_metric_name = f"attention|{domain}|{label}|maker_gross_profit"
            if supported:
                supported_attention_metrics[taker_metric_name] = (
                    _predicate("combined", domain, label),
                    "taker_gross_profit",
                    WEIGHT_SYSTEMS["family_target"],
                )
                supported_attention_metrics[maker_metric_name] = (
                    _predicate("combined", domain, label),
                    "maker_gross_profit",
                    WEIGHT_SYSTEMS["family_target"],
                )
            attention_rows.append(
                {
                    "domain": domain,
                    "group": label,
                    **summary,
                    "support_gate_passed": supported,
                    "taker_metric_name": taker_metric_name,
                    "maker_metric_name": maker_metric_name,
                }
            )
    metrics.update(supported_attention_metrics)
    contrasts = {}
    zero_name = "attention|volume_status|zero_volume|taker_gross_profit"
    positive_name = "attention|volume_status|positive_volume|taker_gross_profit"
    if zero_name in metrics and positive_name in metrics:
        contrasts["zero_minus_positive_volume_taker_gross_profit"] = (
            zero_name,
            positive_name,
        )
    zero_maker_name = "attention|volume_status|zero_volume|maker_gross_profit"
    positive_maker_name = "attention|volume_status|positive_volume|maker_gross_profit"
    if zero_maker_name in metrics and positive_maker_name in metrics:
        contrasts["zero_minus_positive_volume_maker_gross_profit"] = (
            zero_maker_name,
            positive_maker_name,
        )
    bootstrap = bootstrap_metric_intervals(
        rows,
        metrics,
        contrasts=contrasts,
        replicates=replicates,
    )
    for weight_name in WEIGHT_SYSTEMS:
        for group in TAIL_GROUPS:
            result = summaries[weight_name][group]
            result["inference"] = {
                field: bootstrap["intervals"][
                    f"execution|{weight_name}|{group}|{field}"
                ]
                for field in (*TAKER_FIELDS, *MAKER_FIELDS)
            }
    for row in attention_rows:
        row["inference"] = (
            {
                "taker_gross_profit": bootstrap["intervals"][row["taker_metric_name"]],
                "maker_gross_profit": bootstrap["intervals"][row["maker_metric_name"]],
            }
            if row["support_gate_passed"]
            else None
        )
    return summaries, attention_rows, bootstrap


def _cross_category_preflight(
    frame_report_path: Path,
    acceptance_path: Path,
    storage: Mapping[str, int],
) -> dict[str, Any]:
    frame = json.loads(frame_report_path.read_text())
    acceptance = json.loads(acceptance_path.read_text())
    families = frame["eligible_family_counts"]["by_category"]
    contracts = frame["eligible_contract_counts"]["by_category"]
    proposed = {
        "Crypto": min(500, int(families.get("Crypto", 0))),
        "Financials": min(500, int(families.get("Financials", 0))),
        "Climate and Weather": min(500, int(families.get("Climate and Weather", 0))),
        "Commodities": int(families.get("Commodities", 0)),
    }
    maximum_tickers = sum(value * 3 for value in proposed.values())
    bytes_per_ticker = sum(
        (
            float(acceptance["measured_compressed_raw_bytes_per_ticker_request"]),
            float(acceptance["measured_normalized_bytes_per_ticker"]),
            float(acceptance["measured_request_commit_and_manifest_bytes_per_ticker"]),
        )
    )
    projected = int(maximum_tickers * bytes_per_ticker)
    pr1 = frame["b2_planning_rates"]["PR1_M_FIXED_CLOCK_SINGLE_EXACT"]
    categories = []
    for category in (
        "Crypto",
        "Financials",
        "Climate and Weather",
        "Commodities",
        "Sports",
        "Politics",
        "Entertainment",
    ):
        categories.append(
            {
                "category": category,
                "eligible_families": int(families.get(category, 0)),
                "eligible_contracts": int(contracts.get(category, 0)),
                "proposed_families_if_source_validated": int(proposed.get(category, 0)),
                "current_rule": (
                    "PR2_scheduled_start"
                    if category == "Sports"
                    else (
                        "PR1_fixed_clock"
                        if category in families
                        else "no_approved_rule_coverage"
                    )
                ),
            }
        )
    return {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "network_requests_made": 0,
        "sample_drawn": False,
        "categories": categories,
        "current_historical_source": {
            "midpoint_15m_successes": int(pr1["usable_midpoint_15m"]["successes"]),
            "midpoint_15m_trials": int(pr1["usable_midpoint_15m"]["trials"]),
            "trade_15m_successes": int(pr1["usable_trade_close_15m"]["successes"]),
            "trade_15m_trials": int(pr1["usable_trade_close_15m"]["trials"]),
            "scientifically_viable": False,
        },
        "illustrative_design_if_source_validated": {
            "families_by_category": proposed,
            "within_family_contract_cap": 3,
            "maximum_tickers": maximum_tickers,
            "projected_auditable_bytes_using_b2_rates": projected,
            "fits_current_namespace_headroom": projected
            <= int(storage["remaining_budget_bytes"]),
            "draw_authorized": False,
        },
        "hard_stop": {
            "triggered": True,
            "reason": (
                "The validated historical source produced 0/135 usable PR1 "
                "15-minute midpoints and 0/135 usable PR1 15-minute trades."
            ),
            "next_decision": (
                "validate an alternative historical source or approve a new "
                "prospective collection window"
            ),
        },
    }


def _table_rows(summaries: Mapping[str, Any], *, side: str) -> list[dict[str, Any]]:
    rows = []
    for weight_name in WEIGHT_SYSTEMS:
        for group in TAIL_GROUPS:
            result = summaries[weight_name][group]
            gross_field = f"{side}_gross_profit"
            gross_interval = result["inference"][gross_field]
            rows.append(
                {
                    "target": weight_name,
                    "strategy_group": group,
                    "contracts": result["contracts"],
                    "families": result["families"],
                    "family_ess": result["family_aggregated_weight_ess"],
                    "weighted_entry": result[f"weighted_{side}_entry"],
                    "weighted_payout_rate": result["weighted_payout_rate"],
                    "gross_profit_per_contract": result[f"weighted_{gross_field}"],
                    "gross_ci_lower": gross_interval["ci_lower"],
                    "gross_ci_upper": gross_interval["ci_upper"],
                    "gross_capital_return": result[
                        f"weighted_{side}_gross_capital_return"
                    ],
                    "net_profit_c1": result[f"weighted_{side}_net_profit_c1"],
                    "net_profit_c1_ci_lower": result["inference"][
                        f"{side}_net_profit_c1"
                    ]["ci_lower"],
                    "net_profit_c1_ci_upper": result["inference"][
                        f"{side}_net_profit_c1"
                    ]["ci_upper"],
                    "capital_return_c1": result[f"weighted_{side}_capital_return_c1"],
                    "net_profit_c100": result[f"weighted_{side}_net_profit_c100"],
                    "net_profit_c100_ci_lower": result["inference"][
                        f"{side}_net_profit_c100"
                    ]["ci_lower"],
                    "net_profit_c100_ci_upper": result["inference"][
                        f"{side}_net_profit_c100"
                    ]["ci_upper"],
                    "capital_return_c100": result[
                        f"weighted_{side}_capital_return_c100"
                    ],
                }
            )
    return rows


RESULT_FIELDS = (
    "target",
    "strategy_group",
    "contracts",
    "families",
    "family_ess",
    "weighted_entry",
    "weighted_payout_rate",
    "gross_profit_per_contract",
    "gross_ci_lower",
    "gross_ci_upper",
    "gross_capital_return",
    "net_profit_c1",
    "net_profit_c1_ci_lower",
    "net_profit_c1_ci_upper",
    "capital_return_c1",
    "net_profit_c100",
    "net_profit_c100_ci_lower",
    "net_profit_c100_ci_upper",
    "capital_return_c100",
)


def _attention_table(rows: Sequence[Mapping[str, Any]]) -> list[dict[str, Any]]:
    output = []
    for row in rows:
        inference = row["inference"] or {}
        taker_interval = inference.get("taker_gross_profit", {})
        maker_interval = inference.get("maker_gross_profit", {})
        output.append(
            {
                "domain": row["domain"],
                "group": row["group"],
                "contracts": row["contracts"],
                "families": row["families"],
                "family_ess": row["family_aggregated_weight_ess"],
                "gross_taker_profit_per_contract": row["weighted_taker_gross_profit"],
                "taker_ci_lower": taker_interval.get("ci_lower", ""),
                "taker_ci_upper": taker_interval.get("ci_upper", ""),
                "conditional_maker_profit_per_contract": row[
                    "weighted_maker_gross_profit"
                ],
                "maker_ci_lower": maker_interval.get("ci_lower", ""),
                "maker_ci_upper": maker_interval.get("ci_upper", ""),
                "support_gate_passed": row["support_gate_passed"],
            }
        )
    return output


ATTENTION_FIELDS = (
    "domain",
    "group",
    "contracts",
    "families",
    "family_ess",
    "gross_taker_profit_per_contract",
    "taker_ci_lower",
    "taker_ci_upper",
    "conditional_maker_profit_per_contract",
    "maker_ci_lower",
    "maker_ci_upper",
    "support_gate_passed",
)


def _cross_category_table(report: Mapping[str, Any]) -> list[dict[str, Any]]:
    return [
        {
            **row,
            "historical_source_viable": report["current_historical_source"][
                "scientifically_viable"
            ],
        }
        for row in report["categories"]
    ]


CROSS_CATEGORY_FIELDS = (
    "category",
    "eligible_families",
    "eligible_contracts",
    "proposed_families_if_source_validated",
    "current_rule",
    "historical_source_viable",
)


def _fmt(value: float) -> str:
    return f"{100 * value:+.2f}¢"


def _report_markdown(
    summaries: Mapping[str, Any],
    attention: Sequence[Mapping[str, Any]],
    bootstrap: Mapping[str, Any],
    cross_category: Mapping[str, Any],
) -> str:
    primary = summaries["family_target"]["combined"]
    longshot = summaries["family_target"]["longshot"]
    favorite = summaries["family_target"]["favorite"]
    primary_ci = primary["inference"]["taker_gross_profit"]
    zero = next(
        row
        for row in attention
        if row["domain"] == "volume_status" and row["group"] == "zero_volume"
    )
    positive = next(
        row
        for row in attention
        if row["domain"] == "volume_status" and row["group"] == "positive_volume"
    )
    contrast = bootstrap["contrasts"].get(
        "zero_minus_positive_volume_taker_gross_profit"
    )
    contrast_text = (
        f" Its zero-minus-positive-volume difference has a 95% interval of "
        f"[{_fmt(contrast['ci_lower'])}, {_fmt(contrast['ci_upper'])}]."
        if contrast
        else " The pre-specified support gate did not permit an interaction interval."
    )
    maker_contrast = bootstrap["contrasts"].get(
        "zero_minus_positive_volume_maker_gross_profit"
    )
    maker_contrast_text = (
        f" The zero-minus-positive-volume conditional-maker difference has a "
        f"95% interval of [{_fmt(maker_contrast['ci_lower'])}, "
        f"{_fmt(maker_contrast['ci_upper'])}]."
        if maker_contrast
        else ""
    )
    return f"""# Can the apparent pricing result survive the spread?

## Exploratory status

This is a post-confirmatory Phase 10I analysis. It was designed after the Phase
10G outcomes were known. It does not replace or modify the frozen conclusion
that the confirmatory Sports analysis detected no statistically distinguishable
favorite–longshot bias.

## Executable-price proxy

The applied strategy buys NO against midpoint longshots below $0.20 at
`1 - YES bid`, and buys YES favorites at or above $0.80 at the YES ask. It holds
one contract to resolution. These candle-close top-of-book prices incorporate
the observed spread, but not depth, latency, or additional slippage.

Under the family target, the combined gross taker profit is
**{_fmt(primary['weighted_taker_gross_profit'])} per contract** (95% family-
cluster interval [{_fmt(primary_ci['ci_lower'])}, {_fmt(primary_ci['ci_upper'])}])
across {primary['contracts']:,} tail contracts and {primary['families']:,}
families. Longshots contribute {_fmt(longshot['weighted_taker_gross_profit'])}
per contract and favorites {_fmt(favorite['weighted_taker_gross_profit'])}.

![Observed-spread taker results](figure_1_taker_profit.png)

The published general-fee formula is shown as a scenario because exact
historical market-specific overrides and participant rebates are unavailable.
After the one-contract fee-rounding scenario, combined profit is
{_fmt(primary['weighted_taker_net_profit_c1'])}; under the 100-contract
fee-rounding scenario it is {_fmt(primary['weighted_taker_net_profit_c100'])}
per contract. The latter is not a claim that 100 contracts were displayed at
the best price.

## Was the result different in quiet markets?

Within the priced tail sample, zero reported volume in the pre-target hour has
gross taker profit of {_fmt(zero['weighted_taker_gross_profit'])}; positive-
volume markets have {_fmt(positive['weighted_taker_gross_profit'])}.{contrast_text}
Conditional passive-entry profit is
{_fmt(zero['weighted_maker_gross_profit'])} in zero-volume markets and
{_fmt(positive['weighted_maker_gross_profit'])} in positive-volume markets.
{maker_contrast_text}

![Profit by pre-target activity](figure_2_attention_profit.png)

This comparison cannot identify returns for the 928 contracts with no
pre-target candle because they have no executable target-time price. It tests
the low-attention hypothesis only within the price-observable sample. Volume,
trade presence, open interest, spread, staleness, and market age are measured
before the target.

## Market-making scenarios

Passive-entry calculations use the displayed bid side and are conditional on a
complete fill. They do not model queue priority, fill probability, inventory,
adverse selection, latency, or capacity. Their apparent improvement over taker
entries partly reflects mechanical spread capture and must not be called
realized alpha. This warning is especially important for the zero-volume group:
the conditional maker result assumes a fill precisely where the pre-target
window records no transaction.

## Cross-category status

The cross-category extension is stopped before sampling or acquisition. The
validated historical source previously produced 0/135 usable 15-minute PR1
midpoints and 0/135 usable PR1 trades. Politics and Entertainment also have no
eligible families under the frozen anchor rules. The next defensible choice is
between an independently validated historical source and a new prospective
collection window; either requires a new sample identity and outcome
quarantine.

## Bottom line

Phase 10I translates the existing calibration study into top-of-book trading
terms without pretending that candle data are fills. The results are
exploratory, conditional on price observability, and separate from Phase 10G.
"""


def _matplotlib():
    os.environ.setdefault("MPLCONFIGDIR", "/tmp/pmlb-matplotlib")
    import matplotlib

    matplotlib.use("Agg")
    matplotlib.rcParams.update(
        {
            "font.family": "DejaVu Sans",
            "font.size": 9,
            "axes.titlesize": 11,
            "axes.labelsize": 10,
            "figure.dpi": 120,
            "savefig.dpi": 300,
            "svg.hashsalt": SCHEMA_VERSION,
        }
    )
    import matplotlib.pyplot as plt

    return plt


def _figure_bytes(fig: Any, extension: str) -> bytes:
    buffer = io.BytesIO()
    metadata = {"Creator": "Phase 10I deterministic exploratory-report generator"}
    if extension == "svg":
        metadata["Date"] = "2026-09-16"
    fig.savefig(
        buffer,
        format=extension,
        bbox_inches="tight",
        facecolor="white",
        metadata=metadata,
    )
    content = buffer.getvalue()
    if extension == "svg":
        content = b"\n".join(line.rstrip() for line in content.splitlines()) + b"\n"
    return content


def _figures(
    summaries: Mapping[str, Any], attention: Sequence[Mapping[str, Any]]
) -> dict[str, bytes]:
    plt = _matplotlib()
    blue, orange, gray = "#0072B2", "#D55E00", "#666666"
    family = summaries["family_target"]
    labels = ["Longshots:\nbuy NO", "Favorites:\nbuy YES", "Combined"]
    groups = ("longshot", "favorite", "combined")
    points = [family[group]["weighted_taker_gross_profit"] for group in groups]
    intervals = [family[group]["inference"]["taker_gross_profit"] for group in groups]
    fig, ax = plt.subplots(figsize=(7.2, 4.5))
    positions = list(range(3))
    ax.errorbar(
        positions,
        [100 * point for point in points],
        yerr=[
            [
                100 * (point - interval["ci_lower"])
                for point, interval in zip(points, intervals)
            ],
            [
                100 * (interval["ci_upper"] - point)
                for point, interval in zip(points, intervals)
            ],
        ],
        fmt="o",
        color=blue,
        ecolor=blue,
        capsize=4,
        markersize=6,
    )
    ax.axhline(0, color=gray, linewidth=1)
    ax.set_xticks(positions, labels)
    ax.set_ylabel("Gross taker profit (cents per contract)")
    ax.set_title("Top-of-book strategy after crossing the spread")
    ax.grid(axis="y", alpha=0.2)
    artifacts = {}
    for extension in ("png", "svg"):
        artifacts[f"figure_1_taker_profit.{extension}"] = _figure_bytes(fig, extension)
    plt.close(fig)

    activity = [row for row in attention if row["domain"] == "volume_status"]
    fig, ax = plt.subplots(figsize=(7.2, 4.5))
    for index, row in enumerate(activity):
        for offset, side, marker, color in (
            (-0.08, "taker", "o", orange),
            (0.08, "maker", "s", blue),
        ):
            field = f"weighted_{side}_gross_profit"
            point = float(row[field])
            inference = row["inference"] or {}
            interval = inference.get(f"{side}_gross_profit")
            label = (
                "Taker entry"
                if side == "taker" and index == 0
                else (
                    "Conditional maker entry"
                    if side == "maker" and index == 0
                    else None
                )
            )
            if interval:
                ax.errorbar(
                    index + offset,
                    100 * point,
                    yerr=[
                        [100 * (point - interval["ci_lower"])],
                        [100 * (interval["ci_upper"] - point)],
                    ],
                    fmt=marker,
                    color=color,
                    ecolor=color,
                    capsize=4,
                    markersize=6,
                    label=label,
                )
            else:
                ax.plot(index + offset, 100 * point, marker, color=color, label=label)
    ax.axhline(0, color=gray, linewidth=1)
    ax.set_xticks(
        list(range(len(activity))),
        [row["group"].replace("_", " ").title() for row in activity],
    )
    ax.set_ylabel("Gross profit (cents per contract)")
    ax.set_title("Exploratory execution scenarios by pre-target activity")
    ax.legend(frameon=False)
    ax.grid(axis="y", alpha=0.2)
    for extension in ("png", "svg"):
        artifacts[f"figure_2_attention_profit.{extension}"] = _figure_bytes(
            fig, extension
        )
    plt.close(fig)
    return artifacts


def _publish(output_root: Path, artifacts: Mapping[str, bytes]) -> None:
    if sum(len(content) for content in artifacts.values()) > MAX_REPORT_BYTES:
        raise PhaseIError("compact report budget exceeded")
    output_root.mkdir(parents=True, exist_ok=True)
    expected = set(artifacts)
    existing = {path.name for path in output_root.iterdir() if path.is_file()}
    if existing - expected:
        raise PhaseIError(
            f"unexpected Phase 10I artifacts: {sorted(existing - expected)}"
        )
    for name, content in artifacts.items():
        path = output_root / name
        if path.exists() and path.read_bytes() != content:
            raise PhaseIError(f"immutable Phase 10I artifact differs: {name}")
    for name, content in artifacts.items():
        path = output_root / name
        if path.exists():
            continue
        temp = output_root / f".{name}.tmp"
        temp.write_bytes(content)
        os.replace(temp, path)


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
        raise PhaseIError("code commit must be a full lowercase Git SHA")
    return value


def _report_storage_snapshot(
    output_root: Path, current: Mapping[str, int]
) -> dict[str, int]:
    """Reuse the published preflight reading so an offline replay is byte-stable."""
    report_path = output_root / "phase_10i_analysis_report.json"
    if not report_path.exists():
        return dict(current)
    report = json.loads(report_path.read_text())
    recorded = report.get("storage_preflight")
    required = {
        "used_bytes",
        "max_bytes",
        "remaining_budget_bytes",
        "free_bytes",
        "min_free_bytes",
        "free_space_margin_bytes",
    }
    if not isinstance(recorded, Mapping) or set(recorded) != required:
        raise PhaseIError("published Phase 10I storage preflight is invalid")
    if int(recorded["max_bytes"]) != int(current["max_bytes"]) or int(
        recorded["min_free_bytes"]
    ) != int(current["min_free_bytes"]):
        raise PhaseIError("published Phase 10I storage guard changed")
    return {key: int(recorded[key]) for key in required}


def run(args: argparse.Namespace) -> dict[str, Any]:
    paths = {
        "phase_10i_plan": args.analysis_plan,
        "phase_10g_manifest": args.phase_g_manifest,
        "phase_10g_commit": args.phase_g_commit,
        "minimal_outcomes": args.minimal_outcomes,
        "normalized_prices": args.normalized_prices,
        "raw_request_manifest": args.raw_manifest,
        "phase_10f_c_frame_report": args.frame_report,
        "phase_10f_b2_acceptance": args.b2_acceptance,
    }
    hashes = {
        label: _verify(path, INPUT_HASHES[label], label)
        for label, path in paths.items()
    }
    phase_g_manifest = json.loads(args.phase_g_manifest.read_text())
    if (
        phase_g_manifest.get("analysis_identity") != ANALYSIS_IDENTITY
        or not phase_g_manifest.get("complete")
        or phase_g_manifest.get("frozen_methodology_changed")
    ):
        raise PhaseIError("Phase 10G frozen state changed")
    budget = StorageBudget(
        args.guard_root,
        max_bytes=args.max_generated_bytes,
        min_free_bytes=args.min_free_bytes,
    )
    current_storage = budget.snapshot()
    storage = _report_storage_snapshot(args.output_root, current_storage)
    rows, raw_validation = _prepare_rows(args)
    attention_design = _assign_attention(rows)
    summaries, attention_rows, bootstrap = _execution_results(
        rows, args.bootstrap_replicates
    )
    cross_category = _cross_category_preflight(
        args.frame_report, args.b2_acceptance, storage
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "exploratory_post_confirmatory": True,
        "phase_10g_changed": False,
        "phase_10g_analysis_identity": ANALYSIS_IDENTITY,
        "input_hashes": hashes,
        "sample_identity": "8a95158441c245988d2562b732762d9a6f3c5c9cd6d0bb33b9fcc6f3b8de2bc9",
        "raw_activity_validation": raw_validation,
        "attention_design": attention_design,
        "execution_results": summaries,
        "attention_results": attention_rows,
        "bootstrap": bootstrap,
        "cross_category_preflight": cross_category,
        "storage_preflight": storage,
        "controls": {
            "network_requests_made": 0,
            "sample_redrawn": False,
            "anchors_changed": False,
            "prices_changed": False,
            "weights_changed": False,
            "study_rules_changed": False,
            "post_target_candles_used": 0,
            "joined_contract_level_data_persisted": False,
            "market_maker_results_realized_pnl": False,
        },
    }
    taker_rows = _table_rows(summaries, side="taker")
    maker_rows = _table_rows(summaries, side="maker")
    attention_table = _attention_table(attention_rows)
    cross_table = _cross_category_table(cross_category)
    artifacts = {
        "PHASE_10I_RESULTS.md": _report_markdown(
            summaries, attention_rows, bootstrap, cross_category
        ).encode(),
        "table_1_taker_results.csv": _csv_bytes(taker_rows, RESULT_FIELDS),
        "table_2_maker_scenarios.csv": _csv_bytes(maker_rows, RESULT_FIELDS),
        "table_3_attention_results.csv": _csv_bytes(attention_table, ATTENTION_FIELDS),
        "table_4_cross_category_feasibility.csv": _csv_bytes(
            cross_table, CROSS_CATEGORY_FIELDS
        ),
        "phase_10i_analysis_report.json": _json_bytes(report),
        "phase_10i_cross_category_preflight.json": _json_bytes(cross_category),
    }
    artifacts.update(_figures(summaries, attention_rows))
    commit = _code_commit(args.repository_root.resolve(), args.code_commit)
    manifest = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "exploratory_post_confirmatory": True,
        "phase_10g_analysis_identity": ANALYSIS_IDENTITY,
        "code_commit": commit,
        "input_hashes": hashes,
        "generated_artifacts": [
            {
                "path": name,
                "bytes": len(content),
                "sha256": hashlib.sha256(content).hexdigest(),
            }
            for name, content in sorted(artifacts.items())
        ],
        "network_requests_made": 0,
        "contract_level_join_persisted": False,
    }
    artifacts["reproducibility_manifest.json"] = _json_bytes(manifest)
    _publish(args.output_root, artifacts)
    return {
        "complete": True,
        "exploratory_post_confirmatory": True,
        "phase_10g_changed": False,
        "artifact_count": len(artifacts),
        "output_bytes": sum(len(content) for content in artifacts.values()),
        "manifest_sha256": hashlib.sha256(
            artifacts["reproducibility_manifest.json"]
        ).hexdigest(),
        "code_commit": commit,
        "output_root": str(args.output_root),
        "network_requests_made": 0,
        "cross_category_hard_stop": cross_category["hard_stop"],
    }


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser()
    phase_e = Path("data/pipeline_v2/horizon_prices/phase_10f_e")
    phase_g = Path("data/pipeline_v2/horizon_prices/phase_10g_outcome_analysis_v3")
    parser.add_argument("--repository-root", type=Path, default=Path("."))
    parser.add_argument(
        "--analysis-plan",
        type=Path,
        default=Path("PHASE_10I_EXPLORATORY_ANALYSIS_PLAN.md"),
    )
    parser.add_argument(
        "--phase-g-manifest",
        type=Path,
        default=Path("reports/phase_10g/reproducibility_manifest.json"),
    )
    parser.add_argument(
        "--phase-g-commit", type=Path, default=phase_g / "phase_10g_commit.json"
    )
    parser.add_argument(
        "--minimal-outcomes",
        type=Path,
        default=phase_g / "phase_10g_minimal_binary_outcomes.csv.gz",
    )
    parser.add_argument(
        "--normalized-prices",
        type=Path,
        default=phase_e / "phase_10f_e_normalized_prices.csv.gz",
    )
    parser.add_argument(
        "--raw-manifest",
        type=Path,
        default=phase_e / "phase_10f_e_raw_request_manifest.jsonl",
    )
    parser.add_argument("--phase-e-root", type=Path, default=phase_e)
    parser.add_argument(
        "--frame-report",
        type=Path,
        default=Path(
            "data/pipeline_v2/horizon_prices/phase_10f_c/"
            "phase_10f_c_sampling_frame_report.json"
        ),
    )
    parser.add_argument(
        "--b2-acceptance",
        type=Path,
        default=Path(
            "data/pipeline_v2/horizon_prices/phase_10f_b2/"
            "phase_10f_b2_acceptance_report.json"
        ),
    )
    parser.add_argument("--output-root", type=Path, default=Path("reports/phase_10i"))
    parser.add_argument("--code-commit")
    parser.add_argument(
        "--bootstrap-replicates", type=int, default=BOOTSTRAP_REPLICATES
    )
    parser.add_argument("--guard-root", type=Path, default=Path("data/pipeline_v2"))
    parser.add_argument("--max-generated-bytes", type=int, default=5 * 1024**3)
    parser.add_argument("--min-free-bytes", type=int, default=80 * 1024**3)
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        print(json.dumps(run(parse_args(argv)), sort_keys=True))
        return 0
    except (PhaseIError, PhaseIAnalysisError, OSError, ValueError, KeyError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
