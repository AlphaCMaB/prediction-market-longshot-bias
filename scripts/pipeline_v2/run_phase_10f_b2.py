"""Run the strictly bounded Phase 10F-B2 historical-candlestick validation."""

from __future__ import annotations

import argparse
from collections import Counter, defaultdict
import csv
from datetime import datetime, timezone
import gzip
import hashlib
import io
import json
import math
from pathlib import Path
import sys
import time
from typing import Any, Mapping, Sequence
from urllib.parse import quote

import requests

from scripts.common.time_utils import format_iso_utc
from scripts.pipeline_v2.kalshi_metadata_cache import (
    CacheError,
    StorageBudget,
    canonical_json,
    publish_immutable_bytes,
    reject_sensitive_response,
)
from scripts.pipeline_v2.phase_10f_b2 import (
    B2ValidationError,
    HISTORICAL_ROUTE,
    LIVE_ROUTE,
    MAX_NETWORK_REQUESTS,
    SAMPLE_SEED,
    SAMPLE_SIZE,
    SCHEMA_VERSION,
    TickerCandidate,
    diagnostic_distribution,
    extract_observation,
    family_size_stratum,
    normalize_response,
    route_for_settlement,
    sample_identity,
    select_ticker_sample,
)
from scripts.pipeline_v2.phase_10f_planner import (
    EXISTED,
    classify_market_open,
    decode_market_tickers,
)
from scripts.pipeline_v2.run_phase_10f_b_smoke import _load_selected_planner_rows
from scripts.pipeline_v2.study_rules import load_study_rules


DEFAULT_BASE_URL = "https://external-api.kalshi.com/trade-api/v2"
DEFAULT_MAX_BYTES = 5 * 1024**3
DEFAULT_MIN_FREE_BYTES = 80 * 1024**3
CONSERVATIVE_ADDITIONAL_BYTES = 32 * 1024**2
CUTOFF_PATH = "/historical/cutoff"
HISTORICAL_DOC_SHA256 = "ad0fa333f30f9bc3762dc6052d7cc9429db8eaf0bbcfdc33eddc89a01892bf0f"
CUTOFF_DOC_SHA256 = "a44c4a8def2fa1368ef1bdebe50968a596dac126bcba71034a221956862a5162"
SAMPLE_FIELDS = (
    "sample_index",
    "family_id",
    "family_id_source",
    "event_ticker",
    "rule",
    "category",
    "timing_structure",
    "target_time",
    "market_ticker",
    "family_market_count",
    "family_size_stratum",
    "target_month",
    "routing_tier",
)
NORMALIZED_FIELDS = (
    *SAMPLE_FIELDS,
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
)
PROHIBITED_KEYS = frozenset(
    {"result", "outcome", "settlement_value", "settlement_value_dollars"}
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _verify(path: Path, expected: str, label: str) -> str:
    actual = _sha256(path)
    if actual != expected:
        raise B2ValidationError(
            f"{label} SHA-256 mismatch: expected={expected} actual={actual}"
        )
    return actual


def _gzip(content: bytes) -> bytes:
    return gzip.compress(content, compresslevel=9, mtime=0)


def _gzip_csv(rows: Sequence[Mapping[str, Any]], fields: Sequence[str]) -> bytes:
    buffer = io.StringIO(newline="")
    writer = csv.DictWriter(buffer, fieldnames=fields, lineterminator="\n")
    writer.writeheader()
    for row in rows:
        writer.writerow({field: row.get(field, "") for field in fields})
    return _gzip(buffer.getvalue().encode("utf-8"))


def _publish(budget: StorageBudget, path: Path, content: bytes) -> str:
    budget.check_publication(path, content)
    return publish_immutable_bytes(path, content)


def _check_prohibited(value: Any, path: str = "response") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            if str(key).casefold() in PROHIBITED_KEYS:
                raise B2ValidationError(f"prohibited outcome field at {path}.{key}")
            _check_prohibited(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _check_prohibited(item, f"{path}[{index}]")


def _load_candidates(
    planner_path: Path, smoke_plan_path: Path, market_metadata_path: Path
) -> tuple[list[TickerCandidate], dict[str, Any]]:
    planner_rows, smoke_plan, full_stats = _load_selected_planner_rows(
        planner_path, smoke_plan_path
    )
    selected = {
        (row["family_id"], row["family_id_source"]): row for row in planner_rows
    }
    associated = {
        identity: set(
            decode_market_tickers(row["family_id"], row["associated_market_tickers_compact"])
        )
        for identity, row in selected.items()
    }
    candidates: list[TickerCandidate] = []
    seen: dict[tuple[str, str], set[str]] = defaultdict(set)
    with gzip.open(market_metadata_path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        required = {
            "ticker",
            "event_ticker",
            "family_id",
            "family_id_source",
            "open_time",
            "diagnostic_settlement_ts",
        }
        if not required.issubset(header):
            raise B2ValidationError("market metadata routing projection changed")
        index = {field: header.index(field) for field in required}
        for source in reader:
            identity = (source[index["family_id"]], source[index["family_id_source"]])
            plan = selected.get(identity)
            if plan is None:
                continue
            ticker = source[index["ticker"]]
            if ticker not in associated[identity] or ticker in seen[identity]:
                raise B2ValidationError("market association is duplicate or inconsistent")
            seen[identity].add(ticker)
            if source[index["event_ticker"]] != plan["event_ticker"]:
                raise B2ValidationError("event identity changed")
            if classify_market_open(source[index["open_time"]], plan["target_time"]) != EXISTED:
                continue
            candidates.append(
                TickerCandidate(
                    family_id=plan["family_id"],
                    family_id_source=plan["family_id_source"],
                    event_ticker=plan["event_ticker"],
                    rule=plan["rule"],
                    category=plan["category"],
                    timing_structure=plan["timing_structure"],
                    target_time=plan["target_time"],
                    ticker=ticker,
                    family_market_count=int(plan["market_count"]),
                    settlement_time=source[index["diagnostic_settlement_ts"]],
                )
            )
    if sum(len(values) for values in seen.values()) != sum(
        len(values) for values in associated.values()
    ):
        raise B2ValidationError("not all pinned smoke market associations were found")
    if len(candidates) != 12137:
        raise B2ValidationError(f"expected 12,137 eligible tickers, found {len(candidates)}")
    return candidates, {"smoke_plan": smoke_plan, "full_planner": full_stats}


def _sample_rows(
    sample: Sequence[TickerCandidate], routes: Mapping[str, str] | None = None
) -> list[dict[str, Any]]:
    result = []
    for index, candidate in enumerate(sample, 1):
        result.append(
            {
                "sample_index": index,
                "family_id": candidate.family_id,
                "family_id_source": candidate.family_id_source,
                "event_ticker": candidate.event_ticker,
                "rule": candidate.rule,
                "category": candidate.category,
                "timing_structure": candidate.timing_structure,
                "target_time": candidate.target_time,
                "market_ticker": candidate.ticker,
                "family_market_count": candidate.family_market_count,
                "family_size_stratum": family_size_stratum(
                    candidate.family_market_count
                ),
                "target_month": candidate.target_time[:7],
                "routing_tier": routes.get(candidate.ticker, "") if routes else "",
            }
        )
    return result


class BoundedNetworkClient:
    def __init__(
        self,
        *,
        session: Any,
        output_root: Path,
        budget: StorageBudget,
        base_url: str,
        max_requests: int = MAX_NETWORK_REQUESTS,
        max_retries: int = 3,
        timeout_seconds: float = 45,
        requests_per_second: float = 3,
        network_forbidden: bool = False,
        sleep=time.sleep,
        monotonic=time.monotonic,
    ) -> None:
        self.session = session
        self.output_root = output_root
        self.budget = budget
        self.base_url = base_url.rstrip("/")
        self.max_requests = max_requests
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self.requests_per_second = requests_per_second
        self.network_forbidden = network_forbidden
        self.sleep = sleep
        self.monotonic = monotonic
        self.physical_requests = 0
        self.retries = 0
        self.rate_limits = 0
        self.resume_hits = 0
        self.first_network_at: float | None = None
        self.last_network_at: float | None = None
        self.last_request_at: float | None = None

    def _pace(self) -> None:
        now = self.monotonic()
        if self.last_request_at is not None:
            wait = (1 / self.requests_per_second) - (now - self.last_request_at)
            if wait > 0:
                self.sleep(wait)
        self.last_request_at = self.monotonic()

    def _network_get(self, path: str, params: Mapping[str, Any]) -> Any:
        if self.network_forbidden:
            raise B2ValidationError("no-network resume cache miss")
        if self.physical_requests >= self.max_requests:
            raise B2ValidationError("202-request hard cap would be exceeded")
        self._pace()
        now = self.monotonic()
        if self.first_network_at is None:
            self.first_network_at = now
        self.physical_requests += 1
        response = self.session.get(
            self.base_url + path,
            params=dict(params),
            timeout=self.timeout_seconds,
        )
        self.last_network_at = self.monotonic()
        return response

    @property
    def elapsed_network_seconds(self) -> float:
        if self.first_network_at is None or self.last_network_at is None:
            return 0.0
        return max(0.0, self.last_network_at - self.first_network_at)

    def _raw_paths(self, request_id: str) -> tuple[Path, Path]:
        return (
            self.output_root / "raw" / f"request_{request_id}.json.gz",
            self.output_root / "request_commits" / f"request_{request_id}.json",
        )

    def _validate_cached(
        self, request: Mapping[str, Any], request_id: str
    ) -> tuple[dict[str, Any], dict[str, Any]] | None:
        raw, commit_path = self._raw_paths(request_id)
        if not commit_path.exists() and not raw.exists():
            return None
        if not commit_path.exists() or not raw.exists():
            raise B2ValidationError("partial immutable request publication")
        wrapper = json.loads(gzip.decompress(raw.read_bytes()))
        commit = json.loads(commit_path.read_text(encoding="utf-8"))
        if wrapper.get("schema_version") != SCHEMA_VERSION or wrapper.get("request") != request:
            raise B2ValidationError("cached B2 request identity mismatch")
        response = wrapper.get("response")
        if wrapper.get("response_sha256") != hashlib.sha256(canonical_json(response)).hexdigest():
            raise B2ValidationError("cached B2 response hash mismatch")
        if commit.get("raw_sha256") != _sha256(raw) or commit.get("request_id") != request_id:
            raise B2ValidationError("cached B2 commit mismatch")
        self.resume_hits += 1
        return wrapper, commit

    def fetch_cutoff(self) -> tuple[dict[str, Any], dict[str, Any]]:
        request = {"endpoint": CUTOFF_PATH, "params": {}, "purpose": "routing_cutoff"}
        request_id = hashlib.sha256(canonical_json(request)).hexdigest()[:24]
        cached = self._validate_cached(request, request_id)
        if cached is not None:
            wrapper, commit = cached
            return dict(wrapper["response"]), commit
        response = self._network_get(CUTOFF_PATH, {})
        response.raise_for_status()
        payload = response.json()
        reject_sensitive_response(payload)
        _check_prohibited(payload)
        required = {"market_settled_ts", "trades_created_ts", "orders_updated_ts"}
        if not isinstance(payload, Mapping) or not required.issubset(payload):
            raise B2ValidationError("historical cutoff schema changed")
        if route_for_settlement(payload["market_settled_ts"], payload["market_settled_ts"]) != LIVE_ROUTE:
            raise B2ValidationError("historical cutoff timestamp is invalid")
        retrieved = format_iso_utc(datetime.now(timezone.utc))
        wrapper = {
            "schema_version": SCHEMA_VERSION,
            "request": request,
            "http_status": int(response.status_code),
            "retrieved_at_utc": retrieved,
            "response_sha256": hashlib.sha256(canonical_json(payload)).hexdigest(),
            "response": payload,
        }
        raw_content = _gzip(canonical_json(wrapper) + b"\n")
        raw, commit_path = self._raw_paths(request_id)
        _publish(self.budget, raw, raw_content)
        commit = {
            "schema_version": SCHEMA_VERSION,
            "request_id": request_id,
            "request": request,
            "raw_path": str(raw.relative_to(self.output_root)),
            "raw_sha256": _sha256(raw),
            "compressed_bytes": raw.stat().st_size,
            "uncompressed_response_bytes": len(canonical_json(payload)),
            "http_status": int(response.status_code),
            "retrieved_at_utc": retrieved,
            "success": True,
            "complete": True,
        }
        _publish(self.budget, commit_path, canonical_json(commit) + b"\n")
        return dict(payload), commit

    def fetch_candles(
        self,
        *,
        ticker: str,
        route: str,
        start_ts: int,
        end_ts: int,
        cutoff_hash: str,
        purpose: str = "sample_price_window",
    ) -> tuple[list[dict[str, Any]], dict[str, Any]]:
        if route == HISTORICAL_ROUTE:
            path = f"/historical/markets/{quote(ticker, safe='')}/candlesticks"
            params = {"start_ts": start_ts, "end_ts": end_ts, "period_interval": 1}
        elif route == LIVE_ROUTE:
            path = "/markets/candlesticks"
            params = {
                "market_tickers": ticker,
                "start_ts": start_ts,
                "end_ts": end_ts,
                "period_interval": 1,
                "include_latest_before_start": "false",
            }
        else:
            raise B2ValidationError("unknown request route")
        request = {
            "endpoint": path,
            "params": params,
            "purpose": purpose,
            "route": route,
            "routing_cutoff_sha256": cutoff_hash,
        }
        request_id = hashlib.sha256(canonical_json(request)).hexdigest()[:24]
        cached = self._validate_cached(request, request_id)
        if cached is not None:
            wrapper, commit = cached
            if not commit["success"]:
                return [], commit
            return normalize_response(wrapper["response"], route=route, ticker=ticker), commit

        attempts = 0
        rate_limits = 0
        response = None
        transport_error = ""
        while attempts < self.max_retries:
            attempts += 1
            try:
                response = self._network_get(path, params)
                if response.status_code == 429:
                    rate_limits += 1
                    self.rate_limits += 1
                    raise RuntimeError("rate_limited")
                if response.status_code >= 500:
                    raise RuntimeError(f"server_{response.status_code}")
                break
            except B2ValidationError:
                raise
            except Exception as exc:
                transport_error = str(exc)
                if attempts >= self.max_retries:
                    break
                self.retries += 1
                self.sleep(min(2 ** (attempts - 1), 30))

        status = int(response.status_code) if response is not None else 0
        success = status == 200
        failure_kind = ""
        payload: Any
        if success:
            try:
                payload = response.json()
            except Exception as exc:
                raise B2ValidationError("successful candlestick response is not JSON") from exc
            reject_sensitive_response(payload)
            _check_prohibited(payload)
            normalized = normalize_response(payload, route=route, ticker=ticker)
            if any(row["end_period_ts"] > end_ts for row in normalized):
                raise B2ValidationError("API returned a post-request-end candle")
        else:
            normalized = []
            failure_kind = (
                "http_404"
                if status == 404
                else f"http_{status}"
                if status
                else "transport_failure"
            )
            try:
                payload = response.json() if response is not None else {"transport_error": transport_error}
            except Exception:
                content = response.content if response is not None else b""
                payload = {
                    "non_json_body_sha256": hashlib.sha256(content).hexdigest(),
                    "non_json_body_bytes": len(content),
                }
            reject_sensitive_response(payload)
            _check_prohibited(payload)

        wrapper = {
            "schema_version": SCHEMA_VERSION,
            "request": request,
            "http_status": status,
            "response_sha256": hashlib.sha256(canonical_json(payload)).hexdigest(),
            "attempts": attempts,
            "retries": max(0, attempts - 1),
            "rate_limits": rate_limits,
            "response": payload,
        }
        raw_content = _gzip(canonical_json(wrapper) + b"\n")
        raw, commit_path = self._raw_paths(request_id)
        _publish(self.budget, raw, raw_content)
        commit = {
            "schema_version": SCHEMA_VERSION,
            "request_id": request_id,
            "request": request,
            "raw_path": str(raw.relative_to(self.output_root)),
            "raw_sha256": _sha256(raw),
            "compressed_bytes": raw.stat().st_size,
            "uncompressed_response_bytes": len(canonical_json(payload)),
            "http_status": status,
            "attempts": attempts,
            "retries": max(0, attempts - 1),
            "rate_limits": rate_limits,
            "success": success,
            "failure_kind": failure_kind,
            "candle_count": len(normalized),
            "schema_variants": sorted({row["schema_variant"] for row in normalized}),
            "complete": True,
        }
        _publish(self.budget, commit_path, canonical_json(commit) + b"\n")
        return normalized, commit


def _preflight(
    *, budget: StorageBudget, sample: Sequence[TickerCandidate], sample_hash: str
) -> dict[str, Any]:
    if len(sample) != SAMPLE_SIZE:
        raise B2ValidationError("B2 sample is not exactly 200 tickers")
    state = budget.snapshot()
    budget.check_additional(CONSERVATIVE_ADDITIONAL_BYTES)
    families = Counter(row.family_identity for row in sample)
    return {
        "schema_version": SCHEMA_VERSION,
        "sample_seed": SAMPLE_SEED,
        "sample_sha256": sample_hash,
        "sample_ticker_count": len(sample),
        "represented_family_count": len(families),
        "maximum_tickers_per_family": max(families.values()),
        "maximum_total_network_requests": MAX_NETWORK_REQUESTS,
        "request_budget_allocation": {
            "cutoff": 1,
            "sample_tickers": 200,
            "maximum_boundary_probes_remaining": 1,
        },
        "conservative_additional_bytes": CONSERVATIVE_ADDITIONAL_BYTES,
        "storage_before": state,
        "projected_namespace_bytes": state["used_bytes"] + CONSERVATIVE_ADDITIONAL_BYTES,
        "projected_free_bytes": state["free_bytes"] - CONSERVATIVE_ADDITIONAL_BYTES,
        "passes_namespace_ceiling": state["used_bytes"] + CONSERVATIVE_ADDITIONAL_BYTES <= state["max_bytes"],
        "passes_free_space_floor": state["free_bytes"] - CONSERVATIVE_ADDITIONAL_BYTES >= state["min_free_bytes"],
        "network_requests_made": 0,
    }


def _boundary_probe(
    *,
    client: BoundedNetworkClient,
    sample: Sequence[TickerCandidate],
    routes: Mapping[str, str],
    candles_by_ticker: Mapping[str, list[dict[str, Any]]],
    cutoff_hash: str,
) -> tuple[dict[str, Any], dict[str, Any]]:
    candidates = []
    by_ticker = {row.ticker: row for row in sample}
    for ticker, candles in candles_by_ticker.items():
        target = by_ticker[ticker].target_ts
        if target % 60 == 0 and any(row["end_period_ts"] == target for row in candles):
            candidates.append(ticker)
    if not candidates:
        raise B2ValidationError(
            "no sampled response contains an exact-target minute candle; boundary remains ambiguous"
        )
    ticker = sorted(candidates)[0]
    target = by_ticker[ticker].target_ts
    probe_rows, commit = client.fetch_candles(
        ticker=ticker,
        route=routes[ticker],
        start_ts=target - 60,
        end_ts=target - 1,
        cutoff_hash=cutoff_hash,
        purpose="boundary_end_minus_one",
    )
    excluded = all(row["end_period_ts"] < target for row in probe_rows)
    if not excluded:
        raise B2ValidationError("boundary probe returned exact/post-target candle")
    return {
        "passed": True,
        "documented_semantics": "inclusive_end_period_ts",
        "base_exact_target_candle_observed": True,
        "end_minus_one_probe_excluded_exact_target": True,
        "target_on_minute_boundary": True,
        "ticker": ticker,
        "target_ts": target,
        "probe_request_id": commit["request_id"],
        "post_target_information_accepted": False,
    }, commit


def _counts(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    return {
        "sample_tickers": len(rows),
        "successful_historical_requests": sum(row["request_success"] and row["routing_tier"] == HISTORICAL_ROUTE for row in rows),
        "successful_live_requests": sum(row["request_success"] and row["routing_tier"] == LIVE_ROUTE for row in rows),
        "empty_responses": sum(bool(row["empty_response"]) for row in rows),
        "http_404_failures": sum(row["failure_kind"] == "http_404" for row in rows),
        "other_http_or_transport_failures": sum(bool(row["failure_kind"]) and row["failure_kind"] != "http_404" for row in rows),
        "candles_returned": sum(int(row["candle_count"]) for row in rows),
        "post_target_candles": sum(int(row["post_target_candle_count"]) for row in rows),
        "duplicate_candles": sum(int(row["duplicate_candle_count"]) for row in rows),
        "missing_bid": sum(bool(row["missing_bid"]) for row in rows),
        "missing_ask": sum(bool(row["missing_ask"]) for row in rows),
        "usable_midpoint_15m": sum(bool(row["midpoint_within_15m"]) for row in rows),
        "usable_midpoint_60m": sum(bool(row["midpoint_within_60m"]) for row in rows),
        "usable_trade_close_15m": sum(bool(row["trade_within_15m"]) for row in rows),
        "usable_trade_close_60m": sum(bool(row["trade_within_60m"]) for row in rows),
    }


def _breakdowns(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    result = {}
    for field in ("rule", "category", "target_month", "family_size_stratum", "routing_tier"):
        groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[str(row[field])].append(row)
        result[f"by_{field}"] = {key: _counts(group) for key, group in sorted(groups.items())}
    return result


def _projection(
    *,
    rows: Sequence[Mapping[str, Any]],
    commits: Sequence[Mapping[str, Any]],
    normalized_bytes: int,
    request_commit_bytes: int,
    manifest_bytes: int,
    physical_requests: int,
    elapsed_seconds: float,
    storage: Mapping[str, int],
) -> dict[str, Any]:
    ticker_commits = [commit for commit in commits if commit["request"]["purpose"] == "sample_price_window"]
    raw_bytes = sum(int(commit["compressed_bytes"]) for commit in ticker_commits)
    retries = sum(int(commit["retries"]) for commit in ticker_commits)
    raw_per_ticker = raw_bytes / SAMPLE_SIZE
    normalized_per_ticker = normalized_bytes / SAMPLE_SIZE
    operational_per_ticker = (request_commit_bytes + manifest_bytes) / SAMPLE_SIZE
    rate = physical_requests / elapsed_seconds if elapsed_seconds > 0 else None
    retry_rate = retries / SAMPLE_SIZE

    def scope(tickers: int) -> dict[str, Any]:
        requests = tickers + 2
        raw = math.ceil(raw_per_ticker * tickers)
        normalized = math.ceil(normalized_per_ticker * tickers)
        operational = math.ceil(operational_per_ticker * tickers)
        total = raw + normalized + operational
        return {
            "ticker_count": tickers,
            "projected_network_requests_including_cutoff_and_probe": requests,
            "projected_wall_clock_seconds": requests / rate if rate else None,
            "projected_retry_requests": math.ceil(retry_rate * tickers),
            "projected_compressed_raw_bytes": raw,
            "projected_normalized_bytes": normalized,
            "projected_request_commit_and_manifest_bytes": operational,
            "projected_total_namespace_bytes": total,
            "fits_current_namespace_ceiling": storage["used_bytes"] + total <= storage["max_bytes"],
            "fits_current_free_space_floor": storage["free_bytes"] - total >= storage["min_free_bytes"],
        }

    production = scope(4586979)
    feasible = bool(
        production["fits_current_namespace_ceiling"]
        and production["fits_current_free_space_floor"]
        and production["projected_wall_clock_seconds"] is not None
        and production["projected_wall_clock_seconds"] <= 7 * 24 * 3600
    )
    return {
        "measured_compressed_raw_bytes_per_ticker_request": raw_per_ticker,
        "measured_normalized_bytes_per_ticker": normalized_per_ticker,
        "measured_request_commit_and_manifest_bytes_per_ticker": operational_per_ticker,
        "measured_requests_per_second": rate,
        "measured_retry_requests_per_ticker": retry_rate,
        "current_smoke_scope": scope(12137),
        "production_scope": production,
        "per_market_historical_candlesticks_operationally_feasible": feasible,
        "alternatives": {
            "A_historical_per_market_candlesticks": "Measured here; preserves quote and trade-close fields but scales one request per ticker.",
            "B_historical_trades_bounded_windows": "Potentially fewer globally paginated requests, but supplies trades rather than contemporaneous bid/ask and cannot implement the midpoint definition alone.",
            "C_statistically_defensible_contract_subsample": "Preserves the approved price definition for sampled contracts and is the leading option if census requests are infeasible; sampling weights and family clustering must be frozen ex ante.",
            "D_event_or_batch_archive_route": "Prefer only if Kalshi documents an archived multi-market/event endpoint; the tested live batch route cannot serve this scope.",
        },
        "recommended_architecture_if_infeasible": (
            "Use a predeclared, family-aware probability/contract subsample with historical per-market candles; "
            "optionally acquire historical trades separately for trade-close robustness. Do not treat trades as a midpoint substitute."
        ),
    }


def _acceptance_projection(
    *, report: Mapping[str, Any], output_root: Path, budget: StorageBudget
) -> dict[str, Any]:
    """Correctly include commit/manifest overhead in the acceptance estimate."""
    physical = int(report["physical_network_requests"])
    elapsed = float(report["network_elapsed_seconds"])
    rate = physical / elapsed
    raw_per_ticker = float(
        report["feasibility"]["measured_compressed_raw_bytes_per_ticker_request"]
    )
    normalized_per_ticker = float(
        report["feasibility"]["measured_normalized_bytes_per_ticker"]
    )
    commit_bytes = sum(
        path.stat().st_size
        for path in (output_root / "request_commits").glob("*.json")
    )
    manifest_bytes = (output_root / "phase_10f_b2_request_manifest.jsonl").stat().st_size
    operational_per_ticker = (commit_bytes + manifest_bytes) / SAMPLE_SIZE
    state = budget.snapshot()

    def scope(tickers: int) -> dict[str, Any]:
        requests = tickers + 2
        raw = math.ceil(raw_per_ticker * tickers)
        normalized = math.ceil(normalized_per_ticker * tickers)
        operational = math.ceil(operational_per_ticker * tickers)
        total = raw + normalized + operational
        return {
            "ticker_count": tickers,
            "projected_network_requests_including_cutoff_and_probe": requests,
            "projected_wall_clock_seconds": requests / rate,
            "projected_compressed_raw_bytes": raw,
            "projected_normalized_bytes": normalized,
            "projected_request_commit_and_manifest_bytes": operational,
            "projected_total_namespace_bytes": total,
            "fits_current_namespace_ceiling": state["used_bytes"] + total
            <= state["max_bytes"],
            "fits_current_free_space_floor": state["free_bytes"] - total
            >= state["min_free_bytes"],
        }

    return {
        "schema_version": SCHEMA_VERSION,
        "source_report_sha256": _sha256(output_root / "phase_10f_b2_report.json"),
        "correction": (
            "Acceptance projection includes immutable per-request commits and the "
            "deterministic manifest, which the initial raw-plus-normalized estimate omitted."
        ),
        "measured_total_requests_per_second": rate,
        "measured_compressed_raw_bytes_per_ticker_request": raw_per_ticker,
        "measured_normalized_bytes_per_ticker": normalized_per_ticker,
        "measured_request_commit_and_manifest_bytes_per_ticker": operational_per_ticker,
        "current_smoke_scope": scope(12137),
        "production_scope": scope(4586979),
        "per_market_historical_candlesticks_operationally_feasible": False,
        "reason": (
            "Millions of serial per-ticker requests require many weeks at the measured "
            "rate and the full auditable namespace projection exceeds the 5 GiB ceiling."
        ),
    }


def run(args: argparse.Namespace, *, session: Any | None = None) -> dict[str, Any]:
    inputs = {
        "planner_sha256": _verify(args.planner, args.expected_planner_sha256, "planner"),
        "smoke_plan_sha256": _verify(args.smoke_plan, args.expected_smoke_plan_sha256, "smoke plan"),
        "market_metadata_sha256": _verify(args.market_metadata, args.expected_market_metadata_sha256, "market metadata"),
        "phase_10f_b_incomplete_report_sha256": _verify(args.phase_10f_b_report, args.expected_phase_10f_b_report_sha256, "Phase 10F-B report"),
    }
    rules = load_study_rules(args.config)
    if rules.fingerprint != args.expected_study_rules_fingerprint:
        raise B2ValidationError("frozen StudyRules fingerprint changed")
    inputs["study_rules_fingerprint"] = rules.fingerprint
    candidates, planner_context = _load_candidates(
        args.planner, args.smoke_plan, args.market_metadata
    )
    sample = select_ticker_sample(candidates)
    sample_hash = sample_identity(sample)
    budget = StorageBudget(
        args.guard_root,
        max_bytes=args.max_generated_bytes,
        min_free_bytes=args.min_free_bytes,
    )
    preflight = _preflight(budget=budget, sample=sample, sample_hash=sample_hash)
    if args.preflight_only:
        result = {
            **preflight,
            "category_counts": dict(sorted(Counter(row.category for row in sample).items())),
            "rule_counts": dict(sorted(Counter(row.rule for row in sample).items())),
            "month_counts": dict(sorted(Counter(row.target_time[:7] for row in sample).items())),
            "family_size_counts": dict(sorted(Counter(family_size_stratum(row.family_market_count) for row in sample).items())),
        }
        print(json.dumps(result, sort_keys=True))
        return result

    final_commit_path = args.output_root / "phase_10f_b2_commit.json"
    if final_commit_path.exists():
        commit = json.loads(final_commit_path.read_text(encoding="utf-8"))
        for artifact in commit["artifacts"]:
            path = args.output_root / artifact["path"]
            if _sha256(path) != artifact["sha256"]:
                raise B2ValidationError("existing B2 final artifact conflict")
        report = json.loads((args.output_root / "phase_10f_b2_report.json").read_text())
        replay = BoundedNetworkClient(
            session=None,
            output_root=args.output_root,
            budget=budget,
            base_url=args.base_url,
            max_requests=MAX_NETWORK_REQUESTS,
            network_forbidden=True,
        )
        cutoff, cutoff_commit = replay.fetch_cutoff()
        cutoff_hash = cutoff_commit["raw_sha256"]
        routes = {
            row.ticker: route_for_settlement(
                row.settlement_time, cutoff["market_settled_ts"]
            )
            for row in sample
        }
        for candidate in sample:
            replay.fetch_candles(
                ticker=candidate.ticker,
                route=routes[candidate.ticker],
                start_ts=candidate.target_ts - 3600,
                end_ts=candidate.target_ts,
                cutoff_hash=cutoff_hash,
            )
        boundary = report["candle_boundary_validation"]
        probe_ticker = str(boundary["ticker"])
        probe_target = int(boundary["target_ts"])
        replay.fetch_candles(
            ticker=probe_ticker,
            route=routes[probe_ticker],
            start_ts=probe_target - 60,
            end_ts=probe_target - 1,
            cutoff_hash=cutoff_hash,
            purpose="boundary_end_minus_one",
        )
        resume = {
            "expected_request_commits": MAX_NETWORK_REQUESTS,
            "validated_request_commits": replay.resume_hits,
            "physical_network_requests": replay.physical_requests,
            "passed": replay.resume_hits == MAX_NETWORK_REQUESTS
            and replay.physical_requests == 0,
        }
        if not resume["passed"]:
            raise B2ValidationError("B2 no-network resume validation failed")
        acceptance = _acceptance_projection(
            report=report, output_root=args.output_root, budget=budget
        )
        acceptance_content = (
            json.dumps(acceptance, indent=2, sort_keys=True).encode("utf-8") + b"\n"
        )
        acceptance_path = args.output_root / "phase_10f_b2_acceptance_report.json"
        _publish(budget, acceptance_path, acceptance_content)
        result = {
            **report,
            "storage_now": budget.snapshot(),
            "existing_final_reused": True,
            "deterministic_no_network_resume": resume,
            "acceptance_projection": acceptance,
            "acceptance_report_sha256": _sha256(acceptance_path),
        }
        print(json.dumps(result, sort_keys=True))
        return result

    client = BoundedNetworkClient(
        session=session or requests.Session(),
        output_root=args.output_root,
        budget=budget,
        base_url=args.base_url,
        max_requests=MAX_NETWORK_REQUESTS,
        max_retries=args.max_retries,
        timeout_seconds=args.timeout_seconds,
        requests_per_second=args.requests_per_second,
        network_forbidden=args.no_network_resume,
    )
    cutoff, cutoff_commit = client.fetch_cutoff()
    cutoff_hash = cutoff_commit["raw_sha256"]
    routes = {
        row.ticker: route_for_settlement(row.settlement_time, cutoff["market_settled_ts"])
        for row in sample
    }
    sample_rows = _sample_rows(sample, routes)
    sample_content = _gzip_csv(sample_rows, SAMPLE_FIELDS)
    _publish(budget, args.output_root / "phase_10f_b2_ticker_sample.csv.gz", sample_content)

    commits: list[dict[str, Any]] = [cutoff_commit]
    candles_by_ticker: dict[str, list[dict[str, Any]]] = {}
    normalized_rows: list[dict[str, Any]] = []
    sample_projection = {row["market_ticker"]: row for row in sample_rows}
    by_ticker = {row.ticker: row for row in sample}
    for candidate in sample:
        candles, commit = client.fetch_candles(
            ticker=candidate.ticker,
            route=routes[candidate.ticker],
            start_ts=candidate.target_ts - 3600,
            end_ts=candidate.target_ts,
            cutoff_hash=cutoff_hash,
        )
        commits.append(commit)
        candles_by_ticker[candidate.ticker] = candles
        observation = extract_observation(candles, target_ts=candidate.target_ts)
        normalized_rows.append(
            {
                **sample_projection[candidate.ticker],
                "request_id": commit["request_id"],
                "http_status": commit["http_status"],
                "request_success": commit["success"],
                "failure_kind": commit["failure_kind"],
                "empty_response": bool(commit["success"] and not candles),
                **observation,
            }
        )

    boundary, boundary_commit = _boundary_probe(
        client=client,
        sample=sample,
        routes=routes,
        candles_by_ticker=candles_by_ticker,
        cutoff_hash=cutoff_hash,
    )
    commits.append(boundary_commit)
    if client.physical_requests > MAX_NETWORK_REQUESTS:
        raise B2ValidationError("202-request hard cap exceeded")

    normalized_content = _gzip_csv(normalized_rows, NORMALIZED_FIELDS)
    manifest_content = b"".join(
        canonical_json(commit) + b"\n"
        for commit in sorted(commits, key=lambda item: item["request_id"])
    )
    artifacts = {
        "phase_10f_b2_normalized.csv.gz": normalized_content,
        "phase_10f_b2_request_manifest.jsonl": manifest_content,
    }
    for name, content in artifacts.items():
        _publish(budget, args.output_root / name, content)
    counts = _counts(normalized_rows)
    spread = diagnostic_distribution(
        [float(row["spread"]) for row in normalized_rows if row.get("spread") is not None]
    )
    storage_before_final = budget.snapshot()
    feasibility = _projection(
        rows=normalized_rows,
        commits=commits,
        normalized_bytes=len(normalized_content),
        request_commit_bytes=sum(
            (args.output_root / "request_commits" / f"request_{commit['request_id']}.json").stat().st_size
            for commit in commits
        ),
        manifest_bytes=len(manifest_content),
        physical_requests=client.physical_requests,
        elapsed_seconds=client.elapsed_network_seconds,
        storage=storage_before_final,
    )
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "inputs": inputs,
        "sample_seed": SAMPLE_SEED,
        "sample_sha256": sample_hash,
        "sample_method": (
            "one hash-ranked ticker from every one of 135 eligible families, plus a second "
            "ticker from 28 Crypto, 23 Financials, and 14 Climate/Weather families; maximum two per family"
        ),
        "cutoff": {
            "market_settled_ts": cutoff["market_settled_ts"],
            "retrieved_at_utc": cutoff_commit["retrieved_at_utc"],
            "raw_sha256": cutoff_hash,
            "routing_use_only": True,
        },
        "official_document_hashes": {
            "historical_candlesticks": HISTORICAL_DOC_SHA256,
            "historical_cutoff": CUTOFF_DOC_SHA256,
        },
        "boundary": boundary,
        "settlement_timestamp_used_as_research_feature": False,
        "outcome_fields_accessed": 0,
        "study_rules_changed": False,
        "production_acquisition_started": False,
    }
    provenance_content = json.dumps(provenance, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    _publish(budget, args.output_root / "phase_10f_b2_provenance.json", provenance_content)
    report = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "inputs": inputs,
        "preflight": preflight,
        "sample_sha256": sample_hash,
        "cutoff_retrieved_at_utc": cutoff_commit["retrieved_at_utc"],
        "cutoff_market_settled_ts": cutoff["market_settled_ts"],
        "routing_counts": dict(sorted(Counter(routes.values()).items())),
        "diagnostics": counts,
        **_breakdowns(normalized_rows),
        "schema_variants": dict(sorted(Counter(row["schema_variant"] or "empty" for row in normalized_rows).items())),
        "earliest_end_period_ts": min((row["earliest_end_period_ts"] for row in normalized_rows if row["earliest_end_period_ts"] is not None), default=None),
        "latest_end_period_ts": max((row["latest_end_period_ts"] for row in normalized_rows if row["latest_end_period_ts"] is not None), default=None),
        "spread_diagnostics": spread,
        "candle_boundary_validation": boundary,
        "physical_network_requests": client.physical_requests,
        "resume_hits": client.resume_hits,
        "retries": sum(int(commit.get("retries", 0)) for commit in commits),
        "rate_limits": sum(int(commit.get("rate_limits", 0)) for commit in commits),
        "network_elapsed_seconds": client.elapsed_network_seconds,
        "compressed_raw_bytes": sum(int(commit["compressed_bytes"]) for commit in commits),
        "uncompressed_response_bytes": sum(int(commit["uncompressed_response_bytes"]) for commit in commits),
        "normalized_output_bytes": len(normalized_content),
        "feasibility": feasibility,
        "outcome_fields_accessed": 0,
        "production_acquisition_started": False,
        "storage_at_report": budget.snapshot(),
    }
    report_content = json.dumps(report, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    _publish(budget, args.output_root / "phase_10f_b2_report.json", report_content)
    all_artifact_names = (
        "phase_10f_b2_ticker_sample.csv.gz",
        "phase_10f_b2_normalized.csv.gz",
        "phase_10f_b2_request_manifest.jsonl",
        "phase_10f_b2_provenance.json",
        "phase_10f_b2_report.json",
    )
    artifact_refs = [
        {
            "path": name,
            "sha256": _sha256(args.output_root / name),
            "bytes": (args.output_root / name).stat().st_size,
        }
        for name in all_artifact_names
    ]
    final_commit = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "sample_sha256": sample_hash,
        "request_commit_count": len(commits),
        "physical_network_request_count": client.physical_requests,
        "artifacts": artifact_refs,
    }
    _publish(
        budget,
        final_commit_path,
        canonical_json(final_commit) + b"\n",
    )
    final = {
        **report,
        "final_commit_sha256": _sha256(final_commit_path),
        "output_hashes": {item["path"]: item["sha256"] for item in artifact_refs},
        "storage_now": budget.snapshot(),
    }
    print(json.dumps(final, sort_keys=True))
    return final


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--planner", required=True, type=Path)
    parser.add_argument("--smoke-plan", required=True, type=Path)
    parser.add_argument("--market-metadata", required=True, type=Path)
    parser.add_argument("--phase-10f-b-report", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--guard-root", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--expected-planner-sha256", required=True)
    parser.add_argument("--expected-smoke-plan-sha256", required=True)
    parser.add_argument("--expected-market-metadata-sha256", required=True)
    parser.add_argument("--expected-phase-10f-b-report-sha256", required=True)
    parser.add_argument("--expected-study-rules-fingerprint", required=True)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--max-retries", type=int, default=3)
    parser.add_argument("--timeout-seconds", type=float, default=45)
    parser.add_argument("--requests-per-second", type=float, default=3)
    parser.add_argument("--max-generated-bytes", type=int, default=DEFAULT_MAX_BYTES)
    parser.add_argument("--min-free-bytes", type=int, default=DEFAULT_MIN_FREE_BYTES)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument("--no-network-resume", action="store_true")
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        run(parse_args(argv))
        return 0
    except (B2ValidationError, CacheError, OSError, ValueError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
