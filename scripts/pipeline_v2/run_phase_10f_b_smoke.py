"""Run the authorized outcome-blind, 200-family Phase 10F-B price smoke."""

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
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Any, Iterable, Mapping, Sequence

import requests

from scripts.common.time_utils import parse_iso_utc
from scripts.pipeline_v2.kalshi_metadata_cache import (
    CacheError,
    ResourceLimitError,
    StorageBudget,
    canonical_json,
    publish_immutable_bytes,
    reject_sensitive_response,
)
from scripts.pipeline_v2.phase_10f_planner import (
    EXISTED,
    OPENED_AFTER,
    classify_market_open,
    decode_market_tickers,
)
from scripts.pipeline_v2.phase_10f_smoke import (
    BATCH_ENDPOINT,
    CANDLE_TIMESTAMP_SEMANTICS,
    MAX_SMOKE_FAMILIES,
    OFFICIAL_BATCH_DOC_SHA256,
    OFFICIAL_BATCH_DOC_URL,
    RequestGroup,
    SCHEMA_VERSION,
    SmokeFamily,
    SmokeValidationError,
    build_request_groups,
    extract_contract_observation,
    request_group_id,
    sha256_bytes,
    spread_diagnostics,
    validate_batch_payload,
)
from scripts.pipeline_v2.study_rules import load_study_rules


DEFAULT_BASE_URL = "https://external-api.kalshi.com"
DEFAULT_MAX_BYTES = 5 * 1024**3
DEFAULT_MIN_FREE_BYTES = 80 * 1024**3
EXPECTED_FAMILY_COUNT = 200
EXPECTED_ELIGIBLE_TICKERS = 12137
EXPECTED_LOGICAL_GROUPS = 206
PROHIBITED_RESPONSE_KEYS = frozenset(
    {"result", "outcome", "settlement_value", "settlement_ts", "settlement_time"}
)
CONTRACT_FIELDS = (
    "family_id",
    "family_id_source",
    "event_ticker",
    "rule",
    "category",
    "timing_structure",
    "market_ticker",
    "target_time",
    "request_id",
    "api_data_failure",
    "candle_count",
    "midpoint_status",
    "midpoint_reason",
    "yes_bid",
    "yes_ask",
    "midpoint",
    "spread",
    "midpoint_observation_time",
    "midpoint_staleness_minutes",
    "midpoint_within_15m",
    "midpoint_within_60m",
    "trade_status",
    "trade_reason",
    "trade_close",
    "trade_observation_time",
    "trade_staleness_minutes",
    "trade_within_15m",
    "trade_within_60m",
    "previous_trade_used",
)
FAMILY_FIELDS = (
    "family_id",
    "family_id_source",
    "event_ticker",
    "rule",
    "category",
    "timing_structure",
    "target_time",
    "market_existence_at_target",
    "structural_t_minus_1h_failure",
    "eligible_ticker_count",
    "families_requiring_network",
    "any_pre_target_candle",
    "any_midpoint",
    "midpoint_within_15m",
    "midpoint_within_60m",
    "any_trade_close",
    "trade_within_15m",
    "trade_within_60m",
    "market_opened_after_target",
    "no_candle_before_target",
    "missing_bid_or_ask",
    "midpoint_too_stale",
    "no_trade",
    "trade_too_stale",
    "api_data_failure",
)


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for block in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(block)
    return digest.hexdigest()


def _verify_hash(path: Path, expected: str, label: str) -> str:
    actual = _sha256(path)
    if actual != expected:
        raise SmokeValidationError(
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


def _check_prohibited_keys(value: Any, path: str = "response") -> None:
    if isinstance(value, Mapping):
        for key, item in value.items():
            normalized = str(key).casefold()
            if normalized in PROHIBITED_RESPONSE_KEYS:
                raise SmokeValidationError(f"prohibited post-event field at {path}.{key}")
            _check_prohibited_keys(item, f"{path}.{key}")
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _check_prohibited_keys(item, f"{path}[{index}]")


def _load_selected_planner_rows(
    planner_path: Path, smoke_plan_path: Path
) -> tuple[list[dict[str, str]], dict[str, Any], dict[str, int]]:
    smoke_plan = json.loads(smoke_plan_path.read_text(encoding="utf-8"))
    cases = smoke_plan.get("cases")
    if not isinstance(cases, list) or len(cases) != EXPECTED_FAMILY_COUNT:
        raise SmokeValidationError("smoke plan is not the pinned 200-family design")
    identities = {
        (str(case.get("family_id") or ""), str(case.get("family_id_source") or ""))
        for case in cases
    }
    if len(identities) != EXPECTED_FAMILY_COUNT:
        raise SmokeValidationError("smoke plan family identities are not unique")
    rows: list[dict[str, str]] = []
    full_eligible_by_target: Counter[str] = Counter()
    full_family_count = 0
    with planner_path.open("r", encoding="utf-8", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            full_family_count += 1
            count = int(row.get("eligible_market_retrieval_count") or 0)
            if count:
                full_eligible_by_target[str(row.get("target_time") or "")] += count
            identity = (row.get("family_id", ""), row.get("family_id_source", ""))
            if identity in identities:
                rows.append(dict(row))
    if len(rows) != EXPECTED_FAMILY_COUNT or {
        (row["family_id"], row["family_id_source"]) for row in rows
    } != identities:
        raise SmokeValidationError("planner does not exactly match smoke-plan identities")
    full_stats = {
        "family_count": full_family_count,
        "eligible_ticker_count": sum(full_eligible_by_target.values()),
        "distinct_target_count": len(full_eligible_by_target),
        "minimum_batch_request_count": sum(
            math.ceil(count / 100) for count in full_eligible_by_target.values()
        ),
    }
    return (
        sorted(rows, key=lambda row: (row["family_id"], row["family_id_source"])),
        smoke_plan,
        full_stats,
    )


def _attach_eligible_tickers(
    planner_rows: Sequence[Mapping[str, str]], market_metadata_path: Path
) -> list[SmokeFamily]:
    selected = {
        (row["family_id"], row["family_id_source"]): row for row in planner_rows
    }
    eligible: dict[tuple[str, str], list[str]] = defaultdict(list)
    all_seen: dict[tuple[str, str], list[str]] = defaultdict(list)
    with gzip.open(market_metadata_path, "rt", encoding="utf-8", newline="") as handle:
        reader = csv.reader(handle)
        header = next(reader)
        required = {"ticker", "event_ticker", "family_id", "family_id_source", "open_time"}
        if not required.issubset(header):
            raise SmokeValidationError("market projection schema changed")
        index = {name: header.index(name) for name in required}
        for source in reader:
            identity = (source[index["family_id"]], source[index["family_id_source"]])
            row = selected.get(identity)
            if row is None:
                continue
            ticker = source[index["ticker"]]
            if source[index["event_ticker"]] != row["event_ticker"] or not ticker:
                raise SmokeValidationError("selected family-market identity conflict")
            all_seen[identity].append(ticker)
            status = classify_market_open(source[index["open_time"]], row["target_time"])
            if status == EXISTED:
                eligible[identity].append(ticker)
    families: list[SmokeFamily] = []
    ticker_owner: dict[str, tuple[str, str]] = {}
    for identity, row in selected.items():
        planned = set(decode_market_tickers(row["family_id"], row["associated_market_tickers_compact"]))
        observed = all_seen.get(identity, [])
        if len(observed) != len(set(observed)) or set(observed) != planned:
            raise SmokeValidationError(f"market association mismatch for {identity!r}")
        tickers = tuple(sorted(eligible.get(identity, [])))
        if len(tickers) != int(row["eligible_market_retrieval_count"]):
            raise SmokeValidationError(f"eligible ticker count changed for {identity!r}")
        for ticker in tickers:
            previous = ticker_owner.setdefault(ticker, identity)
            if previous != identity:
                raise SmokeValidationError("market ticker belongs to multiple smoke families")
        families.append(
            SmokeFamily(
                family_id=row["family_id"],
                family_id_source=row["family_id_source"],
                event_ticker=row["event_ticker"],
                rule=row["rule"],
                category=row["category"],
                timing_structure=row["timing_structure"],
                target_time=row["target_time"],
                market_existence_at_target=row["market_existence_at_target"],
                eligible_tickers=tickers,
            )
        )
    if sum(len(family.eligible_tickers) for family in families) != EXPECTED_ELIGIBLE_TICKERS:
        raise SmokeValidationError("pinned smoke eligible-ticker count changed")
    return sorted(families, key=lambda family: family.identity)


def _request_metadata(group: RequestGroup) -> dict[str, Any]:
    return {
        "endpoint": BATCH_ENDPOINT,
        "params": group.params,
        "purpose": group.purpose,
        "request_id": group.request_id,
    }


class ImmutableSmokeClient:
    def __init__(
        self,
        *,
        session: Any,
        output_root: Path,
        budget: StorageBudget,
        base_url: str = DEFAULT_BASE_URL,
        max_retries: int = 5,
        timeout_seconds: float = 45,
        requests_per_second: float = 3,
        sleep=time.sleep,
        monotonic=time.monotonic,
        network_forbidden: bool = False,
    ) -> None:
        self.session = session
        self.output_root = output_root
        self.budget = budget
        self.base_url = base_url.rstrip("/")
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self.requests_per_second = requests_per_second
        self.sleep = sleep
        self.monotonic = monotonic
        self.network_forbidden = network_forbidden
        self.last_request_at: float | None = None
        self.physical_requests = 0
        self.retries = 0
        self.rate_limits = 0
        self.resume_hits = 0
        self.uncompressed_response_bytes = 0
        self.compressed_raw_bytes = 0

    def paths(self, group: RequestGroup) -> tuple[Path, Path]:
        raw = self.output_root / "raw" / f"request_{group.request_id}.json.gz"
        commit = self.output_root / "request_commits" / f"request_{group.request_id}.json"
        return raw, commit

    def _load_raw(self, path: Path, group: RequestGroup) -> tuple[dict[str, Any], dict[str, Any]]:
        try:
            wrapper = json.loads(gzip.decompress(path.read_bytes()))
        except Exception as exc:
            raise SmokeValidationError(f"corrupt immutable raw response: {path}") from exc
        if wrapper.get("schema_version") != SCHEMA_VERSION or wrapper.get("request") != _request_metadata(group):
            raise SmokeValidationError("immutable raw request identity mismatch")
        response = wrapper.get("response")
        reject_sensitive_response(response)
        _check_prohibited_keys(response)
        if wrapper.get("response_sha256") != sha256_bytes(canonical_json(response)):
            raise SmokeValidationError("immutable raw response hash mismatch")
        validate_batch_payload(response, group)
        return wrapper, response

    def _commit_record(self, raw: Path, wrapper: Mapping[str, Any], group: RequestGroup) -> dict[str, Any]:
        return {
            "schema_version": SCHEMA_VERSION,
            "request_id": group.request_id,
            "request": _request_metadata(group),
            "raw_path": str(raw.relative_to(self.output_root)),
            "raw_sha256": _sha256(raw),
            "compressed_bytes": raw.stat().st_size,
            "uncompressed_response_bytes": int(wrapper["uncompressed_response_bytes"]),
            "response_sha256": wrapper["response_sha256"],
            "attempts": int(wrapper["attempts"]),
            "retries": int(wrapper["retries"]),
            "rate_limits": int(wrapper["rate_limits"]),
            "complete": True,
        }

    def _validate_commit(self, path: Path, raw: Path, group: RequestGroup) -> dict[str, Any]:
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise SmokeValidationError(f"invalid request commit: {path}") from exc
        wrapper, _ = self._load_raw(raw, group)
        expected = self._commit_record(raw, wrapper, group)
        if record != expected:
            raise SmokeValidationError("request commit does not match immutable raw response")
        return record

    def _rate_limit(self) -> None:
        if self.requests_per_second <= 0:
            raise SmokeValidationError("requests_per_second must be positive")
        now = self.monotonic()
        if self.last_request_at is not None:
            delay = (1 / self.requests_per_second) - (now - self.last_request_at)
            if delay > 0:
                self.sleep(delay)
        self.last_request_at = self.monotonic()

    def fetch(self, group: RequestGroup) -> tuple[dict[str, Any], dict[str, Any]]:
        raw, commit = self.paths(group)
        if commit.exists():
            record = self._validate_commit(commit, raw, group)
            _, response = self._load_raw(raw, group)
            self.resume_hits += 1
            return response, record
        if raw.exists():
            wrapper, response = self._load_raw(raw, group)
            record = self._commit_record(raw, wrapper, group)
            _publish(self.budget, commit, canonical_json(record) + b"\n")
            self.resume_hits += 1
            return response, record
        if self.network_forbidden:
            raise SmokeValidationError(f"no-network resume cache miss: {group.request_id}")

        attempts = rate_limits = 0
        last_error: Exception | None = None
        response_payload: Any = None
        for attempt in range(self.max_retries):
            attempts += 1
            self._rate_limit()
            try:
                self.physical_requests += 1
                response = self.session.get(
                    self.base_url + BATCH_ENDPOINT,
                    params=group.params,
                    timeout=self.timeout_seconds,
                )
                if response.status_code == 429:
                    rate_limits += 1
                    self.rate_limits += 1
                    raise RuntimeError("rate_limited")
                if response.status_code >= 500:
                    raise RuntimeError(f"server_{response.status_code}")
                response.raise_for_status()
                response_payload = response.json()
                reject_sensitive_response(response_payload)
                _check_prohibited_keys(response_payload)
                validate_batch_payload(response_payload, group)
                break
            except Exception as exc:
                last_error = exc
                if attempt + 1 >= self.max_retries:
                    raise SmokeValidationError(
                        f"bounded candlestick request failed: {last_error}"
                    ) from exc
                self.retries += 1
                self.sleep(min(2**attempt, 30))
        if response_payload is None:
            raise SmokeValidationError(f"no response payload: {last_error}")
        response_content = canonical_json(response_payload)
        wrapper = {
            "schema_version": SCHEMA_VERSION,
            "request": _request_metadata(group),
            "response_sha256": sha256_bytes(response_content),
            "uncompressed_response_bytes": len(response_content),
            "attempts": attempts,
            "retries": attempts - 1,
            "rate_limits": rate_limits,
            "response": response_payload,
        }
        raw_content = _gzip(canonical_json(wrapper) + b"\n")
        _publish(self.budget, raw, raw_content)
        record = self._commit_record(raw, wrapper, group)
        _publish(self.budget, commit, canonical_json(record) + b"\n")
        self.uncompressed_response_bytes += len(response_content)
        self.compressed_raw_bytes += len(raw_content)
        return response_payload, record


def _preflight(
    *,
    budget: StorageBudget,
    smoke_plan: Mapping[str, Any],
    families: Sequence[SmokeFamily],
    groups: Sequence[RequestGroup],
) -> dict[str, Any]:
    if len(families) != EXPECTED_FAMILY_COUNT or len(groups) != EXPECTED_LOGICAL_GROUPS:
        raise SmokeValidationError("smoke scope differs from the pinned plan")
    structural = [family for family in families if not family.eligible_tickers]
    if len(structural) != 65 or Counter(family.category for family in structural) != {
        "Crypto": 50,
        "Sports": 15,
    }:
        raise SmokeValidationError("structural late-opening strata changed")
    state = budget.snapshot()
    conservative = int(smoke_plan.get("conservative_additional_bytes") or 0)
    if conservative <= 0:
        raise SmokeValidationError("smoke storage estimate missing")
    budget.check_additional(conservative)
    return {
        "schema_version": SCHEMA_VERSION,
        "network_authorization_scope": "deterministic_200_family_smoke_only",
        "family_count": len(families),
        "structural_late_family_count": len(structural),
        "network_family_count": len(families) - len(structural),
        "eligible_ticker_count": sum(len(family.eligible_tickers) for family in families),
        "logical_request_group_count": len(groups),
        "conservative_additional_bytes": conservative,
        "storage_before": state,
        "projected_namespace_bytes": state["used_bytes"] + conservative,
        "projected_free_bytes": state["free_bytes"] - conservative,
        "passes_namespace_ceiling": state["used_bytes"] + conservative <= state["max_bytes"],
        "passes_free_space_floor": state["free_bytes"] - conservative >= state["min_free_bytes"],
        "network_requests_made": 0,
    }


def _boundary_probe_groups(ticker: str, candle_end: int) -> tuple[RequestGroup, RequestGroup]:
    exact = RequestGroup(
        request_id=request_group_id((ticker,), candle_end, candle_end, "boundary_exact_end"),
        tickers=(ticker,),
        start_ts=candle_end,
        end_ts=candle_end,
        purpose="boundary_exact_end",
    )
    excluded = RequestGroup(
        request_id=request_group_id(
            (ticker,), candle_end - 60, candle_end - 1, "boundary_end_minus_one"
        ),
        tickers=(ticker,),
        start_ts=candle_end - 60,
        end_ts=candle_end - 1,
        purpose="boundary_end_minus_one",
    )
    return exact, excluded


def _validate_empirical_boundary(
    client: ImmutableSmokeClient,
    market_payloads: Mapping[str, list[dict[str, Any]]],
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    source: tuple[str, int] | None = None
    for ticker in sorted(market_payloads):
        candles = market_payloads[ticker]
        if candles:
            source = ticker, int(candles[-1]["end_period_ts"])
            break
    if source is None:
        raise SmokeValidationError("no candle available for empirical boundary probe")
    ticker, candle_end = source
    exact, excluded = _boundary_probe_groups(ticker, candle_end)
    exact_payload, exact_commit = client.fetch(exact)
    excluded_payload, excluded_commit = client.fetch(excluded)
    exact_rows = validate_batch_payload(exact_payload, exact).get(ticker, [])
    excluded_rows = validate_batch_payload(excluded_payload, excluded).get(ticker, [])
    exact_found = any(int(row["end_period_ts"]) == candle_end for row in exact_rows)
    excluded_absent = all(int(row["end_period_ts"]) < candle_end for row in excluded_rows)
    if not exact_found or not excluded_absent:
        raise SmokeValidationError("empirical candle-end boundary validation failed")
    return (
        {
            "documented_semantics": CANDLE_TIMESTAMP_SEMANTICS,
            "official_document_url": OFFICIAL_BATCH_DOC_URL,
            "official_document_sha256": OFFICIAL_BATCH_DOC_SHA256,
            "probe_ticker": ticker,
            "probe_candle_end_ts": candle_end,
            "exact_end_request_returned_candle": exact_found,
            "end_minus_one_excluded_candle": excluded_absent,
            "post_target_candles_accepted": 0,
            "passed": True,
        },
        [exact_commit, excluded_commit],
    )


def _publish_incomplete_report(
    *,
    output_root: Path,
    budget: StorageBudget,
    inputs: Mapping[str, str],
    preflight: Mapping[str, Any],
    groups: Sequence[RequestGroup],
    commits: Sequence[Mapping[str, Any]],
    client: ImmutableSmokeClient,
    market_payloads: Mapping[str, list[dict[str, Any]]],
    reason: str,
) -> dict[str, Any]:
    requested = sum(len(group.tickers) for group in groups)
    report = {
        "schema_version": SCHEMA_VERSION,
        "complete": False,
        "hard_stop": True,
        "hard_stop_reason": reason,
        "inputs": dict(inputs),
        "preflight": dict(preflight),
        "planned_request_groups": len(groups),
        "committed_request_groups": len(commits),
        "physical_api_requests_this_invocation": client.physical_requests,
        "resume_hits_this_invocation": client.resume_hits,
        "retries_this_invocation": client.retries,
        "rate_limits_this_invocation": client.rate_limits,
        "requested_tickers": requested,
        "returned_market_tickers": len(market_payloads),
        "omitted_market_tickers": requested - len(market_payloads),
        "returned_candlesticks": sum(
            len(candles) for candles in market_payloads.values()
        ),
        "compressed_raw_bytes": sum(
            int(commit["compressed_bytes"]) for commit in commits
        ),
        "uncompressed_response_bytes": sum(
            int(commit["uncompressed_response_bytes"]) for commit in commits
        ),
        "candle_boundary_established": False,
        "prices_accepted": 0,
        "normalization_published": False,
        "production_request_projection_status": "blocked_endpoint_routing_unresolved",
        "outcome_fields_accessed": 0,
        "production_acquisition_started": False,
        "storage_at_hard_stop": budget.snapshot(),
        "request_commit_hashes": {
            commit["request_id"]: commit["raw_sha256"]
            for commit in sorted(commits, key=lambda item: item["request_id"])
        },
    }
    content = json.dumps(report, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    path = output_root / "phase_10f_b_incomplete_report.json"
    _publish(budget, path, content)
    return {**report, "report_path": str(path), "report_sha256": _sha256(path)}


def _family_rows(
    families: Sequence[SmokeFamily], contract_rows: Sequence[Mapping[str, Any]]
) -> list[dict[str, Any]]:
    by_identity: dict[tuple[str, str], list[Mapping[str, Any]]] = defaultdict(list)
    for row in contract_rows:
        by_identity[(str(row["family_id"]), str(row["family_id_source"]))].append(row)
    result: list[dict[str, Any]] = []
    for family in families:
        rows = by_identity.get(family.identity, [])
        structural = not family.eligible_tickers
        any_candle = any(int(row.get("candle_count") or 0) > 0 for row in rows)
        any_midpoint = any(row.get("midpoint") is not None for row in rows)
        midpoint_15 = any(bool(row.get("midpoint_within_15m")) for row in rows)
        midpoint_60 = any(bool(row.get("midpoint_within_60m")) for row in rows)
        any_trade = any(row.get("trade_close") is not None for row in rows)
        trade_15 = any(bool(row.get("trade_within_15m")) for row in rows)
        trade_60 = any(bool(row.get("trade_within_60m")) for row in rows)
        result.append(
            {
                "family_id": family.family_id,
                "family_id_source": family.family_id_source,
                "event_ticker": family.event_ticker,
                "rule": family.rule,
                "category": family.category,
                "timing_structure": family.timing_structure,
                "target_time": family.target_time,
                "market_existence_at_target": family.market_existence_at_target,
                "structural_t_minus_1h_failure": structural,
                "eligible_ticker_count": len(family.eligible_tickers),
                "families_requiring_network": not structural,
                "any_pre_target_candle": any_candle,
                "any_midpoint": any_midpoint,
                "midpoint_within_15m": midpoint_15,
                "midpoint_within_60m": midpoint_60,
                "any_trade_close": any_trade,
                "trade_within_15m": trade_15,
                "trade_within_60m": trade_60,
                "market_opened_after_target": structural,
                "no_candle_before_target": bool(not structural and not any_candle),
                "missing_bid_or_ask": bool(any_candle and not any_midpoint),
                "midpoint_too_stale": bool(any_midpoint and not midpoint_15),
                "no_trade": bool(not structural and not any_trade),
                "trade_too_stale": bool(any_trade and not trade_60),
                "api_data_failure": bool(any(row.get("api_data_failure") for row in rows)),
            }
        )
    return result


def _aggregate_family_rows(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    boolean_fields = (
        "structural_t_minus_1h_failure",
        "families_requiring_network",
        "any_pre_target_candle",
        "any_midpoint",
        "midpoint_within_15m",
        "midpoint_within_60m",
        "any_trade_close",
        "trade_within_15m",
        "trade_within_60m",
        "market_opened_after_target",
        "no_candle_before_target",
        "missing_bid_or_ask",
        "midpoint_too_stale",
        "no_trade",
        "trade_too_stale",
        "api_data_failure",
    )
    return {
        "planned_families": len(rows),
        "eligible_tickers": sum(int(row["eligible_ticker_count"]) for row in rows),
        **{field: sum(bool(row[field]) for row in rows) for field in boolean_fields},
    }


def _breakdowns(rows: Sequence[Mapping[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for field in ("rule", "category"):
        groups: dict[str, list[Mapping[str, Any]]] = defaultdict(list)
        for row in rows:
            groups[str(row[field])].append(row)
        result[f"by_{field}"] = {
            key: _aggregate_family_rows(group) for key, group in sorted(groups.items())
        }
    return result


def _build_contract_rows(
    families: Sequence[SmokeFamily],
    payloads: Mapping[str, list[dict[str, Any]]],
    ticker_request_ids: Mapping[str, str],
) -> list[dict[str, Any]]:
    rows: list[dict[str, Any]] = []
    for family in families:
        for ticker in family.eligible_tickers:
            candles = payloads.get(ticker)
            failure = candles is None
            observation = extract_contract_observation(
                ticker=ticker, candles=candles or [], target_ts=family.target_ts
            )
            if failure:
                observation["midpoint_reason"] = "api_data_failure"
                observation["trade_reason"] = "api_data_failure"
            rows.append(
                {
                    "family_id": family.family_id,
                    "family_id_source": family.family_id_source,
                    "event_ticker": family.event_ticker,
                    "rule": family.rule,
                    "category": family.category,
                    "timing_structure": family.timing_structure,
                    "request_id": ticker_request_ids[ticker],
                    "api_data_failure": failure,
                    **observation,
                }
            )
    return rows


def _validate_no_network_resume(
    groups: Sequence[RequestGroup], output_root: Path, budget: StorageBudget
) -> dict[str, Any]:
    client = ImmutableSmokeClient(
        session=None,
        output_root=output_root,
        budget=budget,
        network_forbidden=True,
        sleep=lambda _: None,
    )
    for group in groups:
        client.fetch(group)
    return {
        "groups_expected": len(groups),
        "groups_reused": client.resume_hits,
        "network_requests": client.physical_requests,
        "passed": client.resume_hits == len(groups) and client.physical_requests == 0,
    }


def _existing_final(output_root: Path, budget: StorageBudget) -> dict[str, Any] | None:
    commit_path = output_root / "phase_10f_b_smoke_commit.json"
    if not commit_path.exists():
        return None
    commit = json.loads(commit_path.read_text(encoding="utf-8"))
    if commit.get("schema_version") != SCHEMA_VERSION or not commit.get("complete"):
        raise SmokeValidationError("existing final smoke commit is invalid")
    for item in commit.get("artifacts", []):
        path = output_root / item["path"]
        if not path.is_file() or _sha256(path) != item["sha256"]:
            raise SmokeValidationError("existing final smoke artifact hash mismatch")
    report = json.loads((output_root / "phase_10f_b_smoke_report.json").read_text())
    return {**report, "storage_now": budget.snapshot(), "existing_final_reused": True}


def run(args: argparse.Namespace, *, session: Any | None = None) -> dict[str, Any]:
    planner_hash = _verify_hash(args.planner, args.expected_planner_sha256, "planner")
    smoke_plan_hash = _verify_hash(args.smoke_plan, args.expected_smoke_plan_sha256, "smoke plan")
    market_hash = _verify_hash(
        args.market_metadata, args.expected_market_metadata_sha256, "market metadata"
    )
    rules = load_study_rules(args.config)
    if rules.fingerprint != args.expected_study_rules_fingerprint:
        raise SmokeValidationError("frozen StudyRules fingerprint changed")
    planner_rows, smoke_plan, full_planner = _load_selected_planner_rows(
        args.planner, args.smoke_plan
    )
    families = _attach_eligible_tickers(planner_rows, args.market_metadata)
    groups = build_request_groups(families, batch_size=100)
    budget = StorageBudget(
        args.guard_root,
        max_bytes=args.max_generated_bytes,
        min_free_bytes=args.min_free_bytes,
    )
    preflight = _preflight(
        budget=budget, smoke_plan=smoke_plan, families=families, groups=groups
    )
    if args.preflight_only:
        print(json.dumps(preflight, sort_keys=True))
        return preflight
    existing = _existing_final(args.output_root, budget)
    if existing is not None:
        print(json.dumps(existing, sort_keys=True))
        return existing

    http = session or requests.Session()
    client = ImmutableSmokeClient(
        session=http,
        output_root=args.output_root,
        budget=budget,
        base_url=args.base_url,
        max_retries=args.max_retries,
        timeout_seconds=args.timeout_seconds,
        requests_per_second=args.requests_per_second,
        network_forbidden=args.no_network_resume,
    )
    market_payloads: dict[str, list[dict[str, Any]]] = {}
    ticker_request_ids: dict[str, str] = {}
    commits: list[dict[str, Any]] = []
    for group in groups:
        payload, commit = client.fetch(group)
        found = validate_batch_payload(payload, group)
        for ticker, candles in found.items():
            market_payloads[ticker] = candles
        for ticker in group.tickers:
            ticker_request_ids[ticker] = group.request_id
        commits.append(commit)

    main_commits = list(commits)

    if not any(market_payloads.values()):
        incomplete = _publish_incomplete_report(
            output_root=args.output_root,
            budget=budget,
            inputs={
                "planner_sha256": planner_hash,
                "smoke_plan_sha256": smoke_plan_hash,
                "market_metadata_sha256": market_hash,
                "study_rules_fingerprint": rules.fingerprint,
            },
            preflight=preflight,
            groups=groups,
            commits=commits,
            client=client,
            market_payloads=market_payloads,
            reason=(
                "live batch candlestick endpoint returned zero market objects and "
                "zero candles for all requested archived-market ticker groups"
            ),
        )
        print(json.dumps(incomplete, sort_keys=True))
        raise SmokeValidationError(
            "candle endpoint routing unresolved; incomplete report published"
        )
    boundary, boundary_commits = _validate_empirical_boundary(client, market_payloads)
    commits.extend(boundary_commits)
    all_groups = [*groups]
    for commit in boundary_commits:
        request = commit["request"]
        params = request["params"]
        all_groups.append(
            RequestGroup(
                request_id=commit["request_id"],
                tickers=tuple(params["market_tickers"].split(",")),
                start_ts=int(params["start_ts"]),
                end_ts=int(params["end_ts"]),
                purpose=request["purpose"],
            )
        )
    resume = _validate_no_network_resume(all_groups, args.output_root, budget)
    if not resume["passed"]:
        raise SmokeValidationError("deterministic no-network resume failed")

    contract_rows = _build_contract_rows(
        families, market_payloads, ticker_request_ids
    )
    family_rows = _family_rows(families, contract_rows)
    if len(contract_rows) != EXPECTED_ELIGIBLE_TICKERS or len(family_rows) != EXPECTED_FAMILY_COUNT:
        raise SmokeValidationError("normalized smoke output count mismatch")

    contract_content = _gzip_csv(contract_rows, CONTRACT_FIELDS)
    family_content = _gzip_csv(family_rows, FAMILY_FIELDS)
    manifest_content = b"".join(
        canonical_json(commit) + b"\n"
        for commit in sorted(commits, key=lambda item: item["request_id"])
    )
    inputs = {
        "planner_sha256": planner_hash,
        "smoke_plan_sha256": smoke_plan_hash,
        "market_metadata_sha256": market_hash,
        "study_rules_fingerprint": rules.fingerprint,
    }
    provenance = {
        "schema_version": SCHEMA_VERSION,
        "inputs": inputs,
        "candle_timestamp_semantics": boundary,
        "price_definitions": {
            "primary": "closing_yes_bid_ask_midpoint_no_fallback",
            "robustness": "actual_trade_close_no_fallback",
            "previous_trade_used": False,
            "primary_staleness_minutes": 15,
            "robustness_staleness_minutes": 60,
        },
        "outcome_fields_accessed": 0,
        "outcomes_merged": False,
        "study_rules_changed": False,
        "production_acquisition_started": False,
    }
    provenance_content = canonical_json(provenance) + b"\n"
    artifacts = {
        "phase_10f_b_contract_prices.csv.gz": contract_content,
        "phase_10f_b_family_summary.csv.gz": family_content,
        "phase_10f_b_request_manifest.jsonl": manifest_content,
        "phase_10f_b_provenance.json": provenance_content,
    }
    for name, content in artifacts.items():
        _publish(budget, args.output_root / name, content)

    normalized_bytes = len(contract_content) + len(family_content)
    spread = spread_diagnostics(contract_rows)
    raw_bytes = sum(int(commit["compressed_bytes"]) for commit in commits)
    uncompressed_bytes = sum(int(commit["uncompressed_response_bytes"]) for commit in commits)
    main_raw_bytes = sum(int(commit["compressed_bytes"]) for commit in main_commits)
    main_uncompressed_bytes = sum(
        int(commit["uncompressed_response_bytes"]) for commit in main_commits
    )
    manifest_bytes_per_group = len(manifest_content) / len(commits)
    raw_bytes_per_ticker = main_raw_bytes / len(contract_rows)
    contract_bytes_per_ticker = len(contract_content) / len(contract_rows)
    family_bytes_per_family = len(family_content) / len(family_rows)
    projected_raw = math.ceil(
        raw_bytes_per_ticker * full_planner["eligible_ticker_count"]
    )
    projected_normalized = math.ceil(
        contract_bytes_per_ticker * full_planner["eligible_ticker_count"]
        + family_bytes_per_family * full_planner["family_count"]
    )
    projected_manifest = math.ceil(
        manifest_bytes_per_group * full_planner["minimum_batch_request_count"]
    )
    projected_total = projected_raw + projected_normalized + projected_manifest
    live_storage = budget.snapshot()
    api_omissions = len(contract_rows) - len(market_payloads)
    production_projection = {
        "full_planner_scope": full_planner,
        "revised_minimum_logical_requests": full_planner["minimum_batch_request_count"],
        "request_projection_status": (
            "batch_route_supported_by_complete_smoke_coverage"
            if api_omissions == 0
            else "unresolved_due_to_batch_endpoint_omissions"
        ),
        "projected_compressed_raw_bytes": projected_raw,
        "projected_normalized_bytes": projected_normalized,
        "projected_manifest_bytes": projected_manifest,
        "projected_total_additional_namespace_bytes": projected_total,
        "fits_current_namespace_ceiling": (
            live_storage["used_bytes"] + projected_total <= live_storage["max_bytes"]
        ),
        "fits_current_free_space_floor": (
            live_storage["free_bytes"] - projected_total >= live_storage["min_free_bytes"]
        ),
        "archive_or_new_storage_approval_required": True,
        "archive_plan": (
            "Preserve hashes and immutable commits; copy Phase 10B/10C raw and "
            "partition artifacts to verified cold storage; test byte-identical "
            "restore; delete no local validated artifact without separate approval."
        ),
    }
    network_family_count = sum(bool(family.eligible_tickers) for family in families)
    measurement_rates = {
        "compressed_raw_bytes_per_main_request_group": main_raw_bytes / len(main_commits),
        "uncompressed_response_bytes_per_main_request_group": main_uncompressed_bytes
        / len(main_commits),
        "compressed_raw_bytes_per_network_family": main_raw_bytes / network_family_count,
        "compressed_raw_bytes_per_requested_ticker": raw_bytes_per_ticker,
        "normalized_bytes_per_requested_ticker": contract_bytes_per_ticker,
    }
    midpoint_15_rate = sum(bool(row["midpoint_within_15m"]) for row in family_rows) / network_family_count
    trade_15_rate = sum(bool(row["trade_within_15m"]) for row in family_rows) / network_family_count
    price_recommendation = {
        "recommendation": "retain_midpoint_as_primary_for_next_approval_gate",
        "permanently_frozen": False,
        "reason": (
            "The midpoint remains a non-mixing contemporaneous quote measure; "
            "coverage and spread diagnostics must be assessed against the separately "
            "reported trade-close robustness sample before production approval."
        ),
        "network_family_midpoint_15m_coverage_rate": midpoint_15_rate,
        "network_family_trade_15m_coverage_rate": trade_15_rate,
    }
    report: dict[str, Any] = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "scope": "authorized_deterministic_200_family_smoke",
        "inputs": inputs,
        "preflight": preflight,
        "planned_families": len(families),
        "structural_t_minus_1h_failures": sum(not family.eligible_tickers for family in families),
        "families_requiring_network": sum(bool(family.eligible_tickers) for family in families),
        "contracts_requested": len(contract_rows),
        "planned_logical_request_groups": len(groups),
        "boundary_probe_request_groups": len(boundary_commits),
        "total_committed_request_groups": len(commits),
        "physical_api_requests": client.physical_requests,
        "retries": client.retries,
        "rate_limits": client.rate_limits,
        "resume_hits_during_acquisition": client.resume_hits,
        "compressed_raw_bytes": raw_bytes,
        "uncompressed_response_bytes": uncompressed_bytes,
        "normalized_output_bytes": normalized_bytes,
        "measured_storage_rates": measurement_rates,
        "production_projection": production_projection,
        "price_definition_recommendation": price_recommendation,
        "family_metrics": _aggregate_family_rows(family_rows),
        **_breakdowns(family_rows),
        "spread_diagnostics_all_midpoint_observations": spread,
        "candle_boundary_validation": boundary,
        "deterministic_no_network_resume": resume,
        "api_returned_tickers": len(market_payloads),
        "api_omitted_tickers": api_omissions,
        "outcome_fields_accessed": 0,
        "production_acquisition_started": False,
        "storage_at_report": budget.snapshot(),
    }
    if report["api_omitted_tickers"]:
        report["api_data_warning"] = "batch endpoint omitted requested market tickers"
    report_content = json.dumps(report, indent=2, sort_keys=True).encode("utf-8") + b"\n"
    _publish(budget, args.output_root / "phase_10f_b_smoke_report.json", report_content)
    artifact_refs = []
    for name in sorted((*artifacts, "phase_10f_b_smoke_report.json")):
        path = args.output_root / name
        artifact_refs.append(
            {"path": name, "sha256": _sha256(path), "bytes": path.stat().st_size}
        )
    final_commit = {
        "schema_version": SCHEMA_VERSION,
        "complete": True,
        "inputs": inputs,
        "request_commit_count": len(commits),
        "request_manifest_sha256": _sha256(args.output_root / "phase_10f_b_request_manifest.jsonl"),
        "artifacts": artifact_refs,
    }
    final_content = canonical_json(final_commit) + b"\n"
    _publish(budget, args.output_root / "phase_10f_b_smoke_commit.json", final_content)
    final = {
        **report,
        "final_commit_sha256": _sha256(args.output_root / "phase_10f_b_smoke_commit.json"),
        "storage_now": budget.snapshot(),
        "output_hashes": {item["path"]: item["sha256"] for item in artifact_refs},
    }
    print(json.dumps(final, sort_keys=True))
    return final


def parse_args(argv: Sequence[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--planner", required=True, type=Path)
    parser.add_argument("--smoke-plan", required=True, type=Path)
    parser.add_argument("--market-metadata", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--guard-root", required=True, type=Path)
    parser.add_argument("--config", required=True, type=Path)
    parser.add_argument("--expected-planner-sha256", required=True)
    parser.add_argument("--expected-smoke-plan-sha256", required=True)
    parser.add_argument("--expected-market-metadata-sha256", required=True)
    parser.add_argument("--expected-study-rules-fingerprint", required=True)
    parser.add_argument("--base-url", default=DEFAULT_BASE_URL)
    parser.add_argument("--max-retries", type=int, default=5)
    parser.add_argument("--timeout-seconds", type=float, default=45)
    parser.add_argument("--requests-per-second", type=float, default=3)
    parser.add_argument("--max-generated-bytes", type=int, default=DEFAULT_MAX_BYTES)
    parser.add_argument("--min-free-bytes", type=int, default=DEFAULT_MIN_FREE_BYTES)
    parser.add_argument("--preflight-only", action="store_true")
    parser.add_argument(
        "--no-network-resume",
        action="store_true",
        help="require every request group to be satisfied by a valid immutable commit",
    )
    return parser.parse_args(argv)


def main(argv: Sequence[str] | None = None) -> int:
    try:
        run(parse_args(argv))
        return 0
    except (CacheError, OSError, ValueError, SmokeValidationError) as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
