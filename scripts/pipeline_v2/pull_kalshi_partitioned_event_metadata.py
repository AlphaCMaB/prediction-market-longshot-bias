"""Disk-bounded production acquisition for Kalshi event metadata.

The source universe is streamed from the compressed Phase 10B artifact.  A
partition commit is the only completion boundary; immutable gzip pages written
before an interruption are cache evidence and are reused on resume.
"""

from __future__ import annotations

import argparse
import csv
import gzip
import hashlib
import io
import json
import math
import os
from pathlib import Path
import shutil
import sys
import tempfile
import time
import tomllib
from typing import Any, Iterable, Mapping, Sequence
import zlib

from scripts.common.io_utils import open_csv_dict_reader
from scripts.pipeline_v2.kalshi_event_metadata_client import (
    EVENTS_ENDPOINT,
    MILESTONES_ENDPOINT,
    EventMetadataRequestFailure,
    KalshiEventMetadataClient,
)
from scripts.pipeline_v2.kalshi_metadata_cache import (
    CacheError,
    ResourceLimitError,
    SensitiveResponseError,
    StorageBudget,
    canonical_json,
    publish_immutable_bytes,
    reject_sensitive_response,
    sha256_json,
)
from scripts.pipeline_v2.pull_kalshi_event_metadata import (
    EVENT_METADATA_FIELDS,
    MILESTONE_FIELDS,
    EventAcquisitionError,
    collect_milestones,
    make_batches,
    normalize_event,
    request_parameters,
    research_event_projection,
    research_milestone_projection,
    validate_event_ticker,
)


SCHEMA_VERSION = 1
DEFAULT_CONFIG = Path("configs/pipeline_v2.toml")
DEFAULT_RAW_ROOT = Path("data/pipeline_v2/market_acquisition/partitioned")
EVENT_NAMESPACE = "event_metadata_acquisition"
DEFAULT_PARTITION_EVENTS = 5_000
DEFAULT_MAX_PAGES_PER_BATCH = 10
DEFAULT_MAX_RAW_BYTES = 5 * 1024**3
DEFAULT_MIN_FREE_BYTES = 80 * 1024**3
DEFAULT_RAW_ESTIMATE_PER_EVENT = 1_536
DEFAULT_NORMALIZED_ESTIMATE_PER_EVENT = 512


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024**2), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _gzip_bytes(content: bytes) -> bytes:
    return gzip.compress(content, compresslevel=9, mtime=0)


def _gzip_jsonl(rows: Iterable[Mapping[str, Any]]) -> bytes:
    return _gzip_bytes(b"".join(canonical_json(dict(row)) + b"\n" for row in rows))


def _read_gzip_jsonl(path: Path) -> Iterable[dict[str, Any]]:
    try:
        with gzip.open(path, "rt", encoding="utf-8") as handle:
            for number, line in enumerate(handle, 1):
                if not line.strip():
                    continue
                value = json.loads(line)
                if not isinstance(value, dict):
                    raise CacheError(f"{path}:{number} is not a JSON object")
                yield value
    except (OSError, UnicodeError, json.JSONDecodeError) as exc:
        raise CacheError(f"invalid gzip JSONL artifact: {path}") from exc


def _load_settings(path: Path) -> dict[str, Any]:
    with path.open("rb") as handle:
        section = tomllib.load(handle).get("kalshi_event_metadata", {})
    values = {
        "page_size": int(section.get("page_size", 200)),
        "batch_size": int(section.get("batch_size", 200)),
        "partition_events": int(
            section.get("partition_events", DEFAULT_PARTITION_EVENTS)
        ),
        "max_pages_per_batch": int(
            section.get("max_pages_per_batch", DEFAULT_MAX_PAGES_PER_BATCH)
        ),
        "max_retries": int(section.get("max_retries", 5)),
        "backoff_base_seconds": float(section.get("backoff_base_seconds", 1.0)),
        "backoff_cap_seconds": float(section.get("backoff_cap_seconds", 30.0)),
        "timeout_seconds": float(section.get("timeout_seconds", 45.0)),
        "requests_per_second": float(section.get("requests_per_second", 3.0)),
        "estimated_compressed_raw_bytes_per_event": int(
            section.get(
                "estimated_compressed_raw_bytes_per_event",
                DEFAULT_RAW_ESTIMATE_PER_EVENT,
            )
        ),
        "estimated_compressed_normalized_bytes_per_event": int(
            section.get(
                "estimated_compressed_normalized_bytes_per_event",
                DEFAULT_NORMALIZED_ESTIMATE_PER_EVENT,
            )
        ),
        "estimated_single_event_fallback_fraction": float(
            section.get("estimated_single_event_fallback_fraction", 0.03)
        ),
        "estimated_fallback_raw_bytes_per_event": int(
            section.get("estimated_fallback_raw_bytes_per_event", 4096)
        ),
        "estimated_milestone_pages_per_fallback": int(
            section.get("estimated_milestone_pages_per_fallback", 1)
        ),
        "max_raw_bytes": int(section.get("max_raw_bytes", DEFAULT_MAX_RAW_BYTES)),
        "min_free_bytes": int(section.get("min_free_bytes", DEFAULT_MIN_FREE_BYTES)),
    }
    if not 1 <= values["page_size"] <= 200 or not 1 <= values["batch_size"] <= 200:
        raise ValueError("event page and batch sizes must be between 1 and 200")
    positive = (
        "partition_events",
        "max_pages_per_batch",
        "estimated_compressed_raw_bytes_per_event",
        "estimated_compressed_normalized_bytes_per_event",
        "estimated_fallback_raw_bytes_per_event",
        "estimated_milestone_pages_per_fallback",
        "max_raw_bytes",
    )
    if any(values[key] <= 0 for key in positive) or values["min_free_bytes"] < 0:
        raise ValueError("event partition and storage settings are invalid")
    if values["partition_events"] % values["batch_size"]:
        raise ValueError("partition_events must be an exact multiple of batch_size")
    if values["requests_per_second"] <= 0:
        raise ValueError("requests_per_second must be positive")
    if not 0 <= values["estimated_single_event_fallback_fraction"] <= 1:
        raise ValueError("estimated_single_event_fallback_fraction must be in [0, 1]")
    return values


def scan_event_ticker_universe(
    path: str | Path,
    *,
    expected_sha256: str | None = None,
    expected_merge_id: str | None = None,
) -> dict[str, Any]:
    """Validate and count a sorted universe while retaining no row collection."""
    source = Path(path)
    source_hash = _sha256_file(source)
    if expected_sha256 and source_hash != expected_sha256:
        raise CacheError(
            "event-ticker artifact SHA-256 does not match the pinned value"
        )
    if expected_merge_id and source.parent.name != expected_merge_id:
        raise CacheError("event-ticker artifact is not under the pinned merge identity")
    report_path = source.parent / "merge_report.json"
    merge_report: dict[str, Any] | None = None
    if report_path.is_file():
        merge_report = json.loads(report_path.read_text(encoding="utf-8"))
        if expected_merge_id and merge_report.get("merge_id") != expected_merge_id:
            raise CacheError("Phase 10B merge report identity mismatch")
        references = [
            item
            for item in merge_report.get("artifacts", [])
            if item.get("kind") == source.name
        ]
        if len(references) != 1 or references[0].get("sha256") != source_hash:
            raise CacheError(
                "Phase 10B merge report does not authenticate event tickers"
            )

    total = valid = malformed = duplicate_rows = duplicate_tickers = 0
    malformed_examples: list[dict[str, Any]] = []
    previous: str | None = None
    previous_was_duplicate = False
    sorted_input = True
    with open_csv_dict_reader(source) as (reader, header):
        if "event_ticker" not in header:
            raise ValueError("event ticker input requires event_ticker column")
        for row_number, row in enumerate(reader, 2):
            total += 1
            value = row.get("event_ticker")
            try:
                ticker = validate_event_ticker(value)
            except ValueError as exc:
                malformed += 1
                if len(malformed_examples) < 20:
                    malformed_examples.append(
                        {"row": row_number, "value": value, "error": str(exc)}
                    )
                continue
            valid += 1
            if previous is not None and ticker < previous:
                sorted_input = False
            if ticker == previous:
                duplicate_rows += 1
                if not previous_was_duplicate:
                    duplicate_tickers += 1
                previous_was_duplicate = True
            else:
                previous_was_duplicate = False
            previous = ticker
    unique = valid - duplicate_rows if sorted_input else None
    if merge_report is not None and merge_report.get("event_count") != unique:
        raise CacheError("Phase 10B event count differs from streamed input audit")
    return {
        "source_path": str(source),
        "source_bytes": source.stat().st_size,
        "source_sha256": source_hash,
        "merge_id": source.parent.name,
        "header": list(header),
        "total_events": total,
        "valid_events": valid,
        "unique_events": unique,
        "malformed_tickers": malformed,
        "malformed_examples": malformed_examples,
        "duplicate_rows": duplicate_rows,
        "duplicate_tickers": duplicate_tickers,
        "sorted_input": sorted_input,
        "gzip_input": source.suffix.casefold() == ".gz",
    }


def _iter_unique_tickers(path: Path) -> Iterable[str]:
    previous: str | None = None
    with open_csv_dict_reader(path) as (reader, header):
        if "event_ticker" not in header:
            raise ValueError("event ticker input requires event_ticker column")
        for row_number, row in enumerate(reader, 2):
            try:
                ticker = validate_event_ticker(row.get("event_ticker"))
            except ValueError as exc:
                raise ValueError(
                    f"invalid event_ticker at CSV row {row_number}: {exc}"
                ) from exc
            if previous is not None and ticker < previous:
                raise ValueError(
                    "event ticker input must be sorted for streaming acquisition"
                )
            if ticker != previous:
                yield ticker
            previous = ticker


def _ticker_slice(path: Path, offset: int, count: int) -> tuple[str, ...]:
    selected: list[str] = []
    for index, ticker in enumerate(_iter_unique_tickers(path)):
        if index < offset:
            continue
        if len(selected) == count:
            break
        selected.append(ticker)
    return tuple(selected)


def _scope_definition(
    audit: Mapping[str, Any], settings: Mapping[str, Any], limit_events: int | None
) -> dict[str, Any]:
    available = int(audit["unique_events"])
    selected = min(available, limit_events) if limit_events is not None else available
    return {
        "schema_version": SCHEMA_VERSION,
        "source_sha256": audit["source_sha256"],
        "source_merge_id": audit["merge_id"],
        "available_event_count": available,
        "selected_event_count": selected,
        "requested_limit": limit_events,
        "batch_size": settings["batch_size"],
        "page_size": settings["page_size"],
        "partition_events": settings["partition_events"],
        "max_pages_per_batch": settings["max_pages_per_batch"],
        "endpoint_path": EVENTS_ENDPOINT,
        "with_nested_markets": False,
        "with_milestones": True,
        "missing_event_fallback": "single_event_plus_related_milestones_v1",
        "milestone_page_size": 500,
    }


def _scope_id(definition: Mapping[str, Any]) -> str:
    return sha256_json(definition)[:24]


def _partition_id(
    scope_id: str, index: int, offset: int, tickers: Sequence[str]
) -> str:
    return sha256_json(
        {
            "scope_id": scope_id,
            "partition_index": index,
            "event_offset": offset,
            "event_count": len(tickers),
            "ticker_sha256": sha256_json(list(tickers)),
        }
    )[:24]


def _artifact_reference(kind: str, path: Path) -> dict[str, Any]:
    return {
        "kind": kind,
        "path": str(path),
        "sha256": _sha256_file(path),
        "bytes": path.stat().st_size,
        "compression": "gzip" if path.suffix == ".gz" else "none",
    }


def _milestone_conflict_projection(milestone: Mapping[str, Any]) -> dict[str, Any]:
    """Ignore source freshness markers while preserving all research evidence."""
    projected = research_milestone_projection(milestone)
    return {
        key: value
        for key, value in projected.items()
        if key not in {"last_updated_ts", "updated_time"}
    }


def _milestone_row_conflict_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    return {
        key: value for key, value in row.items() if key != "milestone_last_updated_ts"
    }


def _publish_budgeted(budget: StorageBudget, path: Path, content: bytes) -> None:
    budget.check_publication(path, content)
    publish_immutable_bytes(path, content)


def _load_raw_page(path: Path, request: Mapping[str, Any]) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        wrapper = json.loads(gzip.decompress(path.read_bytes()))
    except Exception as exc:
        raise CacheError(f"corrupt event gzip page: {path}") from exc
    if wrapper.get("request") != request:
        raise CacheError(f"event cache request mismatch: {path}")
    response = wrapper.get("response")
    reject_sensitive_response(response)
    if wrapper.get("response_sha256") != sha256_json(response):
        raise CacheError(f"event cache response hash mismatch: {path}")
    return wrapper


def _publish_raw_page(
    budget: StorageBudget,
    path: Path,
    request: Mapping[str, Any],
    response: Mapping[str, Any],
    acquisition: Mapping[str, Any],
) -> dict[str, Any]:
    reject_sensitive_response(response)
    wrapper = {
        "schema_version": SCHEMA_VERSION,
        "compression": "gzip",
        "request": dict(request),
        "acquisition": dict(acquisition),
        "response_sha256": sha256_json(response),
        "response": dict(response),
    }
    content = _gzip_bytes(canonical_json(wrapper) + b"\n")
    _publish_budgeted(budget, path, content)
    return wrapper


def _valid_partition_commit(
    path: Path,
    *,
    expected_scope_id: str | None = None,
    validated: set[Path] | None = None,
) -> bool:
    if validated is not None and path in validated:
        return path.is_file()
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
        if record.get("schema_version") != SCHEMA_VERSION or not record.get(
            "partition_complete"
        ):
            return False
        if expected_scope_id and record.get("scope_id") != expected_scope_id:
            return False
        if (
            record.get("partition_id")
            != sha256_json(
                {
                    "scope_id": record["scope_id"],
                    "partition_index": record["partition_index"],
                    "event_offset": record["event_offset"],
                    "event_count": record["requested_event_count"],
                    "ticker_sha256": record["ticker_sha256"],
                }
            )[:24]
        ):
            return False
        pages = record.get("source_pages")
        artifacts = record.get("artifacts")
        if not isinstance(pages, list) or not isinstance(artifacts, list):
            return False
        required = {
            "event_metadata",
            "event_milestones",
            "event_provenance",
            "request_manifest",
            "normalization_report",
        }
        if {item.get("kind") for item in artifacts} != required:
            return False
        for item in artifacts:
            artifact = Path(item["path"])
            if not artifact.is_file() or artifact.stat().st_size != item["bytes"]:
                return False
            if _sha256_file(artifact) != item["sha256"]:
                return False
            if item["compression"] == "gzip":
                with gzip.open(artifact, "rb") as handle:
                    while handle.read(1024**2):
                        pass
        pages_by_chain: dict[
            tuple[str, int, str], list[tuple[dict[str, Any], dict[str, Any]]]
        ] = {}
        for page in pages:
            raw = Path(page["path"])
            if not raw.is_file() or _sha256_file(raw) != page["page_file_sha256"]:
                return False
            if raw.stat().st_size != page["compressed_bytes"]:
                return False
            wrapper = json.loads(gzip.decompress(raw.read_bytes()))
            if wrapper.get("compression") != "gzip":
                return False
            acquisition = wrapper.get("acquisition")
            if not isinstance(acquisition, dict) or any(
                int(acquisition.get(key, -1)) < 0
                for key in ("http_attempt_count", "retry_count", "rate_limit_count")
            ):
                return False
            request = wrapper.get("request")
            if (
                not isinstance(request, dict)
                or sha256_json(request) != page["request_identity"]
            ):
                return False
            if wrapper.get("response_sha256") != page["response_sha256"]:
                return False
            if sha256_json(wrapper.get("response")) != page["response_sha256"]:
                return False
            if bool(page["terminal_page"]) != (
                not bool(wrapper["response"].get("cursor"))
            ):
                return False
            chain_key = (
                str(page.get("request_kind") or ""),
                int(page["batch_number"]),
                str(page.get("fallback_ticker") or ""),
            )
            pages_by_chain.setdefault(chain_key, []).append((page, wrapper))
        for batch_pages in pages_by_chain.values():
            expected_cursor: str | None = None
            for expected_page, (page, wrapper) in enumerate(
                sorted(batch_pages, key=lambda item: int(item[0]["page_number"])), 1
            ):
                if int(page["page_number"]) != expected_page:
                    return False
                params = wrapper["request"].get("params")
                if not isinstance(params, dict):
                    return False
                if params.get("cursor") != expected_cursor and not (
                    expected_cursor is None and "cursor" not in params
                ):
                    return False
                if page["request_cursor_hash"] != sha256_json(expected_cursor or ""):
                    return False
                expected_cursor = str(wrapper["response"].get("cursor") or "") or None
                if page["response_cursor_hash"] != sha256_json(expected_cursor or ""):
                    return False
            if expected_cursor is not None:
                return False
        if validated is not None:
            validated.add(path)
        return True
    except (OSError, KeyError, TypeError, ValueError, json.JSONDecodeError):
        return False


def load_partition_chain(
    raw_root: Path,
    scope_id: str,
    *,
    validated: set[Path] | None = None,
) -> list[dict[str, Any]]:
    commit_dir = raw_root / EVENT_NAMESPACE / "partition_commits" / scope_id
    chain: list[dict[str, Any]] = []
    expected_offset = 0
    for index, path in enumerate(sorted(commit_dir.glob("partition_*.json"))):
        if not _valid_partition_commit(
            path, expected_scope_id=scope_id, validated=validated
        ):
            raise CacheError(f"invalid event partition commit: {path}")
        record = json.loads(path.read_text(encoding="utf-8"))
        if record.get("partition_index") != index:
            raise CacheError("event partition indices are not contiguous")
        if record.get("event_offset") != expected_offset:
            raise CacheError("event partition offsets are not contiguous")
        expected_offset += int(record.get("requested_event_count", -1))
        record["_commit_path"] = str(path)
        chain.append(record)
    return chain


def build_preflight(
    *,
    event_tickers_path: Path,
    raw_root: Path,
    settings: Mapping[str, Any],
    limit_events: int | None,
    expected_sha256: str | None,
    expected_merge_id: str | None,
    max_raw_bytes: int,
    min_free_bytes: int,
) -> tuple[dict[str, Any], dict[str, Any], dict[str, Any]]:
    if limit_events is not None and limit_events < 0:
        raise ValueError("--limit-events must be nonnegative")
    audit = scan_event_ticker_universe(
        event_tickers_path,
        expected_sha256=expected_sha256,
        expected_merge_id=expected_merge_id,
    )
    definition = _scope_definition(audit, settings, limit_events)
    scope_id = _scope_id(definition)
    chain = load_partition_chain(raw_root, scope_id)
    selected = int(definition["selected_event_count"])
    committed = sum(int(item["requested_event_count"]) for item in chain)
    if committed > selected:
        raise CacheError("committed event partitions exceed the selected universe")
    remaining = selected - committed
    batch_size = int(settings["batch_size"])
    partition_events = int(settings["partition_events"])
    batch_raw_projection = remaining * int(
        settings["estimated_compressed_raw_bytes_per_event"]
    )
    estimated_fallback_events = math.ceil(
        remaining * float(settings["estimated_single_event_fallback_fraction"])
    )
    fallback_raw_projection = estimated_fallback_events * int(
        settings["estimated_fallback_raw_bytes_per_event"]
    )
    raw_projection = batch_raw_projection + fallback_raw_projection
    partition_normalized_projection = remaining * int(
        settings["estimated_compressed_normalized_bytes_per_event"]
    )
    merge_already_published = False
    merge_root = raw_root / EVENT_NAMESPACE / "merged_event_universes"
    for commit_path in merge_root.glob("*/merge_commit.json"):
        try:
            commit = json.loads(commit_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if commit.get("report", {}).get("scope_id") == scope_id:
            merge_already_published = True
            break
    final_normalized_projection = (
        0
        if merge_already_published
        else (
            selected * int(settings["estimated_compressed_normalized_bytes_per_event"])
        )
    )
    storage = StorageBudget(
        raw_root, max_bytes=max_raw_bytes, min_free_bytes=min_free_bytes
    ).snapshot()
    projected_total = (
        raw_projection + partition_normalized_projection + final_normalized_projection
    )
    audit_ready = (
        audit["malformed_tickers"] == 0
        and audit["duplicate_rows"] == 0
        and audit["sorted_input"]
        and audit["unique_events"] is not None
    )
    report = {
        "schema_version": SCHEMA_VERSION,
        "mode": "preflight",
        "network_requests_performed": 0,
        "scope_id": scope_id,
        "source_audit": audit,
        "total_events": audit["total_events"],
        "unique_events": audit["unique_events"],
        "malformed_tickers": audit["malformed_tickers"],
        "duplicate_tickers": audit["duplicate_tickers"],
        "selected_events": selected,
        "committed_events": committed,
        "remaining_events": remaining,
        "deterministic_batch_count": (
            math.ceil(selected / batch_size) if selected else 0
        ),
        "minimum_requests": math.ceil(selected / batch_size) if selected else 0,
        "estimated_fallback_events": estimated_fallback_events,
        "estimated_fallback_requests": estimated_fallback_events
        * (1 + int(settings["estimated_milestone_pages_per_fallback"])),
        "estimated_total_requests": (
            math.ceil(remaining / batch_size)
            + estimated_fallback_events
            * (1 + int(settings["estimated_milestone_pages_per_fallback"]))
        ),
        "maximum_requests": (
            math.ceil(remaining / batch_size)
            + remaining * (1 + int(settings["max_pages_per_batch"]))
        ),
        "remaining_minimum_requests": (
            math.ceil(remaining / batch_size) if remaining else 0
        ),
        "deterministic_partition_count": (
            math.ceil(selected / partition_events) if selected else 0
        ),
        "committed_partition_count": len(chain),
        "projected_batch_raw_bytes": batch_raw_projection,
        "projected_fallback_raw_bytes": fallback_raw_projection,
        "projected_compressed_raw_bytes": raw_projection,
        "projected_partition_normalized_bytes": partition_normalized_projection,
        "projected_final_normalized_bytes": final_normalized_projection,
        "projected_compressed_normalized_bytes": (
            partition_normalized_projection + final_normalized_projection
        ),
        "projected_additional_bytes": projected_total,
        "projected_namespace_bytes": storage["used_bytes"] + projected_total,
        "projected_free_bytes": storage["free_bytes"] - projected_total,
        "projected_free_space_margin_bytes": (
            storage["free_space_margin_bytes"] - projected_total
        ),
        "fits_namespace_ceiling": projected_total <= storage["remaining_budget_bytes"],
        "preserves_free_space_floor": projected_total
        <= storage["free_space_margin_bytes"],
        "ready_for_network": (
            audit_ready
            and projected_total <= storage["remaining_budget_bytes"]
            and projected_total <= storage["free_space_margin_bytes"]
        ),
        "storage": storage,
        "scope_definition": definition,
    }
    return report, definition, audit


def _partition_paths(
    raw_root: Path, scope_id: str, partition_id: str
) -> dict[str, Path]:
    base = raw_root / EVENT_NAMESPACE
    artifact_dir = base / "partition_artifacts" / scope_id / partition_id
    return {
        "pages": base / "partition_pages" / scope_id / partition_id / "pages",
        "event_metadata": artifact_dir / "event_metadata.jsonl.gz",
        "event_milestones": artifact_dir / "event_milestones.jsonl.gz",
        "event_provenance": artifact_dir / "event_source_provenance.jsonl.gz",
        "request_manifest": artifact_dir / "request_manifest.jsonl.gz",
        "normalization_report": artifact_dir / "normalization_report.json",
    }


def acquire_next_partition(
    *,
    event_tickers_path: Path,
    raw_root: Path,
    settings: Mapping[str, Any],
    definition: Mapping[str, Any],
    budget: StorageBudget,
    session: Any,
    validated: set[Path] | None = None,
    sleep: Any = time.sleep,
) -> dict[str, Any]:
    scope_id = _scope_id(definition)
    chain = load_partition_chain(raw_root, scope_id, validated=validated)
    selected_total = int(definition["selected_event_count"])
    offset = sum(int(item["requested_event_count"]) for item in chain)
    if offset >= selected_total:
        return {
            "scope_complete": True,
            "scope_id": scope_id,
            "partition_committed": False,
        }
    count = min(int(settings["partition_events"]), selected_total - offset)
    tickers = _ticker_slice(event_tickers_path, offset, count)
    if len(tickers) != count:
        raise CacheError(
            "event input ended before the deterministic partition boundary"
        )
    index = len(chain)
    pid = _partition_id(scope_id, index, offset, tickers)
    paths = _partition_paths(raw_root, scope_id, pid)
    client = KalshiEventMetadataClient(
        session,
        timeout_seconds=settings["timeout_seconds"],
        max_retries=settings["max_retries"],
        backoff_base_seconds=settings["backoff_base_seconds"],
        backoff_cap_seconds=settings["backoff_cap_seconds"],
    )
    event_variants: dict[str, dict[bytes, dict[str, Any]]] = {}
    sources: dict[str, list[dict[str, Any]]] = {}
    milestone_variants: dict[tuple[str, str, str], dict[bytes, dict[str, Any]]] = {}
    milestone_definitions: dict[str, bytes] = {}
    milestone_full_variants: dict[str, set[bytes]] = {}
    page_records: list[dict[str, Any]] = []
    duplicate_equivalent = 0
    collection_omission_count = 0
    milestone_timestamp_variant_count = 0
    cache_hits = 0
    fetched_requests = 0

    def ingest_events(
        events: Iterable[Mapping[str, Any]],
        allowed: set[str],
        provenance: Mapping[str, Any],
    ) -> list[Mapping[str, Any]]:
        nonlocal duplicate_equivalent
        materialized = list(events)
        for event in materialized:
            ticker = str(event.get("event_ticker") or event.get("ticker") or "").strip()
            if not ticker:
                raise EventAcquisitionError(
                    "malformed event object without event_ticker"
                )
            if ticker not in allowed:
                raise EventAcquisitionError(f"unexpected event ticker {ticker!r}")
            projected = research_event_projection(event)
            encoded = canonical_json(projected)
            variants = event_variants.setdefault(ticker, {})
            if encoded in variants:
                duplicate_equivalent += 1
            variants[encoded] = projected
            sources.setdefault(ticker, []).append(dict(provenance))
        return materialized

    def ingest_milestones(
        payload: Mapping[str, Any],
        events: Sequence[Mapping[str, Any]],
        allowed: set[str],
    ) -> None:
        nonlocal milestone_timestamp_variant_count
        milestone_rows, _ = collect_milestones(payload, events)
        raw_milestones = list(payload.get("milestones", []) or [])
        for event in events:
            raw_milestones.extend(event.get("milestones", []) or [])
        for milestone in raw_milestones:
            identifier = str(
                milestone.get("milestone_id") or milestone.get("id") or ""
            ).strip()
            if not identifier:
                raise EventAcquisitionError("milestone object lacks id")
            projected = canonical_json(_milestone_conflict_projection(milestone))
            full_projection = canonical_json(research_milestone_projection(milestone))
            if (
                identifier in milestone_definitions
                and milestone_definitions[identifier] != projected
            ):
                raise EventAcquisitionError(
                    f"conflicting duplicate milestone {identifier!r}"
                )
            milestone_definitions[identifier] = projected
            full_variants = milestone_full_variants.setdefault(identifier, set())
            if full_variants and full_projection not in full_variants:
                milestone_timestamp_variant_count += 1
            full_variants.add(full_projection)
        for row in milestone_rows:
            if row["event_ticker"] not in allowed:
                continue
            key = (
                row["event_ticker"],
                row["milestone_id"],
                row["association_type"],
            )
            encoded = canonical_json(row)
            variants = milestone_variants.setdefault(key, {})
            variants[encoded] = row

    batches = make_batches(tickers, int(settings["batch_size"]))
    for batch_number, batch in enumerate(batches, 1):
        batch_set = set(batch)
        cursor: str | None = None
        seen_cursors: set[str] = set()
        seen_responses: set[str] = set()
        for page_number in range(1, int(settings["max_pages_per_batch"]) + 1):
            if cursor and cursor in seen_cursors:
                raise EventAcquisitionError(
                    f"cursor loop in event batch {batch_number}"
                )
            if cursor:
                seen_cursors.add(cursor)
            params = request_parameters(batch, int(settings["page_size"]), cursor)
            request = {
                "schema_version": SCHEMA_VERSION,
                "scope_id": scope_id,
                "partition_id": pid,
                "partition_index": index,
                "batch_number": batch_number,
                "page_number": page_number,
                "request_kind": "batch_events",
                "endpoint_path": EVENTS_ENDPOINT,
                "params": dict(sorted(params.items())),
            }
            request_id = sha256_json(request)
            page_path = paths["pages"] / (
                f"batch_{batch_number:04d}_page_{page_number:04d}_{request_id[:24]}.json.gz"
            )
            wrapper = _load_raw_page(page_path, request)
            if wrapper is None:
                result = client.request_events(params)
                wrapper = _publish_raw_page(
                    budget,
                    page_path,
                    request,
                    result.payload,
                    {
                        "http_attempt_count": result.attempts,
                        "retry_count": result.retries,
                        "rate_limit_count": result.rate_limits,
                    },
                )
                attempts, retries, rate_limits = (
                    result.attempts,
                    result.retries,
                    result.rate_limits,
                )
                fetched_requests += 1
                if settings["requests_per_second"] and fetched_requests:
                    sleep(1.0 / float(settings["requests_per_second"]))
                cache_status = "fetched"
            else:
                acquisition = wrapper.get("acquisition", {})
                attempts = int(acquisition.get("http_attempt_count", 0))
                retries = int(acquisition.get("retry_count", 0))
                rate_limits = int(acquisition.get("rate_limit_count", 0))
                cache_hits += 1
                cache_status = "hit"
            response = wrapper["response"]
            response_hash = wrapper["response_sha256"]
            if response_hash in seen_responses:
                raise EventAcquisitionError(
                    f"duplicate response page in event batch {batch_number}"
                )
            seen_responses.add(response_hash)
            response_cursor = str(response.get("cursor") or "")
            provenance = {
                "request_identity": request_id,
                "partition_index": index,
                "batch_number": batch_number,
                "page_number": page_number,
                "request_kind": "batch_events",
                "endpoint_path": EVENTS_ENDPOINT,
                "request_cursor_hash": sha256_json(cursor or ""),
                "response_cursor_hash": sha256_json(response_cursor),
                "raw_page_relative_path": page_path.relative_to(raw_root).as_posix(),
            }
            events = response["events"]
            page_records.append(
                {
                    **provenance,
                    "path": str(page_path),
                    "request_identity": request_id,
                    "response_sha256": response_hash,
                    "page_file_sha256": _sha256_file(page_path),
                    "compressed_bytes": page_path.stat().st_size,
                    "row_count": len(events),
                    "terminal_page": not bool(response_cursor),
                    "http_attempt_count": attempts,
                    "retry_count": retries,
                    "rate_limit_count": rate_limits,
                    "cache_status": cache_status,
                }
            )
            materialized_events = ingest_events(events, batch_set, provenance)
            ingest_milestones(response, materialized_events, batch_set)
            if not response_cursor:
                break
            cursor = response_cursor
        else:
            raise EventAcquisitionError(
                f"event batch {batch_number} exceeded max_pages_per_batch"
            )

        fallback_tickers = sorted(batch_set - set(event_variants))
        collection_omission_count += len(fallback_tickers)
        for fallback_ticker in fallback_tickers:
            fallback_params = {"with_nested_markets": "false"}
            fallback_endpoint = f"{EVENTS_ENDPOINT}/{fallback_ticker}"
            fallback_request = {
                "schema_version": SCHEMA_VERSION,
                "scope_id": scope_id,
                "partition_id": pid,
                "partition_index": index,
                "batch_number": batch_number,
                "page_number": 1,
                "request_kind": "single_event_fallback",
                "fallback_ticker": fallback_ticker,
                "endpoint_path": fallback_endpoint,
                "params": fallback_params,
            }
            fallback_request_id = sha256_json(fallback_request)
            fallback_path = paths["pages"] / (
                f"fallback_event_{fallback_request_id[:24]}.json.gz"
            )
            fallback_wrapper = _load_raw_page(fallback_path, fallback_request)
            if fallback_wrapper is None:
                fallback_result = client.request_event(fallback_ticker, fallback_params)
                fallback_wrapper = _publish_raw_page(
                    budget,
                    fallback_path,
                    fallback_request,
                    fallback_result.payload,
                    {
                        "http_attempt_count": fallback_result.attempts,
                        "retry_count": fallback_result.retries,
                        "rate_limit_count": fallback_result.rate_limits,
                    },
                )
                attempts, retries, rate_limits = (
                    fallback_result.attempts,
                    fallback_result.retries,
                    fallback_result.rate_limits,
                )
                fetched_requests += 1
                sleep(1.0 / float(settings["requests_per_second"]))
                cache_status = "fetched"
            else:
                acquisition = fallback_wrapper.get("acquisition", {})
                attempts = int(acquisition.get("http_attempt_count", 0))
                retries = int(acquisition.get("retry_count", 0))
                rate_limits = int(acquisition.get("rate_limit_count", 0))
                cache_hits += 1
                cache_status = "hit"
            fallback_response = fallback_wrapper["response"]
            fallback_provenance = {
                "request_identity": fallback_request_id,
                "partition_index": index,
                "batch_number": batch_number,
                "page_number": 1,
                "request_kind": "single_event_fallback",
                "endpoint_path": fallback_endpoint,
                "request_cursor_hash": sha256_json(""),
                "response_cursor_hash": sha256_json(""),
                "raw_page_relative_path": fallback_path.relative_to(
                    raw_root
                ).as_posix(),
            }
            page_records.append(
                {
                    **fallback_provenance,
                    "fallback_ticker": fallback_ticker,
                    "path": str(fallback_path),
                    "response_sha256": fallback_wrapper["response_sha256"],
                    "page_file_sha256": _sha256_file(fallback_path),
                    "compressed_bytes": fallback_path.stat().st_size,
                    "row_count": 1,
                    "terminal_page": True,
                    "http_attempt_count": attempts,
                    "retry_count": retries,
                    "rate_limit_count": rate_limits,
                    "cache_status": cache_status,
                }
            )
            fallback_event = fallback_response["event"]
            materialized_events = ingest_events(
                [fallback_event], batch_set, fallback_provenance
            )
            ingest_milestones({"milestones": []}, materialized_events, batch_set)

            milestone_cursor: str | None = None
            milestone_seen_cursors: set[str] = set()
            for milestone_page in range(1, int(settings["max_pages_per_batch"]) + 1):
                if milestone_cursor and milestone_cursor in milestone_seen_cursors:
                    raise EventAcquisitionError(
                        f"cursor loop in milestone fallback {fallback_ticker!r}"
                    )
                if milestone_cursor:
                    milestone_seen_cursors.add(milestone_cursor)
                milestone_params: dict[str, Any] = {
                    "related_event_ticker": fallback_ticker,
                    "limit": 500,
                }
                if milestone_cursor:
                    milestone_params["cursor"] = milestone_cursor
                milestone_request = {
                    "schema_version": SCHEMA_VERSION,
                    "scope_id": scope_id,
                    "partition_id": pid,
                    "partition_index": index,
                    "batch_number": batch_number,
                    "page_number": milestone_page,
                    "request_kind": "related_milestone_fallback",
                    "fallback_ticker": fallback_ticker,
                    "endpoint_path": MILESTONES_ENDPOINT,
                    "params": dict(sorted(milestone_params.items())),
                }
                milestone_request_id = sha256_json(milestone_request)
                milestone_path = paths["pages"] / (
                    f"fallback_milestones_{milestone_request_id[:24]}.json.gz"
                )
                milestone_wrapper = _load_raw_page(milestone_path, milestone_request)
                if milestone_wrapper is None:
                    milestone_result = client.request_milestones(milestone_params)
                    milestone_wrapper = _publish_raw_page(
                        budget,
                        milestone_path,
                        milestone_request,
                        milestone_result.payload,
                        {
                            "http_attempt_count": milestone_result.attempts,
                            "retry_count": milestone_result.retries,
                            "rate_limit_count": milestone_result.rate_limits,
                        },
                    )
                    attempts, retries, rate_limits = (
                        milestone_result.attempts,
                        milestone_result.retries,
                        milestone_result.rate_limits,
                    )
                    fetched_requests += 1
                    sleep(1.0 / float(settings["requests_per_second"]))
                    cache_status = "fetched"
                else:
                    acquisition = milestone_wrapper.get("acquisition", {})
                    attempts = int(acquisition.get("http_attempt_count", 0))
                    retries = int(acquisition.get("retry_count", 0))
                    rate_limits = int(acquisition.get("rate_limit_count", 0))
                    cache_hits += 1
                    cache_status = "hit"
                milestone_response = milestone_wrapper["response"]
                response_cursor = str(milestone_response.get("cursor") or "")
                milestone_provenance = {
                    "request_identity": milestone_request_id,
                    "partition_index": index,
                    "batch_number": batch_number,
                    "page_number": milestone_page,
                    "request_kind": "related_milestone_fallback",
                    "endpoint_path": MILESTONES_ENDPOINT,
                    "request_cursor_hash": sha256_json(milestone_cursor or ""),
                    "response_cursor_hash": sha256_json(response_cursor),
                    "raw_page_relative_path": milestone_path.relative_to(
                        raw_root
                    ).as_posix(),
                }
                page_records.append(
                    {
                        **milestone_provenance,
                        "fallback_ticker": fallback_ticker,
                        "path": str(milestone_path),
                        "response_sha256": milestone_wrapper["response_sha256"],
                        "page_file_sha256": _sha256_file(milestone_path),
                        "compressed_bytes": milestone_path.stat().st_size,
                        "row_count": len(milestone_response["milestones"]),
                        "terminal_page": not bool(response_cursor),
                        "http_attempt_count": attempts,
                        "retry_count": retries,
                        "rate_limit_count": rate_limits,
                        "cache_status": cache_status,
                    }
                )
                ingest_milestones(milestone_response, [], batch_set)
                if not response_cursor:
                    break
                milestone_cursor = response_cursor
            else:
                raise EventAcquisitionError(
                    f"milestone fallback {fallback_ticker!r} exceeded page limit"
                )

    conflicts = sorted(
        ticker for ticker, values in event_variants.items() if len(values) > 1
    )
    if conflicts:
        raise EventAcquisitionError(
            f"conflicting duplicate events: {', '.join(conflicts)}"
        )
    milestone_conflicts = sorted(
        key
        for key, values in milestone_variants.items()
        if len(
            {
                canonical_json(_milestone_row_conflict_projection(row))
                for row in values.values()
            }
        )
        > 1
    )
    if milestone_conflicts:
        raise EventAcquisitionError("conflicting milestone associations")
    retrieved = sorted(event_variants)
    missing = sorted(set(tickers) - set(retrieved))
    event_rows = [
        normalize_event(next(iter(event_variants[ticker].values())))
        for ticker in retrieved
    ]
    milestone_rows = [
        max(
            milestone_variants[key].values(),
            key=lambda row: (
                str(row.get("milestone_last_updated_ts") or ""),
                canonical_json(row),
            ),
        )
        for key in sorted(milestone_variants)
    ]
    provenance_rows = []
    for row in event_rows:
        ticker = row["event_ticker"]
        associations = sorted(
            {canonical_json(item): item for item in sources[ticker]}.values(),
            key=canonical_json,
        )
        provenance_rows.append(
            {
                "event_ticker": ticker,
                "scope_id": scope_id,
                "partition_id": pid,
                "event_metadata_sha256": sha256_json(row),
                "source_associations": associations,
            }
        )
    contents = {
        "event_metadata": _gzip_jsonl(event_rows),
        "event_milestones": _gzip_jsonl(milestone_rows),
        "event_provenance": _gzip_jsonl(provenance_rows),
        "request_manifest": _gzip_jsonl(
            {key: value for key, value in item.items() if key != "cache_status"}
            for item in page_records
        ),
    }
    normalization = {
        "schema_version": SCHEMA_VERSION,
        "requested_event_count": len(tickers),
        "retrieved_event_count": len(retrieved),
        "missing_event_count": len(missing),
        "missing_event_tickers": missing,
        "collection_omission_count": collection_omission_count,
        "single_event_fallback_count": sum(
            item.get("request_kind") == "single_event_fallback" for item in page_records
        ),
        "related_milestone_fallback_request_count": sum(
            item.get("request_kind") == "related_milestone_fallback"
            for item in page_records
        ),
        "milestone_timestamp_variant_count": milestone_timestamp_variant_count,
        "duplicate_equivalent_event_count": duplicate_equivalent,
        "conflicting_duplicate_event_count": 0,
        "milestone_association_count": len(milestone_rows),
        "outcome_quarantine_enabled": True,
        "normalized_metadata_contains_outcomes": False,
        "timestamps_are_candidate_evidence_only": True,
        "anchors_verified": False,
    }
    contents["normalization_report"] = canonical_json(normalization) + b"\n"
    artifact_order = (
        "event_metadata",
        "event_milestones",
        "event_provenance",
        "request_manifest",
        "normalization_report",
    )
    for kind in artifact_order:
        _publish_budgeted(budget, paths[kind], contents[kind])
    artifacts = [_artifact_reference(kind, paths[kind]) for kind in artifact_order]
    commit = {
        "schema_version": SCHEMA_VERSION,
        "scope_id": scope_id,
        "partition_id": pid,
        "partition_index": index,
        "partition_complete": True,
        "event_offset": offset,
        "requested_event_count": len(tickers),
        "retrieved_event_count": len(retrieved),
        "missing_event_count": len(missing),
        "collection_omission_count": collection_omission_count,
        "single_event_fallback_count": normalization["single_event_fallback_count"],
        "related_milestone_fallback_request_count": normalization[
            "related_milestone_fallback_request_count"
        ],
        "milestone_timestamp_variant_count": normalization[
            "milestone_timestamp_variant_count"
        ],
        "first_event_ticker": tickers[0] if tickers else None,
        "last_event_ticker": tickers[-1] if tickers else None,
        "ticker_sha256": sha256_json(list(tickers)),
        "source_sha256": definition["source_sha256"],
        "source_merge_id": definition["source_merge_id"],
        "effective_configuration": dict(settings),
        "source_pages": [
            {key: value for key, value in page.items() if key != "cache_status"}
            for page in page_records
        ],
        "artifacts": artifacts,
        "normalization_summary": normalization,
    }
    commit_path = (
        raw_root
        / EVENT_NAMESPACE
        / "partition_commits"
        / scope_id
        / f"partition_{index:06d}_{pid}.json"
    )
    _publish_budgeted(budget, commit_path, canonical_json(commit) + b"\n")
    if not _valid_partition_commit(
        commit_path, expected_scope_id=scope_id, validated=validated
    ):
        raise CacheError("published event partition commit failed validation")
    updated_chain = load_partition_chain(raw_root, scope_id, validated=validated)
    run_state = {
        "schema_version": SCHEMA_VERSION,
        "scope_id": scope_id,
        "scope_complete": offset + len(tickers) == selected_total,
        "selected_event_count": selected_total,
        "committed_partition_count": len(updated_chain),
        "committed_event_count": sum(
            int(item["requested_event_count"]) for item in updated_chain
        ),
        "retrieved_event_count": sum(
            int(item["retrieved_event_count"]) for item in updated_chain
        ),
        "missing_event_count": sum(
            int(item["missing_event_count"]) for item in updated_chain
        ),
        "last_partition_commit": str(commit_path),
        "outcome_quarantine_enabled": True,
        "anchors_verified": False,
        "storage": budget.snapshot(),
    }
    report_id = sha256_json(run_state)[:24]
    run_report_path = (
        raw_root
        / EVENT_NAMESPACE
        / "run_reports"
        / scope_id
        / f"run_state_{report_id}.json"
    )
    _publish_budgeted(budget, run_report_path, canonical_json(run_state) + b"\n")
    return {
        "partition_committed": True,
        "partition_id": pid,
        "partition_commit": str(commit_path),
        "partition_index": index,
        "requested_event_count": len(tickers),
        "retrieved_event_count": len(retrieved),
        "missing_event_count": len(missing),
        "collection_omission_count": collection_omission_count,
        "single_event_fallback_count": normalization["single_event_fallback_count"],
        "related_milestone_fallback_request_count": normalization[
            "related_milestone_fallback_request_count"
        ],
        "milestone_timestamp_variant_count": normalization[
            "milestone_timestamp_variant_count"
        ],
        "network_request_count": client.network_request_count,
        "logical_request_count": len(page_records),
        "cache_hit_count": cache_hits,
        "retry_count": client.retry_count,
        "rate_limit_count": client.rate_limit_count,
        "scope_complete": offset + len(tickers) == selected_total,
        "run_report": str(run_report_path),
        "storage": budget.snapshot(),
    }


class _BudgetedGzipSink:
    def __init__(self, path: Path, budget: StorageBudget) -> None:
        self.path = path
        self.budget = budget
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("xb")
        self.compressor = zlib.compressobj(9, zlib.DEFLATED, 31)
        self.digest = hashlib.sha256()
        self.bytes_written = 0

    def _compressed(self, content: bytes) -> None:
        if not content:
            return
        self.budget.check_additional(len(content))
        self.handle.write(content)
        self.digest.update(content)
        self.bytes_written += len(content)

    def write(self, content: bytes) -> None:
        self._compressed(self.compressor.compress(content))

    def close(self) -> dict[str, Any]:
        self._compressed(self.compressor.flush())
        self.handle.flush()
        os.fsync(self.handle.fileno())
        self.handle.close()
        return {
            "sha256": self.digest.hexdigest(),
            "bytes": self.bytes_written,
            "compression": "gzip",
        }


def _csv_chunk(
    rows: Iterable[Mapping[str, Any]], fields: Sequence[str], *, include_header: bool
) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(
        stream, fieldnames=fields, extrasaction="ignore", lineterminator="\n"
    )
    if include_header:
        writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def merge_completed_scope(
    *, raw_root: Path, definition: Mapping[str, Any], budget: StorageBudget
) -> dict[str, Any]:
    scope_id = _scope_id(definition)
    chain = load_partition_chain(raw_root, scope_id)
    expected_partitions = (
        math.ceil(
            int(definition["selected_event_count"])
            / int(definition["partition_events"])
        )
        if definition["selected_event_count"]
        else 0
    )
    complete_count = sum(int(item["requested_event_count"]) for item in chain)
    missing = sum(int(item["missing_event_count"]) for item in chain)
    if (
        len(chain) != expected_partitions
        or complete_count != definition["selected_event_count"]
    ):
        return {
            "merge_complete": False,
            "final_universe_published": False,
            "reason": "event_partitions_incomplete",
            "scope_id": scope_id,
            "committed_partitions": len(chain),
            "expected_partitions": expected_partitions,
        }
    if missing:
        return {
            "merge_complete": False,
            "final_universe_published": False,
            "reason": "missing_events",
            "scope_id": scope_id,
            "missing_event_count": missing,
        }
    commit_refs = [
        {
            "path": item["_commit_path"],
            "sha256": _sha256_file(Path(item["_commit_path"])),
        }
        for item in chain
    ]
    merge_id = sha256_json({"scope_id": scope_id, "partition_commits": commit_refs})[
        :24
    ]
    final_dir = raw_root / EVENT_NAMESPACE / "merged_event_universes" / merge_id
    final_commit = final_dir / "merge_commit.json"
    if final_commit.exists():
        record = json.loads(final_commit.read_text(encoding="utf-8"))
        for item in record.get("artifacts", []):
            path = Path(item["path"])
            if not path.is_file() or _sha256_file(path) != item["sha256"]:
                raise CacheError("existing event merge commit is invalid")
        return record["report"]

    work_parent = raw_root / EVENT_NAMESPACE
    work_parent.mkdir(parents=True, exist_ok=True)
    work_dir = Path(tempfile.mkdtemp(prefix="event-merge-work.", dir=work_parent))
    metadata_sink = milestones_sink = provenance_sink = None
    try:
        metadata_sink = _BudgetedGzipSink(work_dir / "event_metadata.csv.gz", budget)
        milestones_sink = _BudgetedGzipSink(
            work_dir / "event_milestones.csv.gz", budget
        )
        provenance_sink = _BudgetedGzipSink(
            work_dir / "event_source_provenance.jsonl.gz", budget
        )
        event_count = milestone_count = provenance_count = 0
        previous_event: str | None = None
        previous_milestone_key: tuple[str, str, str] | None = None
        milestone_definitions: dict[str, str] = {}
        duplicate_events = duplicate_milestones = event_conflicts = (
            milestone_conflicts
        ) = 0
        metadata_header = milestones_header = True
        for commit in chain:
            artifact_by_kind = {
                item["kind"]: Path(item["path"]) for item in commit["artifacts"]
            }
            event_chunk = []
            for row in _read_gzip_jsonl(artifact_by_kind["event_metadata"]):
                ticker = str(row.get("event_ticker") or "")
                if previous_event is not None and ticker <= previous_event:
                    if ticker == previous_event:
                        duplicate_events += 1
                    else:
                        event_conflicts += 1
                    raise CacheError("event metadata merge order or uniqueness changed")
                previous_event = ticker
                event_chunk.append(row)
                event_count += 1
            metadata_sink.write(
                _csv_chunk(
                    event_chunk, EVENT_METADATA_FIELDS, include_header=metadata_header
                )
            )
            metadata_header = False

            milestone_chunk = []
            for row in _read_gzip_jsonl(artifact_by_kind["event_milestones"]):
                key = (
                    row["event_ticker"],
                    row["milestone_id"],
                    row["association_type"],
                )
                if previous_milestone_key is not None and key <= previous_milestone_key:
                    if key == previous_milestone_key:
                        duplicate_milestones += 1
                    else:
                        milestone_conflicts += 1
                    raise CacheError(
                        "event milestone merge order or uniqueness changed"
                    )
                previous_milestone_key = key
                milestone_definition = sha256_json(
                    {
                        field: row.get(field, "")
                        for field in MILESTONE_FIELDS
                        if field
                        not in {
                            "event_ticker",
                            "association_type",
                            "milestone_last_updated_ts",
                        }
                    }
                )
                identifier = row["milestone_id"]
                if (
                    identifier in milestone_definitions
                    and milestone_definitions[identifier] != milestone_definition
                ):
                    milestone_conflicts += 1
                    raise CacheError(
                        f"cross-partition milestone conflict {identifier!r}"
                    )
                milestone_definitions[identifier] = milestone_definition
                milestone_chunk.append(row)
                milestone_count += 1
            milestones_sink.write(
                _csv_chunk(
                    milestone_chunk, MILESTONE_FIELDS, include_header=milestones_header
                )
            )
            milestones_header = False
            for row in _read_gzip_jsonl(artifact_by_kind["event_provenance"]):
                provenance_sink.write(canonical_json(row) + b"\n")
                provenance_count += 1
        if metadata_header:
            metadata_sink.write(
                _csv_chunk([], EVENT_METADATA_FIELDS, include_header=True)
            )
        if milestones_header:
            milestones_sink.write(_csv_chunk([], MILESTONE_FIELDS, include_header=True))
        refs = {
            "event_metadata.csv.gz": metadata_sink.close(),
            "event_milestones.csv.gz": milestones_sink.close(),
            "event_source_provenance.jsonl.gz": provenance_sink.close(),
        }
        metadata_sink = milestones_sink = provenance_sink = None
        report = {
            "schema_version": SCHEMA_VERSION,
            "merge_complete": True,
            "final_universe_published": True,
            "scope_id": scope_id,
            "merge_id": merge_id,
            "limited_run": definition["requested_limit"] is not None,
            "requested_event_count": definition["selected_event_count"],
            "retrieved_event_count": event_count,
            "missing_event_count": 0,
            "partition_count": len(chain),
            "collection_omission_count": sum(
                int(item.get("collection_omission_count", 0)) for item in chain
            ),
            "single_event_fallback_count": sum(
                int(item.get("single_event_fallback_count", 0)) for item in chain
            ),
            "related_milestone_fallback_request_count": sum(
                int(item.get("related_milestone_fallback_request_count", 0))
                for item in chain
            ),
            "logical_request_count": sum(len(item["source_pages"]) for item in chain),
            "successful_http_attempt_count": sum(
                int(page["http_attempt_count"])
                for item in chain
                for page in item["source_pages"]
            ),
            "retry_count": sum(
                int(page["retry_count"])
                for item in chain
                for page in item["source_pages"]
            ),
            "rate_limit_count": sum(
                int(page["rate_limit_count"])
                for item in chain
                for page in item["source_pages"]
            ),
            "compressed_raw_page_bytes": sum(
                int(page["compressed_bytes"])
                for item in chain
                for page in item["source_pages"]
            ),
            "compressed_partition_artifact_bytes": sum(
                int(artifact["bytes"])
                for item in chain
                for artifact in item["artifacts"]
            ),
            "milestone_association_count": milestone_count,
            "provenance_count": provenance_count,
            "duplicate_event_count": duplicate_events,
            "duplicate_milestone_count": duplicate_milestones,
            "event_conflict_count": event_conflicts,
            "milestone_conflict_count": milestone_conflicts,
            "outcome_quarantine_enabled": True,
            "outcomes_merged_into_research_metadata": False,
            "anchors_verified": False,
        }
        report_content = canonical_json(report) + b"\n"
        report_path = work_dir / "merge_report.json"
        _publish_budgeted(budget, report_path, report_content)
        refs["merge_report.json"] = {
            "sha256": hashlib.sha256(report_content).hexdigest(),
            "bytes": len(report_content),
            "compression": "none",
        }
        final_dir.mkdir(parents=True, exist_ok=True)
        artifacts = []
        for name, reference in refs.items():
            source = work_dir / name
            destination = final_dir / name
            if destination.exists():
                if _sha256_file(destination) != reference["sha256"]:
                    raise CacheError("conflicting immutable event merge artifact")
                source.unlink()
            else:
                os.replace(source, destination)
            artifacts.append({"kind": name, "path": str(destination), **reference})
        work_dir.rmdir()
        report["output_hashes"] = {item["kind"]: item["sha256"] for item in artifacts}
        report["storage"] = budget.snapshot()
        commit = {
            "schema_version": SCHEMA_VERSION,
            "merge_id": merge_id,
            "scope_definition": dict(definition),
            "partition_commits": commit_refs,
            "artifacts": artifacts,
            "report": report,
        }
        _publish_budgeted(budget, final_commit, canonical_json(commit) + b"\n")
        return report
    except Exception:
        for sink in (metadata_sink, milestones_sink, provenance_sink):
            if sink is not None and not sink.handle.closed:
                sink.handle.close()
        if work_dir.exists():
            shutil.rmtree(work_dir)
        raise


def run(
    args: argparse.Namespace, *, session: Any | None = None, sleep: Any = time.sleep
) -> int:
    settings = _load_settings(Path(args.config))
    for argument, key in (
        (args.partition_events, "partition_events"),
        (args.max_pages_per_batch, "max_pages_per_batch"),
    ):
        if argument is not None:
            settings[key] = argument
    if settings["partition_events"] % settings["batch_size"]:
        raise ValueError("partition_events must be an exact multiple of batch_size")
    max_raw_bytes = args.max_raw_bytes or int(settings["max_raw_bytes"])
    min_free_bytes = (
        args.min_free_bytes
        if args.min_free_bytes is not None
        else int(settings["min_free_bytes"])
    )
    raw_root = Path(args.raw_root)
    preflight, definition, _ = build_preflight(
        event_tickers_path=Path(args.event_tickers),
        raw_root=raw_root,
        settings=settings,
        limit_events=args.limit_events,
        expected_sha256=args.expected_event_ticker_sha256,
        expected_merge_id=args.expected_merge_id,
        max_raw_bytes=max_raw_bytes,
        min_free_bytes=min_free_bytes,
    )
    print(json.dumps(preflight, sort_keys=True))
    if args.preflight:
        return 0 if preflight["ready_for_network"] else 3
    if not preflight["ready_for_network"]:
        raise ResourceLimitError("event acquisition preflight did not pass")
    budget = StorageBudget(
        raw_root, max_bytes=max_raw_bytes, min_free_bytes=min_free_bytes
    )
    if args.merge_only:
        report = merge_completed_scope(
            raw_root=raw_root, definition=definition, budget=budget
        )
        print(json.dumps(report, sort_keys=True))
        return 0 if report.get("merge_complete") else 3
    if session is None:
        import requests

        session = requests.Session()
        session.headers.update({"User-Agent": args.user_agent})
    validated: set[Path] = set()
    while True:
        result = acquire_next_partition(
            event_tickers_path=Path(args.event_tickers),
            raw_root=raw_root,
            settings=settings,
            definition=definition,
            budget=budget,
            session=session,
            validated=validated,
            sleep=sleep,
        )
        print(json.dumps(result, sort_keys=True))
        if result.get("missing_event_count"):
            return 3
        if result.get("scope_complete") or not args.continue_all:
            break
    if result.get("scope_complete"):
        merge = merge_completed_scope(
            raw_root=raw_root, definition=definition, budget=budget
        )
        print(json.dumps(merge, sort_keys=True))
        return 0 if merge.get("merge_complete") else 3
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--event-tickers", required=True)
    parser.add_argument("--raw-root", default=str(DEFAULT_RAW_ROOT))
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--expected-event-ticker-sha256")
    parser.add_argument("--expected-merge-id")
    parser.add_argument("--limit-events", type=int)
    parser.add_argument("--partition-events", type=int)
    parser.add_argument("--max-pages-per-batch", type=int)
    parser.add_argument("--max-raw-bytes", type=int)
    parser.add_argument("--min-free-bytes", type=int)
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument("--continue-all", action="store_true")
    parser.add_argument("--merge-only", action="store_true")
    parser.add_argument(
        "--user-agent",
        default="prediction-market-longshot-bias/partitioned-event-metadata-v1",
    )
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return run(args)
    except (
        CacheError,
        EventAcquisitionError,
        EventMetadataRequestFailure,
        ResourceLimitError,
        SensitiveResponseError,
        ValueError,
    ) as exc:
        print(f"partitioned event acquisition failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
