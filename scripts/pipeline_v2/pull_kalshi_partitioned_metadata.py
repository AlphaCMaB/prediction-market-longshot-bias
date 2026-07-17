"""Acquire one disk-bounded Kalshi settled-market partition.

Historical market dates are filtered locally because Kalshi's historical
markets endpoint has no settlement-time query parameters. Invoke in module form.
"""

from __future__ import annotations

import argparse
from dataclasses import asdict
from datetime import datetime
import gzip
import hashlib
import json
from pathlib import Path
import sys
import tomllib
from typing import Any, Iterable, Mapping, Sequence
import uuid

from scripts.common.time_utils import parse_iso_utc
from scripts.pipeline_v2.kalshi_metadata_cache import (
    CacheError,
    CompressedPartitionCache,
    ResourceLimitError,
    StorageBudget,
    append_manifest,
    canonical_json,
    publish_immutable_bytes,
    sha256_json,
)
from scripts.pipeline_v2.kalshi_metadata_client import KalshiMetadataClient
from scripts.pipeline_v2.kalshi_metadata_planner import (
    EndpointSegment,
    cursor_hash,
    format_utc,
    generate_months,
    normalize_inclusive_dates,
    plan_endpoint_segments,
    request_id,
)
from scripts.pipeline_v2.prepare_kalshi_market_universe import (
    _metadata_row,
    _outcome_row,
)
from scripts.pipeline_v2.study_rules import load_study_rules
from scripts.pipeline_v2.pull_kalshi_settled_metadata import (
    DEFAULT_CONFIG,
    _cutoff_datetime,
    load_metadata_config,
)


SCHEMA_VERSION = 1
DEFAULT_RAW_ROOT = Path("data/pipeline_v2/market_acquisition/partitioned")
DEFAULT_MAX_RAW_BYTES = 5 * 1024**3
DEFAULT_MIN_FREE_BYTES = 80 * 1024**3
DEFAULT_PARTITION_PAGES = 25
DEFAULT_ESTIMATED_COMPRESSED_PAGE_BYTES = 16 * 1024**2


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _segment_record(segment: EndpointSegment) -> dict[str, Any]:
    return {
        "endpoint_tier": segment.endpoint_tier,
        "endpoint_path": segment.endpoint_path,
        "month": segment.month,
        "range_start_utc": segment.range_start_utc,
        "range_end_utc_exclusive": segment.range_end_utc_exclusive,
        "date_filter_enforcement": (
            "server_side_settlement_range"
            if segment.endpoint_tier == "live"
            else "client_side_only_no_historical_date_parameters"
        ),
    }


def segment_id(segment: EndpointSegment, cutoff_id: str) -> str:
    identity = {"cutoff_snapshot_id": cutoff_id, **_segment_record(segment)}
    return hashlib.sha256(canonical_json(identity)).hexdigest()[:24]


def partition_id(
    segment_identifier: str, partition_index: int, start_cursor: str | None
) -> str:
    identity = {
        "segment_id": segment_identifier,
        "partition_index": partition_index,
        "start_cursor_hash": cursor_hash(start_cursor),
    }
    return hashlib.sha256(canonical_json(identity)).hexdigest()[:24]


def _gzip_bytes(content: bytes) -> bytes:
    return gzip.compress(content, compresslevel=9, mtime=0)


def _gzip_jsonl(rows: Iterable[Mapping[str, Any]]) -> bytes:
    plain = b"".join(canonical_json(dict(row)) + b"\n" for row in rows)
    return _gzip_bytes(plain)


def _read_gzip_jsonl(path: Path) -> list[dict[str, Any]]:
    try:
        text = gzip.decompress(path.read_bytes()).decode("utf-8")
    except Exception as exc:
        raise CacheError(f"invalid gzip JSONL artifact: {path}") from exc
    values: list[dict[str, Any]] = []
    for line_number, line in enumerate(text.splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise CacheError(f"{path}:{line_number} is not a JSON object")
        values.append(value)
    return values


def _artifact_reference(kind: str, path: Path, content: bytes) -> dict[str, Any]:
    return {
        "kind": kind,
        "path": str(path),
        "sha256": _sha256(content),
        "bytes": len(content),
        "compression": "gzip" if path.suffix == ".gz" else "none",
    }


def _publish_budgeted(budget: StorageBudget, path: Path, content: bytes) -> str:
    budget.check_publication(path, content)
    return publish_immutable_bytes(path, content)


def _append_budgeted_manifest(
    budget: StorageBudget, path: Path, record: Mapping[str, Any]
) -> None:
    budget.check_additional(len(canonical_json(dict(record))) + 1)
    append_manifest(path, record)


def _valid_partition_commit(
    path: Path,
    expected_segment_id: str | None = None,
    validated_commit_paths: set[Path] | None = None,
) -> bool:
    if validated_commit_paths is not None and path in validated_commit_paths:
        return path.is_file()
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(record, dict) or record.get("schema_version") != SCHEMA_VERSION:
        return False
    if expected_segment_id and record.get("segment_id") != expected_segment_id:
        return False
    if not record.get("partition_complete"):
        return False
    try:
        expected_partition_id = partition_id(
            str(record.get("segment_id") or ""),
            int(record.get("partition_index", -1)),
            record.get("start_cursor"),
        )
    except (TypeError, ValueError):
        return False
    if record.get("partition_id") != expected_partition_id:
        return False
    if not isinstance(record.get("effective_configuration"), dict):
        return False
    pages = record.get("source_pages")
    artifacts = record.get("artifacts")
    if not isinstance(pages, list) or not pages or not isinstance(artifacts, list):
        return False
    if record.get("page_count") != len(pages):
        return False
    for page in pages:
        page_path = Path(str(page.get("immutable_page_path") or ""))
        if not page_path.is_file() or _sha256(page_path.read_bytes()) != page.get(
            "page_file_sha256"
        ):
            return False
        if (
            page.get("compression") != "gzip"
            or page.get("compressed_bytes") != page_path.stat().st_size
        ):
            return False
        try:
            wrapper = json.loads(gzip.decompress(page_path.read_bytes()))
        except Exception:
            return False
        request = wrapper.get("request")
        if not isinstance(request, dict) or wrapper.get("compression") != "gzip":
            return False
        if request_id(
            str(request.get("endpoint_path") or ""),
            dict(request.get("params") or {}),
            str(request.get("cutoff_id") or ""),
        ) != page.get("request_id") or request.get("request_cursor_hash") != page.get(
            "request_cursor_hash"
        ):
            return False
        response = wrapper.get("response")
        if wrapper.get("response_sha256") != page.get(
            "page_response_sha256"
        ) or sha256_json(response) != page.get("page_response_sha256"):
            return False
    terminal_pages = sum(bool(page.get("terminal_page")) for page in pages)
    if record.get("compressed_page_bytes") != sum(
        int(page.get("compressed_bytes", -1)) for page in pages
    ):
        return False
    if bool(record.get("archive_complete")) != (terminal_pages == 1):
        return False
    if bool(record.get("archive_complete")) != (record.get("end_cursor") is None):
        return False
    required_kinds = {
        "metadata",
        "outcomes",
        "provenance",
        "request_manifest",
        "normalization_report",
    }
    if {item.get("kind") for item in artifacts} != required_kinds:
        return False
    for artifact in artifacts:
        artifact_path = Path(str(artifact.get("path") or ""))
        if not artifact_path.is_file():
            return False
        content = artifact_path.read_bytes()
        if _sha256(content) != artifact.get("sha256") or len(content) != artifact.get(
            "bytes"
        ):
            return False
        if artifact.get("compression") == "gzip":
            try:
                gzip.decompress(content)
            except Exception:
                return False
    if validated_commit_paths is not None:
        validated_commit_paths.add(path)
    return True


def load_partition_chain(
    raw_root: Path,
    segment_identifier: str,
    validated_commit_paths: set[Path] | None = None,
) -> list[dict[str, Any]]:
    directory = raw_root / "partition_commits" / segment_identifier
    records: list[dict[str, Any]] = []
    for path in sorted(directory.glob("partition_*.json")):
        if not _valid_partition_commit(
            path, segment_identifier, validated_commit_paths
        ):
            raise CacheError(f"invalid partition commit: {path}")
        record = json.loads(path.read_text(encoding="utf-8"))
        record["_commit_path"] = str(path)
        records.append(record)
    expected_cursor: str | None = None
    for index, record in enumerate(records):
        if record.get("partition_index") != index:
            raise CacheError("partition commit indices are not contiguous")
        if record.get("start_cursor") != expected_cursor:
            raise CacheError("partition cursor chain is not contiguous")
        expected_cursor = record.get("end_cursor")
        if record.get("archive_complete") and index != len(records) - 1:
            raise CacheError("partition commits continue after a terminal cursor")
    return records


def _normalize_partition(
    fetched_records: Sequence[Any], interval: Any, rules: Any
) -> tuple[dict[str, bytes], dict[str, Any]]:
    metadata_rows: list[dict[str, Any]] = []
    outcome_rows: list[dict[str, Any]] = []
    provenance_rows: list[dict[str, Any]] = []
    rejects: list[dict[str, Any]] = []
    outside = 0
    for fetched in fetched_records:
        payload = dict(fetched.payload)
        ticker = str(payload.get("ticker") or "").strip()
        settled = parse_iso_utc(payload.get("settlement_ts"))
        if not ticker or settled is None:
            rejects.append(
                {
                    "ticker": ticker,
                    "payload_sha256": sha256_json(payload),
                    "reason": (
                        "missing_ticker" if not ticker else "invalid_settlement_ts"
                    ),
                    "source": fetched.provenance,
                }
            )
            continue
        if not interval.start <= settled < interval.end:
            outside += 1
            continue
        metadata = _metadata_row(payload)
        metadata_sha = sha256_json(metadata)
        metadata_rows.append({"metadata_sha256": metadata_sha, "metadata": metadata})
        outcome_rows.append(
            {
                "metadata_sha256": metadata_sha,
                "outcome": _outcome_row(payload, rules),
            }
        )
        provenance_rows.append(
            {
                "ticker": ticker,
                "metadata_sha256": metadata_sha,
                "source_payload_sha256": sha256_json(payload),
                "source_association": fetched.provenance,
            }
        )
    metadata_rows.sort(key=lambda item: canonical_json(item))
    outcome_rows.sort(key=lambda item: canonical_json(item))
    provenance_rows.sort(key=lambda item: canonical_json(item))
    rejects.sort(key=lambda item: canonical_json(item))
    contents = {
        "metadata": _gzip_jsonl(metadata_rows),
        "outcomes": _gzip_jsonl(outcome_rows),
        "provenance": _gzip_jsonl(provenance_rows),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "input_record_count": len(fetched_records),
        "in_range_record_count": len(metadata_rows),
        "outside_envelope_record_count": outside,
        "rejected_record_count": len(rejects),
        "rejects": rejects,
        "outcome_quarantine_enabled": True,
        "metadata_contains_outcomes": False,
        "date_filter_enforcement": "client_side_for_historical_server_side_plus_client_validation_for_live",
    }
    contents["normalization_report"] = canonical_json(report) + b"\n"
    return contents, report


def _planned_segments(interval: Any, cutoff: datetime) -> tuple[EndpointSegment, ...]:
    return plan_endpoint_segments(generate_months(interval), cutoff)


def _segment_state(
    raw_root: Path,
    segments: Sequence[EndpointSegment],
    cutoff_id: str,
    validated_commit_paths: set[Path] | None = None,
) -> list[dict[str, Any]]:
    state = []
    for segment in segments:
        sid = segment_id(segment, cutoff_id)
        chain = load_partition_chain(raw_root, sid, validated_commit_paths)
        state.append(
            {
                **_segment_record(segment),
                "segment_id": sid,
                "committed_partition_count": len(chain),
                "archive_complete": bool(chain and chain[-1].get("archive_complete")),
                "next_partition_index": len(chain),
                "next_cursor_hash": cursor_hash(
                    chain[-1].get("end_cursor") if chain else None
                ),
            }
        )
    return state


def _select_next_segment(
    raw_root: Path,
    segments: Sequence[EndpointSegment],
    cutoff_id: str,
    validated_commit_paths: set[Path] | None = None,
) -> tuple[EndpointSegment | None, list[dict[str, Any]]]:
    state = _segment_state(raw_root, segments, cutoff_id, validated_commit_paths)
    for segment, item in zip(segments, state):
        if not item["archive_complete"]:
            return segment, state
    return None, state


def _load_partition_settings(path: Path) -> dict[str, int]:
    with path.open("rb") as handle:
        data = tomllib.load(handle)
    section = data.get("kalshi_partitioned_acquisition", {})
    return {
        "partition_pages": int(section.get("partition_pages", DEFAULT_PARTITION_PAGES)),
        "max_raw_bytes": int(section.get("max_raw_bytes", DEFAULT_MAX_RAW_BYTES)),
        "min_free_bytes": int(section.get("min_free_bytes", DEFAULT_MIN_FREE_BYTES)),
        "estimated_compressed_page_bytes": int(
            section.get(
                "estimated_compressed_page_bytes",
                DEFAULT_ESTIMATED_COMPRESSED_PAGE_BYTES,
            )
        ),
    }


def _preflight_record(
    *,
    interval: Any,
    cutoff: datetime,
    cutoff_id: str,
    segment: EndpointSegment | None,
    state: list[dict[str, Any]],
    budget: StorageBudget,
    partition_pages: int,
    estimated_page_bytes: int,
) -> dict[str, Any]:
    storage = budget.snapshot()
    estimated_partition_bytes = partition_pages * estimated_page_bytes
    return {
        "schema_version": SCHEMA_VERSION,
        "mode": "preflight",
        "normalized_range": {
            "start_utc": format_utc(interval.start),
            "end_utc_exclusive": format_utc(interval.end),
        },
        "cutoff_snapshot_id": cutoff_id,
        "cutoff_utc": format_utc(cutoff),
        "next_segment": _segment_record(segment) if segment else None,
        "segment_state": state,
        "minimum_network_requests_next_partition": 0,
        "maximum_requests_next_partition": 0 if segment is None else partition_pages,
        "estimated_compressed_bytes_next_partition": (
            0 if segment is None else estimated_partition_bytes
        ),
        "estimated_compressed_page_bytes": estimated_page_bytes,
        "historical_total_request_estimate": "unknown_endpoint_has_no_date_or_count_filter",
        "historical_server_side_date_filter": False,
        "live_server_side_date_filter": True,
        "estimated_partition_fits_raw_budget": (
            estimated_partition_bytes <= storage["remaining_budget_bytes"]
        ),
        "estimated_partition_preserves_free_space_floor": (
            estimated_partition_bytes <= storage["free_space_margin_bytes"]
        ),
        "storage": storage,
    }


def run(
    args: argparse.Namespace,
    *,
    session: Any | None = None,
    validated_commit_paths: set[Path] | None = None,
) -> int:
    interval = normalize_inclusive_dates(args.start_date, args.end_date)
    metadata_config = load_metadata_config(args.config)
    partition_config = _load_partition_settings(Path(args.config))
    partition_pages = (
        args.partition_pages
        if args.partition_pages is not None
        else partition_config["partition_pages"]
    )
    max_raw_bytes = (
        args.max_raw_bytes
        if args.max_raw_bytes is not None
        else partition_config["max_raw_bytes"]
    )
    min_free_bytes = (
        args.min_free_bytes
        if args.min_free_bytes is not None
        else partition_config["min_free_bytes"]
    )
    if partition_pages <= 0 or max_raw_bytes <= 0 or min_free_bytes < 0:
        raise ValueError("partition and storage limits are invalid")
    raw_root = Path(args.raw_root)
    budget = StorageBudget(
        raw_root, max_bytes=max_raw_bytes, min_free_bytes=min_free_bytes
    )
    if not args.cutoff_snapshot and args.preflight:
        raise ValueError(
            "--preflight requires --cutoff-snapshot and performs no network request"
        )

    if session is None:
        import requests

        session = requests.Session()
        session.headers.update({"User-Agent": args.user_agent})
    settings = {
        key: metadata_config[key]
        for key in (
            "page_size",
            "max_retries",
            "backoff_base_seconds",
            "backoff_cap_seconds",
            "requests_per_second",
            "timeout_seconds",
        )
    }
    client = KalshiMetadataClient(session, **settings)
    if args.cutoff_snapshot:
        cutoff_payload = CompressedPartitionCache.load_cutoff_snapshot(
            args.cutoff_snapshot
        )
        cutoff_id = sha256_json(cutoff_payload)[:20]
        if not args.preflight:
            cutoff_cache = CompressedPartitionCache(
                raw_root, partition_id="cutoff", budget=budget
            )
            stored_cutoff_id, _ = cutoff_cache.store_cutoff_snapshot(cutoff_payload)
            if stored_cutoff_id != cutoff_id:
                raise CacheError("stored cutoff identity mismatch")
    else:
        cutoff_payload = client.fetch_cutoff()
        cutoff_cache = CompressedPartitionCache(
            raw_root, partition_id="cutoff", budget=budget
        )
        cutoff_id, _ = cutoff_cache.store_cutoff_snapshot(cutoff_payload)
    cutoff = _cutoff_datetime(cutoff_payload)
    segments = _planned_segments(interval, cutoff)
    segment, state = _select_next_segment(
        raw_root, segments, cutoff_id, validated_commit_paths
    )
    preflight = _preflight_record(
        interval=interval,
        cutoff=cutoff,
        cutoff_id=cutoff_id,
        segment=segment,
        state=state,
        budget=budget,
        partition_pages=partition_pages,
        estimated_page_bytes=partition_config["estimated_compressed_page_bytes"],
    )
    print(json.dumps(preflight, sort_keys=True))
    if args.preflight or segment is None:
        return 0
    if not preflight["estimated_partition_fits_raw_budget"]:
        raise ResourceLimitError("preflight estimate exceeds remaining raw-data budget")
    if not preflight["estimated_partition_preserves_free_space_floor"]:
        raise ResourceLimitError(
            "preflight estimate would cross minimum free-space guard"
        )

    sid = segment_id(segment, cutoff_id)
    chain = load_partition_chain(raw_root, sid, validated_commit_paths)
    index = len(chain)
    start_cursor = chain[-1].get("end_cursor") if chain else None
    pid = partition_id(sid, index, start_cursor)
    cache = CompressedPartitionCache(raw_root, partition_id=pid, budget=budget)
    request_run_id = uuid.uuid4().hex
    manifest_path = (
        Path(args.manifest) if args.manifest else raw_root / "manifest.jsonl"
    )
    result = client.paginate(
        segment,
        cache,
        cutoff_id=cutoff_id,
        run_id=request_run_id,
        resume=True,
        start_cursor=start_cursor,
        partition_page_limit=partition_pages,
        mve_filter=metadata_config["mve_filter"],
        manifest_sink=lambda record: _append_budgeted_manifest(
            budget, manifest_path, record
        ),
    )
    if not result.partition_complete:
        raise CacheError("partition stopped without a complete bounded boundary")
    rules = load_study_rules(args.config)
    contents, normalization_report = _normalize_partition(
        result.fetched_records, interval, rules
    )
    contents["request_manifest"] = _gzip_jsonl(result.manifest_records)
    artifact_directory = raw_root / "partition_artifacts" / sid / pid
    artifact_paths = {
        "metadata": artifact_directory / "market_metadata.jsonl.gz",
        "outcomes": artifact_directory / "market_outcomes.jsonl.gz",
        "provenance": artifact_directory / "record_provenance.jsonl.gz",
        "request_manifest": artifact_directory / "request_manifest.jsonl.gz",
        "normalization_report": artifact_directory / "normalization_report.json",
    }
    artifacts: list[dict[str, Any]] = []
    for kind in (
        "metadata",
        "outcomes",
        "provenance",
        "request_manifest",
        "normalization_report",
    ):
        content = contents[kind]
        _publish_budgeted(budget, artifact_paths[kind], content)
        artifacts.append(_artifact_reference(kind, artifact_paths[kind], content))

    source_pages = []
    for page in result.page_provenance:
        clean = {key: value for key, value in page.items() if key != "cache_status"}
        clean["compressed_bytes"] = Path(clean["immutable_page_path"]).stat().st_size
        clean["compression"] = "gzip"
        source_pages.append(clean)
    source_pages.sort(key=canonical_json)
    commit_record = {
        "schema_version": SCHEMA_VERSION,
        "partition_id": pid,
        "partition_index": index,
        "partition_complete": True,
        "archive_complete": bool(result.complete),
        "segment_id": sid,
        "segment": _segment_record(segment),
        "cutoff_snapshot_id": cutoff_id,
        "requested_range": {
            "start_utc": format_utc(interval.start),
            "end_utc_exclusive": format_utc(interval.end),
        },
        "start_cursor": start_cursor,
        "start_cursor_hash": cursor_hash(start_cursor),
        "end_cursor": result.next_cursor,
        "end_cursor_hash": cursor_hash(result.next_cursor),
        "page_limit": partition_pages,
        "effective_configuration": {
            "implementation_schema_version": SCHEMA_VERSION,
            "page_size": int(settings["page_size"]),
            "max_retries": int(settings["max_retries"]),
            "backoff_base_seconds": float(settings["backoff_base_seconds"]),
            "backoff_cap_seconds": float(settings["backoff_cap_seconds"]),
            "requests_per_second": float(settings["requests_per_second"]),
            "timeout_seconds": float(settings["timeout_seconds"]),
            "mve_filter": str(metadata_config["mve_filter"]),
            "partition_pages": partition_pages,
            "max_raw_bytes": max_raw_bytes,
            "min_free_bytes": min_free_bytes,
            "raw_page_compression": "gzip",
        },
        "page_count": len(source_pages),
        "compressed_page_bytes": sum(
            int(page["compressed_bytes"]) for page in source_pages
        ),
        "source_pages": source_pages,
        "artifacts": artifacts,
        "normalization_summary": {
            key: normalization_report[key]
            for key in (
                "input_record_count",
                "in_range_record_count",
                "outside_envelope_record_count",
                "rejected_record_count",
                "outcome_quarantine_enabled",
            )
        },
    }
    commit_path = (
        raw_root / "partition_commits" / sid / f"partition_{index:06d}_{pid}.json"
    )
    commit_content = canonical_json(commit_record) + b"\n"
    _publish_budgeted(budget, commit_path, commit_content)
    if not _valid_partition_commit(commit_path, sid, validated_commit_paths):
        raise CacheError("published partition commit failed validation")

    updated_state = _segment_state(
        raw_root, segments, cutoff_id, validated_commit_paths
    )
    run_report = {
        "schema_version": SCHEMA_VERSION,
        "requested_range": commit_record["requested_range"],
        "cutoff_snapshot_id": cutoff_id,
        "segment_state": updated_state,
        "overall_complete": all(item["archive_complete"] for item in updated_state),
        "last_partition_commit": str(commit_path),
        "last_partition_id": pid,
        "last_partition_archive_complete": bool(result.complete),
        "storage": budget.snapshot(),
    }
    report_hash = hashlib.sha256(canonical_json(run_report)).hexdigest()[:24]
    report_path = raw_root / "run_reports" / f"run_state_{report_hash}.json"
    _publish_budgeted(budget, report_path, canonical_json(run_report) + b"\n")
    print(
        json.dumps(
            {
                "partition_committed": True,
                "partition_id": pid,
                "partition_commit": str(commit_path),
                "archive_complete": bool(result.complete),
                "overall_complete": run_report["overall_complete"],
                "run_report": str(report_path),
                "normalization": commit_record["normalization_summary"],
                "actual_compressed_page_bytes": commit_record["compressed_page_bytes"],
                "counters": asdict(client.counters),
                "storage": run_report["storage"],
            },
            sort_keys=True,
        )
    )
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--raw-root", default=str(DEFAULT_RAW_ROOT))
    parser.add_argument("--manifest")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG), type=Path)
    parser.add_argument("--cutoff-snapshot")
    parser.add_argument("--partition-pages", type=int)
    parser.add_argument("--max-raw-bytes", type=int)
    parser.add_argument("--min-free-bytes", type=int)
    parser.add_argument("--preflight", action="store_true")
    parser.add_argument(
        "--continue-segment",
        action="store_true",
        help=(
            "continue independently committed partitions until the current "
            "segment reaches a terminal cursor"
        ),
    )
    parser.add_argument(
        "--user-agent",
        default="prediction-market-longshot-bias/partitioned-metadata-v1",
    )
    return parser


def run_current_segment(args: argparse.Namespace, *, session: Any | None = None) -> int:
    """Continue one segment while avoiding repeated full-chain decompression.

    The existing chain is fully validated once, every new commit is validated
    before it joins the in-memory validation set, cursor continuity is checked
    after each commit, and the completed segment receives a fresh full-chain
    validation before this function returns.
    """

    if args.preflight:
        raise ValueError("--continue-segment cannot be combined with --preflight")
    if not args.cutoff_snapshot:
        raise ValueError("--continue-segment requires a pinned --cutoff-snapshot")
    if session is None:
        import requests

        session = requests.Session()
        session.headers.update({"User-Agent": args.user_agent})

    interval = normalize_inclusive_dates(args.start_date, args.end_date)
    cutoff_payload = CompressedPartitionCache.load_cutoff_snapshot(args.cutoff_snapshot)
    cutoff_id = sha256_json(cutoff_payload)[:20]
    segments = _planned_segments(interval, _cutoff_datetime(cutoff_payload))
    validated_commit_paths: set[Path] = set()
    target, _ = _select_next_segment(
        Path(args.raw_root), segments, cutoff_id, validated_commit_paths
    )
    if target is None:
        return run(
            args,
            session=session,
            validated_commit_paths=validated_commit_paths,
        )
    target_id = segment_id(target, cutoff_id)

    while True:
        status = run(
            args,
            session=session,
            validated_commit_paths=validated_commit_paths,
        )
        if status != 0:
            return status
        chain = load_partition_chain(
            Path(args.raw_root), target_id, validated_commit_paths
        )
        if chain and chain[-1].get("archive_complete"):
            load_partition_chain(Path(args.raw_root), target_id)
            return 0


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = build_parser().parse_args(argv)
        return run_current_segment(args) if args.continue_segment else run(args)
    except (ValueError, CacheError, ResourceLimitError, RuntimeError) as exc:
        print(
            json.dumps(
                {
                    "run_complete": False,
                    "partition_committed": False,
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                sort_keys=True,
            ),
            file=sys.stderr,
        )
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
