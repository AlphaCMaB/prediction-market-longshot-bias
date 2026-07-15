"""Pull metadata; invoke as ``python -m scripts.pipeline_v2.pull_kalshi_settled_metadata``."""

from __future__ import annotations

import argparse
from datetime import datetime, timezone
import hashlib
import json
from pathlib import Path
import shutil
import sys
import tomllib
import uuid
from typing import Any, Sequence

from scripts.common.time_utils import parse_iso_utc
from scripts.pipeline_v2.kalshi_metadata_cache import (
    CacheError,
    ImmutableConflict,
    MetadataCache,
    append_manifest,
    canonical_json,
    publish_immutable_bytes,
    sha256_json,
    validate_immutable_destination,
)
from scripts.pipeline_v2.kalshi_metadata_client import KalshiMetadataClient
from scripts.pipeline_v2.kalshi_metadata_consolidation import (
    ConsolidationError,
    consolidate_month,
    invalid_audit_path,
    invalid_record_audits,
    monthly_audit_path,
    monthly_output_path,
    monthly_provenance_path,
    payload_sha256,
    serialize_jsonl,
    write_derived_jsonl,
)
from scripts.pipeline_v2.kalshi_metadata_planner import (
    estimate_requests,
    filter_month,
    format_utc,
    generate_months,
    normalize_inclusive_dates,
    plan_endpoint_segments,
)


DEFAULT_CONFIG = Path("configs/pipeline_v2.toml")
DEFAULT_RAW_ROOT = Path("data/raw/kalshi/settled_markets")


def _bytes_sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _run_commit_path(raw_root: Path, run_id: str) -> Path:
    return raw_root / "run_commits" / f"run_{run_id}.json"


def _valid_commit(path: Path, expected: dict[str, Any]) -> bool:
    if not path.exists():
        return False
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    for key in (
        "schema_version", "run_id", "selected_months", "cutoff_snapshot_id",
        "date_range", "effective_configuration", "source_pages",
    ):
        if record.get(key) != expected.get(key):
            return False
    expected_artifacts = {
        (item["kind"], item.get("month"), item["path"], item["sha256"])
        for item in expected["artifacts"]
    }
    recorded_artifacts = {
        (item["kind"], item.get("month"), item["path"], item["sha256"])
        for item in record.get("artifacts", [])
    }
    if recorded_artifacts != expected_artifacts:
        return False
    for item in record.get("artifacts", []):
        artifact = Path(item["path"])
        if not artifact.exists() or _bytes_sha256(artifact.read_bytes()) != item["sha256"]:
            return False
    source_pages = record.get("source_pages", [])
    if not source_pages:
        return False
    terminal_groups: dict[tuple[Any, ...], bool] = {}
    for source in source_pages:
        page = Path(source.get("immutable_page_path", ""))
        if not page.exists() or _bytes_sha256(page.read_bytes()) != source.get("page_file_sha256"):
            return False
        try:
            wrapper = json.loads(page.read_text(encoding="utf-8"))
        except Exception:
            return False
        response = wrapper.get("response")
        if (
            wrapper.get("response_sha256") != source.get("page_response_sha256")
            or sha256_json(response) != source.get("page_response_sha256")
        ):
            return False
        group = (
            source.get("endpoint_tier"), source.get("endpoint_path"), source.get("month"),
            source.get("range_start_utc"), source.get("range_end_utc_exclusive"),
        )
        terminal_groups[group] = terminal_groups.get(group, False) or bool(
            source.get("terminal_page")
        )
    if not terminal_groups or not all(terminal_groups.values()):
        return False
    planned_groups = {
        (
            segment.get("endpoint_tier"), segment.get("endpoint_path"),
            segment.get("month"), segment.get("range_start_utc"),
            segment.get("range_end_utc_exclusive"),
        )
        for segment in record.get("effective_configuration", {}).get(
            "endpoint_routing_plan", []
        )
    }
    if planned_groups != set(terminal_groups):
        return False

    artifacts = record.get("artifacts", [])
    for month in record.get("selected_months", []):
        monthly = [
            item for item in artifacts
            if item.get("month") == month and item.get("kind") == "monthly_consolidation"
        ]
        provenance = [
            item for item in artifacts
            if item.get("month") == month and item.get("kind") == "record_provenance"
        ]
        if len(monthly) != 1 or len(provenance) != 1:
            return False
        try:
            output_records = [
                json.loads(line) for line in Path(monthly[0]["path"]).read_text().splitlines()
                if line.strip()
            ]
            provenance_records = [
                json.loads(line) for line in Path(provenance[0]["path"]).read_text().splitlines()
                if line.strip()
            ]
        except Exception:
            return False
        if len(output_records) != len(provenance_records):
            return False
        expected_records = sorted(
            (str(item.get("ticker") or ""), payload_sha256(item))
            for item in output_records
        )
        recorded_records = sorted(
            (str(item.get("ticker") or ""), str(item.get("selected_payload_sha256") or ""))
            for item in provenance_records
        )
        if expected_records != recorded_records:
            return False
        if len({item.get("output_record_id") for item in provenance_records}) != len(
            provenance_records
        ):
            return False
        for entry in provenance_records:
            if not entry.get("source_associations"):
                return False
            identity = entry.get("monthly_output_artifact") or {}
            if (
                identity.get("path") != monthly[0]["path"]
                or identity.get("sha256") != monthly[0]["sha256"]
                or identity.get("source_set_hash") != monthly[0].get("source_set_hash")
            ):
                return False
    return True


def _segment_key(segment: Any) -> tuple[Any, ...]:
    return (
        segment.endpoint_tier, segment.endpoint_path, segment.month,
        segment.range_start_utc, segment.range_end_utc_exclusive,
    )


def _validate_endpoint_coverage_plan(
    months: Sequence[Any], cutoff: datetime, planned_segments: Sequence[Any], limit_pages: int | None
) -> None:
    if limit_pages is not None:
        raise ValueError("--limit-pages is smoke-test-only and cannot produce a committed run")
    required_segments = plan_endpoint_segments(months, cutoff)
    required = {_segment_key(segment) for segment in required_segments}
    planned = {_segment_key(segment) for segment in planned_segments}
    missing = sorted(required - planned, key=str)
    if not required or missing:
        raise ValueError(
            "endpoint modes do not provide required month coverage"
            + (f": {missing}" if missing else "")
        )


def _validate_commit_structure(record: dict[str, Any]) -> None:
    months = record.get("selected_months", [])
    artifacts = record.get("artifacts", [])
    pages = record.get("source_pages", [])
    plan = record.get("effective_configuration", {}).get("endpoint_routing_plan", [])
    if not months or not plan or not pages:
        raise ConsolidationError("transaction lacks selected months, routing, or source pages")
    for month in months:
        if sum(item.get("kind") == "monthly_consolidation" and item.get("month") == month for item in artifacts) != 1:
            raise ConsolidationError(f"month {month} lacks exactly one consolidation artifact")
        if sum(item.get("kind") == "record_provenance" and item.get("month") == month for item in artifacts) != 1:
            raise ConsolidationError(f"month {month} lacks exactly one provenance artifact")
    page_groups = {
        (
            page.get("endpoint_tier"), page.get("endpoint_path"), page.get("month"),
            page.get("range_start_utc"), page.get("range_end_utc_exclusive"),
        )
        for page in pages if page.get("terminal_page")
    }
    planned_groups = {
        (
            item.get("endpoint_tier"), item.get("endpoint_path"), item.get("month"),
            item.get("range_start_utc"), item.get("range_end_utc_exclusive"),
        )
        for item in plan
    }
    if page_groups != planned_groups:
        raise ConsolidationError("source pages do not prove every planned chain complete")


def _validate_prepared_provenance(prepared: Sequence[dict[str, Any]], months: Sequence[Any]) -> None:
    for month in (item.month for item in months):
        monthly = [
            item for item in prepared
            if item["kind"] == "monthly_consolidation" and item["month"] == month
        ]
        provenance = [
            item for item in prepared
            if item["kind"] == "record_provenance" and item["month"] == month
        ]
        if len(monthly) != 1 or len(provenance) != 1:
            raise ConsolidationError(f"month {month} lacks required provenance pairing")
        output_records = [
            json.loads(line) for line in monthly[0]["content"].decode("utf-8").splitlines()
            if line.strip()
        ]
        provenance_records = [
            json.loads(line) for line in provenance[0]["content"].decode("utf-8").splitlines()
            if line.strip()
        ]
        expected = sorted(
            (str(record.get("ticker") or ""), payload_sha256(record))
            for record in output_records
        )
        actual = sorted(
            (str(record.get("ticker") or ""), str(record.get("selected_payload_sha256") or ""))
            for record in provenance_records
        )
        if expected != actual or any(
            not record.get("source_associations") for record in provenance_records
        ):
            raise ConsolidationError(
                f"month {month} record provenance does not cover every output record"
            )


def _canonical_effective_configuration(
    args: argparse.Namespace,
    settings: dict[str, Any],
    config: dict[str, Any],
    interval: Any,
    months: Sequence[Any],
    cutoff_id: str,
    segments: Sequence[Any],
) -> dict[str, Any]:
    return {
        "implementation_schema_version": 2,
        "page_size": int(settings["page_size"]),
        "max_retries": int(settings["max_retries"]),
        "backoff_base_seconds": float(settings["backoff_base_seconds"]),
        "backoff_cap_seconds": float(settings["backoff_cap_seconds"]),
        "requests_per_second": float(settings["requests_per_second"]),
        "timeout_seconds": float(settings["timeout_seconds"]),
        "mve_filter": str(config["mve_filter"]),
        "resume": bool(args.resume),
        "historical_mode": str(args.historical_mode),
        "live_mode": str(args.live_mode),
        "limit_pages": int(args.limit_pages) if args.limit_pages is not None else None,
        "requested_start_utc": format_utc(interval.start),
        "requested_end_utc_exclusive": format_utc(interval.end),
        "selected_month_restriction": months[0].month if args.month else None,
        "selected_months": [month.month for month in months],
        "cutoff_snapshot_id": cutoff_id,
        "endpoint_routing_plan": [
            {
                "endpoint_tier": segment.endpoint_tier,
                "endpoint_path": segment.endpoint_path,
                "month": segment.month,
                "range_start_utc": segment.range_start_utc,
                "range_end_utc_exclusive": segment.range_end_utc_exclusive,
            }
            for segment in segments
        ],
    }


def _transaction_run_id(identity: dict[str, Any]) -> str:
    return hashlib.sha256(canonical_json(identity)).hexdigest()[:24]


def _commit_bytes(record: dict[str, Any]) -> bytes:
    return canonical_json(record) + b"\n"


def _publish_run_commit(path: Path, record: dict[str, Any]) -> str:
    try:
        return publish_immutable_bytes(path, _commit_bytes(record))
    except ImmutableConflict:
        if _valid_commit(path, record):
            return "reused_identical_logical_commit"
        raise


def load_metadata_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("rb") as handle:
        data = tomllib.load(handle)
    section = data.get("kalshi_metadata")
    if not isinstance(section, dict):
        raise ValueError("configuration lacks [kalshi_metadata]")
    required = {
        "page_size",
        "max_retries",
        "backoff_base_seconds",
        "backoff_cap_seconds",
        "requests_per_second",
        "timeout_seconds",
        "mve_filter",
    }
    missing = sorted(required - section.keys())
    if missing:
        raise ValueError(f"missing metadata configuration: {', '.join(missing)}")
    return dict(section)


def _cutoff_datetime(payload: dict[str, Any]) -> datetime:
    value = payload.get("market_settled_ts")
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(value, tz=timezone.utc)
    parsed = parse_iso_utc(value)
    if parsed is None and isinstance(value, str) and value.isdigit():
        parsed = datetime.fromtimestamp(int(value), tz=timezone.utc)
    if parsed is None:
        raise ValueError("cutoff market_settled_ts is invalid")
    return parsed


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description=__doc__,
        epilog=(
            "Example: python -m scripts.pipeline_v2.pull_kalshi_settled_metadata "
            "--start-date 2025-07-01 --end-date 2026-06-30 --dry-run"
        ),
    )
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--month")
    parser.add_argument("--raw-root", default=str(DEFAULT_RAW_ROOT))
    parser.add_argument("--manifest")
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--page-size", type=int)
    parser.add_argument("--limit-pages", type=int)
    parser.add_argument("--dry-run", action="store_true")
    resume = parser.add_mutually_exclusive_group()
    resume.add_argument("--resume", dest="resume", action="store_true", default=True)
    resume.add_argument("--no-resume", dest="resume", action="store_false")
    parser.add_argument("--max-retries", type=int)
    parser.add_argument("--backoff-base-seconds", type=float)
    parser.add_argument("--backoff-cap-seconds", type=float)
    parser.add_argument("--requests-per-second", type=float)
    parser.add_argument("--timeout-seconds", type=float)
    parser.add_argument("--cutoff-snapshot")
    parser.add_argument("--historical-mode", choices=("auto", "require", "skip"), default="auto")
    parser.add_argument("--live-mode", choices=("auto", "require", "skip"), default="auto")
    parser.add_argument("--user-agent", default="prediction-market-longshot-bias/metadata-v2")
    return parser


def _validate_args(args: argparse.Namespace) -> None:
    numeric_positive = (
        "page_size",
        "timeout_seconds",
        "requests_per_second",
        "backoff_cap_seconds",
    )
    for name in numeric_positive:
        value = getattr(args, name, None)
        if value is not None and value <= 0:
            raise ValueError(f"--{name.replace('_', '-')} must be positive")
    if args.limit_pages is not None and args.limit_pages <= 0:
        raise ValueError("--limit-pages must be positive")
    if args.max_retries is not None and args.max_retries < 0:
        raise ValueError("--max-retries must be nonnegative")
    if args.backoff_base_seconds is not None and args.backoff_base_seconds < 0:
        raise ValueError("--backoff-base-seconds must be nonnegative")


def run(args: argparse.Namespace, *, session: Any | None = None) -> int:
    _validate_args(args)
    interval = normalize_inclusive_dates(args.start_date, args.end_date)
    months = filter_month(generate_months(interval), args.month)
    config = load_metadata_config(args.config)
    raw_root = Path(args.raw_root)
    manifest_path = Path(args.manifest) if args.manifest else raw_root / "manifest.jsonl"
    cache = MetadataCache(raw_root)

    cutoff_payload: dict[str, Any] | None = None
    cutoff_source = "unresolved"
    if args.cutoff_snapshot:
        cutoff_payload = cache.load_cutoff_snapshot(args.cutoff_snapshot)
        cutoff_source = f"pinned:{args.cutoff_snapshot}"

    if args.dry_run and cutoff_payload is None:
        print(f"normalized_range=[{format_utc(interval.start)}, {format_utc(interval.end)})")
        print("selected_months=" + ",".join(month.month for month in months))
        print("cutoff_source=unresolved (dry-run sends no cutoff request)")
        print("planned_endpoint_segments=unresolved")
        print("cache_state=not inspected without a cutoff namespace")
        print("known_minimum_requests=unresolved")
        print("unknown_historical_request_component=true")
        print(f"page_limit_incomplete={args.limit_pages is not None}")
        return 0

    settings = {
        key: getattr(args, key) if getattr(args, key, None) is not None else config[key]
        for key in (
            "page_size",
            "max_retries",
            "backoff_base_seconds",
            "backoff_cap_seconds",
            "requests_per_second",
            "timeout_seconds",
        )
    }
    if session is None:
        import requests

        session = requests.Session()
        session.headers.update({"User-Agent": args.user_agent})
    client = KalshiMetadataClient(session, **settings)

    if cutoff_payload is None:
        cutoff_payload = client.fetch_cutoff()
        cutoff_source = "fetched"
        cutoff_id, _ = cache.store_cutoff_snapshot(cutoff_payload)
    else:
        cutoff_id = sha256_json(cutoff_payload)[:20]
    cutoff = _cutoff_datetime(cutoff_payload)
    segments = plan_endpoint_segments(
        months,
        cutoff,
        historical_mode=args.historical_mode,
        live_mode=args.live_mode,
    )
    _validate_endpoint_coverage_plan(months, cutoff, segments, args.limit_pages)
    estimate = estimate_requests(segments)
    cache_hits_known = sum(
        1
        for segment in segments
        if cache.pages_dir(segment.tier, cutoff_id, segment.month).exists()
    )

    print(f"normalized_range=[{format_utc(interval.start)}, {format_utc(interval.end)})")
    print("selected_months=" + ",".join(month.month for month in months))
    print(f"cutoff_source={cutoff_source}; cutoff_id={cutoff_id}; cutoff={format_utc(cutoff)}")
    print("planned_endpoint_segments=" + json.dumps([segment.__dict__ for segment in segments], default=format_utc))
    print(f"cache_state=namespaces_present:{cache_hits_known}/{len(segments)}")
    print(f"known_minimum_requests={estimate['known_minimum_requests']}")
    print(f"unknown_historical_request_component={str(estimate['unknown_historical_request_component']).lower()}")
    print(f"page_limit_incomplete={args.limit_pages is not None}")

    if args.dry_run:
        return 0

    request_run_id = uuid.uuid4().hex
    all_markets: list[dict[str, Any]] = []
    all_sources: list[dict[str, Any]] = []
    all_source_pages: list[dict[str, Any]] = []
    incomplete = False
    for segment in segments:
        result = client.paginate(
            segment,
            cache,
            cutoff_id=cutoff_id,
            run_id=request_run_id,
            resume=args.resume,
            limit_pages=args.limit_pages,
            mve_filter=config["mve_filter"],
            manifest_sink=lambda record: append_manifest(manifest_path, record),
        )
        all_markets.extend(result.markets)
        all_sources.extend(record.provenance for record in result.fetched_records)
        all_source_pages.extend(result.page_provenance)
        incomplete = incomplete or not result.complete

    if incomplete:
        print("required data is incomplete", file=sys.stderr)
        return 2

    effective_configuration = _canonical_effective_configuration(
        args, settings, config, interval, months, cutoff_id, segments
    )
    transaction_identity = {
        "schema_version": 1,
        "date_range": {
            "start_utc": format_utc(interval.start),
            "end_utc_exclusive": format_utc(interval.end),
        },
        "selected_months": [month.month for month in months],
        "cutoff_snapshot_id": cutoff_id,
        "effective_configuration": effective_configuration,
    }
    run_id = _transaction_run_id(transaction_identity)
    staging_directory = raw_root / ".staging" / run_id
    staging_directory.mkdir(parents=True, exist_ok=True)
    try:
        invalid_audits, invalid_source_hash = invalid_record_audits(
            all_markets, source_information=all_sources
        )
        if invalid_audits:
            audit_destination = invalid_audit_path(raw_root, invalid_source_hash)
            audit_status = write_derived_jsonl(audit_destination, invalid_audits)
            print(
                f"invalid_market_records={len(invalid_audits)} "
                f"audit={audit_destination} status={audit_status}",
                file=sys.stderr,
            )
            print("run uncommitted: invalid required records", file=sys.stderr)
            return 2

        prepared: list[dict[str, Any]] = []
        monthly_summaries: list[dict[str, Any]] = []
        for month in months:
            consolidated = consolidate_month(
                all_markets, month, source_information=all_sources
            )
            if consolidated.audit_records:
                audit_path = monthly_audit_path(
                    raw_root, month.month, consolidated.source_set_hash
                )
                audit_bytes = serialize_jsonl(consolidated.audit_records)
                prepared.append(
                    {
                        "kind": "conflict_audit",
                        "month": month.month,
                        "path": str(audit_path),
                        "sha256": _bytes_sha256(audit_bytes),
                        "content": audit_bytes,
                    }
                )
            output_path = monthly_output_path(
                raw_root, month.month, consolidated.source_set_hash
            )
            output_bytes = serialize_jsonl(consolidated.records)
            output_sha256 = _bytes_sha256(output_bytes)
            provenance_path = monthly_provenance_path(
                raw_root, month.month, consolidated.source_set_hash
            )
            provenance_records = []
            for entry in consolidated.record_provenance:
                enriched = dict(entry)
                enriched["monthly_output_artifact"] = {
                    "path": str(output_path),
                    "sha256": output_sha256,
                    "source_set_hash": consolidated.source_set_hash,
                }
                provenance_records.append(enriched)
            provenance_bytes = serialize_jsonl(provenance_records)
            prepared.append(
                {
                    "kind": "record_provenance",
                    "month": month.month,
                    "path": str(provenance_path),
                    "sha256": _bytes_sha256(provenance_bytes),
                    "source_set_hash": consolidated.source_set_hash,
                    "content": provenance_bytes,
                }
            )
            prepared.append(
                {
                    "kind": "monthly_consolidation",
                    "month": month.month,
                    "path": str(output_path),
                    "sha256": output_sha256,
                    "source_set_hash": consolidated.source_set_hash,
                    "content": output_bytes,
                }
            )
            monthly_summaries.append(
                {
                    "month": month.month,
                    "rows": len(consolidated.records),
                    "audit_records": len(consolidated.audit_records),
                }
            )

        _validate_prepared_provenance(prepared, months)
        for artifact in prepared:
            validate_immutable_destination(artifact["path"], artifact["content"])
        for index, artifact in enumerate(prepared):
            staged_path = staging_directory / (
                f"{index:04d}_{artifact['month']}_{artifact['kind']}.prepared"
            )
            publish_immutable_bytes(staged_path, artifact["content"])

        source_pages = sorted(
            {
                canonical_json(
                    {key: value for key, value in source.items() if key != "cache_status"}
                ).decode("utf-8"): {
                    key: value for key, value in source.items() if key != "cache_status"
                }
                for source in all_source_pages
            }.values(),
            key=lambda source: canonical_json(source),
        )
        artifact_references = [
            {key: value for key, value in artifact.items() if key != "content"}
            for artifact in prepared
        ]
        commit_base = {
            **transaction_identity,
            "run_id": run_id,
            "artifacts": artifact_references,
            "source_pages": source_pages,
        }
        _validate_commit_structure(commit_base)
        commit_path = _run_commit_path(raw_root, run_id)
        if _valid_commit(commit_path, commit_base):
            print(f"run_complete=true run_id={run_id} commit={commit_path} status=reused_identical")
            return 0

        # Audits are installed first; monthly files remain invisible as complete until commit.
        publication_order = {
            "conflict_audit": 0,
            "record_provenance": 1,
            "monthly_consolidation": 2,
        }
        for artifact in sorted(
            prepared, key=lambda item: (publication_order[item["kind"]], item["month"])
        ):
            publish_immutable_bytes(artifact["path"], artifact["content"])

        commit_record = dict(commit_base)
        _publish_run_commit(commit_path, commit_record)
        if not _valid_commit(commit_path, commit_record):
            raise ConsolidationError(
                "published run commit failed final transaction validation"
            )
        for summary in monthly_summaries:
            print(
                f"month={summary['month']} rows={summary['rows']} "
                f"audit_records={summary['audit_records']} committed=true"
            )
        print(f"run_complete=true run_id={run_id} commit={commit_path}")
        print("counters=" + json.dumps(client.counters.__dict__, sort_keys=True))
        return 0
    finally:
        shutil.rmtree(staging_directory, ignore_errors=True)


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    try:
        return run(parser.parse_args(argv))
    except (ValueError, CacheError, ConsolidationError, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
