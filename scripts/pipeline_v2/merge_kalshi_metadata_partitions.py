"""Validate and deterministically merge committed Kalshi metadata partitions."""

from __future__ import annotations

import argparse
from collections import defaultdict
import hashlib
import json
from pathlib import Path
import sys
from typing import Any, Mapping, Sequence

from scripts.common.time_utils import parse_iso_utc
from scripts.pipeline_v2.kalshi_metadata_cache import (
    CacheError,
    MetadataCache,
    StorageBudget,
    canonical_json,
    sha256_json,
)
from scripts.pipeline_v2.kalshi_metadata_consolidation import ConsolidationConflict
from scripts.pipeline_v2.kalshi_metadata_planner import (
    generate_months,
    normalize_inclusive_dates,
    plan_endpoint_segments,
)
from scripts.pipeline_v2.prepare_kalshi_market_universe import (
    EVENT_FIELDS,
    METADATA_FIELDS,
    OUTCOME_FIELDS,
    _csv_bytes,
    _event_rows,
    _source_identity,
)
from scripts.pipeline_v2.pull_kalshi_partitioned_metadata import (
    SCHEMA_VERSION,
    _load_partition_settings,
    _publish_budgeted,
    _read_gzip_jsonl,
    _segment_record,
    _sha256,
    _valid_partition_commit,
    load_partition_chain,
    segment_id,
)
from scripts.pipeline_v2.pull_kalshi_settled_metadata import (
    DEFAULT_CONFIG,
    _cutoff_datetime,
)


INCOMPLETE_EXIT = 3


def _valid_merge_commit(path: Path) -> bool:
    try:
        record = json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return False
    if not isinstance(record, dict) or not record.get("merge_id"):
        return False
    partition_paths = record.get("partition_commits")
    artifacts = record.get("artifacts")
    if not isinstance(partition_paths, list) or not isinstance(artifacts, list):
        return False
    if not partition_paths or not artifacts:
        return False
    if any(not _valid_partition_commit(Path(item)) for item in partition_paths):
        return False
    for artifact in artifacts:
        artifact_path = Path(str(artifact.get("path") or ""))
        if not artifact_path.is_file() or _sha256(
            artifact_path.read_bytes()
        ) != artifact.get("sha256"):
            return False
    reports = [item for item in artifacts if item.get("kind") == "merge_report.json"]
    if len(reports) != 1:
        return False
    try:
        report = json.loads(Path(reports[0]["path"]).read_text(encoding="utf-8"))
    except Exception:
        return False
    return bool(report.get("merge_complete") and report.get("final_universe_published"))


def _artifact(commit: Mapping[str, Any], kind: str) -> Path:
    matches = [item for item in commit.get("artifacts", []) if item.get("kind") == kind]
    if len(matches) != 1:
        raise CacheError(f"partition does not have exactly one {kind} artifact")
    path = Path(str(matches[0].get("path") or ""))
    if not path.is_file() or _sha256(path.read_bytes()) != matches[0].get("sha256"):
        raise CacheError(f"partition {kind} artifact is missing or corrupt")
    return path


def _publish_report(
    raw_root: Path, budget: StorageBudget, report: Mapping[str, Any]
) -> Path:
    content = canonical_json(dict(report)) + b"\n"
    digest = hashlib.sha256(content).hexdigest()[:24]
    path = raw_root / "merge_reports" / f"merge_report_{digest}.json"
    _publish_budgeted(budget, path, content)
    return path


def _collect_chains(
    raw_root: Path, interval: Any, cutoff_payload: Mapping[str, Any]
) -> tuple[list[Any], list[list[dict[str, Any]]], list[dict[str, Any]]]:
    cutoff_id = sha256_json(cutoff_payload)[:20]
    cutoff = _cutoff_datetime(dict(cutoff_payload))
    segments = list(plan_endpoint_segments(generate_months(interval), cutoff))
    chains: list[list[dict[str, Any]]] = []
    state: list[dict[str, Any]] = []
    for segment in segments:
        sid = segment_id(segment, cutoff_id)
        chain = load_partition_chain(raw_root, sid)
        chains.append(chain)
        state.append(
            {
                **_segment_record(segment),
                "segment_id": sid,
                "committed_partition_count": len(chain),
                "archive_complete": bool(chain and chain[-1].get("archive_complete")),
                "rejected_record_count": sum(
                    int(
                        item.get("normalization_summary", {}).get(
                            "rejected_record_count", 0
                        )
                    )
                    for item in chain
                ),
            }
        )
    return segments, chains, state


def _merge_rows(
    chains: Sequence[Sequence[Mapping[str, Any]]]
) -> tuple[list[dict[str, Any]], list[dict[str, Any]], list[dict[str, Any]]]:
    metadata_variants: dict[str, dict[str, dict[str, Any]]] = defaultdict(dict)
    outcomes: dict[tuple[str, str], dict[bytes, dict[str, Any]]] = defaultdict(dict)
    provenance: dict[tuple[str, str], dict[bytes, dict[str, Any]]] = defaultdict(dict)
    for chain in chains:
        for commit in chain:
            for wrapper in _read_gzip_jsonl(_artifact(commit, "metadata")):
                metadata = wrapper.get("metadata")
                digest = str(wrapper.get("metadata_sha256") or "")
                if not isinstance(metadata, dict) or sha256_json(metadata) != digest:
                    raise CacheError("normalized metadata hash mismatch")
                ticker = str(metadata.get("ticker") or "").strip()
                if not ticker:
                    raise CacheError("normalized metadata lacks ticker")
                existing = metadata_variants[ticker].get(digest)
                if existing is not None and existing != metadata:
                    raise CacheError("normalized metadata hash collision")
                metadata_variants[ticker][digest] = dict(metadata)
            for wrapper in _read_gzip_jsonl(_artifact(commit, "outcomes")):
                outcome = wrapper.get("outcome")
                digest = str(wrapper.get("metadata_sha256") or "")
                if not isinstance(outcome, dict):
                    raise CacheError("normalized outcome is malformed")
                ticker = str(outcome.get("ticker") or "").strip()
                outcomes[(ticker, digest)][canonical_json(outcome)] = dict(outcome)
            for row in _read_gzip_jsonl(_artifact(commit, "provenance")):
                ticker = str(row.get("ticker") or "").strip()
                digest = str(row.get("metadata_sha256") or "")
                provenance[(ticker, digest)][canonical_json(row)] = dict(row)

    metadata_keys = {
        (ticker, digest)
        for ticker, variants in metadata_variants.items()
        for digest in variants
    }
    if set(outcomes) != metadata_keys:
        raise CacheError("normalized outcomes do not exactly cover normalized metadata")
    if set(provenance) != metadata_keys:
        raise CacheError(
            "normalized provenance does not exactly cover normalized metadata"
        )

    selected_metadata: list[dict[str, Any]] = []
    selected_outcomes: list[dict[str, Any]] = []
    selected_provenance: list[dict[str, Any]] = []
    for ticker in sorted(metadata_variants):
        variants = metadata_variants[ticker]
        if len(variants) == 1:
            chosen_digest = next(iter(variants))
        else:
            ranked = sorted(
                (
                    (parse_iso_utc(metadata.get("updated_time")), digest)
                    for digest, metadata in variants.items()
                ),
                key=lambda item: (
                    item[0] is not None,
                    item[0] or parse_iso_utc("1970-01-01T00:00:00Z"),
                    item[1],
                ),
            )
            if ranked[-1][0] is None or ranked[-2][0] == ranked[-1][0]:
                raise ConsolidationConflict(
                    f"ticker {ticker!r} has unresolved outcome-free metadata variants"
                )
            chosen_digest = ranked[-1][1]
        key = (ticker, chosen_digest)
        outcome_variants = outcomes.get(key, {})
        if len(outcome_variants) != 1:
            raise ConsolidationConflict(
                f"ticker {ticker!r} lacks one unambiguous quarantined outcome for selected metadata"
            )
        sources = sorted(provenance.get(key, {}).values(), key=canonical_json)
        if not sources:
            raise CacheError(
                f"ticker {ticker!r} lacks provenance for selected metadata"
            )
        selected_metadata.append(dict(variants[chosen_digest]))
        selected_outcomes.append(dict(next(iter(outcome_variants.values()))))
        research_projection = dict(variants[chosen_digest])
        research_projection.pop("diagnostic_settlement_ts", None)
        selected_provenance.append(
            {
                "ticker": ticker,
                "research_metadata_sha256": sha256_json(research_projection),
                "source_associations": [
                    _source_identity(row["source_association"]) for row in sources
                ],
            }
        )
    return selected_metadata, selected_outcomes, selected_provenance


def run(args: argparse.Namespace) -> int:
    interval = normalize_inclusive_dates(args.start_date, args.end_date)
    cutoff_payload = MetadataCache.load_cutoff_snapshot(args.cutoff_snapshot)
    raw_root = Path(args.raw_root)
    settings = _load_partition_settings(Path(args.config))
    budget = StorageBudget(
        raw_root,
        max_bytes=args.max_raw_bytes or settings["max_raw_bytes"],
        min_free_bytes=(
            args.min_free_bytes
            if args.min_free_bytes is not None
            else settings["min_free_bytes"]
        ),
    )
    _, chains, state = _collect_chains(raw_root, interval, cutoff_payload)
    incomplete = [item for item in state if not item["archive_complete"]]
    rejected = [item for item in state if item["rejected_record_count"]]
    if incomplete or rejected:
        report = {
            "schema_version": SCHEMA_VERSION,
            "merge_complete": False,
            "final_universe_published": False,
            "reason": (
                "normalization_rejects_present"
                if rejected
                else "partition_chains_incomplete"
            ),
            "segment_state": state,
            "outcome_quarantine_enabled": True,
        }
        path = _publish_report(raw_root, budget, report)
        print(json.dumps({**report, "report": str(path)}, sort_keys=True))
        return INCOMPLETE_EXIT

    metadata, outcomes, provenance = _merge_rows(chains)
    events = _event_rows(metadata)
    contents = {
        "market_metadata.csv": _csv_bytes(metadata, METADATA_FIELDS),
        "market_outcomes.csv": _csv_bytes(outcomes, OUTCOME_FIELDS),
        "event_tickers.csv": _csv_bytes(events, EVENT_FIELDS),
        "market_source_provenance.jsonl": b"".join(
            canonical_json(row) + b"\n" for row in provenance
        ),
    }
    merge_identity = {
        "schema_version": SCHEMA_VERSION,
        "requested_range": {
            "start_date": args.start_date,
            "end_date": args.end_date,
        },
        "cutoff_snapshot_id": sha256_json(cutoff_payload)[:20],
        "partition_commits": [
            commit["_commit_path"] for chain in chains for commit in chain
        ],
        "metadata_sha256": _sha256(contents["market_metadata.csv"]),
        "outcomes_sha256": _sha256(contents["market_outcomes.csv"]),
        "provenance_sha256": _sha256(contents["market_source_provenance.jsonl"]),
    }
    merge_id = hashlib.sha256(canonical_json(merge_identity)).hexdigest()[:24]
    output_dir = raw_root / "merged_universes" / merge_id
    artifacts = []
    for name in (
        "market_metadata.csv",
        "market_outcomes.csv",
        "event_tickers.csv",
        "market_source_provenance.jsonl",
    ):
        path = output_dir / name
        content = contents[name]
        _publish_budgeted(budget, path, content)
        artifacts.append({"kind": name, "path": str(path), "sha256": _sha256(content)})
    report = {
        "schema_version": SCHEMA_VERSION,
        "merge_complete": True,
        "final_universe_published": True,
        "merge_id": merge_id,
        "contract_count": len(metadata),
        "event_count": len(events),
        "segment_state": state,
        "outcome_quarantine_enabled": True,
        "outcomes_merged_into_metadata": False,
        "artifacts": artifacts,
    }
    report_content = canonical_json(report) + b"\n"
    report_path = output_dir / "merge_report.json"
    _publish_budgeted(budget, report_path, report_content)
    commit = {
        **merge_identity,
        "merge_id": merge_id,
        "artifacts": artifacts
        + [
            {
                "kind": "merge_report.json",
                "path": str(report_path),
                "sha256": _sha256(report_content),
            }
        ],
    }
    commit_path = raw_root / "merge_commits" / f"merge_{merge_id}.json"
    _publish_budgeted(budget, commit_path, canonical_json(commit) + b"\n")
    if not _valid_merge_commit(commit_path):
        raise CacheError("published merge commit failed final validation")
    print(json.dumps({**report, "merge_commit": str(commit_path)}, sort_keys=True))
    return 0


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--start-date", required=True)
    parser.add_argument("--end-date", required=True)
    parser.add_argument("--raw-root", required=True)
    parser.add_argument("--cutoff-snapshot", required=True)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--max-raw-bytes", type=int)
    parser.add_argument("--min-free-bytes", type=int)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return run(build_parser().parse_args(argv))
    except (ValueError, CacheError, ConsolidationConflict, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
