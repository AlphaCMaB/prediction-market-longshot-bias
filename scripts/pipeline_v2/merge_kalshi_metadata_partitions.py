"""Validate and deterministically merge committed Kalshi metadata partitions."""

from __future__ import annotations

import argparse
from collections import defaultdict
import csv
import gzip
import hashlib
import io
import json
import os
from pathlib import Path
import shutil
import subprocess
import sys
import tempfile
from typing import Any, Mapping, Sequence
import uuid
import zlib

from scripts.common.time_utils import format_iso_utc, parse_iso_utc
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


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024**2), b""):
            digest.update(chunk)
    return digest.hexdigest()


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
        if not artifact_path.is_file() or _sha256_file(artifact_path) != artifact.get(
            "sha256"
        ):
            return False
        if artifact.get("compression") == "gzip":
            try:
                with gzip.open(artifact_path, "rb") as handle:
                    while handle.read(1024**2):
                        pass
            except Exception:
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


def _ticker_audit(
    chains: Sequence[Sequence[Mapping[str, Any]]], budget: StorageBudget
) -> dict[str, Any]:
    expected_rows = sum(
        int(commit.get("normalization_summary", {}).get("in_range_record_count", 0))
        for chain in chains
        for commit in chain
    )
    estimated_temporary_bytes = expected_rows * 128
    budget.check_additional(estimated_temporary_bytes)
    with tempfile.TemporaryDirectory(
        prefix="kalshi-ticker-audit.", dir="/private/tmp"
    ) as temporary:
        temporary_path = Path(temporary)
        unsorted_path = temporary_path / "tickers.txt"
        sorted_path = temporary_path / "tickers.sorted.txt"
        input_rows = 0
        with unsorted_path.open("wb") as handle:
            for chain in chains:
                for commit in chain:
                    for wrapper in _read_gzip_jsonl(_artifact(commit, "metadata")):
                        metadata = wrapper.get("metadata")
                        digest = str(wrapper.get("metadata_sha256") or "")
                        if (
                            not isinstance(metadata, dict)
                            or sha256_json(metadata) != digest
                        ):
                            raise CacheError("normalized metadata hash mismatch")
                        ticker = str(metadata.get("ticker") or "").strip()
                        if not ticker or "\n" in ticker or "\r" in ticker:
                            raise CacheError(
                                "normalized metadata has an invalid ticker"
                            )
                        handle.write(ticker.encode("utf-8") + b"\n")
                        input_rows += 1
        if input_rows != expected_rows:
            raise CacheError(
                "ticker audit row count does not match partition summaries"
            )
        environment = dict(os.environ)
        environment.update({"LC_ALL": "C", "TMPDIR": temporary})
        try:
            subprocess.run(
                ["sort", str(unsorted_path), "-o", str(sorted_path)],
                check=True,
                env=environment,
                stdout=subprocess.DEVNULL,
                stderr=subprocess.PIPE,
            )
        except (OSError, subprocess.CalledProcessError) as exc:
            raise CacheError("exact ticker sort audit failed") from exc

        unique_tickers = 0
        duplicate_tickers = 0
        excess_rows = 0
        maximum_occurrences = 0
        previous: bytes | None = None
        occurrences = 0
        sorted_digest = hashlib.sha256()
        with sorted_path.open("rb") as handle:
            for line in handle:
                sorted_digest.update(line)
                ticker = line.rstrip(b"\n")
                if ticker == previous:
                    occurrences += 1
                    continue
                if previous is not None:
                    unique_tickers += 1
                    if occurrences > 1:
                        duplicate_tickers += 1
                        excess_rows += occurrences - 1
                        maximum_occurrences = max(maximum_occurrences, occurrences)
                previous = ticker
                occurrences = 1
        if previous is not None:
            unique_tickers += 1
            if occurrences > 1:
                duplicate_tickers += 1
                excess_rows += occurrences - 1
                maximum_occurrences = max(maximum_occurrences, occurrences)
    return {
        "input_rows": input_rows,
        "unique_tickers": unique_tickers,
        "duplicate_tickers": duplicate_tickers,
        "excess_rows": excess_rows,
        "maximum_occurrences": maximum_occurrences,
        "sorted_ticker_sha256": sorted_digest.hexdigest(),
        "temporary_byte_estimate": estimated_temporary_bytes,
    }


class _BudgetedGzipSink:
    def __init__(self, path: Path, budget: StorageBudget):
        self.path = path
        self.budget = budget
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.handle = self.path.open("xb")
        self.compressor = zlib.compressobj(9, zlib.DEFLATED, 31)
        self.digest = hashlib.sha256()
        self.bytes_written = 0

    def _write_compressed(self, content: bytes) -> None:
        if not content:
            return
        self.budget.check_additional(len(content))
        self.handle.write(content)
        self.digest.update(content)
        self.bytes_written += len(content)

    def write(self, content: bytes) -> None:
        self._write_compressed(self.compressor.compress(content))

    def close(self) -> dict[str, Any]:
        self._write_compressed(self.compressor.flush())
        self.handle.flush()
        os.fsync(self.handle.fileno())
        self.handle.close()
        return {
            "path": str(self.path),
            "sha256": self.digest.hexdigest(),
            "bytes": self.bytes_written,
            "compression": "gzip",
        }


def _csv_chunk(
    rows: Sequence[Mapping[str, Any]],
    fields: tuple[str, ...],
    *,
    include_header: bool,
) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(
        stream, fieldnames=fields, lineterminator="\n", extrasaction="ignore"
    )
    if include_header:
        writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


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


def _stream_unique_merge(
    *,
    raw_root: Path,
    budget: StorageBudget,
    chains: Sequence[Sequence[Mapping[str, Any]]],
    state: Sequence[Mapping[str, Any]],
    audit: Mapping[str, Any],
    args: argparse.Namespace,
    cutoff_payload: Mapping[str, Any],
) -> dict[str, Any]:
    if audit["duplicate_tickers"] or audit["excess_rows"]:
        raise ConsolidationConflict(
            "streaming merge requires the exact zero-duplicate ticker audit"
        )
    work_dir = raw_root / "merge_work" / uuid.uuid4().hex
    metadata_sink = _BudgetedGzipSink(work_dir / "market_metadata.csv.gz", budget)
    outcomes_sink = _BudgetedGzipSink(work_dir / "market_outcomes.csv.gz", budget)
    event_state: dict[str, tuple[int, Any | None]] = {}
    events_sink: _BudgetedGzipSink | None = None
    processed = 0
    first_chunk = True
    partition_provenance = []
    try:
        for chain in chains:
            for commit in chain:
                metadata_by_key: dict[tuple[str, str], dict[str, Any]] = {}
                for wrapper in _read_gzip_jsonl(_artifact(commit, "metadata")):
                    metadata = wrapper.get("metadata")
                    digest = str(wrapper.get("metadata_sha256") or "")
                    if (
                        not isinstance(metadata, dict)
                        or sha256_json(metadata) != digest
                    ):
                        raise CacheError("normalized metadata hash mismatch")
                    ticker = str(metadata.get("ticker") or "").strip()
                    key = (ticker, digest)
                    if not ticker or key in metadata_by_key:
                        raise ConsolidationConflict(
                            "duplicate metadata key inside a committed partition"
                        )
                    metadata_by_key[key] = dict(metadata)

                outcomes_by_key: dict[tuple[str, str], dict[str, Any]] = {}
                for wrapper in _read_gzip_jsonl(_artifact(commit, "outcomes")):
                    outcome = wrapper.get("outcome")
                    digest = str(wrapper.get("metadata_sha256") or "")
                    if not isinstance(outcome, dict):
                        raise CacheError("normalized outcome is malformed")
                    ticker = str(outcome.get("ticker") or "").strip()
                    key = (ticker, digest)
                    if key in outcomes_by_key:
                        raise ConsolidationConflict(
                            "multiple outcomes for one metadata key"
                        )
                    outcomes_by_key[key] = dict(outcome)

                provenance_keys: set[tuple[str, str]] = set()
                provenance_artifact = _artifact(commit, "provenance")
                for row in _read_gzip_jsonl(provenance_artifact):
                    key = (
                        str(row.get("ticker") or "").strip(),
                        str(row.get("metadata_sha256") or ""),
                    )
                    if key in provenance_keys:
                        raise ConsolidationConflict(
                            "multiple provenance rows for one metadata key"
                        )
                    provenance_keys.add(key)
                keys = set(metadata_by_key)
                if set(outcomes_by_key) != keys or provenance_keys != keys:
                    raise CacheError(
                        "partition outcome/provenance coverage differs from metadata"
                    )

                ordered_keys = sorted(keys, key=lambda item: (item[0], item[1]))
                metadata_rows = [metadata_by_key[key] for key in ordered_keys]
                outcome_rows = [outcomes_by_key[key] for key in ordered_keys]
                metadata_sink.write(
                    _csv_chunk(
                        metadata_rows,
                        METADATA_FIELDS,
                        include_header=first_chunk,
                    )
                )
                outcomes_sink.write(
                    _csv_chunk(
                        outcome_rows,
                        OUTCOME_FIELDS,
                        include_header=first_chunk,
                    )
                )
                first_chunk = False
                processed += len(metadata_rows)
                for metadata in metadata_rows:
                    event = str(metadata.get("event_ticker") or "").strip()
                    if not event:
                        continue
                    opened = parse_iso_utc(metadata.get("open_time"))
                    count, earliest = event_state.get(event, (0, None))
                    if opened is not None and (earliest is None or opened < earliest):
                        earliest = opened
                    event_state[event] = (count + 1, earliest)

                provenance_entry = next(
                    item
                    for item in commit["artifacts"]
                    if item.get("kind") == "provenance"
                )
                partition_provenance.append(
                    {
                        "partition_commit": commit["_commit_path"],
                        "partition_commit_sha256": _sha256(
                            Path(commit["_commit_path"]).read_bytes()
                        ),
                        "provenance_artifact": provenance_entry,
                    }
                )

        if processed != audit["input_rows"]:
            raise CacheError("streaming merge count differs from exact ticker audit")
        metadata_reference = metadata_sink.close()
        outcomes_reference = outcomes_sink.close()

        events_sink = _BudgetedGzipSink(work_dir / "event_tickers.csv.gz", budget)
        event_rows = []
        for event in sorted(event_state):
            count, earliest = event_state[event]
            event_rows.append(
                {
                    "event_ticker": event,
                    "contract_count": count,
                    "first_open_time": (
                        format_iso_utc(earliest) if earliest is not None else ""
                    ),
                }
            )
            if len(event_rows) == 10_000:
                events_sink.write(
                    _csv_chunk(
                        event_rows,
                        EVENT_FIELDS,
                        include_header=events_sink.bytes_written == 0,
                    )
                )
                event_rows.clear()
        if event_rows or not event_state:
            events_sink.write(
                _csv_chunk(
                    event_rows,
                    EVENT_FIELDS,
                    include_header=events_sink.bytes_written == 0,
                )
            )
        events_reference = events_sink.close()

        provenance_manifest = {
            "schema_version": SCHEMA_VERSION,
            "resolution": "zero_duplicate_identity_preserves_partition_provenance",
            "ticker_audit": dict(audit),
            "partition_provenance": partition_provenance,
        }
        provenance_content = canonical_json(provenance_manifest) + b"\n"
        provenance_path = work_dir / "source_provenance_manifest.json"
        _publish_budgeted(budget, provenance_path, provenance_content)
        provenance_reference = {
            "path": str(provenance_path),
            "sha256": _sha256(provenance_content),
            "bytes": len(provenance_content),
            "compression": "none",
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
            "ticker_audit": dict(audit),
            "metadata_sha256": metadata_reference["sha256"],
            "outcomes_sha256": outcomes_reference["sha256"],
            "events_sha256": events_reference["sha256"],
            "provenance_sha256": provenance_reference["sha256"],
            "streaming_compressed_merge": True,
        }
        merge_id = hashlib.sha256(canonical_json(merge_identity)).hexdigest()[:24]
        output_dir = raw_root / "merged_universes" / merge_id
        artifact_specs = [
            ("market_metadata.csv.gz", metadata_reference),
            ("market_outcomes.csv.gz", outcomes_reference),
            ("event_tickers.csv.gz", events_reference),
            ("source_provenance_manifest.json", provenance_reference),
        ]
        artifacts = []
        for kind, reference in artifact_specs:
            source = Path(reference["path"])
            destination = output_dir / kind
            destination.parent.mkdir(parents=True, exist_ok=True)
            if destination.exists():
                if _sha256_file(destination) != reference["sha256"]:
                    raise CacheError("conflicting immutable merge artifact")
                source.unlink()
            else:
                os.replace(source, destination)
            artifacts.append(
                {
                    "kind": kind,
                    "path": str(destination),
                    "sha256": reference["sha256"],
                    "bytes": reference["bytes"],
                    "compression": reference["compression"],
                }
            )
        work_dir.rmdir()
        return {
            "merge_identity": merge_identity,
            "merge_id": merge_id,
            "artifacts": artifacts,
            "contract_count": processed,
            "event_count": len(event_state),
            "ticker_audit": dict(audit),
            "segment_state": list(state),
        }
    except Exception:
        for sink in (metadata_sink, outcomes_sink, events_sink):
            if sink is not None and not sink.handle.closed:
                sink.handle.close()
        if work_dir.exists():
            shutil.rmtree(work_dir)
        raise


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

    input_rows = sum(
        int(commit.get("normalization_summary", {}).get("in_range_record_count", 0))
        for chain in chains
        for commit in chain
    )
    if input_rows >= args.streaming_threshold:
        audit = _ticker_audit(chains, budget)
        if audit["duplicate_tickers"]:
            raise ConsolidationConflict(
                "exact ticker audit found duplicates; streaming merge stopped"
            )
        streamed = _stream_unique_merge(
            raw_root=raw_root,
            budget=budget,
            chains=chains,
            state=state,
            audit=audit,
            args=args,
            cutoff_payload=cutoff_payload,
        )
        report = {
            "schema_version": SCHEMA_VERSION,
            "merge_complete": True,
            "final_universe_published": True,
            "merge_id": streamed["merge_id"],
            "contract_count": streamed["contract_count"],
            "event_count": streamed["event_count"],
            "segment_state": state,
            "ticker_audit": audit,
            "duplicate_ticker_count": 0,
            "metadata_conflict_count": 0,
            "outcome_quarantine_enabled": True,
            "outcomes_merged_into_metadata": False,
            "streaming_compressed_merge": True,
            "row_provenance_preserved_in_partition_artifacts": True,
            "artifacts": streamed["artifacts"],
        }
        report_content = canonical_json(report) + b"\n"
        report_path = (
            raw_root / "merged_universes" / streamed["merge_id"] / "merge_report.json"
        )
        _publish_budgeted(budget, report_path, report_content)
        artifacts = streamed["artifacts"] + [
            {
                "kind": "merge_report.json",
                "path": str(report_path),
                "sha256": _sha256(report_content),
                "bytes": len(report_content),
                "compression": "none",
            }
        ]
        commit = {
            **streamed["merge_identity"],
            "merge_id": streamed["merge_id"],
            "artifacts": artifacts,
        }
        commit_path = raw_root / "merge_commits" / f"merge_{streamed['merge_id']}.json"
        _publish_budgeted(budget, commit_path, canonical_json(commit) + b"\n")
        if not _valid_merge_commit(commit_path):
            raise CacheError("published streaming merge commit failed final validation")
        print(json.dumps({**report, "merge_commit": str(commit_path)}, sort_keys=True))
        return 0

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
    parser.add_argument("--streaming-threshold", type=int, default=1_000_000)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        return run(build_parser().parse_args(argv))
    except (ValueError, CacheError, ConsolidationConflict, RuntimeError) as exc:
        print(f"error: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
