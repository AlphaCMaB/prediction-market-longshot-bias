"""Build deterministic, unverified Kalshi anchor-candidate review artifacts.

Invoke as ``python -m scripts.pipeline_v2.build_kalshi_anchor_evidence``.
"""

from __future__ import annotations

import argparse
from collections import defaultdict
import csv
from datetime import timezone
import hashlib
import json
import os
from pathlib import Path
import shutil
import sys
import tempfile
from typing import Any, Iterable, Mapping

from scripts.common.io_utils import open_csv_dict_reader, read_csv_with_header
from scripts.common.time_utils import format_iso_utc, parse_iso_utc
from scripts.pipeline_v2.anchor_evidence import (
    ANCHOR_EVIDENCE_FIELDS,
    ANCHOR_FAMILY_REVIEW_FIELDS,
    DECISION_TEMPLATE_FIELDS,
    EVIDENCE_SCHEMA_VERSION,
    EVENT_REQUIRED_FIELDS,
    MARKET_REQUIRED_FIELDS,
    MILESTONE_REQUIRED_FIELDS,
    build_anchor_evidence,
    family_identity,
    parse_candidate_value,
)
from scripts.pipeline_v2.kalshi_metadata_cache import canonical_json
from scripts.pipeline_v2.study_rules import (
    load_study_rules,
    validate_research_feature_columns,
)


DEFAULT_CONFIG = Path("configs/pipeline_v2.toml")
STREAMING_MARKET_THRESHOLD_BYTES = 64 * 1024**2
PRODUCTION_PIN_THRESHOLD_BYTES = 64 * 1024**2
DEFAULT_MAX_GENERATED_BYTES = 5 * 1024**3
DEFAULT_MIN_FREE_BYTES = 80 * 1024**3
CSV_OUTPUT_NAMES = frozenset(
    {
        "anchor_evidence.csv",
        "anchor_family_review.csv",
        "anchor_verification_decisions_template.csv",
    }
)


def _normalized_open_time(value: Any) -> str:
    parsed = parse_iso_utc(value)
    if parsed is None:
        return ""
    return format_iso_utc(parsed.astimezone(timezone.utc)).replace("+00:00", "Z")


def _compact_market_input(
    market_path: Path,
) -> tuple[list[dict[str, Any]], tuple[str, ...], int]:
    """Stream a large market universe into family/candidate sufficient rows."""
    families: dict[tuple[str, str], dict[str, Any]] = {}
    occurrence_groups: dict[tuple[str, str], dict[tuple[str, str], dict[str, Any]]] = (
        defaultdict(dict)
    )
    row_count = 0
    with open_csv_dict_reader(market_path) as (reader, header):
        _require_columns(header, MARKET_REQUIRED_FIELDS, "market metadata")
        validate_research_feature_columns(header)
        for raw_row in reader:
            row = dict(raw_row)
            row_count += 1
            identity = family_identity(row)
            event_ticker = str(row.get("event_ticker") or "").strip()
            source_ticker = str(row.get("ticker") or row.get("market_id") or "").strip()
            if not source_ticker:
                raise ValueError("market metadata row requires ticker or market_id")
            state = families.get(identity)
            representative_key = (event_ticker, source_ticker)
            if state is None:
                state = {
                    "market_count": 0,
                    "event_tickers": set(),
                    "representative_key": representative_key,
                    "representative": row,
                    "first_market_open_time": "",
                    "invalid_candidate_value_count": 0,
                    "sentinel_timestamp_count": 0,
                }
                families[identity] = state
            state["market_count"] += 1
            if event_ticker:
                state["event_tickers"].add(event_ticker)
            if representative_key < state["representative_key"]:
                state["representative_key"] = representative_key
                state["representative"] = row
            opened = _normalized_open_time(
                row.get("market_open_time") or row.get("open_time")
            )
            if opened and (
                not state["first_market_open_time"]
                or opened < state["first_market_open_time"]
            ):
                state["first_market_open_time"] = opened

            parsed = parse_candidate_value(
                row.get("occurrence_datetime"), allow_date_only=False
            )
            if not parsed.valid:
                if parsed.issue == "invalid_candidate_value":
                    state["invalid_candidate_value_count"] += 1
                elif parsed.issue == "sentinel_timestamp":
                    state["sentinel_timestamp_count"] += 1
                continue
            group_key = (event_ticker, parsed.candidate_time_utc)
            group = occurrence_groups[identity].get(group_key)
            if group is None:
                occurrence_groups[identity][group_key] = {
                    "representative_key": representative_key,
                    "representative": row,
                    "original_value": parsed.original_value,
                    "supporting_source_count": 1,
                    "first_ticker": source_ticker,
                    "last_ticker": source_ticker,
                }
            else:
                group["supporting_source_count"] += 1
                group["first_ticker"] = min(group["first_ticker"], source_ticker)
                group["last_ticker"] = max(group["last_ticker"], source_ticker)
                group["original_value"] = min(
                    group["original_value"], parsed.original_value
                )
                if representative_key < group["representative_key"]:
                    group["representative_key"] = representative_key
                    group["representative"] = row

    compacted: list[dict[str, Any]] = []
    for identity in sorted(families, key=lambda item: (item[1], item[0])):
        state = families[identity]
        grouped_events: dict[str, list[dict[str, Any]]] = defaultdict(list)
        for (event_ticker, _), group in occurrence_groups.get(identity, {}).items():
            grouped_events[event_ticker].append(group)
        for event_ticker in sorted(state["event_tickers"]):
            groups = sorted(
                grouped_events.get(event_ticker, ()),
                key=lambda item: (
                    str(item["representative"].get("occurrence_datetime") or ""),
                    item["representative_key"],
                ),
            )
            if not groups:
                groups = [
                    {
                        "representative": state["representative"],
                        "original_value": "",
                        "supporting_source_count": 0,
                        "first_ticker": "",
                        "last_ticker": "",
                    }
                ]
            for group in groups:
                synthetic = dict(group["representative"])
                synthetic["event_ticker"] = event_ticker
                synthetic["occurrence_datetime"] = group["original_value"]
                synthetic["_supporting_source_count"] = group["supporting_source_count"]
                synthetic["_first_supporting_market_ticker"] = group["first_ticker"]
                synthetic["_last_supporting_market_ticker"] = group["last_ticker"]
                synthetic["_family_market_count"] = state["market_count"]
                synthetic["_family_representative_title"] = str(
                    state["representative"].get("title") or ""
                )
                synthetic["_family_first_market_open_time"] = state[
                    "first_market_open_time"
                ]
                synthetic["_family_invalid_candidate_value_count"] = state[
                    "invalid_candidate_value_count"
                ]
                synthetic["_family_sentinel_timestamp_count"] = state[
                    "sentinel_timestamp_count"
                ]
                compacted.append(synthetic)
    return compacted, header, row_count


def _require_columns(
    actual: Iterable[str],
    required: Iterable[str],
    label: str,
) -> tuple[str, ...]:
    columns = tuple(actual)
    missing = sorted(set(required) - set(columns))
    if missing:
        raise ValueError(f"{label} is missing required columns: {', '.join(missing)}")
    return columns


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024**2), b""):
            digest.update(chunk)
    return digest.hexdigest()


class _OutputGuard:
    def __init__(self, root: Path, *, max_bytes: int, min_free_bytes: int):
        self.root = root
        self.root.mkdir(parents=True, exist_ok=True)
        self.initial_used = sum(
            path.stat().st_size for path in root.rglob("*") if path.is_file()
        )
        self.max_bytes = max_bytes
        self.min_free_bytes = min_free_bytes
        self.additional_bytes = 0
        self._next_disk_check = 0
        self.reserve(0)

    def reserve(self, additional: int) -> None:
        projected = self.initial_used + self.additional_bytes + additional
        if projected > self.max_bytes:
            raise ValueError(
                "Phase 10D output would exceed the generated namespace ceiling: "
                f"projected={projected} maximum={self.max_bytes}"
            )
        if self.additional_bytes + additional >= self._next_disk_check:
            free = shutil.disk_usage(self.root).free
            if free - additional < self.min_free_bytes:
                raise ValueError(
                    "Phase 10D output would cross the free-disk floor: "
                    f"free={free} additional={additional} minimum={self.min_free_bytes}"
                )
            self._next_disk_check = self.additional_bytes + additional + 8 * 1024**2
        self.additional_bytes += additional

    def snapshot(self) -> dict[str, int]:
        free = shutil.disk_usage(self.root).free
        return {
            "used_bytes_before_run": self.initial_used,
            "generated_bytes_this_run": self.additional_bytes,
            "projected_namespace_bytes": self.initial_used + self.additional_bytes,
            "max_generated_bytes": self.max_bytes,
            "free_bytes": free,
            "min_free_bytes": self.min_free_bytes,
            "free_space_margin_bytes": free - self.min_free_bytes,
        }


class _HashingTextSink:
    def __init__(self, path: Path | None, guard: _OutputGuard | None):
        self.path = path
        self.guard = guard
        self.digest = hashlib.sha256()
        self.byte_count = 0
        self.handle = path.open("wb") if path is not None else None

    def write(self, text: str) -> int:
        content = text.encode("utf-8")
        if self.guard is not None:
            self.guard.reserve(len(content))
        if self.handle is not None:
            self.handle.write(content)
        self.digest.update(content)
        self.byte_count += len(content)
        return len(text)

    def close(self) -> dict[str, Any]:
        if self.handle is not None:
            self.handle.flush()
            os.fsync(self.handle.fileno())
            self.handle.close()
        return {"sha256": self.digest.hexdigest(), "bytes": self.byte_count}


def _serialize_csv(
    rows: Iterable[Mapping[str, Any]],
    fields: tuple[str, ...],
    path: Path | None,
    guard: _OutputGuard | None,
) -> dict[str, Any]:
    sink = _HashingTextSink(path, guard)
    try:
        writer = csv.DictWriter(
            sink, fieldnames=fields, extrasaction="ignore", lineterminator="\n"
        )
        writer.writeheader()
        writer.writerows(rows)
        return sink.close()
    except Exception:
        if sink.handle is not None and not sink.handle.closed:
            sink.handle.close()
        raise


def _write_bytes(path: Path, content: bytes, guard: _OutputGuard) -> dict[str, Any]:
    guard.reserve(len(content))
    with path.open("wb") as handle:
        handle.write(content)
        handle.flush()
        os.fsync(handle.fileno())
    return {"sha256": _sha256(content), "bytes": len(content)}


def _validate_existing_output(output_root: Path) -> dict[str, Any] | None:
    report_path = output_root / "anchor_evidence_report.json"
    if not output_root.exists():
        return None
    if not output_root.is_dir() or not report_path.is_file():
        raise ValueError("existing Phase 10D output is incomplete")
    report = json.loads(report_path.read_text(encoding="utf-8"))
    output_hashes = report.get("output_hashes", {})
    if set(output_hashes) != CSV_OUTPUT_NAMES:
        raise ValueError("existing Phase 10D report has an incomplete artifact set")
    for name, expected in output_hashes.items():
        path = output_root / name
        if not path.is_file() or _sha256_file(path) != expected:
            raise ValueError(f"existing Phase 10D output hash mismatch: {name}")
    return report


def _projection_hash(rows: Iterable[Mapping[str, Any]], fields: Iterable[str]) -> str:
    digests = sorted(
        hashlib.sha256(
            canonical_json({field: row.get(field, "") for field in fields})
        ).digest()
        for row in rows
    )
    digest = hashlib.sha256()
    for value in digests:
        digest.update(value)
    return digest.hexdigest()


def _load_inputs(
    market_path: Path,
    event_path: Path,
    milestone_path: Path,
) -> tuple[
    list[dict[str, Any]],
    list[dict[str, str]],
    list[dict[str, str]],
    bool,
]:
    compacted = market_path.stat().st_size >= STREAMING_MARKET_THRESHOLD_BYTES
    if compacted:
        markets, market_header, _ = _compact_market_input(market_path)
    else:
        markets, market_header = read_csv_with_header(market_path)
    events, event_header = read_csv_with_header(event_path)
    milestones, milestone_header = read_csv_with_header(milestone_path)
    _require_columns(market_header, MARKET_REQUIRED_FIELDS, "market metadata")
    _require_columns(event_header, EVENT_REQUIRED_FIELDS, "event metadata")
    _require_columns(milestone_header, MILESTONE_REQUIRED_FIELDS, "event milestones")
    validate_research_feature_columns(market_header)
    validate_research_feature_columns(event_header)
    validate_research_feature_columns(milestone_header)
    return markets, events, milestones, compacted


def run(
    market_metadata_path: Path,
    event_metadata_path: Path,
    event_milestones_path: Path,
    output_root: Path,
    *,
    config_path: Path = DEFAULT_CONFIG,
    limit_families: int | None = None,
    dry_run: bool = False,
    guard_root: Path | None = None,
    max_generated_bytes: int = DEFAULT_MAX_GENERATED_BYTES,
    min_free_bytes: int = DEFAULT_MIN_FREE_BYTES,
    expected_market_sha256: str | None = None,
    expected_event_sha256: str | None = None,
    expected_milestone_sha256: str | None = None,
) -> dict[str, Any]:
    rules = load_study_rules(config_path)
    input_paths = {
        "market_metadata": market_metadata_path,
        "event_metadata": event_metadata_path,
        "event_milestones": event_milestones_path,
    }
    input_hashes = {name: _sha256_file(path) for name, path in input_paths.items()}
    if market_metadata_path.stat().st_size >= PRODUCTION_PIN_THRESHOLD_BYTES and any(
        expected is None
        for expected in (
            expected_market_sha256,
            expected_event_sha256,
            expected_milestone_sha256,
        )
    ):
        raise ValueError(
            "production-sized Phase 10D inputs require all pinned SHA-256 values"
        )
    for name, expected in (
        ("market_metadata", expected_market_sha256),
        ("event_metadata", expected_event_sha256),
        ("event_milestones", expected_milestone_sha256),
    ):
        if expected is not None and input_hashes[name] != expected:
            raise ValueError(
                f"{name} SHA-256 mismatch: actual={input_hashes[name]} expected={expected}"
            )
    existing = _validate_existing_output(output_root) if not dry_run else None
    markets, events, milestones, compacted_market_input = _load_inputs(
        market_metadata_path, event_metadata_path, event_milestones_path
    )
    identities = sorted(
        {family_identity(row) for row in markets},
        key=lambda identity: (identity[1], identity[0]),
    )
    families_before_limit = len(identities)
    if limit_families is not None and limit_families < 0:
        raise ValueError("--limit-families must be nonnegative")
    selected_identities = (
        identities[:limit_families] if limit_families is not None else identities
    )
    selected_set = set(selected_identities)
    selected_markets = [row for row in markets if family_identity(row) in selected_set]

    full_event_tickers = {
        str(row.get("event_ticker") or "").strip()
        for row in markets
        if str(row.get("event_ticker") or "").strip()
    }
    for row in events:
        ticker = str(row.get("event_ticker") or "").strip()
        if ticker and ticker not in full_event_tickers:
            raise ValueError(f"unexpected event metadata ticker {ticker!r}")
    for row in milestones:
        ticker = str(row.get("event_ticker") or "").strip()
        if ticker and ticker not in full_event_tickers:
            raise ValueError(f"unexpected milestone event ticker {ticker!r}")

    selected_event_tickers = {
        str(row.get("event_ticker") or "").strip()
        for row in selected_markets
        if str(row.get("event_ticker") or "").strip()
    }
    selected_events = [
        row
        for row in events
        if str(row.get("event_ticker") or "").strip() in selected_event_tickers
    ]
    selected_milestones = [
        row
        for row in milestones
        if str(row.get("event_ticker") or "").strip() in selected_event_tickers
    ]
    research_input_hashes = {
        "market_candidate_projection": _projection_hash(
            selected_markets,
            (
                "ticker",
                "market_id",
                "family_id",
                "family_id_source",
                "event_ticker",
                "title",
                "subtitle",
                "rules_primary",
                "rules_secondary",
                "open_time",
                "market_open_time",
                "occurrence_datetime",
                "_supporting_source_count",
                "_first_supporting_market_ticker",
                "_last_supporting_market_ticker",
                "_family_market_count",
                "_family_representative_title",
                "_family_first_market_open_time",
                "_family_invalid_candidate_value_count",
                "_family_sentinel_timestamp_count",
            ),
        ),
        "event_candidate_projection": _projection_hash(
            selected_events,
            (
                "event_ticker",
                "series_ticker",
                "title",
                "sub_title",
                "category",
                "strike_date",
                "strike_period",
                "settlement_sources_json",
            ),
        ),
        "milestone_candidate_projection": _projection_hash(
            selected_milestones,
            (
                "event_ticker",
                "milestone_id",
                "milestone_category",
                "milestone_type",
                "milestone_title",
                "milestone_start_date",
                "milestone_source_id",
                "milestone_source_ids_json",
                "milestone_details_json",
                "association_type",
            ),
        ),
    }
    built = build_anchor_evidence(
        selected_markets, selected_events, selected_milestones, rules
    )
    work_dir: Path | None = None
    guard: _OutputGuard | None = None
    if not dry_run and existing is None:
        output_root.parent.mkdir(parents=True, exist_ok=True)
        guard = _OutputGuard(
            guard_root or output_root.parent,
            max_bytes=max_generated_bytes,
            min_free_bytes=min_free_bytes,
        )
        work_dir = Path(
            tempfile.mkdtemp(prefix=".phase10d-work.", dir=output_root.parent)
        )
    row_sets = {
        "anchor_evidence.csv": (built.evidence_rows, ANCHOR_EVIDENCE_FIELDS),
        "anchor_family_review.csv": (
            built.family_rows,
            ANCHOR_FAMILY_REVIEW_FIELDS,
        ),
        "anchor_verification_decisions_template.csv": (
            built.decision_rows,
            DECISION_TEMPLATE_FIELDS,
        ),
    }
    references: dict[str, dict[str, Any]] = {}
    try:
        for name, (rows, fields) in row_sets.items():
            references[name] = _serialize_csv(
                rows,
                fields,
                work_dir / name if work_dir is not None else None,
                guard,
            )
    except Exception:
        if work_dir is not None and work_dir.exists():
            shutil.rmtree(work_dir)
        raise
    truncated = len(selected_identities) < families_before_limit
    report = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "study_rules_fingerprint": rules.fingerprint,
        "streaming_market_compaction": compacted_market_input,
        "research_input_hashes": research_input_hashes,
        **built.statistics,
        "limited_run": limit_families is not None,
        "requested_limit": limit_families,
        "families_before_limit": families_before_limit,
        "families_after_limit": len(selected_identities),
        "universe_complete": not truncated,
        "anchors_verified": 0,
        "outcomes_merged": False,
        "network_requests": 0,
        "output_hashes": {
            name: reference["sha256"] for name, reference in sorted(references.items())
        },
        "output_bytes": {
            name: reference["bytes"] for name, reference in sorted(references.items())
        },
    }
    report_content = canonical_json(report) + b"\n"
    if existing is not None:
        if canonical_json(existing) != canonical_json(report):
            raise ValueError(
                "existing Phase 10D output conflicts with deterministic rerun"
            )
        print(canonical_json(existing).decode("utf-8"))
        return existing
    if work_dir is not None and guard is not None:
        try:
            _write_bytes(
                work_dir / "anchor_evidence_report.json", report_content, guard
            )
            os.replace(work_dir, output_root)
            published = _validate_existing_output(output_root)
            if published is None or canonical_json(published) != canonical_json(report):
                raise ValueError("published Phase 10D output failed final validation")
        except Exception:
            if work_dir.exists():
                shutil.rmtree(work_dir)
            raise
    print(canonical_json(report).decode("utf-8"))
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-metadata", required=True, type=Path)
    parser.add_argument("--event-metadata", required=True, type=Path)
    parser.add_argument("--event-milestones", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--config", default=DEFAULT_CONFIG, type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit-families", type=int)
    parser.add_argument("--guard-root", type=Path)
    parser.add_argument(
        "--max-generated-bytes", type=int, default=DEFAULT_MAX_GENERATED_BYTES
    )
    parser.add_argument("--min-free-bytes", type=int, default=DEFAULT_MIN_FREE_BYTES)
    parser.add_argument("--expected-market-sha256")
    parser.add_argument("--expected-event-sha256")
    parser.add_argument("--expected-milestone-sha256")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run(
            args.market_metadata,
            args.event_metadata,
            args.event_milestones,
            args.output_root,
            config_path=args.config,
            limit_families=args.limit_families,
            dry_run=args.dry_run,
            guard_root=args.guard_root,
            max_generated_bytes=args.max_generated_bytes,
            min_free_bytes=args.min_free_bytes,
            expected_market_sha256=args.expected_market_sha256,
            expected_event_sha256=args.expected_event_sha256,
            expected_milestone_sha256=args.expected_milestone_sha256,
        )
        return 0
    except (FileNotFoundError, ValueError) as exc:
        print(f"Invalid input: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
