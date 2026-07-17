from __future__ import annotations

import csv
import gzip
import io
import json
from pathlib import Path

import pytest

from scripts.common.io_utils import iter_csv, read_csv_with_header
from scripts.pipeline_v2.kalshi_metadata_cache import CacheError, StorageBudget
from scripts.pipeline_v2.pull_kalshi_partitioned_event_metadata import (
    EVENT_NAMESPACE,
    _load_settings,
    _scope_definition,
    _scope_id,
    acquire_next_partition,
    build_preflight,
    load_partition_chain,
    merge_completed_scope,
    scan_event_ticker_universe,
)


class Response:
    status_code = 200

    def __init__(self, payload):
        self.payload = payload

    def json(self):
        return self.payload


class Session:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self.payloads:
            raise AssertionError("unexpected network request")
        value = self.payloads.pop(0)
        if isinstance(value, BaseException):
            raise value
        return Response(value)


def event(ticker, **extra):
    return {
        "event_ticker": ticker,
        "series_ticker": "SERIES",
        "title": f"Title {ticker}",
        **extra,
    }


def write_config(path: Path, *, partition_events=2, max_pages=3):
    path.write_text(
        """
[kalshi_event_metadata]
page_size = 2
batch_size = 2
partition_events = %d
max_pages_per_batch = %d
max_retries = 0
backoff_base_seconds = 0
backoff_cap_seconds = 0
timeout_seconds = 1
requests_per_second = 1000
estimated_compressed_raw_bytes_per_event = 100
estimated_compressed_normalized_bytes_per_event = 50
max_raw_bytes = 104857600
min_free_bytes = 0
"""
        % (partition_events, max_pages),
        encoding="utf-8",
    )
    return path


def write_universe(tmp_path: Path, tickers, merge_id="merge-test"):
    directory = tmp_path / "merged_universes" / merge_id
    directory.mkdir(parents=True)
    source = directory / "event_tickers.csv.gz"
    text = "event_ticker,contract_count,first_open_time\n" + "".join(
        f"{ticker},1,2025-01-01T00:00:00Z\n" for ticker in tickers
    )
    source.write_bytes(gzip.compress(text.encode(), compresslevel=9, mtime=0))
    import hashlib

    digest = hashlib.sha256(source.read_bytes()).hexdigest()
    report = {
        "merge_id": merge_id,
        "event_count": len(set(tickers)),
        "artifacts": [{"kind": source.name, "sha256": digest}],
    }
    (directory / "merge_report.json").write_text(json.dumps(report), encoding="utf-8")
    return source, digest, merge_id


def setup_scope(tmp_path: Path, tickers=("A", "B"), *, partition_events=2):
    source, digest, merge_id = write_universe(tmp_path, tickers)
    settings = _load_settings(
        write_config(tmp_path / "config.toml", partition_events=partition_events)
    )
    audit = scan_event_ticker_universe(
        source, expected_sha256=digest, expected_merge_id=merge_id
    )
    definition = _scope_definition(audit, settings, None)
    raw_root = tmp_path / "raw"
    budget = StorageBudget(raw_root, max_bytes=100 * 1024**2, min_free_bytes=0)
    return source, settings, definition, raw_root, budget


def test_shared_csv_reader_streams_plain_and_gzip(tmp_path):
    plain = tmp_path / "rows.csv"
    plain.write_text("a,b\n1,2\n", encoding="utf-8")
    compressed = tmp_path / "rows.csv.gz"
    compressed.write_bytes(gzip.compress(plain.read_bytes(), mtime=0))
    assert list(iter_csv(plain)) == [{"a": "1", "b": "2"}]
    assert list(iter_csv(compressed)) == [{"a": "1", "b": "2"}]
    rows, header = read_csv_with_header(compressed)
    assert header == ("a", "b") and rows[0]["a"] == "1"


def test_production_preflight_is_read_only_and_reports_exact_estimates(tmp_path):
    source, digest, merge_id = write_universe(tmp_path, ("A", "B", "C"))
    settings = _load_settings(write_config(tmp_path / "config.toml"))
    raw_root = tmp_path / "does-not-exist"
    report, definition, audit = build_preflight(
        event_tickers_path=source,
        raw_root=raw_root,
        settings=settings,
        limit_events=None,
        expected_sha256=digest,
        expected_merge_id=merge_id,
        max_raw_bytes=100 * 1024**2,
        min_free_bytes=0,
    )
    assert not raw_root.exists()
    assert audit["gzip_input"] is True
    assert report["total_events"] == report["unique_events"] == 3
    assert report["deterministic_batch_count"] == 2
    assert report["minimum_requests"] == 2
    assert report["remaining_minimum_requests"] == 2
    assert report["deterministic_partition_count"] == 2
    assert report["projected_compressed_raw_bytes"] == 300
    assert report["projected_partition_normalized_bytes"] == 150
    assert report["projected_final_normalized_bytes"] == 150
    assert report["projected_compressed_normalized_bytes"] == 300
    assert report["ready_for_network"] is True
    assert definition["source_sha256"] == digest


def test_preflight_rejects_hash_mismatch_and_reports_duplicate_input(tmp_path):
    source, _, merge_id = write_universe(tmp_path, ("A", "A"))
    with pytest.raises(CacheError, match="SHA-256"):
        scan_event_ticker_universe(source, expected_sha256="0" * 64)
    audit = scan_event_ticker_universe(source, expected_merge_id=merge_id)
    assert audit["duplicate_rows"] == 1
    settings = _load_settings(write_config(tmp_path / "config.toml"))
    report, _, _ = build_preflight(
        event_tickers_path=source,
        raw_root=tmp_path / "raw",
        settings=settings,
        limit_events=None,
        expected_sha256=None,
        expected_merge_id=merge_id,
        max_raw_bytes=100 * 1024**2,
        min_free_bytes=0,
    )
    assert report["ready_for_network"] is False


def test_partition_is_compressed_quarantined_committed_and_mergeable(tmp_path):
    source, settings, definition, raw_root, budget = setup_scope(tmp_path)
    payload = {
        "events": [
            event(
                "A",
                result="yes",
                settlement_value=1,
                product_metadata={"result": "yes", "safe": 1},
            ),
            event(
                "B",
                milestones=[
                    {
                        "id": "M1",
                        "title": "Start",
                        "start_date": "2025-01-01Z",
                        "related_event_tickers": ["B"],
                        "details": {"outcome": "yes", "safe": 2},
                    }
                ],
            ),
        ],
        "cursor": "",
    }
    result = acquire_next_partition(
        event_tickers_path=source,
        raw_root=raw_root,
        settings=settings,
        definition=definition,
        budget=budget,
        session=Session([payload]),
        sleep=lambda _: None,
    )
    assert result["scope_complete"] is True
    chain = load_partition_chain(raw_root, _scope_id(definition))
    assert len(chain) == 1 and chain[0]["missing_event_count"] == 0
    page = Path(chain[0]["source_pages"][0]["path"])
    assert page.suffix == ".gz"
    metadata = next(
        Path(item["path"])
        for item in chain[0]["artifacts"]
        if item["kind"] == "event_metadata"
    )
    normalized = gzip.decompress(metadata.read_bytes()).decode()
    assert '"result"' not in normalized and "settlement_value" not in normalized
    merge = merge_completed_scope(
        raw_root=raw_root, definition=definition, budget=budget
    )
    assert merge["merge_complete"] is True
    assert merge["retrieved_event_count"] == 2
    assert merge["logical_request_count"] == 1
    assert merge["successful_http_attempt_count"] == 1
    assert merge["anchors_verified"] is False
    output = (
        raw_root
        / EVENT_NAMESPACE
        / "merged_event_universes"
        / merge["merge_id"]
        / "event_metadata.csv.gz"
    )
    rows = list(
        csv.DictReader(io.StringIO(gzip.decompress(output.read_bytes()).decode()))
    )
    assert [row["event_ticker"] for row in rows] == ["A", "B"]
    assert "result" not in rows[0]


def test_interruption_reuses_immutable_page_and_resumes_cursor(tmp_path):
    source, settings, definition, raw_root, budget = setup_scope(tmp_path)
    interrupted = Session(
        [
            {"events": [event("A")], "cursor": "next"},
            KeyboardInterrupt(),
        ]
    )
    with pytest.raises(KeyboardInterrupt):
        acquire_next_partition(
            event_tickers_path=source,
            raw_root=raw_root,
            settings=settings,
            definition=definition,
            budget=budget,
            session=interrupted,
            sleep=lambda _: None,
        )
    assert not list((raw_root / EVENT_NAMESPACE / "partition_commits").rglob("*.json"))
    cached = list((raw_root / EVENT_NAMESPACE / "partition_pages").rglob("*.json.gz"))
    assert len(cached) == 1
    resume = Session([{"events": [event("B")], "cursor": ""}])
    result = acquire_next_partition(
        event_tickers_path=source,
        raw_root=raw_root,
        settings=settings,
        definition=definition,
        budget=budget,
        session=resume,
        sleep=lambda _: None,
    )
    assert len(resume.calls) == 1
    assert resume.calls[0][1]["params"]["cursor"] == "next"
    assert result["cache_hit_count"] == 1
    assert result["network_request_count"] == 1


def test_partition_conflict_and_tampering_fail_closed(tmp_path):
    source, settings, definition, raw_root, budget = setup_scope(tmp_path)
    with pytest.raises(Exception, match="unexpected event ticker"):
        acquire_next_partition(
            event_tickers_path=source,
            raw_root=raw_root,
            settings=settings,
            definition=definition,
            budget=budget,
            session=Session([{"events": [event("Z")], "cursor": ""}]),
            sleep=lambda _: None,
        )
    clean_root = tmp_path / "clean"
    clean_budget = StorageBudget(clean_root, max_bytes=100 * 1024**2, min_free_bytes=0)
    acquire_next_partition(
        event_tickers_path=source,
        raw_root=clean_root,
        settings=settings,
        definition=definition,
        budget=clean_budget,
        session=Session([{"events": [event("A"), event("B")], "cursor": ""}]),
        sleep=lambda _: None,
    )
    page = next((clean_root / EVENT_NAMESPACE / "partition_pages").rglob("*.json.gz"))
    page.write_bytes(page.read_bytes() + b"tamper")
    with pytest.raises(CacheError, match="invalid event partition commit"):
        load_partition_chain(clean_root, _scope_id(definition))


def test_missing_event_is_explicit_and_blocks_final_publication(tmp_path):
    source, settings, definition, raw_root, budget = setup_scope(tmp_path)
    result = acquire_next_partition(
        event_tickers_path=source,
        raw_root=raw_root,
        settings=settings,
        definition=definition,
        budget=budget,
        session=Session([{"events": [event("A")], "cursor": ""}]),
        sleep=lambda _: None,
    )
    assert result["missing_event_count"] == 1
    merge = merge_completed_scope(
        raw_root=raw_root, definition=definition, budget=budget
    )
    assert merge["merge_complete"] is False
    assert merge["reason"] == "missing_events"
    assert not (raw_root / EVENT_NAMESPACE / "merged_event_universes").exists()


def test_multiple_partitions_are_independent_contiguous_and_deterministic(tmp_path):
    source, settings, definition, raw_root, budget = setup_scope(
        tmp_path, tickers=("A", "B", "C", "D"), partition_events=2
    )
    first = acquire_next_partition(
        event_tickers_path=source,
        raw_root=raw_root,
        settings=settings,
        definition=definition,
        budget=budget,
        session=Session([{"events": [event("A"), event("B")], "cursor": ""}]),
        sleep=lambda _: None,
    )
    second = acquire_next_partition(
        event_tickers_path=source,
        raw_root=raw_root,
        settings=settings,
        definition=definition,
        budget=budget,
        session=Session([{"events": [event("C"), event("D")], "cursor": ""}]),
        sleep=lambda _: None,
    )
    assert first["scope_complete"] is False and second["scope_complete"] is True
    chain = load_partition_chain(raw_root, _scope_id(definition))
    assert [item["event_offset"] for item in chain] == [0, 2]
    assert [item["first_event_ticker"] for item in chain] == ["A", "C"]
    merge = merge_completed_scope(
        raw_root=raw_root, definition=definition, budget=budget
    )
    output = (
        raw_root
        / EVENT_NAMESPACE
        / "merged_event_universes"
        / merge["merge_id"]
        / "event_metadata.csv.gz"
    )
    before = output.read_bytes()
    repeated = merge_completed_scope(
        raw_root=raw_root, definition=definition, budget=budget
    )
    assert repeated == merge and output.read_bytes() == before


def test_outcome_only_raw_changes_leave_research_artifact_hashes_invariant(tmp_path):
    hashes = []
    for name, result in (("yes", "yes"), ("no", "no")):
        root = tmp_path / name
        source, settings, definition, raw_root, budget = setup_scope(root)
        acquire_next_partition(
            event_tickers_path=source,
            raw_root=raw_root,
            settings=settings,
            definition=definition,
            budget=budget,
            session=Session(
                [
                    {
                        "events": [
                            event(
                                "A",
                                result=result,
                                product_metadata={"result": result, "safe": 1},
                            ),
                            event("B", settlement_value=result),
                        ],
                        "cursor": "",
                    }
                ]
            ),
            sleep=lambda _: None,
        )
        chain = load_partition_chain(raw_root, _scope_id(definition))
        hashes.append(
            {
                item["kind"]: item["sha256"]
                for item in chain[0]["artifacts"]
                if item["kind"]
                in {"event_metadata", "event_milestones", "event_provenance"}
            }
        )
    assert hashes[0] == hashes[1]


def test_preflight_fails_closed_when_projection_exceeds_namespace(tmp_path):
    source, digest, merge_id = write_universe(tmp_path, ("A", "B", "C"))
    settings = _load_settings(write_config(tmp_path / "config.toml"))
    report, _, _ = build_preflight(
        event_tickers_path=source,
        raw_root=tmp_path / "raw",
        settings=settings,
        limit_events=None,
        expected_sha256=digest,
        expected_merge_id=merge_id,
        max_raw_bytes=500,
        min_free_bytes=0,
    )
    assert report["projected_additional_bytes"] == 600
    assert report["fits_namespace_ceiling"] is False
    assert report["ready_for_network"] is False
