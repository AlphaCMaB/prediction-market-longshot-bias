from __future__ import annotations

import csv
from datetime import datetime, timezone
import gzip
import io
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from scripts.pipeline_v2.kalshi_metadata_cache import (
    CacheError,
    CompressedPartitionCache,
    ResourceLimitError,
    StorageBudget,
)
from scripts.pipeline_v2.kalshi_metadata_planner import (
    EndpointSegment,
    segment_params,
)
from scripts.pipeline_v2.merge_kalshi_metadata_partitions import (
    build_parser as build_merge_parser,
    run as run_merge,
)
from scripts.pipeline_v2.pull_kalshi_partitioned_metadata import (
    build_parser,
    load_partition_chain,
    run,
    segment_id,
)
from scripts.pipeline_v2.kalshi_metadata_cache import sha256_json


UTC_RANGE_START = "2025-08-01T00:00:00Z"
UTC_RANGE_END = "2025-09-01T00:00:00Z"
CONFIG = Path("configs/pipeline_v2.toml")


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses=()):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}

    def get(self, url, **kwargs):
        self.calls.append({"url": url, **kwargs})
        if not self.responses:
            raise AssertionError("unexpected network request")
        return self.responses.pop(0)


def market(ticker: str, *, updated: str, result: str, title: str) -> dict:
    return {
        "ticker": ticker,
        "event_ticker": "EVENT-1",
        "title": title,
        "updated_time": updated,
        "open_time": "2025-07-01T00:00:00Z",
        "settlement_ts": "2025-08-15T00:00:00Z",
        "result": result,
        "settlement_value_dollars": "1.0000" if result == "yes" else "0.0000",
    }


def write_cutoff(tmp_path: Path) -> Path:
    path = tmp_path / "cutoff.json"
    path.write_text('{"market_settled_ts":"2026-01-01T00:00:00Z"}\n')
    return path


def acquisition_args(tmp_path: Path, cutoff: Path, *extra: str):
    return build_parser().parse_args(
        [
            "--start-date",
            "2025-08-01",
            "--end-date",
            "2025-08-31",
            "--raw-root",
            str(tmp_path / "raw"),
            "--cutoff-snapshot",
            str(cutoff),
            "--config",
            str(CONFIG),
            "--partition-pages",
            "1",
            "--max-raw-bytes",
            str(100 * 1024**2),
            "--min-free-bytes",
            "0",
            *extra,
        ]
    )


def test_historical_endpoint_has_no_server_side_date_parameters():
    historical = EndpointSegment(
        "historical",
        "/trade-api/v2/historical/markets",
        SimpleNamespace(timestamp=lambda: 1),
        SimpleNamespace(timestamp=lambda: 2),
    )
    params = segment_params(historical, 1000)
    assert params == {"limit": 1000, "mve_filter": "exclude"}


def test_live_endpoint_enforces_settlement_range_server_side():
    live = EndpointSegment(
        "live",
        "/trade-api/v2/markets",
        datetime(2025, 8, 1, tzinfo=timezone.utc),
        datetime(2025, 9, 1, tzinfo=timezone.utc),
        "2025-08",
    )
    params = segment_params(live, 1000)
    assert params["status"] == "settled"
    assert params["min_settled_ts"] == 1754006400
    assert params["max_settled_ts"] == 1756684799


def test_storage_budget_checks_actual_additional_bytes_and_free_floor(tmp_path):
    usage = lambda _: SimpleNamespace(free=100)
    budget = StorageBudget(tmp_path, max_bytes=10, min_free_bytes=90, disk_usage=usage)
    budget.check_publication(tmp_path / "new", b"1234567890")
    with pytest.raises(ResourceLimitError, match="raw-data budget"):
        budget.check_publication(tmp_path / "new", b"12345678901")
    low_free = StorageBudget(
        tmp_path, max_bytes=100, min_free_bytes=95, disk_usage=usage
    )
    with pytest.raises(ResourceLimitError, match="free-space"):
        low_free.check_publication(tmp_path / "another", b"123456")


def test_compressed_cache_is_deterministic_and_validates_gzip(tmp_path):
    budget = StorageBudget(tmp_path, max_bytes=1024**2, min_free_bytes=0)
    cache = CompressedPartitionCache(tmp_path, partition_id="part", budget=budget)
    segment = EndpointSegment(
        "historical", "/trade-api/v2/historical/markets", None, None
    )
    request = {
        "endpoint_path": segment.endpoint_path,
        "request_cursor_hash": "start",
        "cutoff_id": "cutoff",
        "params": {"limit": 1},
    }
    path = cache.page_path(segment, "cutoff", 1, "a" * 64)
    cache.publish_page(
        path, request_metadata=request, response={"markets": [], "cursor": ""}
    )
    first = path.read_bytes()
    cache.publish_page(
        path, request_metadata=request, response={"markets": [], "cursor": ""}
    )
    assert path.suffix == ".gz"
    assert path.read_bytes() == first
    assert json.loads(gzip.decompress(first))["compression"] == "gzip"
    assert cache.load_page(path, request)["response"]["markets"] == []


def test_preflight_is_offline_read_only_and_reports_unknown_historical_total(
    tmp_path, capsys
):
    cutoff = write_cutoff(tmp_path)
    args = acquisition_args(tmp_path, cutoff, "--preflight")
    session = FakeSession()
    assert run(args, session=session) == 0
    assert session.calls == []
    assert not (tmp_path / "raw").exists()
    report = json.loads(capsys.readouterr().out)
    assert report["historical_server_side_date_filter"] is False
    assert report["historical_total_request_estimate"].startswith("unknown")
    assert report["maximum_requests_next_partition"] == 1
    assert report["estimated_partition_fits_raw_budget"] is True


def test_bounded_partitions_commit_resume_normalize_and_merge_without_outcome_selection(
    tmp_path, capsys
):
    cutoff = write_cutoff(tmp_path)
    old = market("MKT-1", updated="2025-08-15T01:00:00Z", result="yes", title="old")
    new = market("MKT-1", updated="2025-08-15T02:00:00Z", result="no", title="new")
    args = acquisition_args(tmp_path, cutoff)

    first_session = FakeSession(
        [FakeResponse({"markets": [old], "cursor": "next-partition"})]
    )
    assert run(args, session=first_session) == 0
    first_output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert first_output[-1]["archive_complete"] is False
    assert len(first_session.calls) == 1
    assert len(list((tmp_path / "raw" / "cutoff_snapshots").glob("cutoff_*.json"))) == 1

    cutoff_id = sha256_json({"market_settled_ts": "2026-01-01T00:00:00Z"})[:20]
    segment = EndpointSegment(
        "historical",
        "/trade-api/v2/historical/markets",
        # These values are represented in the deterministic segment identity.
        datetime(2025, 8, 1, tzinfo=timezone.utc),
        datetime(2025, 9, 1, tzinfo=timezone.utc),
        None,
    )
    sid = segment_id(segment, cutoff_id)
    chain = load_partition_chain(tmp_path / "raw", sid)
    assert len(chain) == 1
    assert chain[0]["end_cursor"] == "next-partition"
    assert {item["kind"] for item in chain[0]["artifacts"]} == {
        "metadata",
        "outcomes",
        "provenance",
        "request_manifest",
        "normalization_report",
    }
    operational_record = json.loads(
        (tmp_path / "raw" / "manifest.jsonl").read_text().splitlines()[0]
    )
    assert operational_record["bounded_partition_boundary"] is True
    assert operational_record["intentionally_incomplete_due_to_page_limit"] is False
    metadata_path = next(
        Path(item["path"])
        for item in chain[0]["artifacts"]
        if item["kind"] == "metadata"
    )
    metadata_text = gzip.decompress(metadata_path.read_bytes()).decode()
    assert '"result"' not in metadata_text
    assert '"settlement_value_dollars"' not in metadata_text
    assert '"title":"old"' in metadata_text

    second_session = FakeSession([FakeResponse({"markets": [new], "cursor": ""})])
    assert run(args, session=second_session) == 0
    second_output = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert second_output[-1]["archive_complete"] is True
    assert second_session.calls[0]["params"]["cursor"] == "next-partition"
    chain = load_partition_chain(tmp_path / "raw", sid)
    assert len(chain) == 2

    merge_args = build_merge_parser().parse_args(
        [
            "--start-date",
            "2025-08-01",
            "--end-date",
            "2025-08-31",
            "--raw-root",
            str(tmp_path / "raw"),
            "--cutoff-snapshot",
            str(cutoff),
            "--config",
            str(CONFIG),
            "--max-raw-bytes",
            str(100 * 1024**2),
            "--min-free-bytes",
            "0",
        ]
    )
    assert run_merge(merge_args) == 0
    merge_report = json.loads(capsys.readouterr().out)
    commit = json.loads(Path(merge_report["merge_commit"]).read_text())
    metadata_csv = Path(
        next(
            item["path"]
            for item in commit["artifacts"]
            if item["kind"] == "market_metadata.csv"
        )
    ).read_text()
    outcomes_csv = Path(
        next(
            item["path"]
            for item in commit["artifacts"]
            if item["kind"] == "market_outcomes.csv"
        )
    ).read_text()
    metadata_rows = list(csv.DictReader(io.StringIO(metadata_csv)))
    outcome_rows = list(csv.DictReader(io.StringIO(outcomes_csv)))
    assert metadata_rows[0]["title"] == "new"
    assert "result" not in metadata_rows[0]
    assert outcome_rows[0]["result"] == "no"


def test_merge_reports_incomplete_and_publishes_no_final_universe(tmp_path, capsys):
    cutoff = write_cutoff(tmp_path)
    args = acquisition_args(tmp_path, cutoff)
    response = FakeResponse(
        {
            "markets": [
                market(
                    "MKT-1", updated="2025-08-15T01:00:00Z", result="yes", title="one"
                )
            ],
            "cursor": "not-terminal",
        }
    )
    assert run(args, session=FakeSession([response])) == 0
    capsys.readouterr()
    merge_args = build_merge_parser().parse_args(
        [
            "--start-date",
            "2025-08-01",
            "--end-date",
            "2025-08-31",
            "--raw-root",
            str(tmp_path / "raw"),
            "--cutoff-snapshot",
            str(cutoff),
            "--config",
            str(CONFIG),
            "--max-raw-bytes",
            str(100 * 1024**2),
            "--min-free-bytes",
            "0",
        ]
    )
    assert run_merge(merge_args) == 3
    report = json.loads(capsys.readouterr().out)
    assert report["merge_complete"] is False
    assert report["final_universe_published"] is False
    assert not (tmp_path / "raw" / "merged_universes").exists()


def test_partition_commit_validation_detects_compressed_page_tampering(
    tmp_path, capsys
):
    cutoff = write_cutoff(tmp_path)
    args = acquisition_args(tmp_path, cutoff)
    response = FakeResponse(
        {
            "markets": [
                market(
                    "MKT-1", updated="2025-08-15T01:00:00Z", result="yes", title="one"
                )
            ],
            "cursor": "",
        }
    )
    assert run(args, session=FakeSession([response])) == 0
    capsys.readouterr()
    raw_root = tmp_path / "raw"
    page = next((raw_root / "partition_pages").rglob("*.json.gz"))
    page.write_bytes(page.read_bytes() + b"tamper")
    cutoff_id = sha256_json({"market_settled_ts": "2026-01-01T00:00:00Z"})[:20]
    segment = EndpointSegment(
        "historical",
        "/trade-api/v2/historical/markets",
        datetime(2025, 8, 1, tzinfo=timezone.utc),
        datetime(2025, 9, 1, tzinfo=timezone.utc),
        None,
    )
    with pytest.raises(CacheError, match="invalid partition commit"):
        load_partition_chain(raw_root, segment_id(segment, cutoff_id))
