"""Offline characterization tests for expanded Kalshi metadata ingestion."""

from __future__ import annotations

from datetime import datetime, timezone
from dataclasses import replace
import json
from pathlib import Path
import subprocess
import sys
import os

import pytest

from scripts.pipeline_v2.kalshi_metadata_cache import (
    CacheError,
    ImmutableConflict,
    MetadataCache,
    SensitiveResponseError,
    append_manifest,
    canonical_sensitive_key,
    canonical_json,
    publish_immutable_bytes,
    reject_sensitive_response,
    sha256_json,
)
from scripts.pipeline_v2.kalshi_metadata_client import (
    CursorLoopError,
    EmptyPageCursorError,
    KalshiMetadataClient,
    KALSHI_PRODUCTION_BASE_URL,
    MetadataClientError,
    RequestFailure,
)
from scripts.pipeline_v2.kalshi_metadata_consolidation import (
    ConsolidationError,
    ConsolidationConflict,
    consolidate_month,
    invalid_audit_path,
    invalid_record_audits,
    monthly_audit_path,
    monthly_output_path,
    payload_sha256,
    write_derived_jsonl,
)
from scripts.pipeline_v2.kalshi_metadata_planner import (
    HISTORICAL_PATH,
    LIVE_PATH,
    EndpointSegment,
    filter_month,
    generate_months,
    normalize_inclusive_dates,
    plan_endpoint_segments,
    request_id,
    segment_params,
)
from scripts.pipeline_v2.pull_kalshi_settled_metadata import (
    _canonical_effective_configuration,
    _commit_bytes,
    _publish_run_commit,
    _transaction_run_id,
    _valid_commit,
    build_parser,
    run,
)
import scripts.pipeline_v2.pull_kalshi_settled_metadata as metadata_cli


UTC = timezone.utc


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {"markets": [], "cursor": ""}

    def json(self):
        return self._payload


class MalformedJsonResponse(FakeResponse):
    def json(self):
        raise ValueError("malformed JSON")


class FakeSession:
    def __init__(self, responses=()):
        self.responses = list(responses)
        self.calls = []
        self.headers = {}

    def get(self, url, *, params, timeout):
        self.calls.append({"url": url, "params": dict(params), "timeout": timeout})
        if not self.responses:
            raise AssertionError("unexpected HTTP request")
        result = self.responses.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result


def live_segment(month="2025-08"):
    return EndpointSegment(
        "live",
        LIVE_PATH,
        datetime(2025, 8, 1, tzinfo=UTC),
        datetime(2025, 9, 1, tzinfo=UTC),
        month,
    )


def historical_segment():
    return EndpointSegment(
        "historical",
        HISTORICAL_PATH,
        datetime(2025, 7, 1, tzinfo=UTC),
        datetime(2025, 8, 1, tzinfo=UTC),
        None,
    )


def client(session, **overrides):
    settings = {
        "sleep": lambda _: None,
        "random_value": lambda: 0.0,
        "requests_per_second": 0,
        "max_retries": 2,
    }
    settings.update(overrides)
    return KalshiMetadataClient(session, **settings)


def test_inclusive_dates_become_half_open_and_generate_exact_twelve_months():
    interval = normalize_inclusive_dates("2025-07-01", "2026-06-30")
    assert interval.start.isoformat() == "2025-07-01T00:00:00+00:00"
    assert interval.end.isoformat() == "2026-07-01T00:00:00+00:00"
    assert [month.month for month in generate_months(interval)] == [
        "2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12",
        "2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06",
    ]


def test_month_boundaries_and_leap_day():
    months = generate_months(normalize_inclusive_dates("2024-02-01", "2024-03-02"))
    assert months[0].end == datetime(2024, 3, 1, tzinfo=UTC)
    assert months[1].end == datetime(2024, 3, 3, tzinfo=UTC)
    assert filter_month(months, "2024-02") == (months[0],)
    with pytest.raises(ValueError, match="outside"):
        filter_month(months, "2025-01")


@pytest.mark.parametrize(
    ("cutoff", "tiers"),
    [
        (datetime(2026, 1, 1, tzinfo=UTC), ["historical"]),
        (datetime(2025, 1, 1, tzinfo=UTC), ["live", "live"]),
        (datetime(2025, 8, 15, tzinfo=UTC), ["historical", "live"]),
    ],
)
def test_historical_live_and_crossing_plans(cutoff, tiers):
    months = generate_months(normalize_inclusive_dates("2025-07-01", "2025-08-31"))
    segments = plan_endpoint_segments(months, cutoff)
    assert [segment.tier for segment in segments] == tiers
    assert sum(segment.tier == "historical" for segment in segments) <= 1


def test_endpoint_parameters_do_not_mix_historical_and_live_filters():
    live = segment_params(live_segment(), 1000)
    historical = segment_params(historical_segment(), 1000)
    assert live["status"] == "settled"
    assert "min_settled_ts" in live and "max_settled_ts" in live
    assert not ({"status", "min_settled_ts", "max_settled_ts"} & historical.keys())
    assert LIVE_PATH.endswith("/markets") and "candlestick" not in LIVE_PATH
    assert "candlestick" not in HISTORICAL_PATH


def test_request_ids_and_paths_are_deterministic_and_cutoff_namespaced(tmp_path):
    params = segment_params(live_segment(), 100, cursor="opaque")
    first = request_id(LIVE_PATH, params, "cutoff-a")
    assert first == request_id(LIVE_PATH, dict(reversed(list(params.items()))), "cutoff-a")
    assert first != request_id(LIVE_PATH, params, "cutoff-b")
    cache = MetadataCache(tmp_path)
    assert cache.page_path(live_segment(), "cutoff-a", 2, first, "opaque") == cache.page_path(
        live_segment(), "cutoff-a", 2, first, "opaque"
    )
    assert cache.pages_dir("historical", "cutoff-a", None) != cache.pages_dir(
        "historical", "cutoff-b", None
    )


def test_pagination_starts_without_cursor_and_uses_returned_cursor(tmp_path):
    session = FakeSession([
        FakeResponse(payload={"markets": [{"ticker": "A"}], "cursor": "next"}),
        FakeResponse(payload={"markets": [{"ticker": "B"}], "cursor": ""}),
    ])
    result = client(session).paginate(live_segment(), MetadataCache(tmp_path), cutoff_id="c", run_id="r")
    assert result.complete and [row["ticker"] for row in result.markets] == ["A", "B"]
    assert "cursor" not in session.calls[0]["params"]
    assert session.calls[1]["params"]["cursor"] == "next"


def test_cursor_loop_and_empty_page_with_cursor_fail(tmp_path):
    looping = FakeSession([
        FakeResponse(payload={"markets": [{"ticker": "A"}], "cursor": "same"}),
        FakeResponse(payload={"markets": [{"ticker": "B"}], "cursor": "same"}),
    ])
    with pytest.raises(CursorLoopError):
        client(looping).paginate(live_segment(), MetadataCache(tmp_path / "loop"), cutoff_id="c", run_id="r")
    empty = FakeSession([FakeResponse(payload={"markets": [], "cursor": "more"})])
    with pytest.raises(EmptyPageCursorError):
        client(empty).paginate(live_segment(), MetadataCache(tmp_path / "empty"), cutoff_id="c", run_id="r")


def test_page_limit_marks_chain_incomplete(tmp_path):
    session = FakeSession([FakeResponse(payload={"markets": [{"ticker": "A"}], "cursor": "next"})])
    result = client(session).paginate(
        live_segment(), MetadataCache(tmp_path), cutoff_id="c", run_id="r", limit_pages=1
    )
    assert not result.complete
    assert result.intentionally_incomplete_due_to_page_limit
    assert len(session.calls) == 1


def test_cache_hit_resumes_cursor_without_requesting_cached_page(tmp_path):
    cache = MetadataCache(tmp_path)
    segment = live_segment()
    params = segment_params(segment, 1000)
    rid = request_id(LIVE_PATH, params, "c")
    metadata = client(FakeSession())._request_metadata(segment, params, 1, None, "c")
    cache.publish_page(
        cache.page_path(segment, "c", 1, rid),
        request_metadata=metadata,
        response={"markets": [{"ticker": "A"}], "cursor": "next"},
    )
    session = FakeSession([FakeResponse(payload={"markets": [{"ticker": "B"}], "cursor": ""})])
    active = client(session)
    result = active.paginate(segment, cache, cutoff_id="c", run_id="r")
    assert [item["ticker"] for item in result.markets] == ["A", "B"]
    assert active.counters.cache_hits == 1
    assert len(session.calls) == 1 and session.calls[0]["params"]["cursor"] == "next"


def test_corrupt_cache_fails_and_existing_page_is_never_overwritten(tmp_path):
    cache = MetadataCache(tmp_path)
    segment = live_segment()
    path = cache.page_path(segment, "c", 1, "request")
    path.parent.mkdir(parents=True)
    path.write_text("not json", encoding="utf-8")
    with pytest.raises(CacheError, match="corrupt"):
        cache.load_page(path, {})
    path.unlink()
    cache.publish_page(path, request_metadata={"x": 1}, response={"markets": []})
    original = path.read_bytes()
    with pytest.raises(CacheError, match="overwrite"):
        cache.publish_page(path, request_metadata={"x": 2}, response={"markets": [1]})
    assert path.read_bytes() == original


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_retryable_statuses_retry_and_count(status, tmp_path):
    session = FakeSession([FakeResponse(status), FakeResponse(200)])
    active = client(session)
    result = active.paginate(live_segment(), MetadataCache(tmp_path), cutoff_id="c", run_id="r")
    assert result.complete and len(session.calls) == 2
    assert active.counters.retries == 1
    assert active.counters.rate_limit_responses == (1 if status == 429 else 0)


def test_timeout_retries_and_retry_cap(tmp_path):
    session = FakeSession([TimeoutError("slow"), FakeResponse(200)])
    active = client(session)
    active.paginate(live_segment(), MetadataCache(tmp_path / "ok"), cutoff_id="c", run_id="r")
    assert active.counters.actual_http_attempts == 2
    exhausted = client(FakeSession([TimeoutError("x"), TimeoutError("x")]), max_retries=1)
    with pytest.raises(MetadataClientError, match="retry limit"):
        exhausted.paginate(live_segment(), MetadataCache(tmp_path / "bad"), cutoff_id="c", run_id="r")
    assert exhausted.counters.actual_http_attempts == 2


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_nonretryable_statuses_are_not_blindly_retried(status, tmp_path):
    session = FakeSession([FakeResponse(status), FakeResponse(200)])
    with pytest.raises(MetadataClientError, match="nonretryable"):
        client(session).paginate(live_segment(), MetadataCache(tmp_path), cutoff_id="c", run_id="r")
    assert len(session.calls) == 1


def market(ticker, settled="2025-08-10T00:00:00Z", **extra):
    return {"ticker": ticker, "settlement_ts": settled, **extra}


def august_month():
    return generate_months(normalize_inclusive_dates("2025-08-01", "2025-08-31"))[0]


def test_consolidation_audits_invalid_identity_and_time_and_preserves_fields():
    final = market("A", status="finalized", close_time="2025-08-09T00:00:00Z")
    result = consolidate_month([{}, {"ticker": "B", "settlement_ts": "bad"}, final], august_month())
    assert result.records == (final,)
    assert result.records[0]["status"] == "finalized"
    assert result.records[0]["close_time"] == "2025-08-09T00:00:00Z"
    assert {row["status"] for row in result.audit_records} == {
        "excluded_missing_ticker", "excluded_invalid_settlement_ts"
    }


def test_identical_duplicates_and_historical_live_overlap_collapse():
    value = market("A", venue="kalshi")
    result = consolidate_month([value, dict(value)], august_month())
    assert result.records == (value,)
    assert not result.audit_records


def test_conflicts_choose_unique_latest_updated_time_and_preserve_provenance():
    old = market("A", value=1, updated_time="2025-08-10T01:00:00Z")
    new = market("A", value=2, updated_time="2025-08-10T02:00:00Z")
    result = consolidate_month([new, old], august_month())
    assert result.records[0]["value"] == 2
    assert result.audit_records[0]["variants"]
    with pytest.raises(ConsolidationConflict):
        consolidate_month([market("B", value=1), market("B", value=2)], august_month())


def test_consolidation_sort_and_byte_identical_rerun(tmp_path):
    result = consolidate_month([
        market("Z", "2025-08-20T00:00:00Z"),
        market("B", "2025-08-10T00:00:00Z"),
        market("A", "2025-08-10T00:00:00Z"),
    ], august_month())
    assert [item["ticker"] for item in result.records] == ["A", "B", "Z"]
    path = monthly_output_path(tmp_path, "2025-08", result.source_set_hash)
    assert write_derived_jsonl(path, result.records) == "published"
    before = path.read_bytes()
    assert write_derived_jsonl(path, result.records) == "reused_identical"
    assert path.read_bytes() == before
    with pytest.raises(Exception, match="overwrite"):
        write_derived_jsonl(path, [market("OTHER")])


def test_dry_run_without_cutoff_sends_and_writes_nothing(tmp_path):
    raw_root = tmp_path / "raw"
    config = tmp_path / "config.toml"
    config.write_text(
        '[kalshi_metadata]\npage_size=1000\nmax_retries=5\nbackoff_base_seconds=1.0\n'
        'backoff_cap_seconds=30.0\nrequests_per_second=3.0\ntimeout_seconds=45.0\n'
        'mve_filter="exclude"\n', encoding="utf-8"
    )
    args = build_parser().parse_args([
        "--start-date", "2025-07-01", "--end-date", "2025-07-31",
        "--raw-root", str(raw_root), "--config", str(config), "--dry-run",
    ])
    session = FakeSession()
    assert run(args, session=session) == 0
    assert not session.calls and not raw_root.exists()


def test_dry_run_client_writes_nothing_and_sends_zero_requests(tmp_path):
    session = FakeSession()
    result = client(session).paginate(
        live_segment(), MetadataCache(tmp_path / "raw"), cutoff_id="c", run_id="r", dry_run=True
    )
    assert not result.complete and not session.calls and not (tmp_path / "raw").exists()


def test_secrets_are_removed_from_cache_and_manifest_material(tmp_path):
    cache = MetadataCache(tmp_path)
    path = cache.page_path(live_segment(), "c", 1, "r")
    cache.publish_page(
        path,
        request_metadata={"params": {"limit": 1}, "authorization": "SECRET"},
        response={"markets": []},
        metadata={"cookie": "SECRET", "safe": True},
    )
    text = path.read_text(encoding="utf-8")
    assert "SECRET" not in text and "authorization" not in text and "cookie" not in text


def test_imports_make_no_files_and_no_network_calls(tmp_path):
    modules = [
        "scripts.pipeline_v2.kalshi_metadata_planner",
        "scripts.pipeline_v2.kalshi_metadata_cache",
        "scripts.pipeline_v2.kalshi_metadata_client",
        "scripts.pipeline_v2.kalshi_metadata_consolidation",
        "scripts.pipeline_v2.pull_kalshi_settled_metadata",
    ]
    code = ";".join(f"import {name}" for name in modules)
    repository = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [sys.executable, "-c", code], cwd=tmp_path,
        env={"PYTHONPATH": str(repository), "PYTHONDONTWRITEBYTECODE": "1"},
        capture_output=True, text=True, check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert not list(tmp_path.iterdir())


def test_offline_cutoff_historical_live_consolidation_end_to_end(tmp_path):
    cutoff_payload = {"market_settled_ts": "2025-08-15T00:00:00Z"}
    cache = MetadataCache(tmp_path / "raw")
    cutoff_id, _ = cache.store_cutoff_snapshot(cutoff_payload)
    months = generate_months(normalize_inclusive_dates("2025-08-01", "2025-08-31"))
    segments = plan_endpoint_segments(months, datetime(2025, 8, 15, tzinfo=UTC))
    responses = FakeSession([
        FakeResponse(payload={"markets": [market("H", "2025-08-10T00:00:00Z")], "cursor": ""}),
        FakeResponse(payload={"markets": [market("L", "2025-08-20T00:00:00Z")], "cursor": ""}),
    ])
    active = client(responses)
    combined = []
    for segment in segments:
        combined.extend(active.paginate(segment, cache, cutoff_id=cutoff_id, run_id="r").markets)
    consolidated = consolidate_month(combined, months[0])
    output = monthly_output_path(cache.raw_root, "2025-08", consolidated.source_set_hash)
    write_derived_jsonl(output, consolidated.records)
    assert [json.loads(line)["ticker"] for line in output.read_text().splitlines()] == ["H", "L"]
    assert all("candlestick" not in call["url"] for call in responses.calls)


def _subprocess_environment(repository: Path, guard_directory: Path | None = None):
    paths = [str(repository)]
    if guard_directory is not None:
        paths.insert(0, str(guard_directory))
    environment = dict(os.environ)
    environment["PYTHONPATH"] = os.pathsep.join(paths)
    environment["PYTHONDONTWRITEBYTECODE"] = "1"
    return environment


def test_metadata_cli_module_help_succeeds():
    repository = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.pipeline_v2.pull_kalshi_settled_metadata",
            "--help",
        ],
        cwd=repository,
        env=_subprocess_environment(repository),
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "python -m scripts.pipeline_v2.pull_kalshi_settled_metadata" in completed.stdout


def test_metadata_cli_module_annual_dry_run_is_offline_and_writes_nothing(tmp_path):
    repository = Path(__file__).resolve().parents[1]
    guard = tmp_path / "guard"
    guard.mkdir()
    (guard / "sitecustomize.py").write_text(
        "import socket\n"
        "def blocked(*args, **kwargs):\n"
        "    raise AssertionError('network access attempted during dry-run')\n"
        "socket.create_connection = blocked\n"
        "socket.socket.connect = blocked\n",
        encoding="utf-8",
    )
    before = {path.relative_to(tmp_path): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    raw_root = tmp_path / "raw"
    manifest = tmp_path / "manifest.jsonl"
    completed = subprocess.run(
        [
            sys.executable,
            "-m",
            "scripts.pipeline_v2.pull_kalshi_settled_metadata",
            "--start-date",
            "2025-07-01",
            "--end-date",
            "2026-06-30",
            "--raw-root",
            str(raw_root),
            "--manifest",
            str(manifest),
            "--config",
            str(repository / "configs/pipeline_v2.toml"),
            "--page-size",
            "1000",
            "--dry-run",
        ],
        cwd=tmp_path,
        env=_subprocess_environment(repository, guard),
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode == 0, completed.stderr
    assert "normalized_range=[2025-07-01T00:00:00Z, 2026-07-01T00:00:00Z)" in completed.stdout
    for month in (
        "2025-07", "2025-08", "2025-09", "2025-10", "2025-11", "2025-12",
        "2026-01", "2026-02", "2026-03", "2026-04", "2026-05", "2026-06",
    ):
        assert month in completed.stdout
    assert "cutoff_source=unresolved" in completed.stdout
    assert "planned_endpoint_segments=unresolved" in completed.stdout
    assert not raw_root.exists() and not manifest.exists()
    after = {path.relative_to(tmp_path): path.read_bytes() for path in tmp_path.rglob("*") if path.is_file()}
    assert after == before


def test_metadata_cli_module_missing_required_dates_is_clear():
    repository = Path(__file__).resolve().parents[1]
    completed = subprocess.run(
        [sys.executable, "-m", "scripts.pipeline_v2.pull_kalshi_settled_metadata"],
        cwd=repository,
        env=_subprocess_environment(repository),
        capture_output=True,
        text=True,
        check=False,
    )
    assert completed.returncode != 0
    assert "--start-date" in completed.stderr
    assert "--end-date" in completed.stderr


def _write_test_config(path: Path):
    path.write_text(
        '[kalshi_metadata]\npage_size=1000\nmax_retries=2\nbackoff_base_seconds=0.0\n'
        'backoff_cap_seconds=1.0\nrequests_per_second=3.0\ntimeout_seconds=45.0\n'
        'mve_filter="exclude"\n',
        encoding="utf-8",
    )


def _live_cli_args(tmp_path: Path):
    config = tmp_path / "config.toml"
    cutoff = tmp_path / "cutoff.json"
    _write_test_config(config)
    cutoff.write_text('{"market_settled_ts":"2025-01-01T00:00:00Z"}\n', encoding="utf-8")
    return build_parser().parse_args(
        [
            "--start-date", "2025-08-01", "--end-date", "2025-08-31",
            "--raw-root", str(tmp_path / "raw"),
            "--manifest", str(tmp_path / "manifest.jsonl"),
            "--config", str(config), "--cutoff-snapshot", str(cutoff),
        ]
    )


def test_invalid_settlement_fails_preserves_audit_and_publishes_no_month(tmp_path):
    args = _live_cli_args(tmp_path)
    invalid = {
        "ticker": "BAD",
        "settlement_ts": "not-a-time",
        "close_time": "2025-08-01T00:00:00Z",
        "expiration_time": "2025-08-02T00:00:00Z",
        "complete": {"raw": "payload"},
    }
    session = FakeSession([FakeResponse(payload={"markets": [invalid], "cursor": ""})])
    assert run(args, session=session) == 2
    raw_root = Path(args.raw_root)
    audits = list((raw_root / "audits").glob("invalid_market_records_*.jsonl"))
    assert len(audits) == 1
    audit_payload = json.loads(audits[0].read_text().strip())
    assert audit_payload["raw_payload"] == invalid
    assert audit_payload["reason"] == "settlement_ts is missing or invalid"
    assert not list((raw_root / "2025-08").glob("settled_markets_*.jsonl"))
    raw_pages = list((raw_root / "2025-08" / "live_pages").glob("*.json"))
    assert len(raw_pages) == 1
    raw_before = raw_pages[0].read_bytes()
    audit_before = audits[0].read_bytes()

    assert run(args, session=FakeSession()) == 2
    assert audits[0].read_bytes() == audit_before
    assert raw_pages[0].read_bytes() == raw_before


def test_invalid_audit_serialization_is_deterministic(tmp_path):
    records = [market("B", settled="bad", nested={"x": 1}), {"settlement_ts": "bad", "x": 2}]
    audits, source_hash = invalid_record_audits(records)
    path = invalid_audit_path(tmp_path, source_hash)
    assert write_derived_jsonl(path, audits) == "published"
    content = path.read_bytes()
    assert write_derived_jsonl(path, audits) == "reused_identical"
    assert path.read_bytes() == content


def test_resolved_conflict_audit_is_immutable_and_contains_both_payloads(tmp_path):
    old = market("A", value=1, updated_time="2025-08-10T01:00:00Z")
    new = market("A", value=2, updated_time="2025-08-10T02:00:00Z")
    sources = [{"endpoint_tier": "historical"}, {"endpoint_tier": "live"}]
    result = consolidate_month([old, new], august_month(), source_information=sources)
    audit = result.audit_records[0]
    assert audit["selected_winner"] == new
    assert audit["resolution_rule"] == "unique_latest_valid_updated_time"
    assert {item["payload"]["value"] for item in audit["variants"]} == {1, 2}
    assert {item["source_information"][0]["endpoint_tier"] for item in audit["variants"]} == {
        "historical", "live"
    }
    path = monthly_audit_path(tmp_path, "2025-08", result.source_set_hash)
    assert write_derived_jsonl(path, result.audit_records) == "published"
    before = path.read_bytes()
    assert write_derived_jsonl(path, result.audit_records) == "reused_identical"
    assert path.read_bytes() == before
    with pytest.raises(ConsolidationError, match="overwrite"):
        write_derived_jsonl(path, [{"different": True}])


def test_cli_publishes_conflict_audit_before_completed_month(tmp_path):
    args = _live_cli_args(tmp_path)
    old = market("A", value=1, updated_time="2025-08-10T01:00:00Z")
    new = market("A", value=2, updated_time="2025-08-10T02:00:00Z")
    session = FakeSession([FakeResponse(payload={"markets": [old, new], "cursor": ""})])
    assert run(args, session=session) == 0
    month_dir = Path(args.raw_root) / "2025-08"
    audits = list(month_dir.glob("settled_markets_audit_*.jsonl"))
    completed = [
        path for path in month_dir.glob("settled_markets_*.jsonl")
        if "_audit_" not in path.name and "_provenance_" not in path.name
    ]
    assert len(audits) == len(completed) == 1
    variants = json.loads(audits[0].read_text())["variants"]
    assert {item["payload"]["value"] for item in variants} == {1, 2}


def test_audit_publication_failure_prevents_month_publication(tmp_path, monkeypatch):
    args = _live_cli_args(tmp_path)
    old = market("A", value=1, updated_time="2025-08-10T01:00:00Z")
    new = market("A", value=2, updated_time="2025-08-10T02:00:00Z")
    original_publish = metadata_cli.publish_immutable_bytes

    def fail_audit(path, content):
        if "_audit_" in Path(path).name:
            raise ConsolidationError("simulated audit publication failure")
        return original_publish(path, content)

    monkeypatch.setattr(metadata_cli, "publish_immutable_bytes", fail_audit)
    with pytest.raises(ConsolidationError, match="audit publication"):
        run(args, session=FakeSession([FakeResponse(payload={"markets": [old, new], "cursor": ""})]))
    month_dir = Path(args.raw_root) / "2025-08"
    assert not [path for path in month_dir.glob("settled_markets_*.jsonl") if "_audit_" not in path.name]
    assert list((month_dir / "live_pages").glob("*.json"))


def test_unresolved_cli_conflict_fails_before_month_publication(tmp_path):
    args = _live_cli_args(tmp_path)
    session = FakeSession([
        FakeResponse(payload={"markets": [market("A", value=1), market("A", value=2)], "cursor": ""})
    ])
    with pytest.raises(ConsolidationConflict):
        run(args, session=session)
    month_dir = Path(args.raw_root) / "2025-08"
    assert not [path for path in month_dir.glob("settled_markets_*.jsonl") if "_audit_" not in path.name]


def test_historical_cache_reuses_cutoff_global_chain_across_ranges(tmp_path):
    first_segment = EndpointSegment(
        "historical", HISTORICAL_PATH,
        datetime(2025, 7, 1, tzinfo=UTC), datetime(2025, 8, 1, tzinfo=UTC), None,
    )
    second_segment = EndpointSegment(
        "historical", HISTORICAL_PATH,
        datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 3, 1, tzinfo=UTC), None,
    )
    cache = MetadataCache(tmp_path)
    first_session = FakeSession([FakeResponse(payload={"markets": [market("H")], "cursor": ""})])
    client(first_session).paginate(first_segment, cache, cutoff_id="same-cutoff", run_id="one")
    second_session = FakeSession()
    active = client(second_session)
    result = active.paginate(second_segment, cache, cutoff_id="same-cutoff", run_id="two")
    assert result.complete and result.markets[0]["ticker"] == "H"
    assert not second_session.calls and active.counters.cache_hits == 1


def test_live_cache_identity_remains_range_specific(tmp_path):
    first = live_segment()
    second = EndpointSegment(
        "live", LIVE_PATH,
        datetime(2025, 8, 2, tzinfo=UTC), datetime(2025, 9, 1, tzinfo=UTC), "2025-08",
    )
    cache = MetadataCache(tmp_path)
    first_session = FakeSession([FakeResponse()])
    client(first_session).paginate(first, cache, cutoff_id="c", run_id="one")
    second_session = FakeSession([FakeResponse()])
    client(second_session).paginate(second, cache, cutoff_id="c", run_id="two")
    assert len(second_session.calls) == 1
    assert len(list((tmp_path / "2025-08" / "live_pages").glob("*.json"))) == 2


def test_malformed_json_and_programming_errors_are_not_retried(tmp_path):
    malformed = FakeSession([MalformedJsonResponse(), FakeResponse()])
    with pytest.raises(RequestFailure) as malformed_failure:
        client(malformed).paginate(live_segment(), MetadataCache(tmp_path / "json"), cutoff_id="c", run_id="r")
    assert malformed_failure.value.attempts == 1 and len(malformed.calls) == 1

    programming = FakeSession([RuntimeError("programming failure"), FakeResponse()])
    with pytest.raises(RequestFailure) as programming_failure:
        client(programming).paginate(live_segment(), MetadataCache(tmp_path / "program"), cutoff_id="c", run_id="r")
    assert programming_failure.value.attempts == 1 and len(programming.calls) == 1

    malformed_structure = FakeSession([FakeResponse(payload={"unexpected": []}), FakeResponse()])
    with pytest.raises(RequestFailure) as structure_failure:
        client(malformed_structure).paginate(
            live_segment(), MetadataCache(tmp_path / "structure"), cutoff_id="c", run_id="r"
        )
    assert structure_failure.value.attempts == 1 and len(malformed_structure.calls) == 1
    assert not list((tmp_path / "structure").rglob("*.json"))


@pytest.mark.parametrize("transport_error", [TimeoutError("timeout"), ConnectionError("connection")])
def test_timeout_and_connection_errors_are_retried(transport_error, tmp_path):
    session = FakeSession([transport_error, FakeResponse()])
    active = client(session)
    assert active.paginate(live_segment(), MetadataCache(tmp_path), cutoff_id="c", run_id="r").complete
    assert len(session.calls) == 2 and active.counters.retries == 1


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_retry_exhaustion_manifest_has_true_counts_and_preserves_raw_pages(status, tmp_path):
    cache = MetadataCache(tmp_path)
    preserved = cache.raw_root / "preserved.json"
    preserved.write_bytes(b"immutable raw page")
    session = FakeSession([FakeResponse(status), FakeResponse(status), FakeResponse(status)])
    active = client(session, max_retries=2)
    manifest = []
    with pytest.raises(RequestFailure):
        active.paginate(
            live_segment(), cache, cutoff_id="c", run_id="r", manifest_sink=manifest.append
        )
    assert len(manifest) == 1
    record = manifest[0]
    assert record["http_attempt_count"] == record["actual_request_count"] == 3
    assert record["retry_count"] == 2
    assert record["rate_limit_count"] == (3 if status == 429 else 0)
    assert record["http_status"] == status
    assert active.counters.actual_http_attempts == record["actual_request_count"]
    assert active.counters.retries == record["retry_count"]
    assert active.counters.rate_limit_responses == record["rate_limit_count"]
    assert preserved.read_bytes() == b"immutable raw page"


def _multi_month_cli_args(tmp_path: Path):
    args = _live_cli_args(tmp_path)
    args.end_date = "2025-09-30"
    return args


def _completed_month_files(raw_root: Path):
    return [
        path
        for path in raw_root.glob("20??-??/settled_markets_*.jsonl")
        if "_audit_" not in path.name and "_provenance_" not in path.name
    ]


def test_later_month_unresolved_conflict_leaves_entire_run_uncommitted(tmp_path):
    args = _multi_month_cli_args(tmp_path)
    session = FakeSession(
        [
            FakeResponse(payload={"markets": [market("AUG")], "cursor": ""}),
            FakeResponse(
                payload={
                    "markets": [
                        market("SEP", "2025-09-10T00:00:00Z", value=1),
                        market("SEP", "2025-09-10T00:00:00Z", value=2),
                    ],
                    "cursor": "",
                }
            ),
        ]
    )
    with pytest.raises(ConsolidationConflict):
        run(args, session=session)
    raw_root = Path(args.raw_root)
    assert not list((raw_root / "run_commits").glob("*.json"))
    assert not _completed_month_files(raw_root)


def test_later_audit_failure_leaves_no_commit_and_no_month_is_complete(tmp_path, monkeypatch):
    args = _multi_month_cli_args(tmp_path)
    aug_old = market("A", value=1, updated_time="2025-08-10T01:00:00Z")
    aug_new = market("A", value=2, updated_time="2025-08-10T02:00:00Z")
    sep_old = market("S", "2025-09-10T00:00:00Z", value=1, updated_time="2025-09-10T01:00:00Z")
    sep_new = market("S", "2025-09-10T00:00:00Z", value=2, updated_time="2025-09-10T02:00:00Z")
    original = metadata_cli.publish_immutable_bytes

    def fail_second_audit(path, content):
        if "2025-09" in str(path) and "_audit_" in Path(path).name:
            raise ConsolidationError("later audit failed")
        return original(path, content)

    monkeypatch.setattr(metadata_cli, "publish_immutable_bytes", fail_second_audit)
    with pytest.raises(ConsolidationError, match="later audit"):
        run(
            args,
            session=FakeSession(
                [
                    FakeResponse(payload={"markets": [aug_old, aug_new], "cursor": ""}),
                    FakeResponse(payload={"markets": [sep_old, sep_new], "cursor": ""}),
                ]
            ),
        )
    raw_root = Path(args.raw_root)
    assert not list((raw_root / "run_commits").glob("*.json"))
    assert not _completed_month_files(raw_root)


def test_successful_multi_month_run_commits_all_artifacts_and_is_idempotent(tmp_path):
    args = _multi_month_cli_args(tmp_path)
    responses = [
        FakeResponse(payload={"markets": [market("AUG")], "cursor": ""}),
        FakeResponse(payload={"markets": [market("SEP", "2025-09-10T00:00:00Z")], "cursor": ""}),
    ]
    assert run(args, session=FakeSession(responses)) == 0
    raw_root = Path(args.raw_root)
    commits = list((raw_root / "run_commits").glob("*.json"))
    assert len(commits) == 1
    commit_before = commits[0].read_bytes()
    record = json.loads(commit_before)
    assert record["selected_months"] == ["2025-08", "2025-09"]
    assert {item["month"] for item in record["artifacts"]} == {"2025-08", "2025-09"}
    assert all(Path(item["path"]).exists() for item in record["artifacts"])
    assert len(record["source_pages"]) == 2
    assert run(args, session=FakeSession()) == 0
    assert commits[0].read_bytes() == commit_before
    assert len(list((raw_root / "run_commits").glob("*.json"))) == 1


def test_resume_reuses_but_does_not_accept_orphan_artifact_as_complete(tmp_path, monkeypatch):
    args = _live_cli_args(tmp_path)
    original = metadata_cli.publish_immutable_bytes

    def fail_commit(path, content):
        if "run_commits" in str(path):
            raise ConsolidationError("commit interruption")
        return original(path, content)

    monkeypatch.setattr(metadata_cli, "publish_immutable_bytes", fail_commit)
    with pytest.raises(ConsolidationError, match="commit interruption"):
        run(args, session=FakeSession([FakeResponse(payload={"markets": [market("A")], "cursor": ""})]))
    raw_root = Path(args.raw_root)
    assert _completed_month_files(raw_root)
    assert not list((raw_root / "run_commits").glob("*.json"))
    monkeypatch.setattr(metadata_cli, "publish_immutable_bytes", original)
    assert run(args, session=FakeSession()) == 0
    assert len(list((raw_root / "run_commits").glob("*.json"))) == 1


def test_duplicate_pages_retain_exact_distinct_page_provenance(tmp_path):
    old = market("A", value=1, updated_time="2025-08-10T01:00:00Z")
    new = market("A", value=2, updated_time="2025-08-10T02:00:00Z")
    session = FakeSession(
        [
            FakeResponse(payload={"markets": [old], "cursor": "next"}),
            FakeResponse(payload={"markets": [new], "cursor": ""}),
        ]
    )
    result = client(session).paginate(
        live_segment(), MetadataCache(tmp_path), cutoff_id="cutoff", run_id="run"
    )
    assert len(result.fetched_records) == 2
    first, second = [item.provenance for item in result.fetched_records]
    for key in (
        "immutable_page_path", "page_response_sha256", "request_id", "page_number",
        "request_cursor_hash", "response_cursor_hash",
    ):
        assert first[key] != second[key]
    consolidated = consolidate_month(
        result.markets,
        august_month(),
        source_information=[item.provenance for item in result.fetched_records],
    )
    provenance = [item["source_information"][0] for item in consolidated.audit_records[0]["variants"]]
    assert {item["page_number"] for item in provenance} == {1, 2}


def test_invalid_audit_has_originating_page_provenance(tmp_path):
    invalid = market("BAD", settled="bad")
    result = client(FakeSession([FakeResponse(payload={"markets": [invalid], "cursor": ""})])).paginate(
        live_segment(), MetadataCache(tmp_path), cutoff_id="c", run_id="r"
    )
    audits, _ = invalid_record_audits(
        result.markets, source_information=[item.provenance for item in result.fetched_records]
    )
    source = audits[0]["source_information"]
    assert source["immutable_page_path"]
    assert source["page_response_sha256"]
    assert source["request_id"]
    assert source["page_number"] == 1


def test_historical_live_overlap_audit_preserves_both_endpoint_provenances():
    old = market("A", value=1, updated_time="2025-08-10T01:00:00Z")
    new = market("A", value=2, updated_time="2025-08-10T02:00:00Z")
    result = consolidate_month(
        [old, new],
        august_month(),
        source_information=[
            {"endpoint_tier": "historical", "immutable_page_path": "historical/page.json"},
            {"endpoint_tier": "live", "immutable_page_path": "2025-08/page.json"},
        ],
    )
    variants = result.audit_records[0]["variants"]
    assert {item["source_information"][0]["endpoint_tier"] for item in variants} == {
        "historical", "live"
    }


@pytest.mark.parametrize(
    "sensitive_key",
    ["Authorization", "proxy-authorization", "Cookie", "set_cookie", "X-API-Key", "token", "access_token", "refresh-token", "client_secret", "password", "credential"],
)
def test_sensitive_nested_response_is_rejected_without_leak_or_retry(
    sensitive_key, tmp_path, capsys
):
    secret_value = "NEVER-EXPOSE-THIS"
    payload = {
        "markets": [market("A")],
        "cursor": "",
        "nested": [{sensitive_key: secret_value}],
    }
    session = FakeSession([FakeResponse(payload=payload), FakeResponse()])
    manifest = []
    with pytest.raises(SensitiveResponseError) as failure:
        client(session).paginate(
            live_segment(), MetadataCache(tmp_path), cutoff_id="c", run_id="r",
            manifest_sink=manifest.append,
        )
    assert len(session.calls) == 1
    assert secret_value not in str(failure.value)
    assert secret_value not in json.dumps(manifest)
    captured = capsys.readouterr()
    assert secret_value not in captured.out + captured.err
    assert not list(tmp_path.rglob("*.json"))
    assert manifest == []


def test_atomic_publication_interruption_and_write_failure_leave_no_target(tmp_path):
    destination = tmp_path / "immutable.json"

    def interrupt(_temporary, _destination):
        raise RuntimeError("before install")

    with pytest.raises(RuntimeError, match="before install"):
        publish_immutable_bytes(destination, b"complete", before_install=interrupt)
    assert not destination.exists()
    assert not list(tmp_path.glob(".*.tmp-*"))

    def failed_write(_descriptor, _content):
        raise OSError("write failed")

    with pytest.raises(OSError, match="write failed"):
        publish_immutable_bytes(destination, b"complete", write_bytes=failed_write)
    assert not destination.exists()


def test_atomic_publication_races_reuse_identical_and_reject_different(tmp_path):
    destination = tmp_path / "immutable.json"

    def winning_racer(_temporary, target):
        publish_immutable_bytes(target, b"same")

    assert publish_immutable_bytes(destination, b"same", before_install=winning_racer) == "reused_identical"
    assert destination.read_bytes() == b"same"
    with pytest.raises(ImmutableConflict):
        publish_immutable_bytes(destination, b"different")


@pytest.mark.parametrize(
    "sensitive_key",
    ["authorization", "cookie", "token", "client_secret", "password", "credentials"],
)
def test_sensitive_cutoff_is_rejected_before_storage_routing_or_leak(
    sensitive_key, tmp_path, capsys
):
    secret_value = "CUTOFF-SECRET-NEVER-EXPOSE"
    config = tmp_path / "config.toml"
    _write_test_config(config)
    args = build_parser().parse_args(
        [
            "--start-date", "2025-08-01", "--end-date", "2025-08-31",
            "--raw-root", str(tmp_path / "raw"), "--manifest", str(tmp_path / "manifest.jsonl"),
            "--config", str(config),
        ]
    )
    payload = {
        "market_settled_ts": "2025-01-01T00:00:00Z",
        "nested": [{"deeper": {sensitive_key: secret_value}}],
    }
    session = FakeSession([FakeResponse(payload=payload), FakeResponse()])
    with pytest.raises(SensitiveResponseError) as failure:
        run(args, session=session)
    captured = capsys.readouterr()
    assert len(session.calls) == 1
    assert secret_value not in str(failure.value)
    assert secret_value not in captured.out + captured.err
    assert not (tmp_path / "raw").exists()
    assert not (tmp_path / "manifest.jsonl").exists()


def test_cutoff_cache_boundary_also_rejects_sensitive_payload(tmp_path):
    cache = MetadataCache(tmp_path)
    with pytest.raises(SensitiveResponseError):
        cache.store_cutoff_snapshot(
            {
                "market_settled_ts": "2025-01-01T00:00:00Z",
                "nested": [{"Authorization": "DO-NOT-CACHE"}],
            }
        )
    assert not list(tmp_path.iterdir())


def test_commit_bytes_are_deterministic_and_identical_publication_is_idempotent(
    tmp_path, monkeypatch
):
    record_one = {"schema_version": 1, "run_id": "same", "artifacts": [], "source_pages": []}
    record_two = json.loads(json.dumps(record_one))
    assert _commit_bytes(record_one) == _commit_bytes(record_two)
    path = tmp_path / "run_same.json"
    original = metadata_cli.publish_immutable_bytes
    raced = False

    def simulated_race(target, content):
        nonlocal raced
        if not raced:
            raced = True
            original(target, content)
        return original(target, content)

    monkeypatch.setattr(metadata_cli, "publish_immutable_bytes", simulated_race)
    assert _publish_run_commit(path, record_one) == "reused_identical"
    assert _publish_run_commit(path, record_two) == "reused_identical"
    with pytest.raises(ImmutableConflict):
        _publish_run_commit(path, {**record_one, "run_id": "different"})


def _effective_identity_for(args, *, page_size=None, max_retries=None):
    interval = normalize_inclusive_dates(args.start_date, args.end_date)
    months = filter_month(generate_months(interval), args.month)
    config = {
        "page_size": 1000,
        "max_retries": 5,
        "backoff_base_seconds": 1.0,
        "backoff_cap_seconds": 30.0,
        "requests_per_second": 3.0,
        "timeout_seconds": 45.0,
        "mve_filter": "exclude",
    }
    settings = {
        "page_size": page_size if page_size is not None else (args.page_size or config["page_size"]),
        "max_retries": max_retries if max_retries is not None else (
            args.max_retries if args.max_retries is not None else config["max_retries"]
        ),
        "backoff_base_seconds": args.backoff_base_seconds if args.backoff_base_seconds is not None else config["backoff_base_seconds"],
        "backoff_cap_seconds": args.backoff_cap_seconds if args.backoff_cap_seconds is not None else config["backoff_cap_seconds"],
        "requests_per_second": args.requests_per_second if args.requests_per_second is not None else config["requests_per_second"],
        "timeout_seconds": args.timeout_seconds if args.timeout_seconds is not None else config["timeout_seconds"],
    }
    segments = plan_endpoint_segments(months, datetime(2025, 1, 1, tzinfo=UTC), historical_mode=args.historical_mode, live_mode=args.live_mode)
    effective = _canonical_effective_configuration(
        args, settings, config, interval, months, "cutoff", segments
    )
    identity = {
        "schema_version": 1,
        "date_range": {
            "start_utc": interval.start.isoformat(),
            "end_utc_exclusive": interval.end.isoformat(),
        },
        "selected_months": [month.month for month in months],
        "cutoff_snapshot_id": "cutoff",
        "effective_configuration": effective,
    }
    return effective, _transaction_run_id(identity)


def test_effective_configuration_changes_run_id_and_normalizes_equal_overrides(tmp_path):
    args = _live_cli_args(tmp_path)
    baseline, baseline_id = _effective_identity_for(args)
    _, page_id = _effective_identity_for(args, page_size=500)
    _, retry_id = _effective_identity_for(args, max_retries=9)
    assert len({baseline_id, page_id, retry_id}) == 3
    args.live_mode = "require"
    _, mode_id = _effective_identity_for(args)
    assert mode_id != baseline_id
    args.live_mode = "auto"
    args.page_size = 1000
    equal_override, equal_id = _effective_identity_for(args)
    assert equal_id == baseline_id
    assert equal_override == baseline


def test_commit_reports_override_and_different_effective_settings_do_not_collide(tmp_path):
    args = _live_cli_args(tmp_path)
    args.page_size = 500
    assert run(args, session=FakeSession([FakeResponse(payload={"markets": [market("A")], "cursor": ""})])) == 0
    args.page_size = 250
    assert run(args, session=FakeSession([FakeResponse(payload={"markets": [market("A")], "cursor": ""})])) == 0
    commits = [json.loads(path.read_text()) for path in (Path(args.raw_root) / "run_commits").glob("*.json")]
    assert len(commits) == 2
    assert {record["effective_configuration"]["page_size"] for record in commits} == {250, 500}
    assert len({record["run_id"] for record in commits}) == 2


def test_terminal_empty_page_is_committed_cached_and_required_for_validity(tmp_path):
    args = _live_cli_args(tmp_path)
    assert run(args, session=FakeSession([FakeResponse(payload={"markets": [], "cursor": ""})])) == 0
    commit_path = next((Path(args.raw_root) / "run_commits").glob("*.json"))
    record = json.loads(commit_path.read_text())
    assert len(record["source_pages"]) == 1
    source = record["source_pages"][0]
    assert source["row_count"] == 0 and source["terminal_page"] is True
    assert _valid_commit(commit_path, record)

    # Resume uses the cached empty terminal page and reproduces the logical commit.
    assert run(args, session=FakeSession()) == 0
    assert json.loads(commit_path.read_text())["source_pages"][0]["initial_acquisition_status"] == "fetched"

    page = Path(source["immutable_page_path"])
    original = page.read_bytes()
    page.unlink()
    assert not _valid_commit(commit_path, record)
    publish_immutable_bytes(page, original)
    assert _valid_commit(commit_path, record)
    page.write_bytes(b"corrupt")
    assert not _valid_commit(commit_path, record)


@pytest.mark.parametrize(
    ("wrapped", "sensitive_key"),
    [
        (False, "Authorization"),
        (True, "proxy-authorization"),
        (False, "Set_Cookie"),
        (True, "X.API.KEY"),
        (False, "ACCESS-TOKEN"),
        (True, "refresh_token"),
        (False, "Client.Secret"),
        (True, "Credentials"),
    ],
)
def test_untrusted_pinned_cutoff_rejects_nested_sensitive_variants_without_side_effects(
    wrapped, sensitive_key, tmp_path, capsys
):
    secret_value = "PINNED-SECRET-NEVER-EXPOSE"
    cutoff_object = {
        "market_settled_ts": "2025-01-01T00:00:00Z",
        "nested": [{"deeper": {sensitive_key: secret_value}}],
    }
    decoded = {"response": cutoff_object} if wrapped else cutoff_object
    cutoff = tmp_path / "untrusted.json"
    cutoff.write_text(json.dumps(decoded), encoding="utf-8")
    config = tmp_path / "config.toml"
    _write_test_config(config)
    before = {path.name: path.read_bytes() for path in tmp_path.iterdir() if path.is_file()}
    args = build_parser().parse_args(
        [
            "--start-date", "2025-08-01", "--end-date", "2025-08-31",
            "--raw-root", str(tmp_path / "raw"), "--manifest", str(tmp_path / "manifest.jsonl"),
            "--config", str(config), "--cutoff-snapshot", str(cutoff),
        ]
    )
    with pytest.raises(SensitiveResponseError) as failure:
        run(args, session=FakeSession())
    captured = capsys.readouterr()
    after = {path.name: path.read_bytes() for path in tmp_path.iterdir() if path.is_file()}
    assert before == after
    assert secret_value not in str(failure.value) + captured.out + captured.err
    assert "cutoff_id=" not in captured.out and "run_id=" not in captured.out
    assert not (tmp_path / "raw").exists() and not (tmp_path / "manifest.jsonl").exists()


@pytest.mark.parametrize(
    ("cutoff_value", "historical_mode", "live_mode"),
    [
        ("2026-01-01T00:00:00Z", "skip", "auto"),
        ("2025-01-01T00:00:00Z", "auto", "skip"),
        ("2025-01-01T00:00:00Z", "skip", "skip"),
        ("2025-08-15T00:00:00Z", "skip", "auto"),
    ],
)
def test_required_endpoint_coverage_cannot_be_skipped(
    cutoff_value, historical_mode, live_mode, tmp_path, capsys
):
    args = _live_cli_args(tmp_path)
    Path(args.cutoff_snapshot).write_text(
        json.dumps({"market_settled_ts": cutoff_value}), encoding="utf-8"
    )
    args.historical_mode = historical_mode
    args.live_mode = live_mode
    session = FakeSession()
    with pytest.raises(ValueError, match="required month coverage"):
        run(args, session=session)
    captured = capsys.readouterr()
    assert not session.calls
    assert "run_complete=true" not in captured.out
    assert not (Path(args.raw_root) / "run_commits").exists()


def test_page_limit_terminal_first_page_is_preserved_but_never_committed(tmp_path, capsys):
    args = _live_cli_args(tmp_path)
    args.limit_pages = 1
    session = FakeSession([FakeResponse(payload={"markets": [], "cursor": ""})])
    assert run(args, session=session) == metadata_cli.SMOKE_INCOMPLETE_EXIT
    captured = capsys.readouterr()
    assert len(session.calls) == 1
    assert "run_complete=true" not in captured.out
    assert "smoke_incomplete=true" in captured.err
    assert list((Path(args.raw_root) / "2025-08" / "live_pages").glob("*.json"))
    assert not (Path(args.raw_root) / "run_commits").exists()


def test_new_commit_must_pass_final_validation_before_success(tmp_path, monkeypatch, capsys):
    args = _live_cli_args(tmp_path)
    monkeypatch.setattr(metadata_cli, "_valid_commit", lambda *_args, **_kwargs: False)
    with pytest.raises(ConsolidationError, match="failed final transaction validation"):
        run(args, session=FakeSession([FakeResponse(payload={"markets": [market("A")], "cursor": ""})]))
    captured = capsys.readouterr()
    assert "run_complete=true" not in captured.out
    assert len(list((Path(args.raw_root) / "run_commits").glob("*.json"))) == 1


def test_success_is_reported_only_after_full_commit_validation(tmp_path, monkeypatch, capsys):
    args = _live_cli_args(tmp_path)
    original = metadata_cli._valid_commit
    calls = []

    def tracked(path, expected):
        result = original(path, expected)
        calls.append(result)
        return result

    monkeypatch.setattr(metadata_cli, "_valid_commit", tracked)
    assert run(args, session=FakeSession([FakeResponse(payload={"markets": [], "cursor": ""})])) == 0
    captured = capsys.readouterr()
    assert calls and calls[-1] is True
    assert "run_complete=true" in captured.out


def _monthly_artifact(commit, kind):
    return next(item for item in commit["artifacts"] if item["kind"] == kind)


def test_ordinary_monthly_record_has_durable_separate_provenance(tmp_path):
    args = _live_cli_args(tmp_path)
    raw = market("A", title="unchanged", nested={"value": 1})
    assert run(args, session=FakeSession([FakeResponse(payload={"markets": [raw], "cursor": ""})])) == 0
    commit_path = next((Path(args.raw_root) / "run_commits").glob("*.json"))
    commit = json.loads(commit_path.read_text())
    monthly = _monthly_artifact(commit, "monthly_consolidation")
    provenance = _monthly_artifact(commit, "record_provenance")
    assert json.loads(Path(monthly["path"]).read_text()) == raw
    entry = json.loads(Path(provenance["path"]).read_text())
    assert entry["ticker"] == "A"
    assert entry["selected_payload_sha256"] == payload_sha256(raw)
    assert len(entry["source_associations"]) == 1
    assert entry["monthly_output_artifact"]["sha256"] == monthly["sha256"]
    assert _valid_commit(commit_path, commit)


def test_identical_duplicate_pages_persist_both_sources_deterministically(tmp_path):
    args = _live_cli_args(tmp_path)
    raw = market("A", title="identical")
    responses = FakeSession(
        [
            FakeResponse(payload={"markets": [raw], "cursor": "next"}),
            FakeResponse(payload={"markets": [dict(raw)], "cursor": ""}),
        ]
    )
    assert run(args, session=responses) == 0
    commit = json.loads(next((Path(args.raw_root) / "run_commits").glob("*.json")).read_text())
    provenance_path = Path(_monthly_artifact(commit, "record_provenance")["path"])
    before = provenance_path.read_bytes()
    entry = json.loads(before)
    assert {source["page_number"] for source in entry["source_associations"]} == {1, 2}
    assert len(entry["payload_variants"]) == 1
    assert run(args, session=FakeSession()) == 0
    assert provenance_path.read_bytes() == before


def test_identical_historical_live_overlap_persists_both_tiers(tmp_path):
    args = _live_cli_args(tmp_path)
    Path(args.cutoff_snapshot).write_text(
        '{"market_settled_ts":"2025-08-15T00:00:00Z"}', encoding="utf-8"
    )
    raw = market("A", "2025-08-10T00:00:00Z")
    session = FakeSession(
        [
            FakeResponse(payload={"markets": [raw], "cursor": ""}),
            FakeResponse(payload={"markets": [dict(raw)], "cursor": ""}),
        ]
    )
    assert run(args, session=session) == 0
    commit = json.loads(next((Path(args.raw_root) / "run_commits").glob("*.json")).read_text())
    entry = json.loads(Path(_monthly_artifact(commit, "record_provenance")["path"]).read_text())
    assert {source["endpoint_tier"] for source in entry["source_associations"]} == {
        "historical", "live"
    }


def test_resolved_conflict_provenance_marks_winner_and_all_variants(tmp_path):
    args = _live_cli_args(tmp_path)
    old = market("A", value=1, updated_time="2025-08-10T01:00:00Z")
    new = market("A", value=2, updated_time="2025-08-10T02:00:00Z")
    assert run(args, session=FakeSession([FakeResponse(payload={"markets": [old, new], "cursor": ""})])) == 0
    commit = json.loads(next((Path(args.raw_root) / "run_commits").glob("*.json")).read_text())
    entry = json.loads(Path(_monthly_artifact(commit, "record_provenance")["path"]).read_text())
    assert len(entry["payload_variants"]) == 2
    assert sum(bool(item["selected"]) for item in entry["payload_variants"]) == 1
    assert entry["selected_payload_sha256"] == payload_sha256(new)


def test_missing_or_corrupt_provenance_invalidates_commit(tmp_path):
    args = _live_cli_args(tmp_path)
    assert run(args, session=FakeSession([FakeResponse(payload={"markets": [market("A")], "cursor": ""})])) == 0
    commit_path = next((Path(args.raw_root) / "run_commits").glob("*.json"))
    commit = json.loads(commit_path.read_text())
    provenance = Path(_monthly_artifact(commit, "record_provenance")["path"])
    original = provenance.read_bytes()
    provenance.unlink()
    assert not _valid_commit(commit_path, commit)
    publish_immutable_bytes(provenance, original)
    assert _valid_commit(commit_path, commit)
    provenance.write_text("{}\n", encoding="utf-8")
    assert not _valid_commit(commit_path, commit)


def test_missing_provenance_entry_prevents_success(tmp_path, monkeypatch, capsys):
    args = _live_cli_args(tmp_path)
    original = metadata_cli.consolidate_month

    def without_provenance(*call_args, **call_kwargs):
        result = original(*call_args, **call_kwargs)
        return replace(result, record_provenance=())

    monkeypatch.setattr(metadata_cli, "consolidate_month", without_provenance)
    with pytest.raises(ConsolidationError, match="provenance does not cover"):
        run(args, session=FakeSession([FakeResponse(payload={"markets": [market("A")], "cursor": ""})]))
    assert "run_complete=true" not in capsys.readouterr().out


def test_provenance_publication_failure_prevents_monthly_completion(tmp_path, monkeypatch):
    args = _live_cli_args(tmp_path)
    original = metadata_cli.publish_immutable_bytes

    def fail_provenance(path, content):
        if "_provenance_" in Path(path).name:
            raise ConsolidationError("provenance publication failed")
        return original(path, content)

    monkeypatch.setattr(metadata_cli, "publish_immutable_bytes", fail_provenance)
    with pytest.raises(ConsolidationError, match="provenance publication"):
        run(args, session=FakeSession([FakeResponse(payload={"markets": [market("A")], "cursor": ""})]))
    assert not _completed_month_files(Path(args.raw_root))
    assert not list((Path(args.raw_root) / "run_commits").glob("*.json"))


CAMELCASE_SENSITIVE_KEYS = (
    "accessToken", "refreshToken", "clientSecret", "apiKey", "xApiKey",
    "setCookie", "proxyAuthorization", "AccessToken", "ACCESS_TOKEN",
    "access-token", "access token", "accesstoken",
)
SENSITIVE_NESTINGS = ("direct", "deep", "list", "dictionary_in_list")


def _with_nested_sensitive(base, key, value, nesting):
    payload = dict(base)
    if nesting == "direct":
        payload[key] = value
    elif nesting == "deep":
        payload["outer"] = {"middle": {"inner": {key: value}}}
    elif nesting == "list":
        payload["outer"] = [{key: value}]
    elif nesting == "dictionary_in_list":
        payload["outer"] = [{"container": [{"deeper": {key: value}}]}]
    else:
        raise AssertionError(nesting)
    return payload


@pytest.mark.parametrize("key", CAMELCASE_SENSITIVE_KEYS)
def test_separator_free_sensitive_key_canonicalization(key):
    assert canonical_sensitive_key(key) == "accesstoken" or key not in {
        "accessToken", "AccessToken", "ACCESS_TOKEN", "access-token", "access token", "accesstoken"
    }
    assert canonical_sensitive_key(key) in {
        "accesstoken", "refreshtoken", "clientsecret", "apikey", "xapikey",
        "setcookie", "proxyauthorization",
    }


@pytest.mark.parametrize("key", CAMELCASE_SENSITIVE_KEYS)
@pytest.mark.parametrize("nesting", SENSITIVE_NESTINGS)
def test_fetched_market_rejects_camelcase_sensitive_keys_without_artifacts(
    key, nesting, tmp_path, capsys
):
    secret_value = "MARKET-CAMELCASE-SECRET"
    payload = _with_nested_sensitive(
        {"markets": [market("A")], "cursor": ""}, key, secret_value, nesting
    )
    session = FakeSession([FakeResponse(payload=payload), FakeResponse()])
    manifest = []
    with pytest.raises(SensitiveResponseError) as failure:
        client(session).paginate(
            live_segment(), MetadataCache(tmp_path / "raw"), cutoff_id="c", run_id="r",
            manifest_sink=manifest.append,
        )
    captured = capsys.readouterr()
    assert len(session.calls) == 1
    assert secret_value not in str(failure.value) + captured.out + captured.err
    assert secret_value not in json.dumps(manifest)
    assert not (tmp_path / "raw").exists()
    assert not any(secret_value in path.name for path in tmp_path.rglob("*"))


@pytest.mark.parametrize("key", CAMELCASE_SENSITIVE_KEYS)
def test_fetched_cutoff_rejects_camelcase_sensitive_keys_without_artifacts(
    key, tmp_path, capsys
):
    secret_value = "FETCHED-CUTOFF-CAMELCASE-SECRET"
    payload = _with_nested_sensitive(
        {"market_settled_ts": "2025-01-01T00:00:00Z"},
        key,
        secret_value,
        "dictionary_in_list",
    )
    session = FakeSession([FakeResponse(payload=payload), FakeResponse()])
    with pytest.raises(SensitiveResponseError) as failure:
        client(session).fetch_cutoff()
    captured = capsys.readouterr()
    assert len(session.calls) == 1
    assert secret_value not in str(failure.value) + captured.out + captured.err
    assert not list(tmp_path.iterdir())


@pytest.mark.parametrize("key", CAMELCASE_SENSITIVE_KEYS)
def test_pinned_cutoff_rejects_camelcase_sensitive_keys_without_derived_artifacts(
    key, tmp_path, capsys
):
    secret_value = "PINNED-CUTOFF-CAMELCASE-SECRET"
    payload = _with_nested_sensitive(
        {"market_settled_ts": "2025-01-01T00:00:00Z"},
        key,
        secret_value,
        "list",
    )
    snapshot = tmp_path / "untrusted-cutoff.json"
    snapshot.write_text(json.dumps({"response": payload}), encoding="utf-8")
    original = snapshot.read_bytes()
    with pytest.raises(SensitiveResponseError) as failure:
        MetadataCache.load_cutoff_snapshot(snapshot)
    captured = capsys.readouterr()
    assert secret_value not in str(failure.value) + captured.out + captured.err
    assert list(tmp_path.iterdir()) == [snapshot]
    assert snapshot.read_bytes() == original
    assert secret_value not in snapshot.name


def test_harmless_related_keys_are_not_falsely_rejected(tmp_path):
    harmless = {
        "token_count": 3,
        "password_policy": "strict",
        "cookie_preferences": "minimal",
        "secretary": "name",
        "authorization_status": "none",
    }
    response = {"markets": [market("A", **harmless)], "cursor": ""}
    reject_sensitive_response(response)
    cache = MetadataCache(tmp_path)
    path = cache.page_path(live_segment(), "c", 1, "harmless")
    cache.publish_page(path, request_metadata={"safe": True}, response=response)
    stored = path.read_text(encoding="utf-8")
    for key in harmless:
        assert key in stored


def test_sensitive_market_bypasses_production_manifest_sink_and_all_filesystem_publication(
    tmp_path, capsys
):
    secret_value = "NEVER-PERSIST-SENSITIVE-MARKET-VALUE"
    raw_root = tmp_path / "raw"
    manifest = tmp_path / "operational" / "manifest.jsonl"
    payload = {
        "markets": [market("A")],
        "cursor": "",
        "nested": [{"accessToken": secret_value}],
    }
    session = FakeSession([FakeResponse(payload=payload), FakeResponse()])
    sink_calls = []

    def production_sink(record):
        sink_calls.append(record)
        append_manifest(manifest, record)

    with pytest.raises(SensitiveResponseError) as failure:
        client(session).paginate(
            live_segment(),
            MetadataCache(raw_root),
            cutoff_id="c",
            run_id="r",
            manifest_sink=production_sink,
        )
    captured = capsys.readouterr()
    assert len(session.calls) == 1
    assert sink_calls == []
    assert not manifest.exists()
    assert not manifest.parent.exists()
    assert not raw_root.exists()
    assert not list(tmp_path.rglob("*.tmp-*"))
    assert secret_value not in str(failure.value) + captured.out + captured.err
    assert not any(secret_value in path.name for path in tmp_path.rglob("*"))


def test_sensitive_cutoff_paths_never_create_a_manifest(tmp_path):
    manifest = tmp_path / "manifest-parent" / "manifest.jsonl"
    fetched = FakeSession(
        [
            FakeResponse(
                payload={
                    "market_settled_ts": "2025-01-01T00:00:00Z",
                    "nested": {"clientSecret": "FETCHED-SECRET"},
                }
            )
        ]
    )
    with pytest.raises(SensitiveResponseError):
        client(fetched).fetch_cutoff()
    pinned = tmp_path / "pinned.json"
    pinned.write_text(
        json.dumps(
            {
                "market_settled_ts": "2025-01-01T00:00:00Z",
                "nested": [{"refreshToken": "PINNED-SECRET"}],
            }
        ),
        encoding="utf-8",
    )
    with pytest.raises(SensitiveResponseError):
        MetadataCache.load_cutoff_snapshot(pinned)
    assert not manifest.exists() and not manifest.parent.exists()


def test_ordinary_retry_exhaustion_still_uses_production_manifest_sink(tmp_path):
    manifest = tmp_path / "operational" / "manifest.jsonl"
    session = FakeSession([FakeResponse(503), FakeResponse(503), FakeResponse(503)])
    with pytest.raises(RequestFailure):
        client(session, max_retries=2).paginate(
            live_segment(),
            MetadataCache(tmp_path / "raw"),
            cutoff_id="c",
            run_id="r",
            manifest_sink=lambda record: append_manifest(manifest, record),
        )
    records = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert len(records) == 1
    assert records[0]["error_type"] == "HTTPRetryExhausted"
    assert records[0]["http_attempt_count"] == 3
    assert records[0]["retry_count"] == 2


SENSITIVE_SCREENING_STATUSES = (200, 400, 401, 403, 404, 422, 429, 500, 502, 503, 504)


@pytest.mark.parametrize("status", SENSITIVE_SCREENING_STATUSES)
def test_sensitive_market_body_is_screened_before_status_retry_schema_and_manifest(
    status, tmp_path, capsys
):
    secret_value = "SCREEN-BEFORE-STATUS-MARKET-SECRET"
    raw_root = tmp_path / "raw"
    manifest = tmp_path / "manifest-parent" / "manifest.jsonl"
    payload = {"unexpected": {"accessToken": secret_value}}
    session = FakeSession([FakeResponse(status, payload), FakeResponse()])
    active = client(session, max_retries=3)
    sink_calls = []

    def production_sink(record):
        sink_calls.append(record)
        append_manifest(manifest, record)

    with pytest.raises(SensitiveResponseError) as failure:
        active.paginate(
            live_segment(), MetadataCache(raw_root), cutoff_id="c", run_id="r",
            manifest_sink=production_sink,
        )
    captured = capsys.readouterr()
    assert len(session.calls) == 1
    assert active.counters.actual_http_attempts == 1
    assert active.counters.retries == 0
    assert active.counters.rate_limit_responses == 0
    assert sink_calls == []
    assert not manifest.exists() and not manifest.parent.exists()
    assert not raw_root.exists()
    assert not list(tmp_path.rglob("*.tmp-*"))
    assert secret_value not in str(failure.value) + captured.out + captured.err
    assert all(secret_value not in path.name for path in tmp_path.rglob("*"))
    assert all(
        secret_value.encode() not in path.read_bytes()
        for path in tmp_path.rglob("*") if path.is_file()
    )


@pytest.mark.parametrize("status", SENSITIVE_SCREENING_STATUSES)
def test_sensitive_cutoff_body_is_screened_before_status_retry_and_schema(
    status, tmp_path, capsys
):
    secret_value = "SCREEN-BEFORE-STATUS-CUTOFF-SECRET"
    payload = {"unexpected": [{"clientSecret": secret_value}]}
    session = FakeSession([FakeResponse(status, payload), FakeResponse()])
    active = client(session, max_retries=3)
    with pytest.raises(SensitiveResponseError) as failure:
        active.fetch_cutoff()
    captured = capsys.readouterr()
    assert len(session.calls) == 1
    assert active.counters.actual_http_attempts == 1
    assert active.counters.retries == 0
    assert active.counters.rate_limit_responses == 0
    assert not list(tmp_path.iterdir())
    assert secret_value not in str(failure.value) + captured.out + captured.err


def test_nonsensitive_malformed_200_still_emits_one_sanitized_manifest_record(tmp_path):
    manifest = tmp_path / "operational" / "manifest.jsonl"
    session = FakeSession([FakeResponse(200, {"unexpected": "safe"}), FakeResponse()])
    active = client(session)
    with pytest.raises(RequestFailure):
        active.paginate(
            live_segment(), MetadataCache(tmp_path / "raw"), cutoff_id="c", run_id="r",
            manifest_sink=lambda record: append_manifest(manifest, record),
        )
    assert len(session.calls) == 1
    assert active.counters.retries == 0
    record = json.loads(manifest.read_text())
    assert record["error_type"] == "MalformedResponse"
    assert record["http_attempt_count"] == 1


def test_valid_nonsensitive_market_and_cutoff_responses_still_work(tmp_path):
    market_session = FakeSession(
        [FakeResponse(200, {"markets": [market("A")], "cursor": ""})]
    )
    result = client(market_session).paginate(
        live_segment(), MetadataCache(tmp_path / "raw"), cutoff_id="c", run_id="r"
    )
    assert result.complete and result.markets[0]["ticker"] == "A"
    cutoff_session = FakeSession(
        [FakeResponse(200, {"market_settled_ts": "2025-01-01T00:00:00Z"})]
    )
    assert client(cutoff_session).fetch_cutoff()["market_settled_ts"].startswith("2025")


def _network_smoke_args(tmp_path):
    config = tmp_path / "config.toml"
    _write_test_config(config)
    return build_parser().parse_args(
        [
            "--start-date", "2026-06-01", "--end-date", "2026-06-30",
            "--month", "2026-06", "--raw-root", str(tmp_path / "raw"),
            "--manifest", str(tmp_path / "operational" / "manifest.jsonl"),
            "--config", str(config), "--page-size", "1000", "--limit-pages", "1",
            "--historical-mode", "skip", "--live-mode", "require",
        ]
    )


def test_limit_pages_one_performs_one_market_request_and_preserves_incomplete_raw_page(
    tmp_path, capsys
):
    args = _network_smoke_args(tmp_path)
    cutoff = {"market_settled_ts": "2026-01-01T00:00:00Z"}
    first_page = {
        "markets": [market("SMOKE", "2026-06-15T00:00:00Z")],
        "cursor": "continue-from-smoke",
    }
    session = FakeSession([FakeResponse(200, cutoff), FakeResponse(200, first_page)])
    assert run(args, session=session) == metadata_cli.SMOKE_INCOMPLETE_EXIT
    captured = capsys.readouterr()
    cutoff_calls = [call for call in session.calls if call["url"].endswith("/historical/cutoff")]
    market_calls = [call for call in session.calls if call["url"].endswith("/markets")]
    assert len(cutoff_calls) == 1
    assert len(market_calls) == 1
    assert all("candlestick" not in call["url"] for call in session.calls)
    assert "smoke_mode=true" in captured.out
    assert "limit_pages=1" in captured.out
    assert "committed_run_possible=false" in captured.out
    assert "raw_pages_will_be_preserved=true" in captured.out
    assert "expected_exit=smoke_incomplete" in captured.out
    assert "run_complete=true" not in captured.out

    raw_root = Path(args.raw_root)
    pages = list((raw_root / "2026-06" / "live_pages").glob("*.json"))
    assert len(pages) == 1
    manifest = Path(args.manifest)
    records = [json.loads(line) for line in manifest.read_text().splitlines()]
    assert len(records) == 1
    assert records[0]["returned_row_count"] == 1
    assert records[0]["intentionally_incomplete_due_to_page_limit"] is True
    assert not list(raw_root.glob("2026-06/settled_markets_*.jsonl"))
    assert not (raw_root / "audits").exists()
    assert not (raw_root / ".staging").exists()
    assert not (raw_root / "run_commits").exists()


def test_unrestricted_resume_reuses_smoke_page_and_continues_cursor(tmp_path):
    args = _network_smoke_args(tmp_path)
    cutoff = {"market_settled_ts": "2026-01-01T00:00:00Z"}
    first_page = {
        "markets": [market("A", "2026-06-10T00:00:00Z")],
        "cursor": "next-page",
    }
    assert run(
        args,
        session=FakeSession([FakeResponse(200, cutoff), FakeResponse(200, first_page)]),
    ) == metadata_cli.SMOKE_INCOMPLETE_EXIT

    args.limit_pages = None
    second_page = {
        "markets": [market("B", "2026-06-20T00:00:00Z")],
        "cursor": "",
    }
    resume_session = FakeSession([FakeResponse(200, cutoff), FakeResponse(200, second_page)])
    assert run(args, session=resume_session) == 0
    market_calls = [call for call in resume_session.calls if call["url"].endswith("/markets")]
    assert len(market_calls) == 1
    assert market_calls[0]["params"]["cursor"] == "next-page"
    assert len(list((Path(args.raw_root) / "2026-06" / "live_pages").glob("*.json"))) == 2
    assert len(list((Path(args.raw_root) / "run_commits").glob("*.json"))) == 1


def test_smoke_sensitive_page_remains_fully_fail_closed(tmp_path):
    args = _live_cli_args(tmp_path)
    args.limit_pages = 1
    secret = "SMOKE-SENSITIVE-DO-NOT-PERSIST"
    session = FakeSession(
        [FakeResponse(200, {"unexpected": {"accessToken": secret}})]
    )
    with pytest.raises(SensitiveResponseError) as failure:
        run(args, session=session)
    assert len(session.calls) == 1
    assert secret not in str(failure.value)
    assert not Path(args.raw_root).exists()
    assert not Path(args.manifest).exists()


def test_dry_run_with_page_limit_sends_nothing_and_writes_nothing(tmp_path, capsys):
    args = _network_smoke_args(tmp_path)
    args.dry_run = True
    session = FakeSession()
    assert run(args, session=session) == 0
    captured = capsys.readouterr()
    assert not session.calls
    assert "smoke_mode=true" in captured.out
    assert "expected_exit=smoke_incomplete" in captured.out
    assert not Path(args.raw_root).exists()
    assert not Path(args.manifest).exists()


def test_default_metadata_urls_use_canonical_external_api_host(tmp_path):
    assert KALSHI_PRODUCTION_BASE_URL == "https://external-api.kalshi.com"

    cutoff_session = FakeSession(
        [FakeResponse(200, {"market_settled_ts": "2025-01-01T00:00:00Z"})]
    )
    client(cutoff_session).fetch_cutoff()
    assert cutoff_session.calls[0]["url"] == (
        "https://external-api.kalshi.com/trade-api/v2/historical/cutoff"
    )

    live_session = FakeSession([FakeResponse()])
    client(live_session).paginate(
        live_segment(), MetadataCache(tmp_path / "live"), cutoff_id="c", run_id="r"
    )
    assert live_session.calls[0]["url"] == (
        "https://external-api.kalshi.com/trade-api/v2/markets"
    )

    historical_session = FakeSession([FakeResponse()])
    client(historical_session).paginate(
        historical_segment(), MetadataCache(tmp_path / "historical"),
        cutoff_id="c", run_id="r",
    )
    assert historical_session.calls[0]["url"] == (
        "https://external-api.kalshi.com/trade-api/v2/historical/markets"
    )


def test_active_pipeline_has_no_elections_host_default_and_injected_base_url_works(tmp_path):
    active_root = Path(__file__).resolve().parents[1] / "scripts" / "pipeline_v2"
    active_source = "\n".join(
        path.read_text(encoding="utf-8") for path in active_root.glob("*.py")
    )
    assert "api.elections.kalshi.com" not in active_source
    session = FakeSession([FakeResponse()])
    injected = client(session, base_url="https://offline-test.invalid")
    injected.paginate(
        live_segment(), MetadataCache(tmp_path), cutoff_id="c", run_id="r"
    )
    assert session.calls[0]["url"] == "https://offline-test.invalid/trade-api/v2/markets"


def test_zero_budget_traverses_later_cached_segment_then_stops_at_first_miss(tmp_path):
    config = tmp_path / "config.toml"
    _write_test_config(config)
    cutoff_payload = {"market_settled_ts": "2025-01-01T00:00:00Z"}
    cutoff_path = tmp_path / "cutoff.json"
    cutoff_path.write_text(json.dumps(cutoff_payload), encoding="utf-8")
    cutoff_id = sha256_json(cutoff_payload)[:20]
    raw_root = tmp_path / "raw"
    cache = MetadataCache(raw_root)

    # Segment B has a complete two-page cursor chain before smoke acquisition starts.
    september = EndpointSegment(
        "live", LIVE_PATH,
        datetime(2025, 9, 1, tzinfo=UTC), datetime(2025, 10, 1, tzinfo=UTC),
        "2025-09",
    )
    cached_session = FakeSession(
        [
            FakeResponse(
                200,
                {
                    "markets": [market("B1", "2025-09-10T00:00:00Z")],
                    "cursor": "cached-next",
                },
            ),
            FakeResponse(
                200,
                {
                    "markets": [market("B2", "2025-09-20T00:00:00Z")],
                    "cursor": "",
                },
            ),
        ]
    )
    client(cached_session).paginate(
        september, cache, cutoff_id=cutoff_id, run_id="setup"
    )

    args = build_parser().parse_args(
        [
            "--start-date", "2025-08-01", "--end-date", "2025-10-31",
            "--raw-root", str(raw_root),
            "--manifest", str(tmp_path / "manifest.jsonl"),
            "--config", str(config), "--cutoff-snapshot", str(cutoff_path),
            "--limit-pages", "1", "--historical-mode", "skip",
            "--live-mode", "require",
        ]
    )
    segment_a_page = {
        "markets": [market("A", "2025-08-10T00:00:00Z")],
        "cursor": "",
    }
    smoke_session = FakeSession([FakeResponse(200, segment_a_page)])
    assert run(args, session=smoke_session) == metadata_cli.SMOKE_INCOMPLETE_EXIT
    market_calls = [call for call in smoke_session.calls if call["url"].endswith("/markets")]
    assert len(market_calls) == 1

    manifest_records = [
        json.loads(line) for line in Path(args.manifest).read_text().splitlines()
    ]
    assert [record["cache_status"] for record in manifest_records] == [
        "published", "hit", "hit"
    ]
    assert [record["month"] for record in manifest_records] == [
        "2025-08", "2025-09", "2025-09"
    ]
    assert all(
        record["intentionally_incomplete_due_to_page_limit"]
        for record in manifest_records
    )
    assert not (raw_root / "2025-10" / "live_pages").exists()
    assert not (raw_root / "run_commits").exists()
    assert not (raw_root / ".staging").exists()
    assert not list(raw_root.glob("20??-??/settled_markets_*.jsonl"))

    # Removing the bound reuses A and both B pages, then fetches only C's miss.
    args.limit_pages = None
    segment_c_page = {
        "markets": [market("C", "2025-10-10T00:00:00Z")],
        "cursor": "",
    }
    resume_session = FakeSession([FakeResponse(200, segment_c_page)])
    assert run(args, session=resume_session) == 0
    resumed_market_calls = [
        call for call in resume_session.calls if call["url"].endswith("/markets")
    ]
    assert len(resumed_market_calls) == 1
    assert "cursor" not in resumed_market_calls[0]["params"]
    assert len(list((raw_root / "run_commits").glob("*.json"))) == 1
