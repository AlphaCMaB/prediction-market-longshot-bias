"""Offline tests for the Methodology V2 Kalshi candlestick layer."""

from __future__ import annotations

import argparse
import importlib.util
import json
import socket
import sys
from pathlib import Path

import pytest

from scripts.common.io_utils import write_csv
from scripts.pipeline_v2 import candlesticks
from scripts.pipeline_v2.extract_kalshi_candlesticks import run
from scripts.pipeline_v2.kalshi_candlestick_client import (
    KalshiCandlestickClient,
    batch_tickers,
    deterministic_cache_key,
    deterministic_cache_path,
)


ROOT = Path(__file__).resolve().parents[1]


class FakeResponse:
    def __init__(self, payload=None, status_code=200, error=None):
        self.payload = payload if payload is not None else {"markets": []}
        self.status_code = status_code
        self.error = error

    def raise_for_status(self):
        if self.error is not None:
            raise self.error
        if self.status_code >= 400:
            raise RuntimeError(f"http_{self.status_code}")

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses=None, handler=None):
        self.responses = list(responses or [])
        self.handler = handler
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append((url, dict(params), timeout))
        if self.handler:
            return self.handler(url, params, timeout)
        return self.responses.pop(0)


def candle(end, **overrides):
    row = {"end_period_ts": end}
    row.update(overrides)
    return row


def test_exact_target_and_latest_before_target_selection():
    rows = [candle(100, label="old"), candle(199, label="latest"), candle(200, label="exact")]
    assert candlesticks.select_latest_at_or_before(rows, 200)["label"] == "exact"
    assert candlesticks.select_latest_at_or_before(rows[:2], 200)["label"] == "latest"


def test_post_target_candles_are_never_selected():
    assert candlesticks.select_latest_at_or_before([candle(201)], 200) is None


def test_midpoint_priority_trade_and_previous_fallbacks():
    midpoint = candlesticks.extract_price_fields(
        candle(
            100,
            yes_bid={"close_dollars": "0.40"},
            yes_ask={"close_dollars": "0.44"},
            price={"close_dollars": "0.50", "previous_dollars": "0.30"},
        )
    )
    trade = candlesticks.extract_price_fields(candle(100, price={"close_dollars": "0.50"}))
    previous = candlesticks.extract_price_fields(candle(100, price={"previous_dollars": "0.30"}))
    assert (midpoint["p_hat"], midpoint["price_source"]) == (pytest.approx(0.42), "yes_bid_ask_midpoint")
    assert (trade["p_hat"], trade["price_source"]) == (0.5, "trade_close")
    assert (previous["p_hat"], previous["price_source"]) == (0.3, "previous_trade")


def test_invalid_probability_and_no_usable_price_have_explicit_reasons():
    invalid = candlesticks.build_snapshot(
        [candle(100, yes_bid={"close_dollars": 1.2}, yes_ask={"close_dollars": 1.4})], 100
    )
    unusable = candlesticks.build_snapshot([candle(100)], 100)
    assert (invalid["snapshot_status"], invalid["snapshot_reason"], invalid["p_hat"]) == (
        "unusable", "invalid_probability", None
    )
    assert (unusable["snapshot_status"], unusable["snapshot_reason"]) == (
        "unusable", "no_usable_price"
    )


def test_no_candle_status_is_explicit():
    result = candlesticks.build_snapshot([], 100)
    assert result["snapshot_status"] == "missing"
    assert result["snapshot_reason"] == "no_candlestick_at_or_before_target"


def test_staleness_calculation_and_specification_boundaries():
    at_15 = candlesticks.build_snapshot(
        [candle(100, price={"close_dollars": 0.5})], 100 + 15 * 60
    )
    after_15 = candlesticks.build_snapshot(
        [candle(100, price={"close_dollars": 0.5})], 101 + 15 * 60
    )
    at_60 = candlesticks.build_snapshot(
        [candle(100, price={"close_dollars": 0.5})], 100 + 60 * 60
    )
    after_60 = candlesticks.build_snapshot(
        [candle(100, price={"close_dollars": 0.5})], 101 + 60 * 60
    )
    assert at_15["snapshot_staleness_minutes"] == 15
    assert at_15["main_specification_eligible"] is True
    assert after_15["main_specification_eligible"] is False
    assert at_60["robustness_specification_eligible"] is True
    assert after_60["robustness_specification_eligible"] is False


@pytest.mark.parametrize(
    ("minutes", "bucket"),
    [(5, "0-5m"), (15, "5-15m"), (60, "15-60m"), (180, "1-3h"), (360, "3-6h"), (361, ">6h")],
)
def test_staleness_buckets(minutes, bucket):
    assert candlesticks.staleness_bucket(minutes) == bucket


def test_new_pure_logic_matches_script_26_for_valid_candle():
    existing_path = ROOT / "scripts/26_pull_clean_kalshi_candlesticks.py"
    spec = importlib.util.spec_from_file_location("transition_26_equivalence", existing_path)
    module = importlib.util.module_from_spec(spec)
    assert spec.loader is not None
    spec.loader.exec_module(module)
    row = candle(100, yes_bid={"close_dollars": 0.4}, yes_ask={"close_dollars": 0.6})
    assert candlesticks.select_latest_at_or_before([row], 100) == module.select_candlestick([row], 100)
    assert candlesticks.extract_price_fields(row)["p_hat"] == module.extract_prices(row)["p_hat_primary"]


def test_cache_key_and_path_are_deterministic_and_order_independent(tmp_path):
    assert deterministic_cache_key(["B", "A"], 100, 200, 1) == deterministic_cache_key(["A", "B"], 100, 200, 1)
    assert deterministic_cache_path(tmp_path, ["B", "A"], 100, 200, 1) == deterministic_cache_path(tmp_path, ["A", "B"], 100, 200, 1)


def test_ticker_batching_is_sorted_deduplicated_and_bounded():
    assert batch_tickers(["C", "A", "B", "A"], 2) == [["A", "B"], ["C"]]


def make_client(tmp_path, session, **kwargs):
    return KalshiCandlestickClient(
        session=session,
        cache_dir=tmp_path,
        max_retries=kwargs.pop("max_retries", 1),
        backoff_base_seconds=kwargs.pop("backoff_base_seconds", 0),
        sleep=kwargs.pop("sleep", lambda _: None),
        **kwargs,
    )


def test_recursive_split_after_simulated_oversized_failure(tmp_path):
    def handler(url, params, timeout):
        tickers = params["market_tickers"].split(",")
        if len(tickers) > 2:
            return FakeResponse(status_code=413)
        return FakeResponse({"markets": [{"market_ticker": ticker, "candlesticks": []} for ticker in tickers]})

    session = FakeSession(handler=handler)
    client = make_client(tmp_path, session)
    markets = client.fetch_with_recursive_split(["A", "B", "C", "D"], 100, 200)
    assert {row["market_ticker"] for row in markets} == {"A", "B", "C", "D"}
    assert len(session.calls) == 3


def test_retry_and_rate_limit_use_exponential_backoff(tmp_path):
    sleeps = []
    session = FakeSession(
        responses=[FakeResponse(status_code=429), FakeResponse(error=RuntimeError("temporary")), FakeResponse({"markets": []})]
    )
    client = make_client(
        tmp_path, session, max_retries=3, backoff_base_seconds=1, sleep=sleeps.append
    )
    assert client.request_batch(["A"], 100, 200) == {"markets": []}
    assert client.completed_request_count == 3
    assert sleeps == [1, 2]


def test_dry_run_sends_zero_requests_and_does_not_create_cache(tmp_path):
    session = FakeSession()
    cache_dir = tmp_path / "cache"
    client = make_client(cache_dir, session, dry_run=True, batch_size=2)
    assert client.fetch(["A", "B", "C"], 100, 200) == []
    assert client.anticipated_request_count == 2
    assert client.completed_request_count == 0
    assert session.calls == []
    assert not cache_dir.exists()


def test_cached_response_avoids_request_and_existing_cache_is_not_overwritten(tmp_path):
    session = FakeSession()
    client = make_client(tmp_path, session)
    path = client.cache_path(["A"], 100, 200)
    path.parent.mkdir(parents=True, exist_ok=True)
    original = '{"markets": [{"market_ticker": "CACHED"}]}'
    path.write_text(original, encoding="utf-8")
    payload = client.request_batch(["A"], 100, 200)
    assert payload["markets"][0]["market_ticker"] == "CACHED"
    assert session.calls == []
    assert path.read_text(encoding="utf-8") == original


def test_exclusive_cache_writer_refuses_to_overwrite(tmp_path):
    client = make_client(tmp_path, FakeSession())
    path = tmp_path / "existing.json"
    path.write_text('{"original": true}', encoding="utf-8")
    client._write_cache_once(path, {"replacement": True})
    assert json.loads(path.read_text(encoding="utf-8")) == {"original": True}


def test_cli_dry_run_writes_no_outputs_and_sends_no_requests(tmp_path):
    input_path = tmp_path / "targets.csv"
    config_path = tmp_path / "config.toml"
    write_csv(
        input_path,
        [{
            "venue": "kalshi",
            "market_id": "A",
            "family_id": "F",
            "family_id_source": "event_ticker",
            "timing_structure": "fixed_clock",
            "anchor_time": "2026-07-01T12:00:00Z",
            "anchor_source": "occurrence_datetime",
            "horizon_hours": "1",
            "target_time": "2026-07-01T11:00:00Z",
        }],
    )
    config_path.write_text(
        "candlestick_interval_minutes = 1\ncandlestick_lookback_hours = 24\nbatch_size = 100\nmain_staleness_minutes = 15\nrobustness_staleness_minutes = 60\n",
        encoding="utf-8",
    )
    session = FakeSession()
    args = argparse.Namespace(
        input=input_path,
        output=tmp_path / "out.csv",
        missing_output=tmp_path / "missing.csv",
        report_output=tmp_path / "report.json",
        cache_dir=tmp_path / "cache",
        config=config_path,
        dry_run=True,
        resume=False,
        market_limit=None,
    )
    summary = run(args, session=session)
    assert summary["anticipated_request_count"] == 1
    assert summary["completed_request_count"] == 0
    assert session.calls == []
    assert not args.output.exists()
    assert not args.missing_output.exists()
    assert not args.report_output.exists()
    assert not args.cache_dir.exists()


NEW_MODULES = [
    ROOT / "scripts/pipeline_v2/candlesticks.py",
    ROOT / "scripts/pipeline_v2/kalshi_candlestick_client.py",
    ROOT / "scripts/pipeline_v2/extract_kalshi_candlesticks.py",
]


@pytest.mark.parametrize("module_path", NEW_MODULES, ids=lambda path: path.stem)
def test_import_causes_no_network_or_filesystem_activity(module_path, monkeypatch, tmp_path):
    before = set(tmp_path.rglob("*"))

    def blocked_socket(*args, **kwargs):
        raise AssertionError("network socket created during import")

    monkeypatch.setattr(socket, "socket", blocked_socket)
    monkeypatch.chdir(tmp_path)
    name = f"phase5_import_{module_path.stem}"
    spec = importlib.util.spec_from_file_location(name, module_path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    try:
        spec.loader.exec_module(module)
    finally:
        sys.modules.pop(name, None)
    assert set(tmp_path.rglob("*")) == before
