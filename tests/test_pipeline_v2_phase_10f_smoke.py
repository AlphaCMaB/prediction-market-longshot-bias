"""Offline integrity tests for the bounded Phase 10F-B price smoke."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from scripts.pipeline_v2.kalshi_metadata_cache import StorageBudget, canonical_json
from scripts.pipeline_v2.phase_10f_planner import EXISTED, OPENED_AFTER
from scripts.pipeline_v2.phase_10f_smoke import (
    RequestGroup,
    SmokeFamily,
    SmokeValidationError,
    build_request_groups,
    extract_contract_observation,
    latest_complete_candle,
    spread_diagnostics,
    validate_batch_payload,
    validate_documented_boundary_semantics,
)
from scripts.pipeline_v2.run_phase_10f_b_smoke import ImmutableSmokeClient


class FakeResponse:
    def __init__(self, payload, status_code=200):
        self.payload = payload
        self.status_code = status_code

    def raise_for_status(self):
        if self.status_code >= 400:
            raise RuntimeError(f"http_{self.status_code}")

    def json(self):
        return self.payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, params, timeout):
        self.calls.append((url, dict(params), timeout))
        return self.responses.pop(0)


def family(index=1, tickers=("A",), existence=EXISTED, target="2026-01-01T01:00:00Z"):
    return SmokeFamily(
        family_id=f"F{index}",
        family_id_source="kalshi_event_ticker",
        event_ticker=f"F{index}",
        rule="PR1_M_FIXED_CLOCK_SINGLE_EXACT",
        category="Financials",
        timing_structure="fixed_clock",
        target_time=target,
        market_existence_at_target=existence,
        eligible_tickers=tuple(tickers),
    )


def candle(end, *, bid=None, ask=None, trade=None, previous=None):
    return {
        "end_period_ts": end,
        "yes_bid": {"close_dollars": bid},
        "yes_ask": {"close_dollars": ask},
        "price": {"close_dollars": trade, "previous_dollars": previous},
    }


def group(tickers=("A",), start=100, end=200, purpose="smoke_price_window"):
    return RequestGroup(
        request_id="a" * 24,
        tickers=tuple(tickers),
        start_ts=start,
        end_ts=end,
        purpose=purpose,
    )


def payload(ticker="A", candles=None):
    return {"markets": [{"market_ticker": ticker, "candlesticks": list(candles or [])}]}


def test_exact_target_is_safe_only_under_inclusive_end_semantics():
    exact = candle(200, bid="0.40", ask="0.50")
    assert latest_complete_candle([exact, candle(201)], 200) == exact
    validate_documented_boundary_semantics("inclusive_end_period")
    with pytest.raises(SmokeValidationError, match="not established"):
        validate_documented_boundary_semantics("candle_start")


def test_midpoint_and_trade_are_separate_without_previous_trade_fallback():
    result = extract_contract_observation(
        ticker="A",
        candles=[
            candle(100, trade="0.30"),
            candle(190, bid="0.40", ask="0.50", previous="0.30"),
        ],
        target_ts=200,
    )
    assert result["midpoint"] == pytest.approx(0.45)
    assert result["midpoint_observation_time"] == "1970-01-01T00:03:10+00:00"
    assert result["trade_close"] == pytest.approx(0.30)
    assert result["trade_observation_time"] == "1970-01-01T00:01:40+00:00"
    assert result["previous_trade_used"] is False


@pytest.mark.parametrize(
    ("bid", "ask", "reason"),
    [(None, "0.50", "missing_bid"), ("0.40", None, "missing_ask"), (None, None, "missing_bid_and_ask")],
)
def test_midpoint_requires_both_quote_sides(bid, ask, reason):
    result = extract_contract_observation(
        ticker="A", candles=[candle(200, bid=bid, ask=ask, trade="0.6")], target_ts=200
    )
    assert result["midpoint"] is None
    assert result["midpoint_reason"] == reason
    assert result["trade_close"] == pytest.approx(0.6)


def test_staleness_boundaries_and_no_post_target_use():
    at_15 = extract_contract_observation(
        ticker="A", candles=[candle(100, bid="0.4", ask="0.6")], target_ts=100 + 15 * 60
    )
    after_15 = extract_contract_observation(
        ticker="A", candles=[candle(99, bid="0.4", ask="0.6")], target_ts=100 + 15 * 60
    )
    assert at_15["midpoint_within_15m"] is True
    assert after_15["midpoint_within_15m"] is False
    assert latest_complete_candle([candle(201)], 200) is None


def test_batch_payload_rejects_post_target_and_duplicate_timestamps():
    with pytest.raises(SmokeValidationError, match="post-target"):
        validate_batch_payload(payload(candles=[candle(201)]), group())
    with pytest.raises(SmokeValidationError, match="duplicate candlestick"):
        validate_batch_payload(payload(candles=[candle(150), candle(150)]), group())


def test_request_groups_skip_structural_late_families_and_are_deterministic():
    active = family(tickers=("C", "A", "B"))
    late = family(index=2, tickers=(), existence=OPENED_AFTER)
    first = build_request_groups([active, late], batch_size=2)
    second = build_request_groups([late, active], batch_size=2)
    assert [(row.request_id, row.tickers) for row in first] == [
        (row.request_id, row.tickers) for row in second
    ]
    assert [row.tickers for row in first] == [("A", "B"), ("C",)]


def test_spread_diagnostics_reports_required_thresholds():
    report = spread_diagnostics([{"spread": 0.01}, {"spread": 0.06}, {"spread": 0.21}])
    assert report["observation_count"] == 3
    assert report["median"] == pytest.approx(0.06)
    assert report["fraction_gt_0_02"] == pytest.approx(2 / 3)
    assert report["fraction_gt_0_20"] == pytest.approx(1 / 3)


def make_client(tmp_path: Path, session, **kwargs):
    return ImmutableSmokeClient(
        session=session,
        output_root=tmp_path / "out",
        budget=StorageBudget(tmp_path, max_bytes=10**7, min_free_bytes=0),
        max_retries=kwargs.pop("max_retries", 1),
        requests_per_second=1000000,
        sleep=kwargs.pop("sleep", lambda _: None),
        **kwargs,
    )


def test_client_publishes_deterministic_gzip_commit_and_resumes_without_network(tmp_path):
    request = group()
    body = payload(candles=[candle(150, bid="0.4", ask="0.6")])
    session = FakeSession([FakeResponse(body)])
    client = make_client(tmp_path, session)
    returned, commit = client.fetch(request)
    raw_path, commit_path = client.paths(request)
    assert returned == body
    assert raw_path.read_bytes()[:2] == b"\x1f\x8b"
    assert commit_path.is_file() and commit["complete"] is True

    forbidden = make_client(tmp_path, None, network_forbidden=True)
    resumed, _ = forbidden.fetch(request)
    assert resumed == body
    assert forbidden.resume_hits == 1
    assert forbidden.physical_requests == 0


def test_orphaned_valid_raw_is_recovered_without_redownload(tmp_path):
    request = group()
    body = payload(candles=[candle(150)])
    first = make_client(tmp_path, FakeSession([FakeResponse(body)]))
    first.fetch(request)
    raw_path, commit_path = first.paths(request)
    commit_path.unlink()
    forbidden = make_client(tmp_path, None, network_forbidden=True)
    returned, _ = forbidden.fetch(request)
    assert returned == body and raw_path.is_file() and commit_path.is_file()


def test_resume_rejects_raw_tampering(tmp_path):
    request = group()
    first = make_client(tmp_path, FakeSession([FakeResponse(payload())]))
    first.fetch(request)
    raw_path, _ = first.paths(request)
    raw_path.write_bytes(raw_path.read_bytes() + b"tamper")
    with pytest.raises(SmokeValidationError):
        make_client(tmp_path, None, network_forbidden=True).fetch(request)


def test_retry_and_rate_limit_accounting(tmp_path):
    sleeps = []
    session = FakeSession([FakeResponse({}, 429), FakeResponse(payload())])
    client = make_client(tmp_path, session, max_retries=2, sleep=sleeps.append)
    client.fetch(group())
    assert client.physical_requests == 2
    assert client.retries == 1
    assert client.rate_limits == 1
    assert sleeps


def test_response_rejects_prohibited_post_event_fields(tmp_path):
    session = FakeSession([FakeResponse({"markets": [], "result": "yes"})])
    with pytest.raises(SmokeValidationError, match="prohibited"):
        make_client(tmp_path, session).fetch(group())


def test_request_identity_is_hash_pinned_in_raw_wrapper(tmp_path):
    request = group()
    client = make_client(tmp_path, FakeSession([FakeResponse(payload())]))
    client.fetch(request)
    raw_path, _ = client.paths(request)
    import gzip

    wrapper = json.loads(gzip.decompress(raw_path.read_bytes()))
    assert wrapper["request"]["params"]["include_latest_before_start"] == "false"
    assert wrapper["response_sha256"] == __import__("hashlib").sha256(
        canonical_json(wrapper["response"])
    ).hexdigest()
