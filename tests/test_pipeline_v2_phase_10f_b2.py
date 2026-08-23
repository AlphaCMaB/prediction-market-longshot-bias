"""Offline tests for strict Phase 10F-B2 routing and normalization."""

from __future__ import annotations

import gzip
import json
from pathlib import Path

import pytest

from scripts.pipeline_v2.kalshi_metadata_cache import StorageBudget
from scripts.pipeline_v2.phase_10f_b2 import (
    B2ValidationError,
    HISTORICAL_ROUTE,
    LIVE_ROUTE,
    SAMPLE_SIZE,
    TickerCandidate,
    extract_observation,
    normalize_candle,
    normalize_response,
    route_for_settlement,
    sample_identity,
    select_ticker_sample,
)
from scripts.pipeline_v2.run_phase_10f_b2 import BoundedNetworkClient


def candidate(
    family: str,
    ticker: str,
    category: str,
    *,
    count: int = 10,
    month: str = "2026-01",
) -> TickerCandidate:
    return TickerCandidate(
        family_id=family,
        family_id_source="kalshi_event_ticker",
        event_ticker=family,
        rule=(
            "PR2_M_SCHEDULED_START_SINGLE_MILESTONE"
            if category == "Sports"
            else "PR1_M_FIXED_CLOCK_SINGLE_EXACT"
        ),
        category=category,
        timing_structure=("scheduled_event_start" if category == "Sports" else "fixed_clock"),
        target_time=f"{month}-02T12:00:00Z",
        ticker=ticker,
        family_market_count=count,
        settlement_time="2026-02-01T00:00:00Z",
    )


def make_candidates():
    rows = []
    definitions = (("Crypto", 30, 5), ("Financials", 25, 5), ("Climate and Weather", 15, 5), ("Sports", 65, 1))
    index = 0
    for category, families, contracts in definitions:
        for family_index in range(families):
            family = f"F{index:03d}"
            month = f"2026-{(family_index % 4) + 1:02d}"
            for ticker_index in range(contracts):
                rows.append(
                    candidate(
                        family,
                        f"{family}-T{ticker_index}",
                        category,
                        count=contracts,
                        month=month,
                    )
                )
            index += 1
    return rows


def historical_candle(end=100, bid="0.40", ask="0.50", trade="0.45", previous="0.30"):
    return {
        "end_period_ts": end,
        "yes_bid": {"open": bid, "low": bid, "high": bid, "close": bid},
        "yes_ask": {"open": ask, "low": ask, "high": ask, "close": ask},
        "price": {"open": trade, "low": trade, "high": trade, "close": trade, "mean": trade, "previous": previous},
        "volume": "1.00",
        "open_interest": "2.00",
    }


def live_candle(end=100, bid="0.40", ask="0.50", trade="0.45"):
    return {
        "end_period_ts": end,
        "yes_bid": {"close_dollars": bid},
        "yes_ask": {"close_dollars": ask},
        "price": {"close_dollars": trade, "previous_dollars": "0.30"},
        "volume_fp": "1.00",
        "open_interest_fp": "2.00",
    }


def test_sample_is_exact_deterministic_and_caps_each_family_at_two():
    rows = make_candidates()
    first = select_ticker_sample(rows)
    second = select_ticker_sample(reversed(rows))
    assert len(first) == SAMPLE_SIZE
    assert [row.ticker for row in first] == [row.ticker for row in second]
    assert sample_identity(first) == sample_identity(second)
    family_counts = {}
    for row in first:
        family_counts[row.family_id] = family_counts.get(row.family_id, 0) + 1
    assert len(family_counts) == 135
    assert max(family_counts.values()) == 2
    assert {row.category for row in first} == {
        "Crypto",
        "Financials",
        "Climate and Weather",
        "Sports",
    }


def test_routing_uses_settlement_strictly_before_cutoff():
    cutoff = "2026-02-01T00:00:00Z"
    assert route_for_settlement("2026-01-31T23:59:59Z", cutoff) == HISTORICAL_ROUTE
    assert route_for_settlement(cutoff, cutoff) == LIVE_ROUTE
    with pytest.raises(B2ValidationError, match="settlement timestamp"):
        route_for_settlement("", cutoff)


def test_historical_and_live_schemas_normalize_to_same_typed_projection():
    historical = normalize_candle(historical_candle(), route=HISTORICAL_ROUTE)
    live = normalize_candle(live_candle(), route=LIVE_ROUTE)
    assert historical["yes_bid_close"] == live["yes_bid_close"] == pytest.approx(0.4)
    assert historical["yes_ask_close"] == live["yes_ask_close"] == pytest.approx(0.5)
    assert historical["trade_close"] == live["trade_close"] == pytest.approx(0.45)
    assert historical["schema_variant"] == "historical_legacy_close"
    assert live["schema_variant"] == "live_fixed_point_dollars"
    assert historical["previous_trade_used"] is False


def test_typed_normalizer_fails_closed_on_ambiguous_or_wrong_schema():
    ambiguous = historical_candle()
    ambiguous["yes_bid"]["close_dollars"] = "0.40"
    with pytest.raises(B2ValidationError, match="ambiguous"):
        normalize_candle(ambiguous, route=HISTORICAL_ROUTE)
    with pytest.raises(B2ValidationError, match="schema"):
        normalize_candle(historical_candle(), route=LIVE_ROUTE)


def test_response_identity_and_shape_are_strict():
    payload = {"ticker": "A", "candlesticks": [historical_candle()]}
    assert len(normalize_response(payload, route=HISTORICAL_ROUTE, ticker="A")) == 1
    with pytest.raises(B2ValidationError, match="schema"):
        normalize_response({**payload, "extra": 1}, route=HISTORICAL_ROUTE, ticker="A")
    with pytest.raises(B2ValidationError, match="ticker"):
        normalize_response(payload, route=HISTORICAL_ROUTE, ticker="B")


def test_observation_does_not_mix_prices_or_use_previous_trade():
    no_trade = historical_candle(trade=None, previous="0.30")
    rows = normalize_response(
        {"ticker": "A", "candlesticks": [no_trade]},
        route=HISTORICAL_ROUTE,
        ticker="A",
    )
    result = extract_observation(rows, target_ts=100)
    assert result["midpoint"] == pytest.approx(0.45)
    assert result["trade_close"] is None
    assert result["previous_trade_used"] is False


def test_observation_staleness_and_post_target_boundaries():
    rows = normalize_response(
        {"ticker": "A", "candlesticks": [historical_candle(end=100)]},
        route=HISTORICAL_ROUTE,
        ticker="A",
    )
    assert extract_observation(rows, target_ts=100 + 15 * 60)["midpoint_within_15m"] is True
    assert extract_observation(rows, target_ts=101 + 15 * 60)["midpoint_within_15m"] is False
    with pytest.raises(B2ValidationError, match="post-target"):
        extract_observation(rows, target_ts=99)


class FakeResponse:
    def __init__(self, payload, status=200):
        self.payload = payload
        self.status_code = status
        self.content = json.dumps(payload).encode()

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


def client(tmp_path: Path, session, **kwargs):
    return BoundedNetworkClient(
        session=session,
        output_root=tmp_path / "b2",
        budget=StorageBudget(tmp_path, max_bytes=10**7, min_free_bytes=0),
        base_url="https://example.test",
        requests_per_second=1000000,
        sleep=kwargs.pop("sleep", lambda _: None),
        **kwargs,
    )


def test_cutoff_is_immutable_and_no_network_resume_reuses_it(tmp_path):
    payload = {
        "market_settled_ts": "2026-02-01T00:00:00Z",
        "trades_created_ts": "2026-02-01T00:00:00Z",
        "orders_updated_ts": "2026-02-01T00:00:00Z",
    }
    first = client(tmp_path, FakeSession([FakeResponse(payload)]))
    returned, commit = first.fetch_cutoff()
    assert returned == payload and commit["success"] is True
    assert first.physical_requests == 1
    raw = tmp_path / "b2" / commit["raw_path"]
    assert raw.read_bytes()[:2] == b"\x1f\x8b"

    resumed = client(tmp_path, None, network_forbidden=True)
    again, _ = resumed.fetch_cutoff()
    assert again == payload
    assert resumed.resume_hits == 1 and resumed.physical_requests == 0


def test_historical_request_is_narrow_and_committed(tmp_path):
    payload = {"ticker": "A/B", "candlesticks": [historical_candle(end=200)]}
    session = FakeSession([FakeResponse(payload)])
    active = client(tmp_path, session)
    rows, commit = active.fetch_candles(
        ticker="A/B",
        route=HISTORICAL_ROUTE,
        start_ts=100,
        end_ts=200,
        cutoff_hash="c" * 64,
    )
    assert len(rows) == 1 and commit["success"] is True
    assert "%2F" in session.calls[0][0]
    assert session.calls[0][1] == {"start_ts": 100, "end_ts": 200, "period_interval": 1}


def test_404_is_a_diagnostic_failure_not_a_schema_fallback(tmp_path):
    active = client(tmp_path, FakeSession([FakeResponse({"error": "missing"}, 404)]))
    rows, commit = active.fetch_candles(
        ticker="A",
        route=HISTORICAL_ROUTE,
        start_ts=100,
        end_ts=200,
        cutoff_hash="c" * 64,
    )
    assert rows == []
    assert commit["success"] is False
    assert commit["failure_kind"] == "http_404"


def test_physical_request_cap_includes_retries(tmp_path):
    active = client(
        tmp_path,
        FakeSession([FakeResponse({}, 429), FakeResponse({}, 429)]),
        max_requests=1,
        max_retries=2,
    )
    with pytest.raises(B2ValidationError, match="hard cap"):
        active.fetch_candles(
            ticker="A",
            route=HISTORICAL_ROUTE,
            start_ts=100,
            end_ts=200,
            cutoff_hash="c" * 64,
        )
    assert active.physical_requests == 1


def test_live_route_uses_single_ticker_batch_without_synthetic_previous(tmp_path):
    payload = {"markets": [{"market_ticker": "A", "candlesticks": [live_candle(end=200)]}]}
    session = FakeSession([FakeResponse(payload)])
    active = client(tmp_path, session)
    rows, _ = active.fetch_candles(
        ticker="A", route=LIVE_ROUTE, start_ts=100, end_ts=200, cutoff_hash="c" * 64
    )
    assert len(rows) == 1
    assert session.calls[0][1]["include_latest_before_start"] == "false"


def test_cached_candle_response_never_redownloads(tmp_path):
    payload = {"ticker": "A", "candlesticks": [historical_candle(end=200)]}
    first = client(tmp_path, FakeSession([FakeResponse(payload)]))
    first.fetch_candles(
        ticker="A", route=HISTORICAL_ROUTE, start_ts=100, end_ts=200, cutoff_hash="c" * 64
    )
    resumed = client(tmp_path, None, network_forbidden=True)
    rows, _ = resumed.fetch_candles(
        ticker="A", route=HISTORICAL_ROUTE, start_ts=100, end_ts=200, cutoff_hash="c" * 64
    )
    assert len(rows) == 1 and resumed.physical_requests == 0 and resumed.resume_hits == 1
