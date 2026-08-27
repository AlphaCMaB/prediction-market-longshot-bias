from datetime import timedelta
import json
from pathlib import Path

from scripts.common.time_utils import format_iso_utc, parse_iso_utc
from scripts.pipeline_v2.build_phase_10f_offline_planner import (
    SMOKE_QUOTAS,
    _empirical_cache_metrics,
    _storage_model,
)
from scripts.pipeline_v2.phase_10f_planner import (
    EXISTED,
    OPENED_AFTER,
    UNKNOWN,
    FamilyPlan,
    classify_market_open,
    decode_market_tickers,
    encode_market_tickers,
    plan_to_row,
    projected_batched_requests,
    select_smoke_cases,
)


def make_plan(index: int, category: str, existence: str) -> FamilyPlan:
    anchor = parse_iso_utc("2026-01-02T12:00:00Z")
    target = anchor - timedelta(hours=1)
    plan = FamilyPlan(
        family_id=f"F{index}",
        family_id_source="kalshi_event_ticker",
        rule=(
            "PR2_M_SCHEDULED_START_SINGLE_MILESTONE"
            if category == "Sports"
            else "PR1_M_FIXED_CLOCK_SINGLE_EXACT"
        ),
        category=category,
        verified_anchor_time=format_iso_utc(anchor),
        target_time=format_iso_utc(target),
        verified_source=(
            "verified_official_scheduled_timestamp"
            if category == "Sports"
            else "validated_strike_date"
        ),
        timing_structure=(
            "scheduled_event_start" if category == "Sports" else "fixed_clock"
        ),
        event_ticker=f"F{index}",
        market_tickers=[f"F{index}-YES", f"F{index}-NO"],
    )
    if existence == EXISTED:
        plan.eligible_market_count = 2
        plan.earliest_market_open_time = target - timedelta(hours=1)
    elif existence == OPENED_AFTER:
        plan.opened_after_target_market_count = 2
        plan.earliest_market_open_time = target + timedelta(minutes=1)
    else:
        plan.unknown_open_time_market_count = 2
    return plan


def test_compact_ticker_encoding_is_lossless_and_prefix_relative():
    tickers = ["F-A", "F-B", "OTHER", "F"]
    encoded = encode_market_tickers("F", tickers)
    assert encoded == "~|A|B|=OTHER"
    assert decode_market_tickers("F", encoded) == tuple(sorted(tickers))


def test_market_open_classification_keeps_anchor_validity_separate():
    target = "2026-01-02T11:00:00Z"
    assert classify_market_open("2026-01-02T10:59:59Z", target) == EXISTED
    assert classify_market_open("2026-01-02T11:00:01Z", target) == OPENED_AFTER
    assert classify_market_open("", target) == UNKNOWN


def test_request_projection_shares_batches_only_within_target():
    assert projected_batched_requests({"T1": 101, "T2": 100, "T3": 0}, 100) == 3


def test_plan_row_has_target_and_lossless_market_association():
    plan = make_plan(1, "Crypto", EXISTED)
    row = plan_to_row(plan)
    assert row["market_existence_at_target"] == EXISTED
    assert row["eligible_market_retrieval_count"] == 2
    assert decode_market_tickers(
        plan.family_id, row["associated_market_tickers_compact"]
    ) == (
        "F1-NO",
        "F1-YES",
    )


def test_smoke_selection_is_deterministic_stratified_and_exactly_200():
    plans = []
    index = 0
    definitions = {
        "crypto_no_t_minus_1h": ("Crypto", OPENED_AFTER),
        "crypto_existed": ("Crypto", EXISTED),
        "financials": ("Financials", EXISTED),
        "climate_weather": ("Climate and Weather", EXISTED),
        "sports_no_t_minus_1h": ("Sports", OPENED_AFTER),
        "sports_existed": ("Sports", EXISTED),
    }
    for key, count in SMOKE_QUOTAS.items():
        category, existence = definitions[key]
        for _ in range(count + 3):
            plans.append(make_plan(index, category, existence))
            index += 1
    first, counts = select_smoke_cases(plans, SMOKE_QUOTAS)
    second, _ = select_smoke_cases(reversed(plans), SMOKE_QUOTAS)
    assert len(first) == 200
    assert counts == SMOKE_QUOTAS
    assert [plan.identity for plan in first] == [plan.identity for plan in second]


def test_empirical_metrics_use_size_only_and_deterministic_gzip(tmp_path: Path):
    source = tmp_path / "one.json"
    source.write_text(
        json.dumps(
            {
                "markets": [
                    {"market_ticker": "A", "candlesticks": [{"end_period_ts": 1}]}
                ]
            }
        )
    )
    metrics = _empirical_cache_metrics(tmp_path)
    assert metrics["available"] is True
    assert metrics["returned_market_count"] == 1
    assert metrics["candlestick_count"] == 1
    assert metrics["deterministic_gzip_bytes"] > 0


def test_storage_model_fails_closed_when_namespace_is_too_small():
    before = {
        "used_bytes": 900,
        "max_bytes": 1000,
        "remaining_budget_bytes": 100,
        "free_bytes": 10000,
        "min_free_bytes": 100,
    }
    model = _storage_model(
        eligible_markets=100,
        request_count=1,
        empirical={"compressed_bytes_per_returned_market": 10},
        storage_before=before,
    )
    assert model["empirical_projection"]["fits_namespace_ceiling"] is False
    assert model["production_acquisition_authorized"] is False
