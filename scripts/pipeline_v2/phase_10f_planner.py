"""Pure outcome-blind helpers for Phase 10F-A horizon-price planning."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
import hashlib
import math
from typing import Any, Iterable, Mapping

from scripts.common.time_utils import format_iso_utc, parse_iso_utc


PLANNER_SCHEMA_VERSION = "phase-10f-a-offline-planner-v1"
SMOKE_SEED = "phase-10f-a-bounded-smoke-v1"
EXISTED = "definitely_existed_by_target"
OPENED_AFTER = "valid_anchor_but_no_t_minus_1h_market"
UNKNOWN = "cannot_determine_offline"


@dataclass
class FamilyPlan:
    family_id: str
    family_id_source: str
    rule: str
    category: str
    verified_anchor_time: str
    target_time: str
    verified_source: str
    timing_structure: str
    event_ticker: str = ""
    market_tickers: list[str] = field(default_factory=list)
    earliest_market_open_time: datetime | None = None
    eligible_market_count: int = 0
    opened_after_target_market_count: int = 0
    unknown_open_time_market_count: int = 0

    @property
    def identity(self) -> tuple[str, str]:
        return self.family_id, self.family_id_source

    @property
    def market_existence_at_target(self) -> str:
        if self.eligible_market_count:
            return EXISTED
        if self.unknown_open_time_market_count:
            return UNKNOWN
        return OPENED_AFTER


def compact_market_ticker(family_id: str, ticker: str) -> str:
    """Losslessly encode a ticker relative to its family identifier."""
    if ticker == family_id:
        return "~"
    prefix = f"{family_id}-"
    return ticker[len(prefix) :] if ticker.startswith(prefix) else f"={ticker}"


def expand_market_ticker(family_id: str, token: str) -> str:
    if token == "~":
        return family_id
    if token.startswith("="):
        return token[1:]
    return f"{family_id}-{token}"


def encode_market_tickers(family_id: str, tickers: Iterable[str]) -> str:
    values = sorted(set(str(value) for value in tickers))
    tokens = [compact_market_ticker(family_id, value) for value in values]
    if any("|" in token for token in tokens):
        raise ValueError("market ticker cannot be represented by compact encoding")
    return "|".join(tokens)


def decode_market_tickers(family_id: str, encoded: str) -> tuple[str, ...]:
    if not encoded:
        return ()
    return tuple(expand_market_ticker(family_id, token) for token in encoded.split("|"))


def classify_market_open(open_time: Any, target_time: Any) -> str:
    opened = parse_iso_utc(open_time)
    target = parse_iso_utc(target_time)
    if target is None:
        raise ValueError("target time must be an exact timestamp")
    if opened is None:
        return UNKNOWN
    return EXISTED if opened <= target else OPENED_AFTER


def projected_batched_requests(
    eligible_markets_by_target: Mapping[str, int], batch_size: int
) -> int:
    if batch_size <= 0:
        raise ValueError("batch size must be positive")
    return sum(
        math.ceil(int(count) / batch_size)
        for count in eligible_markets_by_target.values()
        if int(count) > 0
    )


def deterministic_rank(identity: tuple[str, str], stratum: str) -> str:
    payload = "\x00".join((SMOKE_SEED, stratum, *identity))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def select_smoke_cases(
    plans: Iterable[FamilyPlan], quotas: Mapping[str, int]
) -> tuple[list[FamilyPlan], dict[str, int]]:
    plans = list(plans)

    def stratum(plan: FamilyPlan) -> str:
        category = plan.category
        existence = plan.market_existence_at_target
        if category == "Crypto":
            return (
                "crypto_no_t_minus_1h"
                if existence == OPENED_AFTER
                else "crypto_existed"
            )
        if category == "Financials":
            return "financials"
        if category == "Climate and Weather":
            return "climate_weather"
        if category == "Sports":
            return (
                "sports_no_t_minus_1h"
                if existence == OPENED_AFTER
                else "sports_existed"
            )
        return "other"

    groups: dict[str, list[FamilyPlan]] = {key: [] for key in quotas}
    for plan in plans:
        key = stratum(plan)
        if key in groups:
            groups[key].append(plan)
    selected: list[FamilyPlan] = []
    realized = {}
    for key, requested in quotas.items():
        # Prefer low-contract families within each existed stratum as the best
        # available offline proxy for thin/stale price behavior, then use a
        # deterministic hash to avoid title or outcome-based selection.
        ranked = sorted(
            groups[key],
            key=lambda plan: (
                min(len(plan.market_tickers), 3),
                deterministic_rank(plan.identity, key),
            ),
        )
        if len(ranked) < requested:
            raise ValueError(f"smoke stratum {key!r} has insufficient families")
        chosen = ranked[:requested]
        selected.extend(chosen)
        realized[key] = len(chosen)
    if len({plan.identity for plan in selected}) != sum(quotas.values()):
        raise ValueError("smoke selection is not identity-unique")
    return sorted(selected, key=lambda plan: plan.identity), realized


def plan_to_row(plan: FamilyPlan) -> dict[str, Any]:
    encoded = encode_market_tickers(plan.family_id, plan.market_tickers)
    decoded = decode_market_tickers(plan.family_id, encoded)
    if decoded != tuple(sorted(set(plan.market_tickers))):
        raise ValueError("compact market ticker encoding is not lossless")
    return {
        "family_id": plan.family_id,
        "family_id_source": plan.family_id_source,
        "event_ticker": plan.event_ticker,
        "associated_market_tickers_compact": encoded,
        "market_ticker_encoding": "family-prefix-relative-v1",
        "verified_anchor_time": plan.verified_anchor_time,
        "target_time": plan.target_time,
        "verified_source": plan.verified_source,
        "rule": plan.rule,
        "category": plan.category,
        "timing_structure": plan.timing_structure,
        "earliest_market_open_time": format_iso_utc(plan.earliest_market_open_time),
        "market_existence_at_target": plan.market_existence_at_target,
        "market_count": len(plan.market_tickers),
        "eligible_market_retrieval_count": plan.eligible_market_count,
        "opened_after_target_market_count": plan.opened_after_target_market_count,
        "unknown_open_time_market_count": plan.unknown_open_time_market_count,
        "expected_price_history_retrieval_unit": "bounded_multi_market_candlestick_batch",
    }
