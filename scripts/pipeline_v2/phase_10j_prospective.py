"""Pure helpers for the Phase 10J prospective cross-category design."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from fractions import Fraction
import hashlib
import math
from typing import Any, Iterable, Mapping, Sequence

from scripts.pipeline_v2.study_rules import canonical_field_name


SCHEMA_VERSION = "phase-10j-prospective-design-v1"
SAMPLING_SEED = "phase-10j-prospective-bernoulli-contract-cap-v1"
WINDOW_START = "2026-10-01T00:00:00Z"
WINDOW_END_EXCLUSIVE = "2027-01-01T00:00:00Z"
DISCOVERY_START = "2026-09-23T00:00:00Z"
WINDOW_DAYS = 92
HISTORICAL_DAYS = 365
TARGET_FAMILIES = 625
CONTRACT_CAP = 3
ANNUAL_ELIGIBLE_FAMILIES = {
    "Sports": 64_775,
    "Crypto": 43_889,
    "Financials": 2_930,
    "Climate and Weather": 571,
    "Commodities": 1,
}
RULE_BY_CATEGORY = {
    "Sports": "PR2_M_SCHEDULED_START_SINGLE_MILESTONE",
    "Crypto": "PR1_M_FIXED_CLOCK_SINGLE_EXACT",
    "Financials": "PR1_M_FIXED_CLOCK_SINGLE_EXACT",
    "Climate and Weather": "PR1_M_FIXED_CLOCK_SINGLE_EXACT",
    "Commodities": "PR1_M_FIXED_CLOCK_SINGLE_EXACT",
}
INFERENTIAL_FAMILY_GATE = 500
DEPTH_SIZES = (1, 10, 100)
FORBIDDEN_KEYS = frozenset(
    {
        "result",
        "outcome",
        "binaryoutcome",
        "settlementvalue",
        "settlementvaluedollars",
        "settlementts",
        "settlementtime",
        "expirationvalue",
        "mvesettlementvalue",
        "yessettlementvalue",
        "yessettlementvaluedollars",
    }
)


class ProspectiveDesignError(RuntimeError):
    """Raised when a prospective design or observation fails closed."""


def _utc(value: str) -> datetime:
    try:
        parsed = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    except ValueError as exc:
        raise ProspectiveDesignError(f"invalid UTC timestamp: {value}") from exc
    if parsed.tzinfo is None:
        raise ProspectiveDesignError(f"timezone required: {value}")
    return parsed.astimezone(timezone.utc)


def planning_rows() -> list[dict[str, Any]]:
    """Return fixed category sampling probabilities for the proposed window."""
    rows = []
    for category, annual in ANNUAL_ELIGIBLE_FAMILIES.items():
        planning_population = annual * WINDOW_DAYS / HISTORICAL_DAYS
        probability_fraction = min(
            Fraction(1, 1),
            Fraction(TARGET_FAMILIES * HISTORICAL_DAYS, annual * WINDOW_DAYS),
        )
        probability = float(probability_fraction)
        expected_sample = planning_population * probability
        rows.append(
            {
                "category": category,
                "rule": RULE_BY_CATEGORY[category],
                "annual_historical_eligible_families": annual,
                "planning_population_92_days": planning_population,
                "family_inclusion_probability": probability,
                "family_inclusion_probability_numerator": probability_fraction.numerator,
                "family_inclusion_probability_denominator": probability_fraction.denominator,
                "family_weight_raw": 1 / probability,
                "expected_sampled_families": expected_sample,
                "expected_inferential_support": bool(
                    expected_sample >= INFERENTIAL_FAMILY_GATE
                ),
            }
        )
    return rows


def category_probability(category: str) -> float:
    for row in planning_rows():
        if row["category"] == category:
            return float(row["family_inclusion_probability"])
    raise ProspectiveDesignError(f"category has no approved rule: {category}")


def category_probability_fraction(category: str) -> Fraction:
    for row in planning_rows():
        if row["category"] == category:
            return Fraction(
                int(row["family_inclusion_probability_numerator"]),
                int(row["family_inclusion_probability_denominator"]),
            )
    raise ProspectiveDesignError(f"category has no approved rule: {category}")


def hash_uniform(*parts: str, seed: str = SAMPLING_SEED) -> float:
    material = "\x00".join((seed, *map(str, parts))).encode("utf-8")
    integer = int.from_bytes(hashlib.sha256(material).digest(), "big")
    return integer / 2**256


def family_selected(
    category: str, family_id: str, family_id_source: str
) -> tuple[bool, float, float]:
    probability_fraction = category_probability_fraction(category)
    material = "\x00".join(
        (SAMPLING_SEED, "family", category, family_id, family_id_source)
    ).encode("utf-8")
    integer = int.from_bytes(hashlib.sha256(material).digest(), "big")
    selected = (
        integer * probability_fraction.denominator
        < probability_fraction.numerator * 2**256
    )
    return selected, integer / 2**256, float(probability_fraction)


def sample_contracts(
    family_id: str,
    family_id_source: str,
    contract_ids: Iterable[str],
    *,
    cap: int = CONTRACT_CAP,
) -> list[str]:
    if cap <= 0:
        raise ProspectiveDesignError("contract cap must be positive")
    values = [str(value).strip() for value in contract_ids]
    if not values or any(not value for value in values):
        raise ProspectiveDesignError("contract roster must be nonempty")
    if len(set(values)) != len(values):
        raise ProspectiveDesignError("duplicate contract in frozen roster")
    return sorted(
        values,
        key=lambda ticker: (
            hashlib.sha256(
                "\x00".join(
                    (
                        SAMPLING_SEED,
                        "contract",
                        family_id,
                        family_id_source,
                        ticker,
                    )
                ).encode("utf-8")
            ).hexdigest(),
            ticker,
        ),
    )[:cap]


def inclusion_record(category: str, roster_size: int) -> dict[str, float | int]:
    if roster_size <= 0:
        raise ProspectiveDesignError("roster size must be positive")
    family_fraction = category_probability_fraction(category)
    family_probability = float(family_fraction)
    sampled = min(CONTRACT_CAP, roster_size)
    conditional = sampled / roster_size
    contract_probability = family_probability * conditional
    return {
        "family_inclusion_probability": family_probability,
        "family_inclusion_probability_numerator": family_fraction.numerator,
        "family_inclusion_probability_denominator": family_fraction.denominator,
        "family_weight_raw": 1 / family_probability,
        "family_contract_count": roster_size,
        "sampled_contract_count_in_family": sampled,
        "contract_inclusion_probability_given_family": conditional,
        "contract_inclusion_probability": contract_probability,
        "contract_weight_raw": 1 / contract_probability,
    }


def forbidden_paths(value: Any, prefix: str = "") -> list[str]:
    """Find outcome or settlement keys recursively without inspecting values."""
    found: list[str] = []
    if isinstance(value, Mapping):
        for key, child in value.items():
            path = f"{prefix}.{key}" if prefix else str(key)
            if canonical_field_name(key) in FORBIDDEN_KEYS:
                found.append(path)
            found.extend(forbidden_paths(child, path))
    elif isinstance(value, list):
        for index, child in enumerate(value):
            found.extend(forbidden_paths(child, f"{prefix}[{index}]"))
    return found


def validate_pre_target_capture(
    request_started_at: str, response_received_at: str, target_time: str
) -> dict[str, float | bool]:
    started = _utc(request_started_at)
    received = _utc(response_received_at)
    target = _utc(target_time)
    if received < started:
        raise ProspectiveDesignError("response precedes request")
    return {
        "valid_pre_target": bool(started <= target and received <= target),
        "round_trip_seconds": (received - started).total_seconds(),
        "maximum_snapshot_age_seconds": (target - started).total_seconds(),
    }


def _levels(value: Any, side: str) -> list[tuple[Decimal, Decimal]]:
    if value is None:
        return []
    if not isinstance(value, list):
        raise ProspectiveDesignError(f"{side} levels must be a list")
    output: list[tuple[Decimal, Decimal]] = []
    seen: set[Decimal] = set()
    for level in value:
        if not isinstance(level, (list, tuple)) or len(level) != 2:
            raise ProspectiveDesignError(f"invalid {side} price level")
        try:
            price, quantity = Decimal(str(level[0])), Decimal(str(level[1]))
        except Exception as exc:
            raise ProspectiveDesignError(f"invalid {side} numeric value") from exc
        if not Decimal("0") <= price <= Decimal("1") or quantity <= 0:
            raise ProspectiveDesignError(f"invalid {side} price or quantity")
        if price in seen:
            raise ProspectiveDesignError(f"duplicate {side} price")
        seen.add(price)
        output.append((price, quantity))
    return sorted(output, reverse=True)


def _vwap(opposing_bids: Sequence[tuple[Decimal, Decimal]], size: int) -> float | None:
    remaining = Decimal(size)
    cost = Decimal("0")
    for bid_price, quantity in opposing_bids:
        take = min(remaining, quantity)
        cost += take * (Decimal("1") - bid_price)
        remaining -= take
        if remaining == 0:
            return float(cost / Decimal(size))
    return None


def normalize_orderbook(orderbook: Mapping[str, Any]) -> dict[str, Any]:
    """Normalize YES/NO bid levels into executable asks and depth VWAPs."""
    allowed = {"yes_dollars", "no_dollars"}
    if set(orderbook) != allowed:
        raise ProspectiveDesignError("orderbook schema changed")
    yes = _levels(orderbook.get("yes_dollars"), "yes")
    no = _levels(orderbook.get("no_dollars"), "no")
    best_yes = yes[0] if yes else None
    best_no = no[0] if no else None
    if best_yes and best_no and best_yes[0] + best_no[0] > 1:
        raise ProspectiveDesignError("crossed binary orderbook")
    result: dict[str, Any] = {
        "yes_bid": float(best_yes[0]) if best_yes else None,
        "yes_bid_size": float(best_yes[1]) if best_yes else None,
        "no_bid": float(best_no[0]) if best_no else None,
        "no_bid_size": float(best_no[1]) if best_no else None,
        "yes_ask": float(Decimal("1") - best_no[0]) if best_no else None,
        "no_ask": float(Decimal("1") - best_yes[0]) if best_yes else None,
        "yes_level_count": len(yes),
        "no_level_count": len(no),
    }
    result["yes_spread"] = (
        result["yes_ask"] - result["yes_bid"]
        if result["yes_ask"] is not None and result["yes_bid"] is not None
        else None
    )
    for size in DEPTH_SIZES:
        result[f"buy_yes_vwap_{size}"] = _vwap(no, size)
        result[f"buy_no_vwap_{size}"] = _vwap(yes, size)
    return result


def summarize_trades(
    trades: Sequence[Mapping[str, Any]],
    *,
    ticker: str,
    min_time: str,
    max_time: str,
) -> dict[str, Any]:
    start, end = _utc(min_time), _utc(max_time)
    if start > end:
        raise ProspectiveDesignError("trade interval is reversed")
    seen: set[str] = set()
    total = Decimal("0")
    normalized = []
    for trade in trades:
        trade_id = str(trade.get("trade_id") or "")
        if not trade_id or trade_id in seen:
            raise ProspectiveDesignError("missing or duplicate trade ID")
        seen.add(trade_id)
        if str(trade.get("ticker") or "") != ticker:
            raise ProspectiveDesignError("trade ticker mismatch")
        created = _utc(str(trade.get("created_time") or ""))
        if not start <= created <= end:
            raise ProspectiveDesignError("trade outside pre-target interval")
        count = Decimal(str(trade.get("count_fp") or "0"))
        yes = Decimal(str(trade.get("yes_price_dollars") or "-1"))
        no = Decimal(str(trade.get("no_price_dollars") or "-1"))
        if count <= 0 or not (0 <= yes <= 1) or not (0 <= no <= 1):
            raise ProspectiveDesignError("invalid trade value")
        if abs(yes + no - Decimal("1")) > Decimal("0.0001"):
            raise ProspectiveDesignError("trade YES/NO prices do not sum to one")
        total += count
        normalized.append((created, float(yes), trade_id))
    latest = max(normalized, default=None)
    return {
        "trade_count_60m": len(normalized),
        "traded_contracts_60m": float(total),
        "last_trade_yes_price": latest[1] if latest else None,
        "last_trade_time": (
            latest[0].isoformat().replace("+00:00", "Z") if latest else None
        ),
        "last_trade_id": latest[2] if latest else None,
    }


def storage_scenarios(max_contracts: int) -> list[dict[str, Any]]:
    if max_contracts <= 0:
        raise ProspectiveDesignError("contract count must be positive")
    settings = (
        ("compact", 8 * 1024, 8 * 1024**2),
        ("planning", 24 * 1024, 16 * 1024**2),
        ("stress", 64 * 1024, 32 * 1024**2),
    )
    return [
        {
            "scenario": name,
            "compressed_bytes_per_sampled_contract": per_contract,
            "fixed_discovery_and_report_bytes": fixed,
            "projected_incremental_bytes": fixed + per_contract * max_contracts,
        }
        for name, per_contract, fixed in settings
    ]


def prospective_counts() -> dict[str, int | float]:
    rows = planning_rows()
    families = sum(float(row["expected_sampled_families"]) for row in rows)
    max_contracts = math.ceil(families * CONTRACT_CAP)
    minimum_snapshot_requests = 2 * math.ceil(max_contracts / 100)
    maximum_snapshot_requests = 2 * math.ceil(families)
    return {
        "expected_sampled_families": families,
        "maximum_sampled_contracts": max_contracts,
        "minimum_base_requests_excluding_discovery_and_pagination": (
            minimum_snapshot_requests + max_contracts
        ),
        "maximum_base_requests_excluding_discovery_and_pagination": (
            maximum_snapshot_requests + max_contracts
        ),
    }
