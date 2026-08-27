"""Pure, outcome-blind helpers for the Phase 10E verification audit design.

Tier labels produced here are proposals for audit sampling. They are never
verification decisions and must not be supplied to ``apply_anchor_verification``.
"""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
import hashlib
import json
import math
import re
import unicodedata
from typing import Any, Iterable, Mapping

from scripts.pipeline_v2.timing import classify_timing


DESIGN_SCHEMA_VERSION = "1.0"
AUDIT_SEED = "phase-10e-outcome-blind-audit-v1"
ALLOWED_CANDIDATE_SOURCES = frozenset(
    {
        "market_occurrence_datetime",
        "event_strike_date",
        "event_milestone_start_date",
    }
)
SAFE_CONTEXT_KEYS = {
    "market_occurrence_datetime": frozenset(
        {
            "event_ticker",
            "first_supporting_market_ticker",
            "last_supporting_market_ticker",
            "rules_primary",
            "subtitle",
            "supporting_market_count",
            "ticker",
            "title",
        }
    ),
    "event_strike_date": frozenset(
        {"category", "event_title", "series_ticker", "sub_title"}
    ),
    "event_milestone_start_date": frozenset(
        {
            "association_type",
            "milestone_category",
            "milestone_id",
            "milestone_source_id",
            "milestone_source_ids_json",
            "milestone_title",
            "milestone_type",
        }
    ),
}
_TITLE_STOPWORDS = frozenset(
    {
        "a",
        "an",
        "and",
        "are",
        "as",
        "at",
        "be",
        "before",
        "below",
        "by",
        "during",
        "for",
        "from",
        "have",
        "how",
        "in",
        "is",
        "it",
        "less",
        "more",
        "next",
        "of",
        "on",
        "or",
        "over",
        "than",
        "the",
        "their",
        "this",
        "to",
        "under",
        "v",
        "vs",
        "will",
        "win",
        "winner",
        "with",
    }
)


@dataclass(frozen=True)
class TierAssignment:
    tier: str
    reason: str
    proposed_rule: str
    proposed_timing_structure: str
    semantic_agreement: str


def integer(value: Any) -> int:
    return int(str(value or "0"))


def boolean(value: Any) -> bool:
    return str(value or "").strip().casefold() == "true"


def event_tickers(row: Mapping[str, Any]) -> tuple[str, ...]:
    values = json.loads(str(row.get("event_tickers_json") or "[]"))
    if not isinstance(values, list) or any(
        not isinstance(item, str) for item in values
    ):
        raise ValueError("event_tickers_json must be a string list")
    return tuple(values)


def distinct_exact_times(row: Mapping[str, Any]) -> tuple[str, ...]:
    values = json.loads(str(row.get("distinct_exact_candidate_times_json") or "[]"))
    if not isinstance(values, list) or any(
        not isinstance(item, str) for item in values
    ):
        raise ValueError("distinct exact times must be a string list")
    return tuple(values)


def source_combination(row: Mapping[str, Any]) -> str:
    sources = []
    if integer(row.get("occurrence_candidate_count")):
        sources.append("market_occurrence")
    if integer(row.get("strike_date_candidate_count")):
        sources.append("event_strike")
    if integer(row.get("milestone_start_candidate_count")):
        sources.append("milestone_start")
    return "+".join(sources) if sources else "none"


def evidence_pattern(row: Mapping[str, Any]) -> str:
    candidate_count = integer(row.get("candidate_count"))
    unique_times = len(distinct_exact_times(row))
    if boolean(row.get("has_multiple_event_tickers")):
        return "multiple_event_tickers"
    if integer(row.get("date_only_candidate_count")):
        return "date_only_candidate"
    if integer(row.get("invalid_candidate_value_count")):
        return "invalid_candidate_value"
    if integer(row.get("sentinel_timestamp_count")):
        return "sentinel_timestamp"
    if not candidate_count:
        return "no_candidate"
    if candidate_count == 1 and unique_times == 1:
        return "single_exact_candidate"
    if unique_times == 1:
        return "multiple_candidates_one_exact_time"
    if unique_times > 1:
        return "multiple_distinct_exact_times"
    return "other_ambiguous_pattern"


def title_tokens(value: Any) -> frozenset[str]:
    normalized = unicodedata.normalize("NFKD", str(value or "")).casefold()
    return frozenset(
        token
        for token in re.findall(r"[a-z0-9]+", normalized)
        if len(token) > 1 and token not in _TITLE_STOPWORDS
    )


def title_agreement(left: Any, right: Any) -> tuple[bool, str]:
    left_tokens = title_tokens(left)
    right_tokens = title_tokens(right)
    if not left_tokens or not right_tokens:
        return False, "missing_informative_title_tokens"
    shared = len(left_tokens & right_tokens)
    overlap = shared / min(len(left_tokens), len(right_tokens))
    if left_tokens == right_tokens:
        return True, "exact_informative_token_set"
    if shared >= 2 and overlap >= 0.5:
        return True, "strong_informative_token_overlap"
    return False, "insufficient_informative_token_overlap"


def proposed_timing(
    family_row: Mapping[str, Any], event_row: Mapping[str, Any]
) -> tuple[str, str]:
    ticker = str(event_row.get("series_ticker") or "")
    title = " ".join(
        (
            str(family_row.get("representative_title") or ""),
            str(event_row.get("title") or ""),
            str(event_row.get("sub_title") or ""),
        )
    )
    return classify_timing(ticker, title)


def _common_exact_requirements(row: Mapping[str, Any]) -> bool:
    return (
        not boolean(row.get("has_conflicting_exact_candidate_times"))
        and not boolean(row.get("has_multiple_event_tickers"))
        and integer(row.get("date_only_candidate_count")) == 0
        and integer(row.get("invalid_candidate_value_count")) == 0
        and integer(row.get("sentinel_timestamp_count")) == 0
        and len(distinct_exact_times(row)) == 1
    )


def tier_one_mechanical(row: Mapping[str, Any]) -> bool:
    return (
        _common_exact_requirements(row)
        and integer(row.get("candidate_count")) == 1
        and integer(row.get("exact_timestamp_candidate_count")) == 1
    )


def tier_two_mechanical(row: Mapping[str, Any]) -> bool:
    return (
        _common_exact_requirements(row)
        and integer(row.get("milestone_start_candidate_count")) == 1
    )


def assign_tier(
    family_row: Mapping[str, Any],
    event_row: Mapping[str, Any],
    *,
    single_candidate: Mapping[str, Any] | None,
    milestone_candidate: Mapping[str, Any] | None,
) -> TierAssignment:
    timing, _ = proposed_timing(family_row, event_row)
    if tier_one_mechanical(family_row) and single_candidate is not None:
        source = str(single_candidate.get("candidate_source_type") or "")
        if source in ALLOWED_CANDIDATE_SOURCES and timing == "fixed_clock":
            return TierAssignment(
                "tier_1",
                "single_exact_allowed_candidate_and_proposed_fixed_clock",
                "PR1_FIXED_CLOCK_SINGLE_EXACT",
                "fixed_clock",
                "not_applicable",
            )

    if tier_two_mechanical(family_row) and milestone_candidate is not None:
        context = json.loads(
            str(milestone_candidate.get("evidence_context_json") or "{}")
        )
        milestone_title = str(
            context.get("milestone_title")
            or milestone_candidate.get("candidate_title")
            or ""
        )
        event_ok, event_reason = title_agreement(
            event_row.get("title"), milestone_title
        )
        family_tokens = title_tokens(family_row.get("representative_title"))
        context_tokens = title_tokens(f"{event_row.get('title', '')} {milestone_title}")
        family_ok = bool(family_tokens & context_tokens)
        if (
            str(family_row.get("category") or "") == "Sports"
            and event_ok
            and family_ok
            and timing in {"scheduled_event_start", "unclear"}
        ):
            return TierAssignment(
                "tier_2",
                "single_official_milestone_time_with_conservative_context_agreement_and_no_subevent_flag",
                "PR2_SCHEDULED_START_SINGLE_MILESTONE",
                "scheduled_event_start",
                event_reason,
            )

    pattern = evidence_pattern(family_row)
    if pattern == "multiple_distinct_exact_times":
        reason = "multiple_distinct_exact_candidate_times"
    elif pattern == "no_candidate":
        reason = "no_candidate_anchor_evidence"
    elif pattern in {
        "multiple_event_tickers",
        "date_only_candidate",
        "invalid_candidate_value",
        "sentinel_timestamp",
    }:
        reason = pattern
    elif tier_two_mechanical(family_row):
        reason = f"milestone_timing_or_semantic_ambiguity_{timing}"
    elif tier_one_mechanical(family_row):
        reason = f"single_candidate_proposed_timing_{timing}"
    else:
        reason = "other_manual_review_pattern"
    return TierAssignment(
        "tier_3",
        reason,
        "NONE_MANUAL_REVIEW",
        timing,
        "not_approved_or_not_applicable",
    )


def safe_candidate_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    source = str(row.get("candidate_source_type") or "")
    if source not in ALLOWED_CANDIDATE_SOURCES:
        raise ValueError(f"disallowed candidate source in review packet: {source!r}")
    raw_context = json.loads(str(row.get("evidence_context_json") or "{}"))
    if not isinstance(raw_context, dict):
        raise ValueError("evidence context must be an object")
    allowed = SAFE_CONTEXT_KEYS[source]
    context = {key: raw_context[key] for key in sorted(raw_context) if key in allowed}
    return {
        "candidate_id": str(row.get("candidate_id") or ""),
        "candidate_source_type": source,
        "candidate_original_value": str(row.get("candidate_original_value") or ""),
        "candidate_time_utc": str(row.get("candidate_time_utc") or ""),
        "candidate_date": str(row.get("candidate_date") or ""),
        "candidate_precision": str(row.get("candidate_precision") or ""),
        "potential_verified_anchor_source": str(
            row.get("potential_verified_anchor_source") or ""
        ),
        "candidate_title": str(row.get("candidate_title") or ""),
        "evidence_reference": str(row.get("evidence_reference") or ""),
        "supporting_source_count": integer(row.get("supporting_source_count")),
        "analysis_window_status": str(row.get("analysis_window_status") or ""),
        "safe_evidence_context": context,
    }


def deterministic_rank(
    identity: tuple[str, str], *, tier: str, stratum: str, seed: str = AUDIT_SEED
) -> str:
    payload = "\x00".join((seed, tier, stratum, identity[0], identity[1]))
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def stratified_sample(
    identities: Iterable[tuple[str, str]],
    *,
    tier: str,
    strata: Mapping[tuple[str, str], str],
    sample_size: int,
    seed: str = AUDIT_SEED,
) -> tuple[tuple[tuple[str, str], str, int, int, float], ...]:
    groups: dict[str, list[tuple[str, str]]] = defaultdict(list)
    for identity in identities:
        groups[strata[identity]].append(identity)
    total = sum(len(values) for values in groups.values())
    target = min(sample_size, total)
    if target < len(groups):
        raise ValueError("sample size is smaller than the number of audit strata")
    allocations = {key: 1 for key in groups}
    remaining = target - len(groups)
    capacities = {key: len(values) - 1 for key, values in groups.items()}
    capacity_total = sum(capacities.values())
    fractional: list[tuple[float, str]] = []
    used = 0
    if remaining and capacity_total:
        for key in sorted(groups):
            exact = remaining * capacities[key] / capacity_total
            extra = min(capacities[key], math.floor(exact))
            allocations[key] += extra
            used += extra
            fractional.append((exact - extra, key))
        for _, key in sorted(fractional, key=lambda item: (-item[0], item[1])):
            if used >= remaining:
                break
            if allocations[key] < len(groups[key]):
                allocations[key] += 1
                used += 1
    if sum(allocations.values()) != target:
        raise ValueError("failed to allocate the complete audit sample")
    selected = []
    for stratum in sorted(groups):
        ranked = sorted(
            groups[stratum],
            key=lambda identity: deterministic_rank(
                identity, tier=tier, stratum=stratum, seed=seed
            ),
        )
        allocation = allocations[stratum]
        weight = len(ranked) / allocation
        selected.extend(
            (identity, stratum, len(ranked), allocation, weight)
            for identity in ranked[:allocation]
        )
    return tuple(sorted(selected, key=lambda item: (item[1], item[0])))
