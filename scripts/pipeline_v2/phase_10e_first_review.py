"""Recommendation-only AI first review for Phase 10E audit cases.

This module does not verify candidates. Every output retains the actual frozen
verification status ``needs_review`` pending human audit and explicit rule
approval.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import json
import re
from typing import Any, Mapping


REVIEW_PROTOCOL_VERSION = "phase-10e-ai-first-review-v1"
REVIEW_DECISIONS = frozenset(
    {
        "recommend_rule_case",
        "recommend_reject",
        "uncertain_human_review",
        "quarantine_tier_3",
    }
)
CONFIDENCE_LEVELS = frozenset({"high", "medium", "low"})
_MONTHS = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}


@dataclass(frozen=True)
class FirstReview:
    reviewer_decision: str
    recommended_verification_status: str
    recommended_timing_structure: str
    candidate_is_relevant_ex_ante_anchor: str
    confidence: str
    concise_rationale: str
    ambiguity_flags: tuple[str, ...]
    human_review_required: bool

    def __post_init__(self):
        if self.reviewer_decision not in REVIEW_DECISIONS:
            raise ValueError("unsupported recommendation-only review decision")
        if self.recommended_verification_status != "needs_review":
            raise ValueError("first review must not verify any candidate")
        if self.confidence not in CONFIDENCE_LEVELS:
            raise ValueError("unsupported confidence")
        if self.candidate_is_relevant_ex_ante_anchor not in {"yes", "no", "uncertain"}:
            raise ValueError("unsupported candidate relevance recommendation")


def _candidates(row: Mapping[str, Any]) -> list[dict[str, Any]]:
    candidates = json.loads(str(row.get("candidates_json") or "[]"))
    if not isinstance(candidates, list) or any(
        not isinstance(item, dict) for item in candidates
    ):
        raise ValueError("candidates_json must contain an object list")
    return candidates


def _proposed_candidate(row: Mapping[str, Any]) -> dict[str, Any] | None:
    proposed_id = str(row.get("proposed_candidate_id") or "")
    matches = [
        item for item in _candidates(row) if item.get("candidate_id") == proposed_id
    ]
    if not proposed_id:
        return None
    if len(matches) != 1:
        raise ValueError(
            "proposed candidate is not uniquely present in packet evidence"
        )
    return matches[0]


def _review_text(row: Mapping[str, Any], candidate: Mapping[str, Any] | None) -> str:
    parts = [
        str(row.get("family_id") or ""),
        str(row.get("family_title") or ""),
        str(row.get("event_title") or ""),
        str(row.get("event_sub_title") or ""),
    ]
    if candidate:
        parts.append(str(candidate.get("candidate_title") or ""))
        context = candidate.get("safe_evidence_context") or {}
        if isinstance(context, Mapping):
            parts.extend(str(value) for value in context.values())
    return " ".join(parts).casefold()


def _ticker_date(value: Any) -> date | None:
    match = re.search(
        r"-(?P<year>\d{2})(?P<month>JAN|FEB|MAR|APR|MAY|JUN|JUL|AUG|SEP|OCT|NOV|DEC)(?P<day>\d{2})",
        str(value or "").upper(),
    )
    if not match:
        return None
    try:
        return date(
            2000 + int(match.group("year")),
            _MONTHS[match.group("month")],
            int(match.group("day")),
        )
    except ValueError:
        return None


def _candidate_date(candidate: Mapping[str, Any] | None) -> date | None:
    if not candidate:
        return None
    value = str(
        candidate.get("candidate_time_utc") or candidate.get("candidate_date") or ""
    )
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00")).date()
    except ValueError:
        return None


def _tier_one_review(
    row: Mapping[str, Any], candidate: Mapping[str, Any]
) -> FirstReview:
    text = _review_text(row, candidate)
    flags = []
    short_duration = bool(
        re.search(r"\b15\s*(?:m|min|mins|minute|minutes)\b", text)
        or "next 15" in text
        or "15m-" in text
    )
    deadline = bool(
        re.search(
            r"\b(?:by|before|through)\b", str(row.get("family_title") or "").casefold()
        )
        or re.search(r"\b(?:how high|how low|ever below|ever above)\b", text)
    )
    publication = bool(
        re.search(r"\b(?:publication|published|data release|report release)\b", text)
    )
    settlement_language = bool(
        re.search(r"\b(?:settlement price|settle oil price|daily settlement)\b", text)
    )
    multiple_schedule = "originally scheduled" in text
    source = str(candidate.get("candidate_source_type") or "")
    if short_duration:
        flags.append("recurring_intraday_one_hour_preexistence_risk")
    if deadline:
        flags.append("deadline_or_window_not_fixed_clock")
    if publication:
        flags.append("publication_or_result_timing")
    if settlement_language:
        flags.append("settlement_or_result_timing_language")
    if multiple_schedule:
        flags.append("multiple_plausible_scheduled_times")
    if source == "event_milestone_start_date":
        flags.append("milestone_source_requires_semantic_confirmation")

    if deadline or publication:
        return FirstReview(
            "recommend_reject",
            "needs_review",
            "fixed_clock",
            "no",
            "high" if deadline else "medium",
            "The proposed timestamp describes a deadline/window or publication event rather than an unambiguous fixed-clock occurrence.",
            tuple(sorted(set(flags))),
            True,
        )
    if (
        short_duration
        or settlement_language
        or multiple_schedule
        or source == "event_milestone_start_date"
    ):
        relevance = "yes" if short_duration else "uncertain"
        return FirstReview(
            "uncertain_human_review",
            "needs_review",
            "fixed_clock",
            relevance,
            "medium" if short_duration or settlement_language else "low",
            "The candidate may identify the intended clock time, but an automatic human-review flag prevents rule-level approval for this case.",
            tuple(sorted(set(flags))),
            True,
        )

    category = str(row.get("category") or "")
    confidence = "medium" if category == "Climate and Weather" else "high"
    return FirstReview(
        "recommend_rule_case",
        "needs_review",
        "fixed_clock",
        "yes",
        confidence,
        "The single exact allowed candidate agrees with a point-in-time or fixed calendar measurement described by the outcome-blind titles and context.",
        (),
        False,
    )


def _tier_two_review(
    row: Mapping[str, Any], candidate: Mapping[str, Any]
) -> FirstReview:
    text = _review_text(row, candidate)
    flags = []
    set_or_map = bool(
        re.search(r"\b(?:set|map)\s*\d+\b", text)
        or re.search(r"\bset score\b", text)
        or re.search(r"KX(?:ATP|WTA)SET|KX(?:CS2|VALORANT|DOTA2|LOL)MAP", text.upper())
    )
    partial_event = bool(re.search(r"\b(?:first half|second half|1h|2h)\b", text))
    conditional_subevent = bool(
        re.search(r"\b(?:playoff\?|first touchdown|first goal|first scorer)\b", text)
    )
    if set_or_map:
        flags.append("set_or_map_level_market")
    if partial_event:
        flags.append("partial_or_endogenous_subevent")
    if conditional_subevent:
        flags.append("conditional_endogenous_subevent")
    if str(row.get("evidence_pattern") or "") == "multiple_candidates_one_exact_time":
        flags.append("multiple_candidates_same_time")

    ticker_day = _ticker_date(row.get("family_id"))
    candidate_day = _candidate_date(candidate)
    date_gap = (
        abs((candidate_day - ticker_day).days) if ticker_day and candidate_day else 0
    )
    if date_gap > 1:
        flags.append("ticker_date_candidate_date_mismatch")

    if date_gap > 7 or conditional_subevent:
        return FirstReview(
            "recommend_reject",
            "needs_review",
            "scheduled_event_start",
            "no",
            "high" if date_gap > 7 else "medium",
            "The milestone time is not a defensible scheduled-start anchor for this contract because the date or conditional-subevent semantics conflict with the proposed interpretation.",
            tuple(sorted(set(flags))),
            True,
        )
    if flags:
        return FirstReview(
            "uncertain_human_review",
            "needs_review",
            "scheduled_event_start",
            "uncertain" if date_gap > 1 else "yes",
            "low" if date_gap > 1 else "medium",
            "The official milestone is plausible, but subevent, duplicate-evidence, or schedule-alignment ambiguity requires human review.",
            tuple(sorted(set(flags))),
            True,
        )

    confidence = (
        "high"
        if row.get("semantic_agreement") == "exact_informative_token_set"
        else "medium"
    )
    return FirstReview(
        "recommend_rule_case",
        "needs_review",
        "scheduled_event_start",
        "yes",
        confidence,
        "The unique official milestone start agrees with the event and family context and no subevent or schedule ambiguity is visible.",
        (),
        False,
    )


def _tier_three_review(row: Mapping[str, Any]) -> FirstReview:
    pattern = str(row.get("evidence_pattern") or "")
    reason = str(row.get("tier_reason") or "")
    if pattern == "multiple_distinct_exact_times":
        flags = ("multiple_distinct_exact_candidate_times",)
        rationale = "Competing exact timestamps prevent an outcome-blind scalable decision; Tier 3 quarantine is appropriate."
        confidence = "high"
    elif pattern == "no_candidate":
        flags = ("insufficient_evidence",)
        rationale = "No allowed candidate evidence exists, so the family must remain quarantined."
        confidence = "high"
    else:
        flags = (reason or "semantic_or_timing_ambiguity",)
        rationale = "The evidence does not satisfy either proposed rule and remains appropriate for Tier 3 quarantine."
        confidence = "medium"
    return FirstReview(
        "quarantine_tier_3",
        "needs_review",
        "",
        "uncertain",
        confidence,
        rationale,
        flags,
        False,
    )


def review_case(row: Mapping[str, Any]) -> FirstReview:
    tier = str(row.get("proposed_tier") or "")
    candidate = _proposed_candidate(row)
    if tier == "tier_1":
        if candidate is None:
            raise ValueError("Tier 1 case lacks its proposed candidate")
        return _tier_one_review(row, candidate)
    if tier == "tier_2":
        if candidate is None:
            raise ValueError("Tier 2 case lacks its proposed candidate")
        return _tier_two_review(row, candidate)
    if tier == "tier_3":
        return _tier_three_review(row)
    raise ValueError(f"unsupported proposed tier {tier!r}")
