"""Outcome-blind classifiers for the explicitly approved Phase 10E rules.

These helpers apply PR1-M and PR2-M only to candidate evidence.  They do not
inspect outcomes, price data, or any post-event field.  A failed requirement
always leaves the family in ``needs_review``.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, datetime
import hashlib
import json
import re
from typing import Any, Mapping

from scripts.pipeline_v2.phase_10e_verification_design import title_agreement


RULE_SCHEMA_VERSION = "phase-10e-approved-rules-v1"
PR1 = "PR1_M_FIXED_CLOCK_SINGLE_EXACT"
PR2 = "PR2_M_SCHEDULED_START_SINGLE_MILESTONE"
APPROVED_RULES = (PR1, PR2)
ALLOWED_CANDIDATE_SOURCES = frozenset(
    {"market_occurrence_datetime", "event_strike_date", "event_milestone_start_date"}
)
ALLOWED_VERIFIED_SOURCES = frozenset(
    {
        "verified_occurrence_datetime",
        "validated_strike_date",
        "verified_official_scheduled_timestamp",
    }
)

RULE_SPECIFICATION = {
    "schema_version": RULE_SCHEMA_VERSION,
    "approval_label": "Phase 10E PR1-M and PR2-M explicitly approved",
    "horizon_separation": (
        "Anchor validity is independent of one-hour price availability; short-lived "
        "markets are not excluded merely because t-1h may precede market existence."
    ),
    "PR1_M": {
        "requirements": [
            "one exact allowed ex-ante candidate",
            "one predetermined exact observation time",
            "candidate corresponds to the contract reference",
            "no competing exact clock or semantic/date conflict",
        ],
        "benchmark_exception": (
            "An officially defined benchmark observation or settlement price qualifies "
            "when its time and value are exactly the contract reference."
        ),
    },
    "PR2_M": {
        "requirements": [
            "one unique exact official milestone-start candidate",
            "scheduled start matches the exact event scope",
            "title, event, and milestone semantics agree",
            "no competing start, time conflict, or scope conflict",
        ]
    },
}
RULE_SPECIFICATION_SHA256 = hashlib.sha256(
    json.dumps(RULE_SPECIFICATION, sort_keys=True, separators=(",", ":")).encode()
).hexdigest()


@dataclass(frozen=True)
class RuleDecision:
    approved: bool
    rule: str
    timing_structure: str
    reasons: tuple[str, ...]

    @property
    def primary_reason(self) -> str:
        return self.reasons[0] if self.reasons else "approved"


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


def _text(
    family: Mapping[str, Any], event: Mapping[str, Any], candidate: Mapping[str, Any]
) -> str:
    context = json.loads(str(candidate.get("evidence_context_json") or "{}"))
    if not isinstance(context, dict):
        raise ValueError("candidate evidence context must be an object")
    return " ".join(
        str(value or "")
        for value in (
            family.get("family_id"),
            family.get("representative_title"),
            event.get("series_ticker"),
            event.get("title"),
            event.get("sub_title"),
            candidate.get("candidate_title"),
            *context.values(),
        )
    ).casefold()


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


def _candidate_date(candidate: Mapping[str, Any]) -> date | None:
    try:
        return datetime.fromisoformat(
            str(candidate.get("candidate_time_utc") or "").replace("Z", "+00:00")
        ).date()
    except ValueError:
        return None


def _common_candidate_reasons(candidate: Mapping[str, Any]) -> list[str]:
    reasons = []
    if candidate.get("candidate_source_type") not in ALLOWED_CANDIDATE_SOURCES:
        reasons.append("candidate_source_not_allowed")
    if candidate.get("candidate_precision") != "exact_timestamp":
        reasons.append("candidate_not_exact_timestamp")
    timestamp = str(candidate.get("candidate_time_utc") or "")
    try:
        parsed = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            raise ValueError
    except ValueError:
        reasons.append("invalid_exact_timestamp")
    if (
        candidate.get("potential_verified_anchor_source")
        not in ALLOWED_VERIFIED_SOURCES
    ):
        reasons.append("verified_anchor_source_not_allowed")
    return reasons


def _has_subminute_precision(candidate: Mapping[str, Any]) -> bool:
    try:
        value = datetime.fromisoformat(
            str(candidate.get("candidate_time_utc") or "").replace("Z", "+00:00")
        )
    except ValueError:
        return False
    return bool(value.second or value.microsecond)


def _date_mismatch(family: Mapping[str, Any], candidate: Mapping[str, Any]) -> bool:
    ticker_day = _ticker_date(family.get("family_id"))
    candidate_day = _candidate_date(candidate)
    # A one-day UTC rollover is explicitly allowed.  Larger gaps require review.
    return bool(
        ticker_day and candidate_day and abs((candidate_day - ticker_day).days) > 1
    )


def classify_pr1(
    family: Mapping[str, Any], event: Mapping[str, Any], candidate: Mapping[str, Any]
) -> RuleDecision:
    """Apply modified PR1 to a mechanically eligible Tier-1 family."""
    text = _text(family, event, candidate)
    reasons = _common_candidate_reasons(candidate)
    benchmark = bool(
        re.search(r"\b(?:wti|crude oil|futures)\b", text)
        and re.search(
            r"\b(?:official )?(?:daily )?settlement price\b|\bsettle oil price\b", text
        )
    )
    deadline_or_window = bool(
        re.search(
            r"\b(?:how high|how low|highest|lowest|maximum|minimum|ever above|ever below)\b",
            text,
        )
        or re.search(
            r"\b(?:reach|reaches|reached)\b.{0,60}\b(?:by|before|during)\b", text
        )
        or re.search(
            r"\b(?:by|before|during|through|until|within)\b",
            str(family.get("representative_title") or "").casefold(),
        )
    )
    # Daily high/low weather and other extrema describe an interval, even when
    # the source exposes an exact boundary timestamp.
    if deadline_or_window and not benchmark:
        reasons.append("deadline_or_window_not_fixed_clock")
    if re.search(
        r"\b(?:publication|published|report release|data release|release time)\b", text
    ):
        reasons.append("publication_time_not_contract_defined_event")
    if re.search(
        r"\b(?:kalshi|platform)\b.{0,50}\b(?:settle|settlement|result processing)\b",
        text,
    ) or (
        re.search(r"\b(?:settlement|result processing) (?:time|timestamp)\b", text)
        and not benchmark
    ):
        reasons.append("platform_settlement_or_result_processing_time")
    if re.search(
        r"\b(?:originally scheduled|rescheduled|postponed|multiple scheduled)\b", text
    ):
        reasons.append("multiple_plausible_exact_clocks")
    if _date_mismatch(family, candidate):
        reasons.append("ticker_candidate_date_mismatch")
    if candidate.get("candidate_source_type") == "event_milestone_start_date":
        event_ok, _ = title_agreement(
            event.get("title"), candidate.get("candidate_title")
        )
        if not event_ok:
            reasons.append("semantically_unrelated_milestone")
    return RuleDecision(not reasons, PR1, "fixed_clock", tuple(dict.fromkeys(reasons)))


def classify_pr2(
    family: Mapping[str, Any], event: Mapping[str, Any], candidate: Mapping[str, Any]
) -> RuleDecision:
    """Apply modified PR2 to a mechanically eligible Tier-2 family."""
    text = _text(family, event, candidate)
    reasons = _common_candidate_reasons(candidate)
    if candidate.get("candidate_source_type") != "event_milestone_start_date":
        reasons.append("candidate_not_official_milestone_start")
    if re.search(r"\b(?:set|map)\s*\d+\b|\bset score\b|\bseries[- ]level\b", text):
        reasons.append("set_map_or_series_scope_not_independently_scheduled")
    if re.search(
        r"\b(?:first|second) half\b|\b(?:1h|2h)\b|\b(?:quarter|period|inning)\s*\d+\b"
        r"|\bfirst\s+\d+\s+(?:innings|minutes|periods|quarters)\b",
        text,
    ):
        reasons.append("partial_event_scope")
    if re.search(
        r"\bfirst\s+(?:[a-z0-9'’-]+\s+){0,3}(?:touchdown|goal|goalscorer|scorer|score)\b",
        text,
    ):
        reasons.append("endogenous_subevent")
    if re.search(
        r"\b(?:if necessary|if played|conditional subevent|playoff\?)\b", text
    ):
        reasons.append("conditional_subevent")
    if re.search(
        r"\b(?:settlement|result processing|report release|published)\b", text
    ):
        reasons.append("post_event_settlement_or_reporting_time")
    if re.search(
        r"\b(?:originally scheduled|rescheduled|postponed|multiple scheduled|tbd)\b",
        text,
    ):
        reasons.append("multiple_or_unclear_scheduled_starts")
    if _date_mismatch(family, candidate):
        reasons.append("ticker_candidate_date_mismatch")
    if _has_subminute_precision(candidate):
        reasons.append("subminute_timestamp_not_predetermined_schedule")
    context = json.loads(str(candidate.get("evidence_context_json") or "{}"))
    milestone_title = str(
        context.get("milestone_title") or candidate.get("candidate_title") or ""
    )
    event_ok, _ = title_agreement(event.get("title"), milestone_title)
    family_ok, _ = title_agreement(
        family.get("representative_title"),
        f"{event.get('title', '')} {milestone_title}",
    )
    if not event_ok or not family_ok:
        reasons.append("title_event_milestone_semantic_mismatch")
    return RuleDecision(
        not reasons, PR2, "scheduled_event_start", tuple(dict.fromkeys(reasons))
    )
