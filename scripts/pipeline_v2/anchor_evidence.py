"""Pure construction of unverified Kalshi anchor-candidate evidence."""

from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass
from datetime import date, datetime, timezone
import hashlib
import json
import re
from typing import Any, Iterable, Mapping

from scripts.common.time_utils import format_iso_utc
from scripts.pipeline_v2.kalshi_metadata_cache import canonical_json
from scripts.pipeline_v2.study_rules import StudyRules, analysis_window_bounds


EVIDENCE_SCHEMA_VERSION = "1.0"
DATE_ONLY_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")

ANCHOR_EVIDENCE_FIELDS = (
    "family_id", "family_id_source", "event_ticker", "candidate_id",
    "candidate_source_type", "candidate_original_value", "candidate_time_utc",
    "candidate_date", "candidate_precision", "potential_verified_anchor_source",
    "candidate_title", "evidence_reference", "evidence_context_json",
    "analysis_window_status", "review_status",
)
ANCHOR_FAMILY_REVIEW_FIELDS = (
    "family_id", "family_id_source", "market_count", "event_tickers_json",
    "representative_title", "category", "first_market_open_time", "candidate_count",
    "exact_timestamp_candidate_count", "date_only_candidate_count",
    "occurrence_candidate_count", "strike_date_candidate_count",
    "milestone_start_candidate_count", "distinct_exact_candidate_times_json",
    "has_conflicting_exact_candidate_times", "has_multiple_event_tickers",
    "missing_event_metadata", "invalid_candidate_value_count",
    "sentinel_timestamp_count", "review_status", "review_reason",
    "candidate_ids_json",
)
DECISION_TEMPLATE_FIELDS = (
    "family_id", "family_id_source", "verification_status", "verified_anchor_time",
    "verified_anchor_source", "timing_structure", "evidence_reference", "review_note",
)

MARKET_REQUIRED_FIELDS = frozenset({"family_id", "family_id_source", "event_ticker"})
EVENT_REQUIRED_FIELDS = frozenset({"event_ticker", "strike_date"})
MILESTONE_REQUIRED_FIELDS = frozenset(
    {
        "event_ticker", "milestone_id", "milestone_start_date",
        "milestone_end_date", "association_type",
    }
)


@dataclass(frozen=True)
class CandidateParse:
    original_value: str
    candidate_time_utc: str = ""
    candidate_date: str = ""
    precision: str = ""
    issue: str = ""

    @property
    def valid(self) -> bool:
        return bool(self.precision)


@dataclass(frozen=True)
class EvidenceBuild:
    evidence_rows: tuple[dict[str, Any], ...]
    family_rows: tuple[dict[str, Any], ...]
    decision_rows: tuple[dict[str, Any], ...]
    statistics: dict[str, int]


def family_identity(row: Mapping[str, Any]) -> tuple[str, str]:
    identity = (
        str(row.get("family_id") or "").strip(),
        str(row.get("family_id_source") or "").strip(),
    )
    if not all(identity):
        raise ValueError("market rows require family_id and family_id_source")
    return identity


def _aware_timestamp(value: str) -> datetime | None:
    text = value
    if not text or DATE_ONLY_PATTERN.fullmatch(text):
        return None
    if text.endswith("Z"):
        parse_value = text[:-1] + "+00:00"
    else:
        parse_value = text
    try:
        parsed = datetime.fromisoformat(parse_value)
    except ValueError:
        return None
    if parsed.tzinfo is None:
        return None
    if parsed.year == 1:
        return None
    try:
        return parsed.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        return None


def parse_candidate_value(value: Any, *, allow_date_only: bool) -> CandidateParse:
    original = "" if value is None else str(value)
    if not original.strip():
        return CandidateParse(original_value=original)
    if DATE_ONLY_PATTERN.fullmatch(original):
        try:
            parsed_date = date.fromisoformat(original)
        except ValueError:
            return CandidateParse(original_value=original, issue="invalid_candidate_value")
        if parsed_date.year == 1:
            return CandidateParse(original_value=original, issue="sentinel_timestamp")
        if allow_date_only:
            return CandidateParse(
                original_value=original, candidate_date=original, precision="date_only"
            )
        return CandidateParse(original_value=original, issue="invalid_candidate_value")
    parse_value = original[:-1] + "+00:00" if original.endswith("Z") else original
    try:
        parsed_local = datetime.fromisoformat(parse_value)
    except ValueError:
        return CandidateParse(original_value=original, issue="invalid_candidate_value")
    if parsed_local.tzinfo is None:
        return CandidateParse(original_value=original, issue="invalid_candidate_value")
    if parsed_local.year == 1:
        return CandidateParse(original_value=original, issue="sentinel_timestamp")
    try:
        parsed = parsed_local.astimezone(timezone.utc)
    except (OverflowError, ValueError):
        return CandidateParse(original_value=original, issue="invalid_candidate_value")
    return CandidateParse(
        original_value=original,
        candidate_time_utc=format_iso_utc(parsed).replace("+00:00", "Z"),
        precision="exact_timestamp",
    )


def analysis_window_status(parsed: CandidateParse, rules: StudyRules) -> str:
    if parsed.precision == "date_only":
        return "date_only_unknown"
    if parsed.precision != "exact_timestamp":
        return "invalid_or_missing"
    value = _aware_timestamp(parsed.candidate_time_utc)
    start, end = analysis_window_bounds(rules)
    if value is None:
        return "invalid_or_missing"
    if value < start:
        return "before_analysis_window"
    if value >= end:
        return "at_or_after_analysis_window"
    return "inside_analysis_window"


def _candidate_id(
    identity: tuple[str, str], event_ticker: str, source_type: str, source_identity: str,
    parsed: CandidateParse,
) -> str:
    normalized_value = parsed.candidate_time_utc or parsed.candidate_date
    payload = {
        "family_id": identity[0],
        "family_id_source": identity[1],
        "event_ticker": event_ticker,
        "candidate_source_type": source_type,
        "source_identity": source_identity,
        "normalized_candidate_value": normalized_value,
    }
    return hashlib.sha256(canonical_json(payload)).hexdigest()


def _context(values: Mapping[str, Any]) -> str:
    return canonical_json(
        {key: value for key, value in values.items() if value not in (None, "")}
    ).decode("utf-8")


def _milestone_comparison_projection(row: Mapping[str, Any]) -> dict[str, Any]:
    """Compare only candidate and approved contextual milestone fields."""
    return {
        key: row.get(key, "")
        for key in (
            "event_ticker", "milestone_id", "milestone_category", "milestone_type",
            "milestone_title", "milestone_start_date",
            "milestone_source_id", "milestone_source_ids_json",
            "milestone_details_json", "association_type",
        )
    }


def _candidate(
    identity: tuple[str, str], *, event_ticker: str, source_type: str,
    original_value: Any, allow_date_only: bool, potential_source: str,
    title: str, reference: str, source_identity: str,
    context: Mapping[str, Any], rules: StudyRules,
) -> tuple[dict[str, Any] | None, str]:
    parsed = parse_candidate_value(original_value, allow_date_only=allow_date_only)
    if not parsed.valid:
        return None, parsed.issue
    return {
        "family_id": identity[0],
        "family_id_source": identity[1],
        "event_ticker": event_ticker,
        "candidate_id": _candidate_id(
            identity, event_ticker, source_type, source_identity, parsed
        ),
        "candidate_source_type": source_type,
        "candidate_original_value": parsed.original_value,
        "candidate_time_utc": parsed.candidate_time_utc,
        "candidate_date": parsed.candidate_date,
        "candidate_precision": parsed.precision,
        "potential_verified_anchor_source": potential_source,
        "candidate_title": title,
        "evidence_reference": reference,
        "evidence_context_json": _context(context),
        "analysis_window_status": analysis_window_status(parsed, rules),
        "review_status": "needs_review",
    }, ""


def _first_open_time(markets: Iterable[Mapping[str, Any]]) -> str:
    valid = []
    for market in markets:
        parsed = _aware_timestamp(str(market.get("market_open_time") or market.get("open_time") or ""))
        if parsed is not None:
            valid.append(parsed)
    return format_iso_utc(min(valid)).replace("+00:00", "Z") if valid else ""


def _review_reason(
    *, multiple_events: bool, missing_event: bool, conflicting_times: bool,
    candidate_count: int, date_only_count: int,
) -> str:
    if multiple_events:
        return "multiple_event_tickers"
    if missing_event:
        return "missing_event_metadata"
    if conflicting_times:
        return "multiple_exact_candidate_times"
    if candidate_count == 0:
        return "no_candidate_anchor_evidence"
    if date_only_count:
        return "date_only_candidate_requires_semantic_review"
    return "candidate_evidence_requires_review"


def build_anchor_evidence(
    markets: Iterable[Mapping[str, Any]],
    events: Iterable[Mapping[str, Any]],
    milestones: Iterable[Mapping[str, Any]],
    rules: StudyRules,
) -> EvidenceBuild:
    market_rows = [dict(row) for row in markets]
    event_rows = [dict(row) for row in events]
    milestone_rows = [dict(row) for row in milestones]

    grouped_markets: dict[tuple[str, str], list[dict[str, Any]]] = defaultdict(list)
    all_market_events: set[str] = set()
    for market in market_rows:
        grouped_markets[family_identity(market)].append(market)
        ticker = str(market.get("event_ticker") or "").strip()
        if ticker:
            all_market_events.add(ticker)

    events_by_ticker: dict[str, dict[str, Any]] = {}
    for event in event_rows:
        ticker = str(event.get("event_ticker") or "").strip()
        if not ticker:
            raise ValueError("event metadata row requires event_ticker")
        if ticker not in all_market_events:
            raise ValueError(f"unexpected event metadata ticker {ticker!r}")
        if ticker in events_by_ticker:
            raise ValueError(f"duplicate event metadata row for {ticker!r}")
        events_by_ticker[ticker] = event

    milestones_by_event: dict[str, list[dict[str, Any]]] = defaultdict(list)
    milestone_identities: dict[tuple[str, str], bytes] = {}
    for milestone in milestone_rows:
        ticker = str(milestone.get("event_ticker") or "").strip()
        identifier = str(milestone.get("milestone_id") or "").strip()
        if not ticker or not identifier:
            raise ValueError("milestone rows require event_ticker and milestone_id")
        if ticker not in all_market_events:
            raise ValueError(f"unexpected milestone event ticker {ticker!r}")
        key = (ticker, identifier)
        encoded = canonical_json(_milestone_comparison_projection(milestone))
        if key in milestone_identities and milestone_identities[key] != encoded:
            raise ValueError(f"conflicting duplicate milestone identity {key!r}")
        if key not in milestone_identities:
            milestones_by_event[ticker].append(milestone)
            milestone_identities[key] = encoded

    evidence: list[dict[str, Any]] = []
    family_reviews: list[dict[str, Any]] = []
    decisions: list[dict[str, Any]] = []
    invalid_total = sentinel_total = 0
    family_without_candidate = family_conflicting = family_multiple_events = 0
    family_missing_event = 0

    for identity in sorted(grouped_markets, key=lambda item: (item[1], item[0])):
        family_markets = sorted(
            grouped_markets[identity],
            key=lambda row: (
                str(row.get("event_ticker") or ""),
                str(row.get("ticker") or row.get("market_id") or ""),
            ),
        )
        event_tickers = sorted({
            str(row.get("event_ticker") or "").strip()
            for row in family_markets if str(row.get("event_ticker") or "").strip()
        })
        family_candidates: list[dict[str, Any]] = []
        invalid_count = sentinel_count = 0

        for market in family_markets:
            value = market.get("occurrence_datetime")
            candidate, issue = _candidate(
                identity,
                event_ticker=str(market.get("event_ticker") or "").strip(),
                source_type="market_occurrence_datetime",
                original_value=value,
                allow_date_only=False,
                potential_source="verified_occurrence_datetime",
                title=str(market.get("title") or ""),
                reference=f"market:{str(market.get('ticker') or market.get('market_id') or '').strip()}",
                source_identity=str(market.get("ticker") or market.get("market_id") or "").strip(),
                context={
                    key: market.get(key, "")
                    for key in (
                        "ticker", "event_ticker", "title", "subtitle",
                        "rules_primary", "rules_secondary",
                    )
                },
                rules=rules,
            )
            if candidate:
                family_candidates.append(candidate)
            elif issue:
                invalid_count += issue == "invalid_candidate_value"
                sentinel_count += issue == "sentinel_timestamp"

        missing_event = any(ticker not in events_by_ticker for ticker in event_tickers)
        for ticker in event_tickers:
            event = events_by_ticker.get(ticker)
            if event is not None:
                candidate, issue = _candidate(
                    identity, event_ticker=ticker, source_type="event_strike_date",
                    original_value=event.get("strike_date"), allow_date_only=True,
                    potential_source="validated_strike_date",
                    title=str(event.get("title") or ""),
                    reference=f"event:{ticker}", source_identity=ticker,
                    context={
                        "event_title": event.get("title", ""),
                        "sub_title": event.get("sub_title", ""),
                        "category": event.get("category", ""),
                        "series_ticker": event.get("series_ticker", ""),
                        "strike_period": event.get("strike_period", ""),
                        "settlement_sources_json": event.get("settlement_sources_json", ""),
                    },
                    rules=rules,
                )
                if candidate:
                    family_candidates.append(candidate)
                elif issue:
                    invalid_count += issue == "invalid_candidate_value"
                    sentinel_count += issue == "sentinel_timestamp"

            for milestone in sorted(
                milestones_by_event.get(ticker, ()),
                key=lambda row: (
                    str(row.get("milestone_id") or ""),
                    str(row.get("association_type") or ""),
                ),
            ):
                milestone_id = str(milestone.get("milestone_id") or "").strip()
                candidate, issue = _candidate(
                    identity, event_ticker=ticker,
                    source_type="event_milestone_start_date",
                    original_value=milestone.get("milestone_start_date"),
                    allow_date_only=False,
                    potential_source="verified_official_scheduled_timestamp",
                    title=str(milestone.get("milestone_title") or ""),
                    reference=f"milestone:{ticker}:{milestone_id}",
                    source_identity=f"{ticker}:{milestone_id}:{milestone.get('association_type', '')}",
                    context={
                        key: milestone.get(key, "")
                        for key in (
                            "milestone_id", "milestone_title", "milestone_category",
                            "milestone_type", "association_type",
                            "milestone_source_id", "milestone_source_ids_json",
                            "milestone_details_json",
                        )
                    },
                    rules=rules,
                )
                if candidate:
                    family_candidates.append(candidate)
                elif issue:
                    invalid_count += issue == "invalid_candidate_value"
                    sentinel_count += issue == "sentinel_timestamp"

        unique_candidates: dict[str, dict[str, Any]] = {}
        for row in family_candidates:
            candidate_id = row["candidate_id"]
            existing = unique_candidates.get(candidate_id)
            if existing is None:
                unique_candidates[candidate_id] = row
            elif canonical_json(existing) != canonical_json(row):
                raise ValueError(
                    f"conflicting candidate duplicate for candidate_id {candidate_id}"
                )
        family_candidates = sorted(
            unique_candidates.values(),
            key=lambda row: (
                row["family_id_source"], row["family_id"],
                row["candidate_source_type"],
                row["candidate_time_utc"] or row["candidate_date"],
                row["candidate_id"],
            ),
        )
        evidence.extend(family_candidates)
        exact_times = sorted({
            row["candidate_time_utc"] for row in family_candidates
            if row["candidate_precision"] == "exact_timestamp"
        })
        date_only_count = sum(
            row["candidate_precision"] == "date_only" for row in family_candidates
        )
        conflicting = len(exact_times) > 1
        multiple_events = len(event_tickers) > 1
        family_without_candidate += not family_candidates
        family_conflicting += conflicting
        family_multiple_events += multiple_events
        family_missing_event += missing_event
        invalid_total += invalid_count
        sentinel_total += sentinel_count
        representative_title = next(
            (str(row.get("title") or "") for row in family_markets if row.get("title")), ""
        )
        category = next(
            (
                str(events_by_ticker[ticker].get("category") or "")
                for ticker in event_tickers
                if ticker in events_by_ticker and events_by_ticker[ticker].get("category")
            ),
            "",
        )
        review_reason = _review_reason(
            multiple_events=multiple_events, missing_event=missing_event,
            conflicting_times=conflicting, candidate_count=len(family_candidates),
            date_only_count=date_only_count,
        )
        family_reviews.append({
            "family_id": identity[0],
            "family_id_source": identity[1],
            "market_count": len(family_markets),
            "event_tickers_json": canonical_json(event_tickers).decode("utf-8"),
            "representative_title": representative_title,
            "category": category,
            "first_market_open_time": _first_open_time(family_markets),
            "candidate_count": len(family_candidates),
            "exact_timestamp_candidate_count": len(family_candidates) - date_only_count,
            "date_only_candidate_count": date_only_count,
            "occurrence_candidate_count": sum(
                row["candidate_source_type"] == "market_occurrence_datetime"
                for row in family_candidates
            ),
            "strike_date_candidate_count": sum(
                row["candidate_source_type"] == "event_strike_date"
                for row in family_candidates
            ),
            "milestone_start_candidate_count": sum(
                row["candidate_source_type"] == "event_milestone_start_date"
                for row in family_candidates
            ),
            "distinct_exact_candidate_times_json": canonical_json(exact_times).decode("utf-8"),
            "has_conflicting_exact_candidate_times": str(conflicting).lower(),
            "has_multiple_event_tickers": str(multiple_events).lower(),
            "missing_event_metadata": str(missing_event).lower(),
            "invalid_candidate_value_count": invalid_count,
            "sentinel_timestamp_count": sentinel_count,
            "review_status": "needs_review",
            "review_reason": review_reason,
            "candidate_ids_json": canonical_json(
                [row["candidate_id"] for row in family_candidates]
            ).decode("utf-8"),
        })
        decisions.append({
            "family_id": identity[0],
            "family_id_source": identity[1],
            "verification_status": "needs_review",
            "verified_anchor_time": "",
            "verified_anchor_source": "",
            "timing_structure": "",
            "evidence_reference": "",
            "review_note": "Review anchor_family_review.csv and anchor_evidence.csv",
        })

    statistics = {
        "market_count": len(market_rows),
        "family_count": len(grouped_markets),
        "event_metadata_count": sum(ticker in events_by_ticker for ticker in all_market_events),
        "milestone_association_count": sum(
            len(milestones_by_event.get(ticker, ())) for ticker in all_market_events
        ),
        "candidate_count": len(evidence),
        "occurrence_candidate_count": sum(
            row["candidate_source_type"] == "market_occurrence_datetime" for row in evidence
        ),
        "strike_date_candidate_count": sum(
            row["candidate_source_type"] == "event_strike_date" for row in evidence
        ),
        "milestone_start_candidate_count": sum(
            row["candidate_source_type"] == "event_milestone_start_date" for row in evidence
        ),
        "exact_timestamp_candidate_count": sum(
            row["candidate_precision"] == "exact_timestamp" for row in evidence
        ),
        "date_only_candidate_count": sum(
            row["candidate_precision"] == "date_only" for row in evidence
        ),
        "family_with_no_candidate_count": family_without_candidate,
        "family_with_multiple_exact_candidate_times_count": family_conflicting,
        "family_with_multiple_event_tickers_count": family_multiple_events,
        "family_missing_event_metadata_count": family_missing_event,
        "invalid_candidate_value_count": invalid_total,
        "sentinel_timestamp_count": sentinel_total,
    }
    return EvidenceBuild(
        tuple(evidence), tuple(family_reviews), tuple(decisions), statistics
    )
