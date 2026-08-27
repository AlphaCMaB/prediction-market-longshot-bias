"""Pure date, endpoint, and request planning for Kalshi metadata ingestion."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import date, datetime, time, timedelta, timezone
from typing import Any, Iterable


LIVE_PATH = "/trade-api/v2/markets"
HISTORICAL_PATH = "/trade-api/v2/historical/markets"
CUTOFF_PATH = "/trade-api/v2/historical/cutoff"


@dataclass(frozen=True)
class DateInterval:
    start: datetime
    end: datetime

    def __post_init__(self):
        if self.start.tzinfo is None or self.end.tzinfo is None or self.start >= self.end:
            raise ValueError("interval must be an increasing timezone-aware range")


@dataclass(frozen=True)
class MonthInterval:
    month: str
    start: datetime
    end: datetime


@dataclass(frozen=True)
class EndpointSegment:
    tier: str
    endpoint_path: str
    start: datetime | None
    end: datetime | None
    month: str | None = None

    @property
    def endpoint_tier(self) -> str:
        return self.tier

    @property
    def range_start_utc(self) -> str | None:
        return format_utc(self.start)

    @property
    def range_end_utc_exclusive(self) -> str | None:
        return format_utc(self.end)


def format_utc(value: datetime | None) -> str | None:
    if value is None:
        return None
    return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def normalize_inclusive_dates(start_date: str, end_date: str) -> DateInterval:
    try:
        start_day = date.fromisoformat(start_date)
        end_day = date.fromisoformat(end_date)
    except ValueError as exc:
        raise ValueError("dates must use YYYY-MM-DD") from exc
    if end_day < start_day:
        raise ValueError("end date must not precede start date")
    return DateInterval(
        datetime.combine(start_day, time.min, tzinfo=timezone.utc),
        datetime.combine(end_day + timedelta(days=1), time.min, tzinfo=timezone.utc),
    )


def _next_month(day: date) -> date:
    return date(day.year + (day.month == 12), 1 if day.month == 12 else day.month + 1, 1)


def generate_months(interval: DateInterval) -> tuple[MonthInterval, ...]:
    cursor = date(interval.start.year, interval.start.month, 1)
    result = []
    while datetime.combine(cursor, time.min, tzinfo=timezone.utc) < interval.end:
        next_day = _next_month(cursor)
        month_start = max(interval.start, datetime.combine(cursor, time.min, tzinfo=timezone.utc))
        month_end = min(interval.end, datetime.combine(next_day, time.min, tzinfo=timezone.utc))
        result.append(MonthInterval(cursor.strftime("%Y-%m"), month_start, month_end))
        cursor = next_day
    return tuple(result)


def filter_month(months: Iterable[MonthInterval], selected: str | None) -> tuple[MonthInterval, ...]:
    values = tuple(months)
    if selected is None:
        return values
    try:
        parsed = date.fromisoformat(selected + "-01")
    except ValueError as exc:
        raise ValueError("month must use YYYY-MM") from exc
    canonical = parsed.strftime("%Y-%m")
    filtered = tuple(month for month in values if month.month == canonical)
    if not filtered:
        raise ValueError("selected month is outside the requested date range")
    return filtered


def split_month(month: MonthInterval, cutoff: datetime) -> tuple[EndpointSegment, ...]:
    if cutoff.tzinfo is None:
        raise ValueError("cutoff must be timezone-aware")
    parts = []
    historical_end = min(month.end, cutoff)
    if month.start < historical_end:
        parts.append(EndpointSegment("historical", HISTORICAL_PATH, month.start, historical_end, month.month))
    live_start = max(month.start, cutoff)
    if live_start < month.end:
        parts.append(EndpointSegment("live", LIVE_PATH, live_start, month.end, month.month))
    return tuple(parts)


def plan_endpoint_segments(
    months: Iterable[MonthInterval], cutoff: datetime, *, historical_mode: str = "auto", live_mode: str = "auto"
) -> tuple[EndpointSegment, ...]:
    month_values = tuple(months)
    split = [segment for month in month_values for segment in split_month(month, cutoff)]
    historical_needed = any(segment.tier == "historical" for segment in split)
    live = [segment for segment in split if segment.tier == "live"]
    if historical_mode == "require" and not historical_needed:
        raise ValueError("historical data was required but the range is live-only")
    if live_mode == "require" and not live:
        raise ValueError("live data was required but the range is historical-only")
    result = []
    if historical_needed and historical_mode != "skip":
        starts = [segment.start for segment in split if segment.tier == "historical"]
        ends = [segment.end for segment in split if segment.tier == "historical"]
        result.append(EndpointSegment("historical", HISTORICAL_PATH, min(starts), max(ends), None))
    if live_mode != "skip":
        result.extend(live)
    return tuple(result)


def segment_params(
    segment: EndpointSegment,
    page_size: int,
    cursor: str | None = None,
    mve_filter: str = "exclude",
) -> dict[str, Any]:
    if not 1 <= page_size <= 1000:
        raise ValueError("page size must be between 1 and 1000")
    params: dict[str, Any] = {"limit": page_size, "mve_filter": mve_filter}
    if segment.tier == "live":
        params.update({
            "status": "settled",
            "min_settled_ts": int(segment.start.timestamp()),
            "max_settled_ts": int(segment.end.timestamp()) - 1,
        })
    if cursor:
        params["cursor"] = cursor
    return params


def canonical_parameters(params: dict[str, Any]) -> str:
    return json.dumps(params, sort_keys=True, separators=(",", ":"), ensure_ascii=True)


def request_id(endpoint_path: str, params: dict[str, Any], cutoff_id: str) -> str:
    material = "|".join((endpoint_path, canonical_parameters(params), cutoff_id))
    return hashlib.sha256(material.encode()).hexdigest()


def cursor_hash(cursor: str | None) -> str:
    return "start" if not cursor else hashlib.sha256(cursor.encode()).hexdigest()[:16]


def deterministic_page_filename(page_number: int, cursor: str | None, request_identifier: str) -> str:
    return f"page_{page_number:06d}_{cursor_hash(cursor)}_{request_identifier[:20]}.json"


def estimate_requests(segments: Iterable[EndpointSegment], cached_pages: int = 0) -> dict[str, Any]:
    values = tuple(segments)
    live_count = sum(segment.tier == "live" for segment in values)
    historical = any(segment.tier == "historical" for segment in values)
    minimum = max(0, live_count + int(historical) - cached_pages)
    return {
        "known_minimum_requests": minimum,
        "unknown_historical_request_component": historical,
        "planned_live_chains": live_count,
        "planned_historical_chains": int(historical),
    }
