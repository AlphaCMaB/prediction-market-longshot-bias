"""Approved ex-ante event-anchor selection policy."""

from __future__ import annotations

from dataclasses import asdict, dataclass
from typing import Any

from scripts.common.time_utils import format_iso_utc, parse_iso_utc


@dataclass(frozen=True)
class AnchorSelection:
    anchor_time: str
    anchor_source: str
    validation_status: str
    review_note: str

    def to_dict(self) -> dict[str, str]:
        return asdict(self)


def _verified_candidate(
    value: Any,
    *,
    verified: bool,
    source: str,
    note: str,
) -> AnchorSelection | None:
    parsed = parse_iso_utc(value)
    if not verified or parsed is None:
        return None
    return AnchorSelection(format_iso_utc(parsed), source, "verified", note)


def select_anchor(
    *,
    occurrence_datetime: Any = None,
    occurrence_verified: bool = False,
    scheduled_timestamp: Any = None,
    scheduled_timestamp_verified: bool = False,
    strike_date: Any = None,
    strike_date_semantically_verified: bool = False,
    manual_override: Any = None,
    manual_override_verified: bool = False,
    close_time: Any = None,
    review_note: str = "",
) -> AnchorSelection:
    """Select the highest-priority verified anchor.

    ``close_time`` is accepted only to make its exclusion explicit; it is
    never considered as a candidate.
    """
    del close_time

    candidates = (
        _verified_candidate(
            occurrence_datetime,
            verified=occurrence_verified,
            source="occurrence_datetime",
            note="Verified market occurrence_datetime.",
        ),
        _verified_candidate(
            scheduled_timestamp,
            verified=scheduled_timestamp_verified,
            source="official_scheduled_timestamp",
            note="Manually verified official event or contract timestamp.",
        ),
        _verified_candidate(
            strike_date,
            verified=strike_date_semantically_verified,
            source="strike_date",
            note="Semantically verified strike_date.",
        ),
        _verified_candidate(
            manual_override,
            verified=manual_override_verified,
            source="manual_override",
            note="Verified manual override.",
        ),
    )

    for candidate in candidates:
        if candidate is not None:
            if review_note:
                return AnchorSelection(
                    candidate.anchor_time,
                    candidate.anchor_source,
                    candidate.validation_status,
                    review_note,
                )
            return candidate

    return AnchorSelection("", "", "invalid_or_unverified", review_note or "No verified ex-ante anchor.")
