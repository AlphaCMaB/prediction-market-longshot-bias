"""Pure UTC timestamp parsing and formatting helpers."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def parse_iso_utc(value: Any) -> datetime | None:
    """Parse an ISO timestamp and normalize it to an aware UTC datetime."""
    if value is None:
        return None

    text = str(value).strip()
    if not text:
        return None

    if text.endswith("Z"):
        text = text[:-1] + "+00:00"

    try:
        parsed = datetime.fromisoformat(text)
    except (TypeError, ValueError):
        return None

    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=timezone.utc)

    return parsed.astimezone(timezone.utc)


def format_iso_utc(value: datetime | None) -> str:
    """Format a datetime consistently in UTC, returning blank for None."""
    if value is None:
        return ""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat()
