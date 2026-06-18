from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Optional


def parse_dt(value: Any) -> Optional[datetime]:
    if value is None or value == "":
        return None
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        # Heuristic: milliseconds if too large for unix seconds.
        if value > 10_000_000_000:
            value = value / 1000
        dt = datetime.fromtimestamp(value, tz=timezone.utc)
    elif isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            try:
                dt = datetime.fromtimestamp(float(text), tz=timezone.utc)
            except Exception:
                return None
    else:
        return None
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def as_float(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except Exception:
        return None


def normalize_probability(value: Any) -> Optional[float]:
    x = as_float(value)
    if x is None:
        return None
    # Some APIs express cents 0..100; normalize to 0..1.
    if 1 < x <= 100:
        x = x / 100
    if 0 <= x <= 1:
        return x
    return None


def standardize_category(raw: Optional[str], title: str = "") -> str:
    text = f"{raw or ''} {title}".lower()
    if any(k in text for k in ["nba", "nfl", "ncaab", "mlb", "nhl", "sports", "soccer", "ufc", "tennis"]):
        return "sports"
    if any(k in text for k in ["election", "president", "senate", "congress", "politic", "trump", "biden", "democrat", "republican"]):
        return "politics"
    if any(k in text for k in ["fed", "inflation", "cpi", "gdp", "recession", "rate", "econom", "crypto", "bitcoin"]):
        return "economics"
    if any(k in text for k in ["weather", "temperature", "hurricane", "rain", "snow"]):
        return "weather"
    return "other"
