"""Pure probability parsing, validation, and binning helpers."""

from __future__ import annotations

import math
from typing import Any


def safe_float(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        parsed = float(value)
    except (TypeError, ValueError):
        return None
    return parsed if math.isfinite(parsed) else None


def is_valid_probability(value: Any) -> bool:
    parsed = safe_float(value)
    return parsed is not None and 0.0 <= parsed <= 1.0


def probability_bin(value: Any, *, width: float = 0.1) -> str:
    """Label a valid probability using left-closed, right-open bins."""
    parsed = safe_float(value)
    if parsed is None or not 0.0 <= parsed <= 1.0:
        return "missing"
    if width <= 0 or width > 1:
        raise ValueError("width must be in (0, 1]")

    if parsed == 1.0:
        lower = max(0.0, 1.0 - width)
        upper = 1.0
    else:
        index = int(parsed / width)
        lower = index * width
        upper = min(1.0, lower + width)
    return f"{lower:.1f}-{upper:.1f}"
