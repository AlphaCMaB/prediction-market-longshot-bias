"""Explicit Methodology V2 TOML configuration loading and validation."""

from __future__ import annotations

import tomllib
from dataclasses import dataclass
from pathlib import Path


REQUIRED_KEYS = (
    "candidate_horizons_hours",
    "fixed_clock_selected_horizons_hours",
    "scheduled_event_start_selected_horizons_hours",
    "main_staleness_minutes",
    "robustness_staleness_minutes",
    "early_settlement_tolerance_minutes",
)


@dataclass(frozen=True)
class PipelineV2Config:
    candidate_horizons_hours: tuple[int, ...]
    fixed_clock_selected_horizons_hours: tuple[int, ...]
    scheduled_event_start_selected_horizons_hours: tuple[int, ...]
    main_staleness_minutes: float
    robustness_staleness_minutes: float
    early_settlement_tolerance_minutes: float
    candlestick_interval_minutes: int = 1
    candlestick_lookback_hours: int = 24
    batch_size: int = 100

    @property
    def selected_horizons(self) -> dict[str, tuple[int, ...]]:
        return {
            "fixed_clock": self.fixed_clock_selected_horizons_hours,
            "scheduled_event_start": self.scheduled_event_start_selected_horizons_hours,
        }


def _positive_numbers(values, key: str) -> tuple[int, ...]:
    if not isinstance(values, list) or not values:
        raise ValueError(f"{key} must be a non-empty list")
    try:
        result = tuple(int(value) for value in values)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{key} must contain integers") from exc
    if any(value <= 0 for value in result):
        raise ValueError(f"{key} values must be positive")
    return result


def load_config(path: str | Path) -> PipelineV2Config:
    """Load configuration only when explicitly called."""
    with Path(path).open("rb") as handle:
        data = tomllib.load(handle)
    missing = [key for key in REQUIRED_KEYS if key not in data]
    if missing:
        raise ValueError(f"Missing required configuration values: {', '.join(missing)}")

    candidate = _positive_numbers(data["candidate_horizons_hours"], "candidate_horizons_hours")
    fixed = _positive_numbers(data["fixed_clock_selected_horizons_hours"], "fixed_clock_selected_horizons_hours")
    scheduled = _positive_numbers(data["scheduled_event_start_selected_horizons_hours"], "scheduled_event_start_selected_horizons_hours")
    if not set(fixed + scheduled).issubset(candidate):
        raise ValueError("Selected horizons must be included in candidate_horizons_hours")

    main = float(data["main_staleness_minutes"])
    robust = float(data["robustness_staleness_minutes"])
    tolerance = float(data["early_settlement_tolerance_minutes"])
    if main < 0 or robust < main or tolerance < 0:
        raise ValueError("Staleness and tolerance values are inconsistent")

    return PipelineV2Config(
        candidate_horizons_hours=candidate,
        fixed_clock_selected_horizons_hours=fixed,
        scheduled_event_start_selected_horizons_hours=scheduled,
        main_staleness_minutes=main,
        robustness_staleness_minutes=robust,
        early_settlement_tolerance_minutes=tolerance,
        candlestick_interval_minutes=int(data.get("candlestick_interval_minutes", 1)),
        candlestick_lookback_hours=int(data.get("candlestick_lookback_hours", 24)),
        batch_size=int(data.get("batch_size", 100)),
    )
