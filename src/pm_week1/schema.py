from __future__ import annotations

from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Optional


@dataclass(frozen=True)
class MarketSnapshot:
    """One cleaned binary YES contract snapshot.

    One row = one binary YES contract, observed at a fixed pre-resolution time.
    This is the analysis unit for Brier score, calibration, and later longshot-bias tests.
    """

    venue: str
    market_id: str
    token_id: Optional[str]
    title: str
    category_raw: Optional[str]
    category: str
    resolution_time: datetime
    target_price_time: datetime
    price_time: datetime
    snapshot_hours_before_resolution: float
    p_hat: float
    outcome: int
    volume: Optional[float]
    liquidity: Optional[float]
    price_source: str
    raw_url: Optional[str]

    def to_dict(self) -> dict[str, Any]:
        d = asdict(self)
        for key in ["resolution_time", "target_price_time", "price_time"]:
            d[key] = d[key].isoformat()
        return d
