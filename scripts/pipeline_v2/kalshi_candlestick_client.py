"""Cache-first, injected Kalshi candlestick HTTP client."""

from __future__ import annotations

import hashlib
import json
import time
from pathlib import Path
from typing import Any, Callable, Iterable


DEFAULT_BASE_URL = "https://external-api.kalshi.com/trade-api/v2/markets/candlesticks"


def batch_tickers(tickers: Iterable[str], batch_size: int) -> list[list[str]]:
    if batch_size <= 0:
        raise ValueError("batch_size must be positive")
    normalized = sorted({str(ticker).strip() for ticker in tickers if str(ticker).strip()})
    return [normalized[index:index + batch_size] for index in range(0, len(normalized), batch_size)]


def deterministic_cache_key(
    tickers: Iterable[str], start_ts: int, end_ts: int, interval_minutes: int
) -> str:
    material = "|".join(
        [str(int(start_ts)), str(int(end_ts)), str(int(interval_minutes)), *sorted(set(tickers))]
    )
    return hashlib.sha256(material.encode("utf-8")).hexdigest()[:20]


def deterministic_cache_path(
    cache_dir: str | Path,
    tickers: Iterable[str],
    start_ts: int,
    end_ts: int,
    interval_minutes: int,
) -> Path:
    normalized = sorted(set(tickers))
    key = deterministic_cache_key(normalized, start_ts, end_ts, interval_minutes)
    return Path(cache_dir) / f"batch_{int(end_ts)}_{len(normalized)}_{key}.json"


class KalshiCandlestickClient:
    def __init__(
        self,
        *,
        session: Any,
        cache_dir: str | Path,
        base_url: str = DEFAULT_BASE_URL,
        interval_minutes: int = 1,
        batch_size: int = 100,
        max_retries: int = 5,
        timeout_seconds: float = 45,
        backoff_base_seconds: float = 1,
        max_backoff_seconds: float = 30,
        sleep: Callable[[float], None] = time.sleep,
        dry_run: bool = False,
    ) -> None:
        self.session = session
        self.cache_dir = Path(cache_dir)
        self.base_url = base_url
        self.interval_minutes = interval_minutes
        self.batch_size = batch_size
        self.max_retries = max_retries
        self.timeout_seconds = timeout_seconds
        self.backoff_base_seconds = backoff_base_seconds
        self.max_backoff_seconds = max_backoff_seconds
        self.sleep = sleep
        self.dry_run = dry_run
        self.anticipated_request_count = 0
        self.completed_request_count = 0
        self.cache_hit_count = 0

    def cache_path(self, tickers: Iterable[str], start_ts: int, end_ts: int) -> Path:
        return deterministic_cache_path(
            self.cache_dir, tickers, start_ts, end_ts, self.interval_minutes
        )

    def plan_request_count(self, tickers: Iterable[str]) -> int:
        return len(batch_tickers(tickers, self.batch_size))

    def _load_cache(self, path: Path) -> dict[str, Any] | None:
        if not path.exists():
            return None
        self.cache_hit_count += 1
        with path.open(encoding="utf-8") as handle:
            return json.load(handle)

    def _write_cache_once(self, path: Path, payload: dict[str, Any]) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        try:
            with path.open("x", encoding="utf-8") as handle:
                json.dump(payload, handle, ensure_ascii=False, indent=2)
        except FileExistsError:
            return

    def request_batch(self, tickers: Iterable[str], start_ts: int, end_ts: int) -> dict[str, Any]:
        normalized = sorted(set(tickers))
        path = self.cache_path(normalized, start_ts, end_ts)
        cached = self._load_cache(path)
        if cached is not None:
            return cached

        self.anticipated_request_count += 1
        if self.dry_run:
            return {"markets": [], "dry_run": True}

        params = {
            "market_tickers": ",".join(normalized),
            "start_ts": int(start_ts),
            "end_ts": int(end_ts),
            "period_interval": self.interval_minutes,
            "include_latest_before_start": "false",
        }
        last_error: Exception | None = None
        for attempt in range(self.max_retries):
            try:
                self.completed_request_count += 1
                response = self.session.get(
                    self.base_url, params=params, timeout=self.timeout_seconds
                )
                if response.status_code == 429:
                    raise RuntimeError("rate_limited")
                response.raise_for_status()
                payload = response.json()
                self._write_cache_once(path, payload)
                return payload
            except Exception as exc:
                last_error = exc
                if attempt + 1 < self.max_retries:
                    delay = min(
                        self.backoff_base_seconds * (2 ** attempt), self.max_backoff_seconds
                    )
                    self.sleep(delay)
        raise RuntimeError(f"Kalshi candlestick request failed: {last_error}")

    def fetch_with_recursive_split(
        self, tickers: Iterable[str], start_ts: int, end_ts: int
    ) -> list[dict[str, Any]]:
        normalized = sorted(set(tickers))
        if not normalized:
            return []
        try:
            payload = self.request_batch(normalized, start_ts, end_ts)
            markets = payload.get("markets", [])
            return markets if isinstance(markets, list) else []
        except RuntimeError:
            if len(normalized) == 1:
                return []
            midpoint = len(normalized) // 2
            return self.fetch_with_recursive_split(normalized[:midpoint], start_ts, end_ts) + self.fetch_with_recursive_split(normalized[midpoint:], start_ts, end_ts)

    def fetch(
        self, tickers: Iterable[str], start_ts: int, end_ts: int
    ) -> list[dict[str, Any]]:
        normalized = sorted(set(tickers))
        batches = batch_tickers(normalized, self.batch_size)
        if self.dry_run:
            # request_batch records precisely which cache misses would require requests.
            for batch in batches:
                self.request_batch(batch, start_ts, end_ts)
            return []
        markets: list[dict[str, Any]] = []
        for batch in batches:
            markets.extend(self.fetch_with_recursive_split(batch, start_ts, end_ts))
        return markets
