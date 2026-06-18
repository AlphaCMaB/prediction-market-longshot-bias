from __future__ import annotations

import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

import requests

from .schema import MarketSnapshot
from .utils import normalize_probability, parse_dt, standardize_category, as_float


class FetchError(RuntimeError):
    pass


def request_json(url: str, params: Optional[dict[str, Any]] = None, timeout: int = 30) -> Any:
    headers = {"User-Agent": "prediction-market-week1/0.1"}
    r = requests.get(url, params=params, timeout=timeout, headers=headers)
    r.raise_for_status()
    return r.json()


def write_jsonl(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for rec in records:
            f.write(json.dumps(rec, default=str) + "\n")


def choose_price_point(history: list[dict[str, Any]], target: datetime) -> tuple[Optional[float], Optional[datetime], str]:
    candidates = []
    for p in history:
        t = parse_dt(p.get("t") or p.get("timestamp") or p.get("time") or p.get("end_ts"))
        if t is None or t > target:
            continue
        prob = normalize_probability(p.get("p") or p.get("price") or p.get("close") or p.get("yes_close"))
        if prob is not None:
            candidates.append((t, prob))
    if not candidates:
        return None, None, "missing_history_price"
    # Closest observation at or before target.
    t, prob = max(candidates, key=lambda x: x[0])
    return prob, t, "history_price_closest_before_target"


def fetch_polymarket(start: datetime, end: datetime, snapshot_hours: int, raw_dir: Path, max_pages: int = 50) -> tuple[list[MarketSnapshot], list[str]]:
    """Fetch Polymarket binary resolved markets.

    Primary source: public CLOB markets endpoint. Historical price source: CLOB
    prices-history endpoint for the selected YES token. The function is conservative:
    it only keeps two-token markets with an explicit winner flag.
    """
    errors: list[str] = []
    markets_raw: list[dict[str, Any]] = []
    rows: list[MarketSnapshot] = []
    base = "https://clob.polymarket.com/markets"
    next_cursor: Optional[str] = None
    try:
        for _ in range(max_pages):
            params = {"next_cursor": next_cursor} if next_cursor else None
            payload = request_json(base, params=params)
            data = payload.get("data", payload if isinstance(payload, list) else [])
            if not isinstance(data, list):
                break
            markets_raw.extend(data)
            next_cursor = payload.get("next_cursor") or payload.get("nextCursor") if isinstance(payload, dict) else None
            if not next_cursor or next_cursor == "LTE=":
                break
            time.sleep(0.1)
    except Exception as e:
        errors.append(f"polymarket market fetch failed: {e}")
        write_jsonl(raw_dir / "polymarket_markets_raw.jsonl", markets_raw)
        return rows, errors

    write_jsonl(raw_dir / "polymarket_markets_raw.jsonl", markets_raw)

    for m in markets_raw:
        try:
            if not m.get("closed"):
                continue
            tokens = m.get("tokens") or []
            if len(tokens) != 2:
                continue
            resolution_time = parse_dt(m.get("end_date_iso") or m.get("game_start_time"))
            if resolution_time is None or not (start <= resolution_time <= end):
                continue
            # Use first token as the YES-side analysis token for simple binary rows.
            yes_token = tokens[0]
            token_id = str(yes_token.get("token_id") or "")
            if not token_id:
                continue
            if yes_token.get("winner") is None:
                continue
            outcome = 1 if bool(yes_token.get("winner")) else 0
            target = resolution_time - timedelta(hours=snapshot_hours)
            start_ts = int((target - timedelta(days=3)).timestamp())
            end_ts = int(target.timestamp())
            p_hat = None
            price_time = None
            price_source = "missing_history_price"
            try:
                hist = request_json(
                    "https://clob.polymarket.com/prices-history",
                    params={"market": token_id, "startTs": start_ts, "endTs": end_ts, "fidelity": 60},
                )
                history = hist.get("history", hist if isinstance(hist, list) else [])
                p_hat, price_time, price_source = choose_price_point(history, target)
            except Exception as e:
                errors.append(f"polymarket price fetch failed for {m.get('market_slug')}: {e}")
            # Fallback to current token price only if history missing; QC will reveal the source.
            if p_hat is None:
                p_hat = normalize_probability(yes_token.get("price"))
                price_time = target
                price_source = "fallback_token_price_not_historical"
            if p_hat is None or price_time is None:
                continue
            raw_cat = ",".join(m.get("tags", []) or [])
            title = m.get("question") or m.get("description") or str(m.get("market_slug"))
            rows.append(MarketSnapshot(
                venue="polymarket",
                market_id=str(m.get("market_slug") or m.get("condition_id") or token_id),
                token_id=token_id,
                title=title,
                category_raw=raw_cat,
                category=standardize_category(raw_cat, title),
                resolution_time=resolution_time,
                target_price_time=target,
                price_time=price_time,
                snapshot_hours_before_resolution=snapshot_hours,
                p_hat=float(p_hat),
                outcome=outcome,
                volume=as_float(m.get("volume")),
                liquidity=as_float(m.get("liquidity")),
                price_source=price_source,
                raw_url="https://clob.polymarket.com/markets",
            ))
        except Exception as e:
            errors.append(f"polymarket parse failed: {e}")
    return rows, errors


def fetch_kalshi(start: datetime, end: datetime, snapshot_hours: int, raw_dir: Path, max_pages: int = 100) -> tuple[list[MarketSnapshot], list[str]]:
    """Fetch Kalshi settled binary markets.

    The base URL is configurable by editing `base`. Some environments block
    Kalshi API calls; failures are reported in the quality report instead of
    fabricating data.
    """
    errors: list[str] = []
    rows: list[MarketSnapshot] = []
    markets_raw: list[dict[str, Any]] = []
    base = "https://api.kalshi.com/trade-api/v2"
    cursor: Optional[str] = None
    try:
        for _ in range(max_pages):
            params = {"status": "settled", "limit": 100}
            if cursor:
                params["cursor"] = cursor
            payload = request_json(f"{base}/markets", params=params)
            data = payload.get("markets", [])
            markets_raw.extend(data)
            cursor = payload.get("cursor")
            if not cursor:
                break
            time.sleep(0.1)
    except Exception as e:
        errors.append(f"kalshi market fetch failed: {e}")
        write_jsonl(raw_dir / "kalshi_markets_raw.jsonl", markets_raw)
        return rows, errors

    write_jsonl(raw_dir / "kalshi_markets_raw.jsonl", markets_raw)

    for m in markets_raw:
        try:
            resolution_time = parse_dt(m.get("settlement_time") or m.get("settlement_ts") or m.get("close_time") or m.get("expiration_time"))
            if resolution_time is None or not (start <= resolution_time <= end):
                continue
            settlement = m.get("settlement_value") or m.get("result") or m.get("status")
            if isinstance(settlement, str):
                s = settlement.lower()
                if s in {"yes", "y", "1", "true"}:
                    outcome = 1
                elif s in {"no", "n", "0", "false"}:
                    outcome = 0
                else:
                    continue
            else:
                p = normalize_probability(settlement)
                if p is None:
                    continue
                outcome = 1 if p >= 0.5 else 0
            ticker = str(m.get("ticker") or m.get("market_ticker") or "")
            if not ticker:
                continue
            target = resolution_time - timedelta(hours=snapshot_hours)
            start_ts = int((target - timedelta(days=3)).timestamp())
            end_ts = int(target.timestamp())
            p_hat = None
            price_time = None
            price_source = "missing_candlestick"
            try:
                candles = request_json(
                    f"{base}/markets/{ticker}/candlesticks",
                    params={"start_ts": start_ts, "end_ts": end_ts, "period_interval": 60},
                )
                series = candles.get("candlesticks") or candles.get("candles") or []
                # Try midpoint from bid/ask fields first, then close price.
                normalized = []
                for c in series:
                    t = c.get("end_period_ts") or c.get("time") or c.get("timestamp")
                    bid = normalize_probability(c.get("yes_bid") or c.get("bid"))
                    ask = normalize_probability(c.get("yes_ask") or c.get("ask"))
                    close = normalize_probability(c.get("yes_close") or c.get("close") or c.get("price"))
                    price = (bid + ask) / 2 if bid is not None and ask is not None else close
                    if price is not None:
                        normalized.append({"t": t, "p": price})
                p_hat, price_time, price_source = choose_price_point(normalized, target)
            except Exception as e:
                errors.append(f"kalshi price fetch failed for {ticker}: {e}")
            if p_hat is None:
                bid = normalize_probability(m.get("yes_bid"))
                ask = normalize_probability(m.get("yes_ask"))
                if bid is not None and ask is not None:
                    p_hat = (bid + ask) / 2
                    price_time = target
                    price_source = "fallback_market_bid_ask_not_historical"
            if p_hat is None or price_time is None:
                continue
            title = m.get("title") or m.get("subtitle") or ticker
            raw_cat = m.get("category") or m.get("series_ticker") or m.get("event_ticker")
            rows.append(MarketSnapshot(
                venue="kalshi",
                market_id=ticker,
                token_id=None,
                title=str(title),
                category_raw=str(raw_cat) if raw_cat is not None else None,
                category=standardize_category(str(raw_cat or ""), str(title)),
                resolution_time=resolution_time,
                target_price_time=target,
                price_time=price_time,
                snapshot_hours_before_resolution=snapshot_hours,
                p_hat=float(p_hat),
                outcome=outcome,
                volume=as_float(m.get("volume")),
                liquidity=as_float(m.get("liquidity")),
                price_source=price_source,
                raw_url=f"{base}/markets/{ticker}",
            ))
        except Exception as e:
            errors.append(f"kalshi parse failed: {e}")
    return rows, errors
