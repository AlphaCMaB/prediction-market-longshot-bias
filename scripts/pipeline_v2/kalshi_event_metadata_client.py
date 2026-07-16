"""Secure, injectable client for Kalshi event candidate-evidence metadata."""

from __future__ import annotations

from dataclasses import dataclass
import json
import random
import time
from typing import Any, Callable, Mapping

from scripts.pipeline_v2.kalshi_metadata_cache import SensitiveResponseError, reject_sensitive_response


PRODUCTION_BASE_URL = "https://external-api.kalshi.com"
EVENTS_ENDPOINT = "/trade-api/v2/events"
RETRIABLE_STATUSES = frozenset({429, 500, 502, 503, 504})


class EventMetadataClientError(RuntimeError):
    pass


class EventMetadataResponseError(EventMetadataClientError):
    pass


class EventMetadataRequestFailure(EventMetadataClientError):
    def __init__(self, message: str, *, attempts: int, retries: int,
                 rate_limits: int, status_code: int | None) -> None:
        self.attempts = attempts
        self.retries = retries
        self.rate_limits = rate_limits
        self.status_code = status_code
        super().__init__(message)


@dataclass(frozen=True)
class RequestResult:
    payload: dict[str, Any]
    attempts: int
    retries: int
    rate_limits: int
    status_code: int


class KalshiEventMetadataClient:
    def __init__(
        self,
        session: Any,
        *,
        base_url: str = PRODUCTION_BASE_URL,
        timeout_seconds: float = 45.0,
        max_retries: int = 5,
        backoff_base_seconds: float = 1.0,
        backoff_cap_seconds: float = 30.0,
        sleep: Callable[[float], None] = time.sleep,
        random_value: Callable[[], float] = random.random,
    ) -> None:
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = float(timeout_seconds)
        self.max_retries = int(max_retries)
        self.backoff_base_seconds = float(backoff_base_seconds)
        self.backoff_cap_seconds = float(backoff_cap_seconds)
        self.sleep = sleep
        self.random_value = random_value
        self.network_request_count = 0
        self.retry_count = 0
        self.rate_limit_count = 0

    @property
    def events_url(self) -> str:
        return self.base_url + EVENTS_ENDPOINT

    def _delay(self, retry_number: int) -> None:
        ceiling = min(
            self.backoff_cap_seconds,
            self.backoff_base_seconds * (2 ** max(0, retry_number - 1)),
        )
        self.sleep(ceiling * (0.5 + 0.5 * self.random_value()))

    def request_events(self, params: Mapping[str, Any]) -> RequestResult:
        attempts = retries = rate_limits = 0
        last_status = 0
        while True:
            attempts += 1
            self.network_request_count += 1
            try:
                response = self.session.get(
                    self.events_url,
                    params=dict(params),
                    timeout=self.timeout_seconds,
                    allow_redirects=False,
                )
            except (TimeoutError, ConnectionError, OSError) as exc:
                if retries >= self.max_retries:
                    raise EventMetadataRequestFailure(
                        f"transport failure after {attempts} attempts",
                        attempts=attempts, retries=retries, rate_limits=rate_limits,
                        status_code=None,
                    ) from exc
                retries += 1
                self.retry_count += 1
                self._delay(retries)
                continue

            last_status = int(getattr(response, "status_code", 0))
            try:
                payload = response.json()
            except (ValueError, json.JSONDecodeError) as exc:
                if last_status in RETRIABLE_STATUSES and retries < self.max_retries:
                    retries += 1
                    self.retry_count += 1
                    if last_status == 429:
                        rate_limits += 1
                        self.rate_limit_count += 1
                    self._delay(retries)
                    continue
                raise EventMetadataRequestFailure(
                    f"non-JSON response with HTTP status {last_status}",
                    attempts=attempts, retries=retries, rate_limits=rate_limits,
                    status_code=last_status,
                ) from exc

            reject_sensitive_response(payload)
            if last_status in RETRIABLE_STATUSES:
                if last_status == 429:
                    rate_limits += 1
                    self.rate_limit_count += 1
                if retries >= self.max_retries:
                    raise EventMetadataRequestFailure(
                        f"retry exhaustion after {attempts} attempts; status={last_status}",
                        attempts=attempts, retries=retries, rate_limits=rate_limits,
                        status_code=last_status,
                    )
                retries += 1
                self.retry_count += 1
                self._delay(retries)
                continue
            if last_status < 200 or last_status >= 300:
                raise EventMetadataRequestFailure(
                    f"nonretryable HTTP status {last_status}", attempts=attempts,
                    retries=retries, rate_limits=rate_limits, status_code=last_status,
                )
            if not isinstance(payload, dict):
                raise EventMetadataResponseError("event response must be a JSON object")
            events = payload.get("events")
            if not isinstance(events, list) or any(not isinstance(item, dict) for item in events):
                raise EventMetadataResponseError("event response requires an events object list")
            cursor = payload.get("cursor", "")
            if cursor is not None and not isinstance(cursor, str):
                raise EventMetadataResponseError("event response cursor must be a string")
            return RequestResult(payload, attempts, retries, rate_limits, last_status)
