"""Offline-testable Kalshi settled-market metadata client."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
import hashlib
import random as random_module
import re
import time
from typing import Any, Callable, Mapping

from requests.exceptions import ConnectionError as RequestsConnectionError
from requests.exceptions import ContentDecodingError as RequestsContentDecodingError
from requests.exceptions import ChunkedEncodingError as RequestsChunkedEncodingError
from requests.exceptions import Timeout as RequestsTimeout
from urllib3.exceptions import (
    DecodeError as Urllib3DecodeError,
    IncompleteRead as Urllib3IncompleteRead,
    ProtocolError as Urllib3ProtocolError,
    ReadTimeoutError as Urllib3ReadTimeoutError,
)

from scripts.pipeline_v2.kalshi_metadata_cache import (
    CacheError,
    MetadataCache,
    SensitiveResponseError,
    reject_sensitive_response,
)
from scripts.pipeline_v2.kalshi_metadata_planner import (
    CUTOFF_PATH,
    EndpointSegment,
    cursor_hash,
    request_id,
    segment_params,
)


RETRYABLE_STATUSES = {429, 500, 502, 503, 504}
NONRETRYABLE_STATUSES = {400, 401, 403, 404, 422}
REDIRECT_STATUSES = {300, 301, 302, 303, 304, 305, 306, 307, 308}
KALSHI_PRODUCTION_BASE_URL = "https://external-api.kalshi.com"
RETRYABLE_TRANSPORT_EXCEPTIONS = (
    TimeoutError,
    ConnectionError,
    RequestsTimeout,
    RequestsConnectionError,
    RequestsChunkedEncodingError,
    RequestsContentDecodingError,
    Urllib3ProtocolError,
    Urllib3IncompleteRead,
    Urllib3DecodeError,
    Urllib3ReadTimeoutError,
)


class MetadataClientError(RuntimeError):
    """Raised for an API or response error."""


def sanitize_error_message(error: BaseException | str) -> str:
    """Return a bounded error string with common credential forms redacted."""
    message = str(error)[:500]
    message = re.sub(r"(?i)bearer\s+[^\s,;]+", "Bearer [REDACTED]", message)
    return re.sub(
        r"(?i)(authorization|cookie|api[-_ ]?key|token|secret|signature|"
        r"signed[-_ ]?value|credential|password)\s*[:=]\s*[^\s,;]+",
        r"\1=[REDACTED]",
        message,
    )


def retryable_transport_error(error: BaseException) -> bool:
    """Recognize requests/urllib3 transport failures through wrapper chains."""
    seen: set[int] = set()
    current: BaseException | None = error
    while current is not None and id(current) not in seen:
        seen.add(id(current))
        if isinstance(current, RETRYABLE_TRANSPORT_EXCEPTIONS):
            return True
        next_error = current.__cause__ or current.__context__
        current = next_error if isinstance(next_error, BaseException) else None
    return False


class RequestFailure(MetadataClientError):
    """A nonretryable or exhausted request with complete attempt accounting."""

    def __init__(
        self,
        message: BaseException | str,
        *,
        attempts: int,
        retries: int,
        rate_limits: int,
        last_status: int | None,
        error_type: str | None = None,
    ) -> None:
        self.attempts = attempts
        self.retries = retries
        self.rate_limits = rate_limits
        self.last_status = last_status
        self.error_type = error_type or type(message).__name__
        self.sanitized_message = sanitize_error_message(message)
        super().__init__(self.sanitized_message)


class CursorLoopError(MetadataClientError):
    """Raised when a response repeats an already-consumed cursor."""


class EmptyPageCursorError(MetadataClientError):
    """Raised when an empty page supplies a continuation cursor."""


@dataclass
class RequestCounters:
    logical_pages: int = 0
    actual_http_attempts: int = 0
    successful_requests: int = 0
    cache_hits: int = 0
    retries: int = 0
    rate_limit_responses: int = 0
    elapsed_seconds: float = 0.0


@dataclass
class ChainResult:
    markets: list[dict[str, Any]] = field(default_factory=list)
    fetched_records: list["FetchedMarketRecord"] = field(default_factory=list)
    page_provenance: list[dict[str, Any]] = field(default_factory=list)
    manifest_records: list[dict[str, Any]] = field(default_factory=list)
    complete: bool = False
    intentionally_incomplete_due_to_page_limit: bool = False
    pages_used: int = 0
    market_page_requests: int = 0
    stopped_at_uncached_page_due_to_limit: bool = False
    partition_complete: bool = False
    partition_boundary_reached: bool = False
    next_cursor: str | None = None


@dataclass(frozen=True)
class FetchedMarketRecord:
    payload: dict[str, Any]
    provenance: dict[str, Any]


class KalshiMetadataClient:
    """Paginate metadata endpoints with immutable caching and bounded retries."""

    def __init__(
        self,
        session: Any,
        *,
        base_url: str = KALSHI_PRODUCTION_BASE_URL,
        timeout_seconds: float = 45.0,
        max_retries: int = 5,
        backoff_base_seconds: float = 1.0,
        backoff_cap_seconds: float = 30.0,
        requests_per_second: float = 3.0,
        page_size: int = 1000,
        sleep: Callable[[float], None] = time.sleep,
        random_value: Callable[[], float] = random_module.random,
        monotonic: Callable[[], float] = time.monotonic,
        utcnow: Callable[[], datetime] = lambda: datetime.now(timezone.utc),
    ) -> None:
        self.session = session
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self.max_retries = max_retries
        self.backoff_base_seconds = backoff_base_seconds
        self.backoff_cap_seconds = backoff_cap_seconds
        self.requests_per_second = requests_per_second
        self.page_size = page_size
        self.sleep = sleep
        self.random_value = random_value
        self.monotonic = monotonic
        self.utcnow = utcnow
        self.counters = RequestCounters()
        self._last_request_at: float | None = None
        self._started_at = monotonic()

    @staticmethod
    def _iso(value: datetime) -> str:
        return value.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")

    def _throttle(self) -> None:
        if self.requests_per_second <= 0 or self._last_request_at is None:
            return
        minimum_gap = 1.0 / self.requests_per_second
        remaining = minimum_gap - (self.monotonic() - self._last_request_at)
        if remaining > 0:
            self.sleep(remaining)

    def _backoff(self, retry_number: int) -> None:
        delay = min(
            self.backoff_cap_seconds,
            self.backoff_base_seconds * (2 ** max(0, retry_number - 1)),
        )
        self.sleep(delay * (0.5 + 0.5 * self.random_value()))

    def _request_json(
        self,
        endpoint_path: str,
        params: Mapping[str, Any],
        *,
        expect_markets: bool = False,
    ) -> tuple[dict[str, Any], int, int, int, int, str, str]:
        started = self.utcnow()
        retries = 0
        rate_limits = 0
        status: int | None = None

        def retry_or_raise_transport(exc: BaseException, attempt: int) -> None:
            nonlocal retries
            self._last_request_at = self.monotonic()
            if attempt > self.max_retries:
                raise RequestFailure(
                    f"retry limit reached after {type(exc).__name__}: {exc}",
                    attempts=attempt,
                    retries=retries,
                    rate_limits=rate_limits,
                    last_status=status,
                    error_type=type(exc).__name__,
                ) from exc
            retries += 1
            self.counters.retries += 1
            self._backoff(retries)

        for attempt in range(1, self.max_retries + 2):
            self._throttle()
            self.counters.actual_http_attempts += 1
            try:
                response = self.session.get(
                    f"{self.base_url}{endpoint_path}",
                    params=dict(params),
                    timeout=self.timeout_seconds,
                    allow_redirects=False,
                )
            except Exception as exc:
                if not retryable_transport_error(exc):
                    self._last_request_at = self.monotonic()
                    raise RequestFailure(
                        exc,
                        attempts=attempt,
                        retries=retries,
                        rate_limits=rate_limits,
                        last_status=status,
                        error_type=type(exc).__name__,
                    ) from exc
                retry_or_raise_transport(exc, attempt)
                continue

            self._last_request_at = self.monotonic()
            payload: Any = None
            json_error: Exception | None = None
            try:
                payload = response.json()
            except Exception as exc:
                if retryable_transport_error(exc):
                    retry_or_raise_transport(exc, attempt)
                    continue
                json_error = exc
            else:
                # Security screening precedes every status, retry, and schema branch.
                reject_sensitive_response(payload)

            try:
                status = int(response.status_code)
            except Exception as exc:
                raise RequestFailure(
                    exc,
                    attempts=attempt,
                    retries=retries,
                    rate_limits=rate_limits,
                    last_status=None,
                    error_type=type(exc).__name__,
                ) from exc
            if status in RETRYABLE_STATUSES:
                if status == 429:
                    rate_limits += 1
                    self.counters.rate_limit_responses += 1
                if attempt > self.max_retries:
                    raise RequestFailure(
                        f"retry limit reached after HTTP {status}",
                        attempts=attempt,
                        retries=retries,
                        rate_limits=rate_limits,
                        last_status=status,
                        error_type="HTTPRetryExhausted",
                    )
                retries += 1
                self.counters.retries += 1
                self._backoff(retries)
                continue
            if status in REDIRECT_STATUSES:
                raise RequestFailure(
                    f"redirect HTTP {status} rejected",
                    attempts=attempt,
                    retries=retries,
                    rate_limits=rate_limits,
                    last_status=status,
                    error_type="RedirectError",
                )
            if status in NONRETRYABLE_STATUSES or status >= 400:
                raise RequestFailure(
                    f"nonretryable HTTP {status}",
                    attempts=attempt,
                    retries=retries,
                    rate_limits=rate_limits,
                    last_status=status,
                    error_type="HTTPError",
                )
            if json_error is not None:
                raise RequestFailure(
                    "invalid JSON response",
                    attempts=attempt,
                    retries=retries,
                    rate_limits=rate_limits,
                    last_status=status,
                    error_type=type(json_error).__name__,
                ) from json_error
            if not isinstance(payload, dict):
                raise RequestFailure(
                    "response JSON must be an object",
                    attempts=attempt,
                    retries=retries,
                    rate_limits=rate_limits,
                    last_status=status,
                    error_type="MalformedResponse",
                )
            if expect_markets:
                markets = payload.get("markets")
                if not isinstance(markets, list) or any(
                    not isinstance(item, dict) for item in markets
                ):
                    raise RequestFailure(
                        "response lacks a valid markets list",
                        attempts=attempt,
                        retries=retries,
                        rate_limits=rate_limits,
                        last_status=status,
                        error_type="MalformedResponse",
                    )
            self.counters.successful_requests += 1
            completed = self.utcnow()
            return (
                payload,
                attempt,
                retries,
                rate_limits,
                status,
                self._iso(started),
                self._iso(completed),
            )

        raise AssertionError("unreachable request loop")

    def fetch_cutoff(self) -> dict[str, Any]:
        """Explicitly fetch the live/historical cutoff snapshot."""
        payload, *_ = self._request_json(CUTOFF_PATH, {})
        reject_sensitive_response(payload)
        if "market_settled_ts" not in payload:
            raise MetadataClientError("cutoff response lacks market_settled_ts")
        return payload

    @staticmethod
    def _request_metadata(
        segment: EndpointSegment,
        params: Mapping[str, Any],
        page_number: int,
        request_cursor: str | None,
        cutoff_id: str,
    ) -> dict[str, Any]:
        metadata = {
            "endpoint_path": segment.endpoint_path,
            "request_cursor_hash": cursor_hash(request_cursor),
            "cutoff_id": cutoff_id,
            "params": dict(params),
        }
        if segment.endpoint_tier == "live":
            metadata.update(
                {
                    "endpoint_tier": segment.endpoint_tier,
                    "page_number": page_number,
                    "month": segment.month,
                    "range_start_utc": segment.range_start_utc,
                    "range_end_utc_exclusive": segment.range_end_utc_exclusive,
                }
            )
        return metadata

    def paginate(
        self,
        segment: EndpointSegment,
        cache: MetadataCache,
        *,
        cutoff_id: str,
        run_id: str,
        resume: bool = True,
        dry_run: bool = False,
        limit_pages: int | None = None,
        start_cursor: str | None = None,
        start_page_number: int = 1,
        partition_page_limit: int | None = None,
        mve_filter: str = "exclude",
        manifest_sink: Callable[[Mapping[str, Any]], None] | None = None,
    ) -> ChainResult:
        """Consume one endpoint chain, reusing immutable cached pages."""
        result = ChainResult()
        if start_page_number <= 0:
            raise ValueError("start_page_number must be positive")
        if partition_page_limit is not None and partition_page_limit <= 0:
            raise ValueError("partition_page_limit must be positive")
        cursor: str | None = start_cursor
        seen_response_cursors: set[str] = set()
        page_number = start_page_number
        market_page_requests = 0

        def emit(record: dict[str, Any]) -> None:
            result.manifest_records.append(record)
            if manifest_sink is not None and not dry_run:
                manifest_sink(record)

        while True:
            if partition_page_limit is not None and result.pages_used >= partition_page_limit:
                result.partition_complete = True
                result.partition_boundary_reached = True
                result.next_cursor = cursor
                break
            params = segment_params(
                segment,
                page_size=self.page_size,
                cursor=cursor,
                mve_filter=mve_filter,
            )
            rid = request_id(segment.endpoint_path, params, cutoff_id=cutoff_id)
            request_meta = self._request_metadata(
                segment, params, page_number, cursor, cutoff_id
            )
            path = cache.page_path(segment, cutoff_id, page_number, rid, cursor)
            if (
                limit_pages is not None
                and not path.exists()
                and market_page_requests >= limit_pages
            ):
                result.intentionally_incomplete_due_to_page_limit = True
                result.stopped_at_uncached_page_due_to_limit = True
                break
            self.counters.logical_pages += 1
            result.pages_used += 1
            cache_status = "miss"
            response_payload: dict[str, Any] | None = None
            response_sha = None
            attempts = retries = rate_limits = 0
            http_status: int | None = None
            started = completed = self._iso(self.utcnow())

            if resume and path.exists():
                cached = cache.load_page(path, expected_request=request_meta)
                response_payload = cached["response"]
                response_sha = cached["response_sha256"]
                cache_status = "hit"
                acquisition_status = str(
                    cached.get("metadata", {}).get("acquisition_status") or "fetched"
                )
                self.counters.cache_hits += 1
            elif not resume and path.exists():
                raise CacheError(
                    f"cached page exists and --no-resume cannot overwrite it: {path}"
                )
            elif dry_run:
                emit(
                    self._manifest_record(
                        run_id=run_id,
                        rid=rid,
                        segment=segment,
                        page_number=page_number,
                        request_cursor=cursor,
                        response_cursor=None,
                        started=started,
                        completed=completed,
                        attempts=0,
                        status=None,
                        retries=0,
                        rate_limits=0,
                        returned_rows=0,
                        cache_status="dry_run_cache_miss",
                        path=path,
                        response_sha=None,
                        terminal=False,
                        incomplete=True,
                    )
                )
                result.intentionally_incomplete_due_to_page_limit = bool(limit_pages)
                break
            else:
                acquisition_status = "fetched"
                market_page_requests += 1
                result.market_page_requests += 1
                try:
                    (
                        response_payload,
                        attempts,
                        retries,
                        rate_limits,
                        http_status,
                        started,
                        completed,
                    ) = self._request_json(
                        segment.endpoint_path, params, expect_markets=True
                    )
                    published = cache.publish_page(
                        path,
                        request_metadata=request_meta,
                        response=response_payload,
                        metadata={"acquisition_status": acquisition_status},
                    )
                    response_sha = published["response_sha256"]
                    cache_status = "published"
                except SensitiveResponseError:
                    raise
                except Exception as exc:
                    failure = exc if isinstance(exc, RequestFailure) else None
                    emit(
                        self._manifest_record(
                            run_id=run_id,
                            rid=rid,
                            segment=segment,
                            page_number=page_number,
                            request_cursor=cursor,
                            response_cursor=None,
                            started=started,
                            completed=self._iso(self.utcnow()),
                            attempts=failure.attempts if failure else attempts,
                            status=failure.last_status if failure else http_status,
                            retries=failure.retries if failure else retries,
                            rate_limits=failure.rate_limits if failure else rate_limits,
                            returned_rows=0,
                            cache_status="error",
                            path=path,
                            response_sha=None,
                            terminal=False,
                            incomplete=False,
                            error=exc,
                        )
                    )
                    raise

            if not isinstance(response_payload, dict):
                raise CacheError("cached or fetched response is not an object")
            markets = response_payload.get("markets")
            if not isinstance(markets, list):
                raise MetadataClientError("response lacks a markets list")
            if any(not isinstance(item, dict) for item in markets):
                raise MetadataClientError("markets entries must be objects")
            next_cursor_raw = response_payload.get("cursor")
            next_cursor = str(next_cursor_raw) if next_cursor_raw else None
            if not markets and next_cursor:
                raise EmptyPageCursorError("empty page returned a nonempty cursor")
            if next_cursor and next_cursor in seen_response_cursors:
                raise CursorLoopError(f"repeated response cursor {cursor_hash(next_cursor)}")
            if next_cursor:
                seen_response_cursors.add(next_cursor)

            terminal = next_cursor is None
            partition_boundary_after_page = bool(
                partition_page_limit is not None
                and result.pages_used >= partition_page_limit
                and not terminal
            )
            limited_after_this_page = bool(
                limit_pages is not None
                and (terminal or market_page_requests >= limit_pages)
            )
            result.markets.extend(markets)
            provenance = {
                "endpoint_tier": segment.endpoint_tier,
                "endpoint_path": segment.endpoint_path,
                "immutable_page_path": str(path),
                "page_response_sha256": response_sha,
                "request_id": rid,
                "page_number": page_number,
                "request_cursor_hash": cursor_hash(cursor),
                "response_cursor_hash": cursor_hash(next_cursor),
                "cutoff_id": cutoff_id,
                "month": segment.month,
                "range_start_utc": segment.range_start_utc,
                "range_end_utc_exclusive": segment.range_end_utc_exclusive,
                "page_file_sha256": hashlib.sha256(path.read_bytes()).hexdigest(),
                "row_count": len(markets),
                "terminal_page": terminal,
                "cache_status": cache_status,
                "initial_acquisition_status": acquisition_status,
            }
            result.page_provenance.append(dict(provenance))
            record_provenance = {
                key: value for key, value in provenance.items() if key != "cache_status"
            }
            result.fetched_records.extend(
                FetchedMarketRecord(dict(market), dict(record_provenance))
                for market in markets
            )
            emit(
                self._manifest_record(
                    run_id=run_id,
                    rid=rid,
                    segment=segment,
                    page_number=page_number,
                    request_cursor=cursor,
                    response_cursor=next_cursor,
                    started=started,
                    completed=completed,
                    attempts=attempts,
                    status=http_status,
                    retries=retries,
                    rate_limits=rate_limits,
                    returned_rows=len(markets),
                    cache_status=cache_status,
                    path=path,
                    response_sha=response_sha,
                    terminal=terminal,
                    incomplete=limited_after_this_page,
                    partition_boundary=partition_boundary_after_page,
                )
            )
            if terminal:
                result.next_cursor = None
                if limit_pages is not None:
                    result.intentionally_incomplete_due_to_page_limit = True
                    break
                result.complete = True
                result.partition_complete = True
                break
            if partition_boundary_after_page:
                result.partition_complete = True
                result.partition_boundary_reached = True
                result.next_cursor = next_cursor
                break
            cursor = next_cursor
            page_number += 1

        self.counters.elapsed_seconds = self.monotonic() - self._started_at
        return result

    def _manifest_record(
        self,
        *,
        run_id: str,
        rid: str,
        segment: EndpointSegment,
        page_number: int,
        request_cursor: str | None,
        response_cursor: str | None,
        started: str,
        completed: str,
        attempts: int,
        status: int | None,
        retries: int,
        rate_limits: int,
        returned_rows: int,
        cache_status: str,
        path: Any,
        response_sha: str | None,
        terminal: bool,
        incomplete: bool,
        partition_boundary: bool = False,
        error: Exception | None = None,
    ) -> dict[str, Any]:
        return {
            "schema_version": 1,
            "run_id": run_id,
            "request_id": rid,
            "endpoint_tier": segment.endpoint_tier,
            "endpoint_path": segment.endpoint_path,
            "month": segment.month,
            "page_number": page_number,
            "request_cursor_hash": cursor_hash(request_cursor),
            "response_cursor_hash": cursor_hash(response_cursor),
            "range_start_utc": segment.range_start_utc,
            "range_end_utc_exclusive": segment.range_end_utc_exclusive,
            "page_size": self.page_size,
            "request_started_at_utc": started,
            "request_completed_at_utc": completed,
            "http_attempt_count": attempts,
            "actual_request_count": attempts,
            "http_status": status,
            "retry_count": retries,
            "rate_limit_count": rate_limits,
            "returned_row_count": returned_rows,
            "cache_status": cache_status,
            "page_path": str(path),
            "response_sha256": response_sha,
            "error_type": (
                error.error_type if isinstance(error, RequestFailure)
                else type(error).__name__ if error else None
            ),
            "sanitized_error_message": sanitize_error_message(error) if error else None,
            "terminal_page": terminal,
            "intentionally_incomplete_due_to_page_limit": incomplete,
            "bounded_partition_boundary": partition_boundary,
        }
