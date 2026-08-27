from __future__ import annotations

import pytest

from scripts.pipeline_v2.kalshi_event_metadata_client import (
    EVENTS_ENDPOINT,
    MILESTONES_ENDPOINT,
    PRODUCTION_BASE_URL,
    EventMetadataClientError,
    EventMetadataResponseError,
    KalshiEventMetadataClient,
)
from scripts.pipeline_v2.kalshi_metadata_cache import SensitiveResponseError


class Response:
    def __init__(self, status, payload=None, *, error=None, headers=None):
        self.status_code = status
        self.payload = payload
        self.error = error
        self.headers = headers or {}

    def json(self):
        if self.error:
            raise self.error
        return self.payload


class Session:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        item = self.responses.pop(0)
        if isinstance(item, BaseException):
            raise item
        return item


def test_production_endpoint_and_injected_base_url():
    session = Session([Response(200, {"events": [], "cursor": ""})])
    client = KalshiEventMetadataClient(session)
    client.request_events({"tickers": "A"})
    assert session.calls[0][0] == "https://external-api.kalshi.com/trade-api/v2/events"
    assert "api.elections.kalshi.com" not in PRODUCTION_BASE_URL
    assert session.calls[0][1]["allow_redirects"] is False
    custom = Session([Response(200, {"events": [], "cursor": ""})])
    KalshiEventMetadataClient(custom, base_url="https://test.invalid").request_events({})
    assert custom.calls[0][0] == "https://test.invalid" + EVENTS_ENDPOINT


@pytest.mark.parametrize("status", [429, 500, 502, 503, 504])
def test_retriable_statuses(status):
    session = Session([Response(status, {}), Response(200, {"events": [], "cursor": ""})])
    client = KalshiEventMetadataClient(session, sleep=lambda _: None, random_value=lambda: 0)
    result = client.request_events({})
    assert result.attempts == 2
    assert result.retries == 1


@pytest.mark.parametrize("error", [TimeoutError(), ConnectionError()])
def test_transport_retries(error):
    session = Session([error, Response(200, {"events": [], "cursor": ""})])
    client = KalshiEventMetadataClient(session, sleep=lambda _: None)
    assert client.request_events({}).attempts == 2


@pytest.mark.parametrize("status", [400, 401, 403, 404, 422])
def test_ordinary_http_errors_do_not_retry(status):
    session = Session([Response(status, {})])
    with pytest.raises(EventMetadataClientError):
        KalshiEventMetadataClient(session).request_events({})
    assert len(session.calls) == 1


@pytest.mark.parametrize("status", [301, 302, 307, 308])
def test_redirects_are_disabled_rejected_and_not_retried(status):
    secret = "DO_NOT_EXPOSE"
    session = Session([
        Response(status, {}, headers={"Location": f"https://evil.invalid/{secret}"})
    ])
    with pytest.raises(EventMetadataClientError) as caught:
        KalshiEventMetadataClient(session).request_events({"tickers": "A"})
    assert len(session.calls) == 1
    assert session.calls[0][1]["allow_redirects"] is False
    assert secret not in str(caught.value)


def test_malformed_json_and_schema_fail_once():
    session = Session([Response(200, error=ValueError("bad"))])
    with pytest.raises(EventMetadataClientError):
        KalshiEventMetadataClient(session).request_events({})
    assert len(session.calls) == 1


def test_single_event_and_milestone_fallback_contracts():
    session = Session([
        Response(200, {"event": {"event_ticker": "A(B)"}, "markets": []}),
        Response(200, {"milestones": [], "cursor": ""}),
    ])
    client = KalshiEventMetadataClient(session)
    assert client.request_event("A(B)", {"with_nested_markets": "false"}).payload[
        "event"
    ]["event_ticker"] == "A(B)"
    client.request_milestones({"related_event_ticker": "A(B)", "limit": 500})
    assert session.calls[0][0].endswith("/events/A(B)")
    assert session.calls[1][0].endswith(MILESTONES_ENDPOINT)


def test_single_event_ticker_mismatch_and_milestone_schema_fail_closed():
    mismatch = Session([Response(200, {"event": {"event_ticker": "B"}})])
    with pytest.raises(EventMetadataResponseError, match="ticker mismatch"):
        KalshiEventMetadataClient(mismatch).request_event("A", {})
    malformed = Session([Response(200, {"milestones": {}, "cursor": ""})])
    with pytest.raises(EventMetadataResponseError, match="milestones object list"):
        KalshiEventMetadataClient(malformed).request_milestones({})
    session = Session([Response(200, {"unexpected": []})])
    with pytest.raises(EventMetadataResponseError):
        KalshiEventMetadataClient(session).request_events({})
    assert len(session.calls) == 1


@pytest.mark.parametrize("status", [200, 400, 429, 500])
def test_sensitive_body_is_rejected_before_status_or_schema(status):
    secret = "DO_NOT_LEAK"
    session = Session([Response(status, {"unexpected": {"accessToken": secret}})])
    client = KalshiEventMetadataClient(session, sleep=lambda _: None)
    with pytest.raises(SensitiveResponseError) as caught:
        client.request_events({})
    assert len(session.calls) == 1
    assert client.retry_count == 0
    assert secret not in str(caught.value)
