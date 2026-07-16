from __future__ import annotations

import csv
import json
from pathlib import Path

import pytest

from scripts.pipeline_v2.kalshi_metadata_cache import SensitiveResponseError, canonical_json
from scripts.pipeline_v2.pull_kalshi_event_metadata import (
    EVENT_METADATA_FIELDS,
    MILESTONE_FIELDS,
    EventAcquisitionError,
    acquire,
    collect_milestones,
    load_event_tickers,
    make_batches,
    research_event_projection,
    research_milestone_projection,
    request_parameters,
    validate_event_ticker,
)


CONFIG = Path(__file__).parents[1] / "configs" / "pipeline_v2.toml"


class Response:
    status_code = 200
    def __init__(self, payload): self.payload = payload
    def json(self): return self.payload


class Session:
    def __init__(self, payloads):
        self.payloads = list(payloads)
        self.calls = []
    def get(self, url, **kwargs):
        self.calls.append((url, kwargs))
        return Response(self.payloads.pop(0))


def ticker_csv(path: Path, values, header="event_ticker,contract_count,first_open_time"):
    path.write_text(header + "\n" + "".join(f"{value},1,2025-01-01Z\n" for value in values), encoding="utf-8")
    return path


def event(ticker, **extra):
    return {"event_ticker": ticker, "series_ticker": "S", "title": f"Title {ticker}", **extra}


def read_csv(path):
    with Path(path).open(newline="", encoding="utf-8") as handle:
        return list(csv.DictReader(handle))


def test_input_normalization_validation_and_batching(tmp_path):
    path = ticker_csv(tmp_path / "events.csv", ["B", "A", "B"])
    assert load_event_tickers(path) == ["A", "B"]
    assert make_batches([f"E{i:03}" for i in range(201)], 200)[-1] == ("E200",)
    params = request_parameters(["B", "A"], 200)
    assert params == {"tickers": "A,B", "limit": 200, "with_nested_markets": "false", "with_milestones": "true"}
    with pytest.raises(ValueError, match="blank"):
        load_event_tickers(ticker_csv(tmp_path / "blank.csv", [" "]))
    (tmp_path / "bad.csv").write_text("wrong\nA\n", encoding="utf-8")
    with pytest.raises(ValueError, match="event_ticker"):
        load_event_tickers(tmp_path / "bad.csv")


@pytest.mark.parametrize("ticker", [
    " A", "A ", " A ", "\tA", "A\t", "\nA", "A\n", "A\r", "A B", " \t ",
    "A,B", "A?B", "A&B", "A=B", "A#B", "A%B", "A/B", "A\\B",
    "-A", "A-", "A--B", ".A", "A.", "A..B", "lowercase",
])
def test_invalid_ticker_syntax_is_rejected_without_repair(ticker):
    with pytest.raises(ValueError, match="event ticker"):
        validate_event_ticker(ticker)


def test_invalid_ticker_file_causes_zero_network_requests(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", [" A "])
    session = Session([])
    with pytest.raises(ValueError, match="event_ticker"):
        acquire(event_tickers_path=source, output_root=tmp_path / "out",
                config_path=CONFIG, session=session)
    assert session.calls == []
    assert not (tmp_path / "out").exists()


@pytest.mark.parametrize(
    "ticker", ["A", "KXBTC", "KXBTC-26JUL", "KXFED-26JUL-T4.50", "ABC-123-YES"]
)
def test_valid_ticker_grammar(ticker):
    assert validate_event_ticker(ticker) == ticker


def test_duplicate_valid_tickers_still_deduplicate(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["KXBTC-26JUL", "A", "KXBTC-26JUL"])
    assert load_event_tickers(source) == ["A", "KXBTC-26JUL"]


def test_dry_run_and_deterministic_limit_write_nothing(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["C", "A", "B"])
    root = tmp_path / "does-not-exist"
    report = acquire(event_tickers_path=source, output_root=root, config_path=CONFIG,
                     limit_events=2, dry_run=True, session=Session([]))
    assert report["requested_after_limit"] == 2
    assert report["truncated"] is True
    assert not root.exists()


def test_pagination_request_contract_and_normal_outputs(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["B", "A"])
    milestone = {"id": "M1", "title": "Start", "start_date": "2025-08-01T00:00:00Z",
                 "related_event_tickers": ["A"], "primary_event_tickers": ["A"]}
    session = Session([
        {"events": [event("A", strike_date="2025-08-01", milestones=[milestone])], "cursor": "next"},
        {"events": [event("B")], "cursor": ""},
    ])
    report = acquire(event_tickers_path=source, output_root=tmp_path / "out", config_path=CONFIG, session=session)
    assert report["universe_complete"] is True
    assert len(session.calls) == 2
    assert "cursor" not in session.calls[0][1]["params"]
    assert session.calls[1][1]["params"]["cursor"] == "next"
    assert session.calls[0][1]["params"]["with_nested_markets"] == "false"
    assert session.calls[0][1]["params"]["with_milestones"] == "true"
    assert session.calls[0][1]["params"]["limit"] <= 200
    rows = read_csv(tmp_path / "out/event_metadata.csv")
    assert [row["event_ticker"] for row in rows] == ["A", "B"]
    milestones = read_csv(tmp_path / "out/event_milestones.csv")
    assert milestones[0]["association_type"] == "both"
    assert tuple(rows[0]) == EVENT_METADATA_FIELDS
    assert tuple(milestones[0]) == MILESTONE_FIELDS


def test_multiple_batches_and_missing_event_is_explicit(tmp_path):
    values = [f"E{i:03}" for i in range(201)]
    source = ticker_csv(tmp_path / "events.csv", values)
    session = Session([{"events": [event(ticker) for ticker in values[:200]], "cursor": ""},
                       {"events": [], "cursor": ""}])
    report = acquire(event_tickers_path=source, output_root=tmp_path / "out", config_path=CONFIG, session=session)
    assert len(session.calls) == 2
    assert report["missing_event_tickers"] == ["E200"]
    assert report["universe_complete"] is False


def test_unexpected_conflicting_and_equivalent_duplicates(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["A"])
    with pytest.raises(EventAcquisitionError, match="unexpected"):
        acquire(event_tickers_path=source, output_root=tmp_path / "unexpected", config_path=CONFIG,
                session=Session([{"events": [event("B")], "cursor": ""}]))
    duplicate = event("A")
    report = acquire(event_tickers_path=source, output_root=tmp_path / "equivalent", config_path=CONFIG,
                     session=Session([{"events": [duplicate], "cursor": "next"},
                                      {"events": [duplicate], "cursor": ""}]))
    assert report["duplicate_equivalent_event_count"] == 1
    with pytest.raises(EventAcquisitionError, match="conflicting duplicate"):
        acquire(event_tickers_path=source, output_root=tmp_path / "conflict", config_path=CONFIG,
                session=Session([{"events": [event("A", title="one")], "cursor": "next"},
                                 {"events": [event("A", title="two")], "cursor": ""}]))


def test_cursor_loop_and_duplicate_page_rejection(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["A"])
    with pytest.raises(EventAcquisitionError, match="cursor loop"):
        acquire(event_tickers_path=source, output_root=tmp_path / "loop", config_path=CONFIG,
                session=Session([{"events": [event("A")], "cursor": "same"},
                                 {"events": [], "cursor": "same"}]))
    with pytest.raises(EventAcquisitionError, match="duplicate response page"):
        acquire(event_tickers_path=source, output_root=tmp_path / "dup", config_path=CONFIG,
                session=Session([{"events": [], "cursor": "next"},
                                 {"events": [], "cursor": "next"}]))


def test_malformed_event_and_milestone(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["A"])
    with pytest.raises(EventAcquisitionError, match="event_ticker"):
        acquire(event_tickers_path=source, output_root=tmp_path / "bad-event", config_path=CONFIG,
                session=Session([{"events": [{}], "cursor": ""}]))
    with pytest.raises(EventAcquisitionError, match="milestone"):
        acquire(event_tickers_path=source, output_root=tmp_path / "bad-milestone", config_path=CONFIG,
                session=Session([{"events": [event("A", milestones=[{}])], "cursor": ""}]))


def test_outcomes_are_quarantined_and_research_bytes_invariant(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["A"])
    forbidden_a = {"result": "yes", "settlement_value_dollars": 1,
                   "settlement_ts": "2026-01-01Z", "close_time": "x", "expiration_time": "y"}
    forbidden_b = {"result": "no", "settlement_value_dollars": 0,
                   "settlement_ts": "2026-02-01Z", "close_time": "z", "expiration_time": "q"}
    for root, fields in ((tmp_path / "a", forbidden_a), (tmp_path / "b", forbidden_b)):
        acquire(event_tickers_path=source, output_root=root, config_path=CONFIG,
                session=Session([{"events": [event("A", product_metadata={**fields, "safe": 1}, **fields)], "cursor": ""}]))
    for name in ("event_metadata.csv", "event_milestones.csv", "event_source_provenance.jsonl"):
        assert (tmp_path / "a" / name).read_bytes() == (tmp_path / "b" / name).read_bytes()
        text = (tmp_path / "a" / name).read_text()
        assert "settlement_value" not in text and '"result"' not in text


def test_sensitive_response_writes_nothing(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["A"])
    root = tmp_path / "out"
    with pytest.raises(SensitiveResponseError):
        acquire(event_tickers_path=source, output_root=root, config_path=CONFIG,
                session=Session([{"events": [], "unexpected": {"accessToken": "DO_NOT_LEAK"}}]))
    assert not root.exists()


def test_resume_reuses_pages_and_commit_is_idempotent(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["A"])
    root = tmp_path / "out"
    first = Session([{"events": [event("A")], "cursor": ""}])
    original = acquire(event_tickers_path=source, output_root=root, config_path=CONFIG, session=first)
    snapshots = {name: (root / name).read_bytes() for name in
                 ("event_metadata.csv", "event_milestones.csv", "event_source_provenance.jsonl", "event_metadata_report.json")}
    unused = Session([])
    resumed = acquire(event_tickers_path=source, output_root=root, config_path=CONFIG, session=unused)
    assert resumed == original and unused.calls == []
    assert snapshots == {name: (root / name).read_bytes() for name in snapshots}


def test_corrupt_cache_and_missing_manifest_fail(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["A"])
    root = tmp_path / "out"
    acquire(event_tickers_path=source, output_root=root, config_path=CONFIG,
            session=Session([{"events": [event("A")], "cursor": ""}]))
    next((root / "raw_pages").glob("*.json")).write_text("bad", encoding="utf-8")
    next((root / "commits").glob("*.json")).unlink()
    with pytest.raises(EventAcquisitionError, match="corrupt"):
        acquire(event_tickers_path=source, output_root=root, config_path=CONFIG, session=Session([]))
    root2 = tmp_path / "orphaned"
    (root2 / "raw_pages").mkdir(parents=True)
    (root2 / "raw_pages/page_orphan.json").write_text("{}", encoding="utf-8")
    report = acquire(event_tickers_path=source, output_root=root2, config_path=CONFIG,
                     session=Session([{"events": [event("A")], "cursor": ""}]))
    assert report["retrieved_event_count"] == 1


def test_interrupted_run_resumes_from_valid_page(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["A", "B"])
    root = tmp_path / "out"
    class InterruptingSession(Session):
        def get(self, url, **kwargs):
            self.calls.append((url, kwargs))
            if len(self.calls) == 2:
                raise RuntimeError("interrupted")
            return Response({"events": [event("A")], "cursor": "next"})
    with pytest.raises(RuntimeError, match="interrupted"):
        acquire(event_tickers_path=source, output_root=root, config_path=CONFIG,
                session=InterruptingSession([]))
    assert len(list((root / "raw_pages").glob("*.json"))) == 1
    assert not (root / "event_metadata.csv").exists()
    resumed_session = Session([{"events": [event("B")], "cursor": ""}])
    report = acquire(event_tickers_path=source, output_root=root, config_path=CONFIG,
                     session=resumed_session)
    assert report["retrieved_event_count"] == 2
    assert report["cache_hit_count"] == 1
    assert len(resumed_session.calls) == 1
    uninterrupted = tmp_path / "uninterrupted"
    acquire(event_tickers_path=source, output_root=uninterrupted, config_path=CONFIG,
            session=Session([{"events": [event("A")], "cursor": "next"},
                             {"events": [event("B")], "cursor": ""}]))
    for name in ("event_metadata.csv", "event_milestones.csv", "event_source_provenance.jsonl"):
        assert (root / name).read_bytes() == (uninterrupted / name).read_bytes()


def test_limit_report_policy(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["A", "B"])
    truncated = acquire(event_tickers_path=source, output_root=tmp_path / "limited",
                        config_path=CONFIG, limit_events=1,
                        session=Session([{"events": [event("A")], "cursor": ""}]))
    assert truncated["limited_run"] is True and truncated["universe_complete"] is False
    not_truncated = acquire(event_tickers_path=source, output_root=tmp_path / "not-limited",
                            config_path=CONFIG, limit_events=5,
                            session=Session([{"events": [event("A"), event("B")], "cursor": ""}]))
    assert not_truncated["limited_run"] is True and not_truncated["universe_complete"] is True


def test_conflicting_milestone_across_pages_fails(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["A"])
    with pytest.raises(EventAcquisitionError, match="conflicting duplicate milestone"):
        acquire(event_tickers_path=source, output_root=tmp_path / "out", config_path=CONFIG,
                session=Session([
                    {"events": [event("A", milestones=[{"id": "M", "title": "one"}])], "cursor": "next"},
                    {"events": [], "milestones": [{"id": "M", "title": "two"}], "cursor": ""},
                ]))


def test_header_only_input_produces_deterministic_empty_outputs(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", [])
    root = tmp_path / "out"
    report = acquire(event_tickers_path=source, output_root=root, config_path=CONFIG, session=Session([]))
    assert report["retrieved_event_count"] == 0
    assert report["universe_complete"] is True
    assert read_csv(root / "event_metadata.csv") == []
    assert read_csv(root / "event_milestones.csv") == []
    assert (root / "event_source_provenance.jsonl").read_bytes() == b""


def test_missing_committed_manifest_invalidates_transaction(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["A"])
    root = tmp_path / "out"
    acquire(event_tickers_path=source, output_root=root, config_path=CONFIG,
            session=Session([{"events": [event("A")], "cursor": ""}]))
    (root / "manifest.jsonl").unlink()
    with pytest.raises(EventAcquisitionError, match="commit is invalid"):
        acquire(event_tickers_path=source, output_root=root, config_path=CONFIG, session=Session([]))


def test_milestone_association_modes():
    events = [event("A")]
    rows, count = collect_milestones({"milestones": [
        {"id": "M1", "related_event_tickers": ["A"]},
        {"id": "M2", "primary_event_tickers": ["A"]},
        {"id": "M3", "related_event_tickers": ["A"], "primary_event_tickers": ["A"]},
    ]}, events)
    assert count == 3
    assert [row["association_type"] for row in rows] == [
        "related_event_tickers", "primary_event_tickers", "both"
    ]


def test_projection_canonicalizes_mappings_but_preserves_arbitrary_list_order():
    left = {"product_metadata": {"b": 2, "a": 1, "items": [{"z": 1, "a": 2}, 3]}}
    same = {"product_metadata": {"items": [{"a": 2, "z": 1}, 3], "a": 1, "b": 2}}
    reversed_list = {"product_metadata": {"b": 2, "a": 1, "items": [3, {"a": 2, "z": 1}]}}
    assert canonical_json(research_event_projection(left)) == canonical_json(research_event_projection(same))
    assert canonical_json(research_event_projection(left)) != canonical_json(research_event_projection(reversed_list))
    set_left = {"id": "M", "related_event_tickers": ["B", "A"], "source_ids": ["2", "1"]}
    set_right = {"source_ids": ["1", "2"], "related_event_tickers": ["A", "B"], "id": "M"}
    assert canonical_json(research_milestone_projection(set_left)) == canonical_json(
        research_milestone_projection(set_right)
    )


def test_outcome_only_duplicate_event_and_milestone_changes_are_equivalent(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["A"])
    def pages(first, second):
        return [
            {"events": [event("A", product_metadata={"result": first, "safe": [1, 2]},
                              milestones=[{"id": "M", "title": "Start",
                                           "start_date": "2025-01-01Z",
                                           "details": {"settlement_value": first}}])], "cursor": "next"},
            {"events": [event("A", product_metadata={"result": second, "safe": [1, 2]},
                              milestones=[{"id": "M", "title": "Start",
                                           "start_date": "2025-01-01Z",
                                           "details": {"settlement_value": second}}])], "cursor": ""},
        ]
    for root, values in ((tmp_path / "one", ("yes", "no")), (tmp_path / "two", ("x", "y"))):
        report = acquire(event_tickers_path=source, output_root=root, config_path=CONFIG,
                         session=Session(pages(*values)))
        assert report["duplicate_equivalent_event_count"] == 1
    for name in ("event_metadata.csv", "event_milestones.csv", "event_source_provenance.jsonl"):
        assert (tmp_path / "one" / name).read_bytes() == (tmp_path / "two" / name).read_bytes()


@pytest.mark.parametrize("changed", [
    {"title": "Different"},
    {"category": "Different"},
    {"strike_date": "2025-02-01"},
])
def test_genuine_duplicate_event_difference_fails(tmp_path, changed):
    source = ticker_csv(tmp_path / "events.csv", ["A"])
    with pytest.raises(EventAcquisitionError, match="conflicting duplicate events"):
        acquire(event_tickers_path=source, output_root=tmp_path / "out", config_path=CONFIG,
                session=Session([
                    {"events": [event("A")], "cursor": "next"},
                    {"events": [event("A", **changed)], "cursor": ""},
                ]))


def test_genuine_duplicate_milestone_difference_fails(tmp_path):
    source = ticker_csv(tmp_path / "events.csv", ["A"])
    with pytest.raises(EventAcquisitionError, match="conflicting duplicate milestone"):
        acquire(event_tickers_path=source, output_root=tmp_path / "out", config_path=CONFIG,
                session=Session([
                    {"events": [event("A", milestones=[{"id": "M", "title": "One"}])], "cursor": "next"},
                    {"events": [event("A", milestones=[{"id": "M", "title": "Two"}])], "cursor": ""},
                ]))
