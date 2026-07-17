"""Acquire Kalshi event candidate evidence.

Invoke with ``python -m scripts.pipeline_v2.pull_kalshi_event_metadata``.
Importing this module performs no I/O or network activity.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
import json
from pathlib import Path
import re
import sys
import tomllib
from typing import Any, Iterable, Mapping

from scripts.common.io_utils import read_csv_with_header
from scripts.pipeline_v2.kalshi_event_metadata_client import (
    EVENTS_ENDPOINT,
    PRODUCTION_BASE_URL,
    EventMetadataRequestFailure,
    KalshiEventMetadataClient,
)
from scripts.pipeline_v2.kalshi_metadata_cache import (
    SensitiveResponseError,
    append_manifest,
    canonical_json,
    publish_immutable_bytes,
    reject_sensitive_response,
    sha256_json,
)


SCHEMA_VERSION = "1.0"
DEFAULT_CONFIG = Path("configs/pipeline_v2.toml")
MAX_BATCH_SIZE = 200
EVENT_METADATA_FIELDS = (
    "event_ticker", "series_ticker", "title", "sub_title", "category",
    "strike_date", "strike_period", "mutually_exclusive",
    "settlement_sources_json", "product_metadata_json", "last_updated_ts",
)
MILESTONE_FIELDS = (
    "event_ticker", "milestone_id", "milestone_category", "milestone_type",
    "milestone_title", "milestone_start_date", "milestone_end_date",
    "milestone_source_id", "milestone_source_ids_json", "milestone_details_json",
    "milestone_last_updated_ts", "association_type",
)
FORBIDDEN_NORMALIZED_KEYS = frozenset({
    "result", "outcome", "binaryresult", "binaryoutcome", "settlementvalue",
    "settlementvaluedollars", "settlementts", "diagnosticsettlementts",
    "settlementtime", "settlementtimestamp",
    "closetime", "expirationtime", "resolvedyes", "resolvedno",
    "resolvedoutcome", "finalresult", "finaloutcome", "outcomelabel",
    "resultlabel",
})
SET_LIKE_NORMALIZED_KEYS = frozenset({
    "relatedeventtickers", "primaryeventtickers", "milestonesourceids", "sourceids",
})
EVENT_TICKER_PATTERN = re.compile(
    r"^[A-Z0-9]+(?:[.-][A-Z0-9]+)*(?:\([A-Z0-9]+\))*$"
)


class EventAcquisitionError(RuntimeError):
    pass


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _canonical_key(value: Any) -> str:
    return "".join(character for character in str(value).casefold() if character.isalnum())


def load_event_config(path: str | Path) -> dict[str, Any]:
    with Path(path).open("rb") as handle:
        section = tomllib.load(handle).get("kalshi_event_metadata")
    if not isinstance(section, dict):
        raise ValueError("missing [kalshi_event_metadata] configuration")
    values = {
        "page_size": int(section.get("page_size", 200)),
        "batch_size": int(section.get("batch_size", 200)),
        "max_retries": int(section.get("max_retries", 5)),
        "backoff_base_seconds": float(section.get("backoff_base_seconds", 1.0)),
        "backoff_cap_seconds": float(section.get("backoff_cap_seconds", 30.0)),
        "timeout_seconds": float(section.get("timeout_seconds", 45.0)),
    }
    if not 1 <= values["page_size"] <= MAX_BATCH_SIZE:
        raise ValueError("kalshi_event_metadata.page_size must be between 1 and 200")
    if not 1 <= values["batch_size"] <= MAX_BATCH_SIZE:
        raise ValueError("kalshi_event_metadata.batch_size must be between 1 and 200")
    if values["max_retries"] < 0 or min(
        values["backoff_base_seconds"], values["backoff_cap_seconds"], values["timeout_seconds"]
    ) < 0:
        raise ValueError("event metadata retry and timeout settings must be nonnegative")
    return values


def validate_event_ticker(value: Any) -> str:
    if not isinstance(value, str):
        raise ValueError("event ticker must be a string")
    if not value or value.isspace():
        raise ValueError("event ticker must not be blank")
    if EVENT_TICKER_PATTERN.fullmatch(value) is None:
        raise ValueError(
            f"invalid event ticker {value!r}; expected uppercase alphanumeric groups "
            "separated by hyphens or periods, optionally followed by "
            "parenthesized uppercase alphanumeric tokens"
        )
    return value


def load_event_tickers(path: str | Path) -> list[str]:
    rows, header = read_csv_with_header(path)
    if "event_ticker" not in header:
        raise ValueError("event ticker input requires event_ticker column")
    tickers = []
    for number, row in enumerate(rows, 2):
        try:
            ticker = validate_event_ticker(row.get("event_ticker"))
        except ValueError as exc:
            raise ValueError(f"invalid event_ticker at CSV row {number}: {exc}") from exc
        tickers.append(ticker)
    return sorted(set(tickers))


def make_batches(tickers: Iterable[str], batch_size: int) -> list[tuple[str, ...]]:
    ordered = tuple(sorted(set(tickers)))
    if not 1 <= batch_size <= MAX_BATCH_SIZE:
        raise ValueError("batch size must be between 1 and 200")
    return [ordered[index:index + batch_size] for index in range(0, len(ordered), batch_size)]


def request_parameters(batch: Iterable[str], page_size: int, cursor: str | None = None) -> dict[str, Any]:
    if not 1 <= int(page_size) <= MAX_BATCH_SIZE:
        raise ValueError("page size must not exceed 200")
    validated_batch = tuple(validate_event_ticker(ticker) for ticker in sorted(batch))
    joined_tickers = ",".join(validated_batch)
    if tuple(joined_tickers.split(",")) != validated_batch:
        raise ValueError("serialized ticker request does not reconstruct the validated batch")
    params: dict[str, Any] = {
        "tickers": joined_tickers,
        "limit": int(page_size),
        "with_nested_markets": "false",
        "with_milestones": "true",
    }
    if cursor:
        params["cursor"] = cursor
    return params


def request_identity(params: Mapping[str, Any]) -> str:
    return sha256_json({"endpoint_path": EVENTS_ENDPOINT, "params": dict(sorted(params.items()))})


def _cache_request(batch_number: int, page_number: int, params: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "schema_version": SCHEMA_VERSION,
        "endpoint_path": EVENTS_ENDPOINT,
        "batch_number": batch_number,
        "page_number": page_number,
        "params": dict(sorted(params.items())),
        "request_identity": request_identity(params),
    }


def _page_path(root: Path, request_id: str) -> Path:
    return root / "raw_pages" / f"page_{request_id}.json"


def _load_page(path: Path, request: Mapping[str, Any]) -> dict[str, Any] | None:
    if not path.exists():
        return None
    try:
        wrapper = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as exc:
        raise EventAcquisitionError(f"corrupt event cache page: {path}") from exc
    if wrapper.get("request") != request:
        raise EventAcquisitionError(f"event cache request mismatch: {path}")
    response = wrapper.get("response")
    reject_sensitive_response(response)
    if wrapper.get("response_sha256") != sha256_json(response):
        raise EventAcquisitionError(f"event cache response hash mismatch: {path}")
    return wrapper


def _publish_page(path: Path, request: Mapping[str, Any], response: Mapping[str, Any]) -> tuple[str, str]:
    reject_sensitive_response(response)
    response_hash = sha256_json(response)
    wrapper = {"schema_version": SCHEMA_VERSION, "request": request,
               "response": response, "response_sha256": response_hash}
    publish_immutable_bytes(path, canonical_json(wrapper) + b"\n")
    return response_hash, _sha256(path.read_bytes())


def _manifest_bytes(records: Iterable[Mapping[str, Any]]) -> bytes:
    return b"".join(canonical_json(record) + b"\n" for record in records)


def _csv_bytes(rows: Iterable[Mapping[str, Any]], fields: tuple[str, ...]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields, extrasaction="ignore", lineterminator="\n")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def _research_projection(value: Any, *, parent_key: str = "") -> Any:
    """Remove quarantined fields, canonicalize mappings, and preserve list semantics."""
    if isinstance(value, Mapping):
        return {
            str(key): _research_projection(item, parent_key=_canonical_key(key))
            for key, item in sorted(value.items(), key=lambda pair: str(pair[0]))
            if _canonical_key(key) not in FORBIDDEN_NORMALIZED_KEYS
        }
    if isinstance(value, list):
        projected = [_research_projection(item) for item in value]
        if parent_key in SET_LIKE_NORMALIZED_KEYS:
            return sorted(projected, key=canonical_json)
        return projected
    return value


def research_event_projection(event: Mapping[str, Any]) -> dict[str, Any]:
    return _research_projection(event)


def research_milestone_projection(milestone: Mapping[str, Any]) -> dict[str, Any]:
    return _research_projection(milestone)


def _canonical_nested(value: Any, *, set_like: bool = False) -> str:
    def normalize(item: Any) -> Any:
        if isinstance(item, Mapping):
            return {
                str(key): normalize(val)
                for key, val in sorted(item.items(), key=lambda pair: str(pair[0]))
                if _canonical_key(key) not in FORBIDDEN_NORMALIZED_KEYS
            }
        if isinstance(item, list):
            return [normalize(part) for part in item]
        return item
    normalized = normalize(value)
    if set_like and isinstance(normalized, list):
        normalized = sorted(normalized, key=canonical_json)
    return canonical_json(normalized).decode("utf-8")


def normalize_event(event: Mapping[str, Any]) -> dict[str, Any]:
    ticker = str(event.get("event_ticker") or event.get("ticker") or "").strip()
    if not ticker:
        raise EventAcquisitionError("event object lacks event_ticker")
    sources = event.get("settlement_sources", [])
    product = event.get("product_metadata", {})
    return {
        "event_ticker": ticker,
        "series_ticker": event.get("series_ticker", ""),
        "title": event.get("title", ""),
        "sub_title": event.get("sub_title", event.get("subtitle", "")),
        "category": event.get("category", ""),
        "strike_date": event.get("strike_date", ""),
        "strike_period": event.get("strike_period", ""),
        "mutually_exclusive": event.get("mutually_exclusive", ""),
        "settlement_sources_json": _canonical_nested(sources),
        "product_metadata_json": _canonical_nested(product),
        "last_updated_ts": event.get("last_updated_ts", event.get("updated_time", "")),
    }


def _milestone_id(value: Mapping[str, Any]) -> str:
    return str(value.get("milestone_id") or value.get("id") or "").strip()


def collect_milestones(payload: Mapping[str, Any], events: Iterable[Mapping[str, Any]]) -> tuple[list[dict[str, Any]], int]:
    candidates: list[tuple[Mapping[str, Any], str | None]] = []
    for milestone in payload.get("milestones", []) or []:
        if not isinstance(milestone, Mapping):
            raise EventAcquisitionError("malformed milestone object")
        candidates.append((milestone, None))
    for event in events:
        ticker = str(event.get("event_ticker") or event.get("ticker") or "").strip()
        nested = event.get("milestones", []) or []
        if not isinstance(nested, list):
            raise EventAcquisitionError("event milestones must be a list")
        for milestone in nested:
            if not isinstance(milestone, Mapping):
                raise EventAcquisitionError("malformed milestone object")
            candidates.append((milestone, ticker))

    definitions: dict[str, bytes] = {}
    associations: dict[tuple[str, str], set[str]] = {}
    values: dict[str, Mapping[str, Any]] = {}
    for milestone, containing_ticker in candidates:
        identifier = _milestone_id(milestone)
        if not identifier:
            raise EventAcquisitionError("milestone object lacks id")
        encoded = canonical_json(research_milestone_projection(milestone))
        if identifier in definitions and definitions[identifier] != encoded:
            raise EventAcquisitionError(f"conflicting duplicate milestone {identifier!r}")
        definitions[identifier] = encoded
        values[identifier] = milestone
        related = milestone.get("related_event_tickers", []) or []
        primary = milestone.get("primary_event_tickers", []) or []
        if not isinstance(related, list) or not isinstance(primary, list):
            raise EventAcquisitionError("milestone ticker associations must be lists")
        if containing_ticker and not related and not primary:
            related = [containing_ticker]
        for kind, tickers in (("related_event_tickers", related), ("primary_event_tickers", primary)):
            for ticker in tickers:
                normalized = str(ticker).strip()
                if normalized:
                    associations.setdefault((normalized, identifier), set()).add(kind)

    rows = []
    for (ticker, identifier), kinds in sorted(associations.items()):
        milestone = values[identifier]
        association_type = "both" if len(kinds) == 2 else (
            "primary_event_tickers" if "primary_event_tickers" in kinds else "related_event_tickers"
        )
        rows.append({
            "event_ticker": ticker,
            "milestone_id": identifier,
            "milestone_category": milestone.get("category", ""),
            "milestone_type": milestone.get("type", milestone.get("milestone_type", "")),
            "milestone_title": milestone.get("title", ""),
            "milestone_start_date": milestone.get("start_date", ""),
            "milestone_end_date": milestone.get("end_date", ""),
            "milestone_source_id": milestone.get("source_id", ""),
            "milestone_source_ids_json": _canonical_nested(
                milestone.get("source_ids", []), set_like=True
            ),
            "milestone_details_json": _canonical_nested(milestone.get("details", {})),
            "milestone_last_updated_ts": milestone.get("last_updated_ts", milestone.get("updated_time", "")),
            "association_type": association_type,
        })
    return rows, len(definitions)


def _validate_normalized_safety(rows: Iterable[Mapping[str, Any]]) -> None:
    for row in rows:
        forbidden = {_canonical_key(key) for key in row} & FORBIDDEN_NORMALIZED_KEYS
        if forbidden:
            raise EventAcquisitionError(f"forbidden normalized event columns: {sorted(forbidden)}")


def _commit_valid(path: Path, expected_run_id: str | None = None) -> bool:
    try:
        commit = json.loads(path.read_text(encoding="utf-8"))
        if expected_run_id and commit.get("run_id") != expected_run_id:
            return False
        if commit.get("schema_version") != SCHEMA_VERSION:
            return False
        manifest = commit.get("manifest")
        if not isinstance(manifest, dict):
            return False
        manifest_path = path.parent.parent / manifest["relative_path"]
        if not manifest_path.is_file() or _sha256(manifest_path.read_bytes()) != manifest["sha256"]:
            return False
        for item in commit.get("artifacts", []):
            artifact = path.parent.parent / item["relative_path"]
            if not artifact.is_file() or _sha256(artifact.read_bytes()) != item["sha256"]:
                return False
        for page in commit.get("raw_pages", []):
            artifact = path.parent.parent / page["relative_path"]
            if not artifact.is_file() or _sha256(artifact.read_bytes()) != page["page_file_sha256"]:
                return False
        return True
    except (OSError, KeyError, TypeError, json.JSONDecodeError):
        return False


def acquire(
    *, event_tickers_path: str | Path, output_root: str | Path,
    config_path: str | Path = DEFAULT_CONFIG, limit_events: int | None = None,
    dry_run: bool = False, resume: bool = True, force_new_run: bool = False,
    session: Any | None = None,
) -> dict[str, Any]:
    settings = load_event_config(config_path)
    before = load_event_tickers(event_tickers_path)
    if limit_events is not None and limit_events < 0:
        raise ValueError("--limit-events must be nonnegative")
    selected = before[:limit_events] if limit_events is not None else before
    truncated = len(selected) < len(before)
    batches = make_batches(selected, settings["batch_size"])
    run_definition = {
        "schema_version": SCHEMA_VERSION,
        "endpoint_path": EVENTS_ENDPOINT,
        "base_url": PRODUCTION_BASE_URL,
        "requested_event_tickers": selected,
        "requested_event_count_before_limit": len(before),
        "requested_limit": limit_events,
        "settings": settings,
    }
    run_id = sha256_json(run_definition)
    root = Path(output_root)
    commit_path = root / "commits" / f"run_{run_id}.json"
    plan = {"run_id": run_id, "requested_before_limit": len(before),
            "requested_after_limit": len(selected), "batch_count": len(batches),
            "limited_run": limit_events is not None, "truncated": truncated}
    if dry_run:
        print(canonical_json({**plan, "dry_run": True}).decode("utf-8"))
        return plan
    if commit_path.exists() and resume and not force_new_run:
        if not _commit_valid(commit_path, run_id):
            raise EventAcquisitionError("existing event acquisition commit is invalid")
        return json.loads((root / "event_metadata_report.json").read_text(encoding="utf-8"))

    if session is None:
        import requests
        session = requests.Session()
    client = KalshiEventMetadataClient(
        session,
        timeout_seconds=settings["timeout_seconds"], max_retries=settings["max_retries"],
        backoff_base_seconds=settings["backoff_base_seconds"],
        backoff_cap_seconds=settings["backoff_cap_seconds"],
    )
    manifest_path = root / "manifest.jsonl"
    expected_first_pages = [
        _page_path(root, request_identity(request_parameters(batch, settings["page_size"])))
        for batch in batches
    ]
    if (resume and not manifest_path.exists()
            and any(path.exists() for path in expected_first_pages)):
        raise EventAcquisitionError("cached event pages exist without acquisition manifest")

    manifest: list[dict[str, Any]] = []
    source_by_ticker: dict[str, list[dict[str, Any]]] = {}
    event_variants: dict[str, dict[bytes, dict[str, Any]]] = {}
    all_milestones: list[dict[str, Any]] = []
    milestone_definitions: dict[str, bytes] = {}
    page_records: list[dict[str, Any]] = []
    duplicate_equivalent = 0
    cache_hits = 0
    seen_page_hashes: dict[int, set[str]] = {}

    try:
        for batch_number, batch in enumerate(batches, 1):
            cursor: str | None = None
            seen_cursors: set[str] = set()
            page_number = 1
            while True:
                if cursor and cursor in seen_cursors:
                    raise EventAcquisitionError(f"cursor loop in batch {batch_number}")
                if cursor:
                    seen_cursors.add(cursor)
                params = request_parameters(batch, settings["page_size"], cursor)
                request = _cache_request(batch_number, page_number, params)
                page_path = _page_path(root, request["request_identity"])
                wrapper = _load_page(page_path, request) if resume else None
                if wrapper is not None:
                    cache_hits += 1
                    response = wrapper["response"]
                    response_hash = wrapper["response_sha256"]
                    page_file_hash = _sha256(page_path.read_bytes())
                    attempts = retries = rate_limits = 0
                    cache_status = "hit"
                else:
                    try:
                        result = client.request_events(params)
                    except SensitiveResponseError:
                        raise
                    except EventMetadataRequestFailure as exc:
                        append_manifest(manifest_path, {
                            "schema_version": SCHEMA_VERSION, "run_id": run_id,
                            "request_identity": request["request_identity"],
                            "batch_number": batch_number, "page_number": page_number,
                            "endpoint_path": EVENTS_ENDPOINT,
                            "http_attempt_count": exc.attempts,
                            "retry_count": exc.retries,
                            "rate_limit_count": exc.rate_limits,
                            "http_status": exc.status_code,
                            "error_type": type(exc).__name__,
                            "error_message": str(exc), "cache_status": "error",
                        })
                        raise
                    response = result.payload
                    response_hash, page_file_hash = _publish_page(page_path, request, response)
                    attempts, retries, rate_limits = result.attempts, result.retries, result.rate_limits
                    cache_status = "fetched"
                if response_hash in seen_page_hashes.setdefault(batch_number, set()):
                    raise EventAcquisitionError(f"duplicate response page in batch {batch_number}")
                seen_page_hashes[batch_number].add(response_hash)
                events = response["events"]
                response_cursor = str(response.get("cursor") or "")
                provenance = {
                    "request_identity": request["request_identity"],
                    "batch_number": batch_number,
                    "page_number": page_number,
                    "endpoint_path": EVENTS_ENDPOINT,
                    "request_cursor_hash": sha256_json(cursor or ""),
                    "response_cursor_hash": sha256_json(response_cursor),
                    "raw_page_relative_path": page_path.relative_to(root).as_posix(),
                }
                page_entry = {**provenance, "row_count": len(events),
                              "response_sha256": response_hash,
                              "page_file_sha256": page_file_hash,
                              "cache_status": cache_status,
                              "terminal_page": not bool(response_cursor)}
                page_records.append(page_entry)
                manifest_record = {"schema_version": SCHEMA_VERSION, "run_id": run_id,
                                   **page_entry, "http_attempt_count": attempts,
                                   "retry_count": retries, "rate_limit_count": rate_limits}
                manifest.append(manifest_record)
                append_manifest(manifest_path, manifest_record)
                for event in events:
                    ticker = str(event.get("event_ticker") or event.get("ticker") or "").strip()
                    if not ticker:
                        raise EventAcquisitionError("malformed event object without event_ticker")
                    if ticker not in selected:
                        raise EventAcquisitionError(f"unexpected event ticker {ticker!r}")
                    encoded = canonical_json(research_event_projection(event))
                    variants = event_variants.setdefault(ticker, {})
                    if encoded in variants:
                        duplicate_equivalent += 1
                    variants[encoded] = dict(event)
                    source_by_ticker.setdefault(ticker, []).append(dict(provenance))
                milestone_rows, _ = collect_milestones(response, events)
                raw_milestones = list(response.get("milestones", []) or [])
                for returned_event in events:
                    raw_milestones.extend(returned_event.get("milestones", []) or [])
                for milestone in raw_milestones:
                    identifier = _milestone_id(milestone)
                    definition_key = f"definition\0{identifier}"
                    encoded_definition = canonical_json(research_milestone_projection(milestone))
                    if (definition_key in milestone_definitions
                            and milestone_definitions[definition_key] != encoded_definition):
                        raise EventAcquisitionError(
                            f"conflicting duplicate milestone {identifier!r}"
                        )
                    milestone_definitions[definition_key] = encoded_definition
                for row in milestone_rows:
                    if row["event_ticker"] in selected:
                        key = f'{row["event_ticker"]}\0{row["milestone_id"]}'
                        encoded = canonical_json(row)
                        if key in milestone_definitions and milestone_definitions[key] != encoded:
                            raise EventAcquisitionError(f"conflicting milestone association {key!r}")
                        milestone_definitions[key] = encoded
                        all_milestones.append(row)
                if not response_cursor:
                    break
                cursor = response_cursor
                page_number += 1
    except SensitiveResponseError:
        raise
    except Exception:
        raise

    conflicts = [ticker for ticker, variants in event_variants.items() if len(variants) > 1]
    if conflicts:
        raise EventAcquisitionError(f"conflicting duplicate events: {', '.join(sorted(conflicts))}")
    retrieved = sorted(event_variants)
    missing = sorted(set(selected) - set(retrieved))
    event_rows = [normalize_event(next(iter(event_variants[ticker].values()))) for ticker in retrieved]
    _validate_normalized_safety(event_rows)
    milestone_rows = sorted(
        {canonical_json(row): row for row in all_milestones}.values(),
        key=lambda row: (row["event_ticker"], row["milestone_id"], row["association_type"]),
    )
    _validate_normalized_safety(milestone_rows)
    provenance_rows = []
    for row in event_rows:
        ticker = row["event_ticker"]
        sources = sorted(
            {canonical_json(source): source for source in source_by_ticker[ticker]}.values(),
            key=canonical_json,
        )
        first = sources[0]
        provenance_rows.append({
            "event_ticker": ticker, "acquisition_run_id": run_id,
            "request_identity": first["request_identity"],
            "batch_number": first["batch_number"], "page_number": first["page_number"],
            "endpoint_path": EVENTS_ENDPOINT,
            "request_cursor_hash": first["request_cursor_hash"],
            "response_cursor_hash": first["response_cursor_hash"],
            "raw_page_relative_path": first["raw_page_relative_path"],
            "event_metadata_sha256": sha256_json(row),
            "source_associations": sources,
        })

    contents = {
        "event_metadata.csv": _csv_bytes(event_rows, EVENT_METADATA_FIELDS),
        "event_milestones.csv": _csv_bytes(milestone_rows, MILESTONE_FIELDS),
        "event_source_provenance.jsonl": _manifest_bytes(provenance_rows),
    }
    report = {
        "schema_version": SCHEMA_VERSION,
        "requested_event_count_before_limit": len(before),
        "requested_event_count_after_limit": len(selected),
        "retrieved_event_count": len(retrieved),
        "missing_event_count": len(missing),
        "missing_event_tickers": missing,
        "duplicate_equivalent_event_count": duplicate_equivalent,
        "conflicting_duplicate_event_count": 0,
        "milestone_count": len({row["milestone_id"] for row in milestone_rows}),
        "event_milestone_association_count": len(milestone_rows),
        "limited_run": limit_events is not None,
        "requested_limit": limit_events,
        "universe_complete": not truncated and not missing,
        "request_count": len(page_records),
        "network_request_count": client.network_request_count,
        "cache_hit_count": cache_hits,
        "retry_count": client.retry_count,
        "acquisition_commit_identity": run_id,
        "output_hashes": {name: _sha256(content) for name, content in contents.items()},
    }
    contents["event_metadata_report.json"] = canonical_json(report) + b"\n"
    artifacts = [
        {"relative_path": name, "sha256": _sha256(content)}
        for name, content in sorted(contents.items())
    ]
    if not manifest_path.exists():
        publish_immutable_bytes(manifest_path, b"")
    commit = {"schema_version": SCHEMA_VERSION, "run_id": run_id,
              "run_definition": run_definition, "artifacts": artifacts,
              "manifest": {"relative_path": "manifest.jsonl",
                           "sha256": _sha256(manifest_path.read_bytes())},
              "raw_pages": [
                  {**item, "relative_path": item["raw_page_relative_path"]}
                  for item in sorted(page_records, key=lambda item: (item["batch_number"], item["page_number"]))
              ]}
    # Publish the operational manifest and normalized artifacts before the commit visibility boundary.
    for name, content in contents.items():
        publish_immutable_bytes(root / name, content)
    publish_immutable_bytes(commit_path, canonical_json(commit) + b"\n")
    if not _commit_valid(commit_path, run_id):
        raise EventAcquisitionError("published event acquisition commit failed validation")
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Acquire outcome-quarantined Kalshi event candidate evidence")
    parser.add_argument("--event-tickers", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--config", default=str(DEFAULT_CONFIG))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit-events", type=int)
    parser.add_argument("--resume", action=argparse.BooleanOptionalAction, default=True)
    parser.add_argument("--force-new-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        report = acquire(
            event_tickers_path=args.event_tickers, output_root=args.output_root,
            config_path=args.config, limit_events=args.limit_events,
            dry_run=args.dry_run, resume=args.resume, force_new_run=args.force_new_run,
        )
        if not args.dry_run:
            print(canonical_json(report).decode("utf-8"))
        return 0 if args.dry_run or report.get("universe_complete", False) else 3
    except Exception as exc:
        print(f"event metadata acquisition failed: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
