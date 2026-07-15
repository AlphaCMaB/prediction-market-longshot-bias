"""Prepare the outcome-quarantined Kalshi universe from a committed acquisition.

Invoke as ``python -m scripts.pipeline_v2.prepare_kalshi_market_universe``.
"""

from __future__ import annotations

import argparse
import csv
from collections import defaultdict
import hashlib
import io
import json
from pathlib import Path
import sys
from typing import Any, Iterable, Mapping

from scripts.common.time_utils import format_iso_utc, parse_iso_utc
from scripts.pipeline_v2.kalshi_metadata_cache import canonical_json, publish_immutable_bytes
from scripts.pipeline_v2.kalshi_metadata_consolidation import ConsolidationConflict, payload_sha256
from scripts.pipeline_v2.pull_kalshi_settled_metadata import _valid_commit
from scripts.pipeline_v2.study_rules import StudyRules, load_study_rules


SCHEMA_VERSION = "1.0"
DEFAULT_CONFIG = Path("configs/pipeline_v2.toml")
METADATA_FIELDS = (
    "ticker", "event_ticker", "family_id", "family_id_source", "title", "subtitle", "yes_sub_title", "no_sub_title",
    "rules_primary", "rules_secondary", "market_type", "open_time", "close_time",
    "expiration_time", "expected_expiration_time", "occurrence_datetime", "updated_time",
    "strike_type", "floor_strike", "cap_strike", "custom_strike", "can_close_early",
    "early_close_condition", "diagnostic_settlement_ts",
)
OUTCOME_FIELDS = (
    "ticker", "result", "settlement_value_dollars", "settlement_ts",
    "binary_outcome_status",
)
EVENT_FIELDS = (
    "event_ticker", "contract_count", "first_open_time",
)


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _read_jsonl(path: Path) -> list[dict[str, Any]]:
    rows = []
    for number, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
        if not line.strip():
            continue
        value = json.loads(line)
        if not isinstance(value, dict):
            raise ValueError(f"{path}:{number} is not a JSON object")
        rows.append(value)
    return rows


def find_valid_acquisition_commit(raw_root: Path) -> tuple[Path, dict[str, Any]]:
    commits = []
    for path in sorted((raw_root / "run_commits").glob("run_*.json")):
        try:
            record = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if isinstance(record, dict) and _valid_commit(path, record):
            commits.append((path, record))
    if not commits:
        raise ValueError("no completed valid settled-metadata acquisition commit found")
    if len(commits) != 1:
        raise ValueError("multiple valid acquisition commits found; use an unambiguous raw root")
    return commits[0]


def _artifact(commit: Mapping[str, Any], month: str, kind: str) -> Mapping[str, Any]:
    matches = [
        item for item in commit.get("artifacts", ())
        if item.get("month") == month and item.get("kind") == kind
    ]
    if len(matches) != 1:
        raise ValueError(f"commit does not reference exactly one {kind} for {month}")
    item = matches[0]
    path = Path(item["path"])
    if not path.is_file() or _sha256(path.read_bytes()) != item.get("sha256"):
        raise ValueError(f"committed {kind} is missing or corrupt for {month}")
    return item


def load_committed_records(commit: Mapping[str, Any]) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    pairs = []
    for month in commit["selected_months"]:
        monthly = _artifact(commit, month, "monthly_consolidation")
        provenance = _artifact(commit, month, "record_provenance")
        records = _read_jsonl(Path(monthly["path"]))
        provenance_rows = _read_jsonl(Path(provenance["path"]))
        by_key = {
            (str(row.get("ticker") or ""), str(row.get("selected_payload_sha256") or "")): row
            for row in provenance_rows
        }
        if len(by_key) != len(provenance_rows):
            raise ValueError(f"duplicate committed provenance identity for {month}")
        for record in records:
            key = (str(record.get("ticker") or ""), payload_sha256(record))
            source = by_key.pop(key, None)
            if source is None or not source.get("source_associations"):
                raise ValueError(f"market {key[0]!r} lacks committed record provenance")
            source = dict(source)
            source["source_associations"] = [
                {"acquisition_run_id": commit["run_id"], **dict(association)}
                for association in source["source_associations"]
            ]
            pairs.append((record, source))
        if by_key:
            raise ValueError(f"orphaned committed record provenance for {month}")
    return pairs


def _source_identity(source: Mapping[str, Any]) -> dict[str, Any]:
    """Project validated acquisition provenance into research-stable identity."""
    identity = {
        key: value for key, value in source.items()
        if key not in {
            "payload_sha256", "selected_payload", "page_file_sha256",
            "page_response_sha256", "response_sha256",
        }
    }
    page_path = identity.get("immutable_page_path")
    month = str(identity.get("month") or "")
    if page_path and month:
        parts = Path(str(page_path)).parts
        if month in parts:
            identity["immutable_page_path"] = Path(*parts[parts.index(month):]).as_posix()
    return identity


def deduplicate_markets(
    pairs: Iterable[tuple[Mapping[str, Any], Mapping[str, Any]]]
) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    grouped: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = defaultdict(list)
    for payload, provenance in pairs:
        ticker = str(payload.get("ticker") or "").strip()
        if not ticker:
            raise ValueError("committed market record lacks ticker")
        grouped[ticker].append((dict(payload), dict(provenance)))
    selected = []
    for ticker in sorted(grouped):
        variants: dict[str, dict[str, Any]] = {}
        for payload, provenance in grouped[ticker]:
            digest = payload_sha256(payload)
            variant = variants.setdefault(digest, {"payload": payload, "sources": []})
            variant["sources"].extend(
                _source_identity(source) for source in provenance.get("source_associations", ())
            )
        if len(variants) == 1:
            chosen_hash = next(iter(variants))
        else:
            ordered = sorted(
                ((
                    parse_iso_utc(item["payload"].get("updated_time")), digest
                )
                    for digest, item in variants.items()
                ),
                key=lambda item: (item[0] is not None, item[0] or parse_iso_utc("1970-01-01Z"), item[1]),
            )
            if any(timestamp is None for timestamp, _ in ordered) or ordered[-1][0] == ordered[-2][0]:
                raise ConsolidationConflict(f"ticker {ticker!r} has unresolved cross-month payload conflict")
            chosen_hash = ordered[-1][1]
        sources = sorted(
            {canonical_json(source): source for item in variants.values() for source in item["sources"]}.values(),
            key=canonical_json,
        )
        selected.append(
            (
                variants[chosen_hash]["payload"],
                {
                    "ticker": ticker,
                    "research_metadata_sha256": _research_metadata_sha256(
                        variants[chosen_hash]["payload"]
                    ),
                    "source_associations": sources,
                },
            )
        )
    return selected


def _csv_bytes(rows: list[dict[str, Any]], fields: tuple[str, ...]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(stream, fieldnames=fields, lineterminator="\n", extrasaction="ignore")
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def _jsonl_bytes(rows: list[dict[str, Any]]) -> bytes:
    return b"".join(canonical_json(row) + b"\n" for row in rows)


def _metadata_row(record: Mapping[str, Any]) -> dict[str, Any]:
    row = {field: record.get(field, "") for field in METADATA_FIELDS if field != "diagnostic_settlement_ts"}
    event_ticker = str(record.get("event_ticker") or "").strip()
    row["family_id"] = event_ticker
    row["family_id_source"] = "kalshi_event_ticker" if event_ticker else ""
    row["diagnostic_settlement_ts"] = record.get("settlement_ts", "")
    return row


def _research_metadata_sha256(record: Mapping[str, Any]) -> str:
    metadata = _metadata_row(record)
    metadata.pop("diagnostic_settlement_ts", None)
    return _sha256(canonical_json(metadata))


def _outcome_row(record: Mapping[str, Any], rules: StudyRules) -> dict[str, Any]:
    raw = record.get("result")
    normalized = str(raw).strip().casefold() if raw is not None else ""
    if not normalized:
        status = "missing_result"
    elif normalized == "yes" and normalized in rules.study_window.allowed_binary_results:
        status = "valid_binary_yes"
    elif normalized == "no" and normalized in rules.study_window.allowed_binary_results:
        status = "valid_binary_no"
    else:
        status = "invalid_binary_result"
    return {
        "ticker": record.get("ticker", ""),
        "result": "" if raw is None else raw,
        "settlement_value_dollars": record.get("settlement_value_dollars", ""),
        "settlement_ts": record.get("settlement_ts", ""),
        "binary_outcome_status": status,
    }


def _event_rows(metadata: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[str, list[dict[str, Any]]] = defaultdict(list)
    for row in metadata:
        event = str(row.get("event_ticker") or "").strip()
        if event:
            grouped[event].append(row)
    output = []
    for event in sorted(grouped):
        rows = grouped[event]
        opens = [parse_iso_utc(row.get("open_time")) for row in rows]
        valid_opens = [item for item in opens if item is not None]
        output.append(
            {
                "event_ticker": event,
                "contract_count": len(rows),
                "first_open_time": format_iso_utc(min(valid_opens)) if valid_opens else "",
            }
        )
    return output


def prepare_universe(
    pairs: Iterable[tuple[Mapping[str, Any], Mapping[str, Any]]],
    rules: StudyRules,
    *,
    limit: int | None = None,
) -> tuple[dict[str, bytes], dict[str, Any]]:
    selected = deduplicate_markets(pairs)
    pre_limit_contract_count = len(selected)
    pre_limit_unique_ticker_count = len({record.get("ticker") for record, _ in selected})
    pre_limit_unique_event_ticker_count = len({
        str(record.get("event_ticker") or "").strip() for record, _ in selected
        if str(record.get("event_ticker") or "").strip()
    })
    if limit is not None:
        if limit < 0:
            raise ValueError("--limit must be nonnegative")
        selected = selected[:limit]
    metadata = [_metadata_row(record) for record, _ in selected]
    outcomes = [_outcome_row(record, rules) for record, _ in selected]
    provenance = [source for _, source in selected]
    events = _event_rows(metadata)
    contents = {
        "market_metadata.csv": _csv_bytes(metadata, METADATA_FIELDS),
        "market_outcomes.csv": _csv_bytes(outcomes, OUTCOME_FIELDS),
        "event_tickers.csv": _csv_bytes(events, EVENT_FIELDS),
        "market_source_provenance.jsonl": _jsonl_bytes(provenance),
    }
    counts = {status: sum(row["binary_outcome_status"] == status for row in outcomes) for status in (
        "valid_binary_yes", "valid_binary_no", "invalid_binary_result", "missing_result"
    )}
    summary = {
        "full_input_contract_count": pre_limit_contract_count,
        "full_input_unique_ticker_count": pre_limit_unique_ticker_count,
        "full_input_unique_event_ticker_count": pre_limit_unique_event_ticker_count,
        "pre_limit_contract_count": pre_limit_contract_count,
        "output_contract_count": len(metadata),
        "omitted_contract_count": pre_limit_contract_count - len(metadata),
        "limited_run": limit is not None,
        "requested_limit": limit,
        "universe_complete": pre_limit_contract_count == len(metadata),
        "contract_count": len(metadata),
        "unique_ticker_count": len({row["ticker"] for row in metadata}),
        "unique_event_ticker_count": len(events),
        "binary_yes_count": counts["valid_binary_yes"],
        "binary_no_count": counts["valid_binary_no"],
        "invalid_binary_result_count": counts["invalid_binary_result"],
        "missing_result_count": counts["missing_result"],
    }
    return contents, summary


def run(
    raw_root: Path,
    output_dir: Path,
    *,
    config_path: Path = DEFAULT_CONFIG,
    limit: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    rules = load_study_rules(config_path)
    _, commit = find_valid_acquisition_commit(raw_root)
    pairs = load_committed_records(commit)
    contents, summary = prepare_universe(pairs, rules, limit=limit)
    report = {
        "schema_version": SCHEMA_VERSION,
        "study_rules_schema_version": rules.schema_version,
        "study_rules_fingerprint": rules.fingerprint,
        "validated_acquisition_run_id": commit["run_id"],
        "input_months": list(commit["selected_months"]),
        **summary,
        "metadata_output_sha256": _sha256(contents["market_metadata.csv"]),
        "outcomes_output_sha256": _sha256(contents["market_outcomes.csv"]),
        "event_tickers_output_sha256": _sha256(contents["event_tickers.csv"]),
        "provenance_output_sha256": _sha256(contents["market_source_provenance.jsonl"]),
        "outcome_quarantine_enabled": True,
    }
    contents["universe_report.json"] = canonical_json(report) + b"\n"
    print(json.dumps(summary, sort_keys=True))
    if summary["omitted_contract_count"]:
        print(
            f"WARNING: --limit omitted {summary['omitted_contract_count']} contracts; universe is incomplete",
            file=sys.stderr,
        )
    if not dry_run:
        for name in (
            "market_metadata.csv", "market_outcomes.csv", "event_tickers.csv",
            "market_source_provenance.jsonl", "universe_report.json",
        ):
            publish_immutable_bytes(output_dir / name, contents[name])
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--raw-root", required=True, type=Path)
    parser.add_argument("--output-dir", required=True, type=Path)
    parser.add_argument("--config", type=Path, default=DEFAULT_CONFIG)
    parser.add_argument("--limit", type=int)
    parser.add_argument("--dry-run", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> None:
    args = build_parser().parse_args(argv)
    try:
        run(args.raw_root, args.output_dir, config_path=args.config, limit=args.limit, dry_run=args.dry_run)
    except Exception as exc:
        print(f"market universe preparation failed: {exc}", file=sys.stderr)
        raise SystemExit(2) from exc


if __name__ == "__main__":
    main()
