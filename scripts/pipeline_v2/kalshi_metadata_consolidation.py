"""Pure monthly consolidation and explicit immutable derived-file writing."""

from __future__ import annotations

from dataclasses import dataclass
import hashlib
import json
from pathlib import Path
from typing import Any, Iterable, Mapping

from scripts.common.time_utils import format_iso_utc, parse_iso_utc
from scripts.pipeline_v2.kalshi_metadata_cache import canonical_json, publish_immutable_bytes
from scripts.pipeline_v2.kalshi_metadata_planner import MonthInterval


class ConsolidationError(RuntimeError):
    pass


class ConsolidationConflict(ConsolidationError):
    pass


@dataclass(frozen=True)
class ConsolidationResult:
    records: tuple[dict[str, Any], ...]
    audit_records: tuple[dict[str, Any], ...]
    record_provenance: tuple[dict[str, Any], ...]
    source_set_hash: str


PROVENANCE_UNKNOWN = {"endpoint_tier": "unknown"}


def _semantic_bytes(record: Mapping[str, Any]) -> bytes:
    return canonical_json(dict(record))


def payload_sha256(record: Mapping[str, Any]) -> str:
    return hashlib.sha256(_semantic_bytes(record)).hexdigest()


def _source_hash(
    records: Iterable[Mapping[str, Any]],
    source_information: Iterable[Mapping[str, Any]] | None = None,
) -> str:
    values = list(records)
    sources = list(source_information or [PROVENANCE_UNKNOWN] * len(values))
    if len(values) != len(sources):
        raise ValueError("source information must align with records")
    payloads = sorted(
        hashlib.sha256(
            canonical_json({"payload": dict(record), "source_information": dict(source)})
        ).hexdigest()
        for record, source in zip(values, sources)
    )
    return hashlib.sha256("\n".join(payloads).encode()).hexdigest()[:20]


def _resolve_variants(
    ticker: str, variants: list[tuple[dict[str, Any], dict[str, Any]]]
) -> tuple[dict[str, Any], dict[str, Any] | None]:
    unique: dict[str, tuple[dict[str, Any], list[dict[str, Any]]]] = {}
    for payload, source in variants:
        digest = hashlib.sha256(_semantic_bytes(payload)).hexdigest()
        if digest not in unique:
            unique[digest] = (payload, [])
        unique[digest][1].append(source)
    if len(unique) == 1:
        return next(iter(unique.values()))[0], None

    parsed = [
        (parse_iso_utc(payload.get("updated_time")), payload, sources)
        for payload, sources in unique.values()
    ]
    if all(timestamp is not None for timestamp, _, _ in parsed):
        ordered = sorted(parsed, key=lambda item: (item[0], _semantic_bytes(item[1])))
        if len(ordered) == 1 or ordered[-1][0] > ordered[-2][0]:
            chosen = ordered[-1][1]
            return chosen, {
                "ticker": ticker,
                "status": "resolved_by_latest_updated_time",
                "resolution_rule": "unique_latest_valid_updated_time",
                "chosen_updated_time": chosen.get("updated_time"),
                "selected_winner": chosen,
                "selected_source_information": ordered[-1][2],
                "variants": [
                    {"payload": payload, "source_information": sources}
                    for _, payload, sources in ordered
                ],
            }
    raise ConsolidationConflict(
        f"ticker {ticker!r} has conflicting payloads without a unique latest updated_time"
    )


def consolidate_month(
    records: Iterable[Mapping[str, Any]],
    month: MonthInterval,
    *,
    source_information: Iterable[Mapping[str, Any]] | None = None,
) -> ConsolidationResult:
    """Filter locally by settlement_ts and deduplicate without rewriting raw fields."""
    source_records = [dict(item) for item in records]
    sources = [dict(item) for item in source_information] if source_information is not None else [
        dict(PROVENANCE_UNKNOWN) for _ in source_records
    ]
    if len(source_records) != len(sources):
        raise ValueError("source information must align with records")
    grouped: dict[str, list[tuple[dict[str, Any], dict[str, Any]]]] = {}
    audit: list[dict[str, Any]] = []
    for record, source in zip(source_records, sources):
        ticker = str(record.get("ticker") or "").strip()
        if not ticker:
            audit.append(
                {
                    "status": "excluded_missing_ticker",
                    "reason": "required ticker is missing",
                    "raw_payload": record,
                    "source_information": source,
                }
            )
            continue
        settled = parse_iso_utc(record.get("settlement_ts"))
        if settled is None:
            audit.append(
                {
                    "ticker": ticker,
                    "status": "excluded_invalid_settlement_ts",
                    "reason": "settlement_ts is missing or invalid",
                    "raw_payload": record,
                    "source_information": source,
                }
            )
            continue
        if month.start <= settled < month.end:
            grouped.setdefault(ticker, []).append((record, source))

    selected = []
    record_provenance = []
    for ticker in sorted(grouped):
        chosen, duplicate_audit = _resolve_variants(ticker, grouped[ticker])
        selected.append(chosen)
        chosen_hash = payload_sha256(chosen)
        variants: dict[str, dict[str, Any]] = {}
        for payload, source in grouped[ticker]:
            digest = payload_sha256(payload)
            variant = variants.setdefault(
                digest,
                {
                    "payload_sha256": digest,
                    "selected": digest == chosen_hash,
                    "source_associations": [],
                },
            )
            variant["source_associations"].append(dict(source))
        for variant in variants.values():
            variant["source_associations"].sort(key=canonical_json)
        ordered_variants = [variants[digest] for digest in sorted(variants)]
        all_associations = []
        for variant in ordered_variants:
            for source in variant["source_associations"]:
                all_associations.append(
                    {
                        "payload_sha256": variant["payload_sha256"],
                        "selected_payload": variant["selected"],
                        **source,
                    }
                )
        all_associations.sort(key=canonical_json)
        record_provenance.append(
            {
                "month": month.month,
                "output_record_id": f"{ticker}|{chosen_hash}",
                "ticker": ticker,
                "settlement_ts": chosen.get("settlement_ts"),
                "selected_payload_sha256": chosen_hash,
                "source_associations": all_associations,
                "payload_variants": ordered_variants,
            }
        )
        if duplicate_audit:
            audit.append(duplicate_audit)
    selected.sort(
        key=lambda item: (
            format_iso_utc(parse_iso_utc(item.get("settlement_ts"))),
            str(item["ticker"]),
        )
    )
    return ConsolidationResult(
        records=tuple(selected),
        audit_records=tuple(audit),
        record_provenance=tuple(
            sorted(
                record_provenance,
                key=lambda item: (
                    str(item["month"]), str(item.get("settlement_ts") or ""),
                    str(item["ticker"]), str(item["selected_payload_sha256"]),
                ),
            )
        ),
        source_set_hash=_source_hash(source_records, sources),
    )


def invalid_record_audits(
    records: Iterable[Mapping[str, Any]],
    *,
    source_information: Iterable[Mapping[str, Any]] | None = None,
) -> tuple[tuple[dict[str, Any], ...], str]:
    """Return deterministic fatal audits before any monthly publication."""
    values = [dict(item) for item in records]
    sources = [dict(item) for item in source_information] if source_information is not None else [
        dict(PROVENANCE_UNKNOWN) for _ in values
    ]
    if len(values) != len(sources):
        raise ValueError("source information must align with records")
    audits = []
    for payload, source in zip(values, sources):
        ticker = str(payload.get("ticker") or "").strip()
        if not ticker:
            audits.append(
                {
                    "status": "invalid_required_market_record",
                    "reason": "required ticker is missing",
                    "raw_payload": payload,
                    "source_information": source,
                }
            )
        elif parse_iso_utc(payload.get("settlement_ts")) is None:
            audits.append(
                {
                    "ticker": ticker,
                    "status": "invalid_required_market_record",
                    "reason": "settlement_ts is missing or invalid",
                    "raw_payload": payload,
                    "source_information": source,
                }
            )
    audits.sort(key=lambda item: canonical_json(item))
    return tuple(audits), _source_hash(values, sources)


def monthly_output_path(
    raw_root: str | Path, month: str, source_set_hash: str
) -> Path:
    return Path(raw_root) / month / f"settled_markets_{source_set_hash}.jsonl"


def monthly_audit_path(
    raw_root: str | Path, month: str, source_set_hash: str
) -> Path:
    return Path(raw_root) / month / f"settled_markets_audit_{source_set_hash}.jsonl"


def monthly_provenance_path(
    raw_root: str | Path, month: str, source_set_hash: str
) -> Path:
    return Path(raw_root) / month / f"settled_markets_provenance_{source_set_hash}.jsonl"


def invalid_audit_path(raw_root: str | Path, source_set_hash: str) -> Path:
    return Path(raw_root) / "audits" / f"invalid_market_records_{source_set_hash}.jsonl"


def serialize_jsonl(records: Iterable[Mapping[str, Any]]) -> bytes:
    values = [canonical_json(dict(record)) for record in records]
    return (b"\n".join(values) + (b"\n" if values else b""))


def write_derived_jsonl(path: str | Path, records: Iterable[Mapping[str, Any]]) -> str:
    """Publish deterministic bytes, reusing identical output and never overwriting."""
    try:
        return publish_immutable_bytes(path, serialize_jsonl(records))
    except Exception as exc:
        if isinstance(exc, ConsolidationError):
            raise
        raise ConsolidationError(str(exc)) from exc
