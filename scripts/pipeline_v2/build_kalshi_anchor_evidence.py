"""Build deterministic, unverified Kalshi anchor-candidate review artifacts.

Invoke as ``python -m scripts.pipeline_v2.build_kalshi_anchor_evidence``.
"""

from __future__ import annotations

import argparse
import csv
import hashlib
import io
from pathlib import Path
import sys
from typing import Any, Iterable, Mapping

from scripts.common.io_utils import read_csv_with_header
from scripts.pipeline_v2.anchor_evidence import (
    ANCHOR_EVIDENCE_FIELDS,
    ANCHOR_FAMILY_REVIEW_FIELDS,
    DECISION_TEMPLATE_FIELDS,
    EVIDENCE_SCHEMA_VERSION,
    EVENT_REQUIRED_FIELDS,
    MARKET_REQUIRED_FIELDS,
    MILESTONE_REQUIRED_FIELDS,
    build_anchor_evidence,
    family_identity,
)
from scripts.pipeline_v2.kalshi_metadata_cache import canonical_json, publish_immutable_bytes
from scripts.pipeline_v2.study_rules import (
    load_study_rules,
    validate_research_feature_columns,
)


DEFAULT_CONFIG = Path("configs/pipeline_v2.toml")
OUTPUT_NAMES = (
    "anchor_evidence.csv",
    "anchor_family_review.csv",
    "anchor_verification_decisions_template.csv",
    "anchor_evidence_report.json",
)


def _require_columns(
    actual: Iterable[str], required: Iterable[str], label: str,
) -> tuple[str, ...]:
    columns = tuple(actual)
    missing = sorted(set(required) - set(columns))
    if missing:
        raise ValueError(f"{label} is missing required columns: {', '.join(missing)}")
    return columns


def _csv_bytes(rows: Iterable[Mapping[str, Any]], fields: tuple[str, ...]) -> bytes:
    stream = io.StringIO(newline="")
    writer = csv.DictWriter(
        stream, fieldnames=fields, extrasaction="ignore", lineterminator="\n"
    )
    writer.writeheader()
    writer.writerows(rows)
    return stream.getvalue().encode("utf-8")


def _sha256(content: bytes) -> str:
    return hashlib.sha256(content).hexdigest()


def _load_inputs(
    market_path: Path, event_path: Path, milestone_path: Path,
) -> tuple[list[dict[str, str]], list[dict[str, str]], list[dict[str, str]]]:
    markets, market_header = read_csv_with_header(market_path)
    events, event_header = read_csv_with_header(event_path)
    milestones, milestone_header = read_csv_with_header(milestone_path)
    _require_columns(market_header, MARKET_REQUIRED_FIELDS, "market metadata")
    _require_columns(event_header, EVENT_REQUIRED_FIELDS, "event metadata")
    _require_columns(milestone_header, MILESTONE_REQUIRED_FIELDS, "event milestones")
    validate_research_feature_columns(market_header)
    validate_research_feature_columns(event_header)
    validate_research_feature_columns(milestone_header)
    return markets, events, milestones


def run(
    market_metadata_path: Path,
    event_metadata_path: Path,
    event_milestones_path: Path,
    output_root: Path,
    *,
    config_path: Path = DEFAULT_CONFIG,
    limit_families: int | None = None,
    dry_run: bool = False,
) -> dict[str, Any]:
    rules = load_study_rules(config_path)
    markets, events, milestones = _load_inputs(
        market_metadata_path, event_metadata_path, event_milestones_path
    )
    identities = sorted(
        {family_identity(row) for row in markets},
        key=lambda identity: (identity[1], identity[0]),
    )
    families_before_limit = len(identities)
    if limit_families is not None and limit_families < 0:
        raise ValueError("--limit-families must be nonnegative")
    selected_identities = (
        identities[:limit_families] if limit_families is not None else identities
    )
    selected_set = set(selected_identities)
    selected_markets = [row for row in markets if family_identity(row) in selected_set]

    full_event_tickers = {
        str(row.get("event_ticker") or "").strip()
        for row in markets if str(row.get("event_ticker") or "").strip()
    }
    for row in events:
        ticker = str(row.get("event_ticker") or "").strip()
        if ticker and ticker not in full_event_tickers:
            raise ValueError(f"unexpected event metadata ticker {ticker!r}")
    for row in milestones:
        ticker = str(row.get("event_ticker") or "").strip()
        if ticker and ticker not in full_event_tickers:
            raise ValueError(f"unexpected milestone event ticker {ticker!r}")

    selected_event_tickers = {
        str(row.get("event_ticker") or "").strip()
        for row in selected_markets if str(row.get("event_ticker") or "").strip()
    }
    selected_events = [
        row for row in events
        if str(row.get("event_ticker") or "").strip() in selected_event_tickers
    ]
    selected_milestones = [
        row for row in milestones
        if str(row.get("event_ticker") or "").strip() in selected_event_tickers
    ]
    built = build_anchor_evidence(
        selected_markets, selected_events, selected_milestones, rules
    )
    contents = {
        "anchor_evidence.csv": _csv_bytes(
            built.evidence_rows, ANCHOR_EVIDENCE_FIELDS
        ),
        "anchor_family_review.csv": _csv_bytes(
            built.family_rows, ANCHOR_FAMILY_REVIEW_FIELDS
        ),
        "anchor_verification_decisions_template.csv": _csv_bytes(
            built.decision_rows, DECISION_TEMPLATE_FIELDS
        ),
    }
    truncated = len(selected_identities) < families_before_limit
    report = {
        "schema_version": EVIDENCE_SCHEMA_VERSION,
        "study_rules_fingerprint": rules.fingerprint,
        **built.statistics,
        "limited_run": limit_families is not None,
        "requested_limit": limit_families,
        "families_before_limit": families_before_limit,
        "families_after_limit": len(selected_identities),
        "universe_complete": not truncated,
        "output_hashes": {
            name: _sha256(content) for name, content in sorted(contents.items())
        },
    }
    contents["anchor_evidence_report.json"] = canonical_json(report) + b"\n"
    if not dry_run:
        for name in OUTPUT_NAMES:
            publish_immutable_bytes(output_root / name, contents[name])
    print(canonical_json(report).decode("utf-8"))
    return report


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--market-metadata", required=True, type=Path)
    parser.add_argument("--event-metadata", required=True, type=Path)
    parser.add_argument("--event-milestones", required=True, type=Path)
    parser.add_argument("--output-root", required=True, type=Path)
    parser.add_argument("--config", default=DEFAULT_CONFIG, type=Path)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--limit-families", type=int)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        run(
            args.market_metadata,
            args.event_metadata,
            args.event_milestones,
            args.output_root,
            config_path=args.config,
            limit_families=args.limit_families,
            dry_run=args.dry_run,
        )
        return 0
    except (FileNotFoundError, ValueError) as exc:
        print(f"Invalid input: {exc}", file=sys.stderr)
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
