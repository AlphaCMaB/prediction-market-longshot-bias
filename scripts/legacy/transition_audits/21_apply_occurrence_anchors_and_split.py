"""
Apply Kalshi event-level occurrence_datetime anchors and split markets by
semantic timing structure.

Inputs:
  data/processed/markets_scheduled_absolute_final.csv
  data/processed/kalshi_event_anchor_metadata.csv

Outputs:
  data/processed/markets_occurrence_anchor_all.csv
  data/processed/markets_fixed_clock_final.csv
  data/processed/markets_scheduled_event_start_final.csv
  data/processed/markets_scheduled_window_final.csv
  data/processed/markets_endogenous_subevent_final.csv
  data/processed/markets_deadline_window_from_occurrence_audit.csv
  data/processed/markets_occurrence_anchor_excluded.csv
  outputs/occurrence_anchor_split_report.md
"""

from __future__ import annotations

import csv
from collections import Counter, defaultdict
from pathlib import Path

MARKETS_PATH = Path("data/processed/markets_scheduled_absolute_final.csv")
EVENT_METADATA_PATH = Path("data/processed/kalshi_event_anchor_metadata.csv")

PROCESSED_DIR = Path("data/processed")
OUTPUTS_DIR = Path("outputs")

ALL_PATH = PROCESSED_DIR / "markets_occurrence_anchor_all.csv"
FIXED_PATH = PROCESSED_DIR / "markets_fixed_clock_final.csv"
SCHEDULED_PATH = PROCESSED_DIR / "markets_scheduled_event_start_final.csv"
WINDOW_PATH = PROCESSED_DIR / "markets_scheduled_window_final.csv"
SUBEVENT_PATH = PROCESSED_DIR / "markets_endogenous_subevent_final.csv"
DEADLINE_PATH = PROCESSED_DIR / "markets_deadline_window_from_occurrence_audit.csv"
EXCLUDED_PATH = PROCESSED_DIR / "markets_occurrence_anchor_excluded.csv"
REPORT_PATH = OUTPUTS_DIR / "occurrence_anchor_split_report.md"

FIXED_PREFIXES = (
    "KXBTC", "KXETH", "KXBNB", "KXDOGE", "KXHYPE", "KXSOL", "KXXRP",
    "KXINXU", "KXNASDAQ100U", "KXWTI", "KXTEMPNYCH", "KXHIGH", "KXLOWT",
)
SUBEVENT_PREFIXES = (
    "KXPGAHOLESCORE", "KXATPSETWINNER", "KXWTASETWINNER",
    "KXCS2MAP", "KXVALORANTMAP", "KXDOTA2MAP",
)
WINDOW_PREFIXES = ("KXATP-", "KXWTA-", "KXWNBACCUP-")
DEADLINE_PREFIXES = ("KXKOSPI-",)


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")
    with path.open(newline="", encoding="utf-8-sig") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    if not rows:
        path.write_text("", encoding="utf-8")
        print(f"Saved empty file: {path}")
        return
    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=list(rows[0].keys()), extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)
    print(f"Saved: {path} ({len(rows)} rows)")


def event_ticker(row: dict) -> str:
    family_id = str(row.get("family_id_v2") or row.get("family_id") or "")
    prefix = "kalshi_event::"
    if family_id.startswith(prefix):
        return family_id[len(prefix):]
    return str(row.get("event_ticker") or "").strip()


def classify(ticker: str, title: str) -> tuple[str, str]:
    text = str(title or "").lower()

    if ticker.startswith(DEADLINE_PREFIXES):
        return "deadline_window", "Outcome may become known at any point during a calendar window."
    if ticker.startswith(SUBEVENT_PREFIXES):
        return "endogenous_subevent", "Set, map, or hole timing is not known ex ante."
    if ticker.startswith(WINDOW_PREFIXES):
        return "scheduled_window", "Tournament or multi-day event lacks one clean resolution instant."
    if ticker.startswith(FIXED_PREFIXES):
        return "fixed_clock", "Outcome is tied to a fixed clock or calendar observation boundary."
    if any(x in text for x in ["match", "game", "press conference", "total games", "total maps", "set score"]):
        return "scheduled_event_start", "Parent event has a scheduled occurrence time; anchor is event start."
    return "scheduled_event_start", "Occurrence timestamp exists; retain for a separate pre-event analysis."


def main() -> None:
    markets = read_csv(MARKETS_PATH)
    metadata = read_csv(EVENT_METADATA_PATH)
    meta_by_event = {r["event_ticker"]: r for r in metadata if r.get("event_ticker")}

    rows = []
    for market in markets:
        out = dict(market)
        ticker = event_ticker(market)
        meta = meta_by_event.get(ticker)
        out["event_ticker_joined"] = ticker
        out["anchor_time_previous"] = market.get("anchor_time_final", "")

        if market.get("venue") != "kalshi" or meta is None:
            out.update({
                "timing_structure": "excluded",
                "timing_structure_reason": "No usable Kalshi event metadata for this row.",
                "occurrence_anchor_time": "",
                "occurrence_anchor_source": "",
                "occurrence_anchor_confidence": "",
                "occurrence_anchor_usable": "0",
                "anchor_time_final_v2": "",
                "anchor_time_source_final_v2": "",
            })
            rows.append(out)
            continue

        structure, reason = classify(ticker, market.get("title", ""))
        occurrence = meta.get("candidate_anchor_time", "")
        source = meta.get("candidate_anchor_source", "")
        confidence = meta.get("candidate_anchor_confidence", "")
        usable = bool(
            occurrence
            and source == "market_occurrence_datetime"
            and confidence == "high"
        )

        out.update({
            "timing_structure": structure,
            "timing_structure_reason": reason,
            "occurrence_anchor_time": occurrence,
            "occurrence_anchor_source": source,
            "occurrence_anchor_confidence": confidence,
            "occurrence_anchor_usable": "1" if usable else "0",
            "anchor_time_final_v2": occurrence if usable else "",
            "anchor_time_source_final_v2": source if usable else "",
        })
        rows.append(out)

    groups = {
        "fixed_clock": FIXED_PATH,
        "scheduled_event_start": SCHEDULED_PATH,
        "scheduled_window": WINDOW_PATH,
        "endogenous_subevent": SUBEVENT_PATH,
        "deadline_window": DEADLINE_PATH,
    }

    write_csv(ALL_PATH, rows)
    for structure, path in groups.items():
        selected = [
            r for r in rows
            if r["timing_structure"] == structure
            and r["occurrence_anchor_usable"] == "1"
        ]
        write_csv(path, selected)

    excluded = [
        r for r in rows
        if r["timing_structure"] == "excluded"
        or r["occurrence_anchor_usable"] != "1"
    ]
    write_csv(EXCLUDED_PATH, excluded)

    contract_counts = Counter(r["timing_structure"] for r in rows)
    family_sets = defaultdict(set)
    for r in rows:
        family_sets[r["timing_structure"]].add(
            r.get("family_id_v2") or r.get("family_id") or r.get("event_ticker_joined") or r.get("market_id")
        )

    lines = [
        "# Occurrence-Anchor Timing Split",
        "",
        "Kalshi close_time has been replaced by event-level occurrence_datetime where available.",
        "",
        f"- Input contracts: {len(markets)}",
        f"- Output contracts: {len(rows)}",
        "",
        "## Counts by timing structure",
        "",
    ]
    for structure in sorted(contract_counts):
        lines.append(
            f"- {structure}: {contract_counts[structure]} contracts, "
            f"{len(family_sets[structure])} families"
        )

    lines.extend([
        "",
        "## Analysis roles",
        "",
        "- fixed_clock: strict primary analysis.",
        "- scheduled_event_start: separate pre-event analysis.",
        "- scheduled_window: descriptive analysis.",
        "- endogenous_subevent: exclude from the main analysis.",
        "- deadline_window: use deadline-relative methodology.",
        "",
        "The new horizon means hours before scheduled occurrence, not hours before realized resolution.",
    ])

    REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)
    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved: {REPORT_PATH}")


if __name__ == "__main__":
    main()
