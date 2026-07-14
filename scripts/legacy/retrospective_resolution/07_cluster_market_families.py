"""
07_cluster_market_families.py

Purpose:
    Add event-family clusters to p_hat_batch.csv and create de-clustered datasets.

Inputs:
    data/processed/p_hat_batch.csv

Outputs:
    data/processed/p_hat_batch_clustered.csv
    data/processed/p_hat_batch_declustered_family_bin.csv
    data/processed/p_hat_batch_declustered_one_per_family.csv
    outputs/declustering_report.md
"""

from __future__ import annotations

import csv
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


INPUT_PATH = Path("data/processed/p_hat_batch.csv")

PROCESSED_DIR = Path("data/processed")
OUTPUTS_DIR = Path("outputs")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

CLUSTERED_PATH = PROCESSED_DIR / "p_hat_batch_clustered.csv"
DECLUSTERED_FAMILY_BIN_PATH = PROCESSED_DIR / "p_hat_batch_declustered_family_bin.csv"
DECLUSTERED_ONE_PER_FAMILY_PATH = PROCESSED_DIR / "p_hat_batch_declustered_one_per_family.csv"
REPORT_PATH = OUTPUTS_DIR / "declustering_report.md"

MAX_TARGET_ERROR_HOURS = 2.0


def to_float(value: Any) -> float | None:
    if value is None or value == "":
        return None

    try:
        x = float(value)
    except Exception:
        return None

    if math.isnan(x) or math.isinf(x):
        return None

    return x


def to_int(value: Any) -> int | None:
    if value is None or value == "":
        return None

    try:
        x = int(float(value))
    except Exception:
        return None

    return x


def read_csv(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")

    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def write_csv(path: Path, rows: list[dict]) -> None:
    if not rows:
        path.write_text("", encoding="utf-8")
        print(f"Saved empty file: {path}")
        return

    fieldnames = list(rows[0].keys())

    with path.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames, extrasaction="ignore")
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved: {path} ({len(rows)} rows)")


def valid_analysis_row(row: dict) -> bool:
    if row.get("status") != "ok":
        return False

    p_hat = to_float(row.get("p_hat"))
    outcome = to_int(row.get("outcome"))
    target_error = to_float(row.get("target_error_hours"))

    if p_hat is None or not (0 <= p_hat <= 1):
        return False

    if outcome not in {0, 1}:
        return False

    if target_error is None or target_error > MAX_TARGET_ERROR_HOURS:
        return False

    return True


def normalize_title(text: str) -> str:
    text = text.lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s$%.]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def remove_thresholds(text: str) -> str:
    """
    Remove numbers and threshold words so related markets cluster together.

    Example:
        Espresso FDV above $700M one day after launch
        Espresso FDV above $200M one day after launch

    Both become roughly:
        espresso fdv one day after launch
    """
    text = text.replace(",", "")

    threshold_words = [
        "above",
        "below",
        "over",
        "under",
        "at least",
        "at most",
        "greater than",
        "less than",
        "more than",
        "fewer than",
        "higher than",
        "lower than",
    ]

    for word in threshold_words:
        text = text.replace(word, " ")

    text = re.sub(r"\$?\d+(\.\d+)?\s*(k|m|b|mm|bn|million|billion|thousand|percent|%)?", " ", text)
    text = re.sub(r"\bwill\b", " ", text)
    text = re.sub(r"\byes\b", " ", text)
    text = re.sub(r"\bno\b", " ", text)
    text = re.sub(r"\s+", " ", text).strip()

    return text


def infer_polymarket_family(row: dict) -> str:
    title = row.get("title", "")
    market_id = row.get("market_id", "")

    text = normalize_title(title)
    text = remove_thresholds(text)

    if not text:
        text = f"market_{market_id}"

    return f"polymarket::{text}"


def infer_kalshi_family(row: dict) -> str:
    market_id = row.get("market_id", "")

    if not market_id:
        return "kalshi::unknown"

    parts = market_id.split("-")

    if len(parts) >= 3:
        family = "-".join(parts[:-1])
    else:
        family = market_id

    return f"kalshi::{family}"


def infer_event_family(row: dict) -> str:
    venue = row.get("venue", "")

    if venue == "polymarket":
        return infer_polymarket_family(row)

    if venue == "kalshi":
        return infer_kalshi_family(row)

    return f"unknown::{row.get('market_id', '')}"


def probability_bin(p: float) -> str:
    idx = min(int(p * 10), 9)
    low = idx / 10
    high = (idx + 1) / 10
    return f"{low:.1f}-{high:.1f}"


def choose_best_row(rows: list[dict]) -> dict:
    def sort_key(row: dict) -> tuple:
        target_error = to_float(row.get("target_error_hours"))
        spread = to_float(row.get("spread"))

        if target_error is None:
            target_error = 999999

        if spread is None:
            spread = 999999

        return (
            target_error,
            spread,
            row.get("market_id", ""),
        )

    return sorted(rows, key=sort_key)[0]


def add_cluster_columns(rows: list[dict]) -> list[dict]:
    eligible_rows = [row for row in rows if valid_analysis_row(row)]
    family_counts = Counter(infer_event_family(row) for row in eligible_rows)

    output = []

    for row in rows:
        row = dict(row)

        if valid_analysis_row(row):
            p_hat = to_float(row["p_hat"])
            family = infer_event_family(row)

            row["event_family"] = family
            row["family_size"] = family_counts[family]
            row["probability_bin"] = probability_bin(p_hat)
            row["cluster_analysis_eligible"] = "1"
        else:
            row["event_family"] = ""
            row["family_size"] = ""
            row["probability_bin"] = ""
            row["cluster_analysis_eligible"] = "0"

        output.append(row)

    return output


def decluster_family_bin(rows: list[dict]) -> list[dict]:
    groups = defaultdict(list)

    for row in rows:
        if row.get("cluster_analysis_eligible") != "1":
            continue

        key = (row["event_family"], row["probability_bin"])
        groups[key].append(row)

    selected = [choose_best_row(group_rows) for group_rows in groups.values()]

    return sorted(selected, key=lambda r: (r["venue"], r["event_family"], r["probability_bin"]))


def decluster_one_per_family(rows: list[dict]) -> list[dict]:
    groups = defaultdict(list)

    for row in rows:
        if row.get("cluster_analysis_eligible") != "1":
            continue

        groups[row["event_family"]].append(row)

    selected = []

    for family, group_rows in groups.items():
        selected.append(choose_best_row(group_rows))

    return sorted(selected, key=lambda r: (r["venue"], r["event_family"]))


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else float("nan")


def brier(rows: list[dict]) -> float:
    values = []

    for row in rows:
        p_hat = to_float(row.get("p_hat"))
        outcome = to_int(row.get("outcome"))

        if p_hat is None or outcome not in {0, 1}:
            continue

        values.append((p_hat - outcome) ** 2)

    return mean(values)


def mean_p_hat(rows: list[dict]) -> float:
    values = []

    for row in rows:
        p_hat = to_float(row.get("p_hat"))

        if p_hat is not None:
            values.append(p_hat)

    return mean(values)


def empirical_rate(rows: list[dict]) -> float:
    values = []

    for row in rows:
        outcome = to_int(row.get("outcome"))

        if outcome in {0, 1}:
            values.append(outcome)

    return mean(values)


def calibration_bins(rows: list[dict]) -> list[dict]:
    groups = defaultdict(list)

    for row in rows:
        p_hat = to_float(row.get("p_hat"))

        if p_hat is None:
            continue

        groups[probability_bin(p_hat)].append(row)

    output = []

    for i in range(10):
        label = f"{i / 10:.1f}-{(i + 1) / 10:.1f}"
        bin_rows = groups.get(label, [])

        if not bin_rows:
            output.append(
                {
                    "bin": label,
                    "n": 0,
                    "mean_p_hat": "",
                    "empirical_rate": "",
                    "empirical_minus_p": "",
                }
            )
            continue

        mp = mean_p_hat(bin_rows)
        er = empirical_rate(bin_rows)

        output.append(
            {
                "bin": label,
                "n": len(bin_rows),
                "mean_p_hat": mp,
                "empirical_rate": er,
                "empirical_minus_p": er - mp,
            }
        )

    return output


def summarize_dataset(name: str, rows: list[dict]) -> dict:
    return {
        "name": name,
        "rows": len(rows),
        "families": len(set(row["event_family"] for row in rows)),
        "polymarket": sum(row["venue"] == "polymarket" for row in rows),
        "kalshi": sum(row["venue"] == "kalshi" for row in rows),
        "brier": brier(rows),
        "mean_p_hat": mean_p_hat(rows),
        "empirical_rate": empirical_rate(rows),
    }


def write_report(
    clustered_rows: list[dict],
    family_bin_rows: list[dict],
    one_per_family_rows: list[dict],
) -> None:
    eligible_rows = [
        row for row in clustered_rows
        if row.get("cluster_analysis_eligible") == "1"
    ]

    family_counts = Counter(row["event_family"] for row in eligible_rows)
    venue_counts = Counter(row["venue"] for row in eligible_rows)

    summaries = [
        summarize_dataset("raw_eligible", eligible_rows),
        summarize_dataset("declustered_family_bin", family_bin_rows),
        summarize_dataset("declustered_one_per_family", one_per_family_rows),
    ]

    lines = []
    lines.append("# De-clustering Report")
    lines.append("")
    lines.append("This report checks whether related markets dominate the pilot result.")
    lines.append("")
    lines.append("No API calls were made in this step.")
    lines.append("")
    lines.append("## Input")
    lines.append("")
    lines.append(f"- Total rows in p_hat file: {len(clustered_rows)}")
    lines.append(f"- Eligible rows after status/48h filtering: {len(eligible_rows)}")
    lines.append(f"- Unique event families: {len(family_counts)}")
    lines.append("")
    lines.append("## Eligible rows by venue")
    lines.append("")

    for venue, count in sorted(venue_counts.items()):
        lines.append(f"- {venue}: {count}")

    lines.append("")
    lines.append("## Dataset comparison")
    lines.append("")

    for summary in summaries:
        lines.append(f"### {summary['name']}")
        lines.append(f"- Rows: {summary['rows']}")
        lines.append(f"- Event families: {summary['families']}")
        lines.append(f"- Polymarket rows: {summary['polymarket']}")
        lines.append(f"- Kalshi rows: {summary['kalshi']}")
        lines.append(f"- Brier: {summary['brier']:.6f}")
        lines.append(f"- Mean p_hat: {summary['mean_p_hat']:.6f}")
        lines.append(f"- Empirical rate: {summary['empirical_rate']:.6f}")
        lines.append("")

    lines.append("## Largest event families")
    lines.append("")

    for family, count in family_counts.most_common(20):
        example = next(row for row in eligible_rows if row["event_family"] == family)
        title = example.get("title", "")
        lines.append(f"- {count} rows | {family} | example: {title}")

    lines.append("")
    lines.append("## Calibration after family-bin de-clustering")
    lines.append("")

    for row in calibration_bins(family_bin_rows):
        if row["n"] == 0:
            lines.append(f"- {row['bin']}: n=0")
        else:
            lines.append(
                f"- {row['bin']}: "
                f"n={row['n']}, "
                f"mean_p={float(row['mean_p_hat']):.4f}, "
                f"empirical={float(row['empirical_rate']):.4f}, "
                f"empirical_minus_p={float(row['empirical_minus_p']):.4f}"
            )

    lines.append("")
    lines.append("## Interpretation")
    lines.append("")
    lines.append("- `raw_eligible` is the original usable batch after basic filtering.")
    lines.append("- `declustered_family_bin` keeps one row per event family per probability bin.")
    lines.append("- `declustered_one_per_family` keeps one row per event family.")
    lines.append("")
    lines.append("If the favorite-longshot pattern survives family-bin de-clustering, it is more credible.")
    lines.append("If it disappears, the original signal was probably driven by repeated related markets.")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved: {REPORT_PATH}")


def main() -> None:
    print("=" * 80)
    print("Cluster market families")
    print("No API calls.")

    rows = read_csv(INPUT_PATH)
    clustered_rows = add_cluster_columns(rows)

    family_bin_rows = decluster_family_bin(clustered_rows)
    one_per_family_rows = decluster_one_per_family(clustered_rows)

    eligible_rows = [
        row for row in clustered_rows
        if row.get("cluster_analysis_eligible") == "1"
    ]

    write_csv(CLUSTERED_PATH, clustered_rows)
    write_csv(DECLUSTERED_FAMILY_BIN_PATH, family_bin_rows)
    write_csv(DECLUSTERED_ONE_PER_FAMILY_PATH, one_per_family_rows)

    write_report(
        clustered_rows=clustered_rows,
        family_bin_rows=family_bin_rows,
        one_per_family_rows=one_per_family_rows,
    )

    print("\n" + "=" * 80)
    print("De-clustering complete")
    print("Original rows:", len(rows))
    print("Eligible rows:", len(eligible_rows))
    print("Family-bin de-clustered rows:", len(family_bin_rows))
    print("One-per-family rows:", len(one_per_family_rows))
    print("")
    print("Check:")
    print(f"  {REPORT_PATH}")


if __name__ == "__main__":
    main()