"""
14_balanced_horizon_panel.py

Purpose:
    Test horizon robustness on a balanced panel:
    only markets that have valid p_hat at all three horizons:
        - 24h
        - 48h
        - 168h / 7d

Why:
    The previous horizon robustness analysis used all available markets at each horizon.
    That is useful, but the 168h sample is smaller and may contain a different mix
    of markets. This script checks whether the result is still visible when we
    compare the same markets across horizons.

Input:
    data/processed/p_hat_horizons.csv

Outputs:
    data/processed/p_hat_horizons_balanced_panel.csv
    data/processed/p_hat_horizons_balanced_panel_declustered.csv
    outputs/balanced_horizon_summary.csv
    outputs/balanced_horizon_calibration_bins.csv
    outputs/balanced_horizon_panel_report.md
    outputs/balanced_horizon_calibration_<horizon>h.png, if matplotlib is installed

Important:
    This script does NOT call APIs.
"""

from __future__ import annotations

import csv
import math
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


INPUT_PATH = Path("data/processed/p_hat_horizons.csv")

PROCESSED_DIR = Path("data/processed")
OUTPUTS_DIR = Path("outputs")

PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

BALANCED_PATH = PROCESSED_DIR / "p_hat_horizons_balanced_panel.csv"
DECLUSTERED_PATH = PROCESSED_DIR / "p_hat_horizons_balanced_panel_declustered.csv"

SUMMARY_PATH = OUTPUTS_DIR / "balanced_horizon_summary.csv"
CALIBRATION_PATH = OUTPUTS_DIR / "balanced_horizon_calibration_bins.csv"
REPORT_PATH = OUTPUTS_DIR / "balanced_horizon_panel_report.md"

HORIZONS = ["24", "48", "168"]
MAX_TARGET_ERROR_HOURS = 2.0

BINS = [
    (0.0, 0.1),
    (0.1, 0.2),
    (0.2, 0.3),
    (0.3, 0.4),
    (0.4, 0.5),
    (0.5, 0.6),
    (0.6, 0.7),
    (0.7, 0.8),
    (0.8, 0.9),
    (0.9, 1.0000001),
]


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


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else float("nan")


def valid_row(row: dict) -> bool:
    if row.get("status") != "ok":
        return False

    p = to_float(row.get("p_hat"))
    y = to_int(row.get("outcome"))
    target_error = to_float(row.get("target_error_hours"))

    if p is None or not (0.0 <= p <= 1.0):
        return False

    if y not in {0, 1}:
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

    text = re.sub(
        r"\$?\d+(\.\d+)?\s*(k|m|b|mm|bn|million|billion|thousand|percent|%)?",
        " ",
        text,
    )
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


def bin_for_p(p: float) -> tuple[float, float] | None:
    for low, high in BINS:
        if low <= p < high:
            return low, high

    return None


def add_analysis_columns(row: dict) -> dict:
    row = dict(row)
    p = to_float(row.get("p_hat"))

    row["event_family"] = infer_event_family(row)
    row["probability_bin"] = probability_bin(p)
    row["balanced_panel_eligible"] = "1"

    return row


def choose_best_row(rows: list[dict]) -> dict:
    def key(row: dict) -> tuple:
        target_error = to_float(row.get("target_error_hours"))
        spread = to_float(row.get("spread"))

        if target_error is None:
            target_error = 999999.0

        if spread is None:
            spread = 999999.0

        return (
            target_error,
            spread,
            row.get("market_id", ""),
        )

    return sorted(rows, key=key)[0]


def decluster_family_bin(rows: list[dict]) -> list[dict]:
    groups = defaultdict(list)

    for row in rows:
        key = (
            row.get("horizon_hours"),
            row.get("venue"),
            row.get("event_family"),
            row.get("probability_bin"),
        )
        groups[key].append(row)

    selected = [choose_best_row(group) for group in groups.values()]

    return sorted(
        selected,
        key=lambda row: (
            int(row["horizon_hours"]),
            row["venue"],
            row["event_family"],
            row["probability_bin"],
        ),
    )


def brier(rows: list[dict]) -> float:
    values = []

    for row in rows:
        p = to_float(row.get("p_hat"))
        y = to_int(row.get("outcome"))

        if p is not None and y in {0, 1}:
            values.append((p - y) ** 2)

    return mean(values)


def mean_p(rows: list[dict]) -> float:
    values = []

    for row in rows:
        p = to_float(row.get("p_hat"))
        if p is not None:
            values.append(p)

    return mean(values)


def empirical(rows: list[dict]) -> float:
    values = []

    for row in rows:
        y = to_int(row.get("outcome"))
        if y in {0, 1}:
            values.append(y)

    return mean(values)


def make_summary_rows(raw_rows: list[dict], declustered_rows: list[dict]) -> list[dict]:
    output = []

    for sample_type, rows in [
        ("balanced_raw", raw_rows),
        ("balanced_declustered_family_bin", declustered_rows),
    ]:
        for horizon in HORIZONS:
            subset = [row for row in rows if row.get("horizon_hours") == horizon]

            if not subset:
                continue

            mp = mean_p(subset)
            er = empirical(subset)

            output.append(
                {
                    "sample_type": sample_type,
                    "horizon_hours": horizon,
                    "venue": "all",
                    "n": len(subset),
                    "markets": len(set(row["market_id"] for row in subset)),
                    "families": len(set(row["event_family"] for row in subset)),
                    "mean_p_hat": mp,
                    "empirical_rate": er,
                    "empirical_minus_p": er - mp,
                    "brier_score": brier(subset),
                }
            )

            for venue in sorted(set(row["venue"] for row in subset)):
                venue_rows = [row for row in subset if row["venue"] == venue]
                mp = mean_p(venue_rows)
                er = empirical(venue_rows)

                output.append(
                    {
                        "sample_type": sample_type,
                        "horizon_hours": horizon,
                        "venue": venue,
                        "n": len(venue_rows),
                        "markets": len(set(row["market_id"] for row in venue_rows)),
                        "families": len(set(row["event_family"] for row in venue_rows)),
                        "mean_p_hat": mp,
                        "empirical_rate": er,
                        "empirical_minus_p": er - mp,
                        "brier_score": brier(venue_rows),
                    }
                )

    return output


def make_calibration_rows(rows: list[dict], sample_type: str) -> list[dict]:
    output = []

    for horizon in HORIZONS:
        horizon_rows = [row for row in rows if row.get("horizon_hours") == horizon]

        for venue in ["all"] + sorted(set(row["venue"] for row in horizon_rows)):
            if venue == "all":
                subset = horizon_rows
            else:
                subset = [row for row in horizon_rows if row["venue"] == venue]

            grouped = defaultdict(list)

            for row in subset:
                p = to_float(row.get("p_hat"))
                if p is None:
                    continue

                b = bin_for_p(p)
                if b is not None:
                    grouped[b].append(row)

            for low, high in BINS:
                bin_rows = grouped.get((low, high), [])

                if not bin_rows:
                    output.append(
                        {
                            "sample_type": sample_type,
                            "horizon_hours": horizon,
                            "venue": venue,
                            "bin_low": low,
                            "bin_high": high,
                            "n": 0,
                            "mean_p_hat": "",
                            "empirical_rate": "",
                            "empirical_minus_p": "",
                            "brier_score": "",
                        }
                    )
                    continue

                mp = mean_p(bin_rows)
                er = empirical(bin_rows)

                output.append(
                    {
                        "sample_type": sample_type,
                        "horizon_hours": horizon,
                        "venue": venue,
                        "bin_low": low,
                        "bin_high": high,
                        "n": len(bin_rows),
                        "mean_p_hat": mp,
                        "empirical_rate": er,
                        "empirical_minus_p": er - mp,
                        "brier_score": brier(bin_rows),
                    }
                )

    return output


def try_write_plots(calibration_rows: list[dict]) -> list[Path]:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        print("matplotlib not installed; skipping plots.")
        return []

    saved = []

    rows = [
        row for row in calibration_rows
        if row["sample_type"] == "balanced_declustered_family_bin"
        and row["venue"] == "all"
        and int(row["n"]) > 0
    ]

    for horizon in HORIZONS:
        subset = [row for row in rows if row["horizon_hours"] == horizon]

        xs = []
        ys = []
        sizes = []

        for row in subset:
            if row["mean_p_hat"] == "" or row["empirical_rate"] == "":
                continue

            xs.append(float(row["mean_p_hat"]))
            ys.append(float(row["empirical_rate"]))
            sizes.append(max(20, int(row["n"]) * 2))

        if not xs:
            continue

        path = OUTPUTS_DIR / f"balanced_horizon_calibration_{horizon}h.png"

        plt.figure(figsize=(6, 6))
        plt.scatter(xs, ys, s=sizes, alpha=0.7)
        plt.plot([0, 1], [0, 1], linestyle="--")
        plt.xlabel("Mean market probability p_hat")
        plt.ylabel("Empirical outcome rate")
        plt.title(f"Balanced-panel calibration: {horizon}h")
        plt.xlim(0, 1)
        plt.ylim(0, 1)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(path, dpi=200)
        plt.close()

        saved.append(path)
        print(f"Saved: {path}")

    return saved


def write_report(
    valid_rows_count: int,
    balanced_market_count: int,
    balanced_rows: list[dict],
    declustered_rows: list[dict],
    summary_rows: list[dict],
    calibration_rows: list[dict],
    plot_paths: list[Path],
) -> None:
    lines = []

    lines.append("# Balanced Horizon Panel Report")
    lines.append("")
    lines.append("This report compares horizons using only markets with valid p_hat at 24h, 48h, and 168h.")
    lines.append("")
    lines.append("This checks whether the horizon result is driven by different sample composition across horizons.")
    lines.append("")
    lines.append("## Filtering")
    lines.append("")
    lines.append(f"- Valid rows before balanced-panel filter: {valid_rows_count}")
    lines.append(f"- Markets with all three horizons: {balanced_market_count}")
    lines.append(f"- Balanced panel rows: {len(balanced_rows)}")
    lines.append(f"- Balanced de-clustered rows: {len(declustered_rows)}")
    lines.append(f"- Max target error hours allowed: {MAX_TARGET_ERROR_HOURS}")
    lines.append("")
    lines.append("## Brier summary")
    lines.append("")

    for sample_type in ["balanced_raw", "balanced_declustered_family_bin"]:
        lines.append(f"### {sample_type}")

        rows = [
            row for row in summary_rows
            if row["sample_type"] == sample_type
            and row["venue"] == "all"
        ]

        for row in rows:
            lines.append(
                f"- {row['horizon_hours']}h: "
                f"n={row['n']}, "
                f"markets={row['markets']}, "
                f"families={row['families']}, "
                f"Brier={float(row['brier_score']):.6f}, "
                f"mean_p={float(row['mean_p_hat']):.6f}, "
                f"empirical={float(row['empirical_rate']):.6f}, "
                f"empirical_minus_p={float(row['empirical_minus_p']):.6f}"
            )

        lines.append("")

    lines.append("## Calibration bins after balanced-panel de-clustering")
    lines.append("")

    for horizon in HORIZONS:
        lines.append(f"### {horizon}h")

        rows = [
            row for row in calibration_rows
            if row["sample_type"] == "balanced_declustered_family_bin"
            and row["venue"] == "all"
            and row["horizon_hours"] == horizon
        ]

        for row in rows:
            if int(row["n"]) == 0:
                lines.append(f"- [{row['bin_low']}, {row['bin_high']}): n=0")
            else:
                lines.append(
                    f"- [{row['bin_low']}, {row['bin_high']}): "
                    f"n={row['n']}, "
                    f"mean_p={float(row['mean_p_hat']):.4f}, "
                    f"empirical={float(row['empirical_rate']):.4f}, "
                    f"empirical_minus_p={float(row['empirical_minus_p']):.4f}"
                )

        lines.append("")

    lines.append("## Interpretation guide")
    lines.append("")
    lines.append("- This is a stricter comparison than the previous horizon robustness report.")
    lines.append("- It uses the same markets across all horizons.")
    lines.append("- The sample is smaller, especially for Kalshi, so bin-level results may be noisier.")
    lines.append("- If the 10-20% longshot bin is still negative here, that supports robustness.")
    lines.append("")
    lines.append("## Files")
    lines.append("")
    lines.append(f"- Balanced panel data: `{BALANCED_PATH}`")
    lines.append(f"- Balanced de-clustered data: `{DECLUSTERED_PATH}`")
    lines.append(f"- Summary: `{SUMMARY_PATH}`")
    lines.append(f"- Calibration bins: `{CALIBRATION_PATH}`")

    for path in plot_paths:
        lines.append(f"- Plot: `{path}`")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved: {REPORT_PATH}")


def main() -> None:
    print("=" * 80)
    print("Balanced horizon panel")
    print("No API calls.")

    rows = read_csv(INPUT_PATH)
    valid_rows = [add_analysis_columns(row) for row in rows if valid_row(row)]

    market_to_horizons = defaultdict(set)

    for row in valid_rows:
        market_to_horizons[row["market_id"]].add(row["horizon_hours"])

    balanced_market_ids = {
        market_id
        for market_id, horizons in market_to_horizons.items()
        if all(horizon in horizons for horizon in HORIZONS)
    }

    balanced_rows = [
        row for row in valid_rows
        if row["market_id"] in balanced_market_ids
    ]

    declustered_rows = decluster_family_bin(balanced_rows)

    write_csv(BALANCED_PATH, balanced_rows)
    write_csv(DECLUSTERED_PATH, declustered_rows)

    summary_rows = make_summary_rows(balanced_rows, declustered_rows)
    calibration_rows = (
        make_calibration_rows(balanced_rows, "balanced_raw")
        + make_calibration_rows(declustered_rows, "balanced_declustered_family_bin")
    )

    write_csv(SUMMARY_PATH, summary_rows)
    write_csv(CALIBRATION_PATH, calibration_rows)

    plot_paths = try_write_plots(calibration_rows)

    write_report(
        valid_rows_count=len(valid_rows),
        balanced_market_count=len(balanced_market_ids),
        balanced_rows=balanced_rows,
        declustered_rows=declustered_rows,
        summary_rows=summary_rows,
        calibration_rows=calibration_rows,
        plot_paths=plot_paths,
    )

    print("")
    print("=" * 80)
    print("Balanced horizon panel complete")
    print("Report:", REPORT_PATH)


if __name__ == "__main__":
    main()
