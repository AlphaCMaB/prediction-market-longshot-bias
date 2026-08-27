"""
06_compute_brier_calibration_batch.py

Purpose:
    Compute pilot Brier score and calibration bins from extracted p_hat data.

Important:
    This script uses real p_hat values extracted from real price-history APIs.
    This script does NOT pull new API data.
    This script produces a pilot result, not the final research conclusion.

Inputs:
    data/processed/p_hat_batch.csv

Outputs:
    outputs/brier_summary_batch.csv
    outputs/calibration_bins_batch.csv
    outputs/brier_calibration_batch_report.md
    outputs/calibration_curve_batch.png  if matplotlib is installed
"""

from __future__ import annotations

import csv
import math
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


INPUT_PATH = Path("data/processed/p_hat_batch.csv")

OUTPUTS_DIR = Path("outputs")
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

BRIER_SUMMARY_PATH = OUTPUTS_DIR / "brier_summary_batch.csv"
CALIBRATION_BINS_PATH = OUTPUTS_DIR / "calibration_bins_batch.csv"
REPORT_PATH = OUTPUTS_DIR / "brier_calibration_batch_report.md"
PLOT_PATH = OUTPUTS_DIR / "calibration_curve_batch.png"

# Because price history is hourly, we allow extracted prices within 2 hours
# of the exact 48-hour target.
MAX_TARGET_ERROR_HOURS = 2.0

# Decile bins for calibration curve.
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


def read_p_hat_rows(path: Path) -> list[dict]:
    if not path.exists():
        raise FileNotFoundError(f"Missing input file: {path}")

    with path.open("r", encoding="utf-8") as f:
        return list(csv.DictReader(f))


def clean_analysis_rows(rows: list[dict]) -> tuple[list[dict], Counter]:
    """
    Keep only rows that are safe for pilot Brier calculation.
    """
    clean = []
    drop_reasons = Counter()

    for row in rows:
        if row.get("status") != "ok":
            drop_reasons["status_not_ok"] += 1
            continue

        p_hat = to_float(row.get("p_hat"))
        outcome = to_int(row.get("outcome"))
        target_error_hours = to_float(row.get("target_error_hours"))

        if p_hat is None or not (0.0 <= p_hat <= 1.0):
            drop_reasons["invalid_p_hat"] += 1
            continue

        if outcome not in {0, 1}:
            drop_reasons["invalid_outcome"] += 1
            continue

        if target_error_hours is None:
            drop_reasons["missing_target_error"] += 1
            continue

        if target_error_hours > MAX_TARGET_ERROR_HOURS:
            drop_reasons["too_far_from_48h_target"] += 1
            continue

        row = dict(row)
        row["p_hat_float"] = p_hat
        row["outcome_int"] = outcome
        row["brier"] = (p_hat - outcome) ** 2

        clean.append(row)

    return clean, drop_reasons


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else float("nan")


def summarize_group(rows: list[dict], group_name: str, group_value: str) -> dict:
    p_values = [row["p_hat_float"] for row in rows]
    outcomes = [row["outcome_int"] for row in rows]
    briers = [row["brier"] for row in rows]

    base_rate = mean(outcomes)

    # Baseline: what Brier score would be if we predicted the same base rate
    # for every market in this group?
    baseline_brier = mean([(base_rate - y) ** 2 for y in outcomes])

    return {
        "group_name": group_name,
        "group_value": group_value,
        "n": len(rows),
        "mean_p_hat": mean(p_values),
        "empirical_rate": base_rate,
        "brier_score": mean(briers),
        "baseline_base_rate_brier": baseline_brier,
        "mean_error_empirical_minus_p": base_rate - mean(p_values),
    }


def write_brier_summary(rows: list[dict]) -> list[dict]:
    summary_rows = []

    summary_rows.append(summarize_group(rows, "all", "all"))

    venues = sorted(set(row["venue"] for row in rows))

    for venue in venues:
        venue_rows = [row for row in rows if row["venue"] == venue]
        summary_rows.append(summarize_group(venue_rows, "venue", venue))

    with BRIER_SUMMARY_PATH.open("w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "group_name",
            "group_value",
            "n",
            "mean_p_hat",
            "empirical_rate",
            "brier_score",
            "baseline_base_rate_brier",
            "mean_error_empirical_minus_p",
        ]

        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(summary_rows)

    print(f"Saved: {BRIER_SUMMARY_PATH}")
    return summary_rows


def bin_for_p(p: float) -> tuple[float, float] | None:
    for low, high in BINS:
        if low <= p < high:
            return low, high

    return None


def write_calibration_bins(rows: list[dict]) -> list[dict]:
    grouped = defaultdict(list)

    for row in rows:
        b = bin_for_p(row["p_hat_float"])

        if b is None:
            continue

        grouped[b].append(row)

    output_rows = []

    for low, high in BINS:
        bin_rows = grouped.get((low, high), [])

        if not bin_rows:
            output_rows.append(
                {
                    "bin_low": low,
                    "bin_high": high,
                    "n": 0,
                    "mean_p_hat": "",
                    "empirical_rate": "",
                    "brier_score": "",
                    "empirical_minus_p": "",
                }
            )
            continue

        mean_p = mean([row["p_hat_float"] for row in bin_rows])
        empirical_rate = mean([row["outcome_int"] for row in bin_rows])
        brier_score = mean([row["brier"] for row in bin_rows])

        output_rows.append(
            {
                "bin_low": low,
                "bin_high": high,
                "n": len(bin_rows),
                "mean_p_hat": mean_p,
                "empirical_rate": empirical_rate,
                "brier_score": brier_score,
                "empirical_minus_p": empirical_rate - mean_p,
            }
        )

    with CALIBRATION_BINS_PATH.open("w", newline="", encoding="utf-8") as f:
        fieldnames = [
            "bin_low",
            "bin_high",
            "n",
            "mean_p_hat",
            "empirical_rate",
            "brier_score",
            "empirical_minus_p",
        ]

        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"Saved: {CALIBRATION_BINS_PATH}")
    return output_rows


def try_write_plot(calibration_rows: list[dict]) -> bool:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        print("matplotlib not installed; skipping calibration plot.")
        return False

    xs = []
    ys = []
    sizes = []

    for row in calibration_rows:
        if not row["mean_p_hat"] or not row["empirical_rate"]:
            continue

        xs.append(float(row["mean_p_hat"]))
        ys.append(float(row["empirical_rate"]))
        sizes.append(max(20, int(row["n"]) * 2))

    if not xs:
        print("No non-empty calibration bins to plot.")
        return False

    plt.figure(figsize=(6, 6))
    plt.scatter(xs, ys, s=sizes, alpha=0.7)
    plt.plot([0, 1], [0, 1], linestyle="--")
    plt.xlabel("Mean market probability p_hat")
    plt.ylabel("Empirical outcome rate")
    plt.title("Pilot calibration curve")
    plt.xlim(0, 1)
    plt.ylim(0, 1)
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(PLOT_PATH, dpi=200)
    plt.close()

    print(f"Saved: {PLOT_PATH}")
    return True


def write_report(
    input_count: int,
    analysis_rows: list[dict],
    drop_reasons: Counter,
    summary_rows: list[dict],
    calibration_rows: list[dict],
    plot_saved: bool,
) -> None:
    venue_counts = Counter(row["venue"] for row in analysis_rows)
    outcome_counts = Counter(row["outcome_int"] for row in analysis_rows)

    lines = []
    lines.append("# Pilot Brier Score and Calibration Report")
    lines.append("")
    lines.append("This is the first pilot Brier/calibration result from real API data.")
    lines.append("")
    lines.append("Important caveat: this is not the final research conclusion.")
    lines.append("The current batch is heavily Polymarket-weighted and may contain clustered related markets.")
    lines.append("")
    lines.append("## Filtering")
    lines.append("")
    lines.append(f"- Input rows: {input_count}")
    lines.append(f"- Analysis rows used: {len(analysis_rows)}")
    lines.append(f"- Max target error hours allowed: {MAX_TARGET_ERROR_HOURS}")
    lines.append("")
    lines.append("## Drop reasons")
    lines.append("")

    if not drop_reasons:
        lines.append("- None")
    else:
        for reason, count in sorted(drop_reasons.items()):
            lines.append(f"- {reason}: {count}")

    lines.append("")
    lines.append("## Rows by venue")
    lines.append("")

    for venue, count in sorted(venue_counts.items()):
        lines.append(f"- {venue}: {count}")

    lines.append("")
    lines.append("## Outcomes")
    lines.append("")
    lines.append(f"- outcome = 1: {outcome_counts.get(1, 0)}")
    lines.append(f"- outcome = 0: {outcome_counts.get(0, 0)}")
    lines.append("")
    lines.append("## Brier summary")
    lines.append("")

    for row in summary_rows:
        lines.append(
            f"- {row['group_name']}={row['group_value']}: "
            f"n={row['n']}, "
            f"Brier={float(row['brier_score']):.6f}, "
            f"mean_p_hat={float(row['mean_p_hat']):.6f}, "
            f"empirical_rate={float(row['empirical_rate']):.6f}, "
            f"empirical_minus_p={float(row['mean_error_empirical_minus_p']):.6f}"
        )

    lines.append("")
    lines.append("## Calibration bins")
    lines.append("")

    for row in calibration_rows:
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
    lines.append("## Files")
    lines.append("")
    lines.append(f"- Brier summary: `{BRIER_SUMMARY_PATH}`")
    lines.append(f"- Calibration bins: `{CALIBRATION_BINS_PATH}`")
    if plot_saved:
        lines.append(f"- Calibration plot: `{PLOT_PATH}`")
    else:
        lines.append("- Calibration plot: not created")
    lines.append("")
    lines.append("## Next step")
    lines.append("")
    lines.append("Next, improve the dataset before drawing conclusions:")
    lines.append("")
    lines.append("1. Pull a larger Polymarket batch.")
    lines.append("2. Improve Kalshi selection by excluding short-lived markets that were not open 48 hours before resolution.")
    lines.append("3. Add robustness checks at 24h and 7d horizons.")
    lines.append("4. De-cluster related markets so one event family does not dominate.")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved: {REPORT_PATH}")


def main() -> None:
    print("=" * 80)
    print("Compute pilot Brier score and calibration")
    print("Using extracted p_hat values. No API calls.")

    rows = read_p_hat_rows(INPUT_PATH)
    analysis_rows, drop_reasons = clean_analysis_rows(rows)

    print("Input rows:", len(rows))
    print("Analysis rows:", len(analysis_rows))
    print("Drop reasons:", dict(drop_reasons))

    if not analysis_rows:
        raise RuntimeError("No usable rows after filtering.")

    summary_rows = write_brier_summary(analysis_rows)
    calibration_rows = write_calibration_bins(analysis_rows)
    plot_saved = try_write_plot(calibration_rows)

    write_report(
        input_count=len(rows),
        analysis_rows=analysis_rows,
        drop_reasons=drop_reasons,
        summary_rows=summary_rows,
        calibration_rows=calibration_rows,
        plot_saved=plot_saved,
    )

    print("\n" + "=" * 80)
    print("Pilot Brier/calibration complete")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
