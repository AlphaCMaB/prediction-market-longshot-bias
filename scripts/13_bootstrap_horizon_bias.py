"""
13_bootstrap_horizon_bias.py

Purpose:
    Estimate statistical uncertainty for calibration bias across forecast horizons.

Main statistic:
    empirical_minus_p = empirical outcome rate - mean market probability

Interpretation:
    empirical_minus_p < 0 means the market probability was too high.
        Example: p_hat around 14%, but outcomes happen only 4% of the time.
        This is longshot overpricing.

    empirical_minus_p > 0 means the market probability was too low.
        Example: p_hat around 75%, but outcomes happen 90% of the time.
        This is favorite underpricing.

Input:
    data/processed/p_hat_horizons_declustered_family_bin.csv

Outputs:
    outputs/bootstrap_bias_summary.csv
    outputs/bootstrap_horizon_bias_report.md
    outputs/bootstrap_bias_10_20_by_horizon.png, if matplotlib is installed

Important:
    This script does NOT call APIs.
    It uses the already de-clustered horizon dataset.
"""

from __future__ import annotations

import csv
import math
import random
from collections import defaultdict
from pathlib import Path
from typing import Any


INPUT_PATH = Path("data/processed/p_hat_horizons_declustered_family_bin.csv")
OUTPUTS_DIR = Path("outputs")
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

SUMMARY_PATH = OUTPUTS_DIR / "bootstrap_bias_summary.csv"
REPORT_PATH = OUTPUTS_DIR / "bootstrap_horizon_bias_report.md"
PLOT_PATH = OUTPUTS_DIR / "bootstrap_bias_10_20_by_horizon.png"

RANDOM_SEED = 42
BOOTSTRAP_ITERATIONS = 2000
CONFIDENCE_LEVEL = 0.95
MIN_BIN_N_FOR_STRONG_INTERPRETATION = 20

# Probability groups to test.
# The decile bins give the full calibration picture.
# The focus groups summarize the main favorite-longshot hypothesis.
GROUPS = [
    {"group_type": "decile", "group_label": "0.0-0.1", "low": 0.0, "high": 0.1},
    {"group_type": "decile", "group_label": "0.1-0.2", "low": 0.1, "high": 0.2},
    {"group_type": "decile", "group_label": "0.2-0.3", "low": 0.2, "high": 0.3},
    {"group_type": "decile", "group_label": "0.3-0.4", "low": 0.3, "high": 0.4},
    {"group_type": "decile", "group_label": "0.4-0.5", "low": 0.4, "high": 0.5},
    {"group_type": "decile", "group_label": "0.5-0.6", "low": 0.5, "high": 0.6},
    {"group_type": "decile", "group_label": "0.6-0.7", "low": 0.6, "high": 0.7},
    {"group_type": "decile", "group_label": "0.7-0.8", "low": 0.7, "high": 0.8},
    {"group_type": "decile", "group_label": "0.8-0.9", "low": 0.8, "high": 0.9},
    {"group_type": "decile", "group_label": "0.9-1.0", "low": 0.9, "high": 1.0000001},
    {"group_type": "focus", "group_label": "longshot_10_20", "low": 0.1, "high": 0.2},
    {"group_type": "focus", "group_label": "favorite_70_90", "low": 0.7, "high": 0.9},
    {"group_type": "focus", "group_label": "favorite_50_90", "low": 0.5, "high": 0.9},
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
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Saved: {path} ({len(rows)} rows)")


def clean_rows(rows: list[dict]) -> list[dict]:
    cleaned = []

    for row in rows:
        p = to_float(row.get("p_hat"))
        y = to_int(row.get("outcome"))
        horizon = row.get("horizon_hours")

        if p is None or not (0.0 <= p <= 1.0):
            continue

        if y not in {0, 1}:
            continue

        if horizon is None or horizon == "":
            continue

        new_row = dict(row)
        new_row["p_hat_float"] = p
        new_row["outcome_int"] = y
        new_row["horizon_hours"] = str(int(float(horizon)))
        cleaned.append(new_row)

    return cleaned


def group_contains(group: dict, p: float) -> bool:
    return group["low"] <= p < group["high"]


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else float("nan")


def statistic(rows: list[dict]) -> dict:
    p_values = [row["p_hat_float"] for row in rows]
    y_values = [row["outcome_int"] for row in rows]
    brier_values = [(row["p_hat_float"] - row["outcome_int"]) ** 2 for row in rows]

    mean_p = mean(p_values)
    empirical = mean(y_values)
    brier = mean(brier_values)

    return {
        "n": len(rows),
        "mean_p_hat": mean_p,
        "empirical_rate": empirical,
        "empirical_minus_p": empirical - mean_p,
        "brier_score": brier,
    }


def percentile(sorted_values: list[float], q: float) -> float:
    if not sorted_values:
        return float("nan")

    if len(sorted_values) == 1:
        return sorted_values[0]

    position = q * (len(sorted_values) - 1)
    lower = int(math.floor(position))
    upper = int(math.ceil(position))

    if lower == upper:
        return sorted_values[lower]

    weight = position - lower
    return sorted_values[lower] * (1 - weight) + sorted_values[upper] * weight


def bootstrap_empirical_minus_p(rows: list[dict], rng: random.Random) -> dict:
    n = len(rows)

    if n == 0:
        return {
            "ci_low": "",
            "ci_high": "",
            "bootstrap_mean": "",
            "bootstrap_iterations": 0,
        }

    estimates = []

    for _ in range(BOOTSTRAP_ITERATIONS):
        sample = [rows[rng.randrange(n)] for _ in range(n)]
        estimates.append(statistic(sample)["empirical_minus_p"])

    estimates.sort()

    alpha = 1.0 - CONFIDENCE_LEVEL
    ci_low = percentile(estimates, alpha / 2.0)
    ci_high = percentile(estimates, 1.0 - alpha / 2.0)

    return {
        "ci_low": ci_low,
        "ci_high": ci_high,
        "bootstrap_mean": mean(estimates),
        "bootstrap_iterations": BOOTSTRAP_ITERATIONS,
    }


def classify_bias(empirical_minus_p: float, ci_low: float | str, ci_high: float | str, n: int) -> str:
    if n < MIN_BIN_N_FOR_STRONG_INTERPRETATION:
        sample_note = "small_sample"
    else:
        sample_note = "enough_sample"

    if ci_low == "" or ci_high == "":
        return f"inconclusive_{sample_note}"

    ci_low_f = float(ci_low)
    ci_high_f = float(ci_high)

    if ci_high_f < 0:
        return f"market_overpriced_{sample_note}"

    if ci_low_f > 0:
        return f"market_underpriced_{sample_note}"

    if empirical_minus_p < 0:
        return f"negative_but_ci_crosses_zero_{sample_note}"

    if empirical_minus_p > 0:
        return f"positive_but_ci_crosses_zero_{sample_note}"

    return f"near_zero_{sample_note}"


def make_summary_rows(rows: list[dict]) -> list[dict]:
    rng = random.Random(RANDOM_SEED)
    output = []

    horizons = sorted(set(row["horizon_hours"] for row in rows), key=lambda x: int(x))

    for horizon in horizons:
        horizon_rows = [row for row in rows if row["horizon_hours"] == horizon]

        for group in GROUPS:
            subset = [row for row in horizon_rows if group_contains(group, row["p_hat_float"])]
            observed = statistic(subset) if subset else {
                "n": 0,
                "mean_p_hat": "",
                "empirical_rate": "",
                "empirical_minus_p": "",
                "brier_score": "",
            }

            boot = bootstrap_empirical_minus_p(subset, rng)

            if observed["n"]:
                bias_classification = classify_bias(
                    empirical_minus_p=float(observed["empirical_minus_p"]),
                    ci_low=boot["ci_low"],
                    ci_high=boot["ci_high"],
                    n=int(observed["n"]),
                )
            else:
                bias_classification = "empty_group"

            output.append(
                {
                    "horizon_hours": horizon,
                    "group_type": group["group_type"],
                    "group_label": group["group_label"],
                    "bin_low": group["low"],
                    "bin_high": group["high"],
                    "n": observed["n"],
                    "mean_p_hat": observed["mean_p_hat"],
                    "empirical_rate": observed["empirical_rate"],
                    "empirical_minus_p": observed["empirical_minus_p"],
                    "brier_score": observed["brier_score"],
                    "ci_low": boot["ci_low"],
                    "ci_high": boot["ci_high"],
                    "bootstrap_mean": boot["bootstrap_mean"],
                    "bootstrap_iterations": boot["bootstrap_iterations"],
                    "bias_classification": bias_classification,
                }
            )

    return output


def format_float(value: Any, digits: int = 4) -> str:
    if value == "" or value is None:
        return ""

    try:
        return f"{float(value):.{digits}f}"
    except Exception:
        return str(value)


def try_write_focus_plot(summary_rows: list[dict]) -> Path | None:
    try:
        import matplotlib.pyplot as plt
    except Exception:
        print("matplotlib not installed; skipping plot.")
        return None

    rows = [
        row for row in summary_rows
        if row["group_type"] == "focus"
        and row["group_label"] == "longshot_10_20"
        and int(row["n"] or 0) > 0
    ]

    if not rows:
        return None

    rows = sorted(rows, key=lambda row: int(row["horizon_hours"]))

    xs = [str(row["horizon_hours"]) + "h" for row in rows]
    ys = [float(row["empirical_minus_p"]) for row in rows]
    lower_errors = [float(row["empirical_minus_p"]) - float(row["ci_low"]) for row in rows]
    upper_errors = [float(row["ci_high"]) - float(row["empirical_minus_p"]) for row in rows]

    plt.figure(figsize=(7, 4))
    plt.errorbar(xs, ys, yerr=[lower_errors, upper_errors], fmt="o", capsize=5)
    plt.axhline(0, linestyle="--")
    plt.xlabel("Forecast horizon")
    plt.ylabel("Empirical rate - mean p_hat")
    plt.title("Bootstrap CI for 10-20% longshot bias")
    plt.tight_layout()
    plt.savefig(PLOT_PATH, dpi=200)
    plt.close()

    print(f"Saved: {PLOT_PATH}")
    return PLOT_PATH


def write_report(input_rows: list[dict], cleaned_rows: list[dict], summary_rows: list[dict], plot_path: Path | None) -> None:
    lines = []
    lines.append("# Bootstrap Horizon Bias Report")
    lines.append("")
    lines.append("This report estimates uncertainty around calibration bias using bootstrap resampling.")
    lines.append("")
    lines.append("Statistic: `empirical_minus_p = empirical outcome rate - mean market probability`.")
    lines.append("")
    lines.append("- Negative values mean the market probability was too high, which is overpricing.")
    lines.append("- Positive values mean the market probability was too low, which is underpricing.")
    lines.append("")
    lines.append("## Setup")
    lines.append("")
    lines.append(f"- Input rows: {len(input_rows)}")
    lines.append(f"- Clean usable rows: {len(cleaned_rows)}")
    lines.append(f"- Bootstrap iterations: {BOOTSTRAP_ITERATIONS}")
    lines.append(f"- Confidence level: {int(CONFIDENCE_LEVEL * 100)}%")
    lines.append(f"- Minimum n for strong interpretation: {MIN_BIN_N_FOR_STRONG_INTERPRETATION}")
    lines.append("")

    lines.append("## Focus test: 10-20% longshots")
    lines.append("")

    focus_rows = [
        row for row in summary_rows
        if row["group_type"] == "focus" and row["group_label"] == "longshot_10_20"
    ]
    focus_rows = sorted(focus_rows, key=lambda row: int(row["horizon_hours"]))

    for row in focus_rows:
        lines.append(
            f"- {row['horizon_hours']}h: "
            f"n={row['n']}, "
            f"mean_p={format_float(row['mean_p_hat'])}, "
            f"empirical={format_float(row['empirical_rate'])}, "
            f"empirical_minus_p={format_float(row['empirical_minus_p'])}, "
            f"95% CI=[{format_float(row['ci_low'])}, {format_float(row['ci_high'])}], "
            f"classification={row['bias_classification']}"
        )

    lines.append("")
    lines.append("## Focus test: 70-90% favorites")
    lines.append("")

    favorite_rows = [
        row for row in summary_rows
        if row["group_type"] == "focus" and row["group_label"] == "favorite_70_90"
    ]
    favorite_rows = sorted(favorite_rows, key=lambda row: int(row["horizon_hours"]))

    for row in favorite_rows:
        lines.append(
            f"- {row['horizon_hours']}h: "
            f"n={row['n']}, "
            f"mean_p={format_float(row['mean_p_hat'])}, "
            f"empirical={format_float(row['empirical_rate'])}, "
            f"empirical_minus_p={format_float(row['empirical_minus_p'])}, "
            f"95% CI=[{format_float(row['ci_low'])}, {format_float(row['ci_high'])}], "
            f"classification={row['bias_classification']}"
        )

    lines.append("")
    lines.append("## Decile bins")
    lines.append("")

    decile_rows = [row for row in summary_rows if row["group_type"] == "decile"]
    horizons = sorted(set(row["horizon_hours"] for row in decile_rows), key=lambda x: int(x))

    for horizon in horizons:
        lines.append(f"### {horizon}h")
        rows_for_horizon = [row for row in decile_rows if row["horizon_hours"] == horizon]

        for row in rows_for_horizon:
            lines.append(
                f"- {row['group_label']}: "
                f"n={row['n']}, "
                f"mean_p={format_float(row['mean_p_hat'])}, "
                f"empirical={format_float(row['empirical_rate'])}, "
                f"empirical_minus_p={format_float(row['empirical_minus_p'])}, "
                f"95% CI=[{format_float(row['ci_low'])}, {format_float(row['ci_high'])}], "
                f"classification={row['bias_classification']}"
            )

        lines.append("")

    lines.append("## Interpretation guide")
    lines.append("")
    lines.append("- `market_overpriced` means the 95% bootstrap interval is entirely below zero.")
    lines.append("- `market_underpriced` means the 95% bootstrap interval is entirely above zero.")
    lines.append("- `ci_crosses_zero` means the observed direction exists, but uncertainty is still large.")
    lines.append("- Treat small-sample bins cautiously even if the sign looks strong.")
    lines.append("")
    lines.append("## Files")
    lines.append("")
    lines.append(f"- Bootstrap summary: `{SUMMARY_PATH}`")

    if plot_path is not None:
        lines.append(f"- 10-20% longshot plot: `{plot_path}`")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved: {REPORT_PATH}")


def main() -> None:
    print("=" * 80)
    print("Bootstrap horizon bias")
    print("No API calls.")

    input_rows = read_csv(INPUT_PATH)
    cleaned_rows = clean_rows(input_rows)

    print("Input rows:", len(input_rows))
    print("Clean usable rows:", len(cleaned_rows))
    print("Bootstrap iterations:", BOOTSTRAP_ITERATIONS)

    summary_rows = make_summary_rows(cleaned_rows)
    write_csv(SUMMARY_PATH, summary_rows)

    plot_path = try_write_focus_plot(summary_rows)
    write_report(input_rows, cleaned_rows, summary_rows, plot_path)

    print("")
    print("=" * 80)
    print("Bootstrap horizon bias complete")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
