"""
15_liquidity_spread_robustness.py

Purpose:
    Check whether the observed favorite-longshot pattern survives simple
    liquidity / spread filters.

Why:
    Some Kalshi p_hat values are computed from bid-ask midpoints. If the bid-ask
    spread is very wide, the midpoint may be a noisy probability estimate.
    This script checks whether the calibration pattern still appears after
    excluding wide-spread Kalshi observations.

Input:
    data/processed/p_hat_horizons_declustered_family_bin.csv

Outputs:
    outputs/liquidity_robustness_summary.csv
    outputs/liquidity_robustness_bins.csv
    outputs/liquidity_robustness_focus_bootstrap.csv
    outputs/liquidity_robustness_report.md

Important:
    This script does NOT call APIs.
"""

from __future__ import annotations

import csv
import math
import random
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any, Callable


INPUT_PATH = Path("data/processed/p_hat_horizons_declustered_family_bin.csv")

OUTPUTS_DIR = Path("outputs")
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

SUMMARY_PATH = OUTPUTS_DIR / "liquidity_robustness_summary.csv"
BINS_PATH = OUTPUTS_DIR / "liquidity_robustness_bins.csv"
BOOTSTRAP_PATH = OUTPUTS_DIR / "liquidity_robustness_focus_bootstrap.csv"
REPORT_PATH = OUTPUTS_DIR / "liquidity_robustness_report.md"

HORIZONS = ["24", "48", "168"]
BOOTSTRAP_ITERATIONS = 2000
RANDOM_SEED = 20260702
MIN_N_FOR_STRONG_INTERPRETATION = 20

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

FOCUS_GROUPS = [
    ("longshot_10_20", 0.1, 0.2),
    ("favorite_70_90", 0.7, 0.9),
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


def valid_row(row: dict) -> bool:
    p = to_float(row.get("p_hat"))
    y = to_int(row.get("outcome"))

    if row.get("status") not in {"ok", ""}:
        return False

    if p is None or not (0.0 <= p <= 1.0):
        return False

    if y not in {0, 1}:
        return False

    return True


def kalshi_spread(row: dict) -> float | None:
    if row.get("venue") != "kalshi":
        return None

    return to_float(row.get("spread"))


def sample_all(row: dict) -> bool:
    return True


def sample_polymarket_only(row: dict) -> bool:
    return row.get("venue") == "polymarket"


def sample_kalshi_only(row: dict) -> bool:
    return row.get("venue") == "kalshi"


def sample_keep_poly_kalshi_spread_le_20(row: dict) -> bool:
    if row.get("venue") != "kalshi":
        return True

    spread = kalshi_spread(row)

    return spread is not None and spread <= 0.20


def sample_keep_poly_kalshi_spread_le_10(row: dict) -> bool:
    if row.get("venue") != "kalshi":
        return True

    spread = kalshi_spread(row)

    return spread is not None and spread <= 0.10


SAMPLE_FILTERS: list[tuple[str, Callable[[dict], bool]]] = [
    ("all_declustered", sample_all),
    ("polymarket_only", sample_polymarket_only),
    ("kalshi_only", sample_kalshi_only),
    ("all_keep_poly_kalshi_spread_le_0_20", sample_keep_poly_kalshi_spread_le_20),
    ("all_keep_poly_kalshi_spread_le_0_10", sample_keep_poly_kalshi_spread_le_10),
]


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else float("nan")


def metric_empirical_minus_p(rows: list[dict]) -> float:
    if not rows:
        return float("nan")

    values = []

    for row in rows:
        p = to_float(row.get("p_hat"))
        y = to_int(row.get("outcome"))

        if p is not None and y in {0, 1}:
            values.append(y - p)

    return mean(values)


def mean_p_hat(rows: list[dict]) -> float:
    values = []

    for row in rows:
        p = to_float(row.get("p_hat"))
        if p is not None:
            values.append(p)

    return mean(values)


def empirical_rate(rows: list[dict]) -> float:
    values = []

    for row in rows:
        y = to_int(row.get("outcome"))
        if y in {0, 1}:
            values.append(y)

    return mean(values)


def brier_score(rows: list[dict]) -> float:
    values = []

    for row in rows:
        p = to_float(row.get("p_hat"))
        y = to_int(row.get("outcome"))

        if p is not None and y in {0, 1}:
            values.append((p - y) ** 2)

    return mean(values)


def bin_for_p(p: float) -> tuple[float, float] | None:
    for low, high in BINS:
        if low <= p < high:
            return low, high

    return None


def rows_in_probability_range(rows: list[dict], low: float, high: float) -> list[dict]:
    out = []

    for row in rows:
        p = to_float(row.get("p_hat"))

        if p is not None and low <= p < high:
            out.append(row)

    return out


def classify(n: int, ci_low: float, ci_high: float, observed: float) -> str:
    sample_suffix = "enough_sample" if n >= MIN_N_FOR_STRONG_INTERPRETATION else "small_sample"

    if ci_high < 0:
        return f"market_overpriced_{sample_suffix}"

    if ci_low > 0:
        return f"market_underpriced_{sample_suffix}"

    if observed < 0:
        return f"negative_but_ci_crosses_zero_{sample_suffix}"

    if observed > 0:
        return f"positive_but_ci_crosses_zero_{sample_suffix}"

    return f"near_zero_{sample_suffix}"


def bootstrap_ci(rows: list[dict], rng: random.Random) -> tuple[float, float, float]:
    n = len(rows)

    if n == 0:
        return float("nan"), float("nan"), float("nan")

    observed = metric_empirical_minus_p(rows)

    draws = []

    for _ in range(BOOTSTRAP_ITERATIONS):
        sample = [rows[rng.randrange(n)] for _ in range(n)]
        draws.append(metric_empirical_minus_p(sample))

    draws.sort()

    low_index = int(0.025 * (BOOTSTRAP_ITERATIONS - 1))
    high_index = int(0.975 * (BOOTSTRAP_ITERATIONS - 1))

    return observed, draws[low_index], draws[high_index]


def make_summary_rows(rows: list[dict]) -> list[dict]:
    output = []

    for sample_name, keep in SAMPLE_FILTERS:
        sample_rows = [row for row in rows if keep(row)]

        for horizon in HORIZONS:
            subset = [row for row in sample_rows if row.get("horizon_hours") == horizon]

            if not subset:
                continue

            output.append(
                {
                    "sample_name": sample_name,
                    "horizon_hours": horizon,
                    "n": len(subset),
                    "polymarket_n": sum(row.get("venue") == "polymarket" for row in subset),
                    "kalshi_n": sum(row.get("venue") == "kalshi" for row in subset),
                    "mean_p_hat": mean_p_hat(subset),
                    "empirical_rate": empirical_rate(subset),
                    "empirical_minus_p": metric_empirical_minus_p(subset),
                    "brier_score": brier_score(subset),
                    "median_kalshi_spread": median_spread(subset),
                }
            )

    return output


def median_spread(rows: list[dict]) -> str | float:
    spreads = []

    for row in rows:
        spread = kalshi_spread(row)
        if spread is not None:
            spreads.append(spread)

    if not spreads:
        return ""

    spreads.sort()
    n = len(spreads)

    if n % 2 == 1:
        return spreads[n // 2]

    return (spreads[n // 2 - 1] + spreads[n // 2]) / 2


def make_bin_rows(rows: list[dict]) -> list[dict]:
    output = []

    for sample_name, keep in SAMPLE_FILTERS:
        sample_rows = [row for row in rows if keep(row)]

        for horizon in HORIZONS:
            horizon_rows = [row for row in sample_rows if row.get("horizon_hours") == horizon]
            grouped = defaultdict(list)

            for row in horizon_rows:
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
                            "sample_name": sample_name,
                            "horizon_hours": horizon,
                            "bin_low": low,
                            "bin_high": high,
                            "n": 0,
                            "polymarket_n": 0,
                            "kalshi_n": 0,
                            "mean_p_hat": "",
                            "empirical_rate": "",
                            "empirical_minus_p": "",
                            "brier_score": "",
                            "median_kalshi_spread": "",
                        }
                    )
                    continue

                output.append(
                    {
                        "sample_name": sample_name,
                        "horizon_hours": horizon,
                        "bin_low": low,
                        "bin_high": high,
                        "n": len(bin_rows),
                        "polymarket_n": sum(row.get("venue") == "polymarket" for row in bin_rows),
                        "kalshi_n": sum(row.get("venue") == "kalshi" for row in bin_rows),
                        "mean_p_hat": mean_p_hat(bin_rows),
                        "empirical_rate": empirical_rate(bin_rows),
                        "empirical_minus_p": metric_empirical_minus_p(bin_rows),
                        "brier_score": brier_score(bin_rows),
                        "median_kalshi_spread": median_spread(bin_rows),
                    }
                )

    return output


def make_focus_bootstrap_rows(rows: list[dict]) -> list[dict]:
    output = []
    rng = random.Random(RANDOM_SEED)

    for sample_name, keep in SAMPLE_FILTERS:
        sample_rows = [row for row in rows if keep(row)]

        for horizon in HORIZONS:
            horizon_rows = [row for row in sample_rows if row.get("horizon_hours") == horizon]

            for group_name, low, high in FOCUS_GROUPS:
                focus_rows = rows_in_probability_range(horizon_rows, low, high)
                observed, ci_low, ci_high = bootstrap_ci(focus_rows, rng)

                output.append(
                    {
                        "sample_name": sample_name,
                        "horizon_hours": horizon,
                        "focus_group": group_name,
                        "bin_low": low,
                        "bin_high": high,
                        "n": len(focus_rows),
                        "polymarket_n": sum(row.get("venue") == "polymarket" for row in focus_rows),
                        "kalshi_n": sum(row.get("venue") == "kalshi" for row in focus_rows),
                        "mean_p_hat": mean_p_hat(focus_rows),
                        "empirical_rate": empirical_rate(focus_rows),
                        "empirical_minus_p": observed,
                        "ci_low": ci_low,
                        "ci_high": ci_high,
                        "classification": classify(len(focus_rows), ci_low, ci_high, observed),
                        "median_kalshi_spread": median_spread(focus_rows),
                    }
                )

    return output


def fmt_float(value: Any, digits: int = 4) -> str:
    x = to_float(value)

    if x is None:
        return ""

    return f"{x:.{digits}f}"


def write_report(
    input_count: int,
    clean_count: int,
    sample_sizes: dict[str, int],
    summary_rows: list[dict],
    bin_rows: list[dict],
    focus_rows: list[dict],
) -> None:
    lines = []

    lines.append("# Liquidity / Spread Robustness Report")
    lines.append("")
    lines.append("This report checks whether the calibration pattern survives simple liquidity filters.")
    lines.append("")
    lines.append("For Kalshi, p_hat is often a bid-ask midpoint. Wide spreads can make the midpoint noisy.")
    lines.append("Polymarket rows are kept in the spread-filtered samples because this dataset does not contain comparable order-book spreads for Polymarket.")
    lines.append("")
    lines.append("## Setup")
    lines.append("")
    lines.append(f"- Input rows: {input_count}")
    lines.append(f"- Clean usable rows: {clean_count}")
    lines.append(f"- Bootstrap iterations: {BOOTSTRAP_ITERATIONS}")
    lines.append(f"- Random seed: {RANDOM_SEED}")
    lines.append(f"- Minimum n for strong interpretation: {MIN_N_FOR_STRONG_INTERPRETATION}")
    lines.append("")
    lines.append("## Sample sizes")
    lines.append("")

    for sample_name, size in sample_sizes.items():
        lines.append(f"- {sample_name}: {size}")

    lines.append("")
    lines.append("## Focus test: 10-20% longshots")
    lines.append("")

    for sample_name in [name for name, _ in SAMPLE_FILTERS]:
        lines.append(f"### {sample_name}")

        rows = [
            row for row in focus_rows
            if row["sample_name"] == sample_name
            and row["focus_group"] == "longshot_10_20"
        ]

        for row in rows:
            lines.append(
                f"- {row['horizon_hours']}h: "
                f"n={row['n']}, "
                f"poly={row['polymarket_n']}, "
                f"kalshi={row['kalshi_n']}, "
                f"mean_p={fmt_float(row['mean_p_hat'])}, "
                f"empirical={fmt_float(row['empirical_rate'])}, "
                f"empirical_minus_p={fmt_float(row['empirical_minus_p'])}, "
                f"95% CI=[{fmt_float(row['ci_low'])}, {fmt_float(row['ci_high'])}], "
                f"classification={row['classification']}"
            )

        lines.append("")

    lines.append("## Focus test: 70-90% favorites")
    lines.append("")

    for sample_name in [name for name, _ in SAMPLE_FILTERS]:
        lines.append(f"### {sample_name}")

        rows = [
            row for row in focus_rows
            if row["sample_name"] == sample_name
            and row["focus_group"] == "favorite_70_90"
        ]

        for row in rows:
            lines.append(
                f"- {row['horizon_hours']}h: "
                f"n={row['n']}, "
                f"poly={row['polymarket_n']}, "
                f"kalshi={row['kalshi_n']}, "
                f"mean_p={fmt_float(row['mean_p_hat'])}, "
                f"empirical={fmt_float(row['empirical_rate'])}, "
                f"empirical_minus_p={fmt_float(row['empirical_minus_p'])}, "
                f"95% CI=[{fmt_float(row['ci_low'])}, {fmt_float(row['ci_high'])}], "
                f"classification={row['classification']}"
            )

        lines.append("")

    lines.append("## Decile calibration under spread filters")
    lines.append("")

    key_samples = [
        "all_declustered",
        "all_keep_poly_kalshi_spread_le_0_20",
        "all_keep_poly_kalshi_spread_le_0_10",
    ]

    for sample_name in key_samples:
        lines.append(f"### {sample_name}")

        for horizon in HORIZONS:
            lines.append(f"#### {horizon}h")

            rows = [
                row for row in bin_rows
                if row["sample_name"] == sample_name
                and row["horizon_hours"] == horizon
            ]

            for row in rows:
                if int(row["n"]) == 0:
                    lines.append(f"- [{row['bin_low']}, {row['bin_high']}): n=0")
                else:
                    lines.append(
                        f"- [{row['bin_low']}, {row['bin_high']}): "
                        f"n={row['n']}, "
                        f"poly={row['polymarket_n']}, "
                        f"kalshi={row['kalshi_n']}, "
                        f"mean_p={fmt_float(row['mean_p_hat'])}, "
                        f"empirical={fmt_float(row['empirical_rate'])}, "
                        f"empirical_minus_p={fmt_float(row['empirical_minus_p'])}"
                    )

            lines.append("")

    lines.append("## Interpretation guide")
    lines.append("")
    lines.append("- Negative empirical_minus_p means the market was overpriced.")
    lines.append("- Positive empirical_minus_p means the market was underpriced.")
    lines.append("- If 10-20% longshots remain negative after spread filtering, the result is less likely to be driven only by wide Kalshi spreads.")
    lines.append("- If the result disappears after spread filtering, liquidity may be an important confound.")
    lines.append("")
    lines.append("## Files")
    lines.append("")
    lines.append(f"- Summary: `{SUMMARY_PATH}`")
    lines.append(f"- Calibration bins: `{BINS_PATH}`")
    lines.append(f"- Focus bootstrap: `{BOOTSTRAP_PATH}`")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved: {REPORT_PATH}")


def main() -> None:
    print("=" * 80)
    print("Liquidity / spread robustness")
    print("No API calls.")

    rows = read_csv(INPUT_PATH)
    clean_rows = [row for row in rows if valid_row(row)]

    sample_sizes = {
        sample_name: sum(1 for row in clean_rows if keep(row))
        for sample_name, keep in SAMPLE_FILTERS
    }

    summary_rows = make_summary_rows(clean_rows)
    bin_rows = make_bin_rows(clean_rows)
    focus_rows = make_focus_bootstrap_rows(clean_rows)

    write_csv(SUMMARY_PATH, summary_rows)
    write_csv(BINS_PATH, bin_rows)
    write_csv(BOOTSTRAP_PATH, focus_rows)

    write_report(
        input_count=len(rows),
        clean_count=len(clean_rows),
        sample_sizes=sample_sizes,
        summary_rows=summary_rows,
        bin_rows=bin_rows,
        focus_rows=focus_rows,
    )

    print("")
    print("=" * 80)
    print("Liquidity / spread robustness complete")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
