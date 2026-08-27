"""
12_compute_horizon_robustness.py

Purpose:
    Compare Brier score and calibration across multiple forecast horizons:
        - 24h before resolution
        - 48h before resolution
        - 168h / 7d before resolution

Inputs:
    data/processed/p_hat_horizons.csv

Outputs:
    data/processed/p_hat_horizons_clustered.csv
    data/processed/p_hat_horizons_declustered_family_bin.csv
    outputs/horizon_brier_summary.csv
    outputs/horizon_calibration_bins.csv
    outputs/horizon_robustness_report.md
    outputs/horizon_calibration_<horizon>h.png, if matplotlib is installed

Important:
    This script does NOT call APIs.
    It uses p_hat values already extracted from real price-history data.
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

CLUSTERED_PATH = PROCESSED_DIR / "p_hat_horizons_clustered.csv"
DECLUSTERED_PATH = PROCESSED_DIR / "p_hat_horizons_declustered_family_bin.csv"

SUMMARY_PATH = OUTPUTS_DIR / "horizon_brier_summary.csv"
CALIBRATION_PATH = OUTPUTS_DIR / "horizon_calibration_bins.csv"
REPORT_PATH = OUTPUTS_DIR / "horizon_robustness_report.md"

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


def valid_analysis_row(row: dict) -> bool:
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


def probability_bin(p: float) -> str:
    idx = min(int(p * 10), 9)
    low = idx / 10
    high = (idx + 1) / 10
    return f"{low:.1f}-{high:.1f}"


def brier_score(rows: list[dict]) -> float:
    values = []

    for row in rows:
        p = to_float(row.get("p_hat"))
        y = to_int(row.get("outcome"))

        if p is None or y not in {0, 1}:
            continue

        values.append((p - y) ** 2)

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


def choose_best_row(rows: list[dict]) -> dict:
    def sort_key(row: dict) -> tuple:
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

    return sorted(rows, key=sort_key)[0]


def add_cluster_columns(rows: list[dict]) -> list[dict]:
    eligible = [row for row in rows if valid_analysis_row(row)]

    # Count family sizes within each horizon, because the same family may appear
    # at 24h, 48h, and 168h.
    family_counts = Counter(
        (row.get("horizon_hours"), infer_event_family(row))
        for row in eligible
    )

    output = []

    for row in rows:
        row = dict(row)

        if valid_analysis_row(row):
            p = to_float(row.get("p_hat"))
            family = infer_event_family(row)
            horizon = row.get("horizon_hours")

            row["event_family"] = family
            row["family_size_within_horizon"] = family_counts[(horizon, family)]
            row["probability_bin"] = probability_bin(p)
            row["horizon_analysis_eligible"] = "1"
        else:
            row["event_family"] = ""
            row["family_size_within_horizon"] = ""
            row["probability_bin"] = ""
            row["horizon_analysis_eligible"] = "0"

        output.append(row)

    return output


def decluster_family_bin(rows: list[dict]) -> list[dict]:
    groups = defaultdict(list)

    for row in rows:
        if row.get("horizon_analysis_eligible") != "1":
            continue

        key = (
            row.get("horizon_hours"),
            row.get("venue"),
            row.get("event_family"),
            row.get("probability_bin"),
        )
        groups[key].append(row)

    selected = [choose_best_row(group_rows) for group_rows in groups.values()]

    return sorted(
        selected,
        key=lambda r: (
            int(r["horizon_hours"]),
            r["venue"],
            r["event_family"],
            r["probability_bin"],
        ),
    )


def summarize_group(rows: list[dict], sample_type: str, horizon: str, venue: str) -> dict:
    return {
        "sample_type": sample_type,
        "horizon_hours": horizon,
        "venue": venue,
        "n": len(rows),
        "event_families": len(set(row.get("event_family", "") for row in rows)),
        "mean_p_hat": mean_p_hat(rows),
        "empirical_rate": empirical_rate(rows),
        "brier_score": brier_score(rows),
        "empirical_minus_p": empirical_rate(rows) - mean_p_hat(rows),
    }


def make_summary_rows(raw_rows: list[dict], declustered_rows: list[dict]) -> list[dict]:
    output = []

    for sample_type, rows in [
        ("raw_eligible", raw_rows),
        ("declustered_family_bin", declustered_rows),
    ]:
        horizons = sorted(set(row["horizon_hours"] for row in rows), key=lambda x: int(x))

        for horizon in horizons:
            horizon_rows = [row for row in rows if row["horizon_hours"] == horizon]
            output.append(summarize_group(horizon_rows, sample_type, horizon, "all"))

            venues = sorted(set(row["venue"] for row in horizon_rows))

            for venue in venues:
                venue_rows = [row for row in horizon_rows if row["venue"] == venue]
                output.append(summarize_group(venue_rows, sample_type, horizon, venue))

    return output


def bin_for_p(p: float) -> tuple[float, float] | None:
    for low, high in BINS:
        if low <= p < high:
            return low, high

    return None


def make_calibration_rows(rows: list[dict], sample_type: str) -> list[dict]:
    output = []

    horizons = sorted(set(row["horizon_hours"] for row in rows), key=lambda x: int(x))

    for horizon in horizons:
        horizon_rows = [row for row in rows if row["horizon_hours"] == horizon]

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
                if b is None:
                    continue

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
                            "brier_score": "",
                            "empirical_minus_p": "",
                        }
                    )
                    continue

                mp = mean_p_hat(bin_rows)
                er = empirical_rate(bin_rows)

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
                        "brier_score": brier_score(bin_rows),
                        "empirical_minus_p": er - mp,
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
        if row["sample_type"] == "declustered_family_bin"
        and row["venue"] == "all"
        and row["n"]
        and int(row["n"]) > 0
    ]

    horizons = sorted(set(row["horizon_hours"] for row in rows), key=lambda x: int(x))

    for horizon in horizons:
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

        path = OUTPUTS_DIR / f"horizon_calibration_{horizon}h.png"

        plt.figure(figsize=(6, 6))
        plt.scatter(xs, ys, s=sizes, alpha=0.7)
        plt.plot([0, 1], [0, 1], linestyle="--")
        plt.xlabel("Mean market probability p_hat")
        plt.ylabel("Empirical outcome rate")
        plt.title(f"Calibration curve: {horizon}h before resolution")
        plt.xlim(0, 1)
        plt.ylim(0, 1)
        plt.grid(True, alpha=0.3)
        plt.tight_layout()
        plt.savefig(path, dpi=200)
        plt.close()

        print(f"Saved: {path}")
        saved.append(path)

    return saved


def write_report(
    input_count: int,
    clustered_rows: list[dict],
    raw_rows: list[dict],
    declustered_rows: list[dict],
    summary_rows: list[dict],
    calibration_rows: list[dict],
    plot_paths: list[Path],
) -> None:
    drop_reasons = Counter(
        (row.get("venue"), row.get("horizon_hours"), row.get("reason"))
        for row in clustered_rows
        if row.get("horizon_analysis_eligible") != "1"
    )

    lines = []
    lines.append("# Horizon Robustness Report")
    lines.append("")
    lines.append("This report compares Brier score and calibration across forecast horizons.")
    lines.append("")
    lines.append("Horizons tested: 24h, 48h, and 168h / 7d before resolution.")
    lines.append("")
    lines.append("## Filtering")
    lines.append("")
    lines.append(f"- Input rows: {input_count}")
    lines.append(f"- Raw eligible rows: {len(raw_rows)}")
    lines.append(f"- Family-bin de-clustered rows: {len(declustered_rows)}")
    lines.append(f"- Max target error hours allowed: {MAX_TARGET_ERROR_HOURS}")
    lines.append("")
    lines.append("## Drop reasons")
    lines.append("")

    for (venue, horizon, reason), count in sorted(drop_reasons.items(), key=lambda x: (str(x[0][0]), int(x[0][1]) if str(x[0][1]).isdigit() else 999999, str(x[0][2]))):
        lines.append(f"- {venue} {horizon}h / {reason}: {count}")

    lines.append("")
    lines.append("## Brier summary")
    lines.append("")

    for sample_type in ["raw_eligible", "declustered_family_bin"]:
        lines.append(f"### {sample_type}")

        relevant = [row for row in summary_rows if row["sample_type"] == sample_type and row["venue"] == "all"]
        relevant = sorted(relevant, key=lambda r: int(r["horizon_hours"]))

        for row in relevant:
            lines.append(
                f"- {row['horizon_hours']}h: "
                f"n={row['n']}, "
                f"families={row['event_families']}, "
                f"Brier={float(row['brier_score']):.6f}, "
                f"mean_p={float(row['mean_p_hat']):.6f}, "
                f"empirical={float(row['empirical_rate']):.6f}, "
                f"empirical_minus_p={float(row['empirical_minus_p']):.6f}"
            )

        lines.append("")

    lines.append("## Venue summary after de-clustering")
    lines.append("")

    relevant = [
        row for row in summary_rows
        if row["sample_type"] == "declustered_family_bin"
        and row["venue"] != "all"
    ]
    relevant = sorted(relevant, key=lambda r: (int(r["horizon_hours"]), r["venue"]))

    for row in relevant:
        lines.append(
            f"- {row['horizon_hours']}h {row['venue']}: "
            f"n={row['n']}, "
            f"Brier={float(row['brier_score']):.6f}, "
            f"mean_p={float(row['mean_p_hat']):.6f}, "
            f"empirical={float(row['empirical_rate']):.6f}"
        )

    lines.append("")
    lines.append("## Calibration bins after de-clustering")
    lines.append("")

    for horizon in sorted(set(row["horizon_hours"] for row in declustered_rows), key=lambda x: int(x)):
        lines.append(f"### {horizon}h")

        relevant_bins = [
            row for row in calibration_rows
            if row["sample_type"] == "declustered_family_bin"
            and row["venue"] == "all"
            and row["horizon_hours"] == horizon
        ]

        for row in relevant_bins:
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
    lines.append("- If the same calibration pattern appears at 24h, 48h, and 168h, it is more robust.")
    lines.append("- If the pattern appears only at one horizon, it may be time-specific or liquidity-driven.")
    lines.append("- The 168h horizon has fewer usable markets, so interpret it more cautiously.")
    lines.append("")
    lines.append("## Files")
    lines.append("")
    lines.append(f"- Clustered p_hat data: `{CLUSTERED_PATH}`")
    lines.append(f"- De-clustered p_hat data: `{DECLUSTERED_PATH}`")
    lines.append(f"- Brier summary: `{SUMMARY_PATH}`")
    lines.append(f"- Calibration bins: `{CALIBRATION_PATH}`")

    for path in plot_paths:
        lines.append(f"- Plot: `{path}`")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved: {REPORT_PATH}")


def main() -> None:
    print("=" * 80)
    print("Compute horizon robustness")
    print("No API calls.")

    rows = read_csv(INPUT_PATH)
    clustered_rows = add_cluster_columns(rows)

    raw_rows = [
        row for row in clustered_rows
        if row.get("horizon_analysis_eligible") == "1"
    ]

    declustered_rows = decluster_family_bin(clustered_rows)

    write_csv(CLUSTERED_PATH, clustered_rows)
    write_csv(DECLUSTERED_PATH, declustered_rows)

    summary_rows = make_summary_rows(raw_rows, declustered_rows)
    calibration_rows = (
        make_calibration_rows(raw_rows, "raw_eligible")
        + make_calibration_rows(declustered_rows, "declustered_family_bin")
    )

    write_csv(SUMMARY_PATH, summary_rows)
    write_csv(CALIBRATION_PATH, calibration_rows)

    plot_paths = try_write_plots(calibration_rows)

    write_report(
        input_count=len(rows),
        clustered_rows=clustered_rows,
        raw_rows=raw_rows,
        declustered_rows=declustered_rows,
        summary_rows=summary_rows,
        calibration_rows=calibration_rows,
        plot_paths=plot_paths,
    )

    print("")
    print("=" * 80)
    print("Horizon robustness complete")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
