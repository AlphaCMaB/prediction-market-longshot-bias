"""
16_category_venue_breakdown.py

Purpose:
    Break down calibration bias by:
        - horizon
        - venue
        - broad inferred category
        - probability bin

Why:
    Earlier scripts showed that the favorite-longshot pattern survives:
        - de-clustering
        - multiple horizons
        - bootstrap uncertainty
        - balanced-panel restriction
        - Kalshi spread filters

    This script asks:
        Is the bias concentrated in one venue or category,
        or does it appear across multiple groups?

Input:
    data/processed/p_hat_horizons_declustered_family_bin.csv

Outputs:
    outputs/category_venue_summary.csv
    outputs/category_venue_bins.csv
    outputs/category_venue_focus_bootstrap.csv
    outputs/category_venue_breakdown_report.md
    outputs/category_venue_10_20_heatmap.csv

Important:
    This script does NOT call APIs.
"""

from __future__ import annotations

import csv
import math
import random
import re
from collections import Counter, defaultdict
from pathlib import Path
from typing import Any


INPUT_PATH = Path("data/processed/p_hat_horizons_declustered_family_bin.csv")

OUTPUTS_DIR = Path("outputs")
OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)

SUMMARY_PATH = OUTPUTS_DIR / "category_venue_summary.csv"
BINS_PATH = OUTPUTS_DIR / "category_venue_bins.csv"
FOCUS_BOOTSTRAP_PATH = OUTPUTS_DIR / "category_venue_focus_bootstrap.csv"
HEATMAP_CSV_PATH = OUTPUTS_DIR / "category_venue_10_20_heatmap.csv"
REPORT_PATH = OUTPUTS_DIR / "category_venue_breakdown_report.md"

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

    if p is None or not (0.0 <= p <= 1.0):
        return False

    if y not in {0, 1}:
        return False

    return True


def clean_text(text: str) -> str:
    text = str(text or "").lower()
    text = text.replace("&", " and ")
    text = re.sub(r"[^a-z0-9\s$%.°-]", " ", text)
    text = re.sub(r"\s+", " ", text).strip()
    return text


def broad_category(row: dict) -> str:
    """
    Heuristic category inference.

    The raw Polymarket API pull often does not provide a reliable category field,
    so we infer broad categories from title, market_id, event_family, and venue.
    This is good enough for pilot analysis, but should be documented as heuristic.
    """
    title = clean_text(row.get("title", ""))
    market_id = clean_text(row.get("market_id", ""))
    family = clean_text(row.get("event_family", ""))

    text = f"{title} {market_id} {family}"

    # Weather
    if (
        "high temp" in text
        or "low temp" in text
        or "temperature" in text
        or "weather" in text
        or "°" in text
        or "hurricane" in text
        or "snow" in text
        or "rain" in text
    ):
        return "weather"

    # Sports and esports
    sports_terms = [
        "match", "set ", "game", "win map", "maps", "cs2", "overwatch",
        "atp", "wta", "wnba", "basketball", "football", "soccer",
        "tennis", "cricket", "t20", "pga", "golf", "cup", "tournament",
        "winner", "spread", "total", "score",
    ]
    if any(term in text for term in sports_terms):
        return "sports_esports"

    # Crypto / token-launch / web3 markets
    crypto_terms = [
        "fdv", "token", "airdrop", "crypto", "bitcoin", "btc", "ethereum",
        "eth", "solana", "sol", "auction clearing price", "one day after launch",
        "public sale", "tge", "moonbirds", "zama", "espresso", "citrea",
        "katana", "immunefi", "pharos", "brevis", "owlto", "xmaquina",
        "onefootball", "fluent", "tea", "trove", "space public sale",
    ]
    if any(term in text for term in crypto_terms):
        return "crypto_web3"

    # Politics / law / geopolitics / public affairs
    politics_terms = [
        "trump", "biden", "zelensky", "iran", "scotus", "supreme court",
        "election", "president", "senate", "house", "congress",
        "tariff", "war", "ceasefire", "nato", "mou", "bill", "ballroom",
        "marijuana", "gun",
    ]
    if any(term in text for term in politics_terms):
        return "politics_law_geopolitics"

    # Finance / macro / commodities / company valuation
    finance_terms = [
        "wti", "crude", "oil", "settlement price", "usd bbl", "market cap",
        "ipo", "stock", "nasdaq", "s&p", "dow", "fed", "rate", "cpi",
        "inflation", "gdp", "unemployment", "recession", "yield",
        "dollar", "gold", "silver",
    ]
    if any(term in text for term in finance_terms):
        return "finance_macro"

    return "other"


def mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else float("nan")


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


def empirical_minus_p(rows: list[dict]) -> float:
    values = []

    for row in rows:
        p = to_float(row.get("p_hat"))
        y = to_int(row.get("outcome"))

        if p is not None and y in {0, 1}:
            values.append(y - p)

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


def rows_in_range(rows: list[dict], low: float, high: float) -> list[dict]:
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

    observed = empirical_minus_p(rows)
    draws = []

    for _ in range(BOOTSTRAP_ITERATIONS):
        sample = [rows[rng.randrange(n)] for _ in range(n)]
        draws.append(empirical_minus_p(sample))

    draws.sort()

    low_index = int(0.025 * (BOOTSTRAP_ITERATIONS - 1))
    high_index = int(0.975 * (BOOTSTRAP_ITERATIONS - 1))

    return observed, draws[low_index], draws[high_index]


def add_category(row: dict) -> dict:
    row = dict(row)
    row["broad_category"] = broad_category(row)
    return row


def make_summary_rows(rows: list[dict]) -> list[dict]:
    output = []

    groupings = [
        ("venue", lambda r: r["venue"]),
        ("category", lambda r: r["broad_category"]),
        ("venue_category", lambda r: f"{r['venue']}::{r['broad_category']}"),
    ]

    for grouping_name, group_fn in groupings:
        groups = defaultdict(list)

        for row in rows:
            key = (
                row.get("horizon_hours"),
                group_fn(row),
            )
            groups[key].append(row)

        for (horizon, group_value), group_rows in sorted(groups.items(), key=lambda x: (int(x[0][0]), x[0][1])):
            output.append(
                {
                    "grouping": grouping_name,
                    "horizon_hours": horizon,
                    "group": group_value,
                    "n": len(group_rows),
                    "mean_p_hat": mean_p_hat(group_rows),
                    "empirical_rate": empirical_rate(group_rows),
                    "empirical_minus_p": empirical_minus_p(group_rows),
                    "brier_score": brier_score(group_rows),
                    "polymarket_n": sum(row["venue"] == "polymarket" for row in group_rows),
                    "kalshi_n": sum(row["venue"] == "kalshi" for row in group_rows),
                }
            )

    return output


def make_bin_rows(rows: list[dict]) -> list[dict]:
    output = []

    groupings = [
        ("venue", lambda r: r["venue"]),
        ("category", lambda r: r["broad_category"]),
        ("venue_category", lambda r: f"{r['venue']}::{r['broad_category']}"),
    ]

    for grouping_name, group_fn in groupings:
        grouped = defaultdict(list)

        for row in rows:
            p = to_float(row.get("p_hat"))
            b = bin_for_p(p) if p is not None else None

            if b is None:
                continue

            key = (
                row.get("horizon_hours"),
                group_fn(row),
                b[0],
                b[1],
            )
            grouped[key].append(row)

        for (horizon, group_value, low, high), group_rows in sorted(grouped.items(), key=lambda x: (int(x[0][0]), x[0][1], x[0][2])):
            output.append(
                {
                    "grouping": grouping_name,
                    "horizon_hours": horizon,
                    "group": group_value,
                    "bin_low": low,
                    "bin_high": high,
                    "n": len(group_rows),
                    "mean_p_hat": mean_p_hat(group_rows),
                    "empirical_rate": empirical_rate(group_rows),
                    "empirical_minus_p": empirical_minus_p(group_rows),
                    "brier_score": brier_score(group_rows),
                    "polymarket_n": sum(row["venue"] == "polymarket" for row in group_rows),
                    "kalshi_n": sum(row["venue"] == "kalshi" for row in group_rows),
                }
            )

    return output


def make_focus_bootstrap_rows(rows: list[dict]) -> list[dict]:
    output = []
    rng = random.Random(RANDOM_SEED)

    groupings = [
        ("all", lambda r: "all"),
        ("venue", lambda r: r["venue"]),
        ("category", lambda r: r["broad_category"]),
        ("venue_category", lambda r: f"{r['venue']}::{r['broad_category']}"),
    ]

    for grouping_name, group_fn in groupings:
        groups = defaultdict(list)

        for row in rows:
            key = (
                row.get("horizon_hours"),
                group_fn(row),
            )
            groups[key].append(row)

        for (horizon, group_value), group_rows in sorted(groups.items(), key=lambda x: (int(x[0][0]), x[0][1])):
            for focus_name, low, high in FOCUS_GROUPS:
                focus_rows = rows_in_range(group_rows, low, high)
                observed, ci_low, ci_high = bootstrap_ci(focus_rows, rng)

                output.append(
                    {
                        "grouping": grouping_name,
                        "horizon_hours": horizon,
                        "group": group_value,
                        "focus_group": focus_name,
                        "bin_low": low,
                        "bin_high": high,
                        "n": len(focus_rows),
                        "mean_p_hat": mean_p_hat(focus_rows),
                        "empirical_rate": empirical_rate(focus_rows),
                        "empirical_minus_p": observed,
                        "ci_low": ci_low,
                        "ci_high": ci_high,
                        "classification": classify(len(focus_rows), ci_low, ci_high, observed),
                        "polymarket_n": sum(row["venue"] == "polymarket" for row in focus_rows),
                        "kalshi_n": sum(row["venue"] == "kalshi" for row in focus_rows),
                    }
                )

    return output


def make_heatmap_rows(focus_rows: list[dict]) -> list[dict]:
    rows = [
        row for row in focus_rows
        if row["grouping"] == "category"
        and row["focus_group"] == "longshot_10_20"
    ]

    output = []

    for row in rows:
        output.append(
            {
                "category": row["group"],
                "horizon_hours": row["horizon_hours"],
                "n": row["n"],
                "empirical_minus_p": row["empirical_minus_p"],
                "ci_low": row["ci_low"],
                "ci_high": row["ci_high"],
                "classification": row["classification"],
            }
        )

    return sorted(output, key=lambda r: (r["category"], int(r["horizon_hours"])))


def fmt(value: Any, digits: int = 4) -> str:
    x = to_float(value)

    if x is None:
        return ""

    return f"{x:.{digits}f}"


def write_report(
    rows: list[dict],
    summary_rows: list[dict],
    bin_rows: list[dict],
    focus_rows: list[dict],
    heatmap_rows: list[dict],
) -> None:
    category_counts = Counter(row["broad_category"] for row in rows)
    venue_counts = Counter(row["venue"] for row in rows)

    lines = []

    lines.append("# Category and Venue Breakdown Report")
    lines.append("")
    lines.append("This report breaks down calibration bias by venue and broad inferred category.")
    lines.append("")
    lines.append("Category labels are heuristic, inferred from market titles and event-family strings.")
    lines.append("")
    lines.append("## Setup")
    lines.append("")
    lines.append(f"- Input rows used: {len(rows)}")
    lines.append(f"- Bootstrap iterations: {BOOTSTRAP_ITERATIONS}")
    lines.append(f"- Random seed: {RANDOM_SEED}")
    lines.append(f"- Minimum n for strong interpretation: {MIN_N_FOR_STRONG_INTERPRETATION}")
    lines.append("")
    lines.append("## Sample composition")
    lines.append("")
    lines.append("### By venue")

    for venue, count in venue_counts.most_common():
        lines.append(f"- {venue}: {count}")

    lines.append("")
    lines.append("### By broad category")

    for category, count in category_counts.most_common():
        lines.append(f"- {category}: {count}")

    lines.append("")
    lines.append("## Focus test: 10-20% longshots by venue")
    lines.append("")

    for horizon in HORIZONS:
        lines.append(f"### {horizon}h")
        rows_to_show = [
            row for row in focus_rows
            if row["grouping"] == "venue"
            and row["horizon_hours"] == horizon
            and row["focus_group"] == "longshot_10_20"
        ]

        for row in rows_to_show:
            lines.append(
                f"- {row['group']}: "
                f"n={row['n']}, "
                f"mean_p={fmt(row['mean_p_hat'])}, "
                f"empirical={fmt(row['empirical_rate'])}, "
                f"empirical_minus_p={fmt(row['empirical_minus_p'])}, "
                f"95% CI=[{fmt(row['ci_low'])}, {fmt(row['ci_high'])}], "
                f"classification={row['classification']}"
            )

        lines.append("")

    lines.append("## Focus test: 10-20% longshots by category")
    lines.append("")

    for horizon in HORIZONS:
        lines.append(f"### {horizon}h")
        rows_to_show = [
            row for row in focus_rows
            if row["grouping"] == "category"
            and row["horizon_hours"] == horizon
            and row["focus_group"] == "longshot_10_20"
        ]
        rows_to_show = sorted(rows_to_show, key=lambda r: (-int(r["n"]), r["group"]))

        for row in rows_to_show:
            lines.append(
                f"- {row['group']}: "
                f"n={row['n']}, "
                f"mean_p={fmt(row['mean_p_hat'])}, "
                f"empirical={fmt(row['empirical_rate'])}, "
                f"empirical_minus_p={fmt(row['empirical_minus_p'])}, "
                f"95% CI=[{fmt(row['ci_low'])}, {fmt(row['ci_high'])}], "
                f"classification={row['classification']}"
            )

        lines.append("")

    lines.append("## Focus test: 70-90% favorites by category")
    lines.append("")

    for horizon in HORIZONS:
        lines.append(f"### {horizon}h")
        rows_to_show = [
            row for row in focus_rows
            if row["grouping"] == "category"
            and row["horizon_hours"] == horizon
            and row["focus_group"] == "favorite_70_90"
        ]
        rows_to_show = sorted(rows_to_show, key=lambda r: (-int(r["n"]), r["group"]))

        for row in rows_to_show:
            lines.append(
                f"- {row['group']}: "
                f"n={row['n']}, "
                f"mean_p={fmt(row['mean_p_hat'])}, "
                f"empirical={fmt(row['empirical_rate'])}, "
                f"empirical_minus_p={fmt(row['empirical_minus_p'])}, "
                f"95% CI=[{fmt(row['ci_low'])}, {fmt(row['ci_high'])}], "
                f"classification={row['classification']}"
            )

        lines.append("")

    lines.append("## Strongest 10-20% longshot overpricing groups")
    lines.append("")

    candidates = [
        row for row in focus_rows
        if row["focus_group"] == "longshot_10_20"
        and int(row["n"]) >= MIN_N_FOR_STRONG_INTERPRETATION
        and to_float(row["empirical_minus_p"]) is not None
    ]
    candidates = sorted(candidates, key=lambda r: float(r["empirical_minus_p"]))

    for row in candidates[:20]:
        lines.append(
            f"- {row['grouping']} | {row['horizon_hours']}h | {row['group']}: "
            f"n={row['n']}, "
            f"empirical_minus_p={fmt(row['empirical_minus_p'])}, "
            f"CI=[{fmt(row['ci_low'])}, {fmt(row['ci_high'])}], "
            f"classification={row['classification']}"
        )

    lines.append("")
    lines.append("## Interpretation guide")
    lines.append("")
    lines.append("- Negative empirical_minus_p means the market was overpriced.")
    lines.append("- Positive empirical_minus_p means the market was underpriced.")
    lines.append("- Category labels are heuristic, so use this as a diagnostic breakdown, not a final taxonomy.")
    lines.append("- Groups with n < 20 should be treated as exploratory.")
    lines.append("")
    lines.append("## Files")
    lines.append("")
    lines.append(f"- Summary: `{SUMMARY_PATH}`")
    lines.append(f"- Probability bins: `{BINS_PATH}`")
    lines.append(f"- Focus bootstrap: `{FOCUS_BOOTSTRAP_PATH}`")
    lines.append(f"- 10-20 heatmap CSV: `{HEATMAP_CSV_PATH}`")

    REPORT_PATH.write_text("\n".join(lines), encoding="utf-8")
    print(f"Saved: {REPORT_PATH}")


def main() -> None:
    print("=" * 80)
    print("Category and venue breakdown")
    print("No API calls.")

    rows = read_csv(INPUT_PATH)
    clean_rows = [add_category(row) for row in rows if valid_row(row)]

    summary_rows = make_summary_rows(clean_rows)
    bin_rows = make_bin_rows(clean_rows)
    focus_rows = make_focus_bootstrap_rows(clean_rows)
    heatmap_rows = make_heatmap_rows(focus_rows)

    write_csv(SUMMARY_PATH, summary_rows)
    write_csv(BINS_PATH, bin_rows)
    write_csv(FOCUS_BOOTSTRAP_PATH, focus_rows)
    write_csv(HEATMAP_CSV_PATH, heatmap_rows)

    write_report(
        rows=clean_rows,
        summary_rows=summary_rows,
        bin_rows=bin_rows,
        focus_rows=focus_rows,
        heatmap_rows=heatmap_rows,
    )

    print("")
    print("=" * 80)
    print("Category and venue breakdown complete")
    print(f"Report: {REPORT_PATH}")


if __name__ == "__main__":
    main()
