from __future__ import annotations

import pandas as pd


def brier_score(df: pd.DataFrame) -> float:
    if len(df) == 0:
        return float("nan")
    return float(((df["p_hat"] - df["outcome"]) ** 2).mean())


def baseline_brier(df: pd.DataFrame) -> float:
    if len(df) == 0:
        return float("nan")
    base_rate = float(df["outcome"].mean())
    return float(((base_rate - df["outcome"]) ** 2).mean())


def brier_tables(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    def summarize(group: pd.DataFrame, label: dict) -> dict:
        return {
            **label,
            "n_contracts": len(group),
            "mean_p_hat": float(group["p_hat"].mean()) if len(group) else float("nan"),
            "yes_rate": float(group["outcome"].mean()) if len(group) else float("nan"),
            "brier_score": brier_score(group),
            "baseline_brier": baseline_brier(group),
        }

    overall = pd.DataFrame([summarize(df, {"group": "overall"})])
    by_venue = pd.DataFrame([summarize(g, {"venue": k}) for k, g in df.groupby("venue", dropna=False)])
    by_category = pd.DataFrame([summarize(g, {"category": k}) for k, g in df.groupby("category", dropna=False)])
    by_venue_category = pd.DataFrame([
        summarize(g, {"venue": venue, "category": cat})
        for (venue, cat), g in df.groupby(["venue", "category"], dropna=False)
    ])
    return {
        "overall": overall,
        "by_venue": by_venue,
        "by_category": by_category,
        "by_venue_category": by_venue_category,
    }
