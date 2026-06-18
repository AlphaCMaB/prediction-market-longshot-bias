from __future__ import annotations

import pandas as pd


def clean_analysis_df(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Apply conservative cleaning rules and return (clean, drop_log)."""
    rows = []
    keep = []
    for idx, r in df.iterrows():
        reason = None
        if pd.isna(r.get("market_id")) or r.get("market_id") == "":
            reason = "missing_market_id"
        elif pd.isna(r.get("p_hat")):
            reason = "missing_p_hat"
        elif not (0 <= float(r.get("p_hat")) <= 1):
            reason = "p_hat_out_of_bounds"
        elif r.get("outcome") not in [0, 1, 0.0, 1.0]:
            reason = "invalid_outcome"
        elif pd.to_datetime(r.get("price_time"), utc=True) >= pd.to_datetime(r.get("resolution_time"), utc=True):
            reason = "price_after_or_at_resolution_leakage"
        if reason:
            rows.append({"index": idx, "venue": r.get("venue"), "market_id": r.get("market_id"), "drop_reason": reason})
        else:
            keep.append(idx)
    clean = df.loc[keep].copy()
    if len(clean):
        clean = clean.drop_duplicates(subset=["venue", "market_id", "token_id", "target_price_time"])
        clean["p_hat"] = clean["p_hat"].astype(float)
        clean["outcome"] = clean["outcome"].astype(int)
    return clean, pd.DataFrame(rows)


def quality_report(clean: pd.DataFrame, drop_log: pd.DataFrame, fetch_errors: list[str]) -> str:
    lines = []
    lines.append("# Week 1 Data Quality Report")
    lines.append("")
    lines.append(f"Clean contracts: {len(clean)}")
    lines.append(f"Dropped rows: {len(drop_log)}")
    if fetch_errors:
        lines.append("")
        lines.append("## Fetch errors")
        for err in fetch_errors:
            lines.append(f"- {err}")
    if len(clean):
        lines.append("")
        lines.append("## Venue counts")
        lines.append(clean["venue"].value_counts(dropna=False).to_markdown())
        lines.append("")
        lines.append("## Category counts")
        lines.append(clean["category"].value_counts(dropna=False).to_markdown())
        lines.append("")
        lines.append("## Probability and outcome checks")
        lines.append(f"p_hat min: {clean['p_hat'].min():.4f}")
        lines.append(f"p_hat max: {clean['p_hat'].max():.4f}")
        lines.append(f"mean p_hat: {clean['p_hat'].mean():.4f}")
        lines.append(f"YES rate: {clean['outcome'].mean():.4f}")
        pt = pd.to_datetime(clean["price_time"], utc=True)
        rt = pd.to_datetime(clean["resolution_time"], utc=True)
        leakage_count = int((pt >= rt).sum())
        lines.append(f"price_time >= resolution_time rows: {leakage_count}")
    if len(drop_log):
        lines.append("")
        lines.append("## Drop reasons")
        lines.append(drop_log["drop_reason"].value_counts().to_markdown())
    return "\n".join(lines) + "\n"
