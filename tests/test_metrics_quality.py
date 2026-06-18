import pandas as pd

from src.pm_week1.metrics import brier_score, baseline_brier
from src.pm_week1.quality import clean_analysis_df


def test_brier_score():
    df = pd.DataFrame({"p_hat": [0.7, 0.2], "outcome": [1, 0]})
    assert abs(brier_score(df) - ((0.3**2 + 0.2**2) / 2)) < 1e-9


def test_clean_drops_leakage():
    df = pd.DataFrame({
        "venue": ["x", "x"],
        "market_id": ["a", "b"],
        "token_id": [None, None],
        "p_hat": [0.5, 0.5],
        "outcome": [1, 0],
        "price_time": ["2024-01-01T00:00:00Z", "2024-01-03T00:00:00Z"],
        "resolution_time": ["2024-01-02T00:00:00Z", "2024-01-02T00:00:00Z"],
        "target_price_time": ["2023-12-31T00:00:00Z", "2023-12-31T00:00:00Z"],
    })
    clean, drop_log = clean_analysis_df(df)
    assert len(clean) == 1
    assert len(drop_log) == 1
    assert drop_log.iloc[0]["drop_reason"] == "price_after_or_at_resolution_leakage"
