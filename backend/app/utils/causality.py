import pandas as pd
import numpy as np
from statsmodels.tsa.stattools import grangercausalitytests

def lagged_correlation(
    df: pd.DataFrame,
    sentiment_col: str,
    target_col: str,
    max_lag: int = 5
) -> pd.DataFrame:
    results = []

    for lag in range(0, max_lag + 1):
        shifted = df[sentiment_col].shift(lag)
        valid = pd.concat([shifted, df[target_col]], axis=1).dropna()

        if len(valid) < 10:
            continue

        corr = valid.iloc[:, 0].corr(valid.iloc[:, 1])

        results.append({
            "lag": lag,
            "correlation": round(corr, 4),
            "n_obs": len(valid)
        })

    return pd.DataFrame(results)

def granger_test(
    df: pd.DataFrame,
    sentiment_col: str,
    target_col: str,
    max_lag: int = 5
) -> pd.DataFrame:
    data = df[[target_col, sentiment_col]].dropna()

    if len(data) < 20:
        raise ValueError("Datos insuficientes para Granger")

    test = grangercausalitytests(
        data,
        maxlag=max_lag,
        verbose=False
    )

    rows = []
    for lag, res in test.items():
        p_value = res[0]["ssr_ftest"][1]
        rows.append({
            "lag": lag,
            "p_value": round(p_value, 6),
            "significant_5pct": p_value < 0.05
        })

    return pd.DataFrame(rows)
