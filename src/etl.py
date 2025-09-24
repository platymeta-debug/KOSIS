from __future__ import annotations
import pandas as pd
import numpy as np

def standardize(df: pd.DataFrame) -> pd.DataFrame:
    """Basic cleaning: forward-fill, drop extreme outliers (winsorize), z-score optional."""
    df = df.copy()
    df = df.sort_index()
    df = df.ffill()
    # Simple winsorization at 1st and 99th percentile per column
    for c in df.columns:
        q1, q99 = df[c].quantile(0.01), df[c].quantile(0.99)
        df[c] = df[c].clip(q1, q99)
    return df

def transform_growth(df: pd.DataFrame, log_columns=None) -> pd.DataFrame:
    """Apply Δlog transform for approximate growth where it makes sense."""
    df = df.copy()
    if log_columns is None:
        log_columns = [c for c in df.columns if c not in ("policy_rate", "gdp_growth")]
    for c in log_columns:
        # add 1e-9 to avoid log(0)
        df[c+"_dlog"] = np.log(df[c].astype(float).clip(lower=1e-9)).diff()
    return df
