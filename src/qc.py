"""Lightweight quality-control helpers for wide observation matrices."""

from __future__ import annotations

import pandas as pd


def basic_qc(wide: pd.DataFrame, min_len: int = 24, max_na_ratio: float = 0.2, min_std: float = 1e-8) -> pd.DataFrame:
    """Filter columns failing simple length, missingness, or variance checks."""

    keep = []
    for column in wide.columns:
        series = wide[column]
        valid = series.dropna()
        if len(valid) < min_len:
            continue
        if series.isna().mean() > max_na_ratio:
            continue
        if valid.std(ddof=0) < min_std:
            continue
        keep.append(column)
    return wide[keep]
