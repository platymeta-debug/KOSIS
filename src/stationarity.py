"""Stationarity diagnostics and automatic transformations."""

from __future__ import annotations

from typing import Dict, Tuple

import numpy as np
import pandas as pd
from statsmodels.tsa.stattools import adfuller, kpss


def _clean_series(series: pd.Series) -> pd.Series:
    """Return a numeric series with infinities removed."""

    return series.replace([np.inf, -np.inf], np.nan).dropna()


def adf_p(series: pd.Series) -> float:
    """Return the Augmented Dickey-Fuller p-value for ``series``."""

    clean = _clean_series(series.astype(float))
    if clean.shape[0] < 12:
        return 1.0
    try:
        return float(adfuller(clean, autolag="AIC")[1])
    except Exception:
        return 1.0


def kpss_p(series: pd.Series) -> float:
    """Return the KPSS p-value for ``series`` under a level-stationary null."""

    clean = _clean_series(series.astype(float))
    if clean.shape[0] < 12:
        return 0.0
    try:
        stat, p_value, *_ = kpss(clean, regression="c", nlags="auto")
        _ = stat
        return float(p_value)
    except Exception:
        return 0.0


def decide_transform(series: pd.Series) -> str:
    """Choose an appropriate transformation for ``series``.

    Rules:
    - If the ADF p-value is <= 0.05, keep the series as-is (``"none"``).
    - Otherwise, attempt a log-difference when at least 90% of the
      observations are positive; fall back to a first difference.
    """

    p_value = adf_p(series)
    if p_value <= 0.05:
        return "none"
    positive_ratio = float((series > 0).sum()) / float(len(series)) if len(series) else 0.0
    if positive_ratio >= 0.9:
        return "logdiff"
    return "diff"


def apply_transform(frame: pd.DataFrame) -> Tuple[pd.DataFrame, Dict[str, str]]:
    """Apply automatic transformations to each column of ``frame``."""

    transformed: Dict[str, pd.Series] = {}
    mapping: Dict[str, str] = {}

    for column in frame.columns:
        series = frame[column].astype(float)
        choice = decide_transform(series)
        mapping[column] = choice

        if choice == "none":
            transformed[column] = series
        elif choice == "diff":
            transformed[column] = series.diff()
        elif choice == "logdiff":
            safe = series.mask(series <= 0)
            logged = np.log(safe)
            transformed[column] = logged.diff()
        else:  # pragma: no cover - defensive fallback
            transformed[column] = series

    result = pd.DataFrame(transformed, index=frame.index)
    result = result.dropna(how="all").dropna(axis=0)
    return result, mapping


def stationarity_report(
    raw: pd.DataFrame, transformed: pd.DataFrame, mapping: Dict[str, str]
) -> pd.DataFrame:
    """Return a tidy stationarity diagnostics table."""

    rows = []
    for column in transformed.columns:
        rows.append(
            {
                "var": column,
                "transform": mapping.get(column, "none"),
                "adf_p_raw": adf_p(raw[column]) if column in raw.columns else np.nan,
                "adf_p_trans": adf_p(transformed[column]),
                "kpss_p_trans": kpss_p(transformed[column]),
                "n_obs": int(transformed[column].dropna().shape[0]),
                "std_trans": float(transformed[column].std(ddof=0)),
            }
        )
    if not rows:
        return pd.DataFrame(columns=[
            "var",
            "transform",
            "adf_p_raw",
            "adf_p_trans",
            "kpss_p_trans",
            "n_obs",
            "std_trans",
        ])
    return pd.DataFrame(rows).sort_values("adf_p_trans")
