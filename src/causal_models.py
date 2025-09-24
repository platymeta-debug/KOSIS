"""Causal modelling helpers used by the orchestration script."""

from __future__ import annotations

from typing import Any, Dict, Optional

import numpy as np
import pandas as pd
from statsmodels.tsa.api import VAR, DynamicFactor
from statsmodels.tsa.vector_ar.svar_model import SVAR


def fit_var(df: pd.DataFrame, maxlags: int = 4, ic: str = "aic"):
    """Fit a VAR model using information-criterion lag selection."""

    data = df.dropna()
    if data.empty or data.shape[0] <= maxlags + 1:
        return None

    model = VAR(data)
    try:
        selection = model.select_order(maxlags=maxlags)
        lag_value = getattr(selection, ic)
        if not np.isfinite(lag_value):
            lag_value = min(2, maxlags)
        lags = int(lag_value)
    except Exception:
        lags = min(2, maxlags)

    try:
        return model.fit(lags)
    except Exception:
        return None


def fit_svar_from_var(
    var_res, A: Optional[np.ndarray] = None, B: Optional[np.ndarray] = None, maxiter: int = 500
):
    """Estimate an SVAR model based on a fitted VAR result."""

    if var_res is None:
        return None

    k = var_res.neqs
    if A is None:
        A = np.eye(k)
    try:
        svar = SVAR(var_res, A=A, B=B)
        return svar.fit(maxiter=maxiter)
    except Exception:
        return None


def irf_from_result(result, periods: int = 8):
    """Return an impulse response analysis object if available."""

    if result is None:
        return None
    try:
        return result.irf(periods)
    except Exception:
        return None


def fit_dfm(df: pd.DataFrame, k_factors: int = 1, factor_order: int = 1):
    """Fit a dynamic factor model."""

    data = df.dropna()
    if data.empty:
        return None

    try:
        model = DynamicFactor(
            data,
            k_factors=k_factors,
            factor_order=factor_order,
            enforce_stationarity=False,
        )
        return model.fit(method="em", disp=False, maxiter=200)
    except Exception:
        return None


def rolling_forecast_var(df: pd.DataFrame, lags: int, test_size: int = 8) -> Dict[str, Any]:
    """Produce a simple rolling-origin forecast evaluation for a VAR model."""

    data = df.dropna()
    if data.empty or data.shape[0] <= lags + test_size:
        return {}

    split = len(data) - test_size
    train, test = data.iloc[:split], data.iloc[split:]
    try:
        model = VAR(train).fit(lags)
        forecast = model.forecast(train.values[-lags:], steps=test_size)
    except Exception:
        return {}

    pred = pd.DataFrame(forecast, index=test.index, columns=data.columns)
    rmse = ((pred - test) ** 2).mean() ** 0.5
    return {"rmse": rmse, "pred": pred, "test": test, "lags": lags}
