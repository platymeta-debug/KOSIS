from __future__ import annotations

from typing import Optional

import numpy as np
import pandas as pd
from sklearn.linear_model import LassoCV
from sklearn.preprocessing import StandardScaler
from statsmodels.tsa.api import DynamicFactor, VAR
from statsmodels.tsa.vector_ar.svar_model import SVAR
from statsmodels.tsa.stattools import adfuller


def make_stationary(df: pd.DataFrame) -> pd.DataFrame:
    """Apply unit-root testing and difference/log-difference as needed."""

    out: dict[str, pd.Series] = {}
    for col in df.columns:
        s = df[col].dropna()
        if s.empty:
            continue
        series = s
        try:
            pval = adfuller(series, autolag="AIC")[1]
        except Exception:
            pval = 1.0
        if pval > 0.05:
            if (series > 0).all():
                series = np.log(series).diff()
            else:
                series = series.diff()
        out[col] = series
    if not out:
        return pd.DataFrame(columns=df.columns)
    Z = pd.DataFrame(out)
    return Z.dropna(how="any")


def fit_var(df: pd.DataFrame, lags: int = 2):
    Z = make_stationary(df)
    if Z.empty:
        raise ValueError("Insufficient data after stationarity adjustments")
    return VAR(Z).fit(lags)


def fit_svar(
    df: pd.DataFrame,
    lags: int = 2,
    A: Optional[np.ndarray] = None,
    B: Optional[np.ndarray] = None,
):
    """Fit a simple SVAR model with optional identification matrices."""
    Z = make_stationary(df)
    if Z.empty:
        raise ValueError("Insufficient data after stationarity adjustments")
    base = VAR(Z).fit(lags)
    k = base.neqs
    if A is None:
        A = np.eye(k)
    return SVAR(base, A=A, B=B).fit(maxiter=500)


def irf_var(res, periods: int = 8):
    return res.irf(periods)


def irf_svar(res_svar, periods: int = 8):
    return res_svar.irf(periods)


def fit_dfm(df: pd.DataFrame, k_factors: int = 1, error_order: int = 1):
    Z = df.dropna()
    model = DynamicFactor(
        Z,
        k_factors=k_factors,
        factor_order=error_order,
        enforce_stationarity=False,
    )
    res = model.fit(method="em", disp=False, maxiter=200)
    return res


def ml_nowcast(df_features: pd.DataFrame, y: pd.Series):
    X, y2 = df_features.align(y, join="inner", axis=0)
    X = X.fillna(method="ffill").fillna(0.0)
    sc = StandardScaler()
    Xs = sc.fit_transform(X.values)
    mdl = LassoCV(cv=5, alphas=None, max_iter=5000, n_jobs=-1).fit(Xs, y2.values)
    return mdl, sc
