from __future__ import annotations
import pandas as pd
import numpy as np
from statsmodels.tsa.api import VAR

def fit_var(df: pd.DataFrame, lags: int=2):
    model = VAR(df.dropna())
    res = model.fit(lags)
    return res

def compute_irf(var_res, periods: int=8):
    return var_res.irf(periods)

def baseline_forecast(var_res, steps: int=4):
    return var_res.forecast_interval(var_res.y, steps=steps, alpha=0.2)  # mean, lower, upper
