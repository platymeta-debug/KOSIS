import pandas as pd
from statsmodels.tsa.api import VAR

def fit_var(df: pd.DataFrame, lags=2):
    return VAR(df.dropna()).fit(lags)

def irf(res, periods=8):
    return res.irf(periods)
