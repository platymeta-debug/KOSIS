from __future__ import annotations
import pandas as pd
import numpy as np
from typing import Dict, List, Tuple
from tqdm import tqdm
import itertools
from statsmodels.tsa.stattools import grangercausalitytests

def rolling_corr(df: pd.DataFrame, window: int=20) -> Dict[str, pd.DataFrame]:
    """Compute rolling correlation matrices. Returns dict: {period_end: corr_df}"""
    out = {}
    for end in range(window, len(df)+1):
        sub = df.iloc[end-window:end]
        out[str(sub.index[-1].date())] = sub.corr()
    return out

def granger_matrix(df: pd.DataFrame, maxlag: int=4, p_threshold: float=0.05) -> pd.DataFrame:
    """Compute pairwise Granger causality p-values (min over lags) and flag significance."""
    cols = df.columns
    pvals = pd.DataFrame(np.nan, index=cols, columns=cols)
    for x, y in itertools.permutations(cols, 2):
        try:
            res = grangercausalitytests(df[[y, x]].dropna(), maxlag=maxlag, verbose=False)
            pv = min(r[0]['ssr_ftest'][1] for k, r in res.items())
            pvals.loc[x, y] = pv
        except Exception:
            pvals.loc[x, y] = np.nan
    return pvals

def summarize_top_relations(corr_df: pd.DataFrame, top_n: int=10) -> List[Tuple[str, str, float]]:
    tri = []
    cols = corr_df.columns
    for i in range(len(cols)):
        for j in range(i+1, len(cols)):
            tri.append((cols[i], cols[j], corr_df.iloc[i, j]))
    tri_sorted = sorted(tri, key=lambda x: abs(x[2]), reverse=True)
    return tri_sorted[:top_n]
