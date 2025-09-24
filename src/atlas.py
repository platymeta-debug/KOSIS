import pandas as pd, numpy as np, itertools
from statsmodels.tsa.stattools import grangercausalitytests

def rolling_corr(df: pd.DataFrame, window: int=20):
    out = {}
    for end in range(window, len(df)+1):
        sub = df.iloc[end-window:end]
        out[str(sub.index[-1])] = sub.corr()
    return out

def granger_matrix(df: pd.DataFrame, maxlag: int=4):
    cols = df.columns
    p = pd.DataFrame(np.nan, index=cols, columns=cols)
    for a,b in itertools.permutations(cols, 2):
        try:
            res = grangercausalitytests(df[[b,a]].dropna(), maxlag=maxlag, verbose=False)
            p.loc[a,b] = min(r[0]['ssr_ftest'][1] for r in res.values())
        except Exception:
            pass
    return p

def top_pairs(corr_df: pd.DataFrame, n=10):
    tri = []
    cols = corr_df.columns
    for i in range(len(cols)):
        for j in range(i+1, len(cols)):
            tri.append((cols[i], cols[j], corr_df.iloc[i,j]))
    return sorted(tri, key=lambda x: abs(x[2]), reverse=True)[:n]
