from __future__ import annotations

import itertools

import networkx as nx
import numpy as np
import pandas as pd
from statsmodels.stats.multitest import multipletests
from statsmodels.tsa.stattools import ccf, grangercausalitytests


def partial_corr(df: pd.DataFrame) -> pd.DataFrame:
    """Compute partial correlations from precision matrix."""
    X = (df - df.mean()) / df.std(ddof=0)
    X = X.dropna(axis=1, how="any").dropna()
    S = np.cov(X.values, rowvar=False)
    P = np.linalg.pinv(S)
    d = np.sqrt(np.diag(P))
    pcorr = -P / np.outer(d, d)
    np.fill_diagonal(pcorr, 1.0)
    return pd.DataFrame(pcorr, index=X.columns, columns=X.columns)


def lead_lag(df: pd.DataFrame, max_lag: int = 6) -> pd.DataFrame:
    """Cross-correlation-based lead/lag matrix."""
    cols = df.columns
    out = pd.DataFrame(0, index=cols, columns=cols, dtype=int)
    Z = df.dropna()
    for a, b in itertools.permutations(cols, 2):
        x, y = Z[a].values, Z[b].values
        c = ccf(x - np.mean(x), y - np.mean(y))[: max_lag + 1]
        lag = int(np.argmax(np.abs(c)))
        out.loc[a, b] = lag
    return out


def granger_pvals(df: pd.DataFrame, maxlag: int = 4) -> pd.DataFrame:
    cols = df.columns
    p = pd.DataFrame(np.nan, index=cols, columns=cols)
    Z = df.dropna()
    for a, b in itertools.permutations(cols, 2):
        try:
            res = grangercausalitytests(Z[[b, a]], maxlag=maxlag, verbose=False)
            p.loc[a, b] = min(r[0]["ssr_ftest"][1] for r in res.values())
        except Exception:
            pass
    return p


def fdr_significance(pmat: pd.DataFrame, alpha: float = 0.05) -> pd.DataFrame:
    vals = pmat.values.flatten()
    mask = ~np.isnan(vals)
    rej = np.zeros_like(vals, dtype=bool)
    if mask.sum():
        rej[mask] = multipletests(vals[mask], alpha=alpha, method="fdr_bh")[0]
    return pd.DataFrame(rej.reshape(pmat.shape), index=pmat.index, columns=pmat.columns)


def build_network(
    corr: pd.DataFrame,
    granger_sig: pd.DataFrame,
    leadlag: pd.DataFrame,
    corr_thr: float = 0.3,
) -> nx.DiGraph:
    """Create network combining correlations and Granger causality."""
    G = nx.DiGraph()
    for v in corr.columns:
        G.add_node(v)
    for i, a in enumerate(corr.columns):
        for j, b in enumerate(corr.columns):
            if i == j:
                continue
            g_val = granger_sig.loc[a, b] if a in granger_sig.index and b in granger_sig.columns else False
            g_flag = bool(g_val) if pd.notna(g_val) else False
            if abs(corr.loc[a, b]) >= corr_thr or g_flag:
                w = float(corr.loc[a, b])
                lag = (
                    int(leadlag.loc[a, b])
                    if (a in leadlag.index and b in leadlag.columns)
                    else 0
                )
                G.add_edge(a, b, weight=w, lag=lag)
    return G


def nl_summary(
    corr: pd.DataFrame,
    granger_sig: pd.DataFrame,
    leadlag: pd.DataFrame,
    top_n: int = 5,
) -> list[str]:
    """Generate natural language bullet summary."""
    tri = []
    cols = corr.columns
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            tri.append((cols[i], cols[j], corr.iloc[i, j]))
    tri = sorted(tri, key=lambda x: abs(x[2]), reverse=True)[:top_n]
    bullets = []
    for a, b, r in tri:
        lag_ab = leadlag.loc[a, b] if (a in leadlag.index and b in leadlag.columns) else 0
        g_val = granger_sig.loc[a, b] if a in granger_sig.index and b in granger_sig.columns else False
        g_flag = bool(g_val) if pd.notna(g_val) else False
        sig = " (Granger 유의)" if g_flag else ""
        bullets.append(f"{a} ↔ {b} 상관 {r:.2f}, lead/lag: {lag_ab}기{sig}")
    return bullets
