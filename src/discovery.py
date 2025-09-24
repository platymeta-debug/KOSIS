from __future__ import annotations

import itertools
import numpy as np
import pandas as pd
from sklearn.feature_selection import mutual_info_regression
from sklearn.preprocessing import StandardScaler
from statsmodels.stats.multitest import multipletests
from statsmodels.tsa.stattools import grangercausalitytests


def _mi_xy(x: np.ndarray, y: np.ndarray) -> float:
    x = x.reshape(-1, 1)
    y = y.ravel()
    mask = np.isfinite(x).ravel() & np.isfinite(y)
    if mask.sum() < 20:
        return 0.0
    sc = StandardScaler()
    xs = sc.fit_transform(x[mask])
    mi = mutual_info_regression(xs, y[mask], discrete_features=False, random_state=0)
    return float(mi[0])



def _rolling_corr_consistency(df: pd.DataFrame, window: int = 20, thr: float = 0.3) -> pd.DataFrame:
    """창 내 상관계수의 절대값이 thr 이상인 비율을 계산한다."""

    cols = df.columns
    hits = pd.DataFrame(0.0, index=cols, columns=cols)
    total = 0
    for end in range(window, len(df) + 1):
        sub = df.iloc[end - window : end]
        c = sub.corr().abs()
        hits = hits.add((c >= thr).astype(float), fill_value=0.0)
        total += 1
    if total == 0:
        return hits
    return hits / total


def _partial_corr(df: pd.DataFrame) -> pd.DataFrame:
    X = (df - df.mean()) / df.std(ddof=0)
    X = X.dropna(axis=1, how="any").dropna()
    S = np.cov(X.values, rowvar=False)
    P = np.linalg.pinv(S)
    d = np.sqrt(np.diag(P))
    pcorr = -P / np.outer(d, d)
    np.fill_diagonal(pcorr, 1.0)
    return pd.DataFrame(pcorr, index=X.columns, columns=X.columns)


def _granger_pvals(df: pd.DataFrame, maxlag: int = 4) -> pd.DataFrame:
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


def discover_signals(
    wide: pd.DataFrame,
    corr_thr_strong: float = 0.45,
    corr_thr_medium: float = 0.3,
    maxlag: int = 4,
) -> pd.DataFrame:
    """시계열 전수탐색으로 신호 페어를 추출한다."""

    Z = wide.dropna(axis=1, how="any").dropna()
    if Z.shape[1] < 3:
        return pd.DataFrame()

    corr = Z.corr()
    pcorr = _partial_corr(Z).reindex_like(corr).fillna(0.0)
    gr = _granger_pvals(Z, maxlag=maxlag)
    flat = gr.values.flatten()
    mask = ~np.isnan(flat)
    rej = np.zeros_like(flat, dtype=bool)
    if mask.sum():
        rej[mask] = multipletests(flat[mask], alpha=0.05, method="fdr_bh")[0]
    gr_sig = pd.DataFrame(rej.reshape(gr.shape), index=gr.index, columns=gr.columns)

    def lead_of(a: pd.Series, b: pd.Series, max_lag: int = 6) -> int:
        x = a.values - a.values.mean()
        y = b.values - b.values.mean()
        best, lag = 0.0, 0
        for L in range(0, max_lag + 1):
            if L == 0:
                c = np.corrcoef(x, y)[0, 1]
            else:
                if len(x[L:]) < 5:
                    break
                c = np.corrcoef(x[L:], y[:-L])[0, 1]
            if np.abs(c) > np.abs(best):
                best, lag = c, L
        return lag

    cons = _rolling_corr_consistency(
        Z, window=min(20, max(5, len(Z) // 5)), thr=corr_thr_medium
    )

    rows = []
    cols = list(Z.columns)
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            a, b = cols[i], cols[j]
            c = corr.loc[a, b]
            pc = pcorr.loc[a, b] if a in pcorr.index and b in pcorr.columns else np.nan
            g_ab = bool(gr_sig.loc[a, b]) if (a in gr_sig.index and b in gr_sig.columns) else False
            g_ba = bool(gr_sig.loc[b, a]) if (b in gr_sig.index and a in gr_sig.columns) else False
            lag_ab = lead_of(Z[a], Z[b])
            k = float(np.nan_to_num(pc, nan=0.0))

            mi = _mi_xy(Z[a].values, Z[b].values)
            s = (
                abs(c)
                + 0.5 * abs(k)
                + 0.2 * (g_ab + g_ba)
                + 0.3 * float(cons.loc[a, b])
                + 0.3 * mi
            )

            tier = (
                "strong"
                if abs(c) >= corr_thr_strong
                else "medium"
                if abs(c) >= corr_thr_medium
                else "weak"
            )
            rows.append(
                {
                    "a": a,
                    "b": b,
                    "corr": float(c),
                    "pcorr": float(k),
                    "gr_ab": g_ab,
                    "gr_ba": g_ba,
                    "lead_ab": int(lag_ab),
                    "consistency": float(cons.loc[a, b]),
                    "mi": float(mi),
                    "score": float(s),
                    "tier": tier,
                }
            )
    out = pd.DataFrame(rows).sort_values("score", ascending=False)
    return out

