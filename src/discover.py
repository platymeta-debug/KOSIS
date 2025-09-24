"""Brute-force discovery helpers for cross-series relationship screening."""

from __future__ import annotations

import itertools
from typing import Tuple

import numpy as np
import pandas as pd
from sklearn.feature_selection import mutual_info_regression
from sklearn.preprocessing import StandardScaler
from statsmodels.stats.multitest import multipletests
from statsmodels.tsa.stattools import grangercausalitytests


def _partial_corr(df: pd.DataFrame) -> pd.DataFrame:
    X = (df - df.mean()) / df.std(ddof=0)
    X = X.dropna(axis=1, how="any").dropna()
    if X.empty:
        return pd.DataFrame()
    cov = np.cov(X.values, rowvar=False)
    precision = np.linalg.pinv(cov)
    diag = np.sqrt(np.diag(precision))
    pcorr = -precision / np.outer(diag, diag)
    np.fill_diagonal(pcorr, 1.0)
    return pd.DataFrame(pcorr, index=X.columns, columns=X.columns)


def _mi(a: np.ndarray, b: np.ndarray) -> float:
    mask = np.isfinite(a) & np.isfinite(b)
    if mask.sum() < 30:
        return 0.0
    scaler = StandardScaler()
    x_scaled = scaler.fit_transform(a[mask].reshape(-1, 1))
    score = mutual_info_regression(x_scaled, b[mask], discrete_features=False, random_state=0)
    return float(score[0])


def _rolling_consistency(df: pd.DataFrame, window: int = 20, threshold: float = 0.3) -> pd.DataFrame:
    cols = df.columns
    hits = pd.DataFrame(0.0, index=cols, columns=cols)
    total = 0
    for end in range(window, len(df) + 1):
        subset = df.iloc[end - window : end]
        corr = subset.corr().abs()
        hits = hits.add((corr >= threshold).astype(float), fill_value=0.0)
        total += 1
    return hits / total if total else hits


def _granger(df: pd.DataFrame, maxlag: int = 4) -> Tuple[pd.DataFrame, pd.DataFrame]:
    cols = df.columns
    pvals = pd.DataFrame(np.nan, index=cols, columns=cols)
    clean = df.dropna()
    for a, b in itertools.permutations(cols, 2):
        try:
            tests = grangercausalitytests(clean[[b, a]], maxlag=maxlag, verbose=False)
            pvals.loc[a, b] = min(result[0]["ssr_ftest"][1] for result in tests.values())
        except Exception:
            continue
    flat = pvals.values.flatten()
    mask = ~np.isnan(flat)
    decisions = np.zeros_like(flat, dtype=bool)
    if mask.sum():
        decisions[mask] = multipletests(flat[mask], alpha=0.05, method="fdr_bh")[0]
    significant = pd.DataFrame(decisions.reshape(pvals.shape), index=pvals.index, columns=pvals.columns)
    return pvals, significant


def discover_all(
    wide: pd.DataFrame,
    corr_thr_strong: float = 0.45,
    corr_thr_medium: float = 0.30,
    maxlag: int = 4,
) -> pd.DataFrame:
    dense = wide.dropna(axis=1, how="any").dropna()
    if dense.shape[1] < 3:
        return pd.DataFrame(columns=["a", "b", "corr", "pcorr", "mi", "consistency", "gr_ab", "gr_ba", "score", "tier"])

    corr = dense.corr()
    pcorr = _partial_corr(dense).reindex_like(corr).fillna(0.0)
    pvals, gr_sig = _granger(dense, maxlag=maxlag)
    consistency = _rolling_consistency(dense, window=min(20, max(6, len(dense) // 5)), threshold=corr_thr_medium)

    results = []
    cols = list(dense.columns)
    for i in range(len(cols)):
        for j in range(i + 1, len(cols)):
            a, b = cols[i], cols[j]
            corr_val = float(corr.loc[a, b])
            pcorr_val = float(pcorr.loc[a, b]) if a in pcorr.index and b in pcorr.columns else 0.0
            mi_val = _mi(dense[a].values, dense[b].values)
            g_ab = bool(gr_sig.loc[a, b]) if a in gr_sig.index and b in gr_sig.columns else False
            g_ba = bool(gr_sig.loc[b, a]) if b in gr_sig.index and a in gr_sig.columns else False
            cons_val = float(consistency.loc[a, b]) if a in consistency.index and b in consistency.columns else 0.0
            score = (
                abs(corr_val)
                + 0.5 * abs(pcorr_val)
                + 0.3 * mi_val
                + 0.2 * (g_ab + g_ba)
                + 0.3 * cons_val
            )
            tier = "strong" if abs(corr_val) >= corr_thr_strong else "medium" if abs(corr_val) >= corr_thr_medium else "weak"
            results.append(
                {
                    "a": a,
                    "b": b,
                    "corr": corr_val,
                    "pcorr": pcorr_val,
                    "mi": mi_val,
                    "consistency": cons_val,
                    "gr_ab": g_ab,
                    "gr_ba": g_ba,
                    "score": score,
                    "tier": tier,
                }
            )
    return pd.DataFrame(results).sort_values("score", ascending=False)
