from __future__ import annotations

from typing import Dict, List, Tuple

import numpy as np
import pandas as pd
from sklearn.linear_model import LinearRegression



def local_projection_beta(
    df: pd.DataFrame,
    shock_col: str,
    asset_cols: List[str],
    horizons: Tuple[int, ...] = (1, 4),
):
    out: Dict[str, Dict[int, float]] = {a: {} for a in asset_cols}
    z = df[[shock_col] + asset_cols].dropna()
    for a in asset_cols:
        y = z[a].values
        x = z[shock_col].values.reshape(-1, 1)
        for h in horizons:
            if len(y) <= h + 1:
                continue
            yt = y[h:]
            xt = x[:-h]
            mdl = LinearRegression().fit(xt, yt)
            out[a][h] = float(mdl.coef_[0])
    return out


def summarize_tilt(
    betas: Dict[str, Dict[int, float]],
    horizon: int = 4,
    top: int = 8,
):
    rows = []
    for a, d in betas.items():
        b = d.get(horizon, np.nan)
        rows.append((a, b))
    rows = sorted(rows, key=lambda x: (0 if np.isnan(x[1]) else -abs(x[1])))
    out = []
    for a, b in rows:
        if np.isnan(b):
            stance = "Neutral"
        else:
            stance = "Overweight" if b > 0 else "Underweight"
        out.append({"asset": a, "beta_h" + str(horizon): b, "stance": stance})
    df = pd.DataFrame(out).sort_values("beta_h" + str(horizon), ascending=False)
    return df.head(top), df.tail(top)


def build_portfolio_weights(tilt_df: pd.DataFrame, neutral_weight: float | None = None):
    n = len(tilt_df)
    if n == 0:
        return pd.Series(dtype=float)
    if neutral_weight is None:
        neutral_weight = 1.0 / n
    w = pd.Series(neutral_weight, index=tilt_df["asset"])
    for _, r in tilt_df.iterrows():
        if r["stance"] == "Overweight":
            w[r["asset"]] += 0.01
        elif r["stance"] == "Underweight":
            w[r["asset"]] -= 0.01
    w = (w.clip(lower=0) / w.clip(lower=0).sum()).sort_values(ascending=False)
    return w
