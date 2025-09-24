"""Local projection utilities for impulse-to-target responses."""

from __future__ import annotations

from typing import Dict, Iterable, Tuple

import pandas as pd
from sklearn.linear_model import LinearRegression


def local_projections(
    df: pd.DataFrame,
    shock: str,
    targets: Iterable[str],
    horizons: Tuple[int, ...] = (1, 4, 8),
) -> Dict[str, Dict[int, float]]:
    """Estimate local projection betas for ``shock`` impacting ``targets``."""

    out: Dict[str, Dict[int, float]] = {target: {} for target in targets}
    if df.empty or shock not in df.columns:
        return out

    clean = df.dropna()
    if clean.empty:
        return out

    x = clean[[shock]].values
    for target in targets:
        if target not in clean.columns:
            continue
        y = clean[target].values
        for horizon in horizons:
            if horizon <= 0:
                continue
            if len(clean) <= horizon + 2:
                continue
            y_future = y[horizon:]
            x_current = x[:-horizon]
            if y_future.shape[0] != x_current.shape[0]:
                continue
            model = LinearRegression().fit(x_current, y_future)
            out[target][horizon] = float(model.coef_[0])
    return out
