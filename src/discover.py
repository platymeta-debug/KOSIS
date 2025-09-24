"""Discovery stage helpers for brute-force signal ranking."""

from __future__ import annotations

import numpy as np
import pandas as pd


def quick_discover(wide: pd.DataFrame) -> pd.DataFrame:
    """Compute a simple correlation-based ranking across series pairs."""

    dense = wide.dropna(axis=1, how="any")
    corr_matrix = dense.corr()
    pairs = []
    columns = list(corr_matrix.columns)
    for i, lhs in enumerate(columns):
        for j in range(i + 1, len(columns)):
            rhs = columns[j]
            corr_value = float(corr_matrix.iloc[i, j])
            pairs.append({
                "a": lhs,
                "b": rhs,
                "corr": corr_value,
                "score": abs(corr_value),
            })
    result = pd.DataFrame(pairs).sort_values("score", ascending=False)
    result["tier"] = np.where(
        result["score"] >= 0.45,
        "strong",
        np.where(result["score"] >= 0.3, "medium", "weak"),
    )
    return result
