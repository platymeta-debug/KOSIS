"""Helpers for selecting core variables used in causal modelling."""

from __future__ import annotations

from typing import Iterable, List

import pandas as pd


def select_core_vars(
    wide: pd.DataFrame, top_n: int = 8, prefer: Iterable[str] | None = None
) -> List[str]:
    """Select a shortlist of high-quality series from ``wide``.

    The heuristic favours columns with few missing values and large
    variability, while optionally forcing the inclusion of ``prefer``
    series when available.
    """

    if wide.empty:
        return []

    complete = wide.dropna(axis=1, how="any")
    if complete.empty:
        return []

    stds = complete.std().sort_values(ascending=False)
    candidates = list(stds.head(top_n).index)

    if prefer:
        preferred = [name for name in prefer if name in complete.columns]
        for name in preferred:
            if name in candidates:
                continue
            if len(candidates) < top_n:
                candidates.append(name)
            else:
                # Replace the lowest-ranked candidate to honour preference.
                candidates[-1] = name

    # Preserve column order as they appear in ``wide`` for downstream
    # deterministic modelling.
    ordered = [col for col in wide.columns if col in candidates]
    return ordered
