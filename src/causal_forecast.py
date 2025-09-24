"""Stubs for causal modelling and forecasting utilities."""

from __future__ import annotations

from typing import Any, Optional

import pandas as pd


def fit_var_stub(df: pd.DataFrame) -> Optional[Any]:
    """Placeholder for a future VAR/SVAR/DFM fitting routine."""

    _ = df
    return None


def irf_stub(model: Any, periods: int = 8) -> Optional[Any]:
    """Placeholder for impulse response generation."""

    _ = model, periods
    return None
