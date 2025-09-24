"""Feature engineering helpers for normalised observation tables."""

from __future__ import annotations

import pandas as pd
import numpy as np

from .store import con
from .qc import basic_qc


def load_obs(limit: int = 1200) -> pd.DataFrame:
    """Load the longest logical series from the obs table."""

    connection = con()
    try:
        df = connection.execute("SELECT series_key, period, value FROM obs").df()
    finally:
        connection.close()
    if df.empty:
        return df
    df["logical_name"] = df["series_key"].str.split("|").str[0]
    lengths = df.groupby("logical_name")["period"].nunique().sort_values(ascending=False)
    keep = lengths.head(limit).index
    return df[df["logical_name"].isin(keep)]


def pivot_wide(df_obs: pd.DataFrame) -> pd.DataFrame:
    """Pivot observations into a period-indexed wide matrix."""

    if df_obs.empty:
        return pd.DataFrame()
    wide = (
        df_obs.pivot_table(index="period", columns="logical_name", values="value")
        .sort_index()
    )
    return wide.interpolate(limit_direction="both")


def add_derivatives(wide: pd.DataFrame) -> pd.DataFrame:
    """Append derivative indicators such as YoY growth, spreads, and ratios."""

    out = wide.copy()
    if len(out) > 12:
        yoy = out.pct_change(12)
        yoy.columns = [f"{col}__yoy" for col in out.columns]
        out = pd.concat([out, yoy], axis=1)
    if len(out) > 4:
        qoq4 = out.pct_change(4)
        qoq4.columns = [f"{col}__q4" for col in out.columns]
        out = pd.concat([out, qoq4], axis=1)

    def _maybe(name: str) -> list[str]:
        lower = name.lower()
        return [col for col in out.columns if lower in col.lower()]

    short = _maybe("short") + _maybe("3m")
    long = _maybe("long") + _maybe("10y")
    if short and long:
        out["rates__term_spread"] = out[long[0]] - out[short[0]]

    loans = _maybe("loan")
    deposits = _maybe("deposit")
    if loans and deposits:
        denom = out[deposits[0]].replace(0, np.nan)
        out["bank__loan_to_deposit"] = out[loans[0]] / denom
    return out


def build_wide(limit: int = 1200) -> pd.DataFrame:
    """Construct a QC-ed wide matrix with engineered derivative features."""

    obs = load_obs(limit=limit)
    wide = pivot_wide(obs)
    if wide.empty:
        return wide
    wide = basic_qc(wide, min_len=24)
    wide = add_derivatives(wide)
    wide = wide.loc[:, wide.notna().sum() > 0]
    return wide
