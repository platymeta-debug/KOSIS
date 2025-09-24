"""Feature engineering helpers for the discovery stage."""

from __future__ import annotations

import pandas as pd


def build_wide(df_obs: pd.DataFrame, limit: int = 600) -> pd.DataFrame:
    """Return a wide pivoted dataframe from normalised observations."""

    df = df_obs.copy()
    df["logical_name"] = df["series_key"].str.split("|").str[0]
    counts = df.groupby("logical_name")["period"].nunique().sort_values(ascending=False)
    keep = counts.head(limit).index
    wide = (
        df[df["logical_name"].isin(keep)]
        .pivot_table(index="period", columns="logical_name", values="value")
        .sort_index()
    )
    return wide.interpolate(limit_direction="both")
