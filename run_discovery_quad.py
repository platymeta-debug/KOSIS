from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.db import connect
from src.discovery import discover_signals
from src.harvest import harvest
from src.mapping_beta import local_projection_beta, summarize_tilt
from src.normalize import normalize_latest_snapshot
from src.report_quad import render_quad_report


def wide_from_obs(limit: int = 500) -> pd.DataFrame:
    con = connect()
    df = con.execute("SELECT series_key, period, value FROM obs").df()
    df["logical_name"] = df["series_key"].str.split("|").str[0]
    lens = df.groupby("logical_name")["period"].nunique().sort_values(ascending=False)
    keep = lens.head(limit).index
    wide = (
        df[df["logical_name"].isin(keep)]
        .pivot_table(index="period", columns="logical_name", values="value")
        .sort_index()
    )
    return wide.interpolate(limit_direction="both")


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--series-yaml", default="series.yaml")
    ap.add_argument("--mode", choices=["full", "update"], default="update")
    ap.add_argument("--report", default="out/quad_report.md")
    ap.add_argument("--shock-col", default="macro.policy_rate")
    ap.add_argument("--asset-prefix", default="asset.")
    args = ap.parse_args()

    Path("out").mkdir(exist_ok=True)

    harvest(args.series_yaml, mode=args.mode)
    normalize_latest_snapshot()

    wide = wide_from_obs(limit=600)
    pairs = discover_signals(wide, corr_thr_strong=0.45, corr_thr_medium=0.3, maxlag=4)

    asset_cols = [c for c in wide.columns if c.startswith(args.asset_prefix)]
    inv_df: pd.DataFrame | None = None
    if asset_cols and args.shock_col in wide.columns:
        beta_df = local_projection_beta(
            wide[[args.shock_col] + asset_cols].dropna(), args.shock_col, asset_cols, horizons=(4,)
        )
        ow, uw = summarize_tilt(beta_df, horizon=4, top=8)
        inv_df = pd.concat([ow, uw], ignore_index=True)
        inv_df = inv_df.rename(columns={inv_df.columns[1]: "score"})

    render_quad_report(args.report, pairs, inv_df)
    print(f"[discovery] report → {args.report}, pairs={len(pairs)}")


if __name__ == "__main__":
    main()

