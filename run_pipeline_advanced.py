from __future__ import annotations

import argparse
from pathlib import Path

import pandas as pd

from src.atlas_advanced import (
    fdr_significance,
    granger_pvals,
    lead_lag,
    nl_summary,
    partial_corr,
)
from src.db import connect
from src.harvest import harvest
from src.mapping_beta import (
    build_portfolio_weights,
    local_projection_beta,
    summarize_tilt,
)
from src.models_advanced import fit_dfm, fit_svar, fit_var, irf_svar, irf_var
from src.normalize import normalize_latest_snapshot
from src.report import render_report
from src.scenario_dsl import apply as apply_scen
from src.scenario_dsl import load as load_scen



def wide_from_obs(limit: int = 300):
    con = connect()
    df = con.execute("SELECT series_key, period, value FROM obs").df()
    df["logical_name"] = df["series_key"].str.split("|").str[0]
    lens = df.groupby("logical_name")["period"].nunique().sort_values(ascending=False)
    keep = lens.head(limit).index
    piv = (
        df[df["logical_name"].isin(keep)]
        .pivot_table(index="period", columns="logical_name", values="value")
        .sort_index()
    )
    return piv.interpolate(limit_direction="both")



def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series-yaml", default="series.yaml")
    ap.add_argument("--mode", choices=["full", "update"], default="update")
    ap.add_argument("--scenario", default=None)
    ap.add_argument("--report", default="out/report.md")
    ap.add_argument("--shock-col", default="macro.policy_rate", help="β 추정용 충격 컬럼명")
    ap.add_argument("--asset-prefix", default="asset.", help="자산 시리즈 접두어 (obs.series_key 기준)")
    args = ap.parse_args()

    Path("out").mkdir(exist_ok=True)

    harvest(args.series_yaml, mode=args.mode)
    normalize_latest_snapshot()

    wide = wide_from_obs(limit=300)
    Z = wide.dropna(axis=1, how="any")
    if Z.shape[1] < 3:
        raise RuntimeError("시리즈가 너무 적습니다. series.yaml에 추가하세요.")

    pc = partial_corr(Z)
    gp = granger_pvals(Z, maxlag=4)
    gp_sig = fdr_significance(gp, alpha=0.05)
    ll = lead_lag(Z, max_lag=6)
    bullets = nl_summary(pc, gp_sig, ll, top_n=5)

    core = list(Z.columns[:6])
    var_res = fit_var(Z[core], lags=2)
    irf_v = irf_var(var_res, periods=8)
    try:
        svar_res = fit_svar(Z[core], lags=2)
        irf_s = irf_svar(svar_res, periods=8)
        irf_use = irf_s
    except Exception:
        irf_use = irf_v

    target = [c for c in Z.columns if c.endswith("gdp_growth")]
    if target:
        try:
            dfm = fit_dfm(Z[target + core].dropna(), k_factors=1, error_order=1)
        except Exception:
            pass

    if args.scenario and Path(args.scenario).exists():
        scen = load_scen(args.scenario)
    else:
        scen = {
            "horizon_quarters": 8,
            "shocks": {args.shock_col: {"type": "step", "value": 0.5}},
        }
    resp = apply_scen(irf_use, core, scen)

    asset_cols = [c for c in Z.columns if c.startswith(args.asset_prefix)]
    beta_df = pd.DataFrame()
    ow, uw = pd.DataFrame(), pd.DataFrame()
    if asset_cols and args.shock_col in Z.columns:
        betas = local_projection_beta(
            Z[[args.shock_col] + asset_cols],
            args.shock_col,
            asset_cols,
            horizons=(1, 4, 8),
        )
        beta_df = pd.DataFrame({a: betas[a] for a in betas}).T
        ow, uw = summarize_tilt(betas, horizon=4, top=8)
        weights = build_portfolio_weights(pd.concat([ow, uw]))
        weights.to_csv("out/portfolio_weights.csv")
    else:
        weights = pd.Series(dtype=float)

    inv_df = pd.DataFrame(columns=["asset", "score", "stance"])
    if not ow.empty or not uw.empty:
        tmp = pd.concat(
            [
                ow.assign(score=ow.filter(like="beta_").max(axis=1).values),
                uw.assign(score=uw.filter(like="beta_").max(axis=1).values),
            ]
        )
        tmp["stance"] = tmp["stance"].astype(str)
        inv_df = tmp[["asset", "score", "stance"]].reset_index(drop=True)
    render_report(
        args.report,
        bullets,
        inv_df
        if not inv_df.empty
        else pd.DataFrame(
            [
                {"asset": "(샘플) banks", "score": 0.1, "stance": "Overweight"},
                {"asset": "(샘플) growth_stocks", "score": -0.1, "stance": "Underweight"},
            ]
        ),
    )
    print(
        f"[advanced] report → {args.report}; weights(out/portfolio_weights.csv) rows={len(weights)}"
    )


if __name__ == "__main__":
    main()
