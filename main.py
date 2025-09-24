from __future__ import annotations
import argparse, os
import pandas as pd
import numpy as np
from pathlib import Path
from src.kosis_client import load_mock, fetch_kosis_series, load_series_yaml
from src.etl import standardize, transform_growth
from src.atlas import rolling_corr, granger_matrix, summarize_top_relations
from src.model import fit_var, compute_irf, baseline_forecast
from src.scenario import load_scenario, run_scenario
from src.mapping import simple_asset_mapping
from src.report import build_report


def ensure_dir(p: str):
    Path(p).mkdir(parents=True, exist_ok=True)


def run_demo(use_mock: bool, scenario_path: str|None, report_path: str|None, series_yaml: str|None):
    outdir = Path("out")
    ensure_dir(outdir)
    ensure_dir(outdir/"atlas")
    ensure_dir(outdir/"irf")
    ensure_dir(outdir/"scenario")

    # 1) Load data
    if series_yaml:
        # 실 KOSIS
        entries = load_series_yaml(series_yaml)
        # 필요하면 전체 기간 옵션으로 덮어쓰기 가능: start/end 인자 추가
        df = fetch_kosis_series([e.__dict__ for e in entries], target_freq="Q", use_cache=True)
        if df.empty:
            raise RuntimeError("No data fetched from KOSIS. Check series.yaml/orgId/tblId/userStatsId and API key.")
    elif use_mock:
        df = load_mock()
    else:
        raise NotImplementedError("Implement KOSIS fetch or pass --use-mock / --series-yaml")

    # 2) ETL
    df = standardize(df)
    df_t = transform_growth(df)
    # Choose a small working set for VAR (avoid too many columns)
    work_cols = [c for c in ["population_total_dlog","policy_rate","m2_dlog","house_price_dlog","bank_loan_dlog","cpi_dlog","gdp_growth"] if c in df_t.columns]
    data = df_t[work_cols].dropna()

    # 3) Correlation Atlas
    rc = rolling_corr(data, window=20)
    last_corr = list(rc.values())[-1]
    top = summarize_top_relations(last_corr, top_n=10)
    try:
        gr = granger_matrix(data, maxlag=4, p_threshold=0.05)
    except Exception:
        gr = pd.DataFrame()

    # 4) VAR + IRF
    var_res = fit_var(data, lags=2)
    irf = compute_irf(var_res, periods=8)

    # 5) Scenario (optional)
    if scenario_path and Path(scenario_path).exists():
        import yaml
        with open(scenario_path, 'r', encoding='utf-8') as f:
            scen = yaml.safe_load(f)
        shocks = scen.get('shocks', {})
        horizon = scen.get('horizon_quarters', 8)
        resp = run_scenario(irf, list(data.columns), shocks, horizon=horizon)
        assets = simple_asset_mapping(list(data.columns), resp)
    else:
        # Use a default simple shock
        shocks = {"policy_rate": 0.5}  # +50bp toy
        resp = run_scenario(irf, list(data.columns), shocks, horizon=8)
        assets = simple_asset_mapping(list(data.columns), resp)

    # 6) Report
    if report_path is None:
        report_path = outdir/"report.md"
    build_report(str(report_path), top, gr, assets)
    print(f"Report written to {report_path}")


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--use-mock", action="store_true", help="Use mock local CSV instead of KOSIS API")
    ap.add_argument("--series-yaml", type=str, default=None, help="Path to series.yaml for real KOSIS fetch")
    ap.add_argument("--scenario", type=str, default=None, help="Path to scenario YAML")
    ap.add_argument("--report", type=str, default=None, help="Path to markdown report output")
    args = ap.parse_args()
    run_demo(args.use_mock, args.scenario, args.report, args.series_yaml)
