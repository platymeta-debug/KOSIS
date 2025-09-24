from __future__ import annotations
import os, argparse, pandas as pd
from pathlib import Path
from src.harvest import harvest
from src.normalize import normalize_latest_snapshot
from src.db import connect
from src.atlas import rolling_corr, granger_matrix, top_pairs
from src.model import fit_var, irf
from src.scenario import load as load_scen, apply as apply_scen
from src.mapping import toy_asset_map
from src.report import render_report

def wide_from_obs(target_freq="Q", pick_cols=None, limit=200):
    con = connect()
    # series_key를 logical_name만 남기고(차원 축약) 상위 n개만 쓰기
    df = con.execute("""
      SELECT series_key, period, value
      FROM obs
    """).df()
    # logical_name 추출
    df["logical_name"] = df["series_key"].str.split("|").str[0]
    # 최근 길이 기준 상위 limit
    lens = df.groupby("logical_name")["period"].nunique().sort_values(ascending=False)
    keep = lens.head(limit).index if pick_cols is None else pick_cols
    df = df[df["logical_name"].isin(keep)]
    # 간단 pivot (문자 period 정렬 주의: 실제에선 Period 변환 권장)
    piv = df.pivot_table(index="period", columns="logical_name", values="value")
    piv = piv.sort_index()
    # 결측 보간/앞뒤 채우기 약하게
    piv = piv.interpolate(limit_direction="both")
    return piv

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series-yaml", default="series.yaml")
    ap.add_argument("--mode", choices=["full","update"], default="update",
                    help="full: 전수 재수집, update: 증분만")
    ap.add_argument("--scenario", default=None, help="YAML 시나리오")
    ap.add_argument("--report", default="out/report.md")
    args = ap.parse_args()

    Path("out").mkdir(exist_ok=True)

    # 1) 수집 (전수/증분)
    harvest(args.series_yaml, mode=args.mode)

    # 2) 정규화
    normalize_latest_snapshot()

    # 3) 분석: 상관/그랜저 → 거시 bullet
    wide = wide_from_obs(limit=200)   # 데이터 많으면 숫자 늘려도 됨
    rc = rolling_corr(wide.dropna(axis=1, how="any"), window=20)
    last_corr = list(rc.values())[-1] if rc else wide.corr()
    tops = top_pairs(last_corr, n=5)
    bullets = [f"{a} ↔ {b} 상관 {r:.2f}" for a,b,r in tops]

    # 4) VAR/IRF + 시나리오
    # (컬럼 수가 많을 수 있으니 우선 핵심 소수만 골라 예시)
    core = [c for c in wide.columns if c.startswith("macro.")][:6]
    data = wide[core].dropna()
    v = fit_var(data, lags=2)
    i = irf(v, periods=8)
    if args.scenario and Path(args.scenario).exists():
        scen = load_scen(args.scenario)
        resp = apply_scen(i, list(data.columns), scen.get("shocks", {}), horizon=scen.get("horizon_quarters",8))
    else:
        # 디폴트: 금리 +50bp
        resp = apply_scen(i, list(data.columns), {"macro.policy_rate": 0.5}, horizon=8)

    # 5) 투자 매핑(토이)
    assets = toy_asset_map(list(data.columns), resp[-1])

    # 6) 리포트
    render_report(args.report, bullets, assets)
    print(f"[pipeline] report → {args.report}")

if __name__ == "__main__":
    main()
