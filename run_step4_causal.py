"""Step 4 orchestration: causal models, IRF diagnostics, and forecasts."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Optional

import json
import numpy as np
import pandas as pd

from src import features, store
from src.causal_models import (
    fit_dfm,
    fit_svar_from_var,
    fit_var,
    irf_from_result,
    rolling_forecast_var,
)
from src.local_projections import local_projections
from src.model_select import select_core_vars
from src.report_causal import render_report
from src.stationarity import apply_transform, stationarity_report

ARTIFACT_DIR = Path("artifacts/step4")


def _load_obs() -> pd.DataFrame:
    connection = store.con()
    try:
        frame = connection.execute("SELECT * FROM obs").df()
    finally:
        connection.close()
    return frame


def _irf_to_frame(irf, names) -> Optional[pd.DataFrame]:
    if irf is None:
        return None
    try:
        arr = irf.irfs
    except Exception:
        return None

    rows = []
    horizons = range(arr.shape[0])
    for h in horizons:
        for i, response in enumerate(names):
            for j, shock in enumerate(names):
                rows.append(
                    {
                        "horizon": int(h),
                        "response": response,
                        "shock": shock,
                        "value": float(arr[h, i, j]),
                    }
                )
    return pd.DataFrame(rows)


def _save_dataframe(df: Optional[pd.DataFrame], path: Path) -> None:
    if df is None or df.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def _save_series(series: Optional[pd.Series], path: Path) -> None:
    if series is None or series.empty:
        return
    path.parent.mkdir(parents=True, exist_ok=True)
    series.to_csv(path, header=["value"])


def main() -> None:
    store.init()
    ARTIFACT_DIR.mkdir(parents=True, exist_ok=True)

    obs = _load_obs()
    if obs.empty:
        print("[step4] obs 테이블이 비어 있습니다. 먼저 준비 단계를 실행하세요.")
        return

    wide = features.build_wide(obs)
    if wide.empty:
        print("[step4] 피벗된 데이터가 없습니다.")
        return

    top_n = min(8, wide.shape[1]) if wide.shape[1] else 0
    core_vars = select_core_vars(wide, top_n=top_n)
    if not core_vars:
        core_vars = list(wide.columns[:top_n])
    if not core_vars:
        print("[step4] 핵심 변수 선택에 실패했습니다.")
        return

    core_frame = wide[core_vars]
    transformed, transform_map = apply_transform(core_frame)
    if transformed.empty:
        print("[step4] 변환 후 유효한 표본이 없습니다.")
        return

    stationarity_df = stationarity_report(core_frame, transformed, transform_map)
    stationarity_path = ARTIFACT_DIR / "stationarity.csv"
    stationarity_df.to_csv(stationarity_path, index=False)
    with (ARTIFACT_DIR / "transform_map.json").open("w", encoding="utf-8") as handle:
        json.dump(transform_map, handle, ensure_ascii=False, indent=2)

    var_res = fit_var(transformed)
    var_irf_df: Optional[pd.DataFrame] = None
    notes: Dict[str, str] = {}
    if var_res is not None:
        names = list(var_res.names)
        var_irf = irf_from_result(var_res, periods=8)
        var_irf_df = _irf_to_frame(var_irf, names)
        if var_irf_df is not None:
            _save_dataframe(var_irf_df, ARTIFACT_DIR / "var_irf.csv")
        notes["VAR"] = (
            f"lags={var_res.k_ar}, AIC={var_res.aic:.2f}, BIC={var_res.bic:.2f}"
        )
    else:
        print("[step4] VAR 추정에 실패했습니다.")

    svar_irf_df: Optional[pd.DataFrame] = None
    if var_res is not None:
        try:
            chol = np.linalg.cholesky(var_res.sigma_u)
        except Exception:
            chol = None
        svar_res = fit_svar_from_var(var_res, B=chol)
        if svar_res is not None:
            names = list(var_res.names)
            svar_irf = irf_from_result(svar_res, periods=8)
            svar_irf_df = _irf_to_frame(svar_irf, names)
            if svar_irf_df is not None:
                _save_dataframe(svar_irf_df, ARTIFACT_DIR / "svar_irf.csv")
            notes["SVAR"] = f"converged in {getattr(svar_res, 'iterations', 'n/a')} iterations"

    dfm_res = fit_dfm(transformed, k_factors=1, factor_order=1)
    if dfm_res is not None:
        try:
            notes["DFM"] = f"logL={dfm_res.llf:.2f}, AIC={dfm_res.aic:.2f}"
        except Exception:
            notes["DFM"] = "dynamic factor model fitted"

    rmse_series: Optional[pd.Series] = None
    if var_res is not None:
        bundle = rolling_forecast_var(transformed, lags=var_res.k_ar or 1, test_size=8)
        if bundle:
            rmse_series = bundle.get("rmse")
            _save_dataframe(bundle.get("pred"), ARTIFACT_DIR / "var_forecast.csv")
            _save_dataframe(bundle.get("test"), ARTIFACT_DIR / "var_actual.csv")
            if rmse_series is not None:
                _save_series(rmse_series, ARTIFACT_DIR / "var_rmse.csv")

    shock = core_vars[0]
    targets = core_vars[1:]
    lp_result = local_projections(transformed, shock=shock, targets=targets)
    lp_rows = []
    for target, items in lp_result.items():
        for horizon, beta in items.items():
            lp_rows.append(
                {
                    "shock": shock,
                    "target": target,
                    "horizon": int(horizon),
                    "beta": float(beta),
                    "abs_beta": abs(float(beta)),
                }
            )
    lp_df = pd.DataFrame(lp_rows)
    if not lp_df.empty:
        _save_dataframe(lp_df, ARTIFACT_DIR / "local_projections.csv")

    render_report(
        ARTIFACT_DIR / "summary.txt",
        core_vars=core_vars,
        transform_map=transform_map,
        stationarity=stationarity_df,
        irf=var_irf_df if var_irf_df is not None else svar_irf_df,
        rmse=rmse_series,
        lp_df=lp_df if not lp_df.empty else None,
        notes=notes,
    )

    print("[step4] causal artefacts saved to", ARTIFACT_DIR)


if __name__ == "__main__":
    main()
