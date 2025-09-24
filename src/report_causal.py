"""Textual reporting helpers for the causal modelling stage."""

from __future__ import annotations

from pathlib import Path
from typing import Dict, Iterable, Optional

import pandas as pd


def _stationarity_summary(table: pd.DataFrame) -> str:
    if table.empty:
        return "- 변환 후 정상성 진단을 계산할 충분한 표본이 없습니다."

    stable = int((table["adf_p_trans"] <= 0.05).sum())
    total = int(table.shape[0])
    worst = table.sort_values("adf_p_trans", ascending=False).head(3)
    lines = [f"- 정상 판정 변수: {stable}/{total} (ADF p<=0.05 기준)"]
    if not worst.empty:
        snippet = ", ".join(
            f"{row.var} (ADF {row.adf_p_trans:.2f}, KPSS {row.kpss_p_trans:.2f})"
            for row in worst.itertuples()
        )
        lines.append(f"- 추가 차분 검토 대상: {snippet}")
    return "\n".join(lines)


def top_irf(irf: Optional[pd.DataFrame], top_n: int = 5) -> pd.DataFrame:
    """Return the highest absolute IRF effects."""

    if irf is None or irf.empty:
        return pd.DataFrame(columns=["response", "shock", "horizon", "value", "abs"])

    df = irf[irf["horizon"] > 0].copy()
    if df.empty:
        return pd.DataFrame(columns=df.columns)

    df["abs"] = df["value"].abs()
    return df.sort_values("abs", ascending=False).head(top_n)


def _format_irf_section(irf: Optional[pd.DataFrame], top_n: int = 5) -> str:
    if irf is None or irf.empty:
        return "- 유효한 충격 반응 함수가 생성되지 않았습니다."

    rows = top_irf(irf, top_n=top_n)
    if rows.empty:
        return "- IRF 분석에 사용할 유효한 데이터가 부족합니다."

    lines = []
    for row in rows.itertuples():
        lines.append(
            f"- {row.shock} → {row.response} (h={row.horizon}): {row.value:+.3f}"
        )
    return "\n".join(lines)


def _format_forecast_section(rmse: Optional[pd.Series]) -> str:
    if rmse is None or rmse.empty:
        return "- 롤링 예측 RMSE를 계산할 수 없습니다."

    parts = [f"- {name}: {float(value):.3f}" for name, value in rmse.sort_values().items()]
    return "\n".join(parts)


def _format_lp_section(lp_df: Optional[pd.DataFrame]) -> str:
    if lp_df is None or lp_df.empty:
        return "- LP 추정에서 유의한 계수를 확보하지 못했습니다."

    rows = lp_df.sort_values("abs_beta", ascending=False).head(6)
    lines = []
    for row in rows.itertuples():
        lines.append(
            f"- {row.shock} 쇼크 → {row.target} (h={row.horizon}): β={row.beta:+.3f}"
        )
    return "\n".join(lines)


def render_report(
    path: str | Path,
    *,
    core_vars: Iterable[str],
    transform_map: Dict[str, str],
    stationarity: pd.DataFrame,
    irf: Optional[pd.DataFrame],
    rmse: Optional[pd.Series],
    lp_df: Optional[pd.DataFrame],
    notes: Optional[Dict[str, str]] = None,
) -> None:
    """Write a plain-text causal diagnostics summary."""

    destination = Path(path)
    destination.parent.mkdir(parents=True, exist_ok=True)

    lines = ["# Step4 Causal Diagnostics", ""]
    lines.append("## Core Variables")
    core_list = list(core_vars)
    if core_list:
        lines.append("- " + ", ".join(core_list))
    else:
        lines.append("- 선택된 변수 없음")

    if transform_map:
        lines.append("\n## Transformations")
        for name, tfm in transform_map.items():
            lines.append(f"- {name}: {tfm}")

    lines.append("\n## Stationarity")
    lines.append(_stationarity_summary(stationarity))

    lines.append("\n## Impulse Responses")
    lines.append(_format_irf_section(irf))

    lines.append("\n## Rolling Forecast RMSE")
    lines.append(_format_forecast_section(rmse))

    lines.append("\n## Local Projections")
    lines.append(_format_lp_section(lp_df))

    if notes:
        lines.append("\n## Notes")
        for key, value in notes.items():
            lines.append(f"- {key}: {value}")

    destination.write_text("\n".join(lines), encoding="utf-8")
