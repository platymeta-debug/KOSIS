from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path

import pandas as pd

from .report6_utils import fmt_float


def _pairs_to_md(df: pd.DataFrame) -> list[str]:
    lines: list[str] = []
    if df is None or df.empty:
        lines.append("- (no items)")
        return lines
    for _, row in df.iterrows():
        corr = fmt_float(row.get("corr"))
        cons = fmt_float(row.get("consistency"))
        label = row.get("reliability_label", "")
        lines.append(
            f"- **{row['a']}** ↔ **{row['b']}**: corr {corr}, cons {cons} · {label}"
        )
    return lines


def _lp_to_md(df: pd.DataFrame) -> list[str]:
    lines: list[str] = []
    if df is None or df.empty:
        return ["- (no LP betas)"]
    for _, row in df.iterrows():
        name = row.get("target", "")
        beta4 = fmt_float(row.get("beta_h4"))
        lines.append(f"- {name}: β@h4={beta4}")
    return lines


def _irf_to_md(df: pd.DataFrame) -> list[str]:
    lines: list[str] = []
    if df is None or df.empty:
        return ["- (no IRF)"]
    for _, row in df.iterrows():
        lines.append(
            f"- {row['shock']} → {row['target']} @h={int(row['h'])}: {fmt_float(row['resp'])}"
        )
    return lines


def _scn_to_md(df: pd.DataFrame) -> list[str]:
    lines: list[str] = []
    if df is None or df.empty:
        return ["- (no scenario effects)"]
    for _, row in df.iterrows():
        lines.append(f"- {row['target']}: Δ {fmt_float(row['effect'])}")
    return lines


def render_markdown(bundle: dict, out_md: str) -> None:
    Path(os.path.dirname(out_md)).mkdir(parents=True, exist_ok=True)
    kpi = bundle["kpi"]
    md: list[str] = []
    md.append("# KOSIS Cross-Domain Intelligence — Final Report")
    md.append(f"_Generated: {datetime.now().isoformat()}_  \n")
    md.append(
        "**Coverage**: "
        f"pairs={kpi['n_pairs']} · "
        f"strong={kpi['share_strong'] * 100:.1f}% · "
        f"medium={kpi['share_med'] * 100:.1f}% · "
        f"reliability(avg)={kpi['avg_reliability']}"
    )
    md.append("\n---\n")
    md.append("## Executive Summary")
    md.extend(
        [
            "- 최상위 Cross-Domain 5개, IRF/시나리오 하이라이트, 투자 틸트(요약)를 아래 섹션에서 확인하세요.",
            "",
        ]
    )
    md.append("## Cross-Domain Insights (Top)")
    md.extend(_pairs_to_md(bundle["cross_top"].head(12)))
    md.append("")

    md.append("## Sections")
    for section in [
        "Economy",
        "Corporate",
        "Finance",
        "Real Estate",
        "Debt",
        "Growth",
        "Investment",
        "Assets",
    ]:
        md.append(f"### {section}")
        md.extend(_pairs_to_md(bundle["sec_top"].get(section)))
        md.append("")

    md.append("## IRF Highlights")
    md.extend(_irf_to_md(bundle["irf_top"]))
    md.append("")

    md.append("## Investment Tilt (Local Projections β)")
    md.extend(_lp_to_md(bundle["lp_top"]))
    md.append("")

    md.append("## Scenario Effects (@h=4 default)")
    md.extend(_scn_to_md(bundle["scn_summary"]))
    md.append("")

    md.append("\n---\n_notes_: corr=상관, cons=롤링지속률, reliability=합성 신뢰도(High/Med/Low).")

    with open(out_md, "w", encoding="utf-8") as f:
        f.write("\n".join(md))


def render_html(markdown_path: str, out_html: str) -> None:
    Path(os.path.dirname(out_html)).mkdir(parents=True, exist_ok=True)
    with open(markdown_path, "r", encoding="utf-8") as f:
        md_content = f.read()

    html = f"""<!doctype html>
<html lang=\"ko\"><head>
<meta charset=\"utf-8\"><meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">
<title>KOSIS Final Report</title>
<style>
body{{font-family: ui-sans-serif, system-ui, -apple-system, 'Segoe UI', Roboto, 'Noto Sans KR'; line-height:1.5; padding:24px; background:#f8fafc;}}
h1,h2,h3{{margin-top:1.2em}}
code,pre{{background:#0b1020; color:#e5e7eb; padding:12px; border-radius:8px; display:block; white-space:pre-wrap}}
hr{{border:none; height:1px; background:#e2e8f0; margin:24px 0}}
</style>
</head><body>
<pre>{md_content}</pre>
</body></html>"""

    with open(out_html, "w", encoding="utf-8") as f:
        f.write(html)
