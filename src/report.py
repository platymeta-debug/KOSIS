from __future__ import annotations
import pandas as pd
from datetime import datetime

def macro_summary_text(corr_top, granger_df) -> str:
    lines = ["### Macro Conclusions (Auto)\n"]
    for (a,b,r) in corr_top[:5]:
        lines.append(f"- 최근 창에서 **{a}**↔**{b}** 상관: {r:.2f}")
    lines.append("")    
    # simple granger summary
    try:
        sig = (granger_df < 0.05).sum().sum()
        lines.append(f"- 그랜저 유의 관계 수: **{sig}** (p<0.05)\n")
    except Exception:
        pass
    return "\n".join(lines)

def investment_table_text(df_assets: pd.DataFrame) -> str:
    lines = ["### Investment Outlook (Toy Mapping)\n", "| Asset | Score | Stance |", "|---|---:|---|" ]
    for _, row in df_assets.iterrows():
        lines.append(f"| {row['asset']} | {row['score']:.3f} | {row['stance']} |")
    lines.append("")
    return "\n".join(lines)

def build_report(path: str, corr_top, granger_df, assets_df):
    with open(path, 'w', encoding='utf-8') as f:
        f.write(f"# Macro→Investment Report\n\nGenerated: {datetime.now().isoformat()}\n\n")
        f.write(macro_summary_text(corr_top, granger_df))
        f.write("\n\n")
        f.write(investment_table_text(assets_df))
        f.write("\n\n> NOTE: This is a demo using mock data and toy asset mapping. Plug in real KOSIS series and refine mappings.\n")
