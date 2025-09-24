from __future__ import annotations

from datetime import datetime

import pandas as pd


def _domain(name: str) -> str:
    n = name.lower()
    if n.startswith("firm.") or any(
        k in n for k in ["employment", "wage", "sales", "capex", "rd_", "industry"]
    ):
        return "corporate"
    if n.startswith("asset.") or "etf" in n or "reits" in n or "bond" in n or "gold" in n:
        return "investment"
    if any(
        k in n
        for k in [
            "policy_rate",
            "m2",
            "credit",
            "loan_to_deposit",
            "spread",
            "npl",
            "bank_loan",
            "financ",
        ]
    ):
        return "finance"
    return "economy"


def split_by_domain(pairs: pd.DataFrame) -> dict:
    def pair_dom(a: str, b: str) -> str:
        da, db = _domain(a), _domain(b)
        if "investment" in (da, db):
            return "investment"
        if "finance" in (da, db):
            return "finance"
        if "corporate" in (da, db):
            return "corporate"
        return "economy"

    pairs = pairs.copy()
    pairs["domain"] = pairs.apply(lambda r: pair_dom(r["a"], r["b"]), axis=1)
    return {k: v for k, v in pairs.groupby("domain")}


def bullets_for(df: pd.DataFrame, top_strong: int = 8, top_medium: int = 5) -> list[str]:
    strong = df[df["tier"] == "strong"].head(top_strong)
    medium = df[df["tier"] == "medium"].head(top_medium)
    bl: list[str] = []
    for _, r in strong.iterrows():
        sig = " (Granger)" if (r["gr_ab"] or r["gr_ba"]) else ""
        bl.append(
            f"**{r['a']}** ↔ **{r['b']}**: 상관 {r['corr']:.2f}, 지속률 {r['consistency']:.2f}{sig}"
        )
    if len(medium):
        bl.append("_준유의 신호_")
        for _, r in medium.iterrows():
            bl.append(
                f"- {r['a']} ↔ {r['b']}: 상관 {r['corr']:.2f}, 지속률 {r['consistency']:.2f}"
            )
    return bl


def render_quad_report(
    path: str, pairs: pd.DataFrame, invest_table: pd.DataFrame | None = None
) -> None:
    dom = split_by_domain(pairs)
    with open(path, "w", encoding="utf-8") as f:
        f.write(
            "# Quad Report — Economy · Corporate · Investment · Finance\n"
            f"Generated: {datetime.now().isoformat()}\n\n"
        )
        for section in ["economy", "corporate", "finance", "investment"]:
            f.write(f"## {section.capitalize()}\n")
            if section in dom:
                bl = bullets_for(dom[section])
                for ln in bl:
                    f.write(f"- {ln}\n")
            else:
                f.write("- (유의미한 신호 없음)\n")
            if section == "investment" and invest_table is not None and len(invest_table):
                f.write(
                    "\n**Investment Tilt (β기반)**\n\n| Asset | Score/Beta | Stance |\n|---|---:|---|\n"
                )
                for _, r in invest_table.iterrows():
                    f.write(f"| {r['asset']} | {r['score']:.3f} | {r['stance']} |\n")
            f.write("\n")

