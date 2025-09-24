from __future__ import annotations

import pandas as pd

SECTIONS = {
    "Economy": ["macro.", "gdp", "cpi", "ppi", "trade", "fx", "population"],
    "Corporate": ["firm.", "employment", "wage", "sales", "capex", "industry"],
    "Finance": ["policy_rate", "m2", "credit", "spread", "loan_to_deposit", "bank_loan", "npl"],
    "Investment": ["asset.", "etf", "reits", "bond", "gold"],
    "Real Estate": ["real_estate.", "housing", "rent", "construction", "permit"],
    "Debt": ["debt", "dsr", "dti", "leverage", "npl"],
    "Growth": ["potential", "tfp", "productivity", "growth", "income", "consumption", "saving", "participation"],
    "Assets": ["asset.", "fx.", "commodity.", "bond.", "equity."],
}


def route_section(name: str) -> str:
    n = name.lower()
    for sec, keys in SECTIONS.items():
        if any(n.startswith(k) or (k in n) for k in keys):
            return sec
    return "Assets"


def sectionize(eff_df: pd.DataFrame, h_pick: int = 4) -> dict:
    mix = eff_df[(eff_df["h"] == h_pick) & (eff_df["source"] == "mix")].copy()
    mix["section"] = mix["target"].map(route_section)
    out: dict[str, pd.DataFrame] = {}
    for sec, grp in mix.groupby("section"):
        out[sec] = grp[["target", "effect"]].sort_values("effect", key=lambda s: s.abs(), ascending=False)
    return out


def render_markdown(name: str, summary_at_h: int, section_maps: dict) -> str:
    lines = [f"# Scenario: {name}", f"_Summary @h={summary_at_h}_", ""]
    for sec in [
        "Economy",
        "Corporate",
        "Finance",
        "Real Estate",
        "Debt",
        "Growth",
        "Investment",
        "Assets",
    ]:
        lines.append(f"## {sec}")
        df = section_maps.get(sec)
        if df is None or df.empty:
            lines.append("- (no material effect)")
        else:
            top = df.head(12)
            for _, r in top.iterrows():
                lines.append(f"- {r['target']}: {r['effect']:.3f}")
        lines.append("")
    return "\n".join(lines)
