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
    value = (name or "").lower()
    for section, keys in SECTIONS.items():
        if any(value.startswith(key) or key in value for key in keys):
            return section
    return "Assets"


def add_section_cols(pairs: pd.DataFrame) -> pd.DataFrame:
    if pairs is None or pairs.empty:
        return pairs

    df = pairs.copy()
    df["a_sec"] = df["a"].map(route_section)
    df["b_sec"] = df["b"].map(route_section)
    df["same_section"] = df["a_sec"] == df["b_sec"]
    df["section"] = df.apply(
        lambda row: row["a_sec"] if row["same_section"] else "Cross-Domain", axis=1
    )
    return df


def top_cross_domain(pairs_with_sec: pd.DataFrame, top: int = 15) -> pd.DataFrame:
    if pairs_with_sec is None or pairs_with_sec.empty:
        return pd.DataFrame(columns=pairs_with_sec.columns if pairs_with_sec is not None else [])

    cross = pairs_with_sec[pairs_with_sec["section"] == "Cross-Domain"].copy()
    if "reliability" in cross.columns:
        cross["reliability"] = cross["reliability"].fillna(0.0)
    if "score" in cross.columns:
        cross["score"] = cross["score"].fillna(0.0)
    if cross.empty:
        return cross
    return cross.sort_values(["reliability", "score"], ascending=False).head(top)


def top_by_section(pairs_with_sec: pd.DataFrame, top: int = 12) -> dict[str, pd.DataFrame]:
    out: dict[str, pd.DataFrame] = {}
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
        if pairs_with_sec is None or pairs_with_sec.empty:
            out[section] = pd.DataFrame()
            continue

        section_df = pairs_with_sec[pairs_with_sec["section"] == section].copy()
        if section_df.empty:
            out[section] = section_df
        else:
            if "reliability" in section_df.columns:
                section_df["reliability"] = section_df["reliability"].fillna(0.0)
            if "score" in section_df.columns:
                section_df["score"] = section_df["score"].fillna(0.0)
            out[section] = (
                section_df.sort_values(["reliability", "score"], ascending=False).head(top)
            )
    return out
