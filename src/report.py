"""Reporting helpers for the staged pipeline."""

from __future__ import annotations

from datetime import datetime
from typing import Dict, Iterable, List

import pandas as pd

SECTIONS = [
    "Economy",
    "Corporate",
    "Finance",
    "Investment",
    "Real Estate",
    "Debt",
    "Growth",
    "Assets",
]


def route_section(name: str) -> str:
    """Route a logical series name to one of the eight canonical sections."""

    lowered = name.lower()
    if lowered.startswith("firm.") or any(keyword in lowered for keyword in ("employment", "capex", "sales")):
        return "Corporate"
    if lowered.startswith("asset.") or any(keyword in lowered for keyword in ("etf", "reits", "bond", "gold")):
        return "Investment"
    if any(keyword in lowered for keyword in ["house", "real_estate", "housing", "rent", "construction"]):
        return "Real Estate"
    if any(keyword in lowered for keyword in ["debt", "dsr", "dti", "leverage", "npl"]):
        return "Debt"
    if any(keyword in lowered for keyword in ["potential", "tfp", "productivity", "growth", "income", "consumption", "saving"]):
        return "Growth"
    if any(keyword in lowered for keyword in ["policy_rate", "m2", "credit", "spread", "loan_to_deposit", "bank_loan"]):
        return "Finance"
    if any(keyword in lowered for keyword in ["gdp", "cpi", "ppi", "trade", "fx", "population"]):
        return "Economy"
    return "Assets"


def render_quad8(path: str, pairs_df: pd.DataFrame) -> None:
    """Render the eight-section plus cross-domain markdown report."""

    domain_map: Dict[str, List[pd.Series]] = {}
    for _, row in pairs_df.iterrows():
        section_a = route_section(row["a"])
        section_b = route_section(row["b"])
        section = section_a if section_a == section_b else "Cross-Domain"
        domain_map.setdefault(section, []).append(row)

    with open(path, "w", encoding="utf-8") as handle:
        handle.write(f"# KOSIS Cross-Domain Report\nGenerated: {datetime.now().isoformat()}\n\n")
        handle.write("## Executive Summary\n")
        for row in pairs_df.head(10).itertuples():
            handle.write(f"- {row.a} ↔ {row.b}: corr {row.corr:.2f}\n")
        for section in SECTIONS + ["Cross-Domain"]:
            handle.write(f"\n## {section}\n")
            rows = domain_map.get(section, [])[:12]
            if not rows:
                handle.write("- (신호 없음)\n")
                continue
            for row in rows:
                handle.write(
                    f"- {row['a']} ↔ {row['b']} (corr {row['corr']:.2f}, tier {row['tier']})\n"
                )
