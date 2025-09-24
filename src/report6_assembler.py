from __future__ import annotations

import pandas as pd

from .report6_quality import attach_reliability, kpi_summary
from .report6_sections import add_section_cols, top_cross_domain, top_by_section


def assemble(
    pairs: pd.DataFrame,
    irf: pd.DataFrame | None,
    lp: pd.DataFrame | None,
    scenario: dict[str, object] | None,
    h_pick: int = 4,
) -> Dict[str, object]:
    """Aggregate inputs into the structures required for the final report."""

    # 1) Attach reliability metrics and section routing
    pairs_with_reliability = attach_reliability(pairs)
    pairs_with_sections = add_section_cols(pairs_with_reliability)
    kpi = kpi_summary(pairs_with_reliability)

    # 2) Cross-domain and per-section leaders
    cross_top = top_cross_domain(pairs_with_sections, top=15)
    section_top = top_by_section(pairs_with_sections, top=12)

    # 3) IRF highlights @ selected horizons
    irf_highlights = pd.DataFrame()
    if irf is not None and not irf.empty:
        irf_copy = irf.copy()
        irf_copy["abs"] = irf_copy["resp"].abs()
        irf_highlights = (
            irf_copy[irf_copy["h"].isin([1, 2, 4, 8])]  # typical horizons
            .sort_values("abs", ascending=False)
            .groupby("h")
            .head(10)
        )

    # 4) Local projection betas (investment tilt)
    lp_top = pd.DataFrame()
    if lp is not None and not lp.empty:
        if "beta_h4" in lp.columns:
            lp_top = (
                lp.assign(abs4=lp["beta_h4"].abs())
                .sort_values("abs4", ascending=False)
                .head(12)
            )

    # 5) Scenario effects summary
    scenario = scenario or {}
    scn_summary = pd.DataFrame()
    effects_full = scenario.get("effects_full") if isinstance(scenario, dict) else None
    if effects_full is not None and not effects_full.empty:
        pick = effects_full[(effects_full["h"] == h_pick) & (effects_full["source"] == "mix")].copy()
        pick["abs"] = pick["effect"].abs()
        scn_summary = pick.sort_values("abs", ascending=False).head(15)

    return {
        "pairs": pairs_with_sections,
        "kpi": kpi,
        "cross_top": cross_top,
        "sec_top": section_top,
        "irf_top": irf_highlights,
        "lp_top": lp_top,
        "scn_summary": scn_summary,
    }
