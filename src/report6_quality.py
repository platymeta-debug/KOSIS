from __future__ import annotations

import pandas as pd


def reliability_from_signals(row: pd.Series) -> float:
    """Composite reliability score combining multiple signal statistics."""

    corr = abs(float(row.get("corr", 0) or 0))
    pcorr = abs(float(row.get("pcorr", 0) or 0))
    mi = float(row.get("mi", 0) or 0)
    consistency = float(row.get("consistency", 0) or 0)
    granger = 1.0 if (bool(row.get("gr_ab", False)) or bool(row.get("gr_ba", False))) else 0.0
    score = 0.35 * corr + 0.20 * pcorr + 0.15 * mi + 0.20 * consistency + 0.10 * granger
    return max(0.0, min(1.0, score))


def label_from_reliability(x: float) -> str:
    if x >= 0.66:
        return "High"
    if x >= 0.4:
        return "Med"
    return "Low"


def attach_reliability(df_pairs: pd.DataFrame) -> pd.DataFrame:
    if df_pairs is None or df_pairs.empty:
        return df_pairs
    enriched = df_pairs.copy()
    enriched["reliability"] = enriched.apply(reliability_from_signals, axis=1)
    enriched["reliability_label"] = enriched["reliability"].apply(label_from_reliability)
    return enriched


def kpi_summary(pairs: pd.DataFrame) -> dict:
    if pairs is None or pairs.empty:
        return {"n_pairs": 0, "share_strong": 0, "share_med": 0, "avg_reliability": 0}

    n_pairs = len(pairs)
    strong = (pairs["tier"] == "strong").sum() if "tier" in pairs.columns else 0
    medium = (pairs["tier"] == "medium").sum() if "tier" in pairs.columns else 0

    reliability = (
        pairs["reliability"].mean()
        if "reliability" in pairs.columns and not pairs["reliability"].isna().all()
        else 0
    )

    return {
        "n_pairs": n_pairs,
        "share_strong": round(strong / n_pairs, 3) if n_pairs else 0,
        "share_med": round(medium / n_pairs, 3) if n_pairs else 0,
        "avg_reliability": round(float(reliability), 3) if n_pairs else 0,
    }
