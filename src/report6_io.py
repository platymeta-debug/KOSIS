from __future__ import annotations

import os
import pandas as pd


def load_signals(path: str = "out_signals.csv") -> pd.DataFrame:
    """Load signal pairs data, ensuring optional columns exist."""

    if not os.path.exists(path):
        return pd.DataFrame(
            columns=[
                "a",
                "b",
                "corr",
                "pcorr",
                "mi",
                "consistency",
                "gr_ab",
                "gr_ba",
                "score",
                "tier",
            ]
        )

    df = pd.read_csv(path)
    for col in ["pcorr", "mi", "consistency", "gr_ab", "gr_ba", "score", "tier"]:
        if col not in df.columns:
            df[col] = None
    return df


def load_irf(
    path_svar: str = "out_causal/irf_svar.csv", path_var: str = "out_causal/irf_var.csv"
) -> pd.DataFrame:
    """Load impulse response functions, preferring SVAR if available."""

    target = path_svar if os.path.exists(path_svar) else path_var
    if not os.path.exists(target):
        return pd.DataFrame(columns=["shock", "target", "h", "resp"])
    return pd.read_csv(target)


def load_lp(path: str = "out_causal/lp_betas.csv") -> pd.DataFrame | None:
    return pd.read_csv(path) if os.path.exists(path) else None


def load_stationarity(path: str = "out_causal/stationarity_report.csv") -> pd.DataFrame | None:
    return pd.read_csv(path) if os.path.exists(path) else None


def load_scenario_effects(dirpath: str = "out_scenario") -> dict[str, object]:
    """Load scenario effects and any per-section summaries if present."""

    data: dict[str, object] = {
        "effects_full": pd.DataFrame(),
        "summaries": {},
        "per_section": {},
    }

    effects_path = os.path.join(dirpath, "effects_full.csv")
    if os.path.exists(effects_path):
        data["effects_full"] = pd.read_csv(effects_path)

    if not os.path.exists(dirpath):
        return data  # nothing else to load

    for fname in os.listdir(dirpath):
        fpath = os.path.join(dirpath, fname)
        if not fname.endswith(".csv") or not os.path.isfile(fpath):
            continue
        if fname.startswith("summary_h"):
            data["summaries"][fname] = pd.read_csv(fpath)
            continue

        stem = fname[:-4]
        if any(
            stem.startswith(prefix)
            for prefix in [
                "economy_",
                "corporate_",
                "finance_",
                "real estate_",
                "debt_",
                "growth_",
                "investment_",
                "assets_",
            ]
        ):
            data["per_section"][stem] = pd.read_csv(fpath)

    return data
