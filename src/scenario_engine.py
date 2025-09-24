from __future__ import annotations

from typing import Any, Dict, List

import numpy as np
import pandas as pd


def _filter_targets(all_vars: List[str], targets_spec: Dict[str, Any]) -> List[str]:
    inc = targets_spec.get("include", [])
    pref = targets_spec.get("include_prefix", [])
    exc = targets_spec.get("exclude", [])
    out: List[str] = []
    for v in all_vars:
        if inc and v not in inc:
            continue
        if pref and not any(v.startswith(p) for p in pref):
            continue
        if exc and v in exc:
            continue
        if (not inc) and (not pref) and (v in exc):
            continue
        out.append(v)
    if not inc and not pref:
        out = [v for v in all_vars if v not in exc]
    return out


def apply_irf(irf_df: pd.DataFrame, scenario: Dict[str, Any]) -> pd.DataFrame:
    Hz = int(scenario["horizon"])
    all_targets = sorted(irf_df["target"].unique().tolist())
    chosen_targets = _filter_targets(all_targets, scenario.get("targets", {}))

    profiles = {s["var"]: s["impulse"] for s in scenario["shocks"]}

    rows: List[Dict[str, Any]] = []
    for tgt in chosen_targets:
        agg = np.zeros(Hz + 1)
        contribs: Dict[str, np.ndarray] = {}
        for sh, prof in profiles.items():
            F = irf_df[(irf_df["shock"] == sh) & (irf_df["target"] == tgt)]
            if F.empty:
                continue
            vec = np.zeros(Hz + 1)
            for _, r in F.iterrows():
                h = int(r["h"])
                if 0 <= h <= Hz:
                    vec[h] = float(r["resp"])
            eff = np.convolve(prof, vec)[: Hz + 1]
            agg += eff
            contribs[sh] = eff
        for h in range(Hz + 1):
            rows.append({"target": tgt, "h": h, "effect": float(agg[h]), "source": "mix"})
        for sh, eff in contribs.items():
            for h in range(Hz + 1):
                rows.append({"target": tgt, "h": h, "effect": float(eff[h]), "source": sh})
    return pd.DataFrame(rows)


def summarize_effects(eff_df: pd.DataFrame, horizon_pick: int = 4) -> pd.DataFrame:
    d = eff_df[(eff_df["h"] == horizon_pick) & (eff_df["source"] == "mix")].copy()
    d["abs"] = d["effect"].abs()
    d = d.sort_values("abs", ascending=False)
    return d[["target", "effect"]]
