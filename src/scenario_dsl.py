from __future__ import annotations

import numpy as np
import yaml


def _parse_value(v):
    if isinstance(v, str) and v.endswith("%"):
        return float(v.strip("%")) / 100.0
    return float(v)


def _shock_profile(h: int, typ: str, val: float):
    prof = np.zeros(h + 1)
    if typ == "step":
        prof[:] = val
        prof[0] = val
    elif typ == "pulse":
        prof[0] = val
    elif typ == "gradual":
        for t in range(h + 1):
            prof[t] = val * min(1.0, t / 4.0)
    elif typ == "path":
        raise ValueError("use policy_path for explicit path")
    else:
        prof[0] = val
    return prof


def load(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def apply(irf_obj, variables, scen: dict):
    h = int(scen.get("horizon_quarters", 8))
    total = np.zeros((h + 1, len(variables)))
    orth = irf_obj.orth_irfs

    for var, arr in (scen.get("policy_path") or {}).items():
        if var not in variables:
            continue
        idx = variables.index(var)
        path = np.array(arr, dtype=float)
        for t, a in enumerate(path[: h + 1]):
            total[t, :] += orth[t, :, idx] * a

    for var, spec in (scen.get("shocks") or {}).items():
        if var not in variables:
            continue
        idx = variables.index(var)
        if isinstance(spec, dict):
            typ = spec.get("type", "step")
            val = _parse_value(spec.get("value", 0.0))
        else:
            typ = "step"
            val = _parse_value(spec)
        prof = _shock_profile(h, typ, val)
        for t in range(h + 1):
            total[t, :] += orth[t, :, idx] * prof[t]

    return total
