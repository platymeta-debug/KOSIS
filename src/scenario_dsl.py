from __future__ import annotations

from typing import Any, Dict, List

import numpy as np

VALID_TYPES = {"step", "pulse", "gradual", "path"}


def validate_and_expand(spec: Dict[str, Any]) -> Dict[str, Any]:
    meta = spec.get("meta", {})
    Hz = int(meta.get("horizon", 8))
    if Hz <= 0:
        Hz = 8
    shocks = spec.get("shocks", [])
    out: List[Dict[str, Any]] = []
    for s in shocks:
        t = s.get("type", "step").lower()
        if t not in VALID_TYPES:
            raise ValueError(f"unknown shock type: {t}")
        var = s["var"]
        start = int(s.get("start_h", 0))
        size = float(s.get("size", 0.0))
        duration = int(s.get("duration", 1))
        vec = np.zeros(Hz + 1)
        if t == "step":
            vec[start:] += size
        elif t == "pulse":
            end = min(start + duration, Hz + 1)
            vec[start:end] += size
        elif t == "gradual":
            end = min(start + duration, Hz + 1)
            if end <= start:
                end = start + 1
            steps = end - start
            inc = np.linspace(0, size, steps)
            vec[start:end] += inc
            if end <= Hz:
                vec[end:] += size
        elif t == "path":
            path = s.get("path", [])
            for i, v in enumerate(path):
                h = start + i
                if h <= Hz:
                    vec[h] += float(v)
        out.append({"var": var, "type": t, "impulse": vec})
    targets = spec.get("targets", {})
    return {"horizon": Hz, "shocks": out, "targets": targets, "meta": meta}
