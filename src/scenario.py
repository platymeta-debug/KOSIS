from __future__ import annotations
import pandas as pd
import numpy as np
import yaml

def load_scenario(path: str) -> dict:
    with open(path, 'r', encoding='utf-8') as f:
        return yaml.safe_load(f)

def apply_shock_irf(irf, variables: list, shock_var: str, shock_size: float, kind: str='step'):
    """Return IRF response for a single shock normalized by shock_size.
    For simplicity, we assume unit shock * shock_size scale.
    """
    idx = variables.index(shock_var)
    # IRF.orth_irfs shape: (periods+1, k, k) → response of each variable to each shock
    orth = irf.orth_irfs  # orthogonalized responses
    # Scale by shock_size
    resp = orth[:, :, idx] * shock_size
    return resp  # shape: (periods+1, k)

def run_scenario(irf, variables: list, shocks: dict, horizon: int=8):
    total = np.zeros((horizon+1, len(variables)))
    for var, spec in shocks.items():
        if isinstance(spec, str) and spec.endswith('%'):
            size = float(spec.strip('%'))/100.0
        else:
            size = float(spec)
        resp = apply_shock_irf(irf, variables, var, size, 'step')
        total += resp[:horizon+1, :]
    return total  # cumulative response per period
