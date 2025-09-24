from __future__ import annotations
import pandas as pd
import numpy as np

def simple_asset_mapping(variables: list, cumulative_response: np.ndarray) -> pd.DataFrame:
    """Very simple mapping: demonstrate how macro shock maps to assets.
    Assumptions (toy):
    - policy_rate ↑ hurts long-duration assets (growth stocks, 10Y bonds)
    - population ↑ boosts housing and retail
    - m2 ↑ lifts broad equities and gold
    - cpi ↑ supports gold, hurts bonds
    - bank_loan ↑ supports housing short-term, risks long-term
    """
    horizon = cumulative_response.shape[0]-1
    last = cumulative_response[-1]  # last period cumulative effect per variable
    eff = {v: e for v, e in zip(variables, last)}
    # toy scoring
    assets = {
        'banks': eff.get('policy_rate',0)*0.8 + eff.get('bank_loan',0)*0.3,
        'growth_stocks': -eff.get('policy_rate',0)*0.9 + eff.get('m2',0)*0.4,
        'reits_housing': eff.get('population_total',0)*0.7 + eff.get('bank_loan',0)*0.5 - 0.4*eff.get('policy_rate',0),
        'gold': eff.get('cpi',0)*0.6 + eff.get('m2',0)*0.5,
        'gov_bond_10y': -eff.get('policy_rate',0)*0.8 - eff.get('cpi',0)*0.4,
    }
    df = pd.DataFrame({'asset': list(assets.keys()), 'score': list(assets.values())})
    df['stance'] = pd.cut(df['score'], bins=[-1e9, -0.05, 0.05, 1e9], labels=['Underweight','Neutral','Overweight'])
    return df.sort_values('score', ascending=False)
