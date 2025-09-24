import pandas as pd

def toy_asset_map(vars, resp_last):
    eff = dict(zip(vars, resp_last))
    assets = {
        "banks": eff.get("macro.policy_rate",0)*0.8 + eff.get("macro.bank_loan",0)*0.3,
        "growth_stocks": -0.9*eff.get("macro.policy_rate",0) + 0.4*eff.get("macro.m2",0),
        "reits_housing": 0.7*eff.get("macro.population_total",0) + 0.5*eff.get("macro.bank_loan",0) - 0.4*eff.get("macro.policy_rate",0),
        "gold": 0.6*eff.get("macro.cpi",0) + 0.5*eff.get("macro.m2",0),
        "gov_bond_10y": -0.8*eff.get("macro.policy_rate",0) - 0.4*eff.get("macro.cpi",0),
    }
    df = pd.DataFrame({"asset":list(assets.keys()), "score":list(assets.values())})
    df["stance"] = pd.cut(df["score"], [-1e9,-0.05,0.05,1e9], labels=["Underweight","Neutral","Overweight"])
    return df.sort_values("score", ascending=False)
