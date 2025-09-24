import numpy as np, yaml

def load(path): 
    with open(path,'r',encoding='utf-8') as f: 
        return yaml.safe_load(f)

def apply(irf_obj, variables, shocks: dict, horizon=8):
    orth = irf_obj.orth_irfs  # (periods+1, k, k)
    total = np.zeros((horizon+1, len(variables)))
    for var, spec in shocks.items():
        size = float(str(spec).replace("%",""))/100.0 if str(spec).endswith("%") else float(spec)
        idx = variables.index(var)
        total += orth[:horizon+1,:,idx] * size
    return total
