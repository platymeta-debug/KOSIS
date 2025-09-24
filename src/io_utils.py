from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any, Dict

import pandas as pd
import yaml


def read_yaml(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return yaml.safe_load(f)


def save_md(path: str, text: str) -> None:
    Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        f.write(text)


def save_csv(df: pd.DataFrame, path: str) -> None:
    Path(os.path.dirname(path)).mkdir(parents=True, exist_ok=True)
    df.to_csv(path, index=False)


def load_irf_csv(irf_path: str) -> pd.DataFrame:
    df = pd.read_csv(irf_path)
    must = {"shock", "target", "h", "resp"}
    if not must.issubset(set(df.columns)):
        raise ValueError(f"IRF csv schema mismatch: need {must}, got {df.columns}")
    return df


def load_lp_betas(path: str) -> pd.DataFrame | None:
    if not os.path.exists(path):
        return None
    return pd.read_csv(path)
