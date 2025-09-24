"""Small IO helpers for catalog/data persistence."""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd
import yaml


def save_csv(df: pd.DataFrame, path: str) -> None:
    """Persist a dataframe to CSV without the index."""

    df.to_csv(path, index=False)


def save_yaml(series: List[Dict[str, Any]], path: str) -> None:
    """Persist the provided series descriptors to YAML."""

    with open(path, "w", encoding="utf-8") as file:
        yaml.safe_dump({"series": series}, file, allow_unicode=True, sort_keys=False)
