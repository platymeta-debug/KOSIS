"""Build candidate table catalogues by traversing the KOSIS list API."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd

from .kosis_api import list_stats


def _is_leaf(node: Dict) -> bool:
    """Return ``True`` if the node appears to represent a table entry."""

    if node.get("tblId"):
        return True
    list_se = (node.get("listSe") or "").upper()
    return list_se in {"TBL", "DT", "TB", "STAT", "TABLE"}


def _collect_from_root(
    vw_cd: str,
    root_id: str,
    max_depth: int,
    depth: int = 0,
) -> List[Dict]:
    if depth > max_depth:
        return []

    items = list_stats(vw_cd, root_id)
    collected: List[Dict] = []
    for item in items:
        if _is_leaf(item):
            collected.append(
                {
                    "vwCd": vw_cd,
                    "listId": item.get("listId"),
                    "orgId": item.get("orgId"),
                    "tblId": item.get("tblId"),
                    "listSe": item.get("listSe"),
                    "name": item.get("name"),
                }
            )
        else:
            child = item.get("listId")
            if child:
                collected.extend(
                    _collect_from_root(vw_cd, child, max_depth, depth + 1)
                )
    return collected


def build_catalog(vw_cd: str, roots: List[str], max_depth: int = 6) -> pd.DataFrame:
    """Traverse the statistics list tree and collect candidate tables."""

    rows: List[Dict] = []
    for root in roots:
        try:
            rows.extend(_collect_from_root(vw_cd, root, max_depth))
        except Exception:
            # Skip problematic roots but continue scanning the others.
            continue

    df = pd.DataFrame(rows).drop_duplicates()
    if not df.empty:
        df = df[df["tblId"].notna()]
    return df
