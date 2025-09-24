"""Build candidate table catalogues by traversing the KOSIS list API."""

from __future__ import annotations

from collections import deque
from typing import Any, Dict, List

import pandas as pd

from .kosis_api import list_stats


def _extract(row: Dict[str, Any], key: str) -> Any:
    """Return a value using a handful of common key variants."""

    return row.get(key) or row.get(key.upper()) or row.get(key.lower())


def build_catalog(vw_cd: str, roots: List[str], max_depth: int = 6) -> pd.DataFrame:
    """Traverse the statistics list tree and collect nodes that expose tables."""

    rows: List[Dict[str, Any]] = []
    for root in roots:
        queue: deque[tuple[str, int]] = deque([(root, 0)])
        while queue:
            parent, depth = queue.popleft()
            if depth > max_depth:
                continue
            items = list_stats(vw_cd, parent)
            for item in items:
                item["depth"] = depth
                rows.append(item)
                next_id = (
                    item.get("listId")
                    or item.get("LIST_ID")
                    or item.get("list_id")
                )
                if next_id:
                    queue.append((str(next_id), depth + 1))

    df = pd.DataFrame(rows)

    candidates: List[Dict[str, Any]] = []
    for _, row in df.iterrows():
        record = row.to_dict()
        org = _extract(record, "orgId")
        tbl = _extract(record, "tblId")
        if org and tbl:
            candidates.append(
                {
                    "listId": _extract(record, "listId") or _extract(record, "LIST_ID"),
                    "listNm": _extract(record, "listNm") or _extract(record, "LIST_NM"),
                    "orgId": org,
                    "tblId": tbl,
                    "upListId": _extract(record, "upListId")
                    or _extract(record, "UP_LIST_ID"),
                    "depth": record.get("depth"),
                }
            )

    return pd.DataFrame(candidates).drop_duplicates()
