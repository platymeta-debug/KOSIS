"""Utilities for fetching data payloads using catalog rows."""

from __future__ import annotations

from typing import Any, Dict

import pandas as pd

from .kosis_api import data_by_params, data_by_userstats, get_meta_table
from .validator import normalize_range


def fetch_row(row: Dict[str, Any]) -> pd.DataFrame:
    """Fetch a dataframe for a single catalog row."""

    prd_se = str(row.get("prdSe", "")).strip()
    start, end = normalize_range(
        prd_se, row.get("startPrdDe") or None, row.get("endPrdDe") or None
    )

    mode = row.get("mode")
    if not mode:
        mode = "user" if row.get("userStatsId") else "param"

    if mode == "user":
        data = data_by_userstats(
            user_stats_id=str(row["userStatsId"]),
            prd_se=prd_se,
            start=start,
            end=end,
            newEstPrdCnt=row.get("newEstPrdCnt") or None,
            prdInterval=row.get("prdInterval") or None,
            outputFields=row.get("outputFields") or "PRD_DE,DT,UNIT_NM",
        )
    else:
        obj = {
            key: str(row[key])
            for key in [f"objL{i}" for i in range(1, 9)]
            if key in row and str(row[key]).strip() != ""
        }
        data = data_by_params(
            org_id=str(row["orgId"]),
            tbl_id=str(row["tblId"]),
            prd_se=prd_se,
            obj=obj,
            itm_id=str(row["itmId"]),
            start=start,
            end=end,
            newEstPrdCnt=row.get("newEstPrdCnt") or None,
            prdInterval=row.get("prdInterval") or None,
            outputFields=row.get("outputFields") or "PRD_DE,DT,UNIT_NM",
        )

    return pd.DataFrame(data)


def enrich_with_meta_if_needed(org_id: str, tbl_id: str) -> Dict[str, Any]:
    """Fetch table metadata (classification/item definitions) when required."""

    return get_meta_table(org_id, tbl_id)
