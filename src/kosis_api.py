"""Thin API client helpers for the KOSIS endpoints."""

from __future__ import annotations

from typing import Any, Dict, Optional

from .config import KOSIS_API_KEY, URL_DATA, URL_LIST, URL_PARAM
from .utils import get_json


def list_stats(vw_cd: str, parent_list_id: str, jsonVD: str = "Y") -> Any:
    """Retrieve a statistics list for the provided view and parent list."""

    params = {
        "method": "getList",
        "apiKey": KOSIS_API_KEY,
        "format": "json",
        "vwCd": vw_cd,
        "parentListId": parent_list_id,
        "jsonVD": jsonVD,
    }
    return get_json(URL_LIST, params)


def data_by_userstats(
    user_stats_id: str,
    prd_se: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    newEstPrdCnt: Optional[int] = None,
    prdInterval: Optional[int] = None,
    outputFields: Optional[str] = None,
) -> Any:
    """Retrieve statistics data for the userStatsId style endpoint."""

    params: Dict[str, Any] = {
        "method": "getList",
        "apiKey": KOSIS_API_KEY,
        "format": "json",
        "userStatsId": user_stats_id,
        "prdSe": prd_se,
    }
    if start:
        params["startPrdDe"] = start
    if end:
        params["endPrdDe"] = end
    if newEstPrdCnt:
        params["newEstPrdCnt"] = newEstPrdCnt
    if prdInterval:
        params["prdInterval"] = prdInterval
    if outputFields:
        params["outputFields"] = outputFields
    return get_json(URL_DATA, params)


def data_by_params(
    org_id: str,
    tbl_id: str,
    prd_se: str,
    obj: Optional[Dict[str, str]],
    itm_id: str,
    start: Optional[str] = None,
    end: Optional[str] = None,
    newEstPrdCnt: Optional[int] = None,
    prdInterval: Optional[int] = None,
    outputFields: str = "PRD_DE,DT,UNIT_NM",
) -> Any:
    """Retrieve statistics data for the parameterised table endpoint."""

    params: Dict[str, Any] = {
        "method": "getList",
        "apiKey": KOSIS_API_KEY,
        "format": "json",
        "orgId": org_id,
        "tblId": tbl_id,
        "prdSe": prd_se,
        "itmId": itm_id,
    }
    for key, value in (obj or {}).items():
        if key.lower().startswith("objl") and value:
            params[key] = value
    if start:
        params["startPrdDe"] = start
    if end:
        params["endPrdDe"] = end
    if newEstPrdCnt:
        params["newEstPrdCnt"] = newEstPrdCnt
    if prdInterval:
        params["prdInterval"] = prdInterval
    if outputFields:
        params["outputFields"] = outputFields
    return get_json(URL_PARAM, params)
