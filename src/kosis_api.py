"""Thin API client helpers for the KOSIS endpoints."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .config import KOSIS_API_KEY, URL_DATA, URL_LIST, URL_META, URL_PARAM
from .utils import get_json

COMMON_PARAMS = {"format": "json", "jsonVD": "Y", "content": "json"}


def list_stats(
    vw_cd: str,
    parent_id: str,
    pindex: int = 1,
    psize: int = 1000,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    """Retrieve a statistics list for the provided view and parent list.

    The helper enforces the official parameter names (``apiKey``, ``parentId``
    etc.) and optionally emits verbose HTTP logging.  Some environments wrap the
    payload in a top-level ``{"list": [...]}``, so we normalise the response to a
    plain list for downstream callers.
    """

    params = {
        **COMMON_PARAMS,
        "method": "getList",
        "apiKey": KOSIS_API_KEY,
        "vwCd": vw_cd,
        "parentId": parent_id,
        "pIndex": str(pindex or 1),
        "pSize": str(psize or 1000),
    }
    headers = {
        "Accept": "application/json,text/plain,*/*",
        "User-Agent": "kosis-catalog/1.0",
    }

    rows = get_json(URL_LIST, params, headers=headers, verbose=verbose)
    if isinstance(rows, dict) and "list" in rows:
        rows = rows["list"]
    return rows if isinstance(rows, list) else []


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
        **COMMON_PARAMS,
        "method": "getList",
        "apiKey": KOSIS_API_KEY,
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
) -> List[Dict[str, Any]]:
    """Retrieve statistics data for the parameterised table endpoint."""

    params: Dict[str, Any] = {
        **COMMON_PARAMS,
        "method": "getList",
        "apiKey": KOSIS_API_KEY,
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
    if not any(key for key in params if key.lower().startswith("objl")):
        raise ValueError("Param API 호출 시 objL1~objL8 중 최소 1개가 필요합니다.")
    if not itm_id:
        raise ValueError("Param API 호출 시 itmId는 필수입니다.")
    return get_json(URL_PARAM, params)


def get_meta_table(org_id: str, tbl_id: str) -> Dict[str, Any]:
    """Fetch table metadata for the supplied organisation/table identifiers."""

    params = {
        **COMMON_PARAMS,
        "method": "getMeta",
        "apiKey": KOSIS_API_KEY,
        "type": "TBL",
        "orgId": org_id,
        "tblId": tbl_id,
    }
    return get_json(URL_META, params)


def fetch_userstats(userStatsId: str, verbose: bool = False):
    """Fetch rows directly via the userStatsId (등록URL) endpoint."""

    params = {
        **COMMON_PARAMS,
        "method": "getList",
        "userStatsId": userStatsId,
        "apiKey": KOSIS_API_KEY,
    }
    payload = get_json(URL_DATA, params, verbose=verbose)
    if isinstance(payload, dict):
        if "list" in payload:
            return payload["list"]
        # 일부 응답은 STAT_DATA 키를 사용
        if "STAT_DATA" in payload and isinstance(payload["STAT_DATA"], list):
            return payload["STAT_DATA"]
    return payload
