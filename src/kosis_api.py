"""Thin API client helpers for the KOSIS endpoints."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

from .config import KOSIS_API_KEY, URL_DATA, URL_LIST, URL_META, URL_PARAM
from .utils import get_json


def list_stats(
    vw_cd: str,
    parent_list_id: str,
    pindex: int = 1,
    psize: int = 1000,
) -> List[Dict[str, Any]]:
    """Retrieve a statistics list for the provided view and parent list.

    The KOSIS "list" endpoint is a little inconsistent across environments so we
    normalise the parameters and the response for downstream callers.  The API
    occasionally returns objects instead of plain lists, and may provide field
    names in several different casings.
    """

    params = {
        "serviceKey": KOSIS_API_KEY,
        "vwCd": vw_cd,
        "parentListId": parent_list_id,
        "format": "json",
        "pIndex": str(pindex),
        "pSize": str(psize),
    }

    rows = get_json(URL_LIST, params)

    # In some environments an empty result may come back as {"list": []} or
    # even as an error dict.  Convert to a list of dicts for callers.
    if isinstance(rows, dict) and "list" in rows:
        rows = rows["list"]
    if not isinstance(rows, list):
        return []

    normalised: List[Dict[str, Any]] = []
    for raw in rows:
        list_id = raw.get("listId") or raw.get("LIST_ID") or raw.get("list_id")
        tbl_id = raw.get("tblId") or raw.get("TBL_ID") or raw.get("tbl_id")
        org_id = raw.get("orgId") or raw.get("ORG_ID") or raw.get("org_id")
        list_se = (
            raw.get("listSe")
            or raw.get("LIST_SE")
            or raw.get("list_se")
            or ""
        )
        name = (
            raw.get("listNm")
            or raw.get("LIST_NM")
            or raw.get("list_nm")
            or raw.get("tblNm")
            or raw.get("TBL_NM")
        )

        normalised.append(
            {
                "listId": list_id,
                "tblId": tbl_id,
                "orgId": org_id,
                "listSe": str(list_se).upper(),
                "name": name,
                "raw": raw,
            }
        )

    return normalised


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
) -> List[Dict[str, Any]]:
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
    if not any(key for key in params if key.lower().startswith("objl")):
        raise ValueError("Param API 호출 시 objL1~objL8 중 최소 1개가 필요합니다.")
    if not itm_id:
        raise ValueError("Param API 호출 시 itmId는 필수입니다.")
    return get_json(URL_PARAM, params)


def get_meta_table(org_id: str, tbl_id: str) -> Dict[str, Any]:
    """Fetch table metadata for the supplied organisation/table identifiers."""

    params = {
        "method": "getMeta",
        "apiKey": KOSIS_API_KEY,
        "format": "json",
        "type": "TBL",
        "orgId": org_id,
        "tblId": tbl_id,
    }
    return get_json(URL_META, params)
