"""Thin API client helpers for the KOSIS endpoints."""

from __future__ import annotations

from typing import Any, Dict, List, Optional

__all__ = [
    "list_nodes",
    "get_param",
    "get_stat_data",
    "fetch_userstats",
]


# [ANCHOR:KOSIS_API_CONSTS]
_STAT_LIST_URL = "https://kosis.kr/openapi/statisticsList.do"
_STAT_DATA_URL = "https://kosis.kr/openapi/statisticsData.do"
_STAT_PARAM_URL = "https://kosis.kr/openapi/Param/statisticsParameterData.do"
_STAT_BIG_URL = "https://kosis.kr/openapi/statisticsBigData.do"

_COMMON = {"format": "json", "jsonVD": "Y", "content": "json"}


def _get_api_key() -> str:
    import os

    k = os.getenv("KOSIS_API_KEY", "")
    if not k:
        raise RuntimeError("KOSIS_API_KEY not set")
    return k


# [ANCHOR:KOSIS_API_GETJSON]
import requests
import time


def _get_json(
    url: str,
    params: Dict[str, Any],
    verbose: bool = False,
    timeout: int = 20,
    retry: int = 3,
    backoff: float = 0.6,
):
    sess = requests.Session()
    last = None
    for t in range(retry + 1):
        try:
            r = sess.get(
                url,
                params=params,
                timeout=timeout,
                headers={"Accept": "application/json"},
            )
            if verbose:
                print(
                    f"[HTTP] GET {url} try={t+1} timeout={timeout} params={params}"
                )
                print(
                    f"[HTTP] status={r.status_code} ctype={r.headers.get('Content-Type','')} elapsed={getattr(r,'elapsed',None)}"
                )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            time.sleep(backoff * (t + 1))
    raise last  # type: ignore[misc]


# [ANCHOR:KOSIS_API_LIST]
def list_nodes(
    vwCd: str,
    parentId: str,
    pIndex: int = 1,
    pSize: int = 1000,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        **_COMMON,
        "method": "getList",
        "apiKey": _get_api_key(),
        "vwCd": vwCd,
        "parentId": parentId,
        "pIndex": str(pIndex),
        "pSize": str(pSize),
    }
    data = _get_json(_STAT_LIST_URL, params, verbose=verbose)
    if isinstance(data, dict):
        for key in ("list", "LIST", "rows", "ROW"):
            if key in data and isinstance(data[key], list):
                return data[key]
        for value in data.values():
            if isinstance(value, list):
                return value
        return []
    return data if isinstance(data, list) else []


# [ANCHOR:KOSIS_API_PARAM]
def get_param(orgId: str, tblId: str, verbose: bool = False):
    params = {
        **_COMMON,
        "method": "getList",
        "apiKey": _get_api_key(),
        "orgId": orgId,
        "tblId": tblId,
    }
    return _get_json(_STAT_PARAM_URL, params, verbose=verbose)


# [ANCHOR:KOSIS_API_DATA_URLGEN]
def get_stat_data(
    orgId: str,
    tblId: str,
    *,
    prdSe: Optional[str] = None,
    startPrdDe: Optional[str] = None,
    endPrdDe: Optional[str] = None,
    itmId: Optional[str] = None,
    objL1: Optional[str] = None,
    objL2: Optional[str] = None,
    objL3: Optional[str] = None,
    objL4: Optional[str] = None,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        **_COMMON,
        "method": "getList",
        "apiKey": _get_api_key(),
        "orgId": orgId,
        "tblId": tblId,
    }
    if prdSe:
        params["prdSe"] = prdSe
    if startPrdDe:
        params["startPrdDe"] = startPrdDe
    if endPrdDe:
        params["endPrdDe"] = endPrdDe
    if itmId:
        params["itmId"] = itmId
    if objL1:
        params["objL1"] = objL1
    if objL2:
        params["objL2"] = objL2
    if objL3:
        params["objL3"] = objL3
    if objL4:
        params["objL4"] = objL4

    data = _get_json(_STAT_DATA_URL, params, verbose=verbose)
    if isinstance(data, dict):
        for key in ("list", "LIST", "rows"):
            if key in data and isinstance(data[key], list):
                return data[key]
        for value in data.values():
            if isinstance(value, list):
                return value
        return []
    return data if isinstance(data, list) else []


# [ANCHOR:KOSIS_API_DATA_USERSTATS]
def fetch_userstats(
    userStatsId: str,
    *,
    prdSe: Optional[str] = None,
    startPrdDe: Optional[str] = None,
    endPrdDe: Optional[str] = None,
    verbose: bool = False,
) -> List[Dict[str, Any]]:
    params: Dict[str, Any] = {
        **_COMMON,
        "method": "getList",
        "apiKey": _get_api_key(),
        "userStatsId": userStatsId,
    }
    if prdSe:
        params["prdSe"] = prdSe
    if startPrdDe:
        params["startPrdDe"] = startPrdDe
    if endPrdDe:
        params["endPrdDe"] = endPrdDe

    data = _get_json(_STAT_DATA_URL, params, verbose=verbose)
    if isinstance(data, dict):
        for key in ("list", "LIST", "rows"):
            if key in data and isinstance(data[key], list):
                return data[key]
        for value in data.values():
            if isinstance(value, list):
                return value
        return []
    return data if isinstance(data, list) else []

