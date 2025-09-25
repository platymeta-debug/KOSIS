"""Thin API client helpers for the KOSIS endpoints."""

from __future__ import annotations

import os
import time
from typing import Any, Dict, List, Optional

import requests

from .config import KOSIS_API_KEY, URL_DATA, URL_LIST, URL_META, URL_PARAM
from .utils import get_json

COMMON_PARAMS = {"format": "json", "jsonVD": "Y", "content": "json"}

# API 엔드포인트(기존과 동일한 상수 사용 권장)
_STAT_DATA_URL = URL_DATA
# 공통 파라미터(이미 있다면 중복 정의 금지)
_COMMON = COMMON_PARAMS


def _get_api_key() -> str:
    k = os.getenv("KOSIS_API_KEY", "")
    if not k:
        k = KOSIS_API_KEY
    if not k:
        raise RuntimeError("KOSIS_API_KEY not set")
    return k


def _get_json(
    url: str,
    params: Dict[str, Any],
    verbose: bool = False,
    timeout: int = 20,
    retry: int = 2,
):
    # 간단 리트라이(429/5xx)
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
            ctype = r.headers.get("Content-Type", "")
            if verbose:
                print(
                    f"[HTTP] GET {url} try={t+1} timeout={timeout} params={params}"
                )
                print(
                    f"[HTTP] status={r.status_code} ctype={ctype} elapsed={getattr(r, 'elapsed', None)}"
                )
            r.raise_for_status()
            return r.json()
        except Exception as e:
            last = e
            # 429/5xx 백오프
            time.sleep(0.6 + 0.6 * t)
    raise last


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
        "apiKey": _get_api_key(),
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
        "apiKey": _get_api_key(),
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
        "apiKey": _get_api_key(),
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
        "apiKey": _get_api_key(),
        "type": "TBL",
        "orgId": org_id,
        "tblId": tbl_id,
    }
    return get_json(URL_META, params)


def fetch_userstats(
    userStatsId: str,
    verbose: bool = False,
    prdSe: Optional[str] = None,
    startPrdDe: Optional[str] = None,
    endPrdDe: Optional[str] = None,
) -> List[Dict[str, Any]]:
    """
    등록URL(userStatsId)로 통계자료 호출.
    """
    params: Dict[str, Any] = {
        **_COMMON,
        "method": "getList",
        "apiKey": _get_api_key(),
        "userStatsId": userStatsId,
    }
    # 기간 필터는 제공 시에만 설정
    if prdSe:
        params["prdSe"] = prdSe
    if startPrdDe:
        params["startPrdDe"] = startPrdDe
    if endPrdDe:
        params["endPrdDe"] = endPrdDe

    data = _get_json(_STAT_DATA_URL, params, verbose=verbose)
    # KOSIS는 보통 list/rows 구조 or 바로 배열로 반환
    if isinstance(data, dict):
        # 가장 가능성 높은 키 추정
        for k in ("list", "LIST", "data", "DATA", "rows"):
            if k in data and isinstance(data[k], list):
                return data[k]
        # dict인데 리스트를 못 찾으면 항목값을 스캔
        for v in data.values():
            if isinstance(v, list):
                return v
        return []
    elif isinstance(data, list):
        return data
    return []
