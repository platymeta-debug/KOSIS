from __future__ import annotations

import time
from typing import Any, Dict, List, Tuple

import requests

from .config import (
    KOSIS_API_KEY,
    KOSIS_RATE_SLEEP,
    KOSIS_TIMEOUT,
    KOSIS_URL_BIG,
    KOSIS_URL_DATA,
    KOSIS_URL_META,
    KOSIS_URL_PARAM,
)


def _base_params() -> Dict[str, Any]:
    return {"format": "json", "apiKey": KOSIS_API_KEY}


def choose_endpoint(row: Dict[str, Any]) -> Tuple[str, Dict[str, Any]]:
    """Determine endpoint/method from a series_catalog row."""

    params = (row.get("params") or {})
    big_flag = bool(params.get("bigData") or params.get("bigdata"))

    if row.get("userStatsId"):
        url = KOSIS_URL_BIG if big_flag else KOSIS_URL_DATA
        return url, {"method": "getList", "userStatsId": row["userStatsId"]}

    if row.get("org_id") and row.get("tbl_id"):
        has_obj = any(k.lower().startswith("objl") for k in params.keys())
        if not (has_obj and ("itmId" in params)):
            raise ValueError("Param API는 objL1~objL8 중 최소 하나와 itmId가 필수입니다.")
        url = KOSIS_URL_BIG if big_flag else KOSIS_URL_PARAM
        return url, {"method": "getList", "orgId": row["org_id"], "tblId": row["tbl_id"]}

    raise ValueError("userStatsId 또는 (org_id,tbl_id+objL*/itmId) 중 하나가 필요합니다.")


def build_params(row: Dict[str, Any], override: Dict[str, Any] | None = None) -> Tuple[str, Dict[str, Any]]:
    url, head = choose_endpoint(row)
    params = {**_base_params(), **head}

    prd_se = row.get("prd_se") or row.get("prdSe")
    if prd_se:
        params["prdSe"] = prd_se

    start = row.get("start_prd_de") or row.get("startPrdDe")
    if start:
        params["startPrdDe"] = start

    end = row.get("end_prd_de") or row.get("endPrdDe")
    if end:
        params["endPrdDe"] = end

    extra = dict(row.get("params") or {})
    extra.pop("bigData", None)
    extra.pop("bigdata", None)

    for key in [
        "newEstPrdCnt",
        "prdInterval",
        "outputFields",
        "content",
        "jsonVD",
        "objL1",
        "objL2",
        "objL3",
        "objL4",
        "objL5",
        "objL6",
        "objL7",
        "objL8",
        "itmId",
    ]:
        if key in extra and extra[key] is not None:
            params[key] = extra[key]

    if override:
        params.update(override)

    return url, params


def http_get(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    r = requests.get(url, params=params, timeout=KOSIS_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and (data.get("errMsg") or data.get("errorMsg")):
        raise RuntimeError(str(data))
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("list"), list):
        return data["list"]
    raise RuntimeError(f"Unexpected response: {str(data)[:200]}")


def fetch_full(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    time.sleep(KOSIS_RATE_SLEEP)
    url, params = build_params(row)
    return http_get(url, params)


def fetch_since(row: Dict[str, Any], since_period: str) -> List[Dict[str, Any]]:
    time.sleep(KOSIS_RATE_SLEEP)
    url, params = build_params(row, {"startPrdDe": since_period})
    return http_get(url, params)


def fetch_meta(org_id: str, tbl_id: str) -> Dict[str, Any]:
    """Retrieve optional metadata for a statistics table."""

    params = {
        **_base_params(),
        "method": "getMeta",
        "type": "TBL",
        "orgId": org_id,
        "tblId": tbl_id,
    }
    r = requests.get(KOSIS_URL_META, params=params, timeout=KOSIS_TIMEOUT)
    r.raise_for_status()
    return r.json()
