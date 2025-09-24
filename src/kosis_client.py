from __future__ import annotations

import time
import json
import random
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
from .db import connect


_META_CACHE: Dict[Tuple[str, str], Dict[str, Any]] = {}


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


def build_params(
    row: Dict[str, Any], override: Dict[str, Any] | None = None
) -> Tuple[str, Dict[str, Any]]:
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

    if "outputFields" not in params and "jsonVD" not in params:
        params["outputFields"] = "jsonVD"

    if override:
        params.update(override)

    return url, params


def http_get(
    url: str,
    params: Dict[str, Any],
    retries: int = 4,
    *,
    expect_list: bool = True,
) -> List[Dict[str, Any]] | Dict[str, Any]:
    backoff = 0.6
    for attempt in range(retries):
        try:
            r = requests.get(url, params=params, timeout=KOSIS_TIMEOUT)
            if r.status_code in {429, 500, 502, 503, 504}:
                raise requests.HTTPError(f"retryable status {r.status_code}")
            r.raise_for_status()
            data = r.json()
            if isinstance(data, dict) and (
                data.get("errMsg") or data.get("errorMsg")
            ):
                raise RuntimeError(str(data))
            if not expect_list:
                return data
            if isinstance(data, dict) and isinstance(data.get("list"), list):
                return data["list"]
            if isinstance(data, list):
                return data
            raise RuntimeError(f"Unexpected response: {str(data)[:200]}")
        except Exception:
            if attempt == retries - 1:
                raise
            sleep_for = backoff * (1.5 ** attempt) + random.uniform(0, 0.3)
            time.sleep(sleep_for)


def fetch_full(row: Dict[str, Any]) -> List[Dict[str, Any]]:
    time.sleep(KOSIS_RATE_SLEEP)
    url, params = build_params(row)
    return http_get(url, params)


def fetch_since(row: Dict[str, Any], since_period: str) -> List[Dict[str, Any]]:
    time.sleep(KOSIS_RATE_SLEEP)
    url, params = build_params(row, {"startPrdDe": since_period})
    return http_get(url, params)


def _load_cached_meta(org_id: str, tbl_id: str) -> Dict[str, Any] | None:
    key = (org_id, tbl_id)
    if key in _META_CACHE:
        return _META_CACHE[key]
    con = connect()
    try:
        row = con.execute(
            "SELECT meta FROM series_catalog WHERE org_id=? AND tbl_id=? LIMIT 1",
            [org_id, tbl_id],
        ).fetchone()
    except Exception:
        row = None
    finally:
        con.close()
    if row and row[0]:
        meta = row[0]
        if isinstance(meta, str):
            meta = json.loads(meta)
        _META_CACHE[key] = meta
        return meta
    return None


def fetch_meta(org_id: str, tbl_id: str) -> Dict[str, Any]:
    """Retrieve optional metadata for a statistics table and cache it."""

    cached = _load_cached_meta(org_id, tbl_id)
    if cached is not None:
        return cached

    params = {
        **_base_params(),
        "method": "getMeta",
        "type": "TBL",
        "orgId": org_id,
        "tblId": tbl_id,
    }
    raw = http_get(KOSIS_URL_META, params, expect_list=False)
    meta = raw.get("list") if isinstance(raw, dict) else raw
    key = (org_id, tbl_id)
    _META_CACHE[key] = meta

    con = connect()
    try:
        con.execute(
            """
            UPDATE series_catalog
            SET meta = ?
            WHERE org_id = ? AND tbl_id = ?
            """,
            [json.dumps(meta, ensure_ascii=False), org_id, tbl_id],
        )
    finally:
        con.close()

    return meta
