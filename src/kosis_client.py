from __future__ import annotations
import os, time, json
from typing import Dict, Any, List
import pandas as pd, requests
from .config import (KOSIS_API_KEY, KOSIS_BASE_URL, KOSIS_TIMEOUT,
                     KOSIS_RATE_SLEEP, KOSIS_MAX_WORKERS)
from concurrent.futures import ThreadPoolExecutor, as_completed

def build_params(row: Dict[str,Any], override: Dict[str,Any]|None=None) -> Dict[str,Any]:
    p = {
        "method": "getList",
        "apiKey": KOSIS_API_KEY,
        "format": "json",
        "type": "json",
    }
    if row.get("userStatsId"):
        p["userStatsId"] = row["userStatsId"]
    else:
        p["orgId"] = row.get("orgId","")
        p["tblId"] = row.get("tblId","")
    if row.get("prdSe"): p["prdSe"] = row["prdSe"]
    if row.get("startPrdDe"): p["startPrdDe"] = row["startPrdDe"]
    if row.get("endPrdDe"): p["endPrdDe"] = row["endPrdDe"]
    if row.get("params"): p.update(row["params"])
    if override: p.update(override)
    return p

def http_get(params: Dict[str,Any]) -> List[Dict[str,Any]]:
    r = requests.get(KOSIS_BASE_URL, params=params, timeout=KOSIS_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    if isinstance(data, dict) and (data.get("errMsg") or data.get("errorMsg")):
        raise RuntimeError(str(data))
    if isinstance(data, list):
        return data
    if isinstance(data, dict) and isinstance(data.get("list"), list):
        return data["list"]
    raise RuntimeError(f"Unexpected response: {str(data)[:200]}")

def fetch_full(row: Dict[str,Any]) -> List[Dict[str,Any]]:
    time.sleep(KOSIS_RATE_SLEEP)
    return http_get(build_params(row))

def fetch_since(row: Dict[str,Any], since_period: str) -> List[Dict[str,Any]]:
    # PRD_DE가 YYYY, YYYYMM, YYYYQ 형태 모두 올 수 있음 → 그대로 전달: 최근분기 이후만
    ov = {"startPrdDe": since_period}
    time.sleep(KOSIS_RATE_SLEEP)
    return http_get(build_params(row, ov))
