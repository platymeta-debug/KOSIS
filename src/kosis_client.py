"""
KOSIS API 클라이언트 (실데이터용)
- series.yaml에 정의된 다수 시리즈를 병렬로 수집
- (동일 지표명의) 분리 테이블(예: ~2019, 2020~) 자동 병합
- 월/분기/연간 주기를 공통 주기로 리샘플
- wide DataFrame(index=Period, columns=logical_name) 반환
- 캐시(데이터/원본 parquet) 저장 옵션
"""
from __future__ import annotations
import os, time, math, json
from dataclasses import dataclass
from typing import Dict, List, Optional, Any, Tuple
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import numpy as np
import requests

from .config import KOSIS_API_KEY, KOSIS_BASE_URL, FREQ

# -------- 설정 기본값 --------
DEFAULT_BASE_URL = KOSIS_BASE_URL or "https://kosis.kr/openapi/statisticsData.do"
DEFAULT_TIMEOUT = 30
MAX_WORKERS = int(os.environ.get("KOSIS_MAX_WORKERS", "8"))
RATE_LIMIT_SLEEP = float(os.environ.get("KOSIS_RATE_SLEEP", "0.35"))  # 초/호출 (QPS 조절)
RAW_DIR = os.environ.get("KOSIS_RAW_DIR", "data/raw")

# -------- 시리즈 정의 데이터클래스 --------
@dataclass
class SeriesEntry:
    logical_name: str                 # 최종 wide 컬럼명 (여러 era 묶음의 공통 이름)
    orgId: Optional[str] = None       # (orgId, tblId) 방식용
    tblId: Optional[str] = None
    userStatsId: Optional[str] = None # user 통계 ID 방식용 (선택)
    endpoint: Optional[str] = None    # 개별 endpoint override (없으면 DEFAULT_BASE_URL)
    prdSe: str = "Q"                  # 'M', 'Q', 'A' 등
    startPrdDe: Optional[str] = None  # '200001', '2000', ...
    endPrdDe: Optional[str] = None
    params: Dict[str, Any] = None     # objL1~ filters 등 추가 파라미터
    value_col: str = "DATA_VALUE"     # 응답 값 컬럼명(일반적으로 DATA_VALUE)
    date_col: str = "PRD_DE"          # 기간 컬럼
    unit: Optional[str] = None        # 메타(선택)
    transform_hint: Optional[str] = None  # 'dlog' 등 (후속 단계에서 활용)
    cache_key: Optional[str] = None   # 캐시 파일명 키(없으면 자동)

    def key(self) -> str:
        base = self.cache_key or f"{self.logical_name}_{self.orgId or 'NA'}_{self.tblId or self.userStatsId or 'NA'}_{self.prdSe}"
        return base.replace("/", "_")

# -------- 유틸: PRD_DE → pandas Period 변환 --------
def parse_period(prd_de: str, prdSe: str) -> pd.Period:
    """KOSIS PRD_DE 문자열을 pandas Period로 변환"""
    s = str(prd_de)
    if prdSe == "M":
        # YYYYMM
        year = int(s[:4]); month = int(s[4:6])
        return pd.Period(freq="M", year=year, month=month)
    elif prdSe == "Q":
        # YYYYQ? → 종종 YYYYMM 포맷으로 올 때도 있어 YYYYMM -> 분기 계산
        if len(s) == 6:
            year = int(s[:4]); month = int(s[4:6])
            q = (month - 1)//3 + 1
            return pd.Period(freq="Q", year=year, quarter=q)
        # 혹시 "YYYYQn" 형태면 처리
        if "Q" in s:
            year = int(s[:4]); q = int(s[-1])
            return pd.Period(freq="Q", year=year, quarter=q)
        # 연도만 올 때는 4분기로 가정
        if len(s) == 4:
            return pd.Period(freq="Q", year=int(s), quarter=4)
        raise ValueError(f"Unknown PRD_DE for Q: {s}")
    elif prdSe == "A":
        # YYYY
        return pd.Period(freq="A", year=int(s))
    else:
        # 기본은 YYYYMM 처리
        year = int(s[:4]); month = int(s[4:6])
        return pd.Period(freq="M", year=year, month=month)

# -------- API 호출 --------
def _build_params(ent: SeriesEntry) -> Dict[str, Any]:
    p = {
        "method": "getList",
        "apiKey": KOSIS_API_KEY,
        "format": "json",
        "type": "json",
    }
    # userStatsId 방식
    if ent.userStatsId:
        p["userStatsId"] = ent.userStatsId
    # orgId/tblId 방식
    if ent.orgId: p["orgId"] = ent.orgId
    if ent.tblId: p["tblId"] = ent.tblId
    # 기간/주기
    if ent.prdSe: p["prdSe"] = ent.prdSe
    if ent.startPrdDe: p["startPrdDe"] = ent.startPrdDe
    if ent.endPrdDe: p["endPrdDe"] = ent.endPrdDe
    # 추가 필터
    if ent.params:
        p.update(ent.params)
    return p

def _http_get(url: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    r = requests.get(url, params=params, timeout=DEFAULT_TIMEOUT)
    r.raise_for_status()
    data = r.json()
    # KOSIS는 에러 시 dict로 code/message 반환하기도 함
    if isinstance(data, dict) and data.get("errMsg") or data.get("errorMsg"):
        raise RuntimeError(f"KOSIS error: {data}")
    # 정상은 list[dict]
    if isinstance(data, list):
        return data
    # 간혹 중첩 구조가 있을 수 있어 평탄화 시도
    if "list" in data and isinstance(data["list"], list):
        return data["list"]
    raise RuntimeError(f"Unexpected response: {str(data)[:300]}")

def _fetch_one(ent: SeriesEntry, base_url: Optional[str]=None) -> pd.DataFrame:
    url = base_url or ent.endpoint or DEFAULT_BASE_URL
    params = _build_params(ent)
    time.sleep(RATE_LIMIT_SLEEP)  # rate limit
    rows = _http_get(url, params)
    if not rows:
        return pd.DataFrame(columns=[ent.date_col, ent.value_col])
    df = pd.DataFrame(rows)
    if ent.date_col not in df.columns:
        # 일부 API 응답은 prdDe 등 소문자일 수 있음: 케이스 보정
        candidates = [c for c in df.columns if c.lower() in ("prd_de","prdde","period","time")]
        if candidates:
            df.rename(columns={candidates[0]: ent.date_col}, inplace=True)
        else:
            raise KeyError(f"Cannot find date column in response: {df.columns.tolist()}")
    if ent.value_col not in df.columns:
        # 값 컬럼 보정 시도
        candidates = [c for c in df.columns if c.lower() in ("data_value","dt","value","data")]
        if candidates:
            df.rename(columns={candidates[0]: ent.value_col}, inplace=True)
        else:
            raise KeyError(f"Cannot find value column in response: {df.columns.tolist()}")

    # 숫자 변환
    df[ent.value_col] = pd.to_numeric(df[ent.value_col], errors="coerce")
    # Period index
    df["period"] = df[ent.date_col].map(lambda s: parse_period(str(s), ent.prdSe))
    df = df[["period", ent.value_col]].dropna()
    # 같은 period에 다중 행이면 평균
    df = df.groupby("period", as_index=False).mean(numeric_only=True)
    # 메타 보관
    if ent.unit:
        df.attrs["unit"] = ent.unit
    df.attrs["logical_name"] = ent.logical_name
    df.attrs["prdSe"] = ent.prdSe
    return df

# -------- era(분리 테이블) 자동 병합 --------
def _merge_eras(frames: List[pd.DataFrame], logical_name: str, target_freq: str) -> pd.Series:
    """여러 era를 union하여 하나의 시리즈로 병합. 겹치는 기간은 '가장 최신 호출 순서'를 우선."""
    out = pd.Series(dtype="float64", name=logical_name)
    for df in frames:
        s = df.set_index("period").sort_index()[df.columns[-1]].copy()
        # 동일 period 중복 시 뒤에서 덮어쓰기 (최신 우선)
        out = s.combine_first(out)  # 기존 값을 유지하고, 새로운 값으로 빈칸 채움
        out = out.combine_first(s)  # 양방향 보정
        # 최종은 최근 era가 우선하도록 마지막에 s로 덮기
        out.loc[s.index] = s
    # 주기 표준화
    out.index = out.index.asfreq(target_freq, how="end")
    return out.sort_index()

# -------- public API --------
def load_series_yaml(path: str) -> List[SeriesEntry]:
    """series.yaml 로드.
    - 같은 logical_name을 여러 줄로 정의(era 분할)하면 자동 병합 대상이 됨.
    - 필드:
      logical_name, orgId/tblId (or userStatsId), prdSe(M/Q/A), startPrdDe, endPrdDe,
      params(필터), endpoint(옵션), value_col/ date_col(옵션)
    """
    import yaml
    with open(path, "r", encoding="utf-8") as f:
        raw = yaml.safe_load(f)
    entries: List[SeriesEntry] = []
    for row in raw.get("series", []):
        ent = SeriesEntry(
            logical_name=row["logical_name"],
            orgId=row.get("orgId"),
            tblId=row.get("tblId"),
            userStatsId=row.get("userStatsId"),
            endpoint=row.get("endpoint"),
            prdSe=row.get("prdSe", "Q"),
            startPrdDe=str(row.get("startPrdDe")) if row.get("startPrdDe") else None,
            endPrdDe=str(row.get("endPrdDe")) if row.get("endPrdDe") else None,
            params=row.get("params") or {},
            value_col=row.get("value_col", "DATA_VALUE"),
            date_col=row.get("date_col", "PRD_DE"),
            unit=row.get("unit"),
            transform_hint=row.get("transform_hint"),
            cache_key=row.get("cache_key"),
        )
        entries.append(ent)
    return entries

def _cache_path(key: str) -> str:
    os.makedirs(RAW_DIR, exist_ok=True)
    return os.path.join(RAW_DIR, f"{key}.parquet")

def _load_cache(key: str) -> Optional[pd.DataFrame]:
    p = _cache_path(key)
    if os.path.exists(p):
        try:
            return pd.read_parquet(p)
        except Exception:
            return None
    return None

def _save_cache(key: str, df: pd.DataFrame):
    p = _cache_path(key)
    try:
        df.to_parquet(p, index=False)
    except Exception:
        pass

def fetch_kosis_series(series_list: List[Dict], start: Optional[str]=None, end: Optional[str]=None,
                       target_freq: str = FREQ, use_cache: bool=True) -> pd.DataFrame:
    """series.yaml 등으로 받은 리스트를 바탕으로 wide DataFrame 반환.
    - 동일 logical_name의 여러 era를 자동 병합
    - target_freq('Q','M','A')에 맞춰 PeriodIndex 정규화
    """
    # 1) 입력 정규화
    entries = []
    for row in series_list:
        ent = SeriesEntry(
            logical_name=row["logical_name"],
            orgId=row.get("orgId"),
            tblId=row.get("tblId"),
            userStatsId=row.get("userStatsId"),
            endpoint=row.get("endpoint"),
            prdSe=row.get("prdSe", "Q"),
            startPrdDe=str(row.get("startPrdDe") or start) if (row.get("startPrdDe") or start) else None,
            endPrdDe=str(row.get("endPrdDe") or end) if (row.get("endPrdDe") or end) else None,
            params=row.get("params") or {},
            value_col=row.get("value_col", "DATA_VALUE"),
            date_col=row.get("date_col", "PRD_DE"),
            unit=row.get("unit"),
            transform_hint=row.get("transform_hint"),
            cache_key=row.get("cache_key"),
        )
        entries.append(ent)

    # 2) 병렬 수집
    frames_by_logical: Dict[str, List[pd.DataFrame]] = {}
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as ex:
        futs = {}
        for ent in entries:
            key = ent.key()
            if use_cache:
                cached = _load_cache(key)
                if cached is not None:
                    frames_by_logical.setdefault(ent.logical_name, []).append(cached)
                    continue
            futs[ex.submit(_fetch_one, ent, None)] = ent

        for fut in as_completed(futs):
            ent = futs[fut]
            try:
                df = fut.result()
                frames_by_logical.setdefault(ent.logical_name, []).append(df)
                if use_cache and not df.empty:
                    _save_cache(ent.key(), df)
            except Exception as e:
                print(f"[WARN] fetch failed: {ent.logical_name} ({ent.orgId}/{ent.tblId}/{ent.userStatsId}) — {e}")

    # 3) era 병합 → wide
    series_map: Dict[str, pd.Series] = {}
    for logical_name, frames in frames_by_logical.items():
        if not frames:
            continue
        s = _merge_eras(frames, logical_name, target_freq)
        series_map[logical_name] = s

    if not series_map:
        return pd.DataFrame()

    wide = pd.DataFrame(series_map)
    wide.index.name = "period"
    return wide.sort_index()

# ---- 데모용 기존 함수 유지 (모의데이터 로딩) ----
def load_mock() -> pd.DataFrame:
    """data/mock_timeseries.csv를 분기로 집계한 데모."""
    df = pd.read_csv("data/mock_timeseries.csv", parse_dates=["date"]).set_index("date").sort_index()
    q = df.resample("Q").mean()
    return q
