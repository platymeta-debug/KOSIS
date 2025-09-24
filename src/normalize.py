from __future__ import annotations

import json
import re
from typing import Optional

import pandas as pd

from .db import connect

def _detect_date(cols):
    for k in ["PRD_DE","prdDe","time","period"]:
        if k in cols: return k
    return None

def _detect_val(cols):
    # KOSIS 표준은 DT
    for k in ["DT","DATA_VALUE","data_value","value","DT_VAL"]:
        if k in cols: return k
    return None

def _series_key(logical_name, row: dict):
    drop = {"PRD_DE","prdDe","time","period","DATA_VALUE","DT","DT_VAL","data_value","value"}
    dims = {k: row.get(k) for k in row.keys() if k not in drop}
    return logical_name + "|" + json.dumps(dims, ensure_ascii=False, sort_keys=True)


def _normalize_freq(freq: Optional[str], raw_period: str) -> str:
    if freq:
        base = freq.upper()
        if base.startswith("M"):
            return "M"
        if base.startswith("Q"):
            return "Q"
        if base.startswith("A") or base.startswith("Y"):
            return "A"
    digits = re.sub(r"[^0-9]", "", raw_period)
    if len(digits) >= 6:
        return "M"
    if re.search(r"[1-4]$", raw_period):
        return "Q"
    return "A"


def _parse_period(per: str, freq_hint: Optional[str]) -> Optional[pd.Period]:
    s = str(per).strip()
    if not s:
        return None
    freq = _normalize_freq(freq_hint, s)
    digits = re.sub(r"[^0-9]", "", s)
    if freq == "M":
        if len(digits) < 6:
            return None
        y, m = digits[:4], digits[4:6]
        return pd.Period(f"{y}-{m}", freq="M")
    if freq == "Q":
        year_match = re.search(r"(19|20)\d{2}", s)
        quarter_match = re.search(r"([1-4])", s[::-1])
        if not (year_match and quarter_match):
            return None
        year = year_match.group(0)
        quarter = quarter_match.group(1)
        return pd.Period(f"{year}Q{quarter}", freq="Q")
    year_match = re.search(r"(19|20)\d{2}", s)
    if not year_match:
        return None
    return pd.Period(year_match.group(0), freq="A")

def normalize_latest_snapshot():
    con = connect()
    raw = con.execute("""
      SELECT r.catalog_id, s.logical_name, s.prd_se, r.fetched_at, r.payload
      FROM raw_kosis r
      JOIN series_catalog s USING(catalog_id)
      QUALIFY row_number() OVER (PARTITION BY r.catalog_id ORDER BY r.fetched_at DESC)=1
    """).df()

    rows = []
    for _, row in raw.iterrows():
        payload = json.loads(row["payload"])
        if len(payload) and isinstance(payload[0], list):
            payload = payload[0]  # values 형식 정리
        if not isinstance(payload, list): continue
        if not payload: continue
        cols = set(payload[0].keys())
        dcol = _detect_date(cols); vcol = _detect_val(cols)
        if not dcol or not vcol: continue
        for r in payload:
            val = r.get(vcol, None)
            raw_period = r.get(dcol, "")
            if val in (None, ""):
                continue
            period_obj = _parse_period(str(raw_period), row.get("prd_se"))
            if period_obj is None:
                continue
            ts = period_obj.to_timestamp(how="end")
            freq_code = period_obj.freqstr[0]
            rows.append({
                "catalog_id": row["catalog_id"],
                "series_key": _series_key(row["logical_name"], r),
                "period": ts.strftime("%Y-%m-%d"),
                "period_freq": freq_code,
                "value": float(val),
                "dims": json.dumps({k:r.get(k) for k in r.keys() if k not in (dcol, vcol)}, ensure_ascii=False),
                "unit": r.get("UNIT_NM") or r.get("UNIT_NM_ENG") or r.get("UNIT_NAME") or r.get("unitName") or ""
            })

    if rows:
        df = pd.DataFrame(rows)
        df = df.drop_duplicates(
            subset=["catalog_id", "series_key", "period"], keep="last"
        )
        con.register("df_obs", df)
        con.execute(
            """
            MERGE INTO obs AS t
            USING df_obs AS s
            ON t.catalog_id = s.catalog_id
               AND t.series_key = s.series_key
               AND t.period = s.period
            WHEN MATCHED THEN UPDATE SET
                period_freq = s.period_freq,
                value = s.value,
                dims = s.dims,
                unit = s.unit
            WHEN NOT MATCHED THEN INSERT
                (catalog_id, series_key, period, period_freq, value, dims, unit)
            VALUES
                (s.catalog_id, s.series_key, s.period, s.period_freq, s.value, s.dims, s.unit)
            """
        )
        print(f"[normalize] rows={len(df)}")
    else:
        print("[normalize] no rows")
