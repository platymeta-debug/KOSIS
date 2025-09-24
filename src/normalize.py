from __future__ import annotations
import json, pandas as pd
from .db import connect

def _detect_date(cols):
    for k in ["PRD_DE","prdDe","time","period"]:
        if k in cols: return k
    return None

def _detect_val(cols):
    for k in ["DATA_VALUE","data_value","DT","value"]:
        if k in cols: return k
    return None

def _series_key(logical_name, row: dict):
    dims = {k: row.get(k) for k in row.keys() if k not in ("PRD_DE","prdDe","time","period","DATA_VALUE","DT","value")}
    return logical_name + "|" + json.dumps(dims, ensure_ascii=False, sort_keys=True)

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
            val = r.get(vcol, None); per = str(r.get(dcol, ""))
            if val in (None, "") or not per: continue
            rows.append({
                "catalog_id": row["catalog_id"],
                "series_key": _series_key(row["logical_name"], r),
                "period": per,
                "period_freq": row["prd_se"] or "Q",
                "value": float(val),
                "dims": json.dumps({k:r.get(k) for k in r.keys() if k not in (dcol, vcol)}, ensure_ascii=False),
                "unit": r.get("UNIT_NAME") or r.get("unitName") or ""
            })

    if rows:
        df = pd.DataFrame(rows)
        con.register("df_obs", df)
        # upsert 유사: 같은 키면 최신 스냅샷으로 덮기
        con.execute("DELETE FROM obs")
        con.execute("INSERT INTO obs SELECT * FROM df_obs")
        print(f"[normalize] rows={len(df)}")
    else:
        print("[normalize] no rows")
