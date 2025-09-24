from __future__ import annotations
import yaml, pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

from .db import init_db, overwrite_catalog, connect, latest_periods
from .kosis_client import fetch_full, fetch_since

def load_series_yaml(path: str) -> pd.DataFrame:
    y = yaml.safe_load(Path(path).read_text(encoding="utf-8"))
    df = pd.DataFrame(y["series"])
    if "catalog_id" not in df.columns:
        df["catalog_id"] = range(1, len(df)+1)
    # 컬럼명 통일
    df = df.rename(columns={"orgId":"org_id","tblId":"tbl_id","prdSe":"prd_se",
                            "startPrdDe":"start_prd_de","endPrdDe":"end_prd_de"})
    return df

def harvest(series_yaml: str, mode: str="update"):
    """
    mode:
      - 'full'   : catalog 전수 수집 (raw_kosis에 스냅샷 저장)
      - 'update' : obs 테이블의 마지막 period 이후만 증분 수집
    """
    con = init_db()
    cat = load_series_yaml(series_yaml)
    overwrite_catalog(cat)

    last = latest_periods()  # catalog_id, last_period
    last_map = {int(r.catalog_id): str(r.last_period) for _, r in last.iterrows()}

    results = []
    errors = []

    def work(row):
        rid = int(row.catalog_id)
        if mode == "update" and rid in last_map and last_map[rid]:
            data = fetch_since(row.to_dict(), last_map[rid])
        else:
            data = fetch_full(row.to_dict())
        return rid, data

    with ThreadPoolExecutor(max_workers=8) as ex:
        futs = {ex.submit(work, row): row for _, row in cat.iterrows()}
        for fut in as_completed(futs):
            row = futs[fut]
            try:
                rid, data = fut.result()
                results.append((rid, data))
            except Exception as e:
                errors.append((int(row.catalog_id), row.get("logical_name"), str(e)))

    # 원본 저장
    if results:
        now = pd.Timestamp.utcnow()
        out = [
            {
                "catalog_id": rid,
                "fetched_at": now,
                "payload": pd.Series([data]).to_json(orient="values"),
            }
            for rid, data in results
        ]
        df = pd.DataFrame(out)
        con.register("df_raw", df)
        con.execute("INSERT INTO raw_kosis SELECT * FROM df_raw")

        flat_rows = []
        for rid, data in results:
            if isinstance(data, list):
                for entry in data:
                    if isinstance(entry, dict):
                        rec = entry.copy()
                        rec["catalog_id"] = rid
                        rec["fetched_at"] = now
                        flat_rows.append(rec)
        if flat_rows:
            flat_df = pd.DataFrame(flat_rows)
            out_dir = Path("out/raw_snapshots")
            out_dir.mkdir(parents=True, exist_ok=True)
            raw_path = out_dir / f"kosis_raw_{now.strftime('%Y%m%d%H%M%S')}.parquet"
            try:
                flat_df.to_parquet(raw_path, index=False)
            except Exception:
                raw_path = out_dir / f"kosis_raw_{now.strftime('%Y%m%d%H%M%S')}.csv"
                flat_df.to_csv(raw_path, index=False)

    if errors:
        Path("harvest_errors.log").write_text(
            "\n".join([f"{cid}\t{name}\t{msg}" for cid,name,msg in errors]), encoding="utf-8"
        )

    print(f"[harvest] saved={len(results)}, errors={len(errors)}, mode={mode}")
