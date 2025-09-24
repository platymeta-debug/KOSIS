"""CLI to fetch KOSIS data payloads based on a prepared catalog file."""

from __future__ import annotations

import argparse

import pandas as pd
from tqdm import tqdm

from src.fetcher import fetch_row


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--catalog",
        default="series_catalog.csv",
        help="수집용 CSV (필드: mode/prdSe/startPrdDe/endPrdDe/…)",
    )
    parser.add_argument("--out", default="out_data.parquet")
    args = parser.parse_args()

    catalog = pd.read_csv(args.catalog, dtype=str).fillna("")
    out_rows = []
    for _, row in tqdm(catalog.iterrows(), total=len(catalog)):
        try:
            df = fetch_row(row.to_dict())
            df.insert(0, "logical_name", row.get("logical_name", ""))
            df.insert(1, "orgId", row.get("orgId", ""))
            df.insert(2, "tblId", row.get("tblId", ""))
            out_rows.append(df)
        except Exception as exc:  # pragma: no cover - network usage
            print(
                "[ERR]",
                row.get("logical_name", ""),
                row.get("orgId", ""),
                row.get("tblId", ""),
                str(exc),
            )
    if out_rows:
        all_df = pd.concat(out_rows, ignore_index=True)
        all_df.to_parquet(args.out, index=False)
        print(f"[fetch] 총 {len(all_df):,} 행 저장 → {args.out}")
    else:
        print("[fetch] 저장할 데이터가 없습니다.")


if __name__ == "__main__":
    main()
