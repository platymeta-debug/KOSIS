"""Step 2: Normalise harvested payloads and engineer wide feature matrices."""

from __future__ import annotations

import argparse
import glob
import json
from typing import List

import pandas as pd
from tqdm import tqdm

from src.normalize import normalize_payload, store_obs
from src.features import build_wide
from src import store


def _load_json_glob(pattern: str) -> pd.DataFrame:
    frames: List[pd.DataFrame] = []
    for path in glob.glob(pattern):
        with open(path, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        frames.append(pd.DataFrame(payload))
    return pd.concat(frames, ignore_index=True) if frames else pd.DataFrame()


def _load_raw(path: str) -> pd.DataFrame:
    if path.endswith(".parquet"):
        return pd.read_parquet(path)
    if path.endswith(".csv"):
        return pd.read_csv(path)
    return _load_json_glob(path)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--raw", default="out_data.parquet", help="Harvest output from step 1 (parquet preferred).")
    parser.add_argument("--prdSe", default=None, help="Fallback prdSe to apply when the raw dataset omits it.")
    parser.add_argument("--logical-name", default="kosis.series", help="Fallback logical_name for unnamed payloads.")
    parser.add_argument("--wide-out", default="out_wide.parquet", help="Destination path for the engineered wide frame.")
    parser.add_argument("--limit", type=int, default=1200, help="Maximum logical series to retain in the wide matrix.")
    args = parser.parse_args()

    store.init()
    raw = _load_raw(args.raw)
    if raw.empty:
        print("[prepare] No raw records found – skipping normalisation.")
    else:
        if "prdSe" not in raw.columns and args.prdSe:
            raw["prdSe"] = args.prdSe
        if "logical_name" not in raw.columns:
            raw["logical_name"] = args.logical_name

        groups = raw.groupby(["logical_name", "prdSe"], dropna=False)
        for (logical_name, prd_se), group in tqdm(groups, total=groups.ngroups):
            rows = group.to_dict(orient="records")
            try:
                df_norm = normalize_payload(str(logical_name), rows, str(prd_se))
            except ValueError as exc:
                print(f"[prepare] skip logical_name={logical_name} prdSe={prd_se}: {exc}")
                continue
            store_obs(df_norm)

    wide = build_wide(limit=args.limit)
    if wide.empty:
        print("[prepare] No observations available for wide matrix construction.")
    else:
        wide.to_parquet(args.wide_out)
        print(f"[prepare] wide shape={wide.shape} → {args.wide_out}")
