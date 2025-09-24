"""Step 3: Exhaustive discovery of cross-series relationships."""

from __future__ import annotations

import argparse

import pandas as pd

from src.discover import discover_all


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--wide", default="out_wide.parquet", help="Wide feature matrix produced by step 2.")
    parser.add_argument("--out", default="out_signals.csv", help="Output CSV path for ranked signal pairs.")
    parser.add_argument("--corr-strong", type=float, default=0.45, help="Threshold for strong correlation tiering.")
    parser.add_argument("--corr-medium", type=float, default=0.30, help="Threshold for medium correlation tiering.")
    parser.add_argument("--maxlag", type=int, default=4, help="Maximum lag for Granger causality testing.")
    args = parser.parse_args()

    wide = pd.read_parquet(args.wide)
    pairs = discover_all(
        wide,
        corr_thr_strong=args.corr_strong,
        corr_thr_medium=args.corr_medium,
        maxlag=args.maxlag,
    )
    if pairs.empty:
        print("[discover] No eligible pairs identified.")
    else:
        filtered = pairs[pairs["tier"].isin(["strong", "medium"])].copy()
        filtered.to_csv(args.out, index=False)
        print(f"[discover] pairs={len(pairs)} saved → {args.out}")
