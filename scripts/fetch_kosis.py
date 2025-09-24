import argparse
from pathlib import Path
from src.kosis_client import load_series_yaml, fetch_kosis_series

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--series-yaml", required=True)
    ap.add_argument("--out", default="out/kosis_merged.parquet")
    ap.add_argument("--freq", default="Q")
    args = ap.parse_args()

    entries = load_series_yaml(args.series_yaml)
    df = fetch_kosis_series([e.__dict__ for e in entries], target_freq=args.freq, use_cache=True)
    Path(args.out).parent.mkdir(parents=True, exist_ok=True)
    df.to_parquet(args.out)
    print(f"Saved: {args.out} (shape={df.shape})")

if __name__ == "__main__":
    main()
