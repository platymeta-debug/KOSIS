"""CLI utility to build catalog candidates from the KOSIS list API."""

from __future__ import annotations

import argparse

from src.catalog_builder import build_catalog
from src.io_helpers import save_csv


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--vwcd",
        default="MT_ZTITLE",
        help="목록 보기 코드(예: MT_ZTITLE=주제별, MT_GTITLE=기관별)",
    )
    parser.add_argument(
        "--roots",
        nargs="+",
        required=True,
        help="parentListId 루트들(여러 개 가능)",
    )
    parser.add_argument("--out", default="series_catalog.csv")
    parser.add_argument("--max-depth", type=int, default=6)
    args = parser.parse_args()

    df = build_catalog(args.vwcd, args.roots, max_depth=args.max_depth)
    save_csv(df, args.out)
    print(f"[catalog] 후보 {len(df)}건 저장 → {args.out}")


if __name__ == "__main__":
    main()
