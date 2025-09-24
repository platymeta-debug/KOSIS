# run_build_catalog.py  — 자동-복구형
from __future__ import annotations
import argparse, sys
import pandas as pd

from src.catalog_builder import build_catalog

FALLBACKS = [
    # (vwCd, [parentListId ...])
    ("MT_ZTITLE", ["ROOT", "A", "B", "0"]),           # 주제별
    ("MT_GTITLE", ["ROOT", "A", "B", "0"]),           # 기관별
    ("MT_ZTITLE", ["AA", "AB", "AC", "A1", "A2"]),    # 일부 포털 트리에서 쓰는 패턴
    ("MT_GTITLE", ["AA", "AB", "AC", "A1", "A2"]),
]

def try_build(vwcd: str, roots: list[str], max_depth: int) -> pd.DataFrame:
    print(f"[catalog] trying vwCd={vwcd}, roots={roots}")
    df = build_catalog(vwcd, roots, max_depth=max_depth)
    n = 0 if df is None else len(df)
    print(f"[catalog] result: {n} candidates")
    return df if df is not None else pd.DataFrame()

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--vwcd", default="MT_ZTITLE", help="MT_ZTITLE=주제별, MT_GTITLE=기관별")
    ap.add_argument("--roots", nargs="+", required=True, help="parentListId 루트들")
    ap.add_argument("--out", default="series_catalog.csv")
    ap.add_argument("--max-depth", type=int, default=6)
    ap.add_argument("--auto-fallback", action="store_true", help="실패/빈 결과 시 자동 재시도")
    args = ap.parse_args()

    # 1) 먼저 사용자 입력 조합 시도
    df = pd.DataFrame()
    try:
        df = try_build(args.vwcd, args.roots, args.max_depth)
    except Exception as e:
        print(f"[catalog] initial attempt failed: {e}")

    # 2) 필요하면 자동 재시도
    if (df is None or df.empty) and args.auto_fallback:
        for vw, roots in FALLBACKS:
            try:
                df = try_build(vw, roots, args.max_depth)
                if df is not None and not df.empty:
                    args.vwcd, args.roots = vw, roots
                    break
            except Exception as e:
                print(f"[fallback] {vw}/{roots} failed: {e}")
                continue

    # 3) 최종 판정
    if df is None or df.empty:
        print("[catalog] no data. 올바른 parentListId가 필요합니다. (probe_roots.py로 탐색을 권장)")
        sys.exit(1)

    # 4) 저장
    df.to_csv(args.out, index=False)
    print(f"[catalog] saved {len(df)} rows → {args.out}")
    print(f"[catalog] used vwCd={args.vwcd}, roots={args.roots}")

if __name__ == "__main__":
    main()
