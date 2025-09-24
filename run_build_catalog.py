# run_build_catalog.py — Auto-Discover + Auto-Fallback + Safe Exit
from __future__ import annotations
import argparse, sys, time, itertools, string
import pandas as pd
from typing import Iterable, List

from src.catalog_builder import build_catalog
from src.kosis_api import list_stats

# 기본 폴백 후보 (빠른 재시도)
FALLBACKS = [
    ("MT_ZTITLE", ["ROOT", "A", "B", "0"]),
    ("MT_GTITLE", ["ROOT", "A", "B", "0"]),
    ("MT_ZTITLE", ["AA", "AB", "AC", "A1", "A2"]),
    ("MT_GTITLE", ["AA", "AB", "AC", "A1", "A2"]),
]

def _chunks(it: Iterable[str], n: int) -> Iterable[List[str]]:
    buf = []
    for x in it:
        buf.append(x)
        if len(buf) >= n:
            yield buf; buf = []
    if buf: yield buf

def generate_seeds(depth: int = 2) -> Iterable[str]:
    """
    parentListId 후보 생성기
    depth=1: A..Z, 0..9
    depth=2: AA..ZZ, A0..Z9
    + 특수 루트 후보: ROOT, 0, 00
    """
    yield "ROOT"; yield "0"; yield "00"
    # 1글자
    for c in string.ascii_uppercase: yield c
    for d in "0123456789": yield d
    if depth >= 2:
        for a,b in itertools.product(string.ascii_uppercase, string.ascii_uppercase):
            yield f"{a}{b}"
        for a,b in itertools.product(string.ascii_uppercase, "0123456789"):
            yield f"{a}{b}"

def probe_one(vwcd: str, pid: str) -> int:
    """주어진 vwCd/pid 조합으로 하위 항목 수를 조사 (0=없음)"""
    try:
        rows = list_stats(vwcd, pid)
        return len(rows or [])
    except Exception:
        return 0

def auto_discover_roots(vwcds: List[str], max_tries: int = 500, time_budget: float = 90.0, rate_sleep: float = 0.35) -> tuple[str, List[str]]:
    """
    여러 vwCd(MT_ZTITLE/MT_GTITLE 등)와 다양한 parentListId 후보를 스캔하여
    '하위 항목이 존재'하는 parentListId를 찾아 반환.
    - max_tries: API 호출 상한
    - time_budget: 초 단위 총 소요 한도
    """
    t0 = time.time()
    tried = 0
    seeds = generate_seeds(depth=2)  # 필요하면 depth=3로 확장 가능(호출수↑)
    batch_iter = _chunks(seeds, 8)   # 8개씩 묶어 빠르게 테스트

    for vw in vwcds:
        for batch in batch_iter:
            for pid in batch:
                if tried >= max_tries or (time.time() - t0) > time_budget:
                    break
                n = probe_one(vw, pid)
                tried += 1
                if n > 0:
                    print(f"[discover] HIT: vwCd={vw} parentListId={pid} → {n} rows")
                    # 첫 히트만 반환 (원한다면 여러개 모아도 됨)
                    return vw, [pid]
                time.sleep(rate_sleep)
            if tried >= max_tries or (time.time() - t0) > time_budget:
                break
    print(f"[discover] no root found within tries={tried}, time={time.time()-t0:.1f}s")
    return "", []

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
    ap.add_argument("--auto-fallback", action="store_true", help="실패/빈 결과 시 내장 후보로 재시도")
    ap.add_argument("--auto-discover", action="store_true", help="내장 스캐너로 parentListId 자동 탐색")
    ap.add_argument("--discover-max-tries", type=int, default=500)
    ap.add_argument("--discover-time-budget", type=float, default=90.0)
    args = ap.parse_args()

    # 0) 사용자 입력 우선 시도
    df = pd.DataFrame()
    try:
        df = try_build(args.vwcd, args.roots, args.max_depth)
    except Exception as e:
        print(f"[catalog] initial attempt failed: {e}")

    # 1) 필요 시 fallback 후보로 재시도
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

    # 2) 그래도 실패면 auto-discover로 루트 탐색
    if (df is None or df.empty) and args.auto_discover:
        vw, roots = auto_discover_roots(
            vwcds=["MT_ZTITLE","MT_GTITLE"],
            max_tries=args.discover_max_tries,
            time_budget=args.discover_time_budget,
            rate_sleep=0.35
        )
        if vw and roots:
            try:
                df = try_build(vw, roots, args.max_depth)
            except Exception as e:
                print(f"[discover] build failed: {e}")

            if df is None or df.empty:
                print("[discover] retry with deeper depth (max-depth+4)")
                try:
                    df = try_build(vw, roots, args.max_depth + 4)
                except Exception as e:
                    print(f"[discover] deep build failed: {e}")

            if df is not None and not df.empty:
                args.vwcd, args.roots = vw, roots

    # 3) 최종 판정
    if df is None or df.empty:
        # 빈 카탈로그면 상위에서 템플릿 워크플로로 넘어가도록 종료코드 2 반환
        print("[catalog] no data. could not determine valid parentListId.")
        print("[catalog] run_all.py will generate a template series_catalog.csv and stop gracefully.")
        sys.exit(2)

    # 4) 저장
    df.to_csv(args.out, index=False)
    print(f"[catalog] saved {len(df)} rows → {args.out}")
    print(f"[catalog] used vwCd={args.vwcd}, roots={args.roots}")

if __name__ == "__main__":
    main()
