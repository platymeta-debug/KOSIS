# -*- coding: utf-8 -*-
from __future__ import annotations
import os, csv, sys
from typing import Iterable, List, Dict, Any, Tuple, Optional

try:
    from .kosis_api import fetch_userstats
except ImportError:
    # 상대 임포트 보정(직접 실행 대비)
    from kosis_api import fetch_userstats


def _load_userstats_list(arg_list: Optional[List[str]]) -> List[str]:
    """
    arg_list:
      - ["path/to/file.txt"] → 파일 경로면 줄단위 로드(# 주석, 빈줄 무시)
      - ["a/b/c", "x/y/z"]  → 직접 나열
    """
    if not arg_list:
        return []
    if len(arg_list) == 1 and os.path.isfile(arg_list[0]):
        path = arg_list[0]
        items: List[str] = []
        with open(path, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                items.append(s)
        return items
    # 공백/줄바꿈으로 섞여 들어온 경우도 분리
    items: List[str] = []
    for v in arg_list:
        if not v:
            continue
        for tok in str(v).replace("\r", "\n").split("\n"):
            tok = tok.strip()
            if tok and not tok.startswith("#"):
                items.append(tok)
    return items


def _flatten_rows(rows: List[Dict[str, Any]]) -> Tuple[List[str], List[Dict[str, Any]]]:
    """
    서로 키가 다른 레코드들을 CSV로 쓰기 위해 헤더를 집합화.
    """
    keys = set()
    for r in rows:
        keys.update(r.keys())
    cols = sorted(keys)
    return cols, rows


def run_userstats_batch(
    userstats_args: Optional[List[str]],
    out: Optional[str] = None,
    verbose: bool = False,
    prdSe: Optional[str] = None,
    startPrdDe: Optional[str] = None,
    endPrdDe: Optional[str] = None,
) -> int:
    us_list = _load_userstats_list(userstats_args)
    if verbose:
        print(f"[userstats] input count={len(us_list)}")

    all_rows: List[Dict[str, Any]] = []
    for idx, usid in enumerate(us_list, 1):
        if verbose:
            print(f"[userstats] ({idx}/{len(us_list)}) fetch userStatsId={usid}")
        try:
            rows = fetch_userstats(
                userStatsId=usid,
                verbose=verbose,
                prdSe=prdSe,
                startPrdDe=startPrdDe,
                endPrdDe=endPrdDe,
            ) or []
            for r in rows:
                r["_userStatsId"] = usid
            all_rows.extend(rows)
        except Exception as e:
            print(f"[userstats][warn] failed: {usid} err={e}")

    if out:
        os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
        cols, rows2 = _flatten_rows(all_rows)
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(rows2)
        if verbose:
            print(f"[userstats] saved: {out} rows={len(all_rows)}")
    else:
        if verbose:
            print(f"[userstats] done. rows={len(all_rows)} (no file)")

    return 0
