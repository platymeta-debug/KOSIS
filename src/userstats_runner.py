from __future__ import annotations
import os, csv, re
from typing import List, Dict, Any, Optional
from .kosis_api import fetch_userstats

USERSTATS_RE = re.compile(r"^[\w\-]+/\d+/DT_[A-Z0-9_]+/.+$")  # 느슨한 검증


def _load_userstats_list(userstats_args: Optional[List[str]]) -> List[str]:
    if not userstats_args:
        return []
    # 단일 인자이며 파일처럼 보이면 우선 파일 시도
    cand = userstats_args[0] if len(userstats_args) == 1 else None
    items: List[str] = []
    if cand and (cand.lower().endswith(".txt") or os.path.isfile(cand)) and os.path.isfile(cand):
        with open(cand, "r", encoding="utf-8") as f:
            for line in f:
                s = line.strip()
                if not s or s.startswith("#"):
                    continue
                if s in {'@"', '"@'} or 'Set-Content' in s:
                    continue
                items.append(s)
        return items
    # 직접 나열
    for v in userstats_args:
        for tok in str(v).replace("\r", "\n").split("\n"):
            s = tok.strip()
            if not s or s.startswith("#"):
                continue
            if s in {'@"', '"@'} or 'Set-Content' in s:
                continue
            items.append(s)
    return items


def _flatten_rows(rows: List[Dict[str, Any]]):
    keys = set()
    for r in rows:
        keys.update(r.keys())
    return sorted(keys), rows


def run_userstats_batch(
    userstats_args: Optional[List[str]],
    *,
    out: Optional[str] = None,
    verbose: bool = False,
    prdSe=None,
    startPrdDe=None,
    endPrdDe=None,
) -> int:
    lst = _load_userstats_list(userstats_args)
    if verbose:
        print(f"[userstats] input count={len(lst)}")
    all_rows: List[Dict[str, Any]] = []
    for i, usid in enumerate(lst, 1):
        if verbose:
            print(f"[userstats] ({i}/{len(lst)}) fetch userStatsId={usid}")
        try:
            rows = fetch_userstats(
                usid,
                prdSe=prdSe,
                startPrdDe=startPrdDe,
                endPrdDe=endPrdDe,
                verbose=verbose,
            ) or []
            for r in rows:
                r["_userStatsId"] = usid
            all_rows.extend(rows)
        except Exception as e:
            print(f"[userstats][warn] {usid} err={e}")
    if out:
        import os

        os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
        cols, recs = _flatten_rows(all_rows)
        with open(out, "w", newline="", encoding="utf-8") as f:
            w = csv.DictWriter(f, fieldnames=cols)
            w.writeheader()
            w.writerows(recs)
        if verbose:
            print(f"[userstats] saved: {out} rows={len(all_rows)}")
    else:
        if verbose:
            print(f"[userstats] done. rows={len(all_rows)} (no file)")
    return 0

