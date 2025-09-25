from __future__ import annotations

from typing import Dict, List, Set

from .kosis_api import list_nodes

LEAF_SE = {"TBL", "DT", "TB", "STAT", "TABLE"}
VW_MAP = {"SUBJ": "MT_ZTITLE", "ORG": "MT_OTITLE"}


def _se(node: Dict) -> str:
    return (node.get("LIST_SE") or node.get("listSe") or "").upper()


def _tbl(node: Dict):
    return (
        node.get("TBL_ID")
        or node.get("tblId")
        or node.get("STATBL_ID")
        or node.get("statblId")
    )


def _child(node: Dict):
    return (
        node.get("LIST_ID")
        or node.get("listId")
        or node.get("LIST_CD")
        or node.get("listCd")
    )


def _roots_autoload(vwcd: str, parent: str = "A", verbose: bool = False) -> List[str]:
    rows = list_nodes(vwcd, parent, verbose=verbose) or []
    roots: List[str] = []
    for row in rows:
        child = _child(row)
        if child:
            roots.append(child)
    if verbose:
        print(f"[direct] autoloaded roots={len(roots)}")
    return roots


def run_direct_catalog(
    vwcd: str,
    roots: List[str],
    out: str,
    max_depth: int = 5,
    verbose: bool = False,
):
    if roots and len(roots) == 1 and roots[0].upper() in ("AUTO", "TOP"):
        roots = _roots_autoload(vwcd, "A", verbose) or ["A"]

    seen: Set[str] = set()
    tbl_rows: List[Dict] = []
    noleaf_streak = 0
    MAX_NOLEAF_STREAK = 100

    def dfs(node: str, depth: int, vw: str):
        nonlocal noleaf_streak
        if depth > max_depth:
            return
        rows = list_nodes(vw, node, verbose=verbose) or []
        hit = 0
        for row in rows:
            if _tbl(row) or _se(row) in LEAF_SE:
                tbl = _tbl(row)
                if not tbl:
                    continue
                if tbl in seen:
                    continue
                seen.add(tbl)
                hit += 1
                tbl_rows.append(
                    {
                        "orgId": row.get("ORG_ID") or row.get("orgId"),
                        "tblId": tbl,
                        "tblNm": row.get("TBL_NM") or row.get("tblNm"),
                        "vwCd": vw,
                        "parent": node,
                        "depth": depth,
                    }
                )
            else:
                child = _child(row)
                if not child:
                    continue
                se = _se(row)
                next_vw = VW_MAP.get(se, vw)
                if verbose and next_vw != vw:
                    print(f"[view] switch {vw} -> {next_vw} at node={child} (se={se})")
                dfs(child, depth + 1, next_vw)
        if hit == 0:
            noleaf_streak += 1
            if verbose and (noleaf_streak % 25 == 0):
                print(f"[diag] no-leaf streak={noleaf_streak} at node={node} vwCd={vw}")
            if noleaf_streak >= MAX_NOLEAF_STREAK:
                if verbose:
                    print("[stop] early stop by no-leaf streak")
                return
        else:
            noleaf_streak = 0

    for root in roots:
        dfs(root, 1, vwcd)

    if out:
        import csv
        import os

        os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
        with open(out, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh, fieldnames=["orgId", "tblId", "tblNm", "vwCd", "parent", "depth"]
            )
            writer.writeheader()
            writer.writerows(tbl_rows)
        if verbose:
            print(f"[direct] collected TBL={len(tbl_rows)} saved={out}")

