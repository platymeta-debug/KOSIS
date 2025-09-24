"""Build candidate table catalogues by traversing the KOSIS list API."""

from __future__ import annotations

from typing import Dict, List

import pandas as pd

from .kosis_api import list_stats


def _is_leaf(node: Dict) -> bool:
    tbl = node.get("TBL_ID") or node.get("tblId") or node.get("tbl_id")
    se = (node.get("LIST_SE") or node.get("listSe") or "").upper()
    return bool(tbl) or se in {"TBL", "DT", "TB", "STAT", "TABLE"}


def _norm(node: Dict) -> Dict:
    return {
        "listId": node.get("LIST_ID") or node.get("listId"),
        "orgId": node.get("ORG_ID") or node.get("orgId"),
        "tblId": node.get("TBL_ID") or node.get("tblId"),
        "listSe": (node.get("LIST_SE") or node.get("listSe") or "").upper(),
        "name": node.get("LIST_NM")
        or node.get("listNm")
        or node.get("TBL_NM")
        or node.get("tblNm"),
    }


def _collect_from_root(
    vw_cd: str,
    root_id: str,
    max_depth: int,
    verbose: bool,
    leaf_cap: int,
    depth: int = 0,
    acc: List[Dict] | None = None,
) -> List[Dict]:
    if acc is None:
        acc = []
    if depth > max_depth or (leaf_cap and len(acc) >= leaf_cap):
        return acc
    rows = list_stats(vw_cd, root_id, verbose=verbose)
    if verbose:
        print(f"[tree] depth={depth} root={root_id} rows={len(rows)} acc={len(acc)}")
    for row in rows:
        if leaf_cap and len(acc) >= leaf_cap:
            break
        if _is_leaf(row):
            acc.append(_norm(row))
        else:
            child = row.get("LIST_ID") or row.get("listId")
            if child:
                _collect_from_root(
                    vw_cd,
                    child,
                    max_depth,
                    verbose,
                    leaf_cap,
                    depth + 1,
                    acc,
                )
    return acc


def build_catalog(
    vw_cd: str,
    roots: List[str],
    max_depth: int = 6,
    verbose: bool = False,
    leaf_cap: int = 500,
) -> pd.DataFrame:
    acc: List[Dict] = []
    for root in roots:
        if verbose:
            print(f"[tree] root={root} start")
        _collect_from_root(vw_cd, root, max_depth, verbose, leaf_cap, 0, acc)
        if leaf_cap and len(acc) >= leaf_cap:
            break
    df = pd.DataFrame(acc).dropna(subset=["tblId"]).drop_duplicates()
    if verbose:
        print(f"[tree] collected leaves={len(df)}")
    return df
