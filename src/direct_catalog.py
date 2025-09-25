"""Helpers for the catalogue direct-collection mode.

The direct mode walks the category hierarchy via ``statisticsList`` calls and
collects table identifiers (``TBL_ID``) without performing the expensive
depth-first traversal used by the legacy discover mode.
"""

from __future__ import annotations

import csv
import os
from typing import Iterable

from .kosis_api import list_stats

LEAF_SE = {"TBL", "DT", "TB", "STAT", "TABLE"}


def _leaf_tbl_id(node: dict) -> str | None:
    return (
        node.get("TBL_ID")
        or node.get("tblId")
        or node.get("STATBL_ID")
        or node.get("statblId")
    )


def _child_id(node: dict) -> str | None:
    return (
        node.get("LIST_ID")
        or node.get("listId")
        or node.get("LIST_CD")
        or node.get("listCd")
    )


def _is_leaf(node: dict) -> bool:
    se = (node.get("LIST_SE") or node.get("listSe") or "").upper()
    return bool(_leaf_tbl_id(node)) or (se in LEAF_SE)


def _autoload_roots(vwcd: str, parent: str = "A", verbose: bool = False) -> list[str]:
    rows = list_stats(vwcd, parent, verbose=verbose) or []
    roots: list[str] = []
    for row in rows:
        child = _child_id(row)
        if child:
            roots.append(child)
    if verbose:
        print(f"[direct] autoloaded roots={len(roots)}")
    return roots


def _ensure_iterable(roots: Iterable[str] | None) -> list[str]:
    if not roots:
        return []
    if isinstance(roots, (list, tuple, set)):
        return list(roots)
    return [roots]  # type: ignore[list-item]


def run_direct_catalog(
    *,
    vwcd: str,
    roots: Iterable[str] | None,
    out: str | None,
    max_depth: int = 4,
    verbose: bool = False,
) -> None:
    roots_list = _ensure_iterable(roots)
    if roots_list and len(roots_list) == 1 and roots_list[0].upper() in {"AUTO", "TOP"}:
        roots_list = _autoload_roots(vwcd, "A", verbose) or ["A"]
    elif not roots_list:
        roots_list = _autoload_roots(vwcd, "A", verbose) or ["A"]

    seen: set[str] = set()
    tbl_rows: list[dict[str, str | int | None]] = []

    def dfs(node: str, depth: int) -> None:
        if depth > max_depth:
            return
        rows = list_stats(vwcd, node, verbose=verbose) or []
        for row in rows:
            if _is_leaf(row):
                tbl = _leaf_tbl_id(row)
                if not tbl:
                    continue
                if tbl in seen:
                    continue
                seen.add(tbl)
                tbl_rows.append(
                    {
                        "orgId": row.get("ORG_ID") or row.get("orgId"),
                        "tblId": tbl,
                        "tblNm": row.get("TBL_NM") or row.get("tblNm"),
                        "pathParent": node,
                        "depth": depth,
                    }
                )
            else:
                child = _child_id(row)
                if child:
                    dfs(child, depth + 1)

    for root in roots_list:
        dfs(root, 1)

    if out:
        directory = os.path.dirname(out) or "."
        os.makedirs(directory, exist_ok=True)
        with open(out, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(
                fh, fieldnames=["orgId", "tblId", "tblNm", "pathParent", "depth"]
            )
            writer.writeheader()
            for row in tbl_rows:
                writer.writerow(row)

    if verbose:
        print(f"[direct] collected TBL count={len(tbl_rows)}; saved={bool(out)}")

