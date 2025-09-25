"""Utilities for executing batches of userStatsId fetches."""

from __future__ import annotations

import csv
import os
from typing import Iterable, List

from .kosis_api import fetch_userstats


def _load_userstats(args: Iterable[str] | None) -> List[str]:
    if not args:
        return []
    if isinstance(args, list) and len(args) == 1 and os.path.isfile(args[0]):
        with open(args[0], "r", encoding="utf-8") as fh:
            return [line.strip() for line in fh if line.strip()]
    return [item for item in args if item]


def run_userstats_batch(
    userstats_args: Iterable[str] | None,
    *,
    out: str | None = None,
    verbose: bool = False,
) -> None:
    userstats_list = _load_userstats(list(userstats_args) if userstats_args else None)
    results: list[dict] = []
    for user_stats_id in userstats_list:
        rows = fetch_userstats(user_stats_id, verbose=verbose) or []
        for row in rows:
            row["_userStatsId"] = user_stats_id
        results.extend(rows)

    if out:
        keys: set[str] = set()
        for row in results:
            keys.update(row.keys())
        fieldnames = sorted(keys)
        os.makedirs(os.path.dirname(out) or ".", exist_ok=True)
        with open(out, "w", newline="", encoding="utf-8") as fh:
            writer = csv.DictWriter(fh, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(results)

    if verbose:
        print(
            f"[userstats] userStatsId={len(userstats_list)}, "
            f"rows={len(results)}, saved={bool(out)}"
        )

