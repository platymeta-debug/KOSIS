"""Catalog helpers for collecting candidate KOSIS tables."""

from __future__ import annotations

from typing import Any, Iterable, List

from .kosis_api import list_stats


def harvest_children(vw_cd: str, parent_id: str) -> List[dict[str, Any]]:
    """Return child statistics list entries for the given parent identifier."""

    payload = list_stats(vw_cd=vw_cd, parent_id=parent_id)
    return [entry for entry in payload if isinstance(entry, dict)]


def walk_catalog(vw_cd: str, parent_id: str, depth: int = 1) -> Iterable[dict[str, Any]]:
    """Naïve recursive walk over the statistics list tree.

    This is intentionally lightweight: production code can expand it to
    respect the KOSIS guide paging rules or enrich metadata.
    """

    queue = [(parent_id, 0)]
    while queue:
        node_id, level = queue.pop(0)
        children = harvest_children(vw_cd=vw_cd, parent_id=node_id)
        for child in children:
            yield child
            if level + 1 < depth and child.get("listId"):
                queue.append((child["listId"], level + 1))
