"""Step 1: Harvest statistics metadata and payloads from KOSIS."""

from __future__ import annotations

from src import catalog, kosis_api, store


# TODO: orchestrate statistics list traversal (list_stats) to build candidate catalog entries.
# TODO: fetch payloads via data_by_userstats / data_by_params and persist via store.save_raw.
# Placeholder wiring for future implementation lives here.

if __name__ == "__main__":
    store.init()
    print("Harvest stage placeholder. Extend with actual collection logic.")
