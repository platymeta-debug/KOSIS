"""Step 2: Normalise harvested payloads and engineer base features."""

from __future__ import annotations

from src import features, normalize, store


# TODO: load raw payloads, parse prdSe according to KOSIS guide, and populate obs via normalize.normalize_and_store.
# TODO: extend features.build_wide with ratio/spread/growth calculations before discovery.

if __name__ == "__main__":
    store.init()
    print("Prepare stage placeholder. Implement normalisation and feature creation.")
