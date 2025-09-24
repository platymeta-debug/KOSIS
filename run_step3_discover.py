"""Step 3: Discover cross-series relationships across the observation matrix."""

from __future__ import annotations

from src import discover, features, store


# TODO: pull the obs table into a dataframe, expand quick_discover with partial correlations,
#       Granger causality with FDR control, mutual information, and persistence/lead-lag metrics.

if __name__ == "__main__":
    store.init()
    print("Discover stage placeholder. Expand quick_discover scoring before use.")
