"""KOSIS API client stub.
Plug in your API key and series IDs to fetch real data.
For demo, we use local CSV mock via load_mock().
"""
from __future__ import annotations
import os
import pandas as pd
from typing import List, Dict, Optional
from .config import KOSIS_API_KEY

def fetch_kosis_series(series_list: List[Dict], start: Optional[str]=None, end: Optional[str]=None) -> pd.DataFrame:
    """Fetch multiple series from KOSIS. Returns a wide DataFrame indexed by period.
    series_list: list of dicts with keys: kosis_id, name, freq, transform
    NOTE: Implement actual API call here using requests, build DataFrame, and return.
    """
    raise NotImplementedError("Implement KOSIS API calls here.")

def load_mock() -> pd.DataFrame:
    """Load mock synthetic series (monthly) from data/mock_timeseries.csv and return quarterly aggregated.    Columns include: date,population_total,policy_rate,m2,house_price,bank_loan,cpi,gdp_growth
    """
    df = pd.read_csv("data/mock_timeseries.csv", parse_dates=["date"]).set_index("date").sort_index()
    # Convert to quarterly: average for rates/indexes, sum for growth if needed (we'll average here for simplicity)
    q = df.resample("Q").mean()
    return q
